use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use reqwest::Client;
use serde::Serialize;
use serde_json::{Value, json};

use super::config::PoolConfig;

#[derive(Clone, Default, Serialize)]
pub(super) struct PoolSnapshot {
    pub available_coins: Option<f64>,
    pub available_balance: Option<f64>,
    pub balance_divergence: Option<f64>,
    pub coin_count_divergence: Option<f64>,
    pub active_reservations: Option<f64>,
    pub pending_executions: Option<f64>,
    pub chain_verified: bool,
    pub network_mismatch: bool,
    pub unreleased_growth: bool,
    pub invariant_growth: bool,
    pub oversized_growth: bool,
}

impl PoolSnapshot {
    pub fn active(&self) -> bool {
        self.active_reservations.is_some_and(|count| count > 0.0)
            || self.pending_executions.is_some_and(|count| count > 0.0)
    }
    pub fn idle(&self) -> bool {
        self.active_reservations == Some(0.0) && self.pending_executions == Some(0.0)
    }
}

pub(super) struct PoolMonitor {
    client: Client,
    url: String,
    config: PoolConfig,
    previous: BTreeMap<String, f64>,
    chain_id: Option<String>,
}

impl PoolMonitor {
    pub fn new(config: PoolConfig, url: String) -> Result<Self> {
        Ok(Self {
            client: super::http_client(config.timeout)?,
            url,
            config,
            previous: BTreeMap::new(),
            chain_id: None,
        })
    }

    pub fn reset(&mut self) {
        self.previous.clear();
    }

    pub async fn sample(&mut self) -> Result<PoolSnapshot> {
        let text = self
            .client
            .get(&self.url)
            .send()
            .await
            .context("Metrics request failed")?
            .error_for_status()
            .context("Metrics endpoint returned an error")?
            .text()
            .await
            .context("Cannot read metrics")?;
        let sponsor = self
            .config
            .sponsor_address
            .as_ref()
            .map(|address| {
                address
                    .parse::<iota_sdk_types::Address>()
                    .map(|address| address.to_string())
            })
            .transpose()?;
        let metrics = parse_metrics(&text, sponsor.as_deref())?;
        let mut sample = PoolSnapshot {
            available_coins: metrics.get("gas_station_available_gas_coin_count").copied(),
            available_balance: metrics
                .get("gas_station_available_gas_total_balance")
                .copied(),
            active_reservations: metrics.get("gas_station_active_reservations").copied(),
            pending_executions: metrics.get("gas_station_pending_executions").copied(),
            unreleased_growth: increased(&metrics, &self.previous, "num_unreleased_gas_coins"),
            invariant_growth: increased(
                &metrics,
                &self.previous,
                "num_gas_station_invariant_violations",
            ),
            oversized_growth: increased(&metrics, &self.previous, "oversized_gas_coins_count"),
            ..PoolSnapshot::default()
        };
        self.previous = metrics;
        self.compare_network(&mut sample).await?;
        Ok(sample)
    }

    async fn compare_network(&mut self, sample: &mut PoolSnapshot) -> Result<()> {
        let (Some(url), Some(sponsor)) = (&self.config.rpc_url, &self.config.sponsor_address)
        else {
            return Ok(());
        };
        let chain = self.rpc(url, "iota_getChainIdentifier", json!([])).await?;
        let chain = chain.as_str().context("Invalid chain identifier")?;
        let expected = self
            .config
            .expected_chain_id
            .as_ref()
            .or(self.chain_id.as_ref());
        if expected.is_some_and(|expected| expected != chain) {
            sample.network_mismatch = true;
            return Ok(());
        }
        self.chain_id = Some(chain.into());
        sample.chain_verified = true;
        let balance = self
            .rpc(url, "iotax_getBalance", json!([sponsor, "0x2::iota::IOTA"]))
            .await?;
        let network_balance =
            number(&balance["totalBalance"]).context("Invalid network balance")?;
        let network_coins =
            number(&balance["coinObjectCount"]).context("Invalid network coin count")?;
        // Available registry values omit reserved coins. They are comparable only
        // when the station explicitly reports that all reservations/executions drained.
        if sample.idle() {
            sample.balance_divergence = sample
                .available_balance
                .map(|value| divergence(value, network_balance));
            sample.coin_count_divergence = sample
                .available_coins
                .map(|value| divergence(value, network_coins));
        }
        Ok(())
    }

    async fn rpc(&self, url: &str, method: &str, params: Value) -> Result<Value> {
        let response: Value = self
            .client
            .post(url)
            .json(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))
            .send()
            .await
            .context("Network consistency RPC failed")?
            .error_for_status()
            .context("Network consistency RPC returned an error")?
            .json()
            .await
            .context("Invalid network consistency RPC response")?;
        ensure!(
            response.get("error").is_none_or(Value::is_null),
            "Network consistency RPC rejected the request"
        );
        response
            .get("result")
            .filter(|result| !result.is_null())
            .cloned()
            .context("Missing network consistency RPC result")
    }
}

fn number(value: &Value) -> Option<f64> {
    let number = value
        .as_u64()
        .map(|number| number as f64)
        .or_else(|| value.as_str()?.parse().ok())?;
    (number.is_finite() && number >= 0.0).then_some(number)
}

fn divergence(left: f64, right: f64) -> f64 {
    let maximum = left.max(right);
    if maximum == 0.0 {
        0.0
    } else {
        (left - right).abs() / maximum * 100.0
    }
}

fn increased(
    current: &BTreeMap<String, f64>,
    previous: &BTreeMap<String, f64>,
    name: &str,
) -> bool {
    matches!((current.get(name), previous.get(name)), (Some(current), Some(previous)) if current > previous)
}

fn parse_metrics(text: &str, sponsor: Option<&str>) -> Result<BTreeMap<String, f64>> {
    let mut metrics = BTreeMap::new();
    for line in text
        .lines()
        .filter(|line| !line.starts_with('#') && !line.trim().is_empty())
    {
        let mut fields = line.split_whitespace();
        let series = fields.next().context("Missing metric series")?;
        let name = series.split('{').next().context("Invalid metric name")?;
        let wanted = matches!(
            name,
            "gas_station_available_gas_coin_count"
                | "gas_station_available_gas_total_balance"
                | "gas_station_active_reservations"
                | "gas_station_pending_executions"
                | "num_unreleased_gas_coins"
                | "num_gas_station_invariant_violations"
                | "oversized_gas_coins_count"
        );
        if !wanted {
            continue;
        }
        if let Some(sponsor) = sponsor {
            let per_sponsor = matches!(
                name,
                "gas_station_available_gas_coin_count"
                    | "gas_station_available_gas_total_balance"
                    | "num_unreleased_gas_coins"
                    | "oversized_gas_coins_count"
            );
            if per_sponsor && !series.contains(&format!("sponsor=\"{sponsor}\"")) {
                continue;
            }
        }
        let value: f64 = fields
            .next()
            .context("Missing metric value")?
            .parse()
            .context("Invalid metric value")?;
        ensure!(
            value.is_finite() && value >= 0.0,
            "Pool metric must be finite and nonnegative"
        );
        *metrics.entry(name.to_owned()).or_default() += value;
    }
    Ok(metrics)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn metrics_missing_values_resets_and_divergence_are_conservative() {
        let previous = parse_metrics("num_unreleased_gas_coins{sponsor=\"a\"} 10\n", None).unwrap();
        let reset = parse_metrics("num_unreleased_gas_coins{sponsor=\"a\"} 1\n", None).unwrap();
        assert!(!increased(&reset, &previous, "num_unreleased_gas_coins"));
        assert!(!PoolSnapshot::default().idle());
        assert!(parse_metrics("gas_station_active_reservations NaN", None).is_err());
        assert_eq!(divergence(100.0, 80.0), 20.0);
        assert_eq!(divergence(0.0, 0.0), 0.0);
    }
}
