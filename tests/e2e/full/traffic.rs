use std::{collections::HashSet, time::Duration};

use anyhow::{Context, Result, ensure};
use gas_station_router_v2::services::reservation::ReserveGasResult;
use iota_sdk_crypto::IotaSigner;
use iota_sdk_types::Address;
use rand::Rng;
use reqwest::{
    Client, StatusCode,
    header::{AUTHORIZATION, HeaderMap, HeaderValue},
};
use serde::Serialize;
use serde_json::{Value, json};
use tokio::time::{sleep, timeout};

#[derive(Default, Serialize)]
pub(super) struct TrafficReport {
    pub transactions: usize,
    pub cache_reads: usize,
    pub reservations_by_router: [u64; 2],
    pub executions_by_router: [u64; 2],
    pub redis_errors_by_router: [u64; 2],
    pub sponsors: HashSet<Address>,
}

struct Completed {
    payload: Value,
    digest: String,
    effects: Value,
}

pub(super) struct Traffic {
    client: Client,
    node: Client,
    url: String,
    rpc: String,
    ports: [u16; 2],
    gas_price: u64,
    reservations: HashSet<u64>,
    last: Option<Completed>,
    pub report: TrafficReport,
}

impl Traffic {
    pub async fn new(url: String, ports: [u16; 2]) -> Result<Self> {
        let mut headers = HeaderMap::new();
        let token = std::env::var("GS_AUTH").context("GS_AUTH is missing")?;
        let mut authorization = HeaderValue::from_str(&format!("Bearer {token}"))?;
        authorization.set_sensitive(true);
        headers.insert(AUTHORIZATION, authorization);
        let client = Client::builder()
            .default_headers(headers)
            .no_proxy()
            .timeout(Duration::from_secs(18))
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()?;
        let node = Client::builder().timeout(Duration::from_secs(5)).build()?;
        let rpc = std::env::var("IOTA_RPC_URL").unwrap_or_else(|_| crate::DEFAULT_RPC.into());
        let price = crate::node_read(&node, &rpc, "iotax_getReferenceGasPrice", json!([]))
            .await
            .map_err(|_| anyhow::anyhow!("Cannot read Foundation Testnet gas price"))?;
        let gas_price = price.as_str().context("Invalid gas price")?.parse()?;
        Ok(Self {
            client,
            node,
            url,
            rpc,
            ports,
            gas_price,
            reservations: HashSet::new(),
            last: None,
            report: TrafficReport::default(),
        })
    }

    async fn post(&self, path: &str, payload: &Value) -> Result<(StatusCode, Value, usize)> {
        let response = self
            .client
            .post(format!("{}{path}", self.url))
            .json(payload)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("Proxy request failed: {path}"))?;
        let upstream = response
            .headers()
            .get("x-router-upstream")
            .context("Nginx did not identify the upstream")?
            .to_str()
            .context("Invalid upstream header")?;
        let router = router_index(upstream, self.ports)?;
        let status = response.status();
        let body = response
            .json::<Value>()
            .await
            .context("Non-JSON router response")?;
        Ok((status, body, router))
    }

    async fn reserve(&mut self, budget: u64) -> Result<ReserveGasResult> {
        let (status, body, router) = self
            .post(
                "/v1/reserve_gas",
                &json!({"gas_budget":budget,"reserve_duration_secs":20}),
            )
            .await?;
        ensure!(
            status.is_success() && body.get("error").is_some_and(Value::is_null),
            "Reservation failed: HTTP {status}, router {}",
            router + 1
        );
        let reservation: ReserveGasResult =
            serde_json::from_value(body["result"].clone()).context("Invalid reservation")?;
        ensure!(
            !reservation.gas_coins.is_empty(),
            "Reservation has no gas coins"
        );
        ensure!(
            self.reservations.insert(reservation.reservation_id),
            "Reservation ID was reused"
        );
        self.report.reservations_by_router[router] += 1;
        self.report.sponsors.insert(reservation.sponsor_address);
        Ok(reservation)
    }

    async fn execute(&mut self, payload: &Value, digest: &str) -> Result<Value> {
        let (status, body, router) = self.post("/v1/execute_tx", payload).await?;
        ensure!(
            status.is_success() && body["error"].is_null(),
            "Execution failed: HTTP {status}, router {}",
            router + 1
        );
        ensure!(
            body["effects"]["transactionDigest"] == digest
                && body["effects"]["status"]["status"] == "success",
            "Execution effects are missing, mismatched or failed"
        );
        self.report.executions_by_router[router] += 1;
        Ok(body["effects"].clone())
    }

    pub async fn transaction(&mut self) -> Result<()> {
        let budget = rand::thread_rng().gen_range(5_000_000..=10_000_000u64);
        let reservation = self.reserve(budget).await?;
        let id = reservation.reservation_id;
        let (transaction, key) = crate::random_transaction(&reservation, self.gas_price, budget)?;
        let digest = transaction.digest().to_string();
        let payload = json!({"reservation_id":id, "tx_bytes":transaction.to_base64(),
            "user_sig":key.sign_transaction(&transaction)?.to_base64(), "request_type":"waitForEffectsCert"});
        let effects = self.execute(&payload, &digest).await?;
        self.verify_finality(&digest).await?;
        let cached = self.execute(&payload, &digest).await?;
        ensure!(
            effects == cached,
            "A duplicate request returned different effects"
        );
        self.report.transactions += 1;
        self.report.cache_reads += 1;
        self.last = Some(Completed {
            payload,
            digest: digest.clone(),
            effects,
        });
        println!(
            "  tx {} finalized; reservation={id}, digest={digest}",
            self.report.transactions
        );
        Ok(())
    }

    async fn verify_finality(&self, digest: &str) -> Result<()> {
        timeout(Duration::from_secs(15), async {
            loop {
                if let Ok(result) = crate::node_read(
                    &self.node,
                    &self.rpc,
                    "iota_getTransactionBlock",
                    json!([digest,{"showEffects":true}]),
                )
                .await
                {
                    ensure!(
                        result["digest"] == digest
                            && result["effects"]["status"]["status"] == "success",
                        "On-chain effects mismatch"
                    );
                    return Ok::<_, anyhow::Error>(());
                }
                sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .context("Cannot independently confirm transaction finality")??;
        Ok(())
    }

    pub async fn expect_redis_error(&mut self) -> Result<()> {
        let payload = &self
            .last
            .as_ref()
            .context("No cached transaction for outage probe")?
            .payload;
        let (status, body, router) = self.post("/v1/execute_tx", payload).await?;
        ensure!(
            matches!(
                status,
                StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
            ) && body["effects"].is_null()
                && !body["error"].is_null(),
            "Redis outage returned a false success or unexpected status: {status}"
        );
        self.report.redis_errors_by_router[router] += 1;
        println!(
            "  expected Redis error: router {}, HTTP {status}",
            router + 1
        );
        Ok(())
    }

    pub async fn verify_cached_recovery(&mut self) -> Result<()> {
        let completed = self
            .last
            .as_ref()
            .context("Missing pre-outage transaction")?;
        let payload = completed.payload.clone();
        let digest = completed.digest.clone();
        let expected = completed.effects.clone();
        timeout(Duration::from_secs(15), async {
            let mut recovered = [false; 2];
            loop {
                let (status, body, router) = self.post("/v1/execute_tx", &payload).await?;
                if status.is_success() {
                    ensure!(
                        body["effects"] == expected
                            && body["effects"]["transactionDigest"] == digest,
                        "Redis restart lost or changed cached effects"
                    );
                    self.report.executions_by_router[router] += 1;
                    self.report.cache_reads += 1;
                    recovered[router] = true;
                    if recovered == [true; 2] {
                        return Ok::<_, anyhow::Error>(());
                    }
                    continue;
                }
                ensure!(
                    matches!(
                        status,
                        StatusCode::SERVICE_UNAVAILABLE | StatusCode::GATEWAY_TIMEOUT
                    ),
                    "Cache recovery failed: {status}"
                );
                sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .context("Routers did not recover their cached execution")??;
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.report.transactions >= 12 && self.report.sponsors.len() >= 3,
            "Insufficient finalized transactions or sponsor coverage"
        );
        for index in 0..2 {
            ensure!(
                self.report.reservations_by_router[index] > 0
                    && self.report.executions_by_router[index] > 0,
                "Router {} did not handle reservations and execution",
                index + 1
            );
            ensure!(
                self.report.redis_errors_by_router[index] > 0,
                "Router {} was not tested during Redis outage",
                index + 1
            );
        }
        Ok(())
    }
}

fn router_index(upstream: &str, ports: [u16; 2]) -> Result<usize> {
    ports
        .iter()
        .position(|port| upstream == format!("127.0.0.1:{port}"))
        .context("Nginx returned an unknown upstream or retried a request")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn proxy_replays_cannot_be_counted_as_balanced_traffic() {
        assert_eq!(router_index("127.0.0.1:9262", [9261, 9262]).unwrap(), 1);
        assert!(router_index("127.0.0.1:9261, 127.0.0.1:9262", [9261, 9262]).is_err());
        assert!(router_index("127.0.0.1:9999", [9261, 9262]).is_err());
    }
}
