use anyhow::{Context, Result, ensure};
use reqwest::Client;
use serde::Serialize;

#[derive(Serialize)]
pub(super) struct RouterMetrics {
    pub reservations: u64,
    pub executions: u64,
    pub dispatches: u64,
    pub storage_errors: u64,
    pub unhealthy: u64,
    pub recovered: u64,
    pub healthy_backends: usize,
}

pub(super) async fn sample(client: &Client, ports: [u16; 2]) -> Result<[RouterMetrics; 2]> {
    let mut results = Vec::new();
    for port in ports {
        let text = client
            .get(format!("http://127.0.0.1:{port}/metrics"))
            .send()
            .await?
            .error_for_status()?
            .text()
            .await?;
        let value = |name: &str, labels: &[(&str, &str)]| counter(&text, name, labels);
        results.push(RouterMetrics {
            reservations: value("reserve_gas_successes_total", &[])?,
            executions: value("execute_tx_successes_total", &[])?,
            dispatches: value(
                "storage_execution_phase_transitions_total",
                &[("kind", "dispatching")],
            )?,
            storage_errors: value("storage_errors_total", &[("kind", "backend")])?,
            unhealthy: value(
                "backend_health_transitions_total",
                &[
                    ("backend", "gas_station_1"),
                    ("previous_health", "healthy"),
                    ("health", "unhealthy"),
                ],
            )?,
            recovered: value(
                "backend_health_transitions_total",
                &[
                    ("backend", "gas_station_1"),
                    ("previous_health", "unhealthy"),
                    ("health", "healthy"),
                ],
            )?,
            healthy_backends: (1..=3)
                .map(|index| backend_is_healthy(&text, &format!("gas_station_{index}")))
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .filter(|healthy| *healthy)
                .count(),
        });
    }
    results
        .try_into()
        .map_err(|_| anyhow::anyhow!("Expected two metric endpoints"))
}

fn backend_is_healthy(text: &str, backend: &str) -> Result<bool> {
    // Backends start healthy. Every exit must have a matching return before
    // traffic resumes; an old recovery counter alone does not prove readiness.
    let mut departures = 0;
    let mut returns = 0;
    for state in ["degraded", "unhealthy"] {
        departures += counter(
            text,
            "backend_health_transitions_total",
            &[
                ("backend", backend),
                ("previous_health", "healthy"),
                ("health", state),
            ],
        )?;
        returns += counter(
            text,
            "backend_health_transitions_total",
            &[
                ("backend", backend),
                ("previous_health", state),
                ("health", "healthy"),
            ],
        )?;
    }
    Ok(departures == returns)
}

pub(super) fn validate(samples: &[RouterMetrics; 2], transactions: usize) -> Result<()> {
    for (index, router) in samples.iter().enumerate() {
        ensure!(
            router.reservations > 0 && router.executions > 0,
            "Router {} did not serve real traffic",
            index + 1
        );
        ensure!(
            router.unhealthy > 0 && router.recovered > 0,
            "Router {} did not observe Gas Station failure and recovery",
            index + 1
        );
        ensure!(
            router.storage_errors > 0,
            "Router {} did not observe Redis unavailability",
            index + 1
        );
    }
    ensure!(
        samples.iter().map(|router| router.dispatches).sum::<u64>() == transactions as u64,
        "Retries or cache reads caused extra transaction dispatches"
    );
    Ok(())
}

fn counter(text: &str, name: &str, labels: &[(&str, &str)]) -> Result<u64> {
    let name = format!("gas_station_router_{name}");
    text.lines()
        .find_map(|line| {
            let (series, value) = line.split_once(' ')?;
            let family = series.split('{').next()?;
            let fields = series
                .split_once('{')
                .map(|(_, fields)| fields.trim_end_matches('}'))
                .unwrap_or("");
            (family == name
                && labels.iter().all(|(name, value)| {
                    fields
                        .split(',')
                        .any(|field| field == format!("{name}=\"{value}\""))
                }))
            .then_some(value)
        })
        .with_context(|| format!("Missing metric {name}"))?
        .parse()
        .context("Non-integer counter")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn health_labels_do_not_confuse_previous_and_current_states() {
        let text = "gas_station_router_health{backend=\"a\",health=\"healthy\",previous_health=\"unhealthy\"} 7\n\
                    gas_station_router_health{backend=\"a\",health=\"unhealthy\",previous_health=\"healthy\"} 2";
        assert_eq!(
            counter(
                text,
                "health",
                &[("health", "unhealthy"), ("previous_health", "healthy")]
            )
            .unwrap(),
            2
        );
    }

    #[test]
    fn an_old_recovery_does_not_hide_another_failure() {
        let text = "gas_station_router_backend_health_transitions_total{backend=\"a\",previous_health=\"healthy\",health=\"unhealthy\"} 2\n\
                    gas_station_router_backend_health_transitions_total{backend=\"a\",previous_health=\"unhealthy\",health=\"healthy\"} 1\n\
                    gas_station_router_backend_health_transitions_total{backend=\"a\",previous_health=\"healthy\",health=\"degraded\"} 0\n\
                    gas_station_router_backend_health_transitions_total{backend=\"a\",previous_health=\"degraded\",health=\"healthy\"} 0";
        assert!(!backend_is_healthy(text, "a").unwrap());
        assert!(backend_is_healthy(&text.replace("} 2", "} 1"), "a").unwrap());
    }
}
