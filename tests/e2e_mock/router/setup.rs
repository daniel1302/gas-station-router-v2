use std::{fs, path::Path};

use anyhow::{Context, Result};
use serde_json::{Value, json};

use crate::common::{self, Process, Station};

pub(super) async fn stations(directory: &Path) -> Result<(Vec<Station>, Vec<Process>)> {
    let mut stations = Vec::new();
    let mut processes = Vec::new();
    for index in 1..=3 {
        let station = Station::new(directory, &format!("station-{index}"), index)?;
        processes.push(station.start()?);
        common::wait_health(&station.url).await?;
        stations.push(station);
    }
    Ok((stations, processes))
}

pub(super) fn routers(
    directory: &Path,
    backends: &[Station],
    redis: &str,
    rpc: [u16; 2],
    metrics: [u16; 2],
) -> Result<Vec<Process>> {
    let mut processes = Vec::new();
    for index in 0..2 {
        let config = config(backends, redis, rpc[index], metrics[index]);
        let path = directory.join(format!("router-{}.yaml", index + 1));
        fs::write(&path, serde_yaml_ng::to_string(&config)?)?;
        processes.push(Process::start(
            "gas-station-router-v2",
            &[
                "start",
                "--config-file",
                path.to_str().context("Router config path")?,
            ],
            &directory.join(format!("router-{}.log", index + 1)),
        )?);
    }
    Ok(processes)
}

pub(super) fn summary(
    directory: &Path,
    backends: &[Station],
    proxy: &str,
    redis: &str,
    rpc: [u16; 2],
    metrics: [u16; 2],
) -> Result<()> {
    let setup = json!({
        "load_balancer": proxy,
        "routers": rpc.map(|port| format!("http://127.0.0.1:{port}")),
        "metrics": metrics.map(|port| format!("http://127.0.0.1:{port}/metrics")),
        "gas_stations": backends.iter().map(|station| json!({
            "url": station.url, "sponsor": station.sponsor, "metrics": station.metrics_url
        })).collect::<Vec<_>>(),
        "authorization": format!("Bearer {}", common::TOKEN),
        "redis": redis,
    });
    println!("[setup] {}", serde_json::to_string_pretty(&setup)?);
    fs::write(
        directory.join("setup.json"),
        serde_json::to_vec_pretty(&setup)?,
    )?;
    Ok(())
}

fn config(backends: &[Station], redis: &str, rpc: u16, metrics: u16) -> Value {
    json!({
        "rpc": {"host": "127.0.0.1", "port": rpc, "token": common::TOKEN},
        "metrics": {"host": "127.0.0.1", "port": metrics},
        "iota": {"rpc_url": format!("{}/rpc", backends[0].url), "timeout": "300ms"},
        "storage": {"kind": "redis", "redis": {"url": redis, "key_prefix": "mock-e2e"}},
        "routing": {
            "algorithm": "round_robin",
            "reserve_gas": {"timeout": "1s", "retries": {"max_retries": 2, "backoff": "10ms"}},
            "execute_tx": {
                "timeout": "2s",
                "inflight": {"lease_ttl": "3s", "record_ttl": "40s"}
            },
            "ttl": {"reservation": "20s", "transaction_effects": "30s"}
        },
        "health_checks": {
            "enabled": true, "interval": "150ms", "timeout": "150ms",
            "unhealthy_threshold": 3, "healthy_threshold": 2,
            "passive": {
                "enabled": true, "failure_threshold": 3, "initial_cooldown": "1s",
                "backoff": 2, "max_cooldown": "3s"
            },
            "deep_check": {"enabled": false, "interval": "60s", "timeout": "1s"}
        },
        "backends": backends.iter().enumerate().map(|(index, station)| json!({
            "name": format!("gas_station_{}", index + 1),
            "rpc": station.url, "token": common::TOKEN
        })).collect::<Vec<_>>()
    })
}
