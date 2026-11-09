use std::{
    fmt, fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::Path,
};

use anyhow::{Context, Result};
use gas_station_router_v2::config::{BackendConfig, HttpConfig, IotaConfig, RpcConfig};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::cluster::{ROOT, Settings};

#[derive(Serialize)]
pub(super) struct TestSetup {
    target_url: String,
    load_balancer: Endpoint,
    routers: Vec<RouterSetup>,
    gas_stations: Vec<GasStationSetup>,
    iota_rpc_url: String,
}

#[derive(Serialize)]
struct Endpoint {
    name: String,
    rpc_url: String,
    authorization: Option<String>,
}

#[derive(Serialize)]
struct RouterSetup {
    #[serde(flatten)]
    endpoint: Endpoint,
    metrics_url: String,
    status_url: String,
}

#[derive(Serialize)]
struct GasStationSetup {
    #[serde(flatten)]
    endpoint: Endpoint,
    enabled: bool,
}

// Project only the fields needed for diagnostics. Never serialize a whole
// config or environment: sponsor private keys do not belong in setup output.
#[derive(Deserialize)]
struct RouterConfig {
    rpc: RpcConfig,
    metrics: HttpConfig,
    iota: IotaConfig,
    backends: Vec<BackendConfig>,
}

#[derive(Deserialize)]
struct BackendsConfig {
    backends: Vec<BackendConfig>,
}

impl TestSetup {
    pub fn for_cluster(settings: &Settings, proxy_port: u16) -> Result<Self> {
        let paths = [
            settings.directory.join("router-1.yaml"),
            settings.directory.join("router-2.yaml"),
        ];
        Self::from_configs(
            [&paths[0], &paths[1]],
            &format!("http://127.0.0.1:{proxy_port}"),
        )
    }

    fn from_configs(paths: [&Path; 2], target_url: &str) -> Result<Self> {
        let mut configs = paths
            .into_iter()
            .map(read_config::<RouterConfig>)
            .collect::<Result<Vec<_>>>()?;
        let mut routers = Vec::new();
        for (index, config) in configs.iter().enumerate() {
            let rpc_url = http_url(config.rpc.host, config.rpc.port);
            routers.push(RouterSetup {
                status_url: format!("{rpc_url}/status"),
                metrics_url: format!(
                    "{}/metrics",
                    http_url(config.metrics.host, config.metrics.port)
                ),
                endpoint: Endpoint {
                    name: format!("router-{}", index + 1),
                    rpc_url,
                    authorization: authorization(&config.rpc.token),
                },
            });
        }
        let first = configs.remove(0);
        Ok(Self {
            target_url: target_url.into(),
            load_balancer: Endpoint {
                name: "nginx (round robin)".into(),
                rpc_url: target_url.into(),
                authorization: authorization(&first.rpc.token),
            },
            routers,
            gas_stations: stations(first.backends),
            iota_rpc_url: first.iota.rpc_url,
        })
    }

    pub fn save(&self, directory: &Path) -> Result<()> {
        fs::write(
            directory.join("setup.json"),
            serde_json::to_string_pretty(self)?,
        )?;
        Ok(())
    }
}

impl fmt::Display for TestSetup {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(out, "\n[setup] Test topology")?;
        writeln!(out, "  Traffic target: {}", self.target_url)?;
        write_endpoint(out, "Load balancer", &self.load_balancer)?;
        for router in &self.routers {
            write_endpoint(out, "Router", &router.endpoint)?;
            writeln!(out, "    Metrics: {}", router.metrics_url)?;
            writeln!(out, "    Status: {} (no auth)", router.status_url)?;
        }
        writeln!(out, "  Configured Gas Stations:")?;
        for station in &self.gas_stations {
            write_endpoint(out, "Gas Station", &station.endpoint)?;
            writeln!(out, "    Enabled: {}", station.enabled)?;
        }
        writeln!(out, "  IOTA RPC: {}", self.iota_rpc_url)?;
        writeln!(
            out,
            "  Router auth applies to POST requests; /health and /status need no auth."
        )
    }
}

fn write_endpoint(out: &mut impl fmt::Write, kind: &str, endpoint: &Endpoint) -> fmt::Result {
    writeln!(out, "  {kind} {}: {}", endpoint.name, endpoint.rpc_url)?;
    writeln!(
        out,
        "    Authorization: {}",
        endpoint.authorization.as_deref().unwrap_or("none")
    )
}

fn authorization(token: &str) -> Option<String> {
    (!token.is_empty()).then(|| format!("Bearer {token}"))
}

fn stations(backends: Vec<BackendConfig>) -> Vec<GasStationSetup> {
    backends
        .into_iter()
        .map(|backend| GasStationSetup {
            enabled: backend.enabled,
            endpoint: Endpoint {
                name: backend.name,
                rpc_url: backend.rpc,
                authorization: authorization(&backend.token),
            },
        })
        .collect()
}

fn read_config<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let yaml = fs::read_to_string(path)
        .with_context(|| format!("Cannot read setup config {}", path.display()))?;
    // Parsing errors can echo input lines, including private material from a bad file.
    serde_yaml_ng::from_str(&yaml)
        .map_err(|_| anyhow::anyhow!("Cannot parse setup config {}", path.display()))
}

fn http_url(host: IpAddr, port: u16) -> String {
    let host = if host.is_unspecified() {
        if host.is_ipv4() {
            Ipv4Addr::LOCALHOST.into()
        } else {
            Ipv6Addr::LOCALHOST.into()
        }
    } else {
        host
    };
    format!("http://{}", SocketAddr::new(host, port))
}

pub(super) fn print_incremental(target: &str, iota_rpc: &str, token: &str) {
    println!("\n[setup] Test target (router or load balancer): {target}");
    println!(
        "  Authorization: {}",
        authorization(token).as_deref().unwrap_or("none")
    );
    println!("  Status: {target}/status (no auth)");
    println!("  IOTA RPC: {iota_rpc}");
    let path = Path::new(ROOT).join("docker/router/router.config.yaml");
    match read_config::<BackendsConfig>(&path) {
        Ok(config) => {
            println!("  Gas Stations configured in {}:", path.display());
            for station in stations(config.backends) {
                println!(
                    "    {}: {} (enabled={})",
                    station.endpoint.name, station.endpoint.rpc_url, station.enabled
                );
                println!(
                    "      Authorization: {}",
                    station.endpoint.authorization.as_deref().unwrap_or("none")
                );
            }
        }
        Err(_) => println!(
            "  Gas Station configuration is unavailable locally; target may use a different setup."
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn setup_uses_actual_configs_and_test_auth_without_dumping_private_keys() {
        let directory = std::env::temp_dir().join(format!("router-setup-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&directory).unwrap();
        let paths = [directory.join("one.yaml"), directory.join("two.yaml")];
        for (index, path) in paths.iter().enumerate() {
            fs::write(path, serde_json::to_string(&json!({
                "rpc":{"host":if index == 0 {"0.0.0.0"} else {"::"}, "port":14001 + index,"token":"router-test-token"},
                "metrics":{"host":"127.0.0.1","port":15001 + index},
                "iota":{"rpc_url":"https://api.testnet.iota.cafe","timeout":"1s"},
                "backends":[
                    {"name":"gs-a","rpc":"http://127.0.0.1:16001","token":"station-test-token"},
                    {"name":"gs-b","rpc":"http://127.0.0.1:16002","enabled":false}
                ],
                "private_key":"PRIVATE_KEY_MUST_NOT_APPEAR",
                "storage":{"redis":{"url":"redis://:REDIS_PASSWORD_MUST_NOT_APPEAR@localhost:6379"}}
            })).unwrap()).unwrap();
        }
        let setup =
            TestSetup::from_configs([&paths[0], &paths[1]], "http://127.0.0.1:14000").unwrap();
        let report = serde_json::to_value(&setup).unwrap();
        assert_eq!(report["routers"][0]["rpc_url"], "http://127.0.0.1:14001");
        assert_eq!(report["routers"][1]["rpc_url"], "http://[::1]:14002");
        assert_eq!(
            report["routers"][1]["metrics_url"],
            "http://127.0.0.1:15002/metrics"
        );
        assert_eq!(
            report["load_balancer"]["authorization"],
            "Bearer router-test-token"
        );
        assert_eq!(
            report["gas_stations"][0]["authorization"],
            "Bearer station-test-token"
        );
        assert!(report["gas_stations"][1]["authorization"].is_null());
        assert_eq!(report["gas_stations"][1]["enabled"], false);
        setup.save(&directory).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                &fs::read_to_string(directory.join("setup.json")).unwrap()
            )
            .unwrap(),
            report
        );
        for output in [setup.to_string(), report.to_string()] {
            assert!(output.contains("router-test-token") && output.contains("station-test-token"));
            assert!(output.contains("http://127.0.0.1:16001"));
            assert!(
                !output.contains("PRIVATE_KEY_MUST_NOT_APPEAR")
                    && !output.contains("REDIS_PASSWORD_MUST_NOT_APPEAR")
            );
        }
        fs::remove_dir_all(directory).unwrap();
    }
}
