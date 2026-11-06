use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Deserializer, de};

pub const EXAMPLE_CONFIG: &str = include_str!("../config.example.yaml");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub rpc: RpcConfig,
    pub metrics: HttpConfig,
    pub iota: IotaConfig,
    pub storage: StorageConfig,
    pub routing: RoutingConfig,
    pub backends: Vec<BackendConfig>,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let yaml = fs::read_to_string(path)
            .with_context(|| format!("failed to read config file {}", path.display()))?;
        let config: Self = serde_yaml_ng::from_str(&yaml)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;
        ensure!(
            config.storage.kind != StorageKind::Redis || config.storage.redis.is_some(),
            "storage.redis is required when storage.kind is redis"
        );
        Ok(config)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcConfig {
    pub host: IpAddr,
    pub port: u16,
    #[serde(default, deserialize_with = "env_string")]
    pub token: String,
}

impl RpcConfig {
    pub fn address(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    pub host: IpAddr,
    pub port: u16,
}

impl HttpConfig {
    pub fn address(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IotaConfig {
    #[serde(deserialize_with = "env_string")]
    pub rpc_url: String,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub kind: StorageKind,
    pub redis: Option<RedisConfig>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    Memory,
    Redis,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RedisConfig {
    #[serde(deserialize_with = "env_string")]
    pub url: String,
    pub key_prefix: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    pub algorithm: RoutingAlgorithm,
    pub reserve_gas: ReserveGasConfig,
    pub execute_tx: ExecuteTxConfig,
    pub ttl: TtlConfig,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RoutingAlgorithm {
    RoundRobin,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveGasConfig {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub retries: RetryConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetryConfig {
    pub max_retries: u32,
    #[serde(with = "humantime_serde")]
    pub backoff: Duration,
    pub retry_on: Vec<RetryOn>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RetryOn {
    ConnectError,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteTxConfig {
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub inflight: InflightConfig,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub struct InflightConfig {
    #[serde(with = "humantime_serde")]
    pub lease_ttl: Duration,
    #[serde(with = "humantime_serde")]
    pub record_ttl: Duration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TtlConfig {
    #[serde(with = "humantime_serde")]
    pub reservation: Duration,
    #[serde(with = "humantime_serde")]
    pub transaction_effects: Duration,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BackendConfig {
    pub name: String,
    #[serde(default = "enabled_by_default")]
    pub enabled: bool,
    #[serde(default, deserialize_with = "env_string")]
    pub token: String,
    #[serde(deserialize_with = "env_string")]
    pub rpc: String,
}

fn enabled_by_default() -> bool {
    true
}

fn env_string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    let value = String::deserialize(deserializer)?;
    resolve_env(&value, |name| std::env::var(name).ok()).map_err(de::Error::custom)
}

// Resolve whole-value references after YAML parsing, so environment values stay
// literal strings even when they contain quotes, colons or newlines.
fn resolve_env(value: &str, lookup: impl FnOnce(&str) -> Option<String>) -> Result<String> {
    let Some(reference) = value.strip_prefix('$') else {
        return Ok(value.to_owned());
    };
    let name = if reference.starts_with('{') {
        reference
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
    } else {
        Some(reference)
    };
    let valid_name = name.filter(|name| {
        let mut chars = name.chars();
        chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    });
    let name = valid_name.context("invalid environment reference; use $NAME or ${NAME}")?;
    lookup(name)
        .with_context(|| format!("environment variable {name} is not set or is not Unicode"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_values_remain_literal_and_are_not_expanded_recursively() {
        let secret = "quotes: \"value\"\n${OTHER}";
        for reference in ["$TOKEN", "${TOKEN}"] {
            assert_eq!(
                resolve_env(reference, |name| (name == "TOKEN")
                    .then(|| secret.to_owned()))
                .unwrap(),
                secret
            );
        }
        assert_eq!(resolve_env("", |_| panic!("literal value")).unwrap(), "");
        assert_eq!(
            resolve_env("redis://localhost:6379", |_| panic!("literal value")).unwrap(),
            "redis://localhost:6379"
        );
    }

    #[test]
    fn missing_or_malformed_environment_references_fail() {
        assert!(
            resolve_env("${MISSING}", |_| None)
                .unwrap_err()
                .to_string()
                .contains("MISSING")
        );
        for value in ["$", "${}", "${TOKEN", "$TOKEN/suffix"] {
            assert!(resolve_env(value, |_| Some("value".into())).is_err());
        }
    }

    #[test]
    fn invalid_duration_and_unknown_options_are_rejected() {
        let yaml = EXAMPLE_CONFIG
            .replace("${GS_AUTH}", "literal-rpc-token")
            .replace("${IOTA_RPC_URL}", "http://localhost:9000")
            .replace("${REDIS_URL}", "redis://localhost:6379")
            .replace("${GAS_STATION_B_TOKEN}", "literal-backend-token");
        let config: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(
            config.routing.reserve_gas.retries.backoff,
            Duration::from_millis(200)
        );
        assert_eq!(config.routing.ttl.reservation, Duration::from_secs(15 * 60));
        assert!(
            serde_yaml_ng::from_str::<Config>(&yaml.replace("timeout: 3s", "timeout: invalid"))
                .is_err()
        );
        assert!(
            serde_yaml_ng::from_str::<Config>(&yaml.replace("max_retries:", "max_retry:")).is_err()
        );
    }
}
