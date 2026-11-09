use std::{
    fs,
    net::{IpAddr, SocketAddr},
    path::Path,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Deserializer, de};

pub const EXAMPLE_CONFIG: &str = include_str!("../../config.example.yaml");

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub rpc: RpcConfig,
    pub metrics: HttpConfig,
    pub iota: IotaConfig,
    pub storage: StorageConfig,
    pub routing: RoutingConfig,
    #[serde(default)]
    pub health_checks: HealthChecksConfig,
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

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HealthChecksConfig {
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub unhealthy_threshold: u32,
    pub healthy_threshold: u32,
    pub passive: PassiveHealthCheckConfig,
    pub deep_check: DeepHealthCheckConfig,
}

impl Default for HealthChecksConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
            timeout: Duration::from_millis(1500),
            unhealthy_threshold: 3,
            healthy_threshold: 2,
            passive: PassiveHealthCheckConfig::default(),
            deep_check: DeepHealthCheckConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct PassiveHealthCheckConfig {
    pub enabled: bool,
    pub failure_threshold: u32,
    #[serde(with = "humantime_serde")]
    pub initial_cooldown: Duration,
    /// Integer multiplier applied after each further insufficient-coins rejection.
    pub backoff: u32,
    #[serde(with = "humantime_serde")]
    pub max_cooldown: Duration,
}

impl Default for PassiveHealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            failure_threshold: 3,
            initial_cooldown: Duration::from_secs(15),
            backoff: 2,
            max_cooldown: Duration::from_secs(60),
        }
    }
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DeepHealthCheckConfig {
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

impl Default for DeepHealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval: Duration::from_secs(60),
            timeout: Duration::from_secs(5),
        }
    }
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

    fn example_yaml() -> String {
        EXAMPLE_CONFIG
            .replace("${GS_AUTH}", "literal-rpc-token")
            .replace("${IOTA_RPC_URL}", "http://localhost:9000")
            .replace("${REDIS_URL}", "redis://localhost:6379")
            .replace("${GAS_STATION_B_TOKEN}", "literal-backend-token")
    }

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
        let yaml = example_yaml();
        let config: Config = serde_yaml_ng::from_str(&yaml).unwrap();
        assert_eq!(config.health_checks, HealthChecksConfig::default());
        assert_eq!(config.routing.reserve_gas.retries.max_retries, 2);
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
        for (field, invalid) in [
            ("timeout: 1500ms", "timeout: invalid"),
            ("initial_cooldown: 15s", "initial_cooldown: invalid"),
            ("max_cooldown: 60s", "max_cooldown: invalid"),
            ("initial_cooldown: 15s", "cooldown: 15s"),
            ("backoff: 2\n", "backoff: invalid\n"),
            ("backoff: 2\n", "backoff: 1.5\n"),
            ("backoff: 2\n", "backoff: -1\n"),
            ("interval: 60s", "interval: invalid"),
            ("unhealthy_threshold:", "unhealthy_threshhold:"),
            ("failure_threshold:", "failure_threshhold:"),
            ("interval: 60s", "unknown_interval: 60s"),
        ] {
            assert!(
                serde_yaml_ng::from_str::<Config>(&yaml.replace(field, invalid)).is_err(),
                "accepted invalid health-check option: {invalid}"
            );
        }
    }

    #[test]
    fn health_checks_accept_overrides_and_default_when_section_is_absent() {
        let yaml = example_yaml();
        let config: Config = serde_yaml_ng::from_str(
            &yaml
                .replace("timeout: 1500ms", "timeout: 250ms")
                .replace("failure_threshold: 3", "failure_threshold: 7")
                .replace("initial_cooldown: 15s", "initial_cooldown: 4s")
                .replace("backoff: 2\n", "backoff: 3\n")
                .replace("max_cooldown: 60s", "max_cooldown: 40s")
                .replace("interval: 60s", "interval: 90s"),
        )
        .unwrap();
        assert_eq!(config.health_checks.timeout, Duration::from_millis(250));
        assert_eq!(config.health_checks.passive.failure_threshold, 7);
        assert_eq!(
            config.health_checks.passive.initial_cooldown,
            Duration::from_secs(4)
        );
        assert_eq!(config.health_checks.passive.backoff, 3);
        assert_eq!(
            config.health_checks.passive.max_cooldown,
            Duration::from_secs(40)
        );
        assert_eq!(
            config.health_checks.deep_check.interval,
            Duration::from_secs(90)
        );

        let mut value: serde_yaml_ng::Value = serde_yaml_ng::from_str(&yaml).unwrap();
        value.as_mapping_mut().unwrap().remove("health_checks");
        let config: Config = serde_yaml_ng::from_value(value).unwrap();
        assert_eq!(config.health_checks, HealthChecksConfig::default());
    }
}
