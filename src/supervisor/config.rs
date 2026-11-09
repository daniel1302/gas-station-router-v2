use std::{
    fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub health: HealthConfig,
    pub pool: PoolConfig,
    pub recovery: RecoveryConfig,
    pub process: ProcessConfig,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthConfig {
    pub enabled: bool,
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub unhealthy_threshold: u32,
    pub readiness_path: Option<String>,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval: Duration::from_secs(5),
            timeout: Duration::from_secs(2),
            unhealthy_threshold: 3,
            readiness_path: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PoolConfig {
    #[serde(with = "humantime_serde")]
    pub metrics_interval: Duration,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    pub rpc_url: Option<String>,
    pub sponsor_address: Option<String>,
    pub expected_chain_id: Option<String>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            metrics_interval: Duration::from_secs(15),
            timeout: Duration::from_secs(2),
            rpc_url: None,
            sponsor_address: None,
            expected_chain_id: None,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecoveryConfig {
    pub auto_refresh: bool,
    pub refresh_url: Option<String>,
    #[serde(with = "humantime_serde")]
    pub refresh_grace_period: Duration,
    pub auto_full_rescan: bool,
    pub require_manual_approval: bool,
    pub consistency: ConsistencyConfig,
    pub aggression: AggressionConfig,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            auto_refresh: true,
            refresh_url: None,
            refresh_grace_period: Duration::from_secs(60),
            auto_full_rescan: false,
            require_manual_approval: true,
            consistency: ConsistencyConfig::default(),
            aggression: AggressionConfig::default(),
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ConsistencyConfig {
    pub balance_divergence_threshold: f64,
    pub coin_count_divergence_threshold: f64,
    pub consecutive_failures: u32,
}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self {
            balance_divergence_threshold: 20.0,
            coin_count_divergence_threshold: 20.0,
            consecutive_failures: 3,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct AggressionConfig {
    pub refresh_attempts_before_rescan: u32,
    pub max_full_rescans: u32,
    /// Zero keeps initialization and maintenance locks mandatory.
    pub ignore_locks_after: u32,
}

impl Default for AggressionConfig {
    fn default() -> Self {
        Self {
            refresh_attempts_before_rescan: 1,
            max_full_rescans: 3,
            ignore_locks_after: 0,
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct ProcessConfig {
    pub restart_on_crash: bool,
    #[serde(with = "humantime_serde")]
    pub restart_backoff: Duration,
    #[serde(with = "humantime_serde")]
    pub startup_grace: Duration,
    #[serde(with = "humantime_serde")]
    pub shutdown_timeout: Duration,
    pub binary: PathBuf,
    pub config: PathBuf,
    pub state_dir: PathBuf,
}

impl Default for ProcessConfig {
    fn default() -> Self {
        Self {
            restart_on_crash: true,
            restart_backoff: Duration::from_secs(10),
            startup_grace: Duration::from_secs(60),
            shutdown_timeout: Duration::from_secs(10),
            binary: PathBuf::new(),
            config: PathBuf::new(),
            state_dir: ".supervisor".into(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct StationConfig {
    pub rpc_host_ip: IpAddr,
    pub rpc_port: u16,
    pub metrics_port: u16,
    pub coin_init_config: Option<CoinInitConfig>,
}

#[derive(Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(super) struct CoinInitConfig {
    #[serde(default = "default_refresh_seconds")]
    pub refresh_interval_sec: u64,
}

fn default_refresh_seconds() -> u64 {
    86400
}

impl StationConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let yaml = fs::read_to_string(path).context("Cannot read Gas Station configuration")?;
        serde_yaml_ng::from_str(&yaml).map_err(|_| {
            anyhow::anyhow!("Invalid Gas Station configuration; check RPC and metrics addresses")
        })
    }

    pub fn url(&self, port: u16) -> String {
        let host = match self.rpc_host_ip {
            IpAddr::V4(host) if host.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V6(host) if host.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
            host => host,
        };
        format!("http://{}", SocketAddr::new(host, port))
    }

    pub fn refresh_interval(&self) -> Duration {
        Duration::from_secs(
            self.coin_init_config
                .as_ref()
                .map_or(default_refresh_seconds(), |config| {
                    config.refresh_interval_sec
                }),
        )
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Document {
            supervisor: Config,
        }
        let yaml = fs::read_to_string(path).context("Cannot read supervisor configuration")?;
        let document: Document = serde_yaml_ng::from_str(&yaml)
            .map_err(|_| anyhow::anyhow!("Invalid supervisor configuration"))?;
        Ok(document.supervisor)
    }

    pub(super) fn validate(&self) -> Result<()> {
        ensure!(
            self.process.binary.is_file(),
            "process.binary must point to a Gas Station binary"
        );
        ensure!(
            self.process.config.is_file(),
            "process.config must point to a Gas Station config"
        );
        for (name, duration) in [
            ("health.interval", self.health.interval),
            ("health.timeout", self.health.timeout),
            ("pool.metrics_interval", self.pool.metrics_interval),
            ("pool.timeout", self.pool.timeout),
            ("process.restart_backoff", self.process.restart_backoff),
            ("process.shutdown_timeout", self.process.shutdown_timeout),
            (
                "recovery.refresh_grace_period",
                self.recovery.refresh_grace_period,
            ),
        ] {
            ensure!(!duration.is_zero(), "{name} must be positive");
        }
        ensure!(
            self.health.unhealthy_threshold > 0
                && self.recovery.consistency.consecutive_failures > 0,
            "Failure thresholds must be positive"
        );
        for value in [
            self.recovery.consistency.balance_divergence_threshold,
            self.recovery.consistency.coin_count_divergence_threshold,
        ] {
            ensure!(
                value.is_finite() && (0.0..=100.0).contains(&value),
                "Divergence thresholds must be between 0 and 100 percent"
            );
        }
        ensure!(
            self.pool.rpc_url.is_some() == self.pool.sponsor_address.is_some(),
            "pool.rpc_url and pool.sponsor_address must be configured together"
        );
        ensure!(
            self.pool.expected_chain_id.is_none() || self.pool.rpc_url.is_some(),
            "pool.expected_chain_id requires pool.rpc_url"
        );
        ensure!(
            !self.process.state_dir.as_os_str().is_empty(),
            "process.state_dir must not be empty"
        );
        if let Some(address) = &self.pool.sponsor_address {
            address
                .parse::<iota_sdk_types::Address>()
                .context("Invalid pool.sponsor_address")?;
        }
        for url in [&self.pool.rpc_url, &self.recovery.refresh_url]
            .into_iter()
            .flatten()
        {
            let url = reqwest::Url::parse(url).context("Invalid supervisor HTTP URL")?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none(),
                "Supervisor URLs must use HTTP(S) without credentials or query parameters"
            );
        }
        if let Some(path) = &self.health.readiness_path {
            ensure!(
                path.starts_with('/') && !path.starts_with("//") && !path.contains('?'),
                "health.readiness_path must be an absolute URL path"
            );
        }
        ensure!(
            self.recovery.aggression.refresh_attempts_before_rescan > 0,
            "Refresh attempts before rescan must be positive"
        );
        Ok(())
    }
}
