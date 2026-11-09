use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, bail, ensure};
use serde_yaml_ng::Value;
use tokio::{
    process::{Child, Command},
    time::{sleep, timeout},
};

pub(super) const ROOT: &str = env!("CARGO_MANIFEST_DIR");

#[derive(Clone)]
pub(super) struct Settings {
    pub rpc: [u16; 2],
    pub metrics: [u16; 2],
    pub directory: PathBuf,
    pub redis_url: String,
    pub prefix: Option<String>,
}

pub(super) struct Cluster {
    children: Vec<Child>,
    pub settings: Settings,
}

pub(super) async fn build() -> Result<()> {
    let status = Command::new("cargo")
        .current_dir(ROOT)
        .args(["build", "--locked", "--bin", "gas-station-router-v2"])
        .status()
        .await?;
    ensure!(status.success(), "Router build failed");
    Ok(())
}

pub(super) fn binary() -> PathBuf {
    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(ROOT).join("target"));
    let target = if target.is_absolute() {
        target
    } else {
        PathBuf::from(ROOT).join(target)
    };
    target.join("debug/gas-station-router-v2")
}

pub(super) fn unused_ports() -> Result<[u16; 5]> {
    let listeners = (0..5)
        .map(|_| std::net::TcpListener::bind("127.0.0.1:0"))
        .collect::<std::io::Result<Vec<_>>>()?;
    let ports = listeners
        .iter()
        .map(|listener| listener.local_addr().map(|address| address.port()))
        .collect::<std::io::Result<Vec<_>>>()?;
    Ok(ports.try_into().expect("five listeners"))
}

fn write_config(settings: &Settings, index: usize) -> Result<PathBuf> {
    let source = Path::new(ROOT).join("docker/router/router.config.yaml");
    let mut yaml: Value = serde_yaml_ng::from_str(&fs::read_to_string(source)?)?;
    yaml["rpc"]["port"] = settings.rpc[index].into();
    yaml["metrics"]["port"] = settings.metrics[index].into();
    if let Some(prefix) = &settings.prefix {
        yaml["storage"]["redis"]["key_prefix"] = prefix.clone().into();
        yaml["routing"]["reserve_gas"]["timeout"] = "3s".into();
        yaml["routing"]["execute_tx"]["timeout"] = "12s".into();
        yaml["routing"]["execute_tx"]["inflight"]["lease_ttl"] = "15s".into();
        yaml["routing"]["execute_tx"]["inflight"]["record_ttl"] = "40s".into();
        yaml["routing"]["ttl"]["reservation"] = "20s".into();
        yaml["routing"]["ttl"]["transaction_effects"] = "30s".into();
        yaml["health_checks"]["interval"] = "1s".into();
        yaml["health_checks"]["timeout"] = "500ms".into();
        yaml["backends"]
            .as_sequence_mut()
            .context("Missing backends")?
            .truncate(3);
    }
    let path = settings
        .directory
        .join(format!("router-{}.yaml", index + 1));
    fs::write(&path, serde_yaml_ng::to_string(&yaml)?)?;
    Ok(path)
}

impl Cluster {
    pub async fn start(settings: Settings) -> Result<Self> {
        fs::create_dir_all(&settings.directory)?;
        let mut cluster = Self {
            children: Vec::new(),
            settings,
        };
        for index in 0..2 {
            let config = write_config(&cluster.settings, index)?;
            let log = fs::File::create(
                cluster
                    .settings
                    .directory
                    .join(format!("router-{}.log", index + 1)),
            )?;
            let child = Command::new(binary())
                .current_dir(ROOT)
                .args(["start", "--config-file"])
                .arg(config)
                .env("ROUTER_REDIS_URL", &cluster.settings.redis_url)
                .env("LOG_FORMAT", "json")
                .env("RUST_LOG", "info")
                .stdout(Stdio::null())
                .stderr(Stdio::from(log))
                .kill_on_drop(true)
                .spawn()?;
            cluster.children.push(child);
        }
        cluster.wait_ready().await?;
        Ok(cluster)
    }

    pub fn check_alive(&mut self) -> Result<()> {
        for (index, child) in self.children.iter_mut().enumerate() {
            if let Some(status) = child.try_wait()? {
                bail!(
                    "Router {} exited ({status}); see {}",
                    index + 1,
                    self.settings.directory.display()
                );
            }
        }
        Ok(())
    }

    async fn wait_ready(&mut self) -> Result<()> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(1))
            .build()?;
        timeout(Duration::from_secs(15), async {
            loop {
                self.check_alive()?;
                let mut ready = 0;
                for port in self.settings.rpc {
                    if let Ok(reply) = client
                        .get(format!("http://127.0.0.1:{port}/health"))
                        .send()
                        .await
                        && reply.status().is_success()
                    {
                        ready += 1;
                    }
                }
                if ready == 2 {
                    return Ok::<_, anyhow::Error>(());
                }
                sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .context("Routers did not become ready")??;
        Ok(())
    }

    pub async fn stop(&mut self) -> Result<()> {
        for child in &mut self.children {
            if child.try_wait()?.is_none()
                && let Some(id) = child.id()
            {
                let _ = Command::new("kill")
                    .args(["-TERM", &id.to_string()])
                    .output()
                    .await;
            }
        }
        for child in &mut self.children {
            match timeout(Duration::from_secs(5), child.wait()).await {
                Ok(result) => {
                    result?;
                }
                Err(_) => child.kill().await?,
            }
        }
        Ok(())
    }
}

pub(super) async fn run() -> Result<()> {
    let port = |name, default| {
        std::env::var(name)
            .ok()
            .map(|value| value.parse())
            .transpose()
            .map(|value| value.unwrap_or(default))
    };
    let settings = Settings {
        rpc: [
            port("ROUTER_1_RPC_PORT", 9261)?,
            port("ROUTER_2_RPC_PORT", 9262)?,
        ],
        metrics: [
            port("ROUTER_1_METRICS_PORT", 9185)?,
            port("ROUTER_2_METRICS_PORT", 9186)?,
        ],
        directory: Path::new(ROOT).join("docker/.runtime/cluster"),
        redis_url: std::env::var("ROUTER_REDIS_URL").unwrap_or_else(|_| {
            format!(
                "redis://127.0.0.1:{}/0",
                std::env::var("ROUTER_REDIS_PORT").unwrap_or_else(|_| "6380".into())
            )
        }),
        prefix: None,
    };
    let mut cluster = Cluster::start(settings).await?;
    let setup =
        crate::setup::TestSetup::for_cluster(&cluster.settings, port("ROUTER_PROXY_PORT", 9257)?)?;
    println!("{setup}");
    setup.save(&cluster.settings.directory)?;
    println!(
        "Two routers ready; proxy: http://127.0.0.1:{}; logs: {}",
        std::env::var("ROUTER_PROXY_PORT").unwrap_or_else(|_| "9257".into()),
        cluster.settings.directory.display()
    );
    let result = tokio::select! {
        signal = shutdown_signal() => signal,
        result = async { loop { cluster.check_alive()?; sleep(Duration::from_millis(500)).await; } } => result,
    };
    cluster.stop().await?;
    result
}

pub(super) async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            signal = tokio::signal::ctrl_c() => signal?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    Ok(())
}
