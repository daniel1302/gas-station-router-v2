use std::{
    fs,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use tokio::{
    process::Command,
    time::{sleep, timeout},
};

use crate::cluster::ROOT;

pub(super) struct Compose {
    file: PathBuf,
    env_file: PathBuf,
    project: Option<String>,
}

impl Compose {
    pub fn local(env_file: &Path) -> Self {
        Self {
            file: Path::new(ROOT).join("docker/docker-compose.yml"),
            env_file: env_file.to_owned(),
            project: Some("gas-station-router-v2-e2e".into()),
        }
    }

    pub fn proxy(env_file: &Path, directory: &Path, ports: [u16; 3], id: &str) -> Result<Self> {
        let mut config = fs::read_to_string(Path::new(ROOT).join("docker/nginx.conf.template"))?;
        for (name, port) in [
            ("ROUTER_PROXY_PORT", ports[0]),
            ("ROUTER_1_RPC_PORT", ports[1]),
            ("ROUTER_2_RPC_PORT", ports[2]),
        ] {
            config = config.replace(&format!("${{{name}}}"), &port.to_string());
        }
        let nginx = directory.join("nginx.conf");
        fs::write(&nginx, config)?;
        let yaml = serde_json::json!({"services":{"nginx":{
            "image":"nginx:stable-alpine", "network_mode":"host", "restart":"no",
            "volumes":[format!("{}:/etc/nginx/nginx.conf:ro", nginx.display())]
        }}});
        let file = directory.join("proxy-compose.yaml");
        fs::write(&file, serde_yaml_ng::to_string(&yaml)?)?;
        Ok(Self {
            file,
            env_file: env_file.to_owned(),
            project: Some(format!("router-full-{id}")),
        })
    }

    fn command(&self, args: &[&str]) -> Command {
        let mut command = Command::new("docker");
        command
            .current_dir(Path::new(ROOT).join("docker"))
            .args(["compose", "--env-file"])
            .arg(&self.env_file)
            .arg("-f")
            .arg(&self.file);
        if let Some(project) = &self.project {
            command.args(["--project-name", project]);
        }
        command.args(args).stdin(Stdio::null()).kill_on_drop(true);
        command
    }

    pub async fn run(&self, args: &[&str]) -> Result<String> {
        let output = self
            .command(args)
            .output()
            .await
            .context("Cannot run Docker Compose")?;
        // Compose errors can echo .env lines. Never include raw stderr or rendered config.
        ensure!(
            output.status.success(),
            "Docker Compose {} failed ({})",
            args.join(" "),
            output.status
        );
        String::from_utf8(output.stdout).context("Invalid Docker output")
    }

    pub async fn start_services(&self) -> Result<()> {
        println!("[setup] Starting local Redis and three Gas Stations");
        self.run(&[
            "up",
            "-d",
            "--build",
            "redis",
            "gas-station-1",
            "gas-station-2",
            "gas-station-3",
        ])
        .await?;
        self.wait_stations().await?;
        Ok(())
    }

    async fn wait_stations(&self) -> Result<()> {
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(1))
            .build()?;
        let mut urls = Vec::new();
        for service in ["gas-station-1", "gas-station-2", "gas-station-3"] {
            let address = self.run(&["port", service, "9527"]).await?;
            ensure!(
                address.trim().starts_with("127.0.0.1:"),
                "Gas Stations must be published on loopback"
            );
            urls.push(format!("http://{}/", address.trim()));
        }
        println!("[setup] Waiting for all three Gas Stations to answer health checks");
        timeout(Duration::from_secs(60), async {
            loop {
                let mut ready = 0;
                for url in &urls {
                    if let Ok(response) = client.get(url).send().await
                        && response.status() == reqwest::StatusCode::OK
                    {
                        ready += 1;
                    }
                }
                if ready == urls.len() {
                    return;
                }
                sleep(Duration::from_millis(500)).await;
            }
        })
        .await
        .context("Gas Stations did not become ready within 60s")?;
        Ok(())
    }

    pub async fn stop_service(&self, service: &str) -> Result<()> {
        self.run(&["stop", "--timeout", "0", service]).await?;
        println!("[fault] Stopped {service}");
        Ok(())
    }

    pub async fn start_service(&self, service: &str) -> Result<()> {
        self.run(&["start", service]).await?;
        println!("[recovery] Started {service}");
        Ok(())
    }

    pub async fn redis_url(&self) -> Result<String> {
        let address = self.run(&["port", "redis", "6379"]).await?;
        let address = address.trim();
        ensure!(
            address.starts_with("127.0.0.1:"),
            "Full e2e requires Redis published on loopback"
        );
        Ok(format!("redis://{address}/0"))
    }

    pub async fn save_logs(&self, path: &Path) -> Result<()> {
        fs::write(path, self.run(&["logs", "--no-color", "nginx"]).await?)?;
        Ok(())
    }
}
