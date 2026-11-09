use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use anyhow::{Result, ensure};

pub(super) struct Docker {
    project: String,
    environment: BTreeMap<&'static str, String>,
    directory: PathBuf,
}

impl Docker {
    pub fn start(directory: &Path, ports: [u16; 4]) -> Result<Self> {
        let docker = Self {
            project: format!("router-mock-{}", uuid::Uuid::new_v4()),
            directory: directory.to_owned(),
            environment: [
                "E2E_REDIS_PORT",
                "ROUTER_PROXY_PORT",
                "ROUTER_1_RPC_PORT",
                "ROUTER_2_RPC_PORT",
            ]
            .into_iter()
            .zip(ports.map(|port| port.to_string()))
            .collect(),
        };
        docker.run(&["up", "-d", "--wait"])?;
        Ok(docker)
    }

    fn command(&self) -> Command {
        let mut command = Command::new("docker");
        command
            .current_dir(crate::common::ROOT)
            .args([
                "compose",
                "-f",
                "docker/mock/docker-compose.yml",
                "--project-name",
                &self.project,
            ])
            .envs(&self.environment);
        command
    }

    pub fn run(&self, args: &[&str]) -> Result<()> {
        let output = self.command().args(args).output()?;
        ensure!(
            output.status.success(),
            "Mock Compose {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    pub fn stop(&self) -> Result<()> {
        if !self.directory.join("compose.log").exists()
            && let Ok(output) = self.command().args(["logs", "--no-color"]).output()
        {
            fs::write(self.directory.join("compose.log"), output.stdout)?;
        }
        self.run(&["down", "--volumes", "--remove-orphans"])
    }
}

impl Drop for Docker {
    fn drop(&mut self) {
        let _ = self.stop();
    }
}
