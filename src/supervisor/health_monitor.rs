use std::time::Duration;

use anyhow::Result;
use reqwest::Client;
use serde::Serialize;
use tokio::time::Instant;

use super::config::HealthConfig;

#[cfg(test)]
mod tests;

#[derive(Default, Serialize)]
pub(super) struct HealthStatus {
    live: Option<bool>,
    ready: Option<bool>,
    failure_count: u32,
}

pub(super) struct HealthMonitor {
    client: Client,
    base_url: String,
    config: HealthConfig,
    started: Instant,
    startup_grace: Duration,
    pub status: HealthStatus,
}

impl HealthMonitor {
    pub fn new(config: HealthConfig, base_url: String, startup_grace: Duration) -> Result<Self> {
        Ok(Self {
            client: super::http_client(config.timeout)?,
            base_url,
            config,
            started: Instant::now(),
            startup_grace,
            status: HealthStatus::default(),
        })
    }

    pub fn reset(&mut self) {
        self.started = Instant::now();
        self.status = HealthStatus::default();
    }

    pub async fn check(&mut self) {
        if !self.config.enabled {
            return;
        }
        let live = self.probe("/").await;
        let ready = match (&self.config.readiness_path, live) {
            (Some(path), true) => Some(self.probe(path).await),
            (Some(_), false) => Some(false),
            _ => None,
        };
        self.status.live = Some(live);
        self.status.ready = ready;
        if live && ready != Some(false) {
            self.status.failure_count = 0;
        } else if self.started.elapsed() >= self.startup_grace {
            self.status.failure_count = self.status.failure_count.saturating_add(1);
            tracing::warn!(
                live,
                ?ready,
                failures = self.status.failure_count,
                "Gas Station health check failed"
            );
        }
    }

    pub fn unhealthy(&self) -> bool {
        self.config.enabled && self.status.failure_count >= self.config.unhealthy_threshold
    }

    async fn probe(&self, path: &str) -> bool {
        self.client
            .get(format!("{}{path}", self.base_url))
            .send()
            .await
            .is_ok_and(|response| response.status() == reqwest::StatusCode::OK)
    }
}
