use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use reqwest::{
    Client, RequestBuilder, Url,
    header::{AUTHORIZATION, HeaderValue},
};
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{MissedTickBehavior, interval},
};

use crate::{config::BackendConfig, routing::BackendCandidate};

pub struct BackendRegistry {
    backends: Vec<Arc<Backend>>,
}

pub(crate) struct Backend {
    pub name: String,
    url: Url,
    authorization: Option<HeaderValue>,
    enabled: bool,
    healthy: AtomicBool,
    inflight: AtomicUsize,
}

impl BackendRegistry {
    pub fn new(configs: &[BackendConfig]) -> Result<Self> {
        let mut names = HashSet::new();
        let mut backends = Vec::new();
        for config in configs {
            ensure!(
                !config.name.trim().is_empty(),
                "backend name must not be empty"
            );
            ensure!(
                names.insert(&config.name),
                "duplicate backend name: {}",
                config.name
            );
            let url = Url::parse(&format!("{}/", config.rpc.trim_end_matches('/')))
                .with_context(|| format!("invalid RPC URL for backend {}", config.name))?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host_str().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "backend {} RPC must be an HTTP(S) base URL without credentials, query or fragment",
                config.name
            );
            let authorization = if config.token.is_empty() {
                None
            } else {
                let mut value = HeaderValue::from_str(&format!("Bearer {}", config.token))
                    .with_context(|| format!("invalid auth token for backend {}", config.name))?;
                value.set_sensitive(true);
                Some(value)
            };
            backends.push(Arc::new(Backend {
                name: config.name.clone(),
                url,
                authorization,
                enabled: config.enabled,
                healthy: AtomicBool::new(true),
                inflight: AtomicUsize::new(0),
            }));
        }
        Ok(Self { backends })
    }

    pub fn candidates(&self, excluded: &HashSet<String>) -> Vec<BackendCandidate> {
        self.backends
            .iter()
            .filter(|backend| {
                backend.enabled
                    && backend.healthy.load(Ordering::Relaxed)
                    && !excluded.contains(&backend.name)
            })
            .map(|backend| BackendCandidate {
                name: backend.name.clone(),
                weight: 1,
                inflight: backend.inflight.load(Ordering::Relaxed),
            })
            .collect()
    }

    /// Allows health monitoring to update a backend without changing routing policy.
    pub fn set_healthy(&self, name: &str, healthy: bool) -> bool {
        if let Some(backend) = self.get(name) {
            backend.healthy.store(healthy, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub(crate) fn get(&self, name: &str) -> Option<Arc<Backend>> {
        self.backends
            .iter()
            .find(|backend| backend.name == name)
            .cloned()
    }

    /// Probe each enabled backend's base URL (GET / for a normal Gas Station).
    pub async fn refresh_health(&self, client: &Client, timeout: Duration) {
        let mut checks = JoinSet::new();
        for backend in self.backends.iter().filter(|backend| backend.enabled) {
            let backend = backend.clone();
            let client = client.clone();
            checks.spawn(async move {
                let response = backend
                    .authorize(client.get(backend.url.clone()))
                    .timeout(timeout)
                    .send()
                    .await;
                backend.healthy.store(
                    response.is_ok_and(|response| response.status().is_success()),
                    Ordering::Relaxed,
                );
            });
        }
        while checks.join_next().await.is_some() {}
    }

    pub(crate) async fn monitor_health(
        &self,
        client: Client,
        timeout: Duration,
        mut shutdown: watch::Receiver<()>,
    ) -> Result<()> {
        let mut ticks = interval(Duration::from_secs(5));
        ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
        ticks.tick().await; // Startup already performed the initial probe.
        loop {
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                _ = ticks.tick() => self.refresh_health(&client, timeout).await,
            }
        }
    }
}

impl Backend {
    fn authorize(&self, request: RequestBuilder) -> RequestBuilder {
        match &self.authorization {
            Some(value) => request.header(AUTHORIZATION, value.clone()),
            None => request,
        }
    }

    pub fn reserve_request(&self, client: &Client) -> RequestBuilder {
        let mut url = self.url.clone();
        url.set_path(&format!("{}v1/reserve_gas", self.url.path()));
        self.authorize(client.post(url))
    }

    pub fn start_request(self: &Arc<Self>) -> InflightRequest {
        self.inflight.fetch_add(1, Ordering::Relaxed);
        InflightRequest(self.clone())
    }
}

pub(crate) struct InflightRequest(Arc<Backend>);

impl Drop for InflightRequest {
    fn drop(&mut self) {
        self.0.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}
