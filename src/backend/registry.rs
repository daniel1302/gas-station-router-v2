use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::health::HealthState;
use crate::{config::BackendConfig, metrics, routing::BackendCandidate};
use anyhow::{Context, Result, ensure};
use reqwest::{
    Client, RequestBuilder, Url,
    header::{AUTHORIZATION, HeaderValue},
};

pub struct BackendRegistry {
    backends: Vec<Arc<Backend>>,
}

pub(crate) struct Backend {
    pub name: String,
    url: Url,
    authorization: Option<HeaderValue>,
    enabled: bool,
    health: Mutex<HealthState>,
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
                health: Mutex::new(HealthState::new()),
                inflight: AtomicUsize::new(0),
            }));
        }
        Ok(Self { backends })
    }

    pub fn candidates(&self, excluded: &HashSet<String>) -> Vec<BackendCandidate> {
        let candidates: Vec<_> = self
            .backends
            .iter()
            .filter(|backend| {
                backend.enabled
                    && backend
                        .health
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .is_healthy()
                    && !excluded.contains(&backend.name)
            })
            .map(|backend| BackendCandidate {
                name: backend.name.clone(),
                weight: 1,
                inflight: backend.inflight.load(Ordering::Relaxed),
            })
            .collect();
        metrics::set_backend_registry_candidates(candidates.len());
        candidates
    }

    /// Allows health monitoring to update a backend without changing routing policy.
    pub fn set_healthy(&self, name: &str, healthy: bool) -> bool {
        if let Some(backend) = self.get(name) {
            backend
                .health
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .set_healthy(healthy);
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

    pub(crate) fn enabled_backends(&self) -> impl Iterator<Item = Arc<Backend>> + '_ {
        self.backends
            .iter()
            .filter(|backend| backend.enabled)
            .cloned()
    }
}

impl Backend {
    pub(super) fn health_request(&self, client: &Client) -> RequestBuilder {
        self.authorize(client.get(self.url.clone()))
    }

    pub(super) fn record_health_result(
        &self,
        success: bool,
        healthy_threshold: u32,
        unhealthy_threshold: u32,
    ) {
        let transition = self
            .health
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .record(success, healthy_threshold, unhealthy_threshold);
        if let Some(healthy) = transition {
            tracing::info!(backend = %self.name, healthy, "Backend health changed");
        }
    }

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
