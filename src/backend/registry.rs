use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use super::health::{BackendHealth, HealthCheckOutcome, HealthState, ReservationOutcome};
use crate::{
    config::{BackendConfig, PassiveHealthCheckConfig},
    metrics,
    routing::BackendCandidate,
};
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
    passive: PassiveHealthCheckConfig,
    inflight: AtomicUsize,
}

impl BackendRegistry {
    pub fn new(configs: &[BackendConfig]) -> Result<Self> {
        Self::with_passive_health(configs, PassiveHealthCheckConfig::default())
    }

    pub fn with_passive_health(
        configs: &[BackendConfig],
        passive: PassiveHealthCheckConfig,
    ) -> Result<Self> {
        if passive.enabled {
            ensure!(
                passive.failure_threshold > 0,
                "health_checks.passive.failure_threshold must be greater than zero"
            );
            ensure!(
                !passive.initial_cooldown.is_zero(),
                "health_checks.passive.initial_cooldown must be greater than zero"
            );
            ensure!(
                passive.backoff >= 1,
                "health_checks.passive.backoff must be at least 1"
            );
            ensure!(
                passive.max_cooldown >= passive.initial_cooldown,
                "health_checks.passive.max_cooldown must be at least initial_cooldown"
            );
        }
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
            metrics::init_backend_authorization_errors(&config.name);
            backends.push(Arc::new(Backend {
                name: config.name.clone(),
                url,
                authorization,
                enabled: config.enabled,
                health: Mutex::new(HealthState::new()),
                passive: passive.clone(),
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
                    && backend.health() == BackendHealth::Healthy
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
            let mut state = backend
                .health
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            state.log_cooldown_expiry(&backend.name);
            state.set_healthy(healthy);
            state.log_transition(&backend.name, "manual", "manual_override", None, None);
            true
        } else {
            false
        }
    }

    pub fn health(&self, name: &str) -> Option<BackendHealth> {
        self.get(name).map(|backend| backend.health())
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
    fn health(&self) -> BackendHealth {
        let mut state = self
            .health
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.log_cooldown_expiry(&self.name);
        state.status()
    }

    /// Passive health checks based on the outcome of `reserve_gas`.
    ///
    /// | `reserve_gas` outcome | Reaction |
    /// | --- | --- |
    /// | Success | Reset the reservation failure counter and, after cooldown expires, reset cooldown backoff |
    /// | Connection refused / timeout | Increment the failure counter |
    /// | HTTP 500/502/503/504 | Increment the failure counter, accounting for known business errors |
    /// | HTTP 400 | Do not mark the backend as `Unhealthy` |
    /// | HTTP 401/403 | Emit an ERROR log and increment `gas_station_router_backend_authorization_errors_total` |
    /// | Insufficient gas coins | Mark as `Degraded` and exclude from new reservations during cooldown |
    ///
    /// Insufficient gas coins are detected from a `reserve_gas` error for the requested
    /// budget, without reading coin counts from metrics.
    /// Cooldowns start at `initial_cooldown`, grow by the integer `backoff` multiplier
    /// on subsequent rejections and are capped at `max_cooldown`.
    pub(crate) fn record_reservation_outcome(&self, outcome: ReservationOutcome) {
        if let ReservationOutcome::AuthorizationFailure(status) = outcome {
            metrics::inc_backend_authorization_error(&self.name, status);
            tracing::error!(backend = %self.name, status, alert = "backend_reservation_authorization",
                "Backend reserve_gas authorization failed; check the configured backend token");
        }
        let mut state = self
            .health
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.log_cooldown_expiry(&self.name);
        state.record_reservation(outcome, &self.passive);
        let (reason, http_status) = match outcome {
            ReservationOutcome::Success => ("reservation_success", None),
            ReservationOutcome::BackendFailure(cause) => cause.log_fields(),
            ReservationOutcome::NoGasCoins => ("no_gas_coins", None),
            ReservationOutcome::AuthorizationFailure(status) => {
                ("authorization_failure", Some(status))
            }
            ReservationOutcome::Neutral => ("neutral_response", None),
        };
        let threshold = matches!(outcome, ReservationOutcome::BackendFailure(_))
            .then_some(self.passive.failure_threshold);
        state.log_transition(&self.name, "passive", reason, http_status, threshold);
    }

    pub(super) fn health_request(&self, client: &Client) -> RequestBuilder {
        self.authorize(client.get(self.url.clone()))
    }

    pub(super) fn record_health_result(
        &self,
        outcome: HealthCheckOutcome,
        healthy_threshold: u32,
        unhealthy_threshold: u32,
    ) {
        let mut state = self
            .health
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.log_cooldown_expiry(&self.name);
        let success = outcome == HealthCheckOutcome::HttpStatus(200);
        state.record(success, healthy_threshold, unhealthy_threshold);
        let (reason, http_status) = outcome.log_fields();
        let threshold = if success {
            healthy_threshold
        } else {
            unhealthy_threshold
        };
        state.log_transition(
            &self.name,
            "background",
            reason,
            http_status,
            Some(threshold),
        );
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

    pub(crate) fn execute_request(&self, client: &Client) -> RequestBuilder {
        let mut url = self.url.clone();
        url.set_path(&format!("{}v1/execute_tx", self.url.path()));
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
