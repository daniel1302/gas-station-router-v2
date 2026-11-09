//! Process-wide, lazily initialized metrics. This module does not access storage.
//! Counters describe events; candidate count is the most recent registry snapshot.

use std::sync::LazyLock;

use axum::{
    Router,
    http::{StatusCode, header::CONTENT_TYPE},
    response::{IntoResponse, Response},
    routing::get,
};
use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder, core::Collector,
};

static METRICS: LazyLock<Metrics> = LazyLock::new(Metrics::new);

struct Metrics {
    registry: Registry,
    execution_phases: IntCounterVec,
    execution_claims: IntCounterVec,
    cached_transactions: IntCounterVec,
    storage_errors: IntCounterVec,
    storage_calls: IntCounterVec,
    storage_health: IntCounterVec,
    selector_calls: IntCounterVec,
    candidates: IntGauge,
    reserve_requests: IntCounter,
    reserve_errors: IntCounter,
    reserve_successes: IntCounter,
    backend_authorization_errors: IntCounterVec,
    health_transitions: IntCounterVec,
    health_check_failures: IntCounterVec,
    reserve_attempt_failures: IntCounterVec,
    reserve_retries: IntCounterVec,
    execution_errors: IntCounterVec,
    execution_successes: IntCounter,
    recovery_retries: IntCounterVec,
}

impl Metrics {
    fn new() -> Self {
        let registry = Registry::new();
        let execution_phases = counter_vec(
            &registry,
            "storage_execution_phase_transitions_total",
            "Execution records entering each phase; not a live record count.",
            &["kind"],
        );
        let execution_claims = counter_vec(
            &registry,
            "storage_execution_claims_total",
            "Successful storage claim calls by domain outcome, including conflicts.",
            &["kind"],
        );
        let cached_transactions = counter_vec(
            &registry,
            "storage_cached_transactions_total",
            "Transaction effects inserted into cache; upgrades and reads are excluded.",
            &["backend"],
        );
        let storage_errors = counter_vec(
            &registry,
            "storage_errors_total",
            "Storage calls returning an error, by bounded error kind.",
            &["kind"],
        );
        let storage_calls = counter_vec(
            &registry,
            "storage_calls_total",
            "Storage method invocations, including failed calls.",
            &["method"],
        );
        let storage_health = counter_vec(
            &registry,
            "storage_health_checks_total",
            "Storage health check results; not current health state.",
            &["kind"],
        );
        let selector_calls = counter_vec(
            &registry,
            "backend_selector_calls_total",
            "Selector calls by algorithm and selected backend; empty backend means no selection.",
            &["algorithm", "backend_name"],
        );
        let candidates = register(
            &registry,
            IntGauge::new(
                "gas_station_router_backend_registry_candidates",
                "Number of candidates returned by the latest registry call, after exclusions.",
            )
            .expect("static gauge definition is valid"),
        );
        let backend_authorization_errors = counter_vec(
            &registry,
            "backend_authorization_errors_total",
            "Backend reserve_gas responses with HTTP 401 or 403; check backend authorization configuration.",
            &["backend", "status"],
        );
        let health_transitions = counter_vec(
            &registry,
            "backend_health_transitions_total",
            "Effective backend health changes, including passive checks and cooldown expiry.",
            &["backend", "previous_health", "health"],
        );
        let health_check_failures = counter_vec(
            &registry,
            "backend_health_check_failures_total",
            "Failed active health probes, including failures below the unhealthy threshold.",
            &["backend", "reason"],
        );
        let reserve_attempt_failures = counter_vec(
            &registry,
            "reserve_gas_attempt_failures_total",
            "Failed upstream reservation attempts, including business rejections.",
            &["backend", "reason"],
        );
        let reserve_retries = counter_vec(
            &registry,
            "reserve_gas_retries_total",
            "Additional reservation attempts started, labeled by the backend and failure that caused the retry.",
            &["backend", "reason"],
        );
        let execution_errors = counter_vec(
            &registry,
            "execute_tx_errors_total",
            "Failed execute_tx calls, once per final result, including validation and finalized transaction failures.",
            &["reason"],
        );
        let execution_successes = register(
            &registry,
            IntCounter::new(
                "gas_station_router_execute_tx_successes_total",
                "Successful execute_tx results, once per call, including recovery and cache reads; finalized transaction failures are excluded.",
            )
            .expect("static counter definition is valid"),
        );
        let recovery_retries = counter_vec(
            &registry,
            "execute_tx_recovery_retries_total",
            "Additional RPC lookup rounds started after an inconclusive recovery or local execution check; never transaction resubmissions.",
            &["backend", "stage"],
        );
        let reserve_requests = register(
            &registry,
            IntCounter::new(
                "gas_station_router_reserve_gas_requests_total",
                "Incoming reserve_gas HTTP calls, including rejected requests; retries are not new calls.",
            )
            .expect("static counter definition is valid"),
        );
        let reserve_errors = register(
            &registry,
            IntCounter::new(
                "gas_station_router_reserve_gas_errors_total",
                "reserve_gas calls returning an HTTP or business error, counted once per final response.",
            )
            .expect("static counter definition is valid"),
        );
        let reserve_successes = register(
            &registry,
            IntCounter::new(
                "gas_station_router_reserve_gas_successes_total",
                "Successful reserve_gas responses after storing the reservation mapping, once per call; retries are not additional successes.",
            )
            .expect("static counter definition is valid"),
        );
        let up = register(
            &registry,
            IntGauge::new(
                "gas_station_router_up",
                "Whether the router HTTP servers are running.",
            )
            .expect("static gauge definition is valid"),
        );
        up.set(1);

        for kind in ["claimed", "dispatching", "completed"] {
            execution_phases.with_label_values(&[kind]);
        }
        for kind in [
            "acquired",
            "in_flight",
            "possibly_submitted",
            "completed",
            "conflict",
        ] {
            execution_claims.with_label_values(&[kind]);
        }
        for kind in ["backend", "invalid_ttl", "id_exhausted", "conflict"] {
            storage_errors.with_label_values(&[kind]);
        }
        for method in [
            "create_reservation",
            "get_reservation",
            "put_transaction",
            "get_transaction",
            "try_claim_execution",
            "get_execution",
            "try_claim_recovery",
            "mark_execution_dispatching",
            "complete_execution",
            "cancel_execution",
            "health_check",
        ] {
            storage_calls.with_label_values(&[method]);
        }
        for kind in ["healthy", "unhealthy"] {
            storage_health.with_label_values(&[kind]);
        }
        for reason in [
            "unauthorized",
            "invalid_request",
            "invalid_transaction",
            "invalid_signature",
            "reservation_not_found",
            "backend_not_configured",
            "digest_conflict",
            "storage_conflict",
            "storage_unavailable",
            "invalid_backend_request",
            "backend_metadata_missing",
            "rpc_effects_conflict",
            "timeout",
            "task_failed",
            "transaction_failed",
        ] {
            execution_errors.with_label_values(&[reason]);
        }
        Self {
            registry,
            execution_phases,
            execution_claims,
            cached_transactions,
            storage_errors,
            storage_calls,
            storage_health,
            selector_calls,
            candidates,
            reserve_requests,
            reserve_errors,
            reserve_successes,
            backend_authorization_errors,
            health_transitions,
            health_check_failures,
            reserve_attempt_failures,
            reserve_retries,
            execution_errors,
            execution_successes,
            recovery_retries,
        }
    }
}

fn register<T: Collector + Clone + 'static>(registry: &Registry, metric: T) -> T {
    registry
        .register(Box::new(metric.clone()))
        .expect("static metrics are registered once with unique names");
    metric
}

fn counter_vec(registry: &Registry, name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    register(
        registry,
        IntCounterVec::new(
            Opts::new(format!("gas_station_router_{name}"), help),
            labels,
        )
        .expect("static counter definition is valid"),
    )
}

pub fn inc_storage_execution_phase(kind: &'static str) {
    METRICS.execution_phases.with_label_values(&[kind]).inc();
}

pub fn inc_storage_execution_claim(kind: &'static str) {
    METRICS.execution_claims.with_label_values(&[kind]).inc();
}

pub fn inc_storage_cached_transactions(backend: &str) {
    METRICS
        .cached_transactions
        .with_label_values(&[backend])
        .inc();
}

pub fn inc_storage_error(kind: &'static str) {
    METRICS.storage_errors.with_label_values(&[kind]).inc();
}

pub fn inc_storage_call(method: &'static str) {
    METRICS.storage_calls.with_label_values(&[method]).inc();
}

pub fn inc_storage_health(healthy: bool) {
    METRICS
        .storage_health
        .with_label_values(&[if healthy { "healthy" } else { "unhealthy" }])
        .inc();
}

pub fn inc_backend_selector_call(algorithm: &'static str, backend_name: Option<&str>) {
    METRICS
        .selector_calls
        .with_label_values(&[algorithm, backend_name.unwrap_or("")])
        .inc();
}

pub fn set_backend_registry_candidates(count: usize) {
    METRICS
        .candidates
        .set(i64::try_from(count).unwrap_or(i64::MAX));
}

pub fn inc_reserve_gas() {
    METRICS.reserve_requests.inc();
}

pub fn inc_reserve_gas_error() {
    METRICS.reserve_errors.inc();
}

pub(crate) fn inc_reserve_gas_success() {
    METRICS.reserve_successes.inc();
}

pub(crate) fn init_backend_metrics(backend: &str) {
    for stage in ["recovery", "local_execution"] {
        METRICS
            .recovery_retries
            .with_label_values(&[backend, stage]);
    }
    for status in ["401", "403"] {
        METRICS
            .backend_authorization_errors
            .with_label_values(&[backend, status]);
    }
    for previous in ["healthy", "degraded", "unhealthy"] {
        for health in ["healthy", "degraded", "unhealthy"] {
            if previous != health {
                METRICS
                    .health_transitions
                    .with_label_values(&[backend, previous, health]);
            }
        }
    }
    for reason in [
        "http_error",
        "connection_refused",
        "connection_timeout",
        "request_timeout",
        "connection_error",
        "transport_error",
    ] {
        METRICS
            .health_check_failures
            .with_label_values(&[backend, reason]);
    }
    for reason in [
        "connection_refused",
        "connection_timeout",
        "request_timeout",
        "connection_error",
        "transport_error",
        "invalid_response",
        "redirect",
        "http_error",
        "authorization_error",
        "invalid_request",
        "rate_limited",
        "no_gas_coins",
        "maintenance",
        "daily_cap",
        "business_rejection",
    ] {
        METRICS
            .reserve_attempt_failures
            .with_label_values(&[backend, reason]);
        METRICS
            .reserve_retries
            .with_label_values(&[backend, reason]);
    }
}

pub(crate) fn inc_backend_health_transition(
    backend: &str,
    previous_health: &'static str,
    health: &'static str,
) {
    METRICS
        .health_transitions
        .with_label_values(&[backend, previous_health, health])
        .inc();
}

pub(crate) fn inc_backend_health_check_failure(backend: &str, reason: &'static str) {
    METRICS
        .health_check_failures
        .with_label_values(&[backend, reason])
        .inc();
}

pub(crate) fn inc_reserve_gas_attempt_failure(backend: &str, reason: &'static str) {
    METRICS
        .reserve_attempt_failures
        .with_label_values(&[backend, reason])
        .inc();
}

pub(crate) fn inc_reserve_gas_retry(backend: &str, reason: &'static str) {
    METRICS
        .reserve_retries
        .with_label_values(&[backend, reason])
        .inc();
}

pub(crate) fn inc_execute_tx_error(reason: &'static str) {
    METRICS.execution_errors.with_label_values(&[reason]).inc();
}

pub(crate) fn inc_execute_tx_success() {
    METRICS.execution_successes.inc();
}

pub(crate) fn inc_execute_tx_recovery_retry(backend: &str, stage: &'static str) {
    METRICS
        .recovery_retries
        .with_label_values(&[backend, stage])
        .inc();
}

pub fn inc_backend_authorization_error(backend: &str, status: u16) {
    let status = match status {
        401 => "401",
        403 => "403",
        _ => return,
    };
    METRICS
        .backend_authorization_errors
        .with_label_values(&[backend, status])
        .inc();
}

pub fn render() -> Result<String, prometheus::Error> {
    TextEncoder::new().encode_to_string(&METRICS.registry.gather())
}

pub fn router() -> Router {
    Router::new().route("/metrics", get(scrape))
}

async fn scrape() -> Response {
    match render() {
        Ok(body) => ([(CONTENT_TYPE, TextEncoder::new().format_type())], body).into_response(),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "failed to encode metrics",
        )
            .into_response(),
    }
}
