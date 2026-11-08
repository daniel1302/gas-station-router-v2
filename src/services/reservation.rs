use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use axum::http::StatusCode;
use iota_sdk_types::{Address, ObjectDigest, ObjectId};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::{
    backend::{
        health::{HealthCheckOutcome, ReservationOutcome},
        registry::{Backend, BackendRegistry},
    },
    config::ReserveGasConfig,
    metrics,
    routing::BackendSelector,
    storage::{Reservation, Storage},
};

#[derive(Debug, Serialize, Deserialize)]
pub struct ReserveGasRequest {
    pub gas_budget: u64,
    pub reserve_duration_secs: u64,
}

#[derive(Deserialize)]
struct ReserveGasResponse {
    result: Option<ReserveGasResult>,
    error: Option<String>,
}

#[derive(Deserialize)]
pub struct ReserveGasResult {
    pub sponsor_address: Address,
    pub reservation_id: u64,
    pub gas_coins: Vec<GasCoin>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GasCoin {
    pub object_id: ObjectId,
    pub version: u64,
    pub digest: ObjectDigest,
}

pub struct ReserveReply {
    pub status: StatusCode,
    pub body: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReserveError {
    /// Request was definitely not delivered.
    ConnectionFailed,

    /// Backend explicitly rejected the reservation without creating it.
    ReservationRejected,

    /// Request may have reached the backend.
    UnknownOutcome,

    /// Invalid request or another permanent failure.
    Permanent,
}

impl ReserveError {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::ConnectionFailed | Self::ReservationRejected)
    }
}

struct ReserveFailure {
    kind: ReserveError,
    reply: ReserveReply,
    outcome: ReservationOutcome,
    reason: &'static str,
    http_status: Option<u16>,
}

struct UpstreamReservation {
    reply: ReserveReply,
    reservation_id: u64,
}

impl ReserveReply {
    pub fn error(status: StatusCode, message: &str) -> Self {
        Self {
            status,
            body: json!({ "result": null, "error": message }),
        }
    }
}

pub struct ReservationService {
    registry: Arc<BackendRegistry>,
    selector: Arc<dyn BackendSelector>,
    storage: Arc<dyn Storage>,
    client: Client,
    config: ReserveGasConfig,
    reservation_ttl: Duration,
}

/// Disable hidden retries and redirects: ReserveError controls all POST retries.
pub fn http_client(timeout: Duration) -> Result<Client> {
    ensure!(
        !timeout.is_zero(),
        "routing.reserve_gas.timeout must be greater than zero"
    );
    Ok(Client::builder()
        .timeout(timeout)
        .connect_timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .build()?)
}

impl ReservationService {
    pub(crate) fn backend_registry(&self) -> Arc<BackendRegistry> {
        self.registry.clone()
    }

    pub fn new(
        registry: Arc<BackendRegistry>,
        selector: Arc<dyn BackendSelector>,
        storage: Arc<dyn Storage>,
        client: Client,
        config: ReserveGasConfig,
        reservation_ttl: Duration,
    ) -> Result<Self> {
        ensure!(
            !config.timeout.is_zero(),
            "routing.reserve_gas.timeout must be greater than zero"
        );
        ensure!(
            !reservation_ttl.is_zero(),
            "routing.ttl.reservation must be greater than zero"
        );
        Ok(Self {
            registry,
            selector,
            storage,
            client,
            config,
            reservation_ttl,
        })
    }

    pub async fn reserve_with_retry(&self, request: &ReserveGasRequest) -> ReserveReply {
        if request.gas_budget == 0 || request.reserve_duration_secs == 0 {
            tracing::warn!(reason = "invalid_request", "Reservation request rejected");
            return ReserveReply::error(
                StatusCode::BAD_REQUEST,
                "gas_budget and reserve_duration_secs must be greater than zero",
            );
        }
        let mut excluded = HashSet::new();
        let mut last_failure = None;
        let mut pending_retry: Option<(String, &'static str)> = None;
        for attempt in 0..=self.config.retries.max_retries {
            let candidates = self.registry.candidates(&excluded);
            let Some(candidate) = self
                .selector
                .select(&candidates)
                .and_then(|index| candidates.get(index))
            else {
                tracing::warn!(
                    reason = "no_eligible_backend",
                    attempts = attempt,
                    "Reservation stopped: no eligible backend"
                );
                return last_failure.unwrap_or_else(|| {
                    ReserveReply::error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "no healthy enabled backend available",
                    )
                });
            };
            let Some(backend) = self.registry.get(&candidate.name) else {
                tracing::warn!(backend = %candidate.name, reason = "backend_unavailable",
                    "Selected reservation backend is unavailable");
                return ReserveReply::error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "selected backend is unavailable",
                );
            };
            if attempt > 0 {
                sleep(self.config.retries.backoff).await;
            }
            if let Some((previous_backend, reason)) = pending_retry.take() {
                metrics::inc_reserve_gas_retry(&previous_backend, reason);
                tracing::warn!(
                    backend = %previous_backend,
                    retry_backend = %backend.name,
                    reason,
                    attempt = u64::from(attempt) + 1,
                    "Retrying reservation on another backend"
                );
            }
            let mut reserved = match self.reserve_once(&backend, request).await {
                Ok(reserved) => reserved,
                Err(failure) => {
                    metrics::inc_reserve_gas_attempt_failure(&backend.name, failure.reason);
                    let retry_allowed =
                        failure.kind.is_retryable() && attempt < self.config.retries.max_retries;
                    tracing::warn!(
                        backend = %backend.name,
                        reason = failure.reason,
                        http_status = failure.http_status,
                        response_status = failure.reply.status.as_u16(),
                        attempt = u64::from(attempt) + 1,
                        max_retries = self.config.retries.max_retries,
                        failure_kind = ?failure.kind,
                        retry_allowed,
                        "Backend reservation attempt failed"
                    );
                    backend.record_reservation_outcome(failure.outcome);
                    if !retry_allowed {
                        return failure.reply;
                    }
                    excluded.insert(backend.name.clone());
                    pending_retry = Some((backend.name.clone(), failure.reason));
                    last_failure = Some(failure.reply);
                    continue;
                }
            };
            backend.record_reservation_outcome(ReservationOutcome::Success);
            let id = match self
                .storage
                .create_reservation(
                    Reservation {
                        backend: backend.name.clone(),
                        upstream_reservation_id: reserved.reservation_id,
                    },
                    self.reservation_ttl,
                )
                .await
            {
                Ok(id) => id,
                Err(_) => {
                    tracing::warn!(backend = %backend.name, reason = "storage_unavailable",
                        "Failed to store reservation mapping after backend success");
                    return ReserveReply::error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "failed to store reservation mapping",
                    );
                }
            };
            reserved.reply.body["result"]["reservation_id"] = json!(id);
            return reserved.reply;
        }
        unreachable!("every attempt returns a response or consumes an available retry")
    }

    /// Exactly one HTTP attempt; no selection, retries or storage writes.
    async fn reserve_once(
        &self,
        backend: &Arc<Backend>,
        request: &ReserveGasRequest,
    ) -> std::result::Result<UpstreamReservation, ReserveFailure> {
        let _inflight = backend.start_request();
        let response = backend
            .reserve_request(&self.client)
            .timeout(self.config.timeout)
            .json(request)
            .send()
            .await
            .map_err(ReserveFailure::transport)?;
        let status = response.status();
        if status.is_redirection() {
            return Err(ReserveFailure {
                reason: "redirect",
                http_status: Some(status.as_u16()),
                kind: ReserveError::Permanent,
                outcome: ReservationOutcome::Neutral,
                reply: ReserveReply::error(
                    StatusCode::BAD_GATEWAY,
                    "backend redirects are not supported",
                ),
            });
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(|error| ReserveFailure::response_transport(error, status))?;
        let reply = ReserveReply { status, body };
        if let Ok(response) = serde_json::from_value::<ReserveGasResponse>(reply.body.clone()) {
            if let Some(error) = response.error.as_deref()
                && reply.body.get("result").is_some_and(Value::is_null)
                && !error.trim().is_empty()
            {
                return Err(ReserveFailure {
                    reason: rejection_reason(status, Some(error)),
                    http_status: Some(status.as_u16()),
                    kind: rejection_kind(status, error),
                    outcome: http_failure_outcome(status, Some(error)),
                    reply,
                });
            }
            if status.is_success()
                && response.error.is_none()
                && let Some(result) = response.result
                && !result.gas_coins.is_empty()
            {
                return Ok(UpstreamReservation {
                    reply,
                    reservation_id: result.reservation_id,
                });
            }
        }
        Err(ReserveFailure {
            reason: rejection_reason(status, None),
            http_status: Some(status.as_u16()),
            outcome: http_failure_outcome(status, None),
            kind: if status.is_client_error() {
                ReserveError::Permanent
            } else {
                ReserveError::UnknownOutcome
            },
            reply: if status.is_success() {
                ReserveReply::error(
                    StatusCode::BAD_GATEWAY,
                    "invalid backend reservation response",
                )
            } else {
                reply
            },
        })
    }
}

impl ReserveFailure {
    fn response_transport(error: reqwest::Error, status: StatusCode) -> Self {
        let is_decode = error.is_decode();
        let mut failure = Self::transport(error);
        failure.http_status = Some(status.as_u16());
        if is_decode {
            failure.reason = "invalid_response";
        }
        // Once headers are known, 4xx and auth errors must not turn into
        // backend failures just because their error body is malformed or slow.
        if !status.is_success() {
            failure.outcome = http_failure_outcome(status, None);
        }
        failure
    }

    fn transport(error: reqwest::Error) -> Self {
        // A connect timeout happens before delivery. A response/body timeout may
        // happen after reservation creation and must never cause another POST.
        let outcome = if error.is_connect() || error.is_timeout() {
            ReservationOutcome::BackendFailure(HealthCheckOutcome::from_error(&error))
        } else {
            ReservationOutcome::Neutral
        };
        let kind = if error.is_connect() {
            ReserveError::ConnectionFailed
        } else if error.is_builder() {
            ReserveError::Permanent
        } else {
            ReserveError::UnknownOutcome
        };
        let reply = if error.is_timeout() {
            ReserveReply::error(StatusCode::GATEWAY_TIMEOUT, "backend reservation timed out")
        } else if kind == ReserveError::ConnectionFailed {
            ReserveReply::error(
                StatusCode::BAD_GATEWAY,
                "failed to connect to reservation backend",
            )
        } else {
            ReserveReply::error(
                StatusCode::BAD_GATEWAY,
                "invalid or interrupted backend response",
            )
        };
        Self {
            reason: HealthCheckOutcome::from_error(&error).log_fields().0,
            http_status: None,
            kind,
            reply,
            outcome,
        }
    }
}

/// Stable categories only: upstream error text must never become a metric label.
fn rejection_reason(status: StatusCode, error: Option<&str>) -> &'static str {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => return "authorization_error",
        StatusCode::TOO_MANY_REQUESTS => return "rate_limited",
        status if status.is_client_error() => return "invalid_request",
        _ => {}
    }
    match error {
        Some("Unable to reserve gas coins for the given budget.") => "no_gas_coins",
        Some("Gas station is in maintenance mode. Please try again later.") => "maintenance",
        Some("Gas usage exceeds daily cap") => "daily_cap",
        _ if status.is_server_error() => "http_error",
        Some(_) => "business_rejection",
        None => "invalid_response",
    }
}

fn http_failure_outcome(status: StatusCode, rejection: Option<&str>) -> ReservationOutcome {
    match status {
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => {
            return ReservationOutcome::AuthorizationFailure(status.as_u16());
        }
        status if status.is_client_error() && status != StatusCode::TOO_MANY_REQUESTS => {
            return ReservationOutcome::Neutral;
        }
        _ => {}
    }
    // Only a validated negative envelope may identify a capacity/business error.
    // A generic 5xx is evidence of a fault, but still does not make POST retry safe.
    match rejection {
        Some("Unable to reserve gas coins for the given budget.") => ReservationOutcome::NoGasCoins,
        Some(
            "Gas station is in maintenance mode. Please try again later."
            | "Gas usage exceeds daily cap",
        ) => ReservationOutcome::Neutral,
        _ if matches!(
            status,
            StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        ) =>
        {
            ReservationOutcome::BackendFailure(HealthCheckOutcome::HttpStatus(status.as_u16()))
        }
        _ => ReservationOutcome::Neutral,
    }
}

fn rejection_kind(status: StatusCode, error: &str) -> ReserveError {
    if status.is_success() || status == StatusCode::TOO_MANY_REQUESTS {
        return ReserveError::ReservationRejected;
    }
    if status.is_client_error() {
        return ReserveError::Permanent;
    }
    // The current Gas Station API has string errors, not machine-readable codes.
    // These exact errors are emitted before a reservation is created (the Lua
    // insufficient-budget path restores the coins). Other 5xx errors, including
    // Redis I/O failures, do not establish whether a reservation was created.
    if status.is_server_error()
        && matches!(
            error,
            "Unable to reserve gas coins for the given budget."
                | "Gas station is in maintenance mode. Please try again later."
                | "Gas usage exceeds daily cap"
        )
    {
        ReserveError::ReservationRejected
    } else {
        ReserveError::UnknownOutcome
    }
}
