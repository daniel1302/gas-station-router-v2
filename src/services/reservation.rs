use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use axum::http::StatusCode;
use iota_sdk_types::{Address, ObjectDigest, ObjectId};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::{
    backend::registry::{Backend, BackendRegistry},
    config::ReserveGasConfig,
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
            return ReserveReply::error(
                StatusCode::BAD_REQUEST,
                "gas_budget and reserve_duration_secs must be greater than zero",
            );
        }
        let mut excluded = HashSet::new();
        let mut last_failure = None;
        for attempt in 0..=self.config.retries.max_retries {
            let candidates = self.registry.candidates(&excluded);
            let Some(candidate) = self
                .selector
                .select(&candidates)
                .and_then(|index| candidates.get(index))
            else {
                return last_failure.unwrap_or_else(|| {
                    ReserveReply::error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "no healthy enabled backend available",
                    )
                });
            };
            let Some(backend) = self.registry.get(&candidate.name) else {
                return ReserveReply::error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "selected backend is unavailable",
                );
            };
            if attempt > 0 {
                sleep(self.config.retries.backoff).await;
            }
            let mut reserved = match self.reserve_once(&backend, request).await {
                Ok(reserved) => reserved,
                Err(failure) => {
                    if failure.kind == ReserveError::ConnectionFailed {
                        self.registry.set_healthy(&backend.name, false);
                    }
                    if !failure.kind.is_retryable() || attempt == self.config.retries.max_retries {
                        return failure.reply;
                    }
                    excluded.insert(backend.name.clone());
                    last_failure = Some(failure.reply);
                    continue;
                }
            };
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
                kind: ReserveError::Permanent,
                reply: ReserveReply::error(
                    StatusCode::BAD_GATEWAY,
                    "backend redirects are not supported",
                ),
            });
        }
        let body = response
            .json::<Value>()
            .await
            .map_err(ReserveFailure::transport)?;
        let reply = ReserveReply { status, body };
        if let Ok(response) = serde_json::from_value::<ReserveGasResponse>(reply.body.clone()) {
            if let Some(error) = response.error.as_deref()
                && reply.body.get("result").is_some_and(Value::is_null)
                && !error.trim().is_empty()
            {
                return Err(ReserveFailure {
                    kind: rejection_kind(status, error),
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
    fn transport(error: reqwest::Error) -> Self {
        // A connect timeout happens before delivery. A response/body timeout may
        // happen after reservation creation and must never cause another POST.
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
        Self { kind, reply }
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
