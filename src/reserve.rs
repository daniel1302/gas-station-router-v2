use std::{collections::HashSet, sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use axum::http::StatusCode;
use iota_sdk_types::{Address, ObjectDigest, ObjectId};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::{
    backend::registry::BackendRegistry,
    config::{ReserveGasConfig, RetryOn},
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

impl ReserveReply {
    pub fn error(status: StatusCode, message: &str) -> Self {
        Self {
            status,
            body: json!({ "result": null, "error": message }),
        }
    }
}

pub struct ReserveService {
    registry: Arc<BackendRegistry>,
    selector: Arc<dyn BackendSelector>,
    storage: Arc<dyn Storage>,
    client: Client,
    config: ReserveGasConfig,
    reservation_ttl: Duration,
}

/// Disable hidden retries and redirects: only the explicit connect-error policy retries POST.
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

impl ReserveService {
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

    pub async fn reserve_gas(&self, request: &ReserveGasRequest) -> ReserveReply {
        if request.gas_budget == 0 || request.reserve_duration_secs == 0 {
            return ReserveReply::error(
                StatusCode::BAD_REQUEST,
                "gas_budget and reserve_duration_secs must be greater than zero",
            );
        }
        let mut excluded = HashSet::new();
        for attempt in 0..=self.config.retries.max_retries {
            let candidates = self.registry.candidates(&excluded);
            let Some(candidate) = self
                .selector
                .select(&candidates)
                .and_then(|index| candidates.get(index))
            else {
                return ReserveReply::error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "no healthy enabled backend available",
                );
            };
            let Some(backend) = self.registry.get(&candidate.name) else {
                return ReserveReply::error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "selected backend is unavailable",
                );
            };
            let response = {
                let _inflight = backend.start_request();
                async {
                    let response = backend
                        .reserve_request(&self.client)
                        .timeout(self.config.timeout)
                        .json(request)
                        .send()
                        .await?;
                    let status = response.status();
                    let body = response.json::<Value>().await?;
                    Ok::<_, reqwest::Error>((status, body))
                }
                .await
            };
            let (status, mut body) = match response {
                Ok(reply) => reply,
                Err(error) => {
                    if error.is_timeout() {
                        return ReserveReply::error(
                            StatusCode::GATEWAY_TIMEOUT,
                            "backend reservation timed out",
                        );
                    }
                    if error.is_connect() {
                        self.registry.set_healthy(&backend.name, false);
                        if attempt < self.config.retries.max_retries
                            && self
                                .config
                                .retries
                                .retry_on
                                .contains(&RetryOn::ConnectError)
                        {
                            excluded.insert(backend.name.clone());
                            sleep(self.config.retries.backoff).await;
                            continue;
                        }
                        return ReserveReply::error(
                            StatusCode::BAD_GATEWAY,
                            "failed to connect to reservation backend",
                        );
                    }
                    return ReserveReply::error(
                        StatusCode::BAD_GATEWAY,
                        "invalid or interrupted backend response",
                    );
                }
            };
            if status.is_redirection() {
                return ReserveReply::error(
                    StatusCode::BAD_GATEWAY,
                    "backend redirects are not supported",
                );
            }
            if !status.is_success() {
                return ReserveReply { status, body };
            }
            let Ok(response) = serde_json::from_value::<ReserveGasResponse>(body.clone()) else {
                return ReserveReply::error(
                    StatusCode::BAD_GATEWAY,
                    "invalid backend reservation response",
                );
            };
            if response.error.is_some() && response.result.is_none() {
                return ReserveReply { status, body };
            }
            let Some(result) = response
                .result
                .filter(|result| response.error.is_none() && !result.gas_coins.is_empty())
            else {
                return ReserveReply::error(
                    StatusCode::BAD_GATEWAY,
                    "invalid backend reservation response",
                );
            };
            let id = match self
                .storage
                .create_reservation(
                    Reservation {
                        backend: backend.name.clone(),
                        upstream_reservation_id: result.reservation_id,
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
            body["result"]["reservation_id"] = json!(id);
            return ReserveReply { status, body };
        }
        unreachable!("every attempt returns a response or consumes an available retry")
    }
}
