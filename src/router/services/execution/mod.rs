//! Reservation-bound execution. Dispatch is persisted before any upstream POST;
//! an uncertain result can only be recovered through read-only IOTA RPC calls.

use std::{sync::Arc, time::Duration};

use anyhow::{Result, ensure};
use axum::http::StatusCode;
use iota_sdk_types::{ObjectDigest, ObjectId, Transaction, TransactionDigest, UserSignature};
use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::time::timeout;
use tracing::Instrument;

use crate::router::{
    backend::registry::BackendRegistry,
    config::{ExecuteTxConfig, IotaConfig, TtlConfig},
    metrics,
    storage::{CachedTransaction, ExecutionToken, Storage, StorageError},
};

mod executor;
mod inflight;
mod recovery;

#[derive(Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum RequestType {
    #[default]
    WaitForEffectsCert,
    WaitForLocalExecution,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecuteTxRequest {
    pub reservation_id: u64,
    pub tx_bytes: String,
    pub user_sig: String,
    #[serde(default, alias = "requestType")]
    pub request_type: Option<RequestType>,
}

#[derive(Clone)]
pub struct ExecutionReply {
    pub status: StatusCode,
    pub body: Value,
    error_reason: Option<&'static str>,
}

impl ExecutionReply {
    pub(crate) fn error(status: StatusCode, reason: &'static str, message: &str) -> Self {
        Self {
            status,
            body: json!({"effects": null, "error": message}),
            error_reason: Some(reason),
        }
    }

    fn effects(transaction: &CachedTransaction) -> Self {
        Self {
            status: StatusCode::OK,
            body: json!({"effects": transaction.effects, "error": null,
                "confirmedLocalExecution": transaction.local_execution_confirmed}),
            error_reason: (transaction.effects["status"]["status"] == "failure")
                .then_some("transaction_failed"),
        }
    }

    /// Count the final success or error once, including finalized on-chain failures
    /// returned with HTTP 200. Intermediate failures are logged at their source.
    pub(crate) fn observed(
        self,
        reservation_id: Option<u64>,
        digest: Option<&TransactionDigest>,
    ) -> Self {
        if let Some(reason) = self.error_reason {
            metrics::inc_execute_tx_error(reason);
            tracing::warn!(
                reason,
                http_status = self.status.as_u16(),
                reservation_id,
                digest = digest.map(ToString::to_string),
                "Execution request failed"
            );
        } else {
            metrics::inc_execute_tx_success();
        }
        self
    }
}

impl From<StorageError> for ExecutionReply {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Conflict => Self::error(
                StatusCode::CONFLICT,
                "storage_conflict",
                "execution ownership or cached effects conflict; retry the same reservation and transaction",
            ),
            _ => Self::error(
                StatusCode::SERVICE_UNAVAILABLE,
                "storage_unavailable",
                "execution storage unavailable",
            ),
        }
    }
}

type ExecutionResult = Result<ExecutionReply, ExecutionReply>;

pub struct ExecutionService {
    registry: Arc<BackendRegistry>,
    storage: Arc<dyn Storage>,
    backend_client: Client,
    rpc_client: Client,
    rpc_url: Url,
    config: ExecuteTxConfig,
    effects_ttl: Duration,
}

impl ExecutionService {
    pub fn new(
        registry: Arc<BackendRegistry>,
        storage: Arc<dyn Storage>,
        config: ExecuteTxConfig,
        iota: IotaConfig,
        ttl: TtlConfig,
    ) -> Result<Self> {
        ensure!(
            !config.timeout.is_zero(),
            "routing.execute_tx.timeout must be greater than zero"
        );
        ensure!(
            config.inflight.lease_ttl > config.timeout,
            "routing.execute_tx.inflight.lease_ttl must exceed execute_tx.timeout"
        );
        ensure!(
            config.inflight.record_ttl > config.inflight.lease_ttl,
            "routing.execute_tx.inflight.record_ttl must exceed lease_ttl"
        );
        ensure!(
            !ttl.transaction_effects.is_zero(),
            "routing.ttl.transaction_effects must be greater than zero"
        );
        ensure!(
            config.inflight.record_ttl >= ttl.reservation,
            "routing.execute_tx.inflight.record_ttl must be at least routing.ttl.reservation"
        );
        ensure!(
            !iota.timeout.is_zero(),
            "iota.timeout must be greater than zero"
        );
        let rpc_url =
            Url::parse(&iota.rpc_url).map_err(|_| anyhow::anyhow!("invalid iota.rpc_url"))?;
        ensure!(
            matches!(rpc_url.scheme(), "http" | "https")
                && rpc_url.host_str().is_some()
                && rpc_url.username().is_empty()
                && rpc_url.password().is_none()
                && rpc_url.fragment().is_none(),
            "iota.rpc_url must be an HTTP(S) URL without embedded credentials or a fragment"
        );
        let client = |duration| {
            Client::builder()
                .timeout(duration)
                .connect_timeout(duration)
                .redirect(reqwest::redirect::Policy::none())
                .retry(reqwest::retry::never())
                .no_proxy()
                .build()
        };
        Ok(Self {
            registry,
            storage,
            backend_client: client(config.timeout)?,
            rpc_client: client(iota.timeout)?,
            rpc_url,
            config,
            effects_ttl: ttl.transaction_effects,
        })
    }

    /// Executes once or reads/reconstructs a previous result. Client cancellation
    /// does not cancel the owned task. The overall timeout bounds waiting and RPC
    /// recovery; an unknown result never authorizes a second upstream submission.
    pub async fn execute(self: &Arc<Self>, request: ExecuteTxRequest) -> ExecutionReply {
        let reservation_id = request.reservation_id;
        let Ok(transaction) = Transaction::from_base64(&request.tx_bytes) else {
            return ExecutionReply::error(
                StatusCode::BAD_REQUEST,
                "invalid_transaction",
                "invalid transaction BCS bytes",
            )
            .observed(Some(reservation_id), None);
        };
        if UserSignature::from_base64(&request.user_sig).is_err() {
            return ExecutionReply::error(
                StatusCode::BAD_REQUEST,
                "invalid_signature",
                "invalid user signature encoding",
            )
            .observed(Some(reservation_id), None);
        }
        let digest = transaction.digest();
        let service = self.clone();
        let span = tracing::info_span!("execution", reservation_id, digest = %digest);
        match tokio::spawn(async move {
            let reply = match timeout(service.config.timeout, service.drive(&request, &digest, ExecutionToken::new_v4())).await {
                Ok(Ok(reply) | Err(reply)) => reply,
                Err(_) => ExecutionReply::error(StatusCode::GATEWAY_TIMEOUT, "timeout",
                    "execution result still unknown or local execution unconfirmed; retry the same reservation and transaction"),
            };
            // Observe inside the owned task so client cancellation cannot hide errors.
            reply.observed(Some(reservation_id), Some(&digest))
        }.instrument(span)).await {
            Ok(reply) => reply,
            Err(_) => ExecutionReply::error(StatusCode::INTERNAL_SERVER_ERROR, "task_failed", "execution task failed; retry the same reservation and transaction")
                .observed(Some(reservation_id), Some(&digest)),
        }
    }
}

fn validated_effects(value: &Value, digest: &TransactionDigest) -> Option<Value> {
    let mut effects = value.as_object()?.clone();
    if effects.get("messageVersion")?.as_str()? != "v1"
        || effects
            .get("executedEpoch")?
            .as_str()?
            .parse::<u64>()
            .is_err()
        || !effects.get("gasObject")?.is_object()
    {
        return None;
    }
    for field in [
        "computationCost",
        "storageCost",
        "storageRebate",
        "nonRefundableStorageFee",
    ] {
        effects
            .get("gasUsed")?
            .get(field)?
            .as_str()?
            .parse::<u64>()
            .ok()?;
    }
    let gas_object = effects.get("gasObject")?;
    let reference = gas_object.get("reference")?;
    reference
        .get("objectId")?
        .as_str()?
        .parse::<ObjectId>()
        .ok()?;
    reference
        .get("digest")?
        .as_str()?
        .parse::<ObjectDigest>()
        .ok()?;
    let version = reference.get("version")?;
    version
        .as_u64()
        .or_else(|| version.as_str()?.parse::<u64>().ok())?;
    let owner = gas_object.get("owner")?;
    if !owner.is_object() && owner.as_str() != Some("Immutable") {
        return None;
    }
    if effects
        .get("transactionDigest")?
        .as_str()?
        .parse::<TransactionDigest>()
        .ok()?
        != *digest
        || !matches!(
            effects.get("status")?.get("status")?.as_str()?,
            "success" | "failure"
        )
    {
        return None;
    }
    // Gas Station and JSON-RPC omit different empty optional fields. Normalize
    // those fields before comparing effects during recovery/cache upgrades.
    for field in [
        "modifiedAtVersions",
        "sharedObjects",
        "created",
        "mutated",
        "unwrapped",
        "deleted",
        "unwrappedThenDeleted",
        "wrapped",
        "dependencies",
    ] {
        if effects
            .get(field)
            .is_some_and(|value| value.as_array().is_some_and(Vec::is_empty))
        {
            effects.remove(field);
        }
    }
    if effects.get("eventsDigest").is_some_and(Value::is_null) {
        effects.remove("eventsDigest");
    }
    // The Gas Station uses numeric object-reference versions while JSON-RPC
    // may render them as strings. Canonicalize this representation as well.
    let mut effects = Value::Object(effects);
    normalize_versions(&mut effects);
    if effects["gasUsed"]["computationCostBurned"] == "0" {
        effects["gasUsed"]
            .as_object_mut()?
            .remove("computationCostBurned");
    }
    Some(effects)
}

fn normalize_versions(value: &mut Value) {
    match value {
        Value::Object(object) => {
            for (key, value) in object {
                if matches!(key.as_str(), "version" | "initial_shared_version")
                    && let Some(version) =
                        value.as_str().and_then(|value| value.parse::<u64>().ok())
                {
                    *value = json!(version);
                } else {
                    normalize_versions(value);
                }
            }
        }
        Value::Array(values) => values.iter_mut().for_each(normalize_versions),
        _ => {}
    }
}
