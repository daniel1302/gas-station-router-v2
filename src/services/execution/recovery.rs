use tokio::time::{Duration, sleep};

use super::*;

impl ExecutionService {
    pub(super) async fn recover(
        &self,
        request: &ExecuteTxRequest,
        digest: &TransactionDigest,
        token: ExecutionToken,
        backend: String,
    ) -> ExecutionResult {
        tracing::info!(backend = %backend, reservation_id = request.reservation_id,
            digest = %digest, "Recovering execution through IOTA RPC");
        let mut retry = false;
        loop {
            if retry {
                recovery_retry(&backend, digest, "recovery");
            }
            retry = true;
            if let Some(cached) = self.lookup(digest, &backend).await {
                self.storage
                    .complete_execution(
                        request.reservation_id,
                        token,
                        cached.clone(),
                        self.effects_ttl,
                    )
                    .await?;
                return self
                    .serve_cached(digest, cached, request.request_type.unwrap_or_default())
                    .await;
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    pub(super) async fn serve_cached(
        &self,
        digest: &TransactionDigest,
        mut cached: CachedTransaction,
        request_type: RequestType,
    ) -> ExecutionResult {
        if request_type == RequestType::WaitForEffectsCert || cached.local_execution_confirmed {
            return Ok(ExecutionReply::effects(&cached));
        }
        let mut retry = false;
        loop {
            if retry {
                recovery_retry(&cached.backend, digest, "local_execution");
            }
            retry = true;
            if let Some(found) = self.lookup(digest, &cached.backend).await {
                if validated_effects(&cached.effects, digest).as_ref() != Some(&found.effects) {
                    return Err(ExecutionReply::error(
                        StatusCode::BAD_GATEWAY,
                        "rpc_effects_conflict",
                        "RPC effects conflict with cached effects",
                    ));
                }
                // getTransactionBlock can read an external transaction store, so
                // presence alone is insufficient proof of this node's execution.
                if found.local_execution_confirmed || self.is_indexed(digest, &cached.backend).await
                {
                    cached.local_execution_confirmed = true;
                    self.storage
                        .put_transaction(digest, cached.clone(), self.effects_ttl)
                        .await?;
                    return Ok(ExecutionReply::effects(&cached));
                }
            }
            sleep(Duration::from_millis(200)).await;
        }
    }

    async fn lookup(&self, digest: &TransactionDigest, backend: &str) -> Option<CachedTransaction> {
        let body = self
            .rpc(
                "iota_getTransactionBlock",
                backend,
                digest,
                json!([digest, {"showEffects": true}]),
            )
            .await?;
        if body
            .get("digest")
            .and_then(Value::as_str)
            .and_then(|value| value.parse::<TransactionDigest>().ok())
            != Some(*digest)
            || body.get("errors").is_some_and(|value| {
                !value.is_null() && value.as_array().is_none_or(|errors| !errors.is_empty())
            })
        {
            rpc_failure(
                "iota_getTransactionBlock",
                backend,
                digest,
                "invalid_transaction_response",
                None,
                None,
            );
            return None;
        }
        let Some(effects) = body
            .get("effects")
            .and_then(|value| validated_effects(value, digest))
        else {
            rpc_failure(
                "iota_getTransactionBlock",
                backend,
                digest,
                "invalid_effects",
                None,
                None,
            );
            return None;
        };
        Some(CachedTransaction {
            effects,
            local_execution_confirmed: body.get("confirmedLocalExecution").and_then(Value::as_bool)
                == Some(true),
            backend: backend.to_owned(),
        })
    }

    async fn is_indexed(&self, digest: &TransactionDigest, backend: &str) -> bool {
        match self
            .rpc(
                "iota_isTransactionIndexedOnNode",
                backend,
                digest,
                json!([digest]),
            )
            .await
        {
            Some(Value::Bool(confirmed)) => confirmed,
            Some(_) => {
                rpc_failure(
                    "iota_isTransactionIndexedOnNode",
                    backend,
                    digest,
                    "invalid_response",
                    None,
                    None,
                );
                false
            }
            None => false,
        }
    }

    async fn rpc(
        &self,
        method: &str,
        backend: &str,
        digest: &TransactionDigest,
        params: Value,
    ) -> Option<Value> {
        // This client never carries the router or backend Authorization header.
        let response = match self
            .rpc_client
            .post(self.rpc_url.clone())
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                rpc_failure(
                    method,
                    backend,
                    digest,
                    crate::backend::health::HealthCheckOutcome::from_error(&error)
                        .log_fields()
                        .0,
                    None,
                    None,
                );
                return None;
            }
        };
        let status = Some(response.status().as_u16());
        if !response.status().is_success() {
            rpc_failure(method, backend, digest, "http_error", status, None);
            return None;
        }
        let body: Value = match response.json().await {
            Ok(body) => body,
            Err(error) => {
                rpc_failure(
                    method,
                    backend,
                    digest,
                    if error.is_decode() {
                        "invalid_response"
                    } else {
                        crate::backend::health::HealthCheckOutcome::from_error(&error)
                            .log_fields()
                            .0
                    },
                    status,
                    None,
                );
                return None;
            }
        };
        if body.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || body.get("id") != Some(&json!(1))
        {
            rpc_failure(method, backend, digest, "invalid_response", status, None);
            return None;
        }
        if let Some(error) = body.get("error").filter(|error| !error.is_null()) {
            rpc_failure(
                method,
                backend,
                digest,
                "rpc_error",
                status,
                error.get("code").and_then(Value::as_i64),
            );
            return None;
        }
        match body.get("result") {
            Some(value) if !value.is_null() => Some(value.clone()),
            result => {
                rpc_failure(
                    method,
                    backend,
                    digest,
                    if result.is_some() {
                        "transaction_not_found"
                    } else {
                        "invalid_response"
                    },
                    status,
                    None,
                );
                None
            }
        }
    }
}

fn recovery_retry(backend: &str, digest: &TransactionDigest, stage: &'static str) {
    metrics::inc_execute_tx_recovery_retry(backend, stage);
    tracing::warn!(backend, digest = %digest, stage, "Retrying execution RPC lookup");
}

fn rpc_failure(
    method: &str,
    backend: &str,
    digest: &TransactionDigest,
    reason: &'static str,
    http_status: Option<u16>,
    rpc_code: Option<i64>,
) {
    tracing::warn!(
        backend,
        digest = %digest,
        method,
        reason,
        http_status,
        rpc_code,
        next_action = "retry_rpc_lookup",
        "IOTA RPC execution lookup failed"
    );
}
