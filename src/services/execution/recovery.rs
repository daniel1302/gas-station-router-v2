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
        loop {
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
        loop {
            if let Some(found) = self.lookup(digest, &cached.backend).await {
                if validated_effects(&cached.effects, digest).as_ref() != Some(&found.effects) {
                    return Err(ExecutionReply::error(
                        StatusCode::BAD_GATEWAY,
                        "RPC effects conflict with cached effects",
                    ));
                }
                // getTransactionBlock can read an external transaction store, so
                // presence alone is insufficient proof of this node's execution.
                if found.local_execution_confirmed || self.is_indexed(digest).await {
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
                json!([digest, {"showEffects": true}]),
            )
            .await?;
        if body
            .get("digest")?
            .as_str()?
            .parse::<TransactionDigest>()
            .ok()?
            != *digest
            || body.get("errors").is_some_and(|value| {
                !value.is_null() && value.as_array().is_none_or(|errors| !errors.is_empty())
            })
        {
            return None;
        }
        Some(CachedTransaction {
            effects: validated_effects(body.get("effects")?, digest)?,
            local_execution_confirmed: body.get("confirmedLocalExecution").and_then(Value::as_bool)
                == Some(true),
            backend: backend.to_owned(),
        })
    }

    async fn is_indexed(&self, digest: &TransactionDigest) -> bool {
        self.rpc("iota_isTransactionIndexedOnNode", json!([digest]))
            .await
            == Some(Value::Bool(true))
    }

    async fn rpc(&self, method: &str, params: Value) -> Option<Value> {
        // This client never carries the router or backend Authorization header.
        let response = self
            .rpc_client
            .post(self.rpc_url.clone())
            .json(&json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let body: Value = response.json().await.ok()?;
        if body.get("jsonrpc")?.as_str()? != "2.0"
            || body.get("id")? != 1
            || body.get("error").is_some_and(|error| !error.is_null())
        {
            return None;
        }
        body.get("result").filter(|value| !value.is_null()).cloned()
    }
}
