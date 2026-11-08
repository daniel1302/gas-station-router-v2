use super::*;

impl ExecutionService {
    pub(super) async fn dispatch(
        &self,
        request: &ExecuteTxRequest,
        digest: &TransactionDigest,
        token: ExecutionToken,
    ) -> ExecutionResult {
        let reservation = match self.storage.get_reservation(request.reservation_id).await {
            Ok(Some(reservation)) => reservation,
            result => {
                self.storage
                    .cancel_execution(request.reservation_id, token)
                    .await?;
                return Err(match result {
                    Ok(None) => ExecutionReply::error(
                        StatusCode::NOT_FOUND,
                        "reservation not found or expired",
                    ),
                    Err(error) => error.into(),
                    _ => unreachable!(),
                });
            }
        };
        let Some(backend) = self.registry.get(&reservation.backend) else {
            self.storage
                .cancel_execution(request.reservation_id, token)
                .await?;
            return Err(ExecutionReply::error(
                StatusCode::SERVICE_UNAVAILABLE,
                "original reservation backend is not configured",
            ));
        };
        let cached = self.storage.get_transaction(digest).await?;
        if let Some(cached) = &cached
            && cached.backend != reservation.backend
        {
            self.storage
                .cancel_execution(request.reservation_id, token)
                .await?;
            return Err(StorageError::Conflict.into());
        }
        let mut upstream = request.clone();
        upstream.reservation_id = reservation.upstream_reservation_id;
        upstream.request_type = Some(request.request_type.unwrap_or_default());
        let http_request = backend
            .execute_request(&self.backend_client)
            .json(&upstream)
            .build()
            .map_err(|_| {
                ExecutionReply::error(
                    StatusCode::BAD_GATEWAY,
                    "cannot build backend execution request",
                )
            })?;
        // This durable transition must succeed before the request can leave the router.
        self.storage
            .mark_execution_dispatching(request.reservation_id, token)
            .await?;
        if let Some(cached) = cached {
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
        let response = self.backend_client.execute(http_request).await;
        if let Ok(response) = response
            && response.status().is_success()
            && let Ok(body) = response.json::<Value>().await
            && body.get("error").is_some_and(Value::is_null)
            && let Some(effects) = body
                .get("effects")
                .and_then(|value| validated_effects(value, digest))
        {
            // The Gas Station response does not prove local execution, even when
            // waitForLocalExecution was requested. Confirm independently via RPC.
            let cached = CachedTransaction {
                effects,
                local_execution_confirmed: false,
                backend: reservation.backend,
            };
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
        // Timeouts, redirects, errors and malformed replies are all uncertain after
        // Dispatching. Only RPC lookup is permitted from here, never another POST.
        self.recover(request, digest, token, reservation.backend)
            .await
    }
}
