use tokio::time::{Duration, sleep};

use super::*;
use crate::router::storage::{ExecutionClaim, ExecutionPhase};

impl ExecutionService {
    pub(super) async fn drive(
        &self,
        request: &ExecuteTxRequest,
        digest: &TransactionDigest,
        token: ExecutionToken,
    ) -> ExecutionResult {
        loop {
            if let Some(record) = self.storage.get_execution(request.reservation_id).await? {
                if record.digest != *digest {
                    return Err(ExecutionReply::error(
                        StatusCode::CONFLICT,
                        "digest_conflict",
                        "reservation already belongs to another transaction",
                    ));
                }
                if record.phase == ExecutionPhase::Completed
                    && let Some(cached) = self.storage.get_transaction(digest).await?
                {
                    return self
                        .serve_cached(digest, cached, request.request_type.unwrap_or_default())
                        .await;
                }
                if matches!(
                    record.phase,
                    ExecutionPhase::Dispatching | ExecutionPhase::Completed
                ) {
                    match self
                        .storage
                        .try_claim_recovery(
                            request.reservation_id,
                            digest,
                            token,
                            self.config.inflight.lease_ttl,
                        )
                        .await?
                    {
                        ExecutionClaim::Acquired => {
                            let backend = match record.backend {
                                Some(backend) => backend,
                                None => {
                                    self.storage
                                        .get_reservation(request.reservation_id)
                                        .await?
                                        .ok_or_else(|| {
                                            ExecutionReply::error(
                                                StatusCode::SERVICE_UNAVAILABLE,
                                                "backend_metadata_missing",
                                                "recovery backend metadata unavailable",
                                            )
                                        })?
                                        .backend
                                }
                            };
                            return self.recover(request, digest, token, backend).await;
                        }
                        ExecutionClaim::Conflict => return Err(StorageError::Conflict.into()),
                        _ => {
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    }
                }
            }
            match self
                .storage
                .try_claim_execution(
                    request.reservation_id,
                    digest,
                    token,
                    self.config.inflight.lease_ttl,
                    self.config.inflight.record_ttl,
                )
                .await?
            {
                ExecutionClaim::Acquired => return self.dispatch(request, digest, token).await,
                ExecutionClaim::Conflict => return Err(StorageError::Conflict.into()),
                ExecutionClaim::InFlight => sleep(Duration::from_millis(100)).await,
                ExecutionClaim::PossiblySubmitted | ExecutionClaim::Completed => {}
            }
        }
    }
}
