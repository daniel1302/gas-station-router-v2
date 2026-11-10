//! Shared storage with atomic Lua transitions and bounded retention. Each data key
//! expires; only the single reservation ID counter is persistent.

use std::{io, time::Duration};

use async_trait::async_trait;
use iota_sdk_types::TransactionDigest;
use redis::{
    FromRedisValue, Script,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::time::timeout;

use super::{
    CachedTransaction, ExecutionClaim, ExecutionRecord, ExecutionToken, Reservation, ReservationId,
    Storage, StorageError, StorageResult, TransactionWriteOutcome,
};
use crate::router::metrics;

mod scripts;

const COMMAND_TIMEOUT: Duration = Duration::from_secs(2);
// Fits Redis expiration timestamps and Lua's exact integer range, even after TIME
// is added. Very large durations saturate here instead of creating immortal keys.
const MAX_TTL_MS: u64 = 1 << 52;

#[derive(Clone)]
pub struct RedisStorage {
    connection: ConnectionManager,
    prefix: String,
}

#[derive(Deserialize)]
struct StoredTransaction {
    effects: String,
    backend: String,
    local_execution_confirmed: bool,
}

impl RedisStorage {
    /// Connect to standalone Redis. A shared prefix identifies one router fleet.
    /// An evicting Redis policy can lose execution records before their TTL.
    pub async fn new(url: &str, key_prefix: &str) -> StorageResult<Self> {
        if key_prefix.trim().is_empty() {
            return Err(backend_error("connect", "empty_key_prefix"));
        }
        let client =
            redis::Client::open(url).map_err(|_| backend_error("connect", "invalid_url"))?;
        let options = ConnectionManagerConfig::new()
            .set_connection_timeout(Some(COMMAND_TIMEOUT))
            .set_response_timeout(Some(COMMAND_TIMEOUT))
            .set_number_of_retries(1);
        let connection = timeout(
            COMMAND_TIMEOUT,
            ConnectionManager::new_with_config(client, options),
        )
        .await
        .map_err(|_| backend_error("connect", "timeout"))?
        .map_err(|error| redis_error("connect", error))?;
        let storage = Self {
            connection,
            prefix: format!("{key_prefix}:v2"),
        };
        if let Ok(info) = storage
            .command::<String>("connect", redis::cmd("INFO").arg("memory"))
            .await
        {
            if !info
                .lines()
                .any(|line| line.trim() == "maxmemory_policy:noeviction")
            {
                tracing::warn!(
                    "Redis eviction may remove execution records before their TTL and permit resubmission"
                );
            }
        }
        Ok(storage)
    }

    fn key(&self, kind: &str, id: impl std::fmt::Display) -> String {
        format!("{}:{kind}:{id}", self.prefix)
    }

    async fn command<T: FromRedisValue>(
        &self,
        method: &'static str,
        command: &redis::Cmd,
    ) -> StorageResult<T> {
        let mut connection = self.connection.clone();
        timeout(COMMAND_TIMEOUT, command.query_async(&mut connection))
            .await
            .map_err(|_| backend_error(method, "timeout"))?
            .map_err(|error| redis_error(method, error))
    }

    async fn script<T: FromRedisValue>(
        &self,
        method: &'static str,
        script: &Script,
        keys: &[String],
        args: &[String],
    ) -> StorageResult<T> {
        let mut invocation = script.prepare_invoke();
        for key in keys {
            invocation.key(key);
        }
        invocation.arg(args);
        let mut connection = self.connection.clone();
        timeout(COMMAND_TIMEOUT, invocation.invoke_async(&mut connection))
            .await
            .map_err(|_| backend_error(method, "timeout"))?
            .map_err(|error| redis_error(method, error))
    }

    async fn read<T: DeserializeOwned>(
        &self,
        method: &'static str,
        key: String,
    ) -> StorageResult<Option<T>> {
        let value: Option<String> = self.script(method, &scripts::READ, &[key], &[]).await?;
        value
            .map(|value| {
                serde_json::from_str(&value).map_err(|_| backend_error(method, "invalid_record"))
            })
            .transpose()
    }
}

#[async_trait]
impl Storage for RedisStorage {
    async fn create_reservation(
        &self,
        reservation: Reservation,
        ttl: Duration,
    ) -> StorageResult<ReservationId> {
        const METHOD: &str = "create_reservation";
        metrics::inc_storage_call(METHOD);
        let ttl = ttl_ms(ttl)?;
        let value = encode(METHOD, &reservation)?;
        let id: Option<String> = self
            .script(
                METHOD,
                &scripts::NEXT_ID,
                &[format!("{}:reservation_sequence", self.prefix)],
                &[],
            )
            .await?;
        let id = id
            .ok_or_else(|| {
                metrics::inc_storage_error("id_exhausted");
                StorageError::IdExhausted
            })?
            .parse::<u64>()
            .map_err(|_| backend_error(METHOD, "invalid_sequence"))?;
        let written: Option<String> = self
            .script(
                METHOD,
                &scripts::CREATE_RESERVATION,
                &[self.key("reservation", id)],
                &[value, ttl],
            )
            .await?;
        if written.is_none() {
            return Err(conflict());
        }
        Ok(id)
    }

    async fn get_reservation(&self, id: ReservationId) -> StorageResult<Option<Reservation>> {
        metrics::inc_storage_call("get_reservation");
        self.read("get_reservation", self.key("reservation", id))
            .await
    }

    async fn put_transaction(
        &self,
        digest: &TransactionDigest,
        transaction: CachedTransaction,
        ttl: Duration,
    ) -> StorageResult<TransactionWriteOutcome> {
        const METHOD: &str = "put_transaction";
        metrics::inc_storage_call(METHOD);
        let ttl = ttl_ms(ttl)?;
        let result: i64 = self
            .script(
                METHOD,
                &scripts::PUT_TRANSACTION,
                &[self.key("transaction", digest)],
                &[
                    transaction.backend.clone(),
                    encode(METHOD, &transaction.effects)?,
                    confirmed(transaction.local_execution_confirmed),
                    ttl,
                ],
            )
            .await?;
        write_result(result, &transaction.backend)
    }

    async fn get_transaction(
        &self,
        digest: &TransactionDigest,
    ) -> StorageResult<Option<CachedTransaction>> {
        const METHOD: &str = "get_transaction";
        metrics::inc_storage_call(METHOD);
        let transaction: Option<StoredTransaction> =
            self.read(METHOD, self.key("transaction", digest)).await?;
        transaction
            .map(|transaction| {
                Ok(CachedTransaction {
                    effects: serde_json::from_str(&transaction.effects)
                        .map_err(|_| backend_error(METHOD, "invalid_effects"))?,
                    backend: transaction.backend,
                    local_execution_confirmed: transaction.local_execution_confirmed,
                })
            })
            .transpose()
    }

    async fn health_check(&self) -> StorageResult<()> {
        metrics::inc_storage_call("health_check");
        let result = self
            .command::<String>("health_check", &redis::cmd("PING"))
            .await;
        metrics::inc_storage_health(result.is_ok());
        result.map(|_| ())
    }

    async fn try_claim_execution(
        &self,
        id: ReservationId,
        digest: &TransactionDigest,
        token: ExecutionToken,
        lease_ttl: Duration,
        record_ttl: Duration,
    ) -> StorageResult<ExecutionClaim> {
        const METHOD: &str = "try_claim_execution";
        metrics::inc_storage_call(METHOD);
        let lease_ttl = ttl_ms(lease_ttl)?;
        let record_ttl = ttl_ms(record_ttl)?;
        let (result, created): (i64, i64) = self
            .script(
                METHOD,
                &scripts::TRY_CLAIM_EXECUTION,
                &[self.key("execution", id), self.key("reservation", id)],
                &[digest.to_string(), token.to_string(), lease_ttl, record_ttl],
            )
            .await?;
        if created == 1 {
            metrics::inc_storage_execution_phase("claimed");
        }
        claim_result(result)
    }

    async fn get_execution(&self, id: ReservationId) -> StorageResult<Option<ExecutionRecord>> {
        metrics::inc_storage_call("get_execution");
        self.read("get_execution", self.key("execution", id)).await
    }

    async fn try_claim_recovery(
        &self,
        id: ReservationId,
        digest: &TransactionDigest,
        token: ExecutionToken,
        lease_ttl: Duration,
    ) -> StorageResult<ExecutionClaim> {
        const METHOD: &str = "try_claim_recovery";
        metrics::inc_storage_call(METHOD);
        let (result, _): (i64, i64) = self
            .script(
                METHOD,
                &scripts::TRY_CLAIM_RECOVERY,
                &[self.key("execution", id)],
                &[digest.to_string(), token.to_string(), ttl_ms(lease_ttl)?],
            )
            .await?;
        claim_result(result)
    }

    async fn mark_execution_dispatching(
        &self,
        id: ReservationId,
        token: ExecutionToken,
    ) -> StorageResult<()> {
        const METHOD: &str = "mark_execution_dispatching";
        metrics::inc_storage_call(METHOD);
        let result: i64 = self
            .script(
                METHOD,
                &scripts::MARK_EXECUTION_DISPATCHING,
                &[self.key("execution", id)],
                &[token.to_string()],
            )
            .await?;
        if result != 1 {
            return Err(conflict());
        }
        metrics::inc_storage_execution_phase("dispatching");
        Ok(())
    }

    async fn complete_execution(
        &self,
        id: ReservationId,
        token: ExecutionToken,
        transaction: CachedTransaction,
        ttl: Duration,
    ) -> StorageResult<TransactionWriteOutcome> {
        const METHOD: &str = "complete_execution";
        metrics::inc_storage_call(METHOD);
        let ttl = ttl_ms(ttl)?;
        // Read only to address the cache key. The script rechecks digest, ownership
        // and phase atomically before modifying either record.
        let record: ExecutionRecord = self
            .read(METHOD, self.key("execution", id))
            .await?
            .ok_or_else(conflict)?;
        let (result, transitioned): (i64, i64) = self
            .script(
                METHOD,
                &scripts::COMPLETE_EXECUTION,
                &[
                    self.key("execution", id),
                    self.key("transaction", record.digest),
                ],
                &[
                    record.digest.to_string(),
                    token.to_string(),
                    transaction.backend.clone(),
                    encode(METHOD, &transaction.effects)?,
                    confirmed(transaction.local_execution_confirmed),
                    ttl,
                ],
            )
            .await?;
        let outcome = write_result(result, &transaction.backend)?;
        if transitioned == 1 {
            metrics::inc_storage_execution_phase("completed");
        }
        Ok(outcome)
    }

    async fn cancel_execution(
        &self,
        id: ReservationId,
        token: ExecutionToken,
    ) -> StorageResult<()> {
        const METHOD: &str = "cancel_execution";
        metrics::inc_storage_call(METHOD);
        let result: i64 = self
            .script(
                METHOD,
                &scripts::CANCEL_EXECUTION,
                &[self.key("execution", id)],
                &[token.to_string()],
            )
            .await?;
        if result != 1 {
            return Err(conflict());
        }
        Ok(())
    }
}

fn encode(method: &'static str, value: &impl Serialize) -> StorageResult<String> {
    serde_json::to_string(value).map_err(|_| backend_error(method, "serialization_failed"))
}

fn confirmed(value: bool) -> String {
    if value { "1" } else { "0" }.into()
}

fn ttl_ms(ttl: Duration) -> StorageResult<String> {
    if ttl.is_zero() {
        metrics::inc_storage_error("invalid_ttl");
        return Err(StorageError::InvalidTtl);
    }
    Ok(ttl
        .as_nanos()
        .div_ceil(1_000_000)
        .min(u128::from(MAX_TTL_MS))
        .to_string())
}

fn conflict() -> StorageError {
    metrics::inc_storage_error("conflict");
    StorageError::Conflict
}

fn claim_result(value: i64) -> StorageResult<ExecutionClaim> {
    let (claim, label) = match value {
        1 => (ExecutionClaim::Acquired, "acquired"),
        2 => (ExecutionClaim::InFlight, "in_flight"),
        3 => (ExecutionClaim::PossiblySubmitted, "possibly_submitted"),
        4 => (ExecutionClaim::Completed, "completed"),
        5 => (ExecutionClaim::Conflict, "conflict"),
        -1 => return Err(conflict()),
        _ => return Err(backend_error("claim", "invalid_result")),
    };
    metrics::inc_storage_execution_claim(label);
    Ok(claim)
}

fn write_result(value: i64, backend: &str) -> StorageResult<TransactionWriteOutcome> {
    match value {
        0 => Err(conflict()),
        1 => {
            metrics::inc_storage_cached_transactions(backend);
            Ok(TransactionWriteOutcome::Inserted)
        }
        2 => Ok(TransactionWriteOutcome::Upgraded),
        3 => Ok(TransactionWriteOutcome::Unchanged),
        _ => Err(backend_error("write_transaction", "invalid_result")),
    }
}

fn redis_error(method: &'static str, error: redis::RedisError) -> StorageError {
    // Error kinds are bounded; messages can contain credentials or stored data.
    let reason = match error.code() {
        Some("OOM") => "out_of_memory",
        Some("NOPERM" | "NOAUTH" | "WRONGPASS") => "authorization_failed",
        Some("READONLY") => "read_only",
        Some("WRONGTYPE") => "invalid_record_type",
        _ if error.is_timeout() => "timeout",
        _ if error
            .detail()
            .is_some_and(|detail| detail.contains("router record is missing its TTL")) =>
        {
            "missing_ttl"
        }
        _ => "command_failed",
    };
    metrics::inc_storage_error("backend");
    tracing::warn!(storage = "redis", method, reason, error_kind = ?error.kind(), "Redis command failed");
    StorageError::Backend(io::Error::other(format!("Redis storage: {reason}")).into())
}

fn backend_error(method: &'static str, reason: &'static str) -> StorageError {
    metrics::inc_storage_error("backend");
    tracing::warn!(
        storage = "redis",
        method,
        reason,
        "Redis storage operation failed"
    );
    StorageError::Backend(io::Error::other(format!("Redis storage: {reason}")).into())
}
