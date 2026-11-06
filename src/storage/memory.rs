use std::{
    collections::{HashMap, hash_map::Entry},
    hash::Hash,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use crate::metrics;
use async_trait::async_trait;
use iota_sdk_types::TransactionDigest;
use tokio::{sync::Mutex, time::Instant};

use super::{
    CachedTransaction, ExecutionClaim, ExecutionPhase, ExecutionRecord, ExecutionToken,
    Reservation, ReservationId, Storage, StorageError, StorageResult, TransactionWriteOutcome,
};

// Do not reuse IDs between independent stores within the same process.
static RESERVATION_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Process-local storage. Clones share all entries and execution leases.
///
/// Data is lost on restart. Independent processes do not share IDs or leases;
/// use a shared persistent backend when running multiple router processes.
/// Expired entries are removed on access and swept on writes; no worker is needed.
#[derive(Clone, Default)]
pub struct MemoryStorage {
    state: Arc<Mutex<State>>,
}

#[derive(Default)]
struct State {
    reservations: HashMap<ReservationId, Expiring<Reservation>>,
    transactions: HashMap<TransactionDigest, Expiring<CachedTransaction>>,
    executions: HashMap<ReservationId, Expiring<Execution>>,
}

struct Expiring<T> {
    value: T,
    created_at: Instant,
    ttl: Duration,
}

impl<T> Expiring<T> {
    fn new(value: T, now: Instant, ttl: Duration) -> Self {
        Self {
            value,
            created_at: now,
            ttl,
        }
    }

    fn expired(&self, now: Instant) -> bool {
        // Comparing elapsed durations also supports TTLs too large for Instant + ttl.
        now.duration_since(self.created_at) >= self.ttl
    }
}

struct Execution {
    record: ExecutionRecord,
    token: ExecutionToken,
    lease_started_at: Instant,
    lease_ttl: Duration,
}

impl Execution {
    fn lease_valid(&self, now: Instant) -> bool {
        now.duration_since(self.lease_started_at) < self.lease_ttl
    }
}

impl State {
    fn purge_expired(&mut self, now: Instant) {
        self.reservations.retain(|_, entry| !entry.expired(now));
        self.transactions.retain(|_, entry| !entry.expired(now));
        self.executions.retain(|_, entry| !entry.expired(now));
    }
}

impl MemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Storage for MemoryStorage {
    async fn create_reservation(
        &self,
        reservation: Reservation,
        ttl: Duration,
    ) -> StorageResult<ReservationId> {
        metrics::inc_storage_call("create_reservation");
        validate_ttl(ttl)?;
        let mut state = self.state.lock().await;
        let now = Instant::now();
        state.purge_expired(now);
        let id = next_reservation_id(&RESERVATION_SEQUENCE)?;
        state
            .reservations
            .insert(id, Expiring::new(reservation, now, ttl));
        Ok(id)
    }

    async fn get_reservation(&self, id: ReservationId) -> StorageResult<Option<Reservation>> {
        metrics::inc_storage_call("get_reservation");
        let mut state = self.state.lock().await;
        Ok(live_entry(&mut state.reservations, &id, Instant::now())
            .map(|entry| entry.value.clone()))
    }

    async fn put_transaction(
        &self,
        digest: &TransactionDigest,
        transaction: CachedTransaction,
        ttl: Duration,
    ) -> StorageResult<TransactionWriteOutcome> {
        metrics::inc_storage_call("put_transaction");
        validate_ttl(ttl)?;
        let mut state = self.state.lock().await;
        let now = Instant::now();
        state.purge_expired(now);
        write_transaction(&mut state.transactions, *digest, transaction, ttl, now)
    }

    async fn get_transaction(
        &self,
        digest: &TransactionDigest,
    ) -> StorageResult<Option<CachedTransaction>> {
        metrics::inc_storage_call("get_transaction");
        let mut state = self.state.lock().await;
        Ok(live_entry(&mut state.transactions, digest, Instant::now())
            .map(|entry| entry.value.clone()))
    }

    async fn health_check(&self) -> StorageResult<()> {
        metrics::inc_storage_call("health_check");
        metrics::inc_storage_health(true);
        Ok(())
    }

    async fn try_claim_execution(
        &self,
        reservation_id: ReservationId,
        digest: &TransactionDigest,
        token: ExecutionToken,
        lease_ttl: Duration,
        record_ttl: Duration,
    ) -> StorageResult<ExecutionClaim> {
        metrics::inc_storage_call("try_claim_execution");
        validate_ttl(lease_ttl)?;
        validate_ttl(record_ttl)?;
        let mut state = self.state.lock().await;
        let now = Instant::now();
        state.purge_expired(now);
        match state.executions.entry(reservation_id) {
            Entry::Vacant(entry) => {
                entry.insert(Expiring::new(
                    Execution {
                        record: ExecutionRecord {
                            digest: *digest,
                            phase: ExecutionPhase::Claimed,
                        },
                        token,
                        lease_started_at: now,
                        lease_ttl,
                    },
                    now,
                    record_ttl,
                ));
                metrics::inc_storage_execution_phase("claimed");
                claim_result(ExecutionClaim::Acquired)
            }
            Entry::Occupied(mut entry) => {
                let execution = &mut entry.get_mut().value;
                if execution.record.digest != *digest {
                    return claim_result(ExecutionClaim::Conflict);
                }
                match execution.record.phase {
                    ExecutionPhase::Dispatching => claim_result(ExecutionClaim::PossiblySubmitted),
                    ExecutionPhase::Completed => claim_result(ExecutionClaim::Completed),
                    ExecutionPhase::Claimed if execution.lease_valid(now) => {
                        claim_result(if execution.token == token {
                            ExecutionClaim::Acquired
                        } else {
                            ExecutionClaim::InFlight
                        })
                    }
                    ExecutionPhase::Claimed => {
                        if execution.token == token {
                            return Err(conflict());
                        }
                        execution.token = token;
                        execution.lease_started_at = now;
                        execution.lease_ttl = lease_ttl;
                        // Reclaiming the lease does not extend the record's retention.
                        claim_result(ExecutionClaim::Acquired)
                    }
                }
            }
        }
    }

    async fn get_execution(
        &self,
        reservation_id: ReservationId,
    ) -> StorageResult<Option<ExecutionRecord>> {
        metrics::inc_storage_call("get_execution");
        let mut state = self.state.lock().await;
        Ok(
            live_entry(&mut state.executions, &reservation_id, Instant::now())
                .map(|entry| entry.value.record.clone()),
        )
    }

    async fn mark_execution_dispatching(
        &self,
        reservation_id: ReservationId,
        token: ExecutionToken,
    ) -> StorageResult<()> {
        metrics::inc_storage_call("mark_execution_dispatching");
        let mut state = self.state.lock().await;
        let now = Instant::now();
        let entry = live_entry(&mut state.executions, &reservation_id, now).ok_or_else(conflict)?;
        let execution = &mut entry.value;
        if execution.token != token
            || execution.record.phase != ExecutionPhase::Claimed
            || !execution.lease_valid(now)
        {
            return Err(conflict());
        }
        execution.record.phase = ExecutionPhase::Dispatching;
        metrics::inc_storage_execution_phase("dispatching");
        Ok(())
    }

    async fn complete_execution(
        &self,
        reservation_id: ReservationId,
        token: ExecutionToken,
        transaction: CachedTransaction,
        ttl: Duration,
    ) -> StorageResult<TransactionWriteOutcome> {
        metrics::inc_storage_call("complete_execution");
        validate_ttl(ttl)?;
        let mut state = self.state.lock().await;
        let now = Instant::now();
        state.purge_expired(now);
        let State {
            executions,
            transactions,
            ..
        } = &mut *state;
        let execution = &mut executions
            .get_mut(&reservation_id)
            .ok_or_else(conflict)?
            .value;
        if execution.token != token
            || !matches!(
                execution.record.phase,
                ExecutionPhase::Dispatching | ExecutionPhase::Completed
            )
        {
            return Err(conflict());
        }
        let outcome =
            write_transaction(transactions, execution.record.digest, transaction, ttl, now)?;
        if execution.record.phase != ExecutionPhase::Completed {
            execution.record.phase = ExecutionPhase::Completed;
            metrics::inc_storage_execution_phase("completed");
        }
        Ok(outcome)
    }

    async fn cancel_execution(
        &self,
        reservation_id: ReservationId,
        token: ExecutionToken,
    ) -> StorageResult<()> {
        metrics::inc_storage_call("cancel_execution");
        let mut state = self.state.lock().await;
        let now = Instant::now();
        let execution = &live_entry(&mut state.executions, &reservation_id, now)
            .ok_or_else(conflict)?
            .value;
        if execution.token != token
            || execution.record.phase != ExecutionPhase::Claimed
            || !execution.lease_valid(now)
        {
            return Err(conflict());
        }
        state.executions.remove(&reservation_id);
        Ok(())
    }
}

fn validate_ttl(ttl: Duration) -> StorageResult<()> {
    if ttl.is_zero() {
        metrics::inc_storage_error("invalid_ttl");
        Err(StorageError::InvalidTtl)
    } else {
        Ok(())
    }
}

fn next_reservation_id(sequence: &AtomicU64) -> StorageResult<ReservationId> {
    sequence
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map(|previous| previous + 1)
        .map_err(|_| {
            metrics::inc_storage_error("id_exhausted");
            StorageError::IdExhausted
        })
}

fn live_entry<'a, K: Eq + Hash, T>(
    entries: &'a mut HashMap<K, Expiring<T>>,
    key: &K,
    now: Instant,
) -> Option<&'a mut Expiring<T>> {
    if entries.get(key).is_some_and(|entry| entry.expired(now)) {
        entries.remove(key);
    }
    entries.get_mut(key)
}

fn write_transaction(
    entries: &mut HashMap<TransactionDigest, Expiring<CachedTransaction>>,
    digest: TransactionDigest,
    transaction: CachedTransaction,
    ttl: Duration,
    now: Instant,
) -> StorageResult<TransactionWriteOutcome> {
    match entries.entry(digest) {
        Entry::Vacant(entry) => {
            let entry = entry.insert(Expiring::new(transaction, now, ttl));
            metrics::inc_storage_cached_transactions(&entry.value.backend);
            Ok(TransactionWriteOutcome::Inserted)
        }
        Entry::Occupied(mut entry) => {
            let existing = &mut entry.get_mut().value;
            if existing.effects != transaction.effects || existing.backend != transaction.backend {
                return Err(conflict());
            }
            if transaction.local_execution_confirmed && !existing.local_execution_confirmed {
                existing.local_execution_confirmed = true;
                Ok(TransactionWriteOutcome::Upgraded)
            } else {
                Ok(TransactionWriteOutcome::Unchanged)
            }
        }
    }
}

fn conflict() -> StorageError {
    metrics::inc_storage_error("conflict");
    StorageError::Conflict
}

fn claim_result(claim: ExecutionClaim) -> StorageResult<ExecutionClaim> {
    metrics::inc_storage_execution_claim(match claim {
        ExecutionClaim::Acquired => "acquired",
        ExecutionClaim::InFlight => "in_flight",
        ExecutionClaim::PossiblySubmitted => "possibly_submitted",
        ExecutionClaim::Completed => "completed",
        ExecutionClaim::Conflict => "conflict",
    });
    Ok(claim)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reservation_sequence_exhaustion_never_wraps() {
        let sequence = AtomicU64::new(u64::MAX - 1);
        assert_eq!(next_reservation_id(&sequence).unwrap(), u64::MAX);
        assert!(matches!(
            next_reservation_id(&sequence),
            Err(StorageError::IdExhausted)
        ));
        assert!(matches!(
            next_reservation_id(&sequence),
            Err(StorageError::IdExhausted)
        ));
    }
}
