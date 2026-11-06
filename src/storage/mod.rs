use std::time::Duration;

use uuid::Uuid;

use async_trait::async_trait;
use iota_sdk_types::TransactionDigest;
use serde::{Deserialize, Serialize};

pub mod memory;

pub type ReservationId = u64;
pub type StorageResult<T> = Result<T, StorageError>;

/// Unique identifier of an execution lease owner.
///
/// Must be newly generated for every execution attempt.
/// Used to prevent one router from modifying another
/// router's execution state.
pub type ExecutionToken = Uuid;

/// ExecutionPhase - transaction life cycle
///
/// ```text
///                       execute_tx
///                           |
///                           v
///                +--------------------+
///                | No execution record|
///                +--------------------+
///                           |
///                           | try_claim_execution()
///                           v
///                +--------------------+
///                |      CLAIMED       |
///                |--------------------|
///                | Router owns lease  |
///                | Request NOT sent   |
///                +--------------------+
///                     |          |
///       lease expired |          | mark_execution_dispatching()
///       or cancel     |          |
///                     v          v
///                +--------------------+
///                |    DISPATCHING     |
///                |--------------------|
///                | May be submitted   |
///                | to Gas Station     |
///                +--------------------+
///                           |
///                           | execute_tx HTTP request
///                           v
///                  +------------------+
///                  |   Gas Station    |
///                  |                  |
///                  |  IOTA Full Node  |
///                  +------------------+
///                           |
///                           | Response: TransactionEffects
///                           v
///                +--------------------+
///                |     COMPLETED      |
///                |--------------------|
///                | Effects cached     |
///                | Transaction done   |
///                +--------------------+
/// ```
///
/// Current state of transaction execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionPhase {
    /// Execution was claimed but has not been dispatched.
    ///
    /// Can be reclaimed after the lease expires.
    Claimed,

    /// The router may have sent the transaction upstream.
    ///
    /// This state must be persisted BEFORE the HTTP request.
    /// Expiration of the lease does not permit resubmission.
    Dispatching,

    /// Finalized transaction effects have been cached.
    Completed,
}

/// Persisted execution state associated with a reservation.
///
/// Indexed by the router-generated reservation ID.
/// The digest cannot change once execution is claimed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub digest: TransactionDigest,
    pub phase: ExecutionPhase,
}

/// ExecutionClaim lifecycle
///
/// ```text
///              Router A / Router B
///                       |
///                       | execute_tx(reservation_id, digest)
///                       v
///             +----------------------+
///             |        Redis         |
///             | try_claim_execution()|
///             +----------------------+
///                       |
///          +------------+-------------+
///          |                          |
///          v                          v
///    No record exists           Record exists
///          |                          |
///          v                          v
///    +------------+            Check execution
///    |  ACQUIRED  |                  |
///    +------------+                  |
///          |                         |
///          v          +--------------+--------------+
///    Create CLAIMED   |              |              |
///                      v              v              v
///                   CLAIMED       DISPATCHING     COMPLETED
///                      |              |              |
///                +-----+-----+        |              |
///                |           |        |              |
///                v           v        v              v
///           Lease valid   Expired  POSSIBLY_      COMPLETED
///                |           |     SUBMITTED
///                v           v        |              |
///             IN_FLIGHT   ACQUIRED    |              |
///                |           |        |              |
///                v           v        v              v
///              Wait       Reclaim   Recovery      Read cache
///
///    Different digest for existing reservation:
///                     |
///                     v
///                  CONFLICT
/// ```
///
/// Result of trying to claim a reservation for execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionClaim {
    /// The caller acquired the execution lease.
    Acquired,

    /// Another router currently owns a valid lease.
    InFlight,

    /// Execution may already have reached the Gas Station.
    ///
    /// Do not resubmit. Wait for effects or recover the
    /// transaction result using its digest.
    PossiblySubmitted,

    /// Transaction execution has completed.
    Completed,

    /// The reservation was claimed for a different digest.
    Conflict,
}

/// Identifies the upstream Gas Station that owns a reservation.
///
/// Used to route execute_tx requests to the same backend
/// that originally handled reserve_gas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reservation {
    /// Backend name defined in the router configuration.
    pub backend: String,

    /// Original reservation ID returned by the Gas Station.
    pub upstream_reservation_id: u64,
}

/// Cached result of an executed IOTA transaction.
///
/// Transactions are indexed by their TransactionDigest.
///
/// The cache supports both waitForEffectsCert and
/// waitForLocalExecution request types.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedTransaction {
    /// Transaction effects returned by the upstream Gas Station.
    ///
    /// Contains the JSON value of the `effects` field,
    /// not the entire HTTP response.
    ///
    /// Effects must represent a finalized transaction.
    pub effects: serde_json::Value,

    /// Whether execution was confirmed on the backend's full node.
    ///
    /// Must only be true after actual local execution
    /// has been verified.
    ///
    /// A successful waitForEffectsCert response does not
    /// imply local execution.
    ///
    /// This flag must never transition from true to false.
    pub local_execution_confirmed: bool,

    /// Backend responsible for the transaction.
    ///
    /// Identifies the Gas Station whose full node
    /// is associated with local execution confirmation.
    pub backend: String,
}

/// Result of inserting or updating a cached transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionWriteOutcome {
    /// Transaction was not cached and has been inserted.
    Inserted,

    /// Existing transaction was upgraded to locally confirmed.
    Upgraded,

    /// Transaction already existed and no update was required.
    Unchanged,
}

/// Errors returned by storage implementations.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("storage backend error: {0}")]
    Backend(#[source] Box<dyn std::error::Error + Send + Sync>),

    #[error("TTL must be greater than zero")]
    InvalidTtl,

    #[error("reservation ID space exhausted")]
    IdExhausted,

    #[error("conflicting transaction cache entry")]
    Conflict,
}

/// Storage abstraction for the Gas Station Router.
///
/// Supports in-memory and Redis implementations.
///
/// All implementations must:
/// - Be safe for concurrent access.
/// - Support per-entry TTL.
/// - Provide atomic write operations.
/// - Distinguish missing entries from storage errors.
///
/// Redis implementations must guarantee atomic operations
/// across multiple router instances sharing the same storage.
///
/// Expired entries must behave as if they do not exist.
#[async_trait]
pub trait Storage: Send + Sync + 'static {
    /// Creates a reservation mapping.
    ///
    /// Generates a globally unique router reservation ID
    /// and associates it with an upstream Gas Station.
    ///
    /// The returned ID replaces the upstream reservation ID
    /// in the response returned to the client.
    ///
    /// ID generation and mapping insertion must be atomic.
    /// A successful return guarantees that the mapping
    /// is available for subsequent reads.
    ///
    /// The reservation expires after `ttl`.
    ///
    /// Reservation IDs must not be reused while an older
    /// client may still reference them.
    ///
    /// # Errors
    ///
    /// Returns an error when:
    /// - TTL is zero.
    /// - Storage is unavailable.
    /// - A unique ID cannot be allocated.
    async fn create_reservation(
        &self,
        reservation: Reservation,
        ttl: Duration,
    ) -> StorageResult<ReservationId>;

    /// Retrieves a reservation mapping.
    ///
    /// Returns `None` when:
    /// - The reservation does not exist.
    /// - The reservation has expired.
    ///
    /// Storage failures must return an error.
    async fn get_reservation(&self, id: ReservationId) -> StorageResult<Option<Reservation>>;

    /// Inserts or upgrades a cached transaction.
    ///
    /// Transactions are identified by their TransactionDigest.
    ///
    /// This operation must be atomic across concurrent callers
    /// and multiple router instances.
    ///
    /// Insertion rules:
    /// - Insert the transaction if it does not exist.
    /// - Preserve existing effects and backend.
    /// - Never downgrade local_execution_confirmed.
    /// - Allow upgrading local_execution_confirmed
    ///   from false to true.
    /// - Reject conflicting effects or backend values.
    /// - Never refresh the TTL of an existing entry.
    ///
    /// Only finalized transaction effects may be stored.
    /// Transient HTTP errors, timeouts, and uncertain
    /// execution results must not be cached as effects.
    ///
    /// The caller must independently verify local execution
    /// before supplying local_execution_confirmed = true.
    ///
    /// # Returns
    ///
    /// - Inserted: A new transaction was stored.
    /// - Upgraded: Local execution was newly confirmed.
    /// - Unchanged: No modification was necessary.
    async fn put_transaction(
        &self,
        digest: &TransactionDigest,
        transaction: CachedTransaction,
        ttl: Duration,
    ) -> StorageResult<TransactionWriteOutcome>;

    /// Retrieves a cached transaction by its digest.
    ///
    /// Returns both transaction effects and local
    /// execution confirmation status.
    ///
    /// Returns `None` when the transaction does not exist
    /// or its TTL has expired.
    ///
    /// Storage failures must return an error.
    async fn get_transaction(
        &self,
        digest: &TransactionDigest,
    ) -> StorageResult<Option<CachedTransaction>>;

    /// Checks whether the storage backend is operational.
    ///
    /// Memory implementations should return Ok(()).
    ///
    /// Redis implementations should perform a PING
    /// or an equivalent connectivity check.
    ///
    /// Intended for readiness checks and monitoring.
    async fn health_check(&self) -> StorageResult<()>;

    /// Atomically claims a reservation for transaction execution.
    ///
    /// Only one router may own the active execution lease.
    ///
    /// If an existing execution is still in Claimed state
    /// and its lease has expired, another router may acquire
    /// it using a new token.
    ///
    /// Executions in Dispatching or Completed states must
    /// never be reclaimed for another upstream submission.
    ///
    /// The digest associated with a reservation is immutable.
    /// Requests using a different digest return Conflict.
    ///
    /// `lease_ttl` controls how long the caller has to begin
    /// dispatching the transaction.
    ///
    /// `record_ttl` controls retention of the execution state
    /// and must outlive the execution and recovery window.
    ///
    /// Both TTL values must be greater than zero.
    async fn try_claim_execution(
        &self,
        reservation_id: ReservationId,
        digest: &TransactionDigest,
        token: ExecutionToken,
        lease_ttl: Duration,
        record_ttl: Duration,
    ) -> StorageResult<ExecutionClaim>;

    /// Returns the current execution state for a reservation.
    ///
    /// Returns None if no record exists or it has expired.
    ///
    /// Does not acquire or modify an execution lease.
    async fn get_execution(
        &self,
        reservation_id: ReservationId,
    ) -> StorageResult<Option<ExecutionRecord>>;

    /// Marks an execution as potentially submitted.
    ///
    /// Must be called BEFORE sending the HTTP request
    /// to the upstream Gas Station.
    ///
    /// The transition from Claimed to Dispatching must
    /// atomically verify the caller's ownership token
    /// and that the lease has not expired.
    ///
    /// Once Dispatching, another router must not
    /// automatically submit the transaction again,
    /// even after the original lease expires.
    ///
    /// If this operation fails or its result is uncertain,
    /// the caller must not forward the request.
    async fn mark_execution_dispatching(
        &self,
        reservation_id: ReservationId,
        token: ExecutionToken,
    ) -> StorageResult<()>;

    /// Atomically completes an execution and caches its effects.
    ///
    /// The execution must be in Dispatching state and the
    /// supplied token must match the execution owner.
    ///
    /// The transaction digest is taken from the execution
    /// record and used as the cache key.
    ///
    /// Must atomically:
    /// - Store or safely merge the finalized effects.
    /// - Change the execution state to Completed.
    /// - Preserve the existing execution record TTL.
    ///
    /// `ttl` applies to the cached transaction effects.
    ///
    /// The operation must be idempotent and must not
    /// overwrite conflicting transaction effects.
    async fn complete_execution(
        &self,
        reservation_id: ReservationId,
        token: ExecutionToken,
        transaction: CachedTransaction,
        ttl: Duration,
    ) -> StorageResult<TransactionWriteOutcome>;

    /// Cancels an execution before upstream submission.
    ///
    /// Only the current lease owner may cancel.
    /// The execution must still be in Claimed state.
    ///
    /// Must not remove Dispatching or Completed records.
    ///
    /// Intended for failures detected before sending
    /// any request to the upstream Gas Station.
    async fn cancel_execution(
        &self,
        reservation_id: ReservationId,
        token: ExecutionToken,
    ) -> StorageResult<()>;
}
