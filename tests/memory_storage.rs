use std::{collections::HashSet, sync::Arc, time::Duration};

use gas_station_router_v2::storage::{
    CachedTransaction, ExecutionClaim, ExecutionPhase, ExecutionToken, Reservation, Storage,
    StorageError, TransactionWriteOutcome, memory::MemoryStorage,
};
use iota_sdk_types::TransactionDigest;
use serde_json::json;
use tokio::{sync::Barrier, time::advance};
use uuid::Uuid;

const TTL: Duration = Duration::from_secs(10);
const LEASE: Duration = Duration::from_secs(3);

fn digest() -> TransactionDigest {
    TransactionDigest::new([42; 32])
}

fn reservation(upstream_reservation_id: u64) -> Reservation {
    Reservation {
        backend: "station-a".into(),
        upstream_reservation_id,
    }
}

fn transaction(confirmed: bool) -> CachedTransaction {
    CachedTransaction {
        effects: json!({"status": "success", "gasUsed": 7}),
        local_execution_confirmed: confirmed,
        backend: "station-a".into(),
    }
}

async fn claim(store: &MemoryStorage, id: u64, token: ExecutionToken) {
    assert_eq!(
        store
            .try_claim_execution(id, &digest(), token, LEASE, TTL)
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
}

#[tokio::test(start_paused = true)]
async fn reservations_expire_at_the_boundary_and_ids_are_not_reused() {
    let store = MemoryStorage::new();
    assert!(store.get_reservation(u64::MAX).await.unwrap().is_none());
    let id = store
        .create_reservation(reservation(99), TTL)
        .await
        .unwrap();
    let clone = store.clone();
    let mut result = clone.get_reservation(id).await.unwrap().unwrap();
    assert_eq!(result.upstream_reservation_id, 99);
    result.backend = "changed-copy".into();
    assert_eq!(
        store.get_reservation(id).await.unwrap().unwrap().backend,
        "station-a"
    );
    advance(TTL).await;
    assert!(clone.get_reservation(id).await.unwrap().is_none());
    assert!(
        store
            .create_reservation(reservation(99), TTL)
            .await
            .unwrap()
            > id
    );
    assert!(
        MemoryStorage::new()
            .create_reservation(reservation(99), TTL)
            .await
            .unwrap()
            > id
    );
    store.health_check().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn cache_upgrades_are_monotonic_and_do_not_refresh_ttl() {
    let store = MemoryStorage::new();
    assert_eq!(
        store
            .put_transaction(&digest(), transaction(false), TTL)
            .await
            .unwrap(),
        TransactionWriteOutcome::Inserted
    );
    advance(Duration::from_secs(4)).await;
    assert_eq!(
        store
            .put_transaction(&digest(), transaction(true), Duration::from_secs(100))
            .await
            .unwrap(),
        TransactionWriteOutcome::Upgraded
    );
    assert_eq!(
        store
            .put_transaction(&digest(), transaction(false), Duration::from_secs(1))
            .await
            .unwrap(),
        TransactionWriteOutcome::Unchanged
    );
    assert!(
        store
            .get_transaction(&digest())
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
    advance(Duration::from_secs(6)).await;
    assert!(store.get_transaction(&digest()).await.unwrap().is_none());
    assert_eq!(
        store
            .put_transaction(&digest(), transaction(false), TTL)
            .await
            .unwrap(),
        TransactionWriteOutcome::Inserted
    );
}

#[tokio::test]
async fn conflicting_effects_or_backend_do_not_change_the_cache() {
    let store = MemoryStorage::new();
    store
        .put_transaction(&digest(), transaction(false), TTL)
        .await
        .unwrap();
    let mut effects_conflict = transaction(true);
    effects_conflict.effects = json!({"status": "different"});
    let mut backend_conflict = transaction(true);
    backend_conflict.backend = "station-b".into();
    for conflicting in [effects_conflict, backend_conflict] {
        assert!(matches!(
            store.put_transaction(&digest(), conflicting, TTL).await,
            Err(StorageError::Conflict)
        ));
    }
    let cached = store.get_transaction(&digest()).await.unwrap().unwrap();
    assert_eq!(cached.effects, transaction(false).effects);
    assert_eq!(cached.backend, "station-a");
    assert!(!cached.local_execution_confirmed);
}

#[tokio::test]
async fn zero_ttls_are_rejected_without_partial_writes() {
    let store = MemoryStorage::new();
    assert!(matches!(
        store
            .create_reservation(reservation(1), Duration::ZERO)
            .await,
        Err(StorageError::InvalidTtl)
    ));
    assert!(matches!(
        store
            .put_transaction(&digest(), transaction(false), Duration::ZERO)
            .await,
        Err(StorageError::InvalidTtl)
    ));
    let token = Uuid::new_v4();
    for (lease, record) in [(Duration::ZERO, TTL), (LEASE, Duration::ZERO)] {
        assert!(matches!(
            store
                .try_claim_execution(1, &digest(), token, lease, record)
                .await,
            Err(StorageError::InvalidTtl)
        ));
        assert!(store.get_execution(1).await.unwrap().is_none());
    }
    claim(&store, 1, token).await;
    store.mark_execution_dispatching(1, token).await.unwrap();
    assert!(matches!(
        store
            .complete_execution(1, token, transaction(false), Duration::ZERO)
            .await,
        Err(StorageError::InvalidTtl)
    ));
    assert_eq!(
        store.get_execution(1).await.unwrap().unwrap().phase,
        ExecutionPhase::Dispatching
    );
    assert!(store.get_transaction(&digest()).await.unwrap().is_none());
}

#[tokio::test(start_paused = true)]
async fn only_a_fresh_owner_can_reclaim_an_expired_claimed_lease() {
    let store = MemoryStorage::new();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    claim(&store, 1, first).await;
    assert_eq!(
        store
            .try_claim_execution(1, &digest(), second, LEASE, TTL)
            .await
            .unwrap(),
        ExecutionClaim::InFlight
    );
    assert_eq!(
        store
            .try_claim_execution(1, &TransactionDigest::new([43; 32]), first, LEASE, TTL)
            .await
            .unwrap(),
        ExecutionClaim::Conflict
    );
    assert!(matches!(
        store.mark_execution_dispatching(1, second).await,
        Err(StorageError::Conflict)
    ));
    advance(Duration::from_secs(2)).await;
    // Retrying an owned claim does not renew its lease or record retention.
    assert_eq!(
        store
            .try_claim_execution(1, &digest(), first, TTL, Duration::from_secs(100))
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
    advance(Duration::from_secs(1)).await;
    assert!(matches!(
        store.mark_execution_dispatching(1, first).await,
        Err(StorageError::Conflict)
    ));
    assert!(matches!(
        store.cancel_execution(1, first).await,
        Err(StorageError::Conflict)
    ));
    assert!(matches!(
        store
            .try_claim_execution(1, &digest(), first, LEASE, TTL)
            .await,
        Err(StorageError::Conflict)
    ));
    claim(&store, 1, second).await;
    assert!(matches!(
        store.mark_execution_dispatching(1, first).await,
        Err(StorageError::Conflict)
    ));
    assert!(matches!(
        store.cancel_execution(1, first).await,
        Err(StorageError::Conflict)
    ));
    store.mark_execution_dispatching(1, second).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn dispatching_cannot_be_reclaimed_or_cancelled_after_lease_expiry() {
    let store = MemoryStorage::new();
    let owner = Uuid::new_v4();
    claim(&store, 1, owner).await;
    assert!(matches!(
        store
            .complete_execution(1, owner, transaction(false), TTL)
            .await,
        Err(StorageError::Conflict)
    ));
    store.mark_execution_dispatching(1, owner).await.unwrap();
    assert!(matches!(
        store.mark_execution_dispatching(1, owner).await,
        Err(StorageError::Conflict)
    ));
    advance(LEASE).await;
    assert_eq!(
        store
            .try_claim_execution(1, &digest(), Uuid::new_v4(), LEASE, TTL)
            .await
            .unwrap(),
        ExecutionClaim::PossiblySubmitted
    );
    assert!(matches!(
        store.cancel_execution(1, owner).await,
        Err(StorageError::Conflict)
    ));
    assert!(matches!(
        store
            .complete_execution(1, Uuid::new_v4(), transaction(false), TTL)
            .await,
        Err(StorageError::Conflict)
    ));
    // The dispatch owner can finish even though its original lease has expired.
    assert_eq!(
        store
            .complete_execution(1, owner, transaction(false), TTL)
            .await
            .unwrap(),
        TransactionWriteOutcome::Inserted
    );
    assert_eq!(
        store
            .try_claim_execution(1, &digest(), Uuid::new_v4(), LEASE, TTL)
            .await
            .unwrap(),
        ExecutionClaim::Completed
    );
    assert!(matches!(
        store.cancel_execution(1, owner).await,
        Err(StorageError::Conflict)
    ));
}

#[tokio::test(start_paused = true)]
async fn reclaim_and_completion_preserve_record_ttl_and_cache_ttl_is_independent() {
    let store = MemoryStorage::new();
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    claim(&store, 1, first).await;
    advance(LEASE).await;
    assert_eq!(
        store
            .try_claim_execution(1, &digest(), second, LEASE, Duration::from_secs(100))
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
    store.mark_execution_dispatching(1, second).await.unwrap();
    advance(LEASE).await;
    store
        .complete_execution(1, second, transaction(false), Duration::from_secs(100))
        .await
        .unwrap();
    assert_eq!(
        store.get_execution(1).await.unwrap().unwrap().phase,
        ExecutionPhase::Completed
    );
    advance(Duration::from_secs(4)).await;
    assert!(store.get_execution(1).await.unwrap().is_none());
    assert!(store.get_transaction(&digest()).await.unwrap().is_some());
    assert!(matches!(
        store.mark_execution_dispatching(1, second).await,
        Err(StorageError::Conflict)
    ));
    assert!(matches!(
        store
            .complete_execution(1, second, transaction(false), TTL)
            .await,
        Err(StorageError::Conflict)
    ));
}

#[tokio::test(start_paused = true)]
async fn completion_is_idempotent_and_cannot_refresh_or_overwrite_effects() {
    let store = MemoryStorage::new();
    let owner = Uuid::new_v4();
    store
        .try_claim_execution(1, &digest(), owner, LEASE, Duration::from_secs(100))
        .await
        .unwrap();
    store.mark_execution_dispatching(1, owner).await.unwrap();
    store
        .complete_execution(1, owner, transaction(false), TTL)
        .await
        .unwrap();
    advance(Duration::from_secs(4)).await;
    assert_eq!(
        store
            .complete_execution(1, owner, transaction(true), Duration::from_secs(100))
            .await
            .unwrap(),
        TransactionWriteOutcome::Upgraded
    );
    assert_eq!(
        store
            .complete_execution(1, owner, transaction(false), Duration::from_secs(100))
            .await
            .unwrap(),
        TransactionWriteOutcome::Unchanged
    );
    let mut conflict = transaction(false);
    conflict.effects = json!({"status": "different"});
    assert!(matches!(
        store.complete_execution(1, owner, conflict, TTL).await,
        Err(StorageError::Conflict)
    ));
    assert!(
        store
            .get_transaction(&digest())
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
    advance(Duration::from_secs(6)).await;
    assert!(store.get_transaction(&digest()).await.unwrap().is_none());
    assert_eq!(
        store.get_execution(1).await.unwrap().unwrap().phase,
        ExecutionPhase::Completed
    );
}

#[tokio::test]
async fn cache_conflict_during_completion_keeps_execution_dispatching() {
    let store = MemoryStorage::new();
    let owner = Uuid::new_v4();
    store
        .put_transaction(&digest(), transaction(false), TTL)
        .await
        .unwrap();
    claim(&store, 1, owner).await;
    store.mark_execution_dispatching(1, owner).await.unwrap();
    let mut conflict = transaction(true);
    conflict.backend = "station-b".into();
    assert!(matches!(
        store.complete_execution(1, owner, conflict, TTL).await,
        Err(StorageError::Conflict)
    ));
    assert_eq!(
        store.get_execution(1).await.unwrap().unwrap().phase,
        ExecutionPhase::Dispatching
    );
    assert!(
        !store
            .get_transaction(&digest())
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
}

#[tokio::test]
async fn only_the_active_claim_owner_can_cancel_before_dispatch() {
    let store = MemoryStorage::new();
    let owner = Uuid::new_v4();
    assert!(matches!(
        store.cancel_execution(1, owner).await,
        Err(StorageError::Conflict)
    ));
    claim(&store, 1, owner).await;
    assert!(matches!(
        store.cancel_execution(1, Uuid::new_v4()).await,
        Err(StorageError::Conflict)
    ));
    store.cancel_execution(1, owner).await.unwrap();
    assert!(store.get_execution(1).await.unwrap().is_none());
    assert!(matches!(
        store.mark_execution_dispatching(1, owner).await,
        Err(StorageError::Conflict)
    ));
    claim(&store, 1, Uuid::new_v4()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_claims_and_dispatch_have_exactly_one_winner() {
    let store = MemoryStorage::new();
    let barrier = Arc::new(Barrier::new(32));
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let store = store.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            let token = Uuid::new_v4();
            barrier.wait().await;
            (
                token,
                store
                    .try_claim_execution(
                        1,
                        &digest(),
                        token,
                        Duration::from_secs(60),
                        Duration::from_secs(120),
                    )
                    .await
                    .unwrap(),
            )
        }));
    }
    let mut owners = Vec::new();
    for task in tasks {
        let (token, outcome) = task.await.unwrap();
        match outcome {
            ExecutionClaim::Acquired => owners.push(token),
            ExecutionClaim::InFlight => {}
            unexpected => panic!("unexpected claim: {unexpected:?}"),
        }
    }
    assert_eq!(owners.len(), 1);
    let owner = owners[0];
    let mut tasks = Vec::new();
    for _ in 0..32 {
        let store = store.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store.mark_execution_dispatching(1, owner).await
        }));
    }
    let mut successful = 0;
    for task in tasks {
        match task.await.unwrap() {
            Ok(()) => successful += 1,
            Err(StorageError::Conflict) => {}
            unexpected => panic!("unexpected dispatch: {unexpected:?}"),
        }
    }
    assert_eq!(successful, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_reservations_are_readable_and_have_unique_ids() {
    let store = MemoryStorage::new();
    let mut tasks = Vec::new();
    for index in 0..128 {
        let store = store.clone();
        tasks.push(tokio::spawn(async move {
            let id = store
                .create_reservation(reservation(index), Duration::from_secs(60))
                .await
                .unwrap();
            assert_eq!(
                store
                    .get_reservation(id)
                    .await
                    .unwrap()
                    .unwrap()
                    .upstream_reservation_id,
                index
            );
            id
        }));
    }
    let mut ids = HashSet::new();
    for task in tasks {
        assert!(ids.insert(task.await.unwrap()));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_cache_writes_never_downgrade_confirmation() {
    let store = MemoryStorage::new();
    let barrier = Arc::new(Barrier::new(32));
    let mut tasks = Vec::new();
    for index in 0..32 {
        let store = store.clone();
        let barrier = barrier.clone();
        tasks.push(tokio::spawn(async move {
            barrier.wait().await;
            store
                .put_transaction(
                    &digest(),
                    transaction(index % 2 == 0),
                    Duration::from_secs(60),
                )
                .await
                .unwrap()
        }));
    }
    let mut inserts = 0;
    let mut upgrades = 0;
    for task in tasks {
        match task.await.unwrap() {
            TransactionWriteOutcome::Inserted => inserts += 1,
            TransactionWriteOutcome::Upgraded => upgrades += 1,
            TransactionWriteOutcome::Unchanged => {}
        }
    }
    assert_eq!(inserts, 1);
    assert!(upgrades <= 1);
    assert!(
        store
            .get_transaction(&digest())
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
}

#[tokio::test]
async fn maximum_ttls_do_not_overflow() {
    let store = MemoryStorage::new();
    let id = store
        .create_reservation(reservation(1), Duration::MAX)
        .await
        .unwrap();
    store
        .put_transaction(&digest(), transaction(false), Duration::MAX)
        .await
        .unwrap();
    let owner = Uuid::new_v4();
    store
        .try_claim_execution(id, &digest(), owner, Duration::MAX, Duration::MAX)
        .await
        .unwrap();
    store.mark_execution_dispatching(id, owner).await.unwrap();
    assert!(store.get_reservation(id).await.unwrap().is_some());
    assert!(store.get_transaction(&digest()).await.unwrap().is_some());
}
