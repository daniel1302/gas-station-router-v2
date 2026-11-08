use std::{collections::HashSet, process::Command, sync::Arc, time::Duration};

use gas_station_router_v2::storage::{
    CachedTransaction, ExecutionClaim, ExecutionPhase, Reservation, Storage, StorageError,
    TransactionWriteOutcome, redis::RedisStorage,
};
use iota_sdk_types::TransactionDigest;
use redis::{FromRedisValue, aio::MultiplexedConnection};
use serde_json::json;
use tokio::{
    sync::Barrier,
    time::{sleep, timeout},
};
use uuid::Uuid;

const PREFIX: &str = "router-test";
const TTL: Duration = Duration::from_secs(30);

/// Each test owns its Redis instance. No external database is read or modified.
struct RedisServer {
    name: String,
    url: String,
    connection: Option<MultiplexedConnection>,
}

impl RedisServer {
    async fn new() -> Self {
        let mut server = Self {
            name: format!("gs-router-storage-test-{}", Uuid::new_v4()),
            url: String::new(),
            connection: None,
        };
        // Docker can change an automatically assigned host port on restart.
        // Reserve a concrete endpoint so the reconnect test uses the same URL.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let publication = format!("127.0.0.1:{}:6379", listener.local_addr().unwrap().port());
        drop(listener);
        let result = Command::new("docker")
            .args([
                "run",
                "--detach",
                "--rm",
                "--name",
                &server.name,
                "--publish",
                &publication,
                "redis:7.4-alpine",
                "redis-server",
                "--save",
                "",
                "--appendonly",
                "no",
                "--maxmemory-policy",
                "noeviction",
            ])
            .output()
            .expect("Redis integration tests require Docker");
        assert!(
            result.status.success(),
            "Could not start isolated Redis: {}",
            String::from_utf8_lossy(&result.stderr)
        );
        let port = Command::new("docker")
            .args(["port", &server.name, "6379/tcp"])
            .output()
            .unwrap();
        assert!(port.status.success());
        server.url = format!(
            "redis://{}/0",
            String::from_utf8(port.stdout).unwrap().trim()
        );
        let client = redis::Client::open(server.url.as_str()).unwrap();
        server.connection = Some(
            timeout(Duration::from_secs(10), async {
                loop {
                    if let Ok(connection) = client.get_multiplexed_async_connection().await {
                        break connection;
                    }
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("isolated Redis did not start"),
        );
        server
    }

    async fn store(&self) -> RedisStorage {
        RedisStorage::new(&self.url, PREFIX).await.unwrap()
    }

    async fn command<T: FromRedisValue>(&self, command: &mut redis::Cmd) -> T {
        command
            .query_async(&mut self.connection.as_ref().unwrap().clone())
            .await
            .unwrap()
    }

    async fn pttl(&self, key: &str) -> i64 {
        self.command(redis::cmd("PTTL").arg(key)).await
    }
    async fn keys(&self) -> Vec<String> {
        // KEYS is limited to an isolated, tiny test instance, never a shared server.
        self.command(redis::cmd("KEYS").arg(format!("{PREFIX}:*")))
            .await
    }
}

impl Drop for RedisServer {
    fn drop(&mut self) {
        let _ = Command::new("docker")
            .args(["rm", "--force", &self.name])
            .output();
    }
}

fn key(kind: &str, id: impl std::fmt::Display) -> String {
    format!("{PREFIX}:v2:{kind}:{id}")
}
fn sequence_key() -> String {
    format!("{PREFIX}:v2:reservation_sequence")
}
fn digest() -> TransactionDigest {
    TransactionDigest::new([42; 32])
}
fn reservation(id: u64) -> Reservation {
    Reservation {
        backend: "station-a".into(),
        upstream_reservation_id: id,
    }
}
fn transaction(confirmed: bool) -> CachedTransaction {
    CachedTransaction {
        effects: json!({"status":{"status":"success"}, "version":u64::MAX,
        "nested":[{"value":9_007_199_254_740_993_u64}]}),
        backend: "station-a".into(),
        local_execution_confirmed: confirmed,
    }
}

#[tokio::test]
async fn every_data_key_expires_without_indexes_or_persistent_lease_keys() {
    let redis = RedisServer::new().await;
    let store = redis.store().await;
    let ttl = Duration::from_millis(500);
    for index in 0..32 {
        let digest = TransactionDigest::new([index; 32]);
        let id = store
            .create_reservation(reservation(index.into()), ttl)
            .await
            .unwrap();
        let owner = Uuid::new_v4();
        store
            .try_claim_execution(id, &digest, owner, ttl, ttl)
            .await
            .unwrap();
        store.mark_execution_dispatching(id, owner).await.unwrap();
        store
            .complete_execution(id, owner, transaction(false), ttl)
            .await
            .unwrap();
        for data_key in [
            key("reservation", id),
            key("execution", id),
            key("transaction", digest),
        ] {
            let remaining = redis.pttl(&data_key).await;
            assert!(
                remaining > 0 && remaining <= 500,
                "missing/bad TTL: {data_key}: {remaining}"
            );
        }
    }
    sleep(Duration::from_millis(700)).await;
    assert_eq!(
        redis.keys().await,
        vec![sequence_key()],
        "only the one ID counter may remain"
    );
    assert_eq!(redis.pttl(&sequence_key()).await, -1);
    assert!(store.get_execution(1).await.unwrap().is_none());
    assert!(store.get_reservation(1).await.unwrap().is_none());
    assert!(store.get_transaction(&digest()).await.unwrap().is_none());
    assert!(store.create_reservation(reservation(1), TTL).await.unwrap() > 32);
}

#[tokio::test]
async fn cache_upgrades_preserve_ttl_exact_integers_and_conflict_atomically() {
    let redis = RedisServer::new().await;
    let first = redis.store().await;
    let second = redis.store().await;
    assert_eq!(
        first
            .put_transaction(&digest(), transaction(false), TTL)
            .await
            .unwrap(),
        TransactionWriteOutcome::Inserted
    );
    let before = redis.pttl(&key("transaction", digest())).await;
    sleep(Duration::from_millis(30)).await;
    assert_eq!(
        second
            .put_transaction(&digest(), transaction(true), Duration::from_secs(300))
            .await
            .unwrap(),
        TransactionWriteOutcome::Upgraded
    );
    assert_eq!(
        second
            .put_transaction(&digest(), transaction(false), Duration::from_secs(1))
            .await
            .unwrap(),
        TransactionWriteOutcome::Unchanged
    );
    let mut conflicting = transaction(true);
    conflicting.effects["version"] = json!(1);
    assert!(matches!(
        first.put_transaction(&digest(), conflicting, TTL).await,
        Err(StorageError::Conflict)
    ));
    let mut conflicting = transaction(true);
    conflicting.backend = "station-b".into();
    assert!(matches!(
        first.put_transaction(&digest(), conflicting, TTL).await,
        Err(StorageError::Conflict)
    ));
    let value = first.get_transaction(&digest()).await.unwrap().unwrap();
    assert_eq!(
        value.effects,
        transaction(false).effects,
        "Lua must not round u64 data"
    );
    assert!(value.local_execution_confirmed);
    assert!(
        redis.pttl(&key("transaction", digest())).await < before,
        "reads and upgrades must not extend TTL"
    );
    redis
        .command::<()>(
            redis::cmd("PEXPIRE")
                .arg(key("transaction", digest()))
                .arg(1),
        )
        .await;
    sleep(Duration::from_millis(20)).await;
    assert!(first.get_transaction(&digest()).await.unwrap().is_none());
    assert_eq!(
        second
            .put_transaction(&digest(), transaction(false), TTL)
            .await
            .unwrap(),
        TransactionWriteOutcome::Inserted
    );
}

#[tokio::test]
async fn concurrent_clients_have_one_claim_dispatch_and_cache_insert() {
    let redis = RedisServer::new().await;
    let clients = [redis.store().await, redis.store().await];
    let barrier = Arc::new(Barrier::new(32));
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let store = clients[index % 2].clone();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            let owner = Uuid::new_v4();
            barrier.wait().await;
            (
                owner,
                store
                    .try_claim_execution(123, &digest(), owner, TTL, TTL)
                    .await
                    .unwrap(),
            )
        });
    }
    let mut owners = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            (owner, ExecutionClaim::Acquired) => owners.push(owner),
            (_, ExecutionClaim::InFlight) => {}
            other => panic!("unexpected claim: {other:?}"),
        }
    }
    assert_eq!(owners.len(), 1);
    for index in 0..32 {
        let store = clients[index % 2].clone();
        let owner = owners[0];
        tasks.spawn(async move {
            (
                owner,
                match store.mark_execution_dispatching(123, owner).await {
                    Ok(()) => ExecutionClaim::Acquired,
                    Err(StorageError::Conflict) => ExecutionClaim::Conflict,
                    other => panic!("unexpected dispatch: {other:?}"),
                },
            )
        });
    }
    let mut dispatches = 0;
    while let Some(result) = tasks.join_next().await {
        if result.unwrap().1 == ExecutionClaim::Acquired {
            dispatches += 1;
        }
    }
    assert_eq!(dispatches, 1);
    let mut writes = tokio::task::JoinSet::new();
    for index in 0..32 {
        let store = clients[index % 2].clone();
        writes.spawn(async move {
            store
                .put_transaction(&digest(), transaction(index % 2 == 0), TTL)
                .await
                .unwrap()
        });
    }
    let mut inserts = 0;
    while let Some(result) = writes.join_next().await {
        if result.unwrap() == TransactionWriteOutcome::Inserted {
            inserts += 1;
        }
    }
    assert_eq!(inserts, 1);
    assert!(
        clients[0]
            .get_transaction(&digest())
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
}

#[tokio::test]
async fn recovery_fences_old_owners_without_extending_record_or_cache_ttls() {
    let redis = RedisServer::new().await;
    let store = redis.store().await;
    let other = redis.store().await;
    let id = store
        .create_reservation(reservation(77), TTL)
        .await
        .unwrap();
    let owner = Uuid::new_v4();
    store
        .try_claim_execution(id, &digest(), owner, Duration::from_millis(50), TTL)
        .await
        .unwrap();
    store.mark_execution_dispatching(id, owner).await.unwrap();
    let before = redis.pttl(&key("execution", id)).await;
    sleep(Duration::from_millis(80)).await;
    assert_eq!(
        other
            .try_claim_execution(id, &digest(), Uuid::new_v4(), TTL, TTL)
            .await
            .unwrap(),
        ExecutionClaim::PossiblySubmitted
    );
    assert!(matches!(
        store.cancel_execution(id, owner).await,
        Err(StorageError::Conflict)
    ));
    assert!(matches!(
        store.try_claim_recovery(id, &digest(), owner, TTL).await,
        Err(StorageError::Conflict)
    ));
    let next = Uuid::new_v4();
    assert_eq!(
        other
            .try_claim_recovery(id, &digest(), next, TTL)
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
    assert!(matches!(
        store
            .complete_execution(id, owner, transaction(false), TTL)
            .await,
        Err(StorageError::Conflict)
    ));
    assert_eq!(
        other
            .complete_execution(id, next, transaction(false), TTL)
            .await
            .unwrap(),
        TransactionWriteOutcome::Inserted
    );
    let cache_ttl = redis.pttl(&key("transaction", digest())).await;
    sleep(Duration::from_millis(20)).await;
    assert_eq!(
        other
            .complete_execution(id, next, transaction(true), Duration::from_secs(300))
            .await
            .unwrap(),
        TransactionWriteOutcome::Upgraded
    );
    assert!(redis.pttl(&key("execution", id)).await < before);
    assert!(redis.pttl(&key("transaction", digest())).await < cache_ttl);
    let record = other.get_execution(id).await.unwrap().unwrap();
    assert_eq!(record.phase, ExecutionPhase::Completed);
    assert_eq!(record.backend.as_deref(), Some("station-a"));
    redis
        .command::<()>(redis::cmd("DEL").arg(key("reservation", id)))
        .await;
    redis
        .command::<()>(redis::cmd("DEL").arg(key("transaction", digest())))
        .await;
    let recoverer = Uuid::new_v4();
    assert_eq!(
        store
            .try_claim_recovery(id, &digest(), recoverer, TTL)
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
    assert!(matches!(
        other
            .complete_execution(id, next, transaction(false), TTL)
            .await,
        Err(StorageError::Conflict)
    ));
    store
        .complete_execution(id, recoverer, transaction(false), TTL)
        .await
        .unwrap();
    redis
        .command::<()>(redis::cmd("PEXPIRE").arg(key("execution", id)).arg(1))
        .await;
    sleep(Duration::from_millis(20)).await;
    assert_eq!(
        store
            .try_claim_recovery(id, &digest(), Uuid::new_v4(), TTL)
            .await
            .unwrap(),
        ExecutionClaim::Conflict
    );
    assert!(
        store.get_execution(id).await.unwrap().is_none(),
        "recovery must not recreate expired guards"
    );
}

#[tokio::test]
async fn invalid_ttls_conflicting_completion_and_bad_records_do_not_partially_write() {
    let redis = RedisServer::new().await;
    let store = redis.store().await;
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
    let owner = Uuid::new_v4();
    for (lease, record) in [(Duration::ZERO, TTL), (TTL, Duration::ZERO)] {
        assert!(matches!(
            store
                .try_claim_execution(1, &digest(), owner, lease, record)
                .await,
            Err(StorageError::InvalidTtl)
        ));
    }
    assert!(redis.keys().await.is_empty());
    store
        .try_claim_execution(1, &digest(), owner, TTL, TTL)
        .await
        .unwrap();
    assert!(matches!(
        store
            .complete_execution(1, owner, transaction(false), TTL)
            .await,
        Err(StorageError::Conflict)
    ));
    store.mark_execution_dispatching(1, owner).await.unwrap();
    assert!(matches!(
        store
            .complete_execution(1, owner, transaction(false), Duration::ZERO)
            .await,
        Err(StorageError::InvalidTtl)
    ));
    store
        .put_transaction(&digest(), transaction(false), TTL)
        .await
        .unwrap();
    let mut conflicting = transaction(true);
    conflicting.backend = "station-b".into();
    assert!(matches!(
        store.complete_execution(1, owner, conflicting, TTL).await,
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
    redis
        .command::<()>(redis::cmd("PERSIST").arg(key("execution", 1)))
        .await;
    assert!(
        matches!(store.get_execution(1).await, Err(StorageError::Backend(_))),
        "persistent data records must fail closed"
    );
    assert!(matches!(
        store
            .complete_execution(1, owner, transaction(true), TTL)
            .await,
        Err(StorageError::Backend(_))
    ));
}

#[tokio::test]
async fn ids_survive_new_clients_are_exact_above_f64_precision_and_never_wrap() {
    let redis = RedisServer::new().await;
    let store = redis.store().await;
    let id = store
        .create_reservation(reservation(u64::MAX), TTL)
        .await
        .unwrap();
    drop(store);
    let store = redis.store().await;
    assert_eq!(
        store
            .get_reservation(id)
            .await
            .unwrap()
            .unwrap()
            .upstream_reservation_id,
        u64::MAX
    );
    assert!(store.create_reservation(reservation(1), TTL).await.unwrap() > id);
    redis
        .command::<()>(
            redis::cmd("SET")
                .arg(sequence_key())
                .arg("9223372036854775806"),
        )
        .await;
    let id = store
        .create_reservation(reservation(u64::MAX), TTL)
        .await
        .unwrap();
    assert_eq!(id, i64::MAX as u64);
    assert_eq!(
        store
            .get_reservation(id)
            .await
            .unwrap()
            .unwrap()
            .upstream_reservation_id,
        u64::MAX
    );
    assert!(matches!(
        store.create_reservation(reservation(1), TTL).await,
        Err(StorageError::IdExhausted)
    ));
    assert!(matches!(
        store.create_reservation(reservation(1), TTL).await,
        Err(StorageError::IdExhausted)
    ));
    assert_eq!(redis.pttl(&sequence_key()).await, -1);
}

#[tokio::test]
async fn oom_rejects_writes_without_eviction_and_eviction_policies_are_rejected() {
    let redis = RedisServer::new().await;
    let store = redis.store().await;
    let id = store
        .create_reservation(reservation(77), TTL)
        .await
        .unwrap();
    store.health_check().await.unwrap();
    redis
        .command::<()>(redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg(1))
        .await;
    assert!(matches!(
        store.create_reservation(reservation(2), TTL).await,
        Err(StorageError::Backend(_))
    ));
    let value: Option<String> = redis
        .command(redis::cmd("GET").arg(key("reservation", id)))
        .await;
    assert!(
        value.is_some(),
        "memory pressure must not evict existing reservations"
    );
    let stats: String = redis.command(redis::cmd("INFO").arg("stats")).await;
    assert!(stats.lines().any(|line| line.trim() == "evicted_keys:0"));
    redis
        .command::<()>(redis::cmd("CONFIG").arg("SET").arg("maxmemory").arg(0))
        .await;
    assert!(store.get_reservation(id).await.unwrap().is_some());
    redis
        .command::<()>(
            redis::cmd("CONFIG")
                .arg("SET")
                .arg("maxmemory-policy")
                .arg("allkeys-lru"),
        )
        .await;
    assert!(RedisStorage::new(&redis.url, PREFIX).await.is_err());
}

#[tokio::test]
async fn canceled_claims_and_large_durations_leave_no_immortal_data() {
    let redis = RedisServer::new().await;
    let store = redis.store().await;
    let owner = Uuid::new_v4();
    store
        .try_claim_execution(1, &digest(), owner, TTL, TTL)
        .await
        .unwrap();
    assert!(matches!(
        store.cancel_execution(1, Uuid::new_v4()).await,
        Err(StorageError::Conflict)
    ));
    store.cancel_execution(1, owner).await.unwrap();
    assert!(store.get_execution(1).await.unwrap().is_none());
    let id = store
        .create_reservation(reservation(1), Duration::MAX)
        .await
        .unwrap();
    store
        .try_claim_execution(id, &digest(), owner, Duration::MAX, Duration::MAX)
        .await
        .unwrap();
    store.mark_execution_dispatching(id, owner).await.unwrap();
    store
        .complete_execution(id, owner, transaction(false), Duration::MAX)
        .await
        .unwrap();
    for data_key in [
        key("reservation", id),
        key("execution", id),
        key("transaction", digest()),
    ] {
        assert!(redis.pttl(&data_key).await > 0);
    }
    // Sub-millisecond positive TTLs round up rather than becoming persistent.
    let id = store
        .create_reservation(reservation(1), Duration::from_nanos(1))
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;
    assert!(store.get_reservation(id).await.unwrap().is_none());
    let keys: HashSet<_> = redis.keys().await.into_iter().collect();
    assert_eq!(keys.len(), 4);
}

#[tokio::test]
async fn claimed_leases_and_parallel_recovery_do_not_refresh_retention() {
    let redis = RedisServer::new().await;
    let clients = [redis.store().await, redis.store().await];
    let owner = Uuid::new_v4();
    let lease = Duration::from_millis(200);
    clients[0]
        .try_claim_execution(1, &digest(), owner, lease, TTL)
        .await
        .unwrap();
    let before = redis.pttl(&key("execution", 1)).await;
    sleep(Duration::from_millis(40)).await;
    assert_eq!(
        clients[1]
            .try_claim_execution(1, &digest(), owner, TTL, Duration::from_secs(300))
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
    assert_eq!(
        clients[1]
            .try_claim_execution(1, &TransactionDigest::new([1; 32]), owner, TTL, TTL)
            .await
            .unwrap(),
        ExecutionClaim::Conflict
    );
    sleep(Duration::from_millis(220)).await;
    assert!(
        matches!(
            clients[0].mark_execution_dispatching(1, owner).await,
            Err(StorageError::Conflict)
        ),
        "repeating an owned claim must not extend its lease"
    );
    assert!(matches!(
        clients[0]
            .try_claim_execution(1, &digest(), owner, TTL, TTL)
            .await,
        Err(StorageError::Conflict)
    ));
    let next = Uuid::new_v4();
    assert_eq!(
        clients[1]
            .try_claim_execution(1, &digest(), next, lease, Duration::from_secs(300))
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
    clients[1]
        .mark_execution_dispatching(1, next)
        .await
        .unwrap();
    sleep(Duration::from_millis(250)).await;
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let client = clients[index % 2].clone();
        tasks.spawn(async move {
            let owner = Uuid::new_v4();
            (
                owner,
                client
                    .try_claim_recovery(1, &digest(), owner, TTL)
                    .await
                    .unwrap(),
            )
        });
    }
    let mut winners = Vec::new();
    while let Some(result) = tasks.join_next().await {
        match result.unwrap() {
            (owner, ExecutionClaim::Acquired) => winners.push(owner),
            (_, ExecutionClaim::InFlight) => {}
            other => panic!("unexpected recovery: {other:?}"),
        }
    }
    assert_eq!(winners.len(), 1);
    assert!(redis.pttl(&key("execution", 1)).await < before);
    clients[0]
        .complete_execution(1, winners[0], transaction(false), TTL)
        .await
        .unwrap();
}

#[tokio::test]
async fn reconnect_and_new_clients_preserve_state_after_a_redis_restart() {
    let redis = RedisServer::new().await;
    let store = redis.store().await;
    let id = store
        .create_reservation(reservation(77), TTL)
        .await
        .unwrap();
    let owner = Uuid::new_v4();
    store
        .try_claim_execution(id, &digest(), owner, TTL, TTL)
        .await
        .unwrap();
    store.mark_execution_dispatching(id, owner).await.unwrap();
    let before = redis.pttl(&key("execution", id)).await;
    // The fixture normally disables persistence; explicitly snapshot this test's
    // own database before restarting its container.
    redis.command::<()>(&mut redis::cmd("SAVE")).await;
    let restarted = Command::new("docker")
        .args(["restart", &redis.name])
        .output()
        .unwrap();
    assert!(restarted.status.success());
    let port = Command::new("docker")
        .args(["port", &redis.name, "6379/tcp"])
        .output()
        .unwrap();
    assert_eq!(
        format!(
            "redis://{}/0",
            String::from_utf8(port.stdout).unwrap().trim()
        ),
        redis.url,
        "test Redis endpoint changed during restart"
    );
    timeout(Duration::from_secs(10), async {
        loop {
            if store.get_reservation(id).await.is_ok() {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let other = redis.store().await;
    assert_eq!(
        other
            .get_reservation(id)
            .await
            .unwrap()
            .unwrap()
            .upstream_reservation_id,
        77
    );
    assert_eq!(
        other.get_execution(id).await.unwrap().unwrap().phase,
        ExecutionPhase::Dispatching
    );
    assert_eq!(
        other
            .try_claim_execution(id, &digest(), Uuid::new_v4(), TTL, TTL)
            .await
            .unwrap(),
        ExecutionClaim::PossiblySubmitted
    );
    assert!(other.create_reservation(reservation(1), TTL).await.unwrap() > id);
    let mut connection = redis::Client::open(redis.url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let remaining: i64 = redis::cmd("PTTL")
        .arg(key("execution", id))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(
        remaining > 0 && remaining <= before,
        "restart must preserve the absolute expiration"
    );
}

struct RunningRouter {
    task: tokio::task::JoinHandle<anyhow::Result<()>>,
    url: String,
}

impl Drop for RunningRouter {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RunningRouter {
    async fn start(redis: &RedisServer, backend: &str, rpc: &str) -> Self {
        let mut config: serde_yaml_ng::Value =
            serde_yaml_ng::from_str(gas_station_router_v2::config::EXAMPLE_CONFIG).unwrap();
        let unused_port = || {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let port = unused_port();
        config["rpc"] = serde_yaml_ng::to_value(
            json!({"host":"127.0.0.1","port":port,"token":"router-test-token"}),
        )
        .unwrap();
        config["metrics"] =
            serde_yaml_ng::to_value(json!({"host":"127.0.0.1","port":unused_port()})).unwrap();
        config["storage"] = serde_yaml_ng::to_value(
            json!({"kind":"redis","redis":{"url":redis.url,"key_prefix":PREFIX}}),
        )
        .unwrap();
        config["iota"]["rpc_url"] = rpc.into();
        config["health_checks"]["enabled"] = false.into();
        config["backends"] = serde_yaml_ng::to_value(
            json!([{"name":"station-a", "rpc":backend,"token":"backend-test-token"}]),
        )
        .unwrap();
        let config = serde_yaml_ng::from_value(config).unwrap();
        let task = tokio::spawn(gas_station_router_v2::server::start(config));
        let router = Self {
            task,
            url: format!("http://127.0.0.1:{port}"),
        };
        timeout(Duration::from_secs(5), async {
            loop {
                assert!(
                    !router.task.is_finished(),
                    "router stopped before becoming ready"
                );
                if let Ok(response) = reqwest::get(format!("{}/health", router.url)).await
                    && response.status().is_success()
                {
                    break;
                }
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("router did not become ready");
        router
    }
}

#[tokio::test]
async fn two_http_routers_share_reservations_execute_once_and_read_cache_after_restart() {
    use iota_sdk_crypto::{IotaSigner, ed25519::Ed25519PrivateKey};
    use iota_sdk_types::{
        Address, GasPayment, ObjectDigest, ObjectId, ObjectReference, ProgrammableTransaction,
        Transaction, TransactionExpiration, TransactionKind, TransactionV1,
    };
    use rand::rngs::OsRng;
    use serde_json::Value;
    use wiremock::{
        Mock, MockServer, ResponseTemplate,
        matchers::{body_partial_json, header, method, path},
    };

    let redis = RedisServer::new().await;
    let backend = MockServer::start().await;
    let rpc = MockServer::start().await;
    let first = RunningRouter::start(&redis, &backend.uri(), &rpc.uri()).await;
    let second = RunningRouter::start(&redis, &backend.uri(), &rpc.uri()).await;
    Mock::given(method("POST")).and(path("/v1/reserve_gas")).and(header("authorization", "Bearer backend-test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":{
            "reservation_id":77,"sponsor_address":Address::new([1;32]),
            "gas_coins":[{"objectId":ObjectId::new([2;32]),"version":1,"digest":ObjectDigest::new([3;32])}]},"error":null})))
        .expect(1).mount(&backend).await;
    let client = reqwest::Client::new();
    let reply = client
        .post(format!("{}/v1/reserve_gas", first.url))
        .bearer_auth("router-test-token")
        .json(&json!({"gas_budget":1_000_000,"reserve_duration_secs":5}))
        .send()
        .await
        .unwrap();
    assert!(reply.status().is_success());
    let reservation: Value = reply.json().await.unwrap();
    let id = reservation["result"]["reservation_id"].as_u64().unwrap();
    let signer = Ed25519PrivateKey::generate(OsRng);
    let tx: Transaction = TransactionV1 {
        kind: TransactionKind::new_programmable(ProgrammableTransaction {
            inputs: vec![],
            commands: vec![],
        }),
        sender: signer.public_key().derive_address(),
        gas_payment: GasPayment {
            objects: vec![ObjectReference::new(
                ObjectId::new([2; 32]),
                1.into(),
                ObjectDigest::new([3; 32]),
            )],
            owner: Address::new([1; 32]),
            price: 1000,
            budget: 1_000_000,
        },
        expiration: TransactionExpiration::None,
    }
    .into();
    let digest = tx.digest();
    let effects = json!({"messageVersion":"v1", "transactionDigest":digest, "status":{"status":"success"},
        "executedEpoch":"1", "gasUsed":{"computationCost":"1", "storageCost":"0", "storageRebate":"0", "nonRefundableStorageFee":"0"},
        "gasObject":{"owner":{"AddressOwner":"0x1"},"reference":{"objectId":"0x2","version":1,"digest":ObjectDigest::new([3;32])}}});
    Mock::given(method("POST"))
        .and(path("/v1/execute_tx"))
        .and(header("authorization", "Bearer backend-test-token"))
        .and(body_partial_json(json!({"reservation_id":77})))
        .respond_with(
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis(40))
                .set_body_json(json!({"effects":effects,"error":null})),
        )
        .expect(1)
        .mount(&backend)
        .await;
    let request = json!({"reservation_id":id,"tx_bytes":tx.to_base64(),"user_sig":signer.sign_transaction(&tx).unwrap().to_base64()});
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..16 {
        let client = client.clone();
        let url = if index % 2 == 0 {
            &first.url
        } else {
            &second.url
        };
        let url = format!("{url}/v1/execute_tx");
        let request = request.clone();
        tasks.spawn(async move {
            let response = client
                .post(url)
                .bearer_auth("router-test-token")
                .json(&request)
                .send()
                .await
                .unwrap();
            assert!(response.status().is_success());
            response.json::<Value>().await.unwrap()
        });
    }
    while let Some(reply) = tasks.join_next().await {
        assert_eq!(reply.unwrap()["effects"], effects);
    }
    drop(first);
    let restarted = RunningRouter::start(&redis, &backend.uri(), &rpc.uri()).await;
    let response = client
        .post(format!("{}/v1/execute_tx", restarted.url))
        .bearer_auth("router-test-token")
        .json(&request)
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert_eq!(response.json::<Value>().await.unwrap()["effects"], effects);
    backend.verify().await;
    assert!(rpc.received_requests().await.unwrap().is_empty());
    let store = redis.store().await;
    assert_eq!(
        store.get_execution(id).await.unwrap().unwrap().phase,
        ExecutionPhase::Completed
    );
    for data_key in [
        key("reservation", id),
        key("execution", id),
        key("transaction", digest),
    ] {
        assert!(redis.pttl(&data_key).await > 0);
    }
}
