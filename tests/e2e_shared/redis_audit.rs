use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use redis::{Client, aio::MultiplexedConnection};
use serde::Serialize;
use tokio::time::{Instant, timeout};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum KeyKind {
    Reservation,
    Execution,
    TransactionEffects,
}

impl KeyKind {
    fn from_suffix(suffix: &str) -> Result<Self> {
        if suffix.starts_with("reservation:") {
            Ok(Self::Reservation)
        } else if suffix.starts_with("execution:") {
            Ok(Self::Execution)
        } else if suffix.starts_with("transaction:") {
            Ok(Self::TransactionEffects)
        } else {
            anyhow::bail!("Unexpected test key: {suffix}")
        }
    }

    fn ttl_ms(self) -> u64 {
        match self {
            Self::Reservation => 20_000,
            Self::Execution => 40_000,
            Self::TransactionEffects => 30_000,
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Reservation => "reservation",
            Self::Execution => "execution",
            Self::TransactionEffects => "transaction effects",
        }
    }
}

#[derive(Default, Serialize)]
pub(super) struct KeyReport {
    pub expected_ttl_ms: u64,
    pub observed_keys: usize,
    pub expired_keys: usize,
    pub remaining_keys: usize,
    pub ttl_checks: usize,
    pub missing_ttl_checks: usize,
    pub excessive_ttl_checks: usize,
    pub renewed_ttl_checks: usize,
    pub min_observed_ttl_ms: Option<i64>,
    pub max_observed_ttl_ms: Option<i64>,
}

impl KeyReport {
    pub fn expiry_verified(&self) -> bool {
        self.observed_keys > 0
            && self.expired_keys == self.observed_keys
            && self.remaining_keys == 0
            && self.missing_ttl_checks == 0
            && self.excessive_ttl_checks == 0
            && self.renewed_ttl_checks == 0
    }
}

#[derive(Serialize)]
pub(super) struct AuditReport {
    pub observed_keys: usize,
    pub expired_keys: usize,
    pub snapshots: usize,
    pub redis_restarts: usize,
    pub evicted_keys: u64,
    pub ttl_wait_seconds: u64,
    pub persistent_counter_observed: bool,
    pub persistent_counter_removed: bool,
    pub namespace_empty_after_cleanup: bool,
    pub by_type: BTreeMap<KeyKind, KeyReport>,
}

impl Default for AuditReport {
    fn default() -> Self {
        Self {
            observed_keys: 0,
            expired_keys: 0,
            snapshots: 0,
            redis_restarts: 0,
            evicted_keys: 0,
            ttl_wait_seconds: 0,
            persistent_counter_observed: false,
            persistent_counter_removed: false,
            namespace_empty_after_cleanup: false,
            by_type: [
                KeyKind::Reservation,
                KeyKind::Execution,
                KeyKind::TransactionEffects,
            ]
            .into_iter()
            .map(|kind| {
                (
                    kind,
                    KeyReport {
                        expected_ttl_ms: kind.ttl_ms(),
                        ..KeyReport::default()
                    },
                )
            })
            .collect(),
        }
    }
}

struct ObservedKey {
    kind: KeyKind,
    expires_at: Instant,
}

pub(super) struct RedisAudit {
    client: Client,
    prefix: String,
    expiries: HashMap<String, ObservedKey>,
    sequence: u64,
    run_id: String,
    evictions: u64,
    pub report: AuditReport,
}

impl RedisAudit {
    pub fn new(url: &str, prefix: &str) -> Result<Self> {
        Ok(Self {
            client: Client::open(url).context("Invalid local Redis URL")?,
            prefix: format!("{prefix}:v2:"),
            expiries: HashMap::new(),
            sequence: 0,
            run_id: String::new(),
            evictions: 0,
            report: AuditReport::default(),
        })
    }

    async fn connect(&self) -> Result<MultiplexedConnection> {
        timeout(
            Duration::from_secs(3),
            self.client.get_multiplexed_async_connection(),
        )
        .await
        .context("Redis connection timed out")?
        .context("Local Redis is unavailable")
    }

    pub async fn wait_ready(&self) -> Result<()> {
        timeout(Duration::from_secs(30), async {
            loop {
                if self.connect().await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        })
        .await
        .context("Redis did not recover")?;
        Ok(())
    }

    async fn keys(&self, connection: &mut MultiplexedConnection) -> Result<Vec<String>> {
        let mut cursor = 0_u64;
        let mut keys = Vec::new();
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(format!("{}*", self.prefix))
                .arg("COUNT")
                .arg(100)
                .query_async(connection)
                .await?;
            keys.extend(batch);
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        keys.sort();
        keys.dedup();
        Ok(keys)
    }

    pub async fn preflight(&mut self) -> Result<()> {
        self.wait_ready().await?;
        let mut connection = self.connect().await?;
        ensure!(
            self.keys(&mut connection).await?.is_empty(),
            "Test namespace is already in use"
        );
        let policy: HashMap<String, String> = redis::cmd("CONFIG")
            .arg("GET")
            .arg("maxmemory-policy")
            .arg("appendonly")
            .arg("appendfsync")
            .query_async(&mut connection)
            .await?;
        ensure!(
            policy
                .get("maxmemory-policy")
                .is_some_and(|value| value == "noeviction"),
            "Redis must use noeviction"
        );
        ensure!(
            policy.get("appendonly").is_some_and(|value| value == "yes")
                && policy
                    .get("appendfsync")
                    .is_some_and(|value| value == "always"),
            "Crash tests require AOF with appendfsync always"
        );
        self.observe().await
    }

    pub async fn observe(&mut self) -> Result<()> {
        let mut connection = self.connect().await?;
        self.check_evictions(&mut connection).await?;
        let keys = self.keys(&mut connection).await?;
        let now = Instant::now();
        for report in self.report.by_type.values_mut() {
            report.remaining_keys = 0;
        }
        for key in &keys {
            let ttl: i64 = redis::cmd("PTTL")
                .arg(key)
                .query_async(&mut connection)
                .await?;
            if ttl == -2 {
                continue;
            } // Expired between SCAN and PTTL.
            if key == &format!("{}reservation_sequence", self.prefix) {
                ensure!(ttl == -1, "ID counter must stay persistent");
                self.report.persistent_counter_observed = true;
                let value: u64 = redis::cmd("GET")
                    .arg(key)
                    .query_async(&mut connection)
                    .await?;
                ensure!(
                    value >= self.sequence,
                    "Reservation IDs went backwards after restart"
                );
                self.sequence = value;
                continue;
            }
            self.observe_data_key(key, ttl, Instant::now())?;
        }
        self.count_expired(&keys.into_iter().collect(), now)?;
        self.report.snapshots += 1;
        Ok(())
    }

    fn observe_data_key(&mut self, key: &str, ttl: i64, now: Instant) -> Result<()> {
        let kind = KeyKind::from_suffix(
            key.strip_prefix(&self.prefix)
                .context("Key escaped test prefix")?,
        )?;
        let report = self
            .report
            .by_type
            .get_mut(&kind)
            .expect("all key types registered");
        report.remaining_keys += 1;
        report.ttl_checks += 1;
        report.min_observed_ttl_ms = Some(
            report
                .min_observed_ttl_ms
                .map_or(ttl, |previous| previous.min(ttl)),
        );
        report.max_observed_ttl_ms = Some(
            report
                .max_observed_ttl_ms
                .map_or(ttl, |previous| previous.max(ttl)),
        );
        if ttl < 0 {
            report.missing_ttl_checks += 1;
            anyhow::bail!("Missing TTL on {key}: {ttl}");
        }
        if ttl as u64 > report.expected_ttl_ms {
            report.excessive_ttl_checks += 1;
            anyhow::bail!("Excessive TTL on {key}: {ttl}");
        }
        let expiry = now + Duration::from_millis(ttl as u64);
        if let Some(previous) = self.expiries.get(key) {
            if expiry > previous.expires_at + Duration::from_secs(1) {
                report.renewed_ttl_checks += 1;
                anyhow::bail!("TTL was refreshed for {key}");
            }
        } else {
            self.expiries.insert(
                key.to_owned(),
                ObservedKey {
                    kind,
                    expires_at: expiry,
                },
            );
            report.observed_keys += 1;
            self.report.observed_keys += 1;
        }
        Ok(())
    }

    fn count_expired(&mut self, keys: &HashSet<String>, now: Instant) -> Result<()> {
        for report in self.report.by_type.values_mut() {
            report.expired_keys = 0;
            report.remaining_keys = 0;
        }
        for (key, observed) in &self.expiries {
            if keys.contains(key) {
                self.report
                    .by_type
                    .get_mut(&observed.kind)
                    .expect("all key types registered")
                    .remaining_keys += 1;
                continue;
            }
            ensure!(
                now + Duration::from_secs(1) >= observed.expires_at,
                "Key disappeared before its TTL expired: {key}"
            );
            if now >= observed.expires_at {
                self.report
                    .by_type
                    .get_mut(&observed.kind)
                    .expect("all key types registered")
                    .expired_keys += 1;
            }
        }
        self.report.expired_keys = self
            .report
            .by_type
            .values()
            .map(|report| report.expired_keys)
            .sum();
        Ok(())
    }

    pub fn all_data_expired(&self) -> bool {
        self.report.by_type.values().all(KeyReport::expiry_verified)
    }

    pub fn latest_expiry(&self) -> Option<Instant> {
        self.expiries
            .values()
            .map(|observed| observed.expires_at)
            .max()
    }

    async fn check_evictions(&mut self, connection: &mut MultiplexedConnection) -> Result<()> {
        let info: String = redis::cmd("INFO").query_async(connection).await?;
        let value = |name: &str| {
            info.lines()
                .find_map(|line| line.strip_prefix(&format!("{name}:")))
                .map(str::trim)
        };
        let run_id = value("run_id").context("Missing Redis run ID")?;
        let evictions = value("evicted_keys")
            .context("Missing eviction count")?
            .parse::<u64>()?;
        if self.run_id != run_id {
            if !self.run_id.is_empty() {
                self.report.redis_restarts += 1;
            }
            self.evictions = if self.run_id.is_empty() { evictions } else { 0 };
            self.run_id = run_id.to_owned();
        }
        self.report.evicted_keys += evictions.saturating_sub(self.evictions);
        ensure!(
            evictions == self.evictions,
            "Redis evicted keys during the scenario"
        );
        Ok(())
    }

    pub async fn finish(&mut self) -> Result<()> {
        self.observe().await?;
        let mut connection = self.connect().await?;
        let keys = self.keys(&mut connection).await?;
        ensure!(
            keys == [format!("{}reservation_sequence", self.prefix)],
            "Router data did not expire: {keys:?}"
        );
        self.validate_expiry()?;
        ensure!(
            self.report.redis_restarts == 1,
            "Expected one real Redis restart"
        );
        Ok(())
    }

    fn validate_expiry(&self) -> Result<()> {
        for (kind, report) in &self.report.by_type {
            ensure!(
                report.expiry_verified(),
                "Incomplete TTL verification for {}: observed={}, expired={}, remaining={}",
                kind.label(),
                report.observed_keys,
                report.expired_keys,
                report.remaining_keys
            );
        }
        Ok(())
    }

    /// Call only after both owned routers have stopped and finish proved that all
    /// data expired naturally. Remove just this run's counter, never data records.
    pub async fn remove_test_counter(&mut self) -> Result<()> {
        let mut connection = self.connect().await?;
        let counter = format!("{}reservation_sequence", self.prefix);
        ensure!(
            self.keys(&mut connection).await? == [counter.clone()],
            "Refusing cleanup while data records remain"
        );
        let _: u64 = redis::cmd("DEL")
            .arg(counter)
            .query_async(&mut connection)
            .await?;
        ensure!(
            self.keys(&mut connection).await?.is_empty(),
            "Test namespace was not removed"
        );
        self.report.persistent_counter_removed = true;
        self.report.namespace_empty_after_cleanup = true;
        Ok(())
    }

    pub async fn cleanup_stopped_run(&mut self) -> Result<()> {
        timeout(Duration::from_secs(45), async {
            loop {
                let mut connection = self.connect().await?;
                let keys = self.keys(&mut connection).await?;
                if keys.is_empty() {
                    self.report.namespace_empty_after_cleanup = true;
                    return Ok::<_, anyhow::Error>(());
                }
                if keys == [format!("{}reservation_sequence", self.prefix)] {
                    return self.remove_test_counter().await;
                }
                // Never hide a leak with DEL/FLUSHDB. Let owned data expire.
                for key in keys {
                    if key.ends_with(":reservation_sequence") {
                        continue;
                    }
                    let ttl: i64 = redis::cmd("PTTL")
                        .arg(&key)
                        .query_async(&mut connection)
                        .await?;
                    ensure!(ttl >= 0 || ttl == -2, "Leaked persistent data key: {key}");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        })
        .await
        .context("Test data did not expire during cleanup")?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audit() -> RedisAudit {
        RedisAudit::new("redis://127.0.0.1:1/0", "test").unwrap()
    }

    #[test]
    fn unknown_indexes_and_lease_keys_are_rejected() {
        assert!(KeyKind::from_suffix("lease:123").is_err());
        assert!(KeyKind::from_suffix("index:pending").is_err());
        assert_eq!(
            KeyKind::from_suffix("execution:123").unwrap().ttl_ms(),
            40_000
        );
    }

    #[test]
    fn each_type_must_expire_before_the_audit_can_pass() {
        let mut audit = audit();
        let now = Instant::now();
        let reservation = "test:v2:reservation:1".to_owned();
        let execution = "test:v2:execution:1".to_owned();
        let effects = "test:v2:transaction:digest".to_owned();
        for (key, ttl) in [
            (&reservation, 20_000),
            (&execution, 40_000),
            (&effects, 30_000),
        ] {
            audit.observe_data_key(key, ttl, now).unwrap();
        }
        let remaining = HashSet::from([execution, effects]);
        audit
            .count_expired(&remaining, now + Duration::from_secs(21))
            .unwrap();
        assert_eq!(audit.report.by_type[&KeyKind::Reservation].expired_keys, 1);
        assert_eq!(audit.report.by_type[&KeyKind::Execution].remaining_keys, 1);
        assert!(audit.validate_expiry().is_err());
        audit
            .count_expired(&HashSet::new(), now + Duration::from_secs(41))
            .unwrap();
        audit.validate_expiry().unwrap();
        assert_eq!(audit.report.observed_keys, 3);
        assert_eq!(audit.report.expired_keys, 3);
        assert!(audit.all_data_expired());
    }

    #[test]
    fn missing_type_cannot_silently_pass_the_expiry_check() {
        let mut audit = audit();
        let now = Instant::now();
        audit
            .observe_data_key("test:v2:reservation:1", 20_000, now)
            .unwrap();
        audit
            .count_expired(&HashSet::new(), now + Duration::from_secs(21))
            .unwrap();
        assert!(audit.validate_expiry().is_err());
        assert!(!audit.all_data_expired());
    }

    #[test]
    fn missing_excessive_and_renewed_ttls_are_reported_as_failures() {
        let mut audit = audit();
        let now = Instant::now();
        let key = "test:v2:reservation:1";
        assert!(audit.observe_data_key(key, -1, now).is_err());
        assert!(audit.observe_data_key(key, 20_001, now).is_err());
        audit.observe_data_key(key, 20_000, now).unwrap();
        assert!(
            audit
                .observe_data_key(key, 19_000, now + Duration::from_secs(5))
                .is_err()
        );
        let stats = &audit.report.by_type[&KeyKind::Reservation];
        assert_eq!(stats.missing_ttl_checks, 1);
        assert_eq!(stats.excessive_ttl_checks, 1);
        assert_eq!(stats.renewed_ttl_checks, 1);
        assert!(!stats.expiry_verified());
    }

    #[test]
    fn lost_keys_cannot_be_reported_as_natural_expiry() {
        let mut audit = audit();
        let now = Instant::now();
        audit
            .observe_data_key("test:v2:execution:1", 40_000, now)
            .unwrap();
        assert!(
            audit
                .count_expired(&HashSet::new(), now + Duration::from_secs(1))
                .is_err()
        );
        assert_eq!(audit.report.expired_keys, 0);
    }
}
