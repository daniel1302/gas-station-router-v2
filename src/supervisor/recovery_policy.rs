use std::time::Duration;

use serde::Serialize;
use tokio::time::Instant;
use uuid::Uuid;

use super::{config::RecoveryConfig, pool_monitor::PoolSnapshot};

#[cfg(test)]
mod tests;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Signal {
    StaleObject,
    InvariantViolation,
    Divergence,
    LockBusy,
    ReinitializationRequired,
}

impl Signal {
    pub fn from_output(text: &str) -> Option<Self> {
        if text.contains("Could not find the referenced object") {
            Some(Self::StaleObject)
        } else if text.contains("Another instance is already performing maintenance")
            || text.contains("Another task is already initializing")
        {
            Some(Self::LockBusy)
        } else if text.contains("reinitialization is not allowed") {
            Some(Self::ReinitializationRequired)
        } else if text.contains("Invariant violation") {
            Some(Self::InvariantViolation)
        } else if text.contains("divergence detected") {
            Some(Self::Divergence)
        } else {
            None
        }
    }
}

#[derive(Serialize)]
pub(super) struct PendingRecovery {
    pub id: String,
    pub reason: &'static str,
}

pub(super) enum Action {
    Refresh,
    FullRescan { ignore_locks: bool },
}

pub(super) struct RecoveryPolicy {
    pub config: RecoveryConfig,
    pub pending: Option<PendingRecovery>,
    pub full_rescans: u32,
    pub refreshes: u32,
    pub last_signal: Option<Signal>,
    pub failures: u32,
    pub latest: Option<PoolSnapshot>,
    next_action: Instant,
    observed_at: Option<Instant>,
    freshness: Duration,
    refresh_wait: Duration,
    refresh_needed: bool,
    lock_failures: u32,
    network_blocked: bool,
    healthy_samples: u32,
    activity_blocked: bool,
}

impl RecoveryPolicy {
    pub fn new(
        config: RecoveryConfig,
        native_refresh: Duration,
        freshness: Duration,
        approved: bool,
    ) -> Self {
        let refresh_wait = if config.refresh_url.is_some() {
            config.refresh_grace_period
        } else {
            native_refresh.saturating_add(config.refresh_grace_period)
        };
        let pending = approved.then(|| PendingRecovery {
            id: Uuid::new_v4().to_string(),
            reason: "operator_requested_rescan",
        });
        Self {
            config,
            pending,
            full_rescans: 0,
            refreshes: 0,
            last_signal: None,
            failures: 0,
            latest: None,
            next_action: Instant::now(),
            observed_at: None,
            freshness,
            refresh_wait,
            refresh_needed: false,
            lock_failures: 0,
            network_blocked: false,
            healthy_samples: 0,
            activity_blocked: false,
        }
    }

    pub fn signal(&mut self, signal: Signal) {
        self.last_signal = Some(signal);
        match signal {
            Signal::LockBusy => {
                self.lock_failures = self.lock_failures.saturating_add(1);
                if self.full_rescans > 0 {
                    self.request_rescan("rescan_lock_busy");
                }
            }
            Signal::ReinitializationRequired => {
                self.request_rescan("station_requires_reinitialization")
            }
            _ => self.refresh_needed = true,
        }
    }

    pub fn observe(&mut self, pool: PoolSnapshot) {
        if pool.active() || pool.idle() {
            self.activity_blocked = pool.active();
        }
        if pool.unreleased_growth {
            tracing::warn!(
                reason = "unreleased_coins",
                "Verify object ownership before scheduling a full rescan"
            );
        }
        self.refresh_needed |= pool.invariant_growth || pool.oversized_growth;
        let divergent = pool
            .balance_divergence
            .is_some_and(|value| value > self.config.consistency.balance_divergence_threshold)
            || pool.coin_count_divergence.is_some_and(|value| {
                value > self.config.consistency.coin_count_divergence_threshold
            });
        self.failures = if divergent {
            self.failures.saturating_add(1)
        } else {
            0
        };
        let consistent = pool.idle()
            && pool.chain_verified
            && pool.balance_divergence.is_some()
            && pool.coin_count_divergence.is_some()
            && !divergent
            && !pool.invariant_growth
            && !pool.oversized_growth;
        self.healthy_samples = if consistent {
            self.healthy_samples.saturating_add(1)
        } else {
            0
        };
        if self.healthy_samples >= self.config.consistency.consecutive_failures {
            self.refreshes = 0;
            self.refresh_needed = false;
            if self
                .pending
                .as_ref()
                .is_some_and(|pending| pending.reason == "persistent_registry_network_divergence")
            {
                self.pending = None;
            }
            if self.pending.is_none() {
                self.lock_failures = 0;
            }
        }
        if self.failures >= self.config.consistency.consecutive_failures {
            self.refresh_needed = true;
        }
        if pool.network_mismatch {
            tracing::error!(
                reason = "network_changed",
                "Recovery blocked: verify Gas Station and fullnode network configuration"
            );
        }
        if pool.network_mismatch || pool.chain_verified {
            self.network_blocked = pool.network_mismatch;
        }
        self.latest = Some(pool);
        self.observed_at = Some(Instant::now());
    }

    pub fn missing_sample(&mut self) {
        self.latest = None;
        self.observed_at = None;
        self.failures = 0;
    }

    pub fn decide(&mut self, approved: bool) -> Option<Action> {
        if Instant::now() < self.next_action || self.network_blocked {
            return None;
        }
        let fresh = self
            .observed_at
            .is_some_and(|observed| observed.elapsed() <= self.freshness);
        // Known in-flight work remains a blocker even if the next scrape failed.
        if self.activity_blocked {
            return None;
        }
        if self.refresh_needed
            && self.config.auto_refresh
            && self.refreshes < self.config.aggression.refresh_attempts_before_rescan
        {
            self.refresh_needed = false;
            return Some(Action::Refresh);
        }
        if self.failures >= self.config.consistency.consecutive_failures
            && (!self.config.auto_refresh
                || self.refreshes >= self.config.aggression.refresh_attempts_before_rescan)
        {
            self.request_rescan("persistent_registry_network_divergence");
        }
        self.pending.as_ref()?;
        let automatically_approved =
            self.config.auto_full_rescan && !self.config.require_manual_approval;
        let sample = self.latest.as_ref().filter(|_| fresh);
        if !approved
            && (!automatically_approved
                || !sample.is_some_and(|sample| sample.idle() && sample.chain_verified))
        {
            return None;
        }
        if self.full_rescans >= self.config.aggression.max_full_rescans {
            return None;
        }
        let after = self.config.aggression.ignore_locks_after;
        let ignore_locks = after > 0 && self.lock_failures >= after;
        Some(Action::FullRescan { ignore_locks })
    }

    pub fn refreshed(&mut self) {
        self.refreshes = self.refreshes.saturating_add(1);
        self.failures = 0;
        self.next_action = Instant::now() + self.refresh_wait;
    }

    pub fn refresh_failed(&mut self) {
        self.refresh_needed = true;
        self.next_action = Instant::now() + self.config.refresh_grace_period;
    }

    pub fn rescanned(&mut self) {
        self.full_rescans = self.full_rescans.saturating_add(1);
        self.pending = None;
        self.refresh_needed = false;
        self.failures = 0;
        self.next_action = Instant::now() + self.config.refresh_grace_period;
        self.missing_sample();
    }

    fn request_rescan(&mut self, reason: &'static str) {
        if self.pending.is_none() {
            let pending = PendingRecovery {
                id: Uuid::new_v4().to_string(),
                reason,
            };
            tracing::warn!(recovery_id = %pending.id, reason, "Full rescan requested; checking approval and activity guards");
            self.pending = Some(pending);
        }
    }
}
