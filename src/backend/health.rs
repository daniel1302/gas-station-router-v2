use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use reqwest::{Client, StatusCode};
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval},
};

use super::registry::{Backend, BackendRegistry};
use crate::config::{HealthChecksConfig, PassiveHealthCheckConfig};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendHealth {
    /// Backend is available for new reservations.
    Healthy,

    /// Backend is reachable but temporarily unable
    /// to accept new reservations.
    Degraded,

    /// Backend is unreachable or repeatedly failing.
    Unhealthy,
}

/// Reservation health observations are independent of whether retrying is safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReservationOutcome {
    Success,
    BackendFailure,
    NoGasCoins,
    AuthorizationFailure(u16),
    Neutral,
}

/// ```text
///              Background Health Checker
///                         |
///                         | Every 5 seconds
///                         v
///               GET Gas Station /
///                         |
///                 +-------+-------+
///                 |               |
///               200 OK          Error
///                 |               |
///                 v               v
///             Success           Failure
///                 |               |
///                 v               v
///           success_count     failure_count
///                 |               |
///                 v               v
///              >= 2 ?          >= 3 ?
///                 |               |
///                 v               v
///              HEALTHY        UNHEALTHY
///                 |               |
///                 v               v
///           Eligible for      Excluded from
///            reserve_gas       new reserves
/// ```
///
/// Independent background task with a concurrent worker per enabled backend.
pub struct BackgroundHealthChecker {
    registry: Arc<BackendRegistry>,
    client: Client,
    config: HealthChecksConfig,
}

impl BackgroundHealthChecker {
    pub fn new(registry: Arc<BackendRegistry>, config: HealthChecksConfig) -> Result<Self> {
        if config.enabled {
            ensure!(
                !config.interval.is_zero(),
                "health_checks.interval must be greater than zero"
            );
            ensure!(
                !config.timeout.is_zero(),
                "health_checks.timeout must be greater than zero"
            );
            ensure!(
                config.unhealthy_threshold > 0,
                "health_checks.unhealthy_threshold must be greater than zero"
            );
            ensure!(
                config.healthy_threshold > 0,
                "health_checks.healthy_threshold must be greater than zero"
            );
        }
        let client = Client::builder()
            .timeout(config.timeout)
            .connect_timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .build()
            .context("failed to build health-check HTTP client")?;
        Ok(Self {
            registry,
            client,
            config,
        })
    }

    /// One concurrent round. A slow node does not delay another node's state update.
    pub async fn check_once(&self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        let mut checks = JoinSet::new();
        for backend in self.registry.enabled_backends() {
            let client = self.client.clone();
            let healthy_threshold = self.config.healthy_threshold;
            let unhealthy_threshold = self.config.unhealthy_threshold;
            checks.spawn(async move {
                probe(&backend, &client, healthy_threshold, unhealthy_threshold).await;
            });
        }
        while let Some(result) = checks.join_next().await {
            result.context("backend health-check task failed")?;
        }
        Ok(())
    }

    /// Each worker probes immediately, then on its own interval. Slow nodes cannot
    /// delay other workers; probes for the same node never overlap.
    /// Dropping this future aborts every worker and its outstanding HTTP request.
    pub async fn run(self, mut shutdown: watch::Receiver<()>) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }
        let mut checks = JoinSet::new();
        for backend in self.registry.enabled_backends() {
            let client = self.client.clone();
            let healthy_threshold = self.config.healthy_threshold;
            let unhealthy_threshold = self.config.unhealthy_threshold;
            let probe_interval = self.config.interval;
            checks.spawn(async move {
                let mut ticks = interval(probe_interval);
                ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
                loop {
                    ticks.tick().await;
                    probe(&backend, &client, healthy_threshold, unhealthy_threshold).await;
                }
            });
        }
        tokio::select! {
            biased;
            _ = shutdown.changed() => Ok(()),
            result = checks.join_next(), if !checks.is_empty() => {
                result.context("health-check worker was not started")?
                    .context("backend health-check worker failed")?;
                bail!("backend health-check worker stopped unexpectedly")
            }
        }
    }
}

async fn probe(
    backend: &Backend,
    client: &Client,
    healthy_threshold: u32,
    unhealthy_threshold: u32,
) {
    let success = backend
        .health_request(client)
        .send()
        .await
        .is_ok_and(|response| response.status() == StatusCode::OK);
    backend.record_health_result(success, healthy_threshold, unhealthy_threshold);
}

/// Updated under the backend's mutex, including explicit health changes from reservations.
pub(super) struct HealthState {
    health: BackendHealth,
    success_count: u32,
    failure_count: u32,
    reservation_failure_count: u32,
    // Retain the last cooldown after expiry to grow the next one until a successful reservation.
    cooldown: Option<Cooldown>,
}

struct Cooldown {
    started: Instant,
    duration: Duration,
}

impl HealthState {
    pub(super) fn new() -> Self {
        Self {
            health: BackendHealth::Healthy,
            success_count: 0,
            failure_count: 0,
            reservation_failure_count: 0,
            cooldown: None,
        }
    }

    fn cooldown_active(&self) -> bool {
        self.cooldown
            .as_ref()
            .is_some_and(|cooldown| cooldown.started.elapsed() < cooldown.duration)
    }

    pub(super) fn status(&self) -> BackendHealth {
        if self.health == BackendHealth::Unhealthy {
            BackendHealth::Unhealthy
        } else if self.cooldown_active() {
            BackendHealth::Degraded
        } else {
            BackendHealth::Healthy
        }
    }

    pub(super) fn set_healthy(&mut self, healthy: bool) {
        self.health = if healthy {
            BackendHealth::Healthy
        } else {
            BackendHealth::Unhealthy
        };
        self.success_count = 0;
        self.failure_count = 0;
    }

    pub(super) fn record(
        &mut self,
        success: bool,
        healthy_threshold: u32,
        unhealthy_threshold: u32,
    ) -> Option<BackendHealth> {
        let previous = self.status();
        if success {
            self.failure_count = 0;
            self.success_count = self.success_count.saturating_add(1);
            if self.success_count >= healthy_threshold {
                self.health = BackendHealth::Healthy;
            }
        } else {
            self.success_count = 0;
            self.failure_count = self.failure_count.saturating_add(1);
            if self.failure_count >= unhealthy_threshold {
                self.health = BackendHealth::Unhealthy;
            }
        }
        let current = self.status();
        (current != previous).then_some(current)
    }

    pub(super) fn record_reservation(
        &mut self,
        outcome: ReservationOutcome,
        config: &PassiveHealthCheckConfig,
    ) -> Option<BackendHealth> {
        if !config.enabled {
            return None;
        }
        let previous = self.status();
        match outcome {
            ReservationOutcome::Success => {
                // An already in-flight success must not undo a newer cooldown
                // or bypass the background checker's recovery threshold.
                self.reservation_failure_count = 0;
                if !self.cooldown_active() {
                    self.cooldown = None;
                }
            }
            ReservationOutcome::BackendFailure => {
                self.reservation_failure_count = self.reservation_failure_count.saturating_add(1);
                if self.reservation_failure_count >= config.failure_threshold {
                    self.health = BackendHealth::Unhealthy;
                    self.success_count = 0;
                }
            }
            ReservationOutcome::NoGasCoins => {
                let duration = self
                    .cooldown
                    .as_ref()
                    .map_or(config.initial_cooldown, |last| {
                        last.duration
                            .saturating_mul(config.backoff)
                            .min(config.max_cooldown)
                    });
                self.cooldown = Some(Cooldown {
                    started: Instant::now(),
                    duration,
                });
            }
            ReservationOutcome::AuthorizationFailure(_) | ReservationOutcome::Neutral => {}
        }
        let current = self.status();
        (current != previous).then_some(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cooldown_growth_cannot_overflow_and_still_respects_the_cap() {
        let config = PassiveHealthCheckConfig {
            initial_cooldown: Duration::MAX / 2,
            backoff: u32::MAX,
            max_cooldown: Duration::MAX - Duration::from_secs(1),
            ..PassiveHealthCheckConfig::default()
        };
        let mut state = HealthState::new();
        state.record_reservation(ReservationOutcome::NoGasCoins, &config);
        assert_eq!(state.status(), BackendHealth::Degraded);
        state.record_reservation(ReservationOutcome::NoGasCoins, &config);
        assert_eq!(
            state.cooldown.as_ref().unwrap().duration,
            config.max_cooldown
        );
        state.record_reservation(ReservationOutcome::NoGasCoins, &config);
        assert_eq!(
            state.cooldown.as_ref().unwrap().duration,
            config.max_cooldown
        );
        assert_eq!(state.status(), BackendHealth::Degraded);
    }
}
