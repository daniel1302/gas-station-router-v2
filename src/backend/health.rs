use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail, ensure};
use reqwest::Client;
use serde::Serialize;
use tokio::{
    sync::watch,
    task::JoinSet,
    time::{Instant, MissedTickBehavior, interval},
};

use super::registry::{Backend, BackendRegistry};
use crate::{
    config::{HealthChecksConfig, PassiveHealthCheckConfig},
    metrics,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BackendHealth {
    /// Backend is available for new reservations.
    Healthy,

    /// Backend is reachable but temporarily unable
    /// to accept new reservations.
    Degraded,

    /// Backend is unreachable or repeatedly failing.
    Unhealthy,
}

impl BackendHealth {
    fn label(self) -> &'static str {
        match self {
            Self::Healthy => "healthy",
            Self::Degraded => "degraded",
            Self::Unhealthy => "unhealthy",
        }
    }
}

/// Reservation health observations are independent of whether retrying is safe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReservationOutcome {
    Success,
    BackendFailure(HealthCheckOutcome),
    NoGasCoins,
    AuthorizationFailure(u16),
    Neutral,
}

/// Bounded observations retain the cause without logging request URLs or bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HealthCheckOutcome {
    HttpStatus(u16),
    ConnectionRefused,
    ConnectionTimeout,
    RequestTimeout,
    ConnectionError,
    TransportError,
}

impl HealthCheckOutcome {
    pub(crate) fn from_error(error: &reqwest::Error) -> Self {
        if error.is_timeout() {
            return if error.is_connect() {
                Self::ConnectionTimeout
            } else {
                Self::RequestTimeout
            };
        }
        let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(error);
        while let Some(error) = cause {
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::ConnectionRefused)
            {
                return Self::ConnectionRefused;
            }
            cause = error.source();
        }
        if error.is_connect() {
            Self::ConnectionError
        } else {
            Self::TransportError
        }
    }

    pub(crate) fn log_fields(self) -> (&'static str, Option<u16>) {
        match self {
            Self::HttpStatus(200) => ("http_ok", Some(200)),
            Self::HttpStatus(status) => ("http_error", Some(status)),
            Self::ConnectionRefused => ("connection_refused", None),
            Self::ConnectionTimeout => ("connection_timeout", None),
            Self::RequestTimeout => ("request_timeout", None),
            Self::ConnectionError => ("connection_error", None),
            Self::TransportError => ("transport_error", None),
        }
    }
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
    let outcome = match backend.health_request(client).send().await {
        Ok(response) => HealthCheckOutcome::HttpStatus(response.status().as_u16()),
        Err(error) => HealthCheckOutcome::from_error(&error),
    };
    backend.record_health_result(outcome, healthy_threshold, unhealthy_threshold);
}

/// Updated under the backend's mutex, including explicit health changes from reservations.
pub(super) struct HealthState {
    health: BackendHealth,
    last_logged_health: BackendHealth,
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

#[derive(Serialize)]
pub(super) struct HealthSnapshot {
    pub health: BackendHealth,
    active_success_count: u32,
    active_failure_count: u32,
    reservation_failure_count: u32,
    cooldown: Option<CooldownSnapshot>,
}

#[derive(Serialize)]
struct CooldownSnapshot {
    active: bool,
    duration_ms: u64,
    remaining_ms: u64,
    next_duration_ms: u64,
}

fn milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

impl HealthState {
    pub(super) fn new() -> Self {
        Self {
            health: BackendHealth::Healthy,
            last_logged_health: BackendHealth::Healthy,
            success_count: 0,
            failure_count: 0,
            reservation_failure_count: 0,
            cooldown: None,
        }
    }

    fn cooldown_active(&self) -> bool {
        !self.cooldown_remaining(Instant::now()).is_zero()
    }

    fn cooldown_remaining(&self, now: Instant) -> Duration {
        self.cooldown.as_ref().map_or(Duration::ZERO, |cooldown| {
            cooldown
                .duration
                .saturating_sub(now.saturating_duration_since(cooldown.started))
        })
    }

    pub(super) fn status(&self) -> BackendHealth {
        self.status_at(Instant::now())
    }

    fn status_at(&self, now: Instant) -> BackendHealth {
        if self.health == BackendHealth::Unhealthy {
            BackendHealth::Unhealthy
        } else if !self.cooldown_remaining(now).is_zero() {
            BackendHealth::Degraded
        } else {
            BackendHealth::Healthy
        }
    }

    pub(super) fn snapshot(&self, config: &PassiveHealthCheckConfig) -> HealthSnapshot {
        let now = Instant::now();
        let remaining = self.cooldown_remaining(now);
        HealthSnapshot {
            health: self.status_at(now),
            active_success_count: self.success_count,
            active_failure_count: self.failure_count,
            reservation_failure_count: self.reservation_failure_count,
            cooldown: self.cooldown.as_ref().map(|cooldown| CooldownSnapshot {
                active: !remaining.is_zero(),
                duration_ms: milliseconds(cooldown.duration),
                remaining_ms: milliseconds(remaining),
                next_duration_ms: milliseconds(
                    cooldown
                        .duration
                        .saturating_mul(config.backoff)
                        .min(config.max_cooldown),
                ),
            }),
        }
    }

    /// Observe lazy cooldown expiry before applying another outcome, so a later
    /// HTTP response cannot be incorrectly reported as the cause of recovery.
    pub(super) fn log_cooldown_expiry(&mut self, backend: &str) {
        self.log_transition(backend, "cooldown", "cooldown_expired", None, None);
    }

    pub(super) fn log_transition(
        &mut self,
        backend: &str,
        source: &'static str,
        reason: &'static str,
        http_status: Option<u16>,
        threshold: Option<u32>,
    ) {
        let health = self.status();
        let previous_health = self.last_logged_health;
        if health == previous_health {
            return;
        }
        self.last_logged_health = health;
        metrics::inc_backend_health_transition(backend, previous_health.label(), health.label());
        let cooldown_ms = self
            .cooldown
            .as_ref()
            .map(|cooldown| milliseconds(cooldown.duration));
        let cooldown_remaining_ms = self.cooldown.as_ref().map(|cooldown| {
            milliseconds(cooldown.duration.saturating_sub(cooldown.started.elapsed()))
        });
        macro_rules! log {
            ($level:ident) => {
                tracing::$level!(
                    backend = %backend,
                    ?previous_health,
                    ?health,
                    source,
                    reason,
                    http_status,
                    active_success_count = self.success_count,
                    active_failure_count = self.failure_count,
                    reservation_failure_count = self.reservation_failure_count,
                    threshold,
                    cooldown_ms,
                    cooldown_remaining_ms,
                    "Backend health changed"
                );
            };
        }
        if health == BackendHealth::Healthy {
            log!(info);
        } else {
            log!(warn);
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

    pub(super) fn active_failure_count(&self) -> u32 {
        self.failure_count
    }

    pub(super) fn record(
        &mut self,
        success: bool,
        healthy_threshold: u32,
        unhealthy_threshold: u32,
    ) {
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
    }

    pub(super) fn record_reservation(
        &mut self,
        outcome: ReservationOutcome,
        config: &PassiveHealthCheckConfig,
    ) {
        if !config.enabled {
            return;
        }
        match outcome {
            ReservationOutcome::Success => {
                // An already in-flight success must not undo a newer cooldown
                // or bypass the background checker's recovery threshold.
                self.reservation_failure_count = 0;
                if !self.cooldown_active() {
                    self.cooldown = None;
                }
            }
            ReservationOutcome::BackendFailure(_) => {
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
