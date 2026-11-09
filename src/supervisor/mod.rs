//! Own one Gas Station process and recover conservatively from observed failures.
pub mod config;
mod health_monitor;
mod pool_monitor;
mod process_controller;
mod recovery_policy;

use std::{
    fs::{self, File},
    path::PathBuf,
    time::Duration,
};

use anyhow::{Context, Result};
use serde_json::json;
use tokio::{
    sync::mpsc,
    time::{Instant, MissedTickBehavior},
};

use config::{Config, StationConfig};
use health_monitor::HealthMonitor;
use pool_monitor::PoolMonitor;
use process_controller::ProcessController;
use recovery_policy::{Action, RecoveryPolicy, Signal};

/// Runs until SIGINT/SIGTERM, then terminates and reaps the owned process group.
/// Approval authorizes one full rescan; subsequent rescans need new approval.
pub async fn run(mut config: Config, approve_recovery: bool) -> Result<()> {
    config.validate()?;
    config.process.binary = process_controller::absolute(&config.process.binary)?;
    config.process.config = process_controller::absolute(&config.process.config)?;
    let station = StationConfig::load(&config.process.config)?;
    let state = StateDirectory::open(config.process.state_dir.clone())?;
    let mut runtime = Runtime::new(config, station, state, approve_recovery)?;
    let result = tokio::select! {
        result = runtime.work() => result,
        result = shutdown_signal() => result,
    };
    let stopped = runtime
        .process
        .stop(runtime.process_config.shutdown_timeout)
        .await;
    runtime.write_status("stopped")?;
    result.and(stopped)
}

struct Runtime {
    process: ProcessController,
    process_config: config::ProcessConfig,
    health: HealthMonitor,
    pool: PoolMonitor,
    policy: RecoveryPolicy,
    state: StateDirectory,
    events: mpsc::Sender<(u64, Signal)>,
    receiver: mpsc::Receiver<(u64, Signal)>,
    health_interval: Duration,
    pool_interval: Duration,
    next_restart: Instant,
    crash_restarts: u64,
    approved: bool,
}

impl Runtime {
    fn new(
        config: Config,
        station: StationConfig,
        state: StateDirectory,
        approved: bool,
    ) -> Result<Self> {
        let (events, receiver) = mpsc::channel(64);
        let health_interval = config.health.interval;
        let pool_interval = config.pool.metrics_interval;
        let health = HealthMonitor::new(
            config.health,
            station.url(station.rpc_port),
            config.process.startup_grace,
        )?;
        let pool = PoolMonitor::new(
            config.pool,
            format!("{}/metrics", station.url(station.metrics_port)),
        )?;
        let policy = RecoveryPolicy::new(
            config.recovery,
            station.refresh_interval(),
            pool_interval.saturating_mul(3),
            approved,
        );
        Ok(Self {
            process: ProcessController::new(),
            process_config: config.process,
            health,
            pool,
            policy,
            state,
            events,
            receiver,
            health_interval,
            pool_interval,
            next_restart: Instant::now(),
            crash_restarts: 0,
            approved,
        })
    }

    async fn work(&mut self) -> Result<()> {
        self.start(None)?;
        let mut health_tick = interval(self.health_interval);
        let mut pool_tick = interval(self.pool_interval);
        let mut tick = interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = health_tick.tick(), if self.process.pid().is_some() => self.health.check().await,
                _ = pool_tick.tick(), if self.process.pid().is_some() => {
                    match self.pool.sample().await {
                        Ok(sample) => self.policy.observe(sample),
                        Err(_) => {
                            tracing::warn!(reason = "pool_sample_failed", "Cannot verify Gas Station pool; automatic rescan remains blocked");
                            self.policy.missing_sample();
                        }
                    }
                }
                Some((generation, signal)) = self.receiver.recv() => {
                    if generation == self.process.generation {
                        tracing::warn!(?signal, "Gas Station reported a recovery signal");
                        self.policy.signal(signal);
                    }
                }
                _ = tick.tick() => self.maintain().await?,
            }
            self.write_status("running")?;
        }
    }

    async fn maintain(&mut self) -> Result<()> {
        if let Some(exit) = self.process.exited()? {
            tracing::warn!(%exit, "Gas Station exited");
            self.next_restart = Instant::now() + self.process_config.restart_backoff;
            if !self.process_config.restart_on_crash {
                anyhow::bail!("Gas Station exited and restart_on_crash is disabled");
            }
        }
        let approved = self.approved || self.state.approved(&self.policy)?;
        if let Some(action) = self.policy.decide(approved) {
            self.recover(action).await?;
        }
        if self.process.pid().is_none() && Instant::now() >= self.next_restart {
            self.crash_restarts += 1;
            self.start(None)?;
        } else if self.health.unhealthy() {
            tracing::warn!(
                reason = "health_threshold",
                "Restarting unhealthy Gas Station"
            );
            self.process
                .stop(self.process_config.shutdown_timeout)
                .await?;
            self.health.reset();
            self.next_restart = Instant::now() + self.process_config.restart_backoff;
        }
        Ok(())
    }

    async fn recover(&mut self, action: Action) -> Result<()> {
        match action {
            Action::Refresh => {
                if let Some(url) = &self.policy.config.refresh_url {
                    let result = http_client(self.pool_interval)?.post(url).send().await;
                    if !result.is_ok_and(|response| response.status().is_success()) {
                        tracing::warn!(
                            reason = "refresh_failed",
                            "Refresh hook failed; retaining non-destructive recovery"
                        );
                        self.policy.refresh_failed();
                        return Ok(());
                    }
                    tracing::info!("Refresh requested; waiting for pool observations");
                } else {
                    tracing::info!(
                        "Waiting for the Gas Station scheduled refresh; no refresh HTTP API is configured"
                    );
                }
                self.policy.refreshed();
            }
            Action::FullRescan { ignore_locks } => {
                self.state.consume_approval()?;
                self.approved = false;
                tracing::warn!(
                    ignore_locks,
                    "Starting full rescan; recovery flags apply to this launch only"
                );
                self.process
                    .stop(self.process_config.shutdown_timeout)
                    .await?;
                self.start(Some(ignore_locks))?;
                self.policy.rescanned();
            }
        }
        Ok(())
    }

    fn start(&mut self, recovery: Option<bool>) -> Result<()> {
        self.process
            .start(&self.process_config, recovery, self.events.clone())?;
        self.health.reset();
        self.pool.reset();
        self.policy.missing_sample();
        Ok(())
    }

    fn write_status(&self, phase: &str) -> Result<()> {
        let status = json!({
            "phase": phase, "pid": self.process.pid(), "generation": self.process.generation,
            "crash_restarts": self.crash_restarts, "health": self.health.status,
            "pool": self.policy.latest, "consecutive_divergences": self.policy.failures,
            "refresh_attempts": self.policy.refreshes, "full_rescan_attempts": self.policy.full_rescans,
            "pending_recovery": self.policy.pending, "last_signal": self.policy.last_signal,
        });
        let temporary = self.state.path.join("status.json.tmp");
        fs::write(&temporary, serde_json::to_vec_pretty(&status)?)?;
        fs::rename(temporary, self.state.path.join("status.json"))
            .context("Cannot update supervisor status")
    }
}

struct StateDirectory {
    path: PathBuf,
    _lock: File,
}

impl StateDirectory {
    fn open(path: PathBuf) -> Result<Self> {
        fs::create_dir_all(&path).context("Cannot create supervisor state directory")?;
        let lock = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join("supervisor.lock"))?;
        lock.try_lock()
            .context("Another supervisor owns this state directory")?;
        Ok(Self { path, _lock: lock })
    }

    fn approved(&self, policy: &RecoveryPolicy) -> Result<bool> {
        let Some(pending) = &policy.pending else {
            return Ok(false);
        };
        match fs::read_to_string(self.path.join("approve-recovery")) {
            Ok(value) => Ok(value.trim() == pending.id),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error).context("Cannot read recovery approval"),
        }
    }

    fn consume_approval(&self) -> Result<()> {
        match fs::remove_file(self.path.join("approve-recovery")) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error).context("Cannot consume recovery approval"),
        }
    }
}

fn interval(duration: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(duration);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    interval
}

fn http_client(timeout: Duration) -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .timeout(timeout)
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .build()?)
}

async fn shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! { result = tokio::signal::ctrl_c() => result.context("Cannot handle Ctrl+C"), _ = terminate.recv() => Ok(()) }
}
