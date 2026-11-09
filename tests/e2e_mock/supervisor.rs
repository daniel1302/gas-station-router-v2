use std::{fs, path::PathBuf, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use tokio::time::sleep;

use crate::common::{self, Process, Station, wait_for};

struct Fixture {
    directory: PathBuf,
    state: PathBuf,
    station: Station,
    process: Process,
}

impl Fixture {
    async fn start() -> Result<Self> {
        let directory = common::directory("supervisor")?;
        let station = Station::new(&directory, "station", 1)?;
        let state = directory.join("supervisor");
        let config = directory.join("supervisor.yaml");
        let yaml = json!({"supervisor":{
            "health":{"interval":"150ms","timeout":"150ms","readiness_path":"/ready","unhealthy_threshold":3},
            "pool":{"metrics_interval":"150ms","timeout":"500ms","rpc_url":format!("{}/rpc",station.url),"sponsor_address":station.sponsor,"expected_chain_id":"mock-testnet"},
            "recovery":{"refresh_url":format!("{}/refresh",station.url),"refresh_grace_period":"500ms",
                "consistency":{"consecutive_failures":3},"aggression":{"ignore_locks_after":2,"max_full_rescans":5}},
            "process":{"binary":common::binary("mock-gas-station")?,"config":station.config,"state_dir":state,
                "restart_backoff":"200ms","startup_grace":"500ms","shutdown_timeout":"2s"}
        }});
        fs::write(&config, serde_yaml_ng::to_string(&yaml)?)?;
        let process = Process::start_with_env(
            "gas-station-router-v2",
            &[
                "supervisor",
                "--config-file",
                config.to_str().context("Supervisor config path")?,
            ],
            &directory.join("supervisor.log"),
            &[
                ("ALLOW_REINIT", "true"),
                ("DELETE_COIN_REGISTRY", "true"),
                ("IGNORE_LOCKS", "true"),
            ],
        )?;
        println!(
            "[setup] Supervisor PID {}, station {}, metrics {}, auth Bearer {}",
            process.pid,
            station.url,
            station.metrics_url,
            common::TOKEN
        );
        let fixture = Self {
            directory,
            state,
            station,
            process,
        };
        fixture.wait_ready().await?;
        Ok(fixture)
    }

    fn status(&self) -> Result<Value> {
        Ok(serde_json::from_slice(&fs::read(
            self.state.join("status.json"),
        )?)?)
    }

    async fn wait_ready(&self) -> Result<()> {
        common::wait_health(&self.station.url).await?;
        wait_for("supervisor observes a healthy, verified station", || {
            let Ok(status) = self.status() else {
                return Ok(false);
            };
            Ok(status["health"]["ready"] == true && status["pool"]["chain_verified"] == true)
        })
        .await
    }

    async fn pending(&self) -> Result<String> {
        wait_for("full rescan waits for manual approval", || {
            Ok(self.status()?["pending_recovery"]["id"].is_string())
        })
        .await?;
        Ok(self.status()?["pending_recovery"]["id"]
            .as_str()
            .context("Pending recovery ID")?
            .to_owned())
    }

    fn approve(&self, id: &str) -> Result<()> {
        fs::write(self.state.join("approve-recovery"), id)?;
        Ok(())
    }

    async fn crash(&self) -> Result<()> {
        let generation = self.status()?["generation"]
            .as_u64()
            .context("Generation")?;
        common::client()?
            .post(format!("{}/crash", self.station.url))
            .send()
            .await?
            .error_for_status()?;
        wait_for("crash restarts the owned station", || {
            Ok(self.status()?["generation"].as_u64().unwrap_or(0) > generation)
        })
        .await?;
        self.wait_ready().await
    }
}

pub async fn run() -> Result<()> {
    let mut fixture = Fixture::start().await?;
    ensure!(
        fixture.station.event_count("rescan")? == 0,
        "Inherited recovery flags bypassed supervisor policy"
    );
    readiness_and_crash(&fixture).await?;
    gentle_recovery(&fixture).await?;
    approval_and_activity_guards(&fixture).await?;
    lock_escalation(&fixture).await?;
    let pid = fixture.status()?["pid"].as_u64().context("Station PID")?;
    fixture.process.stop()?;
    ensure!(
        fixture.status()?["phase"] == "stopped",
        "Supervisor did not finish shutdown"
    );
    ensure!(
        unsafe { libc::kill(pid as i32, 0) } == -1,
        "Owned Gas Station survived supervisor shutdown"
    );
    common::report(
        &fixture.directory,
        "supervisor",
        json!({"crash_restarts":fixture.status()?["crash_restarts"],
        "refreshes":fixture.station.event_count("refresh")?,"rescans":fixture.station.event_count("rescan")?,
        "locks_respected":fixture.station.event_count("lock_blocked")?, "shutdown_reaped_child":true}),
    )
}

async fn readiness_and_crash(fixture: &Fixture) -> Result<()> {
    let generation = fixture.status()?["generation"]
        .as_u64()
        .context("Generation")?;
    fixture.station.set(json!({"ready":false}))?;
    wait_for("readiness failures restart a live process", || {
        Ok(fixture.status()?["generation"].as_u64().unwrap_or(0) > generation)
    })
    .await?;
    fixture.station.set(json!({"ready":true}))?;
    fixture.wait_ready().await?;
    fixture.crash().await
}

async fn gentle_recovery(fixture: &Fixture) -> Result<()> {
    fixture.station.set(json!({"unreleased":10}))?;
    sleep(Duration::from_millis(600)).await;
    ensure!(
        fixture.station.event_count("rescan")? == 0 && fixture.station.event_count("refresh")? == 0,
        "Unreleased coins caused an automatic recovery"
    );
    fixture
        .station
        .set(json!({"oversized":1,"available_coins":10,"available_balance":100}))?;
    wait_for("oversized coins try refresh first", || {
        Ok(fixture.station.event_count("refresh")? == 1)
    })
    .await?;
    wait_for("refresh reconciles the pool without a restart", || {
        Ok(fixture.status()?["pool"]["balance_divergence"] == 0.0)
    })
    .await?;
    sleep(Duration::from_millis(600)).await;
    ensure!(
        fixture.station.event_count("rescan")? == 0,
        "Gentle recovery rebuilt the registry"
    );
    fixture
        .station
        .set(json!({"include_activity":false,"available_coins":10,"available_balance":100}))?;
    sleep(Duration::from_millis(700)).await;
    ensure!(
        fixture.status()?["pool"]["balance_divergence"].is_null(),
        "Missing activity metrics were treated as idle"
    );
    Ok(())
}

async fn approval_and_activity_guards(fixture: &Fixture) -> Result<()> {
    fixture
        .station
        .set(json!({"include_activity":true,"refresh_heals":false}))?;
    let id = fixture.pending().await?;
    fixture.approve("stale-request-id")?;
    sleep(Duration::from_millis(400)).await;
    ensure!(
        fixture.station.event_count("rescan")? == 0,
        "Stale approval was accepted"
    );
    fixture.station.set(json!({"pending_executions":1}))?;
    wait_for("in-flight execution is observed", || {
        Ok(fixture.status()?["pool"]["pending_executions"] == 1.0)
    })
    .await?;
    fixture.approve(&id)?;
    sleep(Duration::from_millis(400)).await;
    ensure!(
        fixture.station.event_count("rescan")? == 0,
        "Active execution did not block recovery"
    );
    fixture.station.set(json!({"chain_id":"wrong-network"}))?;
    wait_for("network mismatch is detected", || {
        Ok(fixture.status()?["pool"]["network_mismatch"] == true)
    })
    .await?;
    fixture.station.set(json!({"pending_executions":0}))?;
    sleep(Duration::from_millis(400)).await;
    ensure!(
        fixture.station.event_count("rescan")? == 0,
        "Network mismatch did not block recovery"
    );
    fixture.station.set(json!({"valid_metrics":false}))?;
    sleep(Duration::from_millis(400)).await;
    ensure!(
        fixture.station.event_count("rescan")? == 0,
        "Failed scrape cleared a network blocker"
    );
    fixture
        .station
        .set(json!({"valid_metrics":true,"chain_id":"mock-testnet"}))?;
    wait_for("one approved rescan respects locks", || {
        Ok(fixture.station.event_count("rescan")? == 1)
    })
    .await?;
    ensure!(
        !fixture.state.join("approve-recovery").exists(),
        "Approval was not consumed"
    );
    fixture.wait_ready().await?;
    fixture.crash().await?;
    let starts = fixture.station.events()?;
    let last = starts
        .iter()
        .rev()
        .find(|event| event["event"] == "start")
        .context("Start event")?;
    ensure!(
        last["details"]["delete_coin_registry"] == false
            && last["details"]["ignore_locks"] == false,
        "Crash replayed destructive flags"
    );
    Ok(())
}

async fn lock_escalation(fixture: &Fixture) -> Result<()> {
    sleep(Duration::from_millis(600)).await;
    fixture
        .station
        .set(json!({"available_coins":10,"available_balance":100,"locked":true}))?;
    for expected_failures in 1..=2 {
        let id = fixture.pending().await?;
        fixture.approve(&id)?;
        wait_for("rescan refuses a held Gas Station lock", || {
            Ok(fixture.station.event_count("lock_blocked")? >= expected_failures)
        })
        .await?;
        wait_for("lock failure creates a new approval request", || {
            Ok(fixture.status()?["pending_recovery"]["id"]
                .as_str()
                .is_some_and(|next| next != id))
        })
        .await?;
    }
    let id = fixture.pending().await?;
    fixture.approve(&id)?;
    wait_for(
        "configured aggression bypasses locks after two failures",
        || {
            Ok(fixture.station.events()?.iter().any(|event| {
                event["event"] == "rescan" && event["details"]["ignore_locks"] == true
            }))
        },
    )
    .await?;
    fixture.wait_ready().await
}
