mod docker;
#[path = "../e2e_shared/metrics.rs"]
mod metrics;
#[path = "../e2e_shared/redis_audit.rs"]
mod redis_audit;
mod setup;
mod traffic;

use std::{path::PathBuf, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep};

use crate::common::{self, Process, Station};
use docker::Docker;
use redis_audit::RedisAudit;
use traffic::Traffic;

struct Fixture {
    routers: Vec<Process>,
    stations: Vec<Process>,
    docker: Docker,
    backends: Vec<Station>,
    directory: PathBuf,
    rpc: [u16; 2],
    metrics: [u16; 2],
    audit: RedisAudit,
    traffic: Traffic,
}

impl Fixture {
    async fn start() -> Result<Self> {
        let directory = common::directory("router")?;
        let ports = common::ports(6)?;
        let rpc = [ports[2], ports[3]];
        let metrics = [ports[4], ports[5]];
        let docker = Docker::start(&directory, [ports[0], ports[1], rpc[0], rpc[1]])?;
        let redis = format!("redis://127.0.0.1:{}/0", ports[0]);
        let mut audit = RedisAudit::new(&redis, "mock-e2e")?;
        audit.preflight().await?;
        let (backends, stations) = setup::stations(&directory).await?;
        let routers = setup::routers(&directory, &backends, &redis, rpc, metrics)?;
        let proxy = format!("http://127.0.0.1:{}", ports[1]);
        setup::summary(&directory, &backends, &proxy, &redis, rpc, metrics)?;
        let fixture = Self {
            routers,
            stations,
            docker,
            backends,
            directory,
            rpc,
            metrics,
            audit,
            traffic: Traffic::new(proxy)?,
        };
        fixture.wait_backend_health(3).await?;
        Ok(fixture)
    }

    async fn wait_backend_health(&self, healthy: u64) -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let mut observed = 0;
            for port in self.rpc {
                if let Ok(response) = self
                    .traffic
                    .client
                    .get(format!("http://127.0.0.1:{port}/status"))
                    .send()
                    .await
                    && let Ok(status) = response.json::<Value>().await
                    && status["summary"]["healthy"] == healthy
                {
                    observed += 1;
                }
            }
            if observed == 2 {
                println!("[ok] Both routers report {healthy} healthy stations");
                return Ok(());
            }
            ensure!(
                Instant::now() < deadline,
                "Both routers did not observe backend health={healthy}"
            );
            sleep(Duration::from_millis(100)).await;
        }
    }

    async fn traffic(&mut self, count: usize, excluded: Option<String>) -> Result<()> {
        for _ in 0..count {
            let sponsor = self.traffic.transaction().await?;
            ensure!(
                excluded.as_ref() != Some(&sponsor),
                "Failed backend accepted a reservation"
            );
            self.audit.observe().await?;
        }
        println!("[ok] {count} transactions executed and replayed through Nginx");
        Ok(())
    }
}

pub async fn run() -> Result<()> {
    let mut fixture = Fixture::start().await?;
    let result = scenario(&mut fixture).await;
    for router in &mut fixture.routers {
        router.stop()?;
    }
    if result.is_err() {
        fixture.audit.cleanup_stopped_run().await?;
    }
    result?;
    fixture.audit.remove_test_counter().await?;
    let reports = &fixture.audit.report.by_type;
    for (kind, report) in reports {
        println!(
            "[TTL] {}: observed={}, expired={}, remaining={}, TTL={}..{} ms",
            kind.label(),
            report.observed_keys,
            report.expired_keys,
            report.remaining_keys,
            report.min_observed_ttl_ms.unwrap_or(0),
            report.max_observed_ttl_ms.unwrap_or(0)
        );
    }
    for station in &mut fixture.stations {
        station.stop()?;
    }
    fixture.docker.stop()?;
    common::report(
        &fixture.directory,
        "router",
        json!({"transactions":fixture.traffic.transactions,"upstreams":fixture.traffic.upstreams,"redis":fixture.audit.report}),
    )
}

async fn scenario(fixture: &mut Fixture) -> Result<()> {
    fixture.traffic(12, None).await?;
    ensure!(
        fixture.traffic.sponsors.len() == 3,
        "Round robin did not reach all three sponsors"
    );
    fixture.stations[0].stop()?;
    fixture.wait_backend_health(2).await?;
    fixture
        .traffic(6, Some(fixture.backends[0].sponsor.clone()))
        .await?;
    fixture.stations[0] = fixture.backends[0].start()?;
    fixture.wait_backend_health(3).await?;
    redis_failure(fixture).await?;
    fixture.traffic(6, None).await?;
    let samples = metrics::sample(&fixture.traffic.client, fixture.metrics).await?;
    metrics::validate(&samples, fixture.traffic.transactions)?;
    ensure!(
        fixture.traffic.upstreams.len() == 2,
        "Nginx did not reach both routers"
    );
    let executed: usize = fixture
        .backends
        .iter()
        .map(|station| station.event_count("execute"))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .sum();
    ensure!(
        executed == fixture.traffic.transactions,
        "Cached retries caused duplicate Gas Station executions"
    );
    wait_for_expiry(&mut fixture.audit).await
}

async fn redis_failure(fixture: &mut Fixture) -> Result<()> {
    fixture
        .docker
        .run(&["kill", "--signal", "SIGKILL", "redis"])?;
    let request = fixture.traffic.last.as_ref().context("Missing execution")?;
    for port in fixture.rpc {
        let reply = fixture
            .traffic
            .client
            .post(format!("http://127.0.0.1:{port}/v1/execute_tx"))
            .bearer_auth(common::TOKEN)
            .json(request)
            .send()
            .await?;
        ensure!(
            reply.status().is_server_error(),
            "Redis outage did not fail closed"
        );
    }
    fixture.docker.run(&["start", "redis"])?;
    fixture.audit.wait_ready().await?;
    fixture.audit.observe().await?;
    fixture.traffic.replay().await?;
    println!("[ok] Redis crash preserved the cache; both routers reported the outage");
    Ok(())
}

async fn wait_for_expiry(audit: &mut RedisAudit) -> Result<()> {
    println!("[TTL] Waiting for reservation, execution and effects keys to expire naturally");
    loop {
        audit.observe().await?;
        if audit.all_data_expired() {
            break;
        }
        let deadline =
            audit.latest_expiry().context("No observed expiries")? + Duration::from_secs(3);
        ensure!(Instant::now() < deadline, "Data keys survived their TTL");
        sleep(Duration::from_millis(250)).await;
    }
    audit.finish().await
}
