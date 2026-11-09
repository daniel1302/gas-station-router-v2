//! Live Testnet scenario. Faults target only the repository's local
//! Compose services. Router data is checked in a unique namespace, never flushed.

use std::{fs, path::Path, time::Duration};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use tokio::time::{Instant, sleep, sleep_until, timeout};

use crate::cluster::{self, Cluster, Settings};
use compose::Compose;
use redis_audit::RedisAudit;
use traffic::Traffic;

mod compose;
#[path = "../../e2e_shared/metrics.rs"]
mod metrics;
#[path = "../../e2e_shared/redis_audit.rs"]
mod redis_audit;
mod summary;
mod traffic;

pub(super) async fn run(env_file: &Path) -> Result<()> {
    let started = Instant::now();
    let runtime = Path::new(cluster::ROOT).join("docker/.runtime");
    fs::create_dir_all(&runtime)?;
    let lock = fs::File::options()
        .write(true)
        .create(true)
        .truncate(false)
        .open(runtime.join("full-e2e.lock"))?;
    lock.try_lock()
        .context("Another full e2e scenario is already running")?;
    cluster::build().await?;
    let env_file = fs::canonicalize(env_file).context("Full e2e requires an existing .env")?;
    let base = Compose::local(&env_file);
    base.start_services().await?;
    let id = uuid::Uuid::new_v4().simple().to_string();
    let directory = Path::new(cluster::ROOT).join(format!("docker/.runtime/full-{id}"));
    fs::create_dir_all(&directory)?;
    let ports = cluster::unused_ports()?;
    let proxy = Compose::proxy(&env_file, &directory, [ports[0], ports[1], ports[2]], &id)?;
    let redis_url = base.redis_url().await?;
    let prefix = format!("gas-station-router-full-{id}");
    let mut audit = RedisAudit::new(&redis_url, &prefix)?;
    let settings = Settings {
        rpc: [ports[1], ports[2]],
        metrics: [ports[3], ports[4]],
        directory: directory.clone(),
        redis_url,
        prefix: Some(prefix),
    };
    let mut routers = None;
    let mut setup = None;
    println!("[setup] Reports and logs: {}", directory.display());
    let result = tokio::select! {
        result = scenario(&base, &proxy, &mut routers, &mut audit, settings, ports[0], &mut setup) => result,
        _ = cluster::shutdown_signal() => Err(anyhow::anyhow!("Full e2e interrupted")),
    };
    let cleanup = cleanup(&base, &proxy, &mut routers, &mut audit, &directory).await;
    let report = match &result {
        Ok(report) => report.clone(),
        Err(error) => json!({"status":"failed", "error":format!("{error:#}")}),
    };
    let mut report = report;
    report["status"] = if result.is_ok() && cleanup.is_ok() {
        "passed"
    } else {
        "failed"
    }
    .into();
    report["total_duration_seconds"] = started.elapsed().as_secs().into();
    report["redis"] = serde_json::to_value(&audit.report)?;
    report["setup"] = serde_json::to_value(&setup)?;
    report["cleanup_succeeded"] = cleanup.is_ok().into();
    if let Err(error) = &cleanup {
        report["cleanup_error"] = format!("{error:#}").into();
    }
    fs::write(
        directory.join("report.json"),
        serde_json::to_string_pretty(&report)?,
    )?;
    summary::print(&report, &audit.report, &directory);
    match result {
        Ok(_) => {
            cleanup?;
            Ok(())
        }
        Err(error) => {
            if let Err(cleanup_error) = cleanup {
                eprintln!("Cleanup also failed: {cleanup_error}");
            }
            Err(error.context(format!("Full e2e failed; logs: {}", directory.display())))
        }
    }
}

async fn scenario(
    base: &Compose,
    proxy: &Compose,
    routers: &mut Option<Cluster>,
    audit: &mut RedisAudit,
    settings: Settings,
    proxy_port: u16,
    setup: &mut Option<crate::setup::TestSetup>,
) -> Result<Value> {
    audit.preflight().await?;
    *routers = Some(Cluster::start(settings.clone()).await?);
    let topology = crate::setup::TestSetup::for_cluster(&settings, proxy_port)?;
    println!("{topology}");
    topology.save(&settings.directory)?;
    *setup = Some(topology);
    proxy.run(&["up", "-d", "nginx"]).await?;
    wait_proxy(proxy_port).await?;
    let mut traffic = Traffic::new(format!("http://127.0.0.1:{proxy_port}"), settings.rpc).await?;
    let routers = routers.as_mut().context("Routers not started")?;
    let origin = Instant::now();
    phase("baseline", 45, origin, routers, &mut traffic, audit, false).await?;
    base.stop_service("gas-station-1").await?;
    wait_backend_health(settings.metrics, false).await?;
    phase(
        "Gas Station down",
        30,
        origin,
        routers,
        &mut traffic,
        audit,
        false,
    )
    .await?;
    base.start_service("gas-station-1").await?;
    phase(
        "Gas Station recovery",
        30,
        origin,
        routers,
        &mut traffic,
        audit,
        false,
    )
    .await?;
    wait_backend_health(settings.metrics, true).await?;
    audit.observe().await?;
    base.stop_service("redis").await?;
    phase(
        "Redis down: cached execution must fail",
        10,
        origin,
        routers,
        &mut traffic,
        audit,
        true,
    )
    .await?;
    base.start_service("redis").await?;
    audit.wait_ready().await?;
    traffic.verify_cached_recovery().await?;
    wait_all_backends(settings.metrics).await?;
    phase(
        "Redis recovery",
        35,
        origin,
        routers,
        &mut traffic,
        audit,
        false,
    )
    .await?;
    phase(
        "healthy traffic after both failures",
        80,
        origin,
        routers,
        &mut traffic,
        audit,
        false,
    )
    .await?;
    settle(origin, routers, audit).await?;
    audit.finish().await?;
    traffic.validate()?;
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()?;
    let snapshots = metrics::sample(&client, settings.metrics).await?;
    metrics::validate(&snapshots, traffic.report.transactions)?;
    Ok(
        json!({"duration_seconds":origin.elapsed().as_secs(), "traffic":traffic.report,
        "redis":audit.report, "routers":snapshots, "checks":{
            "both_routers_served_requests":true, "one_dispatch_per_transaction":true,
            "gas_station_failure_and_recovery":true, "redis_failure_and_recovery":true,
            "data_expired_without_deletion":true, "no_evictions":true}}),
    )
}

async fn wait_all_backends(ports: [u16; 2]) -> Result<()> {
    println!("[recovery] Waiting for all Gas Stations to recover after Redis restart");
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    timeout(Duration::from_secs(30), async {
        loop {
            if metrics::sample(&client, ports)
                .await?
                .iter()
                .all(|sample| sample.healthy_backends == 3)
            {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .context("Gas Stations did not recover in both routers after Redis restart")??;
    Ok(())
}

async fn phase(
    name: &str,
    duration_seconds: u64,
    origin: Instant,
    routers: &mut Cluster,
    traffic: &mut Traffic,
    audit: &mut RedisAudit,
    redis_down: bool,
) -> Result<()> {
    println!("[{:>3}s] {name}", origin.elapsed().as_secs());
    let deadline = Instant::now() + Duration::from_secs(duration_seconds);
    let before = traffic.report.transactions;
    while Instant::now() < deadline {
        routers.check_alive()?;
        if redis_down {
            traffic.expect_redis_error().await?;
        } else {
            traffic
                .transaction()
                .await
                .with_context(|| format!("Phase: {name}"))?;
            audit.observe().await?;
        }
        sleep_until((Instant::now() + Duration::from_secs(3)).min(deadline)).await;
    }
    if !redis_down {
        ensure!(
            traffic.report.transactions > before,
            "No successful transaction during {name}"
        );
    }
    Ok(())
}

async fn settle(origin: Instant, routers: &mut Cluster, audit: &mut RedisAudit) -> Result<()> {
    println!(
        "[{:>3}s] Stop traffic; wait for all router data TTLs",
        origin.elapsed().as_secs()
    );
    let started = Instant::now();
    let deadline = audit.latest_expiry().unwrap_or(started).max(started) + Duration::from_secs(30);
    loop {
        routers.check_alive()?;
        audit.observe().await?;
        audit.report.ttl_wait_seconds = started.elapsed().as_secs();
        if audit.all_data_expired() {
            return Ok(());
        }
        ensure!(
            Instant::now() < deadline,
            "Router data did not expire before the TTL deadline plus 30s grace period"
        );
        sleep_until((Instant::now() + Duration::from_secs(5)).min(deadline)).await;
    }
}

async fn wait_backend_health(ports: [u16; 2], recovered: bool) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()?;
    timeout(Duration::from_secs(20), async {
        loop {
            let samples = metrics::sample(&client, ports).await?;
            if samples.iter().all(|sample| {
                if recovered {
                    sample.recovered > 0
                } else {
                    sample.unhealthy > 0
                }
            }) {
                return Ok::<_, anyhow::Error>(());
            }
            sleep(Duration::from_millis(500)).await;
        }
    })
    .await
    .context("Both routers did not observe backend health transition")??;
    Ok(())
}

async fn wait_proxy(port: u16) -> Result<()> {
    timeout(Duration::from_secs(15), async {
        loop {
            if let Ok(reply) = reqwest::get(format!("http://127.0.0.1:{port}/health")).await
                && reply.status().is_success()
            {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .context("Nginx did not become ready")?;
    Ok(())
}

async fn cleanup(
    base: &Compose,
    proxy: &Compose,
    routers: &mut Option<Cluster>,
    audit: &mut RedisAudit,
    directory: &Path,
) -> Result<()> {
    // Always restore services first, including after an interrupted fault command.
    let redis = base.start_service("redis").await;
    let gs = base.start_service("gas-station-1").await;
    let stopped = if let Some(routers) = routers {
        routers.stop().await
    } else {
        Ok(())
    };
    let _ = proxy.save_logs(&directory.join("nginx.log")).await;
    let proxy_stopped = proxy.run(&["down", "--remove-orphans"]).await;
    redis?;
    gs?;
    stopped?;
    proxy_stopped?;
    audit.wait_ready().await?;
    audit.cleanup_stopped_run().await
}
