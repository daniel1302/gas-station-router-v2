use std::sync::Arc;

use anyhow::{Context, Result, bail};
use tokio::{net::TcpListener, sync::watch};

use crate::{
    api,
    backend::registry::BackendRegistry,
    config::{Config, RoutingAlgorithm, StorageKind},
    metrics,
    routing::round_robin::RoundRobinSelector,
    services::reservation::{self, ReservationService},
    storage::{Storage, memory::MemoryStorage},
};

pub async fn start(config: Config) -> Result<()> {
    tracing::info!("Starting Gas Station router");
    let storage: Arc<dyn Storage> = match config.storage.kind {
        StorageKind::Memory => Arc::new(MemoryStorage::new()),
        StorageKind::Redis => {
            bail!("Redis storage is not implemented yet; use storage.kind: memory")
        }
    };
    let timeout = config.routing.reserve_gas.timeout;
    let client = reservation::http_client(timeout)?;
    let registry = Arc::new(BackendRegistry::new(&config.backends)?);
    let selector = match config.routing.algorithm {
        RoutingAlgorithm::RoundRobin => Arc::new(RoundRobinSelector::new()),
    };
    let reservation = Arc::new(ReservationService::new(
        registry.clone(),
        selector,
        storage,
        client.clone(),
        config.routing.reserve_gas,
        config.routing.ttl.reservation,
    )?);
    // Bind both sockets before serving, so a failed bind cannot leave a partial start.
    let rpc = TcpListener::bind(config.rpc.address())
        .await
        .context("failed to bind RPC listener")?;
    let metrics = TcpListener::bind(config.metrics.address())
        .await
        .context("failed to bind metrics listener")?;
    registry.refresh_health(&client, timeout).await;
    tracing::info!(listener = "rpc", address = %rpc.local_addr()?, "HTTP server listening");
    tracing::info!(listener = "metrics", address = %metrics.local_addr()?, "HTTP server listening");

    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let rpc_server = async {
        axum::serve(rpc, api::router(reservation, config.rpc.token))
            .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()))
            .await
            .context("RPC server failed")
    };
    let metrics_server = async {
        axum::serve(metrics, metrics::router())
            .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()))
            .await
            .context("metrics server failed")
    };
    let shutdown = async {
        shutdown_signal().await?;
        tracing::info!("Shutdown requested");
        let _ = shutdown_tx.send(());
        Ok::<_, anyhow::Error>(())
    };

    let health = registry.monitor_health(client, timeout, shutdown_rx.clone());
    tokio::try_join!(rpc_server, metrics_server, health, shutdown)?;
    tracing::info!("Gas Station router stopped");
    Ok(())
}

async fn wait_for_shutdown(mut receiver: watch::Receiver<()>) {
    let _ = receiver.changed().await;
}

async fn shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                .context("failed to install SIGTERM handler")?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result.context("failed to receive SIGINT")?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c()
        .await
        .context("failed to receive Ctrl-C")?;
    Ok(())
}
