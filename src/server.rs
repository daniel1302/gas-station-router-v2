use anyhow::{Context, Result};
use axum::{
    Json, Router,
    http::StatusCode,
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::watch};

use crate::config::Config;

pub async fn start(config: Config) -> Result<()> {
    // Bind both sockets before serving, so a failed bind cannot leave a partial start.
    let rpc = TcpListener::bind(config.rpc.address())
        .await
        .context("failed to bind RPC listener")?;
    let metrics = TcpListener::bind(config.metrics.address())
        .await
        .context("failed to bind metrics listener")?;
    eprintln!("RPC listening on {}", rpc.local_addr()?);
    eprintln!("Metrics listening on {}", metrics.local_addr()?);

    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let rpc_server = async {
        axum::serve(rpc, rpc_routes())
            .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()))
            .await
            .context("RPC server failed")
    };
    let metrics_server = async {
        axum::serve(metrics, metrics_routes())
            .with_graceful_shutdown(wait_for_shutdown(shutdown_rx.clone()))
            .await
            .context("metrics server failed")
    };
    let shutdown = async {
        shutdown_signal().await?;
        let _ = shutdown_tx.send(());
        Ok::<_, anyhow::Error>(())
    };

    tokio::try_join!(rpc_server, metrics_server, shutdown)?;
    Ok(())
}

fn rpc_routes() -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({ "status": "ok" })) }))
        .route("/v1/reserve_gas", post(not_implemented))
        .route("/v1/execute_transaction", post(not_implemented))
}

fn metrics_routes() -> Router {
    Router::new().route(
        "/metrics",
        get(|| async {
            (
                [(
                    axum::http::header::CONTENT_TYPE,
                    "text/plain; version=0.0.4; charset=utf-8",
                )],
                "# HELP gas_station_router_up Whether the router HTTP servers are running.\n\
             # TYPE gas_station_router_up gauge\n\
             gas_station_router_up 1\n",
            )
        }),
    )
}

async fn not_implemented() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(json!({ "error": "not implemented" })),
    )
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
