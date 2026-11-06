use std::sync::Arc;

use anyhow::{Context, Result, bail};
use axum::{
    Json, Router,
    extract::{State, rejection::JsonRejection},
    http::{HeaderMap, StatusCode, header::AUTHORIZATION},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::watch};

use crate::{
    backend::registry::BackendRegistry,
    config::{Config, RoutingAlgorithm, StorageKind},
    metrics,
    reserve::{self, ReserveGasRequest, ReserveReply, ReserveService},
    routing::round_robin::RoundRobinSelector,
    storage::{Storage, memory::MemoryStorage},
};

pub async fn start(config: Config) -> Result<()> {
    let storage: Arc<dyn Storage> = match config.storage.kind {
        StorageKind::Memory => Arc::new(MemoryStorage::new()),
        StorageKind::Redis => {
            bail!("Redis storage is not implemented yet; use storage.kind: memory")
        }
    };
    let timeout = config.routing.reserve_gas.timeout;
    let client = reserve::http_client(timeout)?;
    let registry = Arc::new(BackendRegistry::new(&config.backends)?);
    let selector = match config.routing.algorithm {
        RoutingAlgorithm::RoundRobin => Arc::new(RoundRobinSelector::new()),
    };
    let reserve = Arc::new(ReserveService::new(
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
    eprintln!("RPC listening on {}", rpc.local_addr()?);
    eprintln!("Metrics listening on {}", metrics.local_addr()?);

    let (shutdown_tx, shutdown_rx) = watch::channel(());
    let rpc_server = async {
        axum::serve(rpc, rpc_routes(reserve, config.rpc.token))
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
        let _ = shutdown_tx.send(());
        Ok::<_, anyhow::Error>(())
    };

    let health = registry.monitor_health(client, timeout, shutdown_rx.clone());
    tokio::try_join!(rpc_server, metrics_server, health, shutdown)?;
    Ok(())
}

struct RpcState {
    reserve: Arc<ReserveService>,
    token: String,
}

pub fn rpc_routes(reserve: Arc<ReserveService>, token: String) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({ "status": "ok" })) }))
        .route("/v1/reserve_gas", post(reserve_gas))
        .route("/v1/execute_transaction", post(not_implemented))
        .with_state(Arc::new(RpcState { reserve, token }))
}

async fn reserve_gas(
    State(state): State<Arc<RpcState>>,
    headers: HeaderMap,
    request: Result<Json<ReserveGasRequest>, JsonRejection>,
) -> Response {
    metrics::inc_reserve_gas();
    if !state.token.is_empty()
        && headers
            .get(AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            != Some(state.token.as_str())
    {
        return reserve_response(ReserveReply::error(
            StatusCode::UNAUTHORIZED,
            "invalid or missing authorization token",
        ));
    }
    let Ok(Json(request)) = request else {
        return reserve_response(ReserveReply::error(
            StatusCode::BAD_REQUEST,
            "invalid reserve_gas request",
        ));
    };
    reserve_response(state.reserve.reserve_gas(&request).await)
}

fn reserve_response(reply: ReserveReply) -> Response {
    if !reply.status.is_success()
        || reply
            .body
            .get("error")
            .is_some_and(|error| !error.is_null())
    {
        metrics::inc_reserve_gas_error();
    }
    (reply.status, Json(reply.body)).into_response()
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
