use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post},
};

use crate::router::backend::registry::BackendRegistry;
use crate::router::services::execution::ExecutionService;
use crate::router::services::reservation::ReservationService;

mod execution;
mod health;
mod reservation;
mod status;

struct RpcState {
    registry: Arc<BackendRegistry>,
    reservation: Arc<ReservationService>,
    execution: Arc<ExecutionService>,
    token: String,
}

pub fn router(
    reservation: Arc<ReservationService>,
    execution: Arc<ExecutionService>,
    token: String,
) -> Router {
    Router::new()
        .route("/health", get(health::health))
        .route("/status", get(status::status))
        .route("/v1/reserve_gas", post(reservation::reserve_gas))
        .route("/v1/execute_tx", post(execution::execute_transaction))
        .route(
            "/v1/execute_transaction",
            post(execution::execute_transaction),
        )
        .with_state(Arc::new(RpcState {
            registry: reservation.backend_registry(),
            reservation,
            execution,
            token,
        }))
}
