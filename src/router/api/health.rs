use axum::Json;
use serde_json::{Value, json};

pub(super) async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}
