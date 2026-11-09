use axum::{
    Json, Router,
    extract::State as Extract,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use iota_sdk_types::{ObjectDigest, Transaction};
use serde_json::{Value, json};

use super::state::State;

type Reply = Result<Json<Value>, StatusCode>;

pub fn router(state: State) -> Router {
    Router::new()
        .route("/", get(health))
        .route("/ready", get(ready))
        .route("/metrics", get(metrics))
        .route("/refresh", post(refresh))
        .route("/rpc", post(rpc))
        .route("/crash", post(crash))
        .route("/v1/reserve_gas", post(reserve))
        .route("/v1/execute_tx", post(execute))
        .route("/v1/execute_transaction", post(execute))
        .with_state(state)
}

async fn health(Extract(state): Extract<State>) -> StatusCode {
    status(state.read().is_ok_and(|control| control.healthy))
}

async fn ready(Extract(state): Extract<State>) -> StatusCode {
    status(state.read().is_ok_and(|control| control.ready))
}

fn status(available: bool) -> StatusCode {
    if available {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn metrics(Extract(state): Extract<State>) -> Result<String, StatusCode> {
    let control = state.read().map_err(internal)?;
    if !control.valid_metrics {
        return Ok("gas_station_available_gas_coin_count invalid\n".into());
    }
    let mut text = String::new();
    for (name, value) in [
        (
            "gas_station_available_gas_coin_count",
            control.available_coins,
        ),
        (
            "gas_station_available_gas_total_balance",
            control.available_balance,
        ),
        ("num_unreleased_gas_coins", control.unreleased),
        ("oversized_gas_coins_count", control.oversized),
    ] {
        text.push_str(&format!(
            "{name}{{sponsor=\"{}\"}} {value}\n",
            state.sponsor
        ));
    }
    text.push_str(&format!(
        "num_gas_station_invariant_violations {}\n",
        control.invariant_violations
    ));
    if control.include_activity {
        text.push_str(&format!(
            "gas_station_active_reservations {}\ngas_station_pending_executions {}\n",
            control.active_reservations, control.pending_executions
        ));
    }
    Ok(text)
}

async fn refresh(Extract(state): Extract<State>) -> Result<StatusCode, StatusCode> {
    state.event("refresh", json!({})).map_err(internal)?;
    state
        .update(|control| {
            if control.refresh_heals {
                control.reconcile();
            }
        })
        .map_err(internal)?;
    Ok(StatusCode::ACCEPTED)
}

async fn rpc(Extract(state): Extract<State>, Json(body): Json<Value>) -> Reply {
    let control = state.read().map_err(internal)?;
    let result = match body["method"].as_str() {
        Some("iota_getChainIdentifier") => json!(control.chain_id),
        Some("iotax_getBalance") => {
            json!({"totalBalance":control.network_balance.to_string(), "coinObjectCount":control.network_coins})
        }
        _ => {
            return Ok(Json(
                json!({"jsonrpc":"2.0", "id":body["id"], "error":{"code":-32601,"message":"Unknown mock RPC method"}}),
            ));
        }
    };
    Ok(Json(
        json!({"jsonrpc":"2.0", "id":body["id"], "result":result}),
    ))
}

async fn reserve(
    Extract(state): Extract<State>,
    headers: HeaderMap,
    Json(_body): Json<Value>,
) -> Reply {
    authorize(&state, &headers)?;
    let control = state.read().map_err(internal)?;
    if !control.ready {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    if control.available_coins == 0 {
        return Ok(Json(
            json!({"result":null,"error":"Not enough gas coins available"}),
        ));
    }
    let id = state
        .update(|control| {
            control.next_reservation += 1;
            control.next_reservation
        })
        .map_err(internal)?;
    state
        .event("reserve", json!({"reservation_id":id}))
        .map_err(internal)?;
    Ok(Json(
        json!({"result":{"reservation_id":id,"sponsor_address":state.sponsor,
        "gas_coins":[{"objectId":"0x2","version":1,"digest":ObjectDigest::new([3;32])}]},"error":null}),
    ))
}

async fn execute(
    Extract(state): Extract<State>,
    headers: HeaderMap,
    Json(body): Json<Value>,
) -> Reply {
    authorize(&state, &headers)?;
    let transaction = body["tx_bytes"]
        .as_str()
        .and_then(|bytes| Transaction::from_base64(bytes).ok())
        .ok_or(StatusCode::BAD_REQUEST)?;
    state
        .event(
            "execute",
            json!({"reservation_id":body["reservation_id"],"digest":transaction.digest()}),
        )
        .map_err(internal)?;
    Ok(Json(
        json!({"effects":{"messageVersion":"v1", "transactionDigest":transaction.digest(),
        "status":{"status":"success"}, "executedEpoch":"1",
        "gasUsed":{"computationCost":"1","storageCost":"0","storageRebate":"0","nonRefundableStorageFee":"0"},
        "gasObject":{"owner":{"AddressOwner":state.sponsor},"reference":{"objectId":"0x2","version":1,"digest":ObjectDigest::new([3;32])}}}, "error":null}),
    ))
}

fn authorize(state: &State, headers: &HeaderMap) -> Result<(), StatusCode> {
    if headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        == Some(&format!("Bearer {}", state.token))
    {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

fn internal(_: impl std::fmt::Display) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}

async fn crash() -> StatusCode {
    tokio::spawn(async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        std::process::exit(42);
    });
    StatusCode::ACCEPTED
}
