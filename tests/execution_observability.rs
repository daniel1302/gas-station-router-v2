use std::{
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gas_station_router_v2::{
    api,
    backend::registry::BackendRegistry,
    config::{
        BackendConfig, ExecuteTxConfig, InflightConfig, IotaConfig, ReserveGasConfig, RetryConfig,
        TtlConfig,
    },
    metrics,
    routing::round_robin::RoundRobinSelector,
    services::{
        execution::{ExecuteTxRequest, ExecutionService},
        reservation::{ReservationService, http_client},
    },
    storage::{Reservation, Storage, memory::MemoryStorage},
};
use iota_sdk_crypto::{IotaSigner, ed25519::Ed25519PrivateKey};
use iota_sdk_types::{
    GasPayment, ObjectDigest, ObjectId, ObjectReference, ProgrammableTransaction, Transaction,
    TransactionDigest, TransactionExpiration, TransactionKind, TransactionV1,
};
use rand::rngs::OsRng;
use serde_json::{Value, json};
use tower::ServiceExt;
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

#[derive(Clone)]
struct LogWriter(Arc<Mutex<Vec<u8>>>);

impl Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn request(id: u64) -> (ExecuteTxRequest, TransactionDigest) {
    let key = Ed25519PrivateKey::generate(OsRng);
    let tx: Transaction = TransactionV1 {
        kind: TransactionKind::new_programmable(ProgrammableTransaction {
            inputs: vec![],
            commands: vec![],
        }),
        sender: key.public_key().derive_address(),
        gas_payment: GasPayment {
            objects: vec![ObjectReference::new(
                ObjectId::new([2; 32]),
                1.into(),
                ObjectDigest::new([3; 32]),
            )],
            owner: key.public_key().derive_address(),
            price: 1000,
            budget: 1_000_000,
        },
        expiration: TransactionExpiration::None,
    }
    .into();
    (
        ExecuteTxRequest {
            reservation_id: id,
            tx_bytes: tx.to_base64(),
            user_sig: key.sign_transaction(&tx).unwrap().to_base64(),
            request_type: None,
        },
        tx.digest(),
    )
}

fn effects(digest: &TransactionDigest) -> Value {
    json!({"messageVersion":"v1", "transactionDigest":digest, "status":{"status":"success"},
        "executedEpoch":"1", "gasUsed":{"computationCost":"1", "storageCost":"0", "storageRebate":"0", "nonRefundableStorageFee":"0"},
        "gasObject":{"owner":{"AddressOwner":"0x1"},"reference":{"objectId":"0x2","version":1,"digest":ObjectDigest::new([3;32])}}})
}

fn count(reason: &str) -> u64 {
    metrics::render()
        .unwrap()
        .lines()
        .find_map(|line| {
            line.strip_prefix(&format!(
                "gas_station_router_execute_tx_errors_total{{reason=\"{reason}\"}} "
            ))
        })
        .unwrap()
        .parse()
        .unwrap()
}

fn total() -> u64 {
    metrics::render()
        .unwrap()
        .lines()
        .filter(|line| line.starts_with("gas_station_router_execute_tx_errors_total{"))
        .map(|line| line.rsplit_once(' ').unwrap().1.parse::<u64>().unwrap())
        .sum()
}

fn successes() -> u64 {
    metrics::render()
        .unwrap()
        .lines()
        .find_map(|line| line.strip_prefix("gas_station_router_execute_tx_successes_total "))
        .unwrap()
        .parse()
        .unwrap()
}

async fn mapping(storage: &MemoryStorage) -> u64 {
    storage
        .create_reservation(
            Reservation {
                backend: "execution-node".into(),
                upstream_reservation_id: 77,
            },
            Duration::from_secs(60),
        )
        .await
        .unwrap()
}

async fn send(router: &Router, payload: Value, authorized: bool) -> (StatusCode, Value) {
    let mut request = Request::post("/v1/execute_tx").header("content-type", "application/json");
    if authorized {
        request = request.header("authorization", "Bearer PRIVATE_CLIENT_TOKEN");
    }
    let response = router
        .clone()
        .oneshot(request.body(Body::from(payload.to_string())).unwrap())
        .await
        .unwrap();
    (
        response.status(),
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
            .unwrap(),
    )
}

// One test isolates process-wide counters and captures logs from owned execution tasks.
#[tokio::test]
async fn counts_final_errors_once_and_logs_recovered_failures_without_secrets() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let writer = LogWriter(output.clone());
    tracing::subscriber::set_global_default(
        tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::INFO)
            .with_writer(move || writer.clone())
            .finish(),
    )
    .unwrap();
    let backend = MockServer::start().await;
    let rpc = MockServer::start().await;
    let registry = Arc::new(
        BackendRegistry::new(&[BackendConfig {
            name: "execution-node".into(),
            enabled: false,
            token: "PRIVATE_BACKEND_TOKEN".into(),
            rpc: format!("{}/PRIVATE_BACKEND_PATH", backend.uri()),
        }])
        .unwrap(),
    );
    let storage = Arc::new(MemoryStorage::new());
    let service = Arc::new(
        ExecutionService::new(
            registry.clone(),
            storage.clone(),
            ExecuteTxConfig {
                timeout: Duration::from_millis(600),
                inflight: InflightConfig {
                    lease_ttl: Duration::from_secs(1),
                    record_ttl: Duration::from_secs(60),
                },
            },
            IotaConfig {
                rpc_url: format!("{}/PRIVATE_RPC_PATH", rpc.uri()),
                timeout: Duration::from_millis(100),
            },
            TtlConfig {
                reservation: Duration::from_secs(60),
                transaction_effects: Duration::from_secs(60),
            },
        )
        .unwrap(),
    );
    let reserve = ReservationService::new(
        registry,
        Arc::new(RoundRobinSelector::new()),
        storage.clone(),
        http_client(Duration::from_secs(1)).unwrap(),
        ReserveGasConfig {
            timeout: Duration::from_secs(1),
            retries: RetryConfig {
                max_retries: 0,
                backoff: Duration::ZERO,
            },
        },
        Duration::from_secs(60),
    )
    .unwrap();
    let router = api::router(
        Arc::new(reserve),
        service.clone(),
        "PRIVATE_CLIENT_TOKEN".into(),
    );

    assert_eq!(
        send(&router, json!({}), false).await.0,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(&router, json!({}), true).await.0,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(send(&router, json!({"reservation_id":1,"tx_bytes":"PRIVATE_INVALID_TX","user_sig":"PRIVATE_INVALID_SIG"}), true).await.0, StatusCode::BAD_REQUEST);
    let (mut invalid_signature, _) = request(1);
    invalid_signature.user_sig = "PRIVATE_INVALID_SIG".into();
    assert_eq!(
        service.execute(invalid_signature).await.status,
        StatusCode::BAD_REQUEST
    );
    let (missing, _) = request(u64::MAX);
    assert_eq!(service.execute(missing).await.status, StatusCode::NOT_FOUND);
    for reason in [
        "unauthorized",
        "invalid_request",
        "invalid_transaction",
        "invalid_signature",
        "reservation_not_found",
    ] {
        assert_eq!(count(reason), 1, "{reason}");
    }
    assert_eq!(total(), 5);
    assert_eq!(
        successes(),
        0,
        "rejected requests must not count as successes"
    );

    let (execute, digest) = request(mapping(&storage).await);
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_string("PRIVATE_BACKEND_ERROR"))
        .expect(1)
        .mount(&backend)
        .await;
    let attempts = Mutex::new(0);
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            let mut attempts = attempts.lock().unwrap();
            *attempts += 1;
            if *attempts == 1 {
                ResponseTemplate::new(503).set_body_string("PRIVATE_RPC_ERROR")
            } else {
                ResponseTemplate::new(200).set_body_json(json!({"jsonrpc":"2.0", "id":1,
                "result":{"digest":digest,"effects":effects(&digest)}}))
            }
        })
        .expect(2)
        .mount(&rpc)
        .await;
    for expected in 1..=2 {
        let (status, reply) = send(&router, json!(execute), true).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reply["effects"]["status"]["status"], "success");
        assert_eq!(
            successes(),
            expected,
            "recovery and cache reads each count once"
        );
    }
    assert_eq!(
        total(),
        5,
        "recovered failures and successful cache reads are not failed executions"
    );
    backend.verify().await;
    rpc.verify().await;
    assert!(metrics::render().unwrap().lines().any(|line| line ==
        "gas_station_router_execute_tx_recovery_retries_total{backend=\"execution-node\",stage=\"recovery\"} 1"),
        "only the second RPC lookup is a retry; cache reads must not increment it");
    backend.reset().await;
    rpc.reset().await;
    let (conflict, _) = request(execute.reservation_id);
    assert_eq!(service.execute(conflict).await.status, StatusCode::CONFLICT);
    assert_eq!(count("digest_conflict"), 1);

    let (failed, digest) = request(mapping(&storage).await);
    let mut failed_effects = effects(&digest);
    failed_effects["status"] = json!({"status":"failure", "error":"PRIVATE_EXECUTION_ERROR"});
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"effects":failed_effects, "error":null})),
        )
        .expect(1)
        .mount(&backend)
        .await;
    for _ in 0..2 {
        let reply = service.execute(failed.clone()).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.body["effects"]["status"]["status"], "failure");
    }
    assert_eq!(
        count("transaction_failed"),
        2,
        "each failed result counts, including cache reads"
    );
    backend.verify().await;
    backend.reset().await;

    let (unknown, _) = request(mapping(&storage).await);
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("PRIVATE_INVALID_RESPONSE"))
        .expect(1)
        .mount(&backend)
        .await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"PRIVATE_RPC_ERROR"}}),
        ))
        .mount(&rpc)
        .await;
    let running = tokio::spawn({
        let service = service.clone();
        async move { service.execute(unknown).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while backend.received_requests().await.unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    running.abort();
    tokio::time::timeout(Duration::from_secs(2), async {
        while count("timeout") == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        count("timeout"),
        1,
        "client cancellation cannot hide the owned task's final error"
    );
    assert_eq!(total(), 9);
    assert_eq!(
        successes(),
        2,
        "conflicts, timeouts, and failed effects returned with HTTP 200 are not successes"
    );
    backend.verify().await;

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    let events: Vec<Value> = logs
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let warnings: Vec<_> = events
        .iter()
        .filter(|event| event["level"] == "WARN")
        .collect();
    let errors: Vec<_> = warnings
        .iter()
        .filter(|event| event["fields"]["message"] == "Execution request failed")
        .collect();
    assert_eq!(errors.len(), 9, "one final warning per failed call");
    let uncertain = warnings
        .iter()
        .find(|event| {
            event["fields"]["message"] == "Backend execution result is uncertain"
                && event["fields"]["reason"] == "http_error"
        })
        .unwrap();
    assert_eq!(uncertain["fields"]["backend"], "execution-node");
    assert_eq!(uncertain["fields"]["http_status"], 500);
    assert_eq!(uncertain["fields"]["next_action"], "rpc_recovery");
    assert!(warnings.iter().any(|event| event["fields"]["message"]
        == "IOTA RPC execution lookup failed"
        && event["fields"]["http_status"] == 503));
    assert!(
        warnings
            .iter()
            .any(|event| event["fields"]["rpc_code"] == -32000)
    );
    assert!(
        !logs.contains("PRIVATE_"),
        "credentials, transaction inputs, upstream messages and URLs must not be logged"
    );
}
