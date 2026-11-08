use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
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
    routing::round_robin::RoundRobinSelector,
    services::{
        execution::{ExecuteTxRequest, ExecutionService, RequestType},
        reservation::{ReservationService, http_client},
    },
    storage::{
        CachedTransaction, ExecutionClaim, ExecutionPhase, ExecutionRecord, ExecutionToken,
        Reservation, ReservationId, Storage, StorageError, StorageResult, TransactionWriteOutcome,
        memory::MemoryStorage,
    },
};
use iota_sdk_crypto::{IotaSigner, ed25519::Ed25519PrivateKey};
use iota_sdk_types::{
    GasPayment, ObjectDigest, ObjectId, ObjectReference, ProgrammableTransaction, Transaction,
    TransactionDigest, TransactionExpiration, TransactionKind, TransactionV1,
};
use rand::rngs::OsRng;
use serde_json::{Value, json};
use tower::ServiceExt;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{body_json, header, method, path},
};

fn request(id: u64, budget: u64) -> (ExecuteTxRequest, TransactionDigest) {
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
            budget,
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

fn rpc_found(digest: &TransactionDigest, confirmed: Option<bool>) -> Value {
    let mut result = json!({"digest":digest, "effects":effects(digest)});
    if let Some(confirmed) = confirmed {
        result["confirmedLocalExecution"] = json!(confirmed);
    }
    json!({"jsonrpc":"2.0", "id":1, "result":result})
}

fn build_service(
    registry: Arc<BackendRegistry>,
    storage: Arc<dyn Storage>,
    rpc: &str,
    timeout: Duration,
) -> Arc<ExecutionService> {
    Arc::new(
        ExecutionService::new(
            registry,
            storage,
            ExecuteTxConfig {
                timeout,
                inflight: InflightConfig {
                    lease_ttl: timeout + Duration::from_millis(100),
                    record_ttl: Duration::from_secs(60),
                },
            },
            IotaConfig {
                rpc_url: rpc.into(),
                timeout: Duration::from_millis(100),
            },
            TtlConfig {
                reservation: Duration::from_secs(5),
                transaction_effects: Duration::from_secs(20),
            },
        )
        .unwrap(),
    )
}

struct Fixture {
    backend: MockServer,
    rpc: MockServer,
    registry: Arc<BackendRegistry>,
    storage: Arc<MemoryStorage>,
    service: Arc<ExecutionService>,
    id: u64,
}

impl Fixture {
    async fn new() -> Self {
        let backend = MockServer::start().await;
        let rpc = MockServer::start().await;
        let registry = Arc::new(
            BackendRegistry::new(&[BackendConfig {
                name: "original".into(),
                rpc: backend.uri(),
                token: "backend-secret".into(),
                enabled: false,
            }])
            .unwrap(),
        );
        let storage = Arc::new(MemoryStorage::new());
        let id = storage
            .create_reservation(
                Reservation {
                    backend: "original".into(),
                    upstream_reservation_id: 77,
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();
        let service = build_service(
            registry.clone(),
            storage.clone(),
            &rpc.uri(),
            Duration::from_millis(600),
        );
        Self {
            backend,
            rpc,
            registry,
            storage,
            service,
            id,
        }
    }

    async fn seed(
        &self,
        digest: &TransactionDigest,
        cached: Option<bool>,
        lease: Duration,
    ) -> ExecutionToken {
        let token = ExecutionToken::new_v4();
        assert_eq!(
            self.storage
                .try_claim_execution(self.id, digest, token, lease, Duration::from_secs(60))
                .await
                .unwrap(),
            ExecutionClaim::Acquired
        );
        self.storage
            .mark_execution_dispatching(self.id, token)
            .await
            .unwrap();
        if let Some(confirmed) = cached {
            self.storage
                .complete_execution(
                    self.id,
                    token,
                    CachedTransaction {
                        effects: effects(digest),
                        local_execution_confirmed: confirmed,
                        backend: "original".into(),
                    },
                    Duration::from_secs(20),
                )
                .await
                .unwrap();
        }
        token
    }

    async fn lookup(&self, digest: &TransactionDigest, body: Value, calls: u64) {
        Mock::given(method("POST")).and(body_json(json!({"jsonrpc":"2.0", "id":1, "method":"iota_getTransactionBlock", "params":[digest,{"showEffects":true}]})))
            .respond_with(ResponseTemplate::new(200).set_body_json(body)).expect(calls).mount(&self.rpc).await;
    }

    async fn no_backend_posts(&self) {
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&self.backend)
            .await;
    }

    fn app(&self) -> Router {
        let reserve = Arc::new(
            ReservationService::new(
                self.registry.clone(),
                Arc::new(RoundRobinSelector::new()),
                self.storage.clone(),
                http_client(Duration::from_secs(1)).unwrap(),
                ReserveGasConfig {
                    timeout: Duration::from_secs(1),
                    retries: RetryConfig {
                        max_retries: 0,
                        backoff: Duration::ZERO,
                    },
                },
                Duration::from_secs(5),
            )
            .unwrap(),
        );
        api::router(reserve, self.service.clone(), "client-secret".into())
    }
}

#[tokio::test]
async fn dispatch_uses_original_backend_auth_and_id_after_persisting_dispatching() {
    let f = Fixture::new().await;
    let (request, digest) = request(f.id, 1);
    let mut upstream = serde_json::to_value(&request).unwrap();
    upstream["reservation_id"] = json!(77);
    upstream["request_type"] = json!("waitForEffectsCert");
    Mock::given(method("POST"))
        .and(path("/v1/execute_tx"))
        .and(header("Authorization", "Bearer backend-secret"))
        .and(body_json(upstream))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"effects":effects(&digest),"error":null}))
                .set_delay(Duration::from_millis(40)),
        )
        .expect(1)
        .mount(&f.backend)
        .await;
    let task = tokio::spawn({
        let service = f.service.clone();
        async move { service.execute(request).await }
    });
    while f.backend.received_requests().await.unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        f.storage.get_execution(f.id).await.unwrap().unwrap().phase,
        ExecutionPhase::Dispatching
    );
    let reply = task.await.unwrap();
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.body["effects"], effects(&digest));
    assert_eq!(reply.body["confirmedLocalExecution"], false);
    assert_eq!(
        f.storage.get_execution(f.id).await.unwrap().unwrap().phase,
        ExecutionPhase::Completed
    );
    assert!(f.rpc.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn cache_obeys_all_four_request_type_combinations_and_upgrades_monotonically() {
    for (confirmed, request_type, needs_rpc) in [
        (false, RequestType::WaitForEffectsCert, false),
        (true, RequestType::WaitForEffectsCert, false),
        (true, RequestType::WaitForLocalExecution, false),
        (false, RequestType::WaitForLocalExecution, true),
    ] {
        let f = Fixture::new().await;
        let (mut request, digest) = request(f.id, 1);
        request.request_type = Some(request_type);
        f.seed(&digest, Some(confirmed), Duration::from_secs(1))
            .await;
        f.no_backend_posts().await;
        // Different empty-list omission and numeric/string versions must compare equal.
        let mut found = rpc_found(&digest, None);
        found["result"]["effects"]["created"] = json!([]);
        found["result"]["effects"]["gasObject"]["reference"]["version"] = json!("1");
        f.lookup(&digest, found, u64::from(needs_rpc)).await;
        Mock::given(body_json(json!({"jsonrpc":"2.0", "id":1, "method":"iota_isTransactionIndexedOnNode", "params":[digest]})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"jsonrpc":"2.0","id":1,"result":true})))
            .expect(u64::from(needs_rpc)).mount(&f.rpc).await;
        let reply = f.service.execute(request).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(
            reply.body["confirmedLocalExecution"],
            confirmed || needs_rpc
        );
        assert_eq!(
            f.storage
                .get_transaction(&digest)
                .await
                .unwrap()
                .unwrap()
                .local_execution_confirmed,
            confirmed || needs_rpc
        );
        for request in f.rpc.received_requests().await.unwrap() {
            assert!(!request.headers.contains_key("authorization"));
        }
    }
}

#[tokio::test]
async fn finalized_failed_transactions_are_completed_and_cached() {
    let f = Fixture::new().await;
    let (request, digest) = request(f.id, 1);
    let mut failed = effects(&digest);
    failed["status"] = json!({"status":"failure","error":"MoveAbort"});
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"effects":failed,"error":null})),
        )
        .expect(1)
        .mount(&f.backend)
        .await;
    for _ in 0..2 {
        let reply = f.service.execute(request.clone()).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.body["effects"]["status"]["status"], "failure");
        assert!(reply.body["error"].is_null());
    }
    assert_eq!(
        f.storage.get_execution(f.id).await.unwrap().unwrap().phase,
        ExecutionPhase::Completed
    );
    assert_eq!(
        f.storage
            .get_transaction(&digest)
            .await
            .unwrap()
            .unwrap()
            .effects["status"]["status"],
        "failure"
    );
    assert!(f.rpc.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn conflicting_rpc_effects_cannot_upgrade_or_replace_cached_effects() {
    let f = Fixture::new().await;
    let (mut request, digest) = request(f.id, 1);
    request.request_type = Some(RequestType::WaitForLocalExecution);
    f.seed(&digest, Some(false), Duration::from_secs(1)).await;
    f.no_backend_posts().await;
    let mut conflicting = rpc_found(&digest, Some(true));
    conflicting["result"]["effects"]["gasUsed"]["computationCost"] = json!("999");
    f.lookup(&digest, conflicting, 1).await;
    assert_eq!(
        f.service.execute(request).await.status,
        StatusCode::BAD_GATEWAY
    );
    let cached = f.storage.get_transaction(&digest).await.unwrap().unwrap();
    assert!(!cached.local_execution_confirmed);
    assert_eq!(cached.effects, effects(&digest));
}

#[tokio::test]
async fn concurrent_duplicates_across_services_send_one_post_and_wait_for_cache() {
    let f = Fixture::new().await;
    let (request, digest) = request(f.id, 1);
    Mock::given(method("POST"))
        .and(path("/v1/execute_tx"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"effects":effects(&digest),"error":null}))
                .set_delay(Duration::from_millis(40)),
        )
        .expect(1)
        .mount(&f.backend)
        .await;
    let other = build_service(
        f.registry.clone(),
        f.storage.clone(),
        &f.rpc.uri(),
        Duration::from_millis(600),
    );
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..12 {
        let service = if index % 2 == 0 {
            f.service.clone()
        } else {
            other.clone()
        };
        let request = request.clone();
        tasks.spawn(async move { service.execute(request).await });
    }
    while let Some(reply) = tasks.join_next().await {
        assert_eq!(reply.unwrap().status, StatusCode::OK);
    }
    assert!(f.rpc.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn client_cancellation_does_not_abort_dispatched_execution() {
    let f = Fixture::new().await;
    let (request, digest) = request(f.id, 1);
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"effects":effects(&digest),"error":null}))
                .set_delay(Duration::from_millis(40)),
        )
        .expect(1)
        .mount(&f.backend)
        .await;
    let client = tokio::spawn({
        let service = f.service.clone();
        async move { service.execute(request).await }
    });
    while f.backend.received_requests().await.unwrap().is_empty() {
        tokio::task::yield_now().await;
    }
    client.abort();
    tokio::time::timeout(Duration::from_secs(1), async {
        while f.storage.get_execution(f.id).await.unwrap().unwrap().phase
            != ExecutionPhase::Completed
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(f.storage.get_transaction(&digest).await.unwrap().is_some());
}

#[tokio::test]
async fn recovery_uses_rpc_only_and_retains_backend_after_mapping_expiry() {
    for completed in [false, true] {
        let f = Fixture::new().await;
        let (request, digest) = request(f.id, 1);
        let old = f.seed(&digest, None, Duration::from_millis(1)).await;
        if completed {
            f.storage
                .complete_execution(
                    f.id,
                    old,
                    CachedTransaction {
                        effects: effects(&digest),
                        local_execution_confirmed: false,
                        backend: "original".into(),
                    },
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
        }
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(6)).await;
        tokio::time::resume();
        assert!(f.storage.get_reservation(f.id).await.unwrap().is_none());
        f.no_backend_posts().await;
        f.lookup(&digest, rpc_found(&digest, None), 1).await;
        let reply = f.service.execute(request).await;
        assert_eq!(reply.status, StatusCode::OK);
        assert_eq!(reply.body["confirmedLocalExecution"], false);
        assert_eq!(
            f.storage.get_execution(f.id).await.unwrap().unwrap().phase,
            ExecutionPhase::Completed
        );
        assert_eq!(
            f.storage
                .get_transaction(&digest)
                .await
                .unwrap()
                .unwrap()
                .backend,
            "original"
        );
        assert!(
            f.storage
                .complete_execution(
                    f.id,
                    old,
                    CachedTransaction {
                        effects: effects(&digest),
                        local_execution_confirmed: false,
                        backend: "original".into()
                    },
                    Duration::from_secs(20)
                )
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn recovery_retries_not_found_then_completes_without_resubmitting() {
    let f = Fixture::new().await;
    let (request, digest) = request(f.id, 1);
    f.seed(&digest, None, Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    f.no_backend_posts().await;
    let found = rpc_found(&digest, None);
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    Mock::given(method("POST"))
        .respond_with(move |_: &wiremock::Request| {
            ResponseTemplate::new(200).set_body_json(if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"not found"}})
            } else {
                found.clone()
            })
        })
        .expect(2)
        .mount(&f.rpc)
        .await;
    assert_eq!(f.service.execute(request).await.status, StatusCode::OK);
}

#[tokio::test]
async fn recovery_lease_allows_only_one_rpc_worker_across_services() {
    let f = Fixture::new().await;
    let (request, digest) = request(f.id, 1);
    f.seed(&digest, None, Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    f.no_backend_posts().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(rpc_found(&digest, None))
                .set_delay(Duration::from_millis(40)),
        )
        .expect(1)
        .mount(&f.rpc)
        .await;
    let other = build_service(
        f.registry.clone(),
        f.storage.clone(),
        &f.rpc.uri(),
        Duration::from_millis(600),
    );
    let (a, b) = tokio::join!(f.service.execute(request.clone()), other.execute(request));
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(b.status, StatusCode::OK);
}

#[tokio::test]
async fn uncertain_backend_replies_recover_without_following_redirects_or_reposting() {
    for status in [200, 302, 500] {
        let f = Fixture::new().await;
        let (request, digest) = request(f.id, 1);
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(status)
                    .insert_header("Location", format!("{}/should-not-follow", f.rpc.uri()))
                    .set_body_json(
                        json!({"effects":effects(&TransactionDigest::new([9;32])),"error":null}),
                    ),
            )
            .expect(1)
            .mount(&f.backend)
            .await;
        f.lookup(&digest, rpc_found(&digest, None), 1).await;
        assert_eq!(f.service.execute(request).await.status, StatusCode::OK);
        assert_eq!(
            f.storage
                .get_transaction(&digest)
                .await
                .unwrap()
                .unwrap()
                .effects,
            effects(&digest)
        );
    }
}

#[tokio::test]
async fn rpc_errors_and_not_found_remain_unknown_and_never_authorize_resubmission() {
    let f = Fixture::new().await;
    let (request, digest) = request(f.id, 1);
    f.seed(&digest, None, Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(3)).await;
    f.no_backend_posts().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"not found"}}),
        ))
        .expect(1..)
        .mount(&f.rpc)
        .await;
    let service = build_service(
        f.registry.clone(),
        f.storage.clone(),
        &f.rpc.uri(),
        Duration::from_millis(80),
    );
    assert_eq!(
        service.execute(request.clone()).await.status,
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(
        f.storage.get_execution(f.id).await.unwrap().unwrap().phase,
        ExecutionPhase::Dispatching
    );
    assert!(f.storage.get_transaction(&digest).await.unwrap().is_none());
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert_eq!(
        service.execute(request).await.status,
        StatusCode::GATEWAY_TIMEOUT
    );
    assert!(f.storage.get_transaction(&digest).await.unwrap().is_none());
}

#[tokio::test]
async fn unconfirmed_local_execution_keeps_effects_cached_and_can_be_upgraded_on_retry() {
    let f = Fixture::new().await;
    let (mut request, digest) = request(f.id, 1);
    request.request_type = Some(RequestType::WaitForLocalExecution);
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"effects":effects(&digest),"error":null,"confirmedLocalExecution":true}),
        ))
        .expect(1)
        .mount(&f.backend)
        .await;
    Mock::given(body_json(json!({"jsonrpc":"2.0", "id":1, "method":"iota_getTransactionBlock", "params":[digest,{"showEffects":true}]})))
        .respond_with(ResponseTemplate::new(200).set_body_json(rpc_found(&digest, None))).mount(&f.rpc).await;
    let confirmed = Arc::new(AtomicBool::new(false));
    let flag = confirmed.clone();
    Mock::given(body_json(json!({"jsonrpc":"2.0", "id":1, "method":"iota_isTransactionIndexedOnNode", "params":[digest]})))
        .respond_with(move |_: &wiremock::Request| ResponseTemplate::new(200).set_body_json(json!({"jsonrpc":"2.0","id":1,"result":flag.load(Ordering::SeqCst)})))
        .mount(&f.rpc).await;
    let service = build_service(
        f.registry.clone(),
        f.storage.clone(),
        &f.rpc.uri(),
        Duration::from_millis(80),
    );
    assert_eq!(
        service.execute(request.clone()).await.status,
        StatusCode::GATEWAY_TIMEOUT
    );
    assert_eq!(
        f.storage.get_execution(f.id).await.unwrap().unwrap().phase,
        ExecutionPhase::Completed
    );
    assert!(
        !f.storage
            .get_transaction(&digest)
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
    confirmed.store(true, Ordering::SeqCst);
    assert_eq!(
        service.execute(request).await.body["confirmedLocalExecution"],
        true
    );
    assert!(
        f.storage
            .get_transaction(&digest)
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
}

#[tokio::test]
async fn conflicting_digests_are_rejected_and_missing_reservations_are_cancelled() {
    let f = Fixture::new().await;
    let (original, digest) = request(f.id, 1);
    f.seed(&digest, Some(false), Duration::from_secs(1)).await;
    f.no_backend_posts().await;
    let (other, _) = request(f.id, 2);
    assert_eq!(f.service.execute(other).await.status, StatusCode::CONFLICT);
    let mut missing = original;
    missing.reservation_id = u64::MAX;
    assert_eq!(
        f.service.execute(missing).await.status,
        StatusCode::NOT_FOUND
    );
    assert!(f.storage.get_execution(u64::MAX).await.unwrap().is_none());
}

#[tokio::test]
async fn execution_http_auth_validation_and_both_routes_are_wired() {
    let f = Fixture::new().await;
    f.no_backend_posts().await;
    for route in ["/v1/execute_tx", "/v1/execute_transaction"] {
        for (token, body, expected) in [
            ("Bearer wrong", json!({}), StatusCode::UNAUTHORIZED),
            ("Bearer client-secret", json!({}), StatusCode::BAD_REQUEST),
            (
                "Bearer client-secret",
                json!({"reservation_id":f.id,"tx_bytes":"bad","user_sig":"bad"}),
                StatusCode::BAD_REQUEST,
            ),
        ] {
            let response = f
                .app()
                .oneshot(
                    Request::post(route)
                        .header("content-type", "application/json")
                        .header("authorization", token)
                        .body(Body::from(body.to_string()))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            let body: Value =
                serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap())
                    .unwrap();
            assert!(body["effects"].is_null());
        }
    }
    let (mut invalid_signature, _) = request(f.id, 1);
    invalid_signature.user_sig = "bad".into();
    assert_eq!(
        f.service.execute(invalid_signature).await.status,
        StatusCode::BAD_REQUEST
    );
    assert!(f.storage.get_execution(f.id).await.unwrap().is_none());
    let (request, digest) = request(f.id, 1);
    f.seed(&digest, Some(true), Duration::from_secs(1)).await;
    let mut body = serde_json::to_value(request).unwrap();
    body.as_object_mut().unwrap().remove("request_type");
    body["requestType"] = json!("waitForLocalExecution");
    for route in ["/v1/execute_tx", "/v1/execute_transaction"] {
        let response = f
            .app()
            .oneshot(
                Request::post(route)
                    .header("content-type", "application/json")
                    .header("authorization", "Bearer client-secret")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn invalid_timeouts_leases_retention_and_rpc_configuration_are_rejected() {
    let f = Fixture::new().await;
    for field in [
        "timeout",
        "lease",
        "record",
        "retention",
        "effects",
        "rpc_timeout",
        "rpc_url",
    ] {
        let mut config = ExecuteTxConfig {
            timeout: Duration::from_millis(10),
            inflight: InflightConfig {
                lease_ttl: Duration::from_millis(20),
                record_ttl: Duration::from_secs(1),
            },
        };
        let mut ttl = TtlConfig {
            reservation: Duration::from_millis(100),
            transaction_effects: Duration::from_secs(1),
        };
        let mut iota = IotaConfig {
            rpc_url: f.rpc.uri(),
            timeout: Duration::from_secs(1),
        };
        match field {
            "timeout" => config.timeout = Duration::ZERO,
            "lease" => config.inflight.lease_ttl = config.timeout,
            "record" => config.inflight.record_ttl = config.inflight.lease_ttl,
            "retention" => config.inflight.record_ttl = Duration::from_millis(50),
            "effects" => ttl.transaction_effects = Duration::ZERO,
            "rpc_timeout" => iota.timeout = Duration::ZERO,
            "rpc_url" => iota.rpc_url = "ftp://127.0.0.1".into(),
            _ => unreachable!(),
        }
        assert!(
            ExecutionService::new(f.registry.clone(), f.storage.clone(), config, iota, ttl)
                .is_err(),
            "accepted invalid {field}"
        );
    }
}

struct FailingStorage {
    inner: Arc<MemoryStorage>,
    fail_mark: AtomicBool,
    fail_complete: AtomicBool,
}

fn storage_failure<T>() -> StorageResult<T> {
    Err(StorageError::Backend(
        std::io::Error::other("injected storage failure").into(),
    ))
}

#[async_trait::async_trait]
impl Storage for FailingStorage {
    async fn create_reservation(
        &self,
        value: Reservation,
        ttl: Duration,
    ) -> StorageResult<ReservationId> {
        self.inner.create_reservation(value, ttl).await
    }
    async fn get_reservation(&self, id: ReservationId) -> StorageResult<Option<Reservation>> {
        self.inner.get_reservation(id).await
    }
    async fn put_transaction(
        &self,
        digest: &TransactionDigest,
        value: CachedTransaction,
        ttl: Duration,
    ) -> StorageResult<TransactionWriteOutcome> {
        self.inner.put_transaction(digest, value, ttl).await
    }
    async fn get_transaction(
        &self,
        digest: &TransactionDigest,
    ) -> StorageResult<Option<CachedTransaction>> {
        self.inner.get_transaction(digest).await
    }
    async fn health_check(&self) -> StorageResult<()> {
        self.inner.health_check().await
    }
    async fn try_claim_execution(
        &self,
        id: ReservationId,
        digest: &TransactionDigest,
        token: ExecutionToken,
        lease: Duration,
        record: Duration,
    ) -> StorageResult<ExecutionClaim> {
        self.inner
            .try_claim_execution(id, digest, token, lease, record)
            .await
    }
    async fn get_execution(&self, id: ReservationId) -> StorageResult<Option<ExecutionRecord>> {
        self.inner.get_execution(id).await
    }
    async fn try_claim_recovery(
        &self,
        id: ReservationId,
        digest: &TransactionDigest,
        token: ExecutionToken,
        lease: Duration,
    ) -> StorageResult<ExecutionClaim> {
        self.inner
            .try_claim_recovery(id, digest, token, lease)
            .await
    }
    async fn mark_execution_dispatching(
        &self,
        id: ReservationId,
        token: ExecutionToken,
    ) -> StorageResult<()> {
        if self.fail_mark.swap(false, Ordering::SeqCst) {
            storage_failure()
        } else {
            self.inner.mark_execution_dispatching(id, token).await
        }
    }
    async fn complete_execution(
        &self,
        id: ReservationId,
        token: ExecutionToken,
        value: CachedTransaction,
        ttl: Duration,
    ) -> StorageResult<TransactionWriteOutcome> {
        if self.fail_complete.swap(false, Ordering::SeqCst) {
            storage_failure()
        } else {
            self.inner.complete_execution(id, token, value, ttl).await
        }
    }
    async fn cancel_execution(
        &self,
        id: ReservationId,
        token: ExecutionToken,
    ) -> StorageResult<()> {
        self.inner.cancel_execution(id, token).await
    }
}

#[tokio::test]
async fn storage_failures_before_dispatch_or_after_effects_never_cause_a_duplicate_post() {
    for fail_mark in [true, false] {
        let f = Fixture::new().await;
        let (request, digest) = request(f.id, 1);
        let storage = Arc::new(FailingStorage {
            inner: f.storage.clone(),
            fail_mark: AtomicBool::new(fail_mark),
            fail_complete: AtomicBool::new(!fail_mark),
        });
        let service = build_service(
            f.registry.clone(),
            storage,
            &f.rpc.uri(),
            Duration::from_millis(600),
        );
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"effects":effects(&digest),"error":null})),
            )
            .expect(1)
            .mount(&f.backend)
            .await;
        f.lookup(&digest, rpc_found(&digest, None), u64::from(!fail_mark))
            .await;
        assert_eq!(
            service.execute(request.clone()).await.status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            f.backend.received_requests().await.unwrap().len(),
            usize::from(!fail_mark)
        );
        assert_eq!(
            f.storage.get_execution(f.id).await.unwrap().unwrap().phase,
            if fail_mark {
                ExecutionPhase::Claimed
            } else {
                ExecutionPhase::Dispatching
            }
        );
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::time::resume();
        assert_eq!(service.execute(request).await.status, StatusCode::OK);
        assert_eq!(
            f.storage.get_execution(f.id).await.unwrap().unwrap().phase,
            ExecutionPhase::Completed
        );
    }
}

#[tokio::test]
async fn local_confirmation_upgrade_preserves_the_original_cache_ttl() {
    let f = Fixture::new().await;
    let (mut request, digest) = request(f.id, 1);
    request.request_type = Some(RequestType::WaitForLocalExecution);
    let token = f.seed(&digest, None, Duration::from_secs(1)).await;
    f.storage
        .complete_execution(
            f.id,
            token,
            CachedTransaction {
                effects: effects(&digest),
                local_execution_confirmed: false,
                backend: "original".into(),
            },
            Duration::from_secs(3),
        )
        .await
        .unwrap();
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::time::resume();
    f.no_backend_posts().await;
    f.lookup(&digest, rpc_found(&digest, Some(true)), 1).await;
    assert_eq!(f.service.execute(request).await.status, StatusCode::OK);
    assert!(
        f.storage
            .get_transaction(&digest)
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(f.storage.get_transaction(&digest).await.unwrap().is_none());
}
