// One test keeps assertions on process-wide counters independent of test scheduling.
mod common;
use std::{collections::HashSet, sync::Arc, time::Duration};

use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use gas_station_router_v2::{
    api,
    backend::registry::BackendRegistry,
    config::{BackendConfig, PassiveHealthCheckConfig, ReserveGasConfig, RetryConfig},
    metrics,
    routing::round_robin::RoundRobinSelector,
    services::reservation::{ReservationService, http_client},
    storage::{
        CachedTransaction, ExecutionClaim, ExecutionPhase, Reservation, Storage, StorageError,
        memory::MemoryStorage,
    },
};
use iota_sdk_types::{Address, ObjectDigest, ObjectId, TransactionDigest};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tower::ServiceExt;
use uuid::Uuid;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

fn sample(body: &str, series: &str) -> u64 {
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{series} ")))
        .unwrap_or_else(|| panic!("missing series {series}\n{body}"))
        .parse()
        .unwrap()
}

fn count(name: &str, labels: &str) -> u64 {
    let series = if labels.is_empty() {
        format!("gas_station_router_{name}")
    } else {
        format!("gas_station_router_{name}{{{labels}}}")
    };
    sample(&metrics::render().unwrap(), &series)
}

fn cached(confirmed: bool) -> CachedTransaction {
    CachedTransaction {
        effects: json!({"status":"success"}),
        local_execution_confirmed: confirmed,
        backend: "metrics-test".into(),
    }
}

#[tokio::test]
async fn exports_static_events_and_scrapes_without_accessing_storage() {
    // Concurrent first use must register the families once and lose no increments.
    let threads: Vec<_> = (0..16)
        .map(|_| std::thread::spawn(|| metrics::inc_storage_error("backend")))
        .collect();
    for thread in threads {
        thread.join().unwrap();
    }
    assert_eq!(count("storage_errors_total", "kind=\"backend\""), 16);

    let store = Arc::new(MemoryStorage::new());
    let weak_store = Arc::downgrade(&store);
    let ttl = Duration::from_secs(60);
    let id = store
        .create_reservation(
            Reservation {
                backend: "metrics-test".into(),
                upstream_reservation_id: 123,
            },
            ttl,
        )
        .await
        .unwrap();
    store.get_reservation(id).await.unwrap().unwrap();
    assert!(matches!(
        store
            .create_reservation(
                Reservation {
                    backend: "metrics-test".into(),
                    upstream_reservation_id: 124
                },
                Duration::ZERO
            )
            .await,
        Err(StorageError::InvalidTtl)
    ));
    store.health_check().await.unwrap();
    metrics::inc_storage_health(false); // Future fallible storage backends use the same helper.

    let digest = TransactionDigest::new([7; 32]);
    let owner = Uuid::new_v4();
    assert_eq!(
        store
            .try_claim_execution(id, &digest, owner, ttl, ttl)
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
    assert_eq!(
        store
            .try_claim_execution(id, &digest, owner, ttl, ttl)
            .await
            .unwrap(),
        ExecutionClaim::Acquired
    );
    assert_eq!(
        store
            .try_claim_execution(id, &digest, Uuid::new_v4(), ttl, ttl)
            .await
            .unwrap(),
        ExecutionClaim::InFlight
    );
    assert_eq!(
        store
            .try_claim_execution(
                id,
                &TransactionDigest::new([8; 32]),
                Uuid::new_v4(),
                ttl,
                ttl
            )
            .await
            .unwrap(),
        ExecutionClaim::Conflict
    );
    assert!(matches!(
        store.mark_execution_dispatching(id, Uuid::new_v4()).await,
        Err(StorageError::Conflict)
    ));
    store.mark_execution_dispatching(id, owner).await.unwrap();
    assert_eq!(
        store
            .try_claim_execution(id, &digest, Uuid::new_v4(), ttl, ttl)
            .await
            .unwrap(),
        ExecutionClaim::PossiblySubmitted
    );
    assert_eq!(
        store.get_execution(id).await.unwrap().unwrap().phase,
        ExecutionPhase::Dispatching
    );

    store
        .put_transaction(&digest, cached(false), ttl)
        .await
        .unwrap();
    store
        .put_transaction(&digest, cached(true), ttl)
        .await
        .unwrap();
    store
        .put_transaction(&digest, cached(false), ttl)
        .await
        .unwrap();
    assert!(
        store
            .get_transaction(&digest)
            .await
            .unwrap()
            .unwrap()
            .local_execution_confirmed
    );
    store
        .complete_execution(id, owner, cached(false), ttl)
        .await
        .unwrap();
    store
        .complete_execution(id, owner, cached(false), ttl)
        .await
        .unwrap();
    assert_eq!(
        store
            .try_claim_execution(id, &digest, Uuid::new_v4(), ttl, ttl)
            .await
            .unwrap(),
        ExecutionClaim::Completed
    );
    let mut conflicting = cached(false);
    conflicting.effects = json!({"status":"conflicting"});
    assert!(matches!(
        store.complete_execution(id, owner, conflicting, ttl).await,
        Err(StorageError::Conflict)
    ));

    let second = store
        .create_reservation(
            Reservation {
                backend: "metrics-test".into(),
                upstream_reservation_id: 125,
            },
            ttl,
        )
        .await
        .unwrap();
    store
        .try_claim_execution(second, &digest, owner, ttl, ttl)
        .await
        .unwrap();
    store.cancel_execution(second, owner).await.unwrap();
    assert!(matches!(
        store.cancel_execution(second, owner).await,
        Err(StorageError::Conflict)
    ));

    for (method, expected) in [
        ("create_reservation", 3),
        ("get_reservation", 1),
        ("put_transaction", 3),
        ("get_transaction", 1),
        ("try_claim_execution", 7),
        ("get_execution", 1),
        ("mark_execution_dispatching", 2),
        ("complete_execution", 3),
        ("cancel_execution", 2),
        ("health_check", 1),
    ] {
        assert_eq!(
            count("storage_calls_total", &format!("method=\"{method}\"")),
            expected,
            "{method}"
        );
    }
    for (kind, expected) in [("claimed", 2), ("dispatching", 1), ("completed", 1)] {
        assert_eq!(
            count(
                "storage_execution_phase_transitions_total",
                &format!("kind=\"{kind}\"")
            ),
            expected
        );
    }
    for (kind, expected) in [
        ("acquired", 3),
        ("in_flight", 1),
        ("possibly_submitted", 1),
        ("completed", 1),
        ("conflict", 1),
    ] {
        assert_eq!(
            count(
                "storage_execution_claims_total",
                &format!("kind=\"{kind}\"")
            ),
            expected
        );
    }
    assert_eq!(
        count(
            "storage_cached_transactions_total",
            "backend=\"metrics-test\""
        ),
        1
    );
    assert_eq!(count("storage_errors_total", "kind=\"invalid_ttl\""), 1);
    assert_eq!(count("storage_errors_total", "kind=\"conflict\""), 3);
    assert_eq!(count("storage_health_checks_total", "kind=\"healthy\""), 1);
    assert_eq!(
        count("storage_health_checks_total", "kind=\"unhealthy\""),
        1
    );
    drop(store);
    assert!(
        weak_store.upgrade().is_none(),
        "metrics must not retain a storage instance"
    );

    reservation_metrics().await;

    // Label encoding is handled by the library, including escaping special characters.
    metrics::inc_storage_cached_transactions("quote\"slash\\newline\n");
    let text = metrics::render().unwrap();
    assert_eq!(
        sample(
            &text,
            r#"gas_station_router_storage_cached_transactions_total{backend="quote\"slash\\newline\n"}"#
        ),
        1
    );
    for family in [
        "storage_execution_phase_transitions",
        "storage_execution_claims",
        "storage_cached_transactions",
        "storage_errors",
        "storage_calls",
        "storage_health_checks",
        "backend_selector_calls",
        "backend_authorization_errors",
        "backend_health_transitions",
        "backend_health_check_failures",
        "reserve_gas_attempt_failures",
        "reserve_gas_retries",
        "execute_tx_errors",
        "execute_tx_recovery_retries",
        "reserve_gas_requests",
        "reserve_gas_errors",
        "reserve_gas_successes",
        "execute_tx_successes",
    ] {
        assert!(text.contains(&format!(
            "# TYPE gas_station_router_{family}_total counter\n"
        )));
    }
    assert!(text.contains("# TYPE gas_station_router_backend_registry_candidates gauge\n"));
    for status in ["401", "403"] {
        assert_eq!(
            sample(
                &text,
                &format!(
                    "gas_station_router_backend_authorization_errors_total{{backend=\"b\",status=\"{status}\"}}"
                )
            ),
            0
        );
    }

    let router = metrics::router();
    for _ in 0..2 {
        let response = router
            .clone()
            .oneshot(Request::get("/metrics").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()["content-type"], prometheus::TEXT_FORMAT);
        let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
        assert_eq!(
            std::str::from_utf8(&body).unwrap(),
            text,
            "scraping must not call storage or mutate counters"
        );
    }
}

async fn send(router: Router, body: Value, authorized: bool) -> StatusCode {
    let mut request = Request::post("/v1/reserve_gas").header("content-type", "application/json");
    if authorized {
        request = request.header("authorization", "Bearer client-token");
    }
    router
        .oneshot(request.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap()
        .status()
}

async fn reservation_metrics() {
    let upstream = MockServer::start().await;
    let closed = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let unavailable = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    let configs = [
        BackendConfig {
            name: "a".into(),
            enabled: true,
            token: String::new(),
            rpc: unavailable,
        },
        BackendConfig {
            name: "b".into(),
            enabled: true,
            token: String::new(),
            rpc: upstream.uri(),
        },
    ];
    // This scenario deliberately uses immediate passive exclusion to keep the
    // selector/HTTP counter assertions independent of the default threshold.
    let registry = Arc::new(
        BackendRegistry::with_passive_health(
            &configs,
            PassiveHealthCheckConfig {
                failure_threshold: 1,
                ..PassiveHealthCheckConfig::default()
            },
        )
        .unwrap(),
    );
    let client = http_client(Duration::from_secs(1)).unwrap();
    let service = ReservationService::new(
        registry.clone(),
        Arc::new(RoundRobinSelector::new()),
        Arc::new(MemoryStorage::new()),
        client,
        ReserveGasConfig {
            timeout: Duration::from_secs(1),
            retries: RetryConfig {
                max_retries: 1,
                backoff: Duration::ZERO,
            },
        },
        Duration::from_secs(900),
    )
    .unwrap();
    let router = api::router(
        Arc::new(service),
        common::execution(registry.clone(), Arc::new(MemoryStorage::new())),
        "client-token".into(),
    );
    let payload = json!({"gas_budget":1000000,"reserve_duration_secs":60});
    assert_eq!(
        send(router.clone(), payload.clone(), false).await,
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        send(router.clone(), json!({}), true).await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(count("reserve_gas_requests_total", ""), 2);
    assert_eq!(count("reserve_gas_errors_total", ""), 2);
    assert_eq!(count("reserve_gas_successes_total", ""), 0);
    assert_eq!(count("execute_tx_successes_total", ""), 0);

    Mock::given(method("POST")).and(path("/v1/reserve_gas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result":{"reservation_id":900,"sponsor_address":Address::new([1;32]),"gas_coins":[{"objectId":ObjectId::new([2;32]),"version":1,"digest":ObjectDigest::new([3;32])}]},"error":null
        }))).expect(1).mount(&upstream).await;
    assert_eq!(
        send(router.clone(), payload.clone(), true).await,
        StatusCode::OK
    );
    assert_eq!(
        count("reserve_gas_requests_total", ""),
        3,
        "retry is not an extra client call"
    );
    assert_eq!(
        count("reserve_gas_errors_total", ""),
        2,
        "successful failover is not a client error"
    );
    assert_eq!(
        count("reserve_gas_successes_total", ""),
        1,
        "successful failover is one successful client result"
    );
    assert_eq!(
        count(
            "backend_selector_calls_total",
            "algorithm=\"round_robin\",backend_name=\"a\""
        ),
        1
    );
    assert_eq!(
        count(
            "backend_selector_calls_total",
            "algorithm=\"round_robin\",backend_name=\"b\""
        ),
        1
    );
    assert_eq!(count("backend_registry_candidates", ""), 1);
    assert_eq!(
        count(
            "reserve_gas_attempt_failures_total",
            "backend=\"a\",reason=\"connection_refused\""
        ),
        1
    );
    assert_eq!(
        count(
            "reserve_gas_retries_total",
            "backend=\"a\",reason=\"connection_refused\""
        ),
        1,
        "a real failover attempt is attributed to the backend that failed"
    );
    assert_eq!(
        count("storage_calls_total", "method=\"create_reservation\""),
        4
    );
    upstream.verify().await;
    upstream.reset().await;

    // Business errors carried by HTTP 200 still count as a failed client call.
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":null,"error":"business rejection"})),
        )
        .expect(1)
        .mount(&upstream)
        .await;
    assert_eq!(
        send(router.clone(), payload.clone(), true).await,
        StatusCode::OK
    );
    assert_eq!(count("reserve_gas_errors_total", ""), 3);
    assert_eq!(
        count(
            "reserve_gas_attempt_failures_total",
            "backend=\"b\",reason=\"business_rejection\""
        ),
        1
    );
    assert_eq!(
        count(
            "reserve_gas_retries_total",
            "backend=\"b\",reason=\"business_rejection\""
        ),
        0,
        "retryable failure without an eligible peer must not count as an actual retry"
    );
    upstream.verify().await;
    upstream.reset().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(500).set_body_json(json!({"error":"backend failed"})))
        .expect(1)
        .mount(&upstream)
        .await;
    assert_eq!(
        send(router.clone(), payload.clone(), true).await,
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(count("reserve_gas_errors_total", ""), 4);
    assert_eq!(
        count(
            "reserve_gas_attempt_failures_total",
            "backend=\"b\",reason=\"http_error\""
        ),
        1
    );
    assert_eq!(
        count(
            "reserve_gas_retries_total",
            "backend=\"b\",reason=\"http_error\""
        ),
        0,
        "an ambiguous backend failure must not trigger a retry"
    );

    registry.set_healthy("b", false);
    assert!(registry.candidates(&HashSet::new()).is_empty());
    assert_eq!(
        send(router.clone(), payload, true).await,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(
        send(
            router,
            json!({"gas_budget":0,"reserve_duration_secs":60}),
            true
        )
        .await,
        StatusCode::BAD_REQUEST
    );
    assert_eq!(count("reserve_gas_requests_total", ""), 7);
    assert_eq!(count("reserve_gas_errors_total", ""), 6);
    assert_eq!(
        count("reserve_gas_successes_total", ""),
        1,
        "validation, business, backend, and availability errors must not count as successes"
    );
    assert_eq!(count("backend_registry_candidates", ""), 0);
    assert_eq!(
        count(
            "backend_selector_calls_total",
            "algorithm=\"round_robin\",backend_name=\"\""
        ),
        2
    );
    assert_eq!(
        count(
            "backend_selector_calls_total",
            "algorithm=\"round_robin\",backend_name=\"b\""
        ),
        3
    );
}
