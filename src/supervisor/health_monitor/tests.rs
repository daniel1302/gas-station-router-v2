use super::*;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

#[tokio::test]
async fn startup_grace_prevents_restart_and_readiness_can_block_a_live_process() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/"))
        .respond_with(ResponseTemplate::new(200))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/ready"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let config = HealthConfig {
        readiness_path: Some("/ready".into()),
        unhealthy_threshold: 2,
        ..Default::default()
    };
    let mut monitor = HealthMonitor::new(config, server.uri(), Duration::from_secs(60)).unwrap();
    monitor.check().await;
    assert_eq!(monitor.status.live, Some(true));
    assert_eq!(monitor.status.ready, Some(false));
    assert!(!monitor.unhealthy());
    monitor.started = Instant::now() - Duration::from_secs(61);
    monitor.check().await;
    monitor.check().await;
    assert!(monitor.unhealthy());
    monitor.reset();
    assert!(!monitor.unhealthy());
}

#[tokio::test]
async fn disabled_monitor_does_not_send_probes() {
    let server = MockServer::start().await;
    let config = HealthConfig {
        enabled: false,
        ..Default::default()
    };
    let mut monitor = HealthMonitor::new(config, server.uri(), Duration::ZERO).unwrap();
    monitor.check().await;
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(!monitor.unhealthy());
}
