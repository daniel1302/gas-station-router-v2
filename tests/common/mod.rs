use std::{sync::Arc, time::Duration};

use gas_station_router_v2::{
    backend::registry::BackendRegistry,
    config::{ExecuteTxConfig, InflightConfig, IotaConfig, TtlConfig},
    services::execution::ExecutionService,
    storage::Storage,
};

pub fn execution(
    registry: Arc<BackendRegistry>,
    storage: Arc<dyn Storage>,
) -> Arc<ExecutionService> {
    Arc::new(
        ExecutionService::new(
            registry,
            storage,
            ExecuteTxConfig {
                timeout: Duration::from_secs(1),
                inflight: InflightConfig {
                    lease_ttl: Duration::from_secs(2),
                    record_ttl: Duration::from_secs(60),
                },
            },
            IotaConfig {
                rpc_url: "http://127.0.0.1:9".into(),
                timeout: Duration::from_secs(1),
            },
            TtlConfig {
                reservation: Duration::from_secs(60),
                transaction_effects: Duration::from_secs(60),
            },
        )
        .unwrap(),
    )
}
