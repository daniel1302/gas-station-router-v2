use serde::Serialize;

use super::health::{BackendHealth, HealthSnapshot};

#[derive(Serialize)]
pub(crate) struct RegistryStatus {
    summary: Summary,
    gas_stations: Vec<BackendStatus>,
}

#[derive(Default, Serialize)]
struct Summary {
    total: usize,
    enabled: usize,
    disabled: usize,
    healthy: usize,
    degraded: usize,
    unhealthy: usize,
    eligible_for_reservations: usize,
    inflight_reservations: usize,
}

#[derive(Serialize)]
pub(super) struct BackendStatus {
    pub name: String,
    pub enabled: bool,
    pub eligible_for_reservations: bool,
    pub inflight_reservations: usize,
    #[serde(flatten)]
    pub health: HealthSnapshot,
}

impl RegistryStatus {
    pub(super) fn new(gas_stations: Vec<BackendStatus>) -> Self {
        let mut summary = Summary {
            total: gas_stations.len(),
            ..Summary::default()
        };
        for station in &gas_stations {
            if station.enabled {
                summary.enabled += 1;
            } else {
                summary.disabled += 1;
            }
            match station.health.health {
                BackendHealth::Healthy => summary.healthy += 1,
                BackendHealth::Degraded => summary.degraded += 1,
                BackendHealth::Unhealthy => summary.unhealthy += 1,
            }
            summary.eligible_for_reservations += usize::from(station.eligible_for_reservations);
            summary.inflight_reservations += station.inflight_reservations;
        }
        Self {
            summary,
            gas_stations,
        }
    }
}
