use std::{collections::VecDeque, sync::Mutex};

use super::{BackendCandidate, BackendSelector};
use crate::router::metrics;

/// Rotates backend names, so filtering a failed backend does not skip its peer.
#[derive(Default)]
pub struct RoundRobinSelector {
    order: Mutex<VecDeque<String>>,
}

impl RoundRobinSelector {
    pub fn new() -> Self {
        Self::default()
    }
}

impl BackendSelector for RoundRobinSelector {
    fn select(&self, candidates: &[BackendCandidate]) -> Option<usize> {
        let selected = self.select_candidate(candidates);
        metrics::inc_backend_selector_call(
            "round_robin",
            selected
                .and_then(|index| candidates.get(index))
                .map(|backend| backend.name.as_str()),
        );
        selected
    }
}

impl RoundRobinSelector {
    fn select_candidate(&self, candidates: &[BackendCandidate]) -> Option<usize> {
        if candidates.is_empty() {
            return None;
        }
        let mut order = self.order.lock().unwrap_or_else(|error| error.into_inner());
        for candidate in candidates {
            if !order.contains(&candidate.name) {
                order.push_back(candidate.name.clone());
            }
        }
        let position = order
            .iter()
            .position(|name| candidates.iter().any(|candidate| candidate.name == *name))?;
        let name = order.remove(position)?;
        let selected = candidates
            .iter()
            .position(|candidate| candidate.name == name);
        order.push_back(name);
        selected
    }
}
