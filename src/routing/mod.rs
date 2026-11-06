pub mod round_robin;

/// A backend eligible to receive a new gas reservation.
#[derive(Debug, Clone)]
pub struct BackendCandidate {
    /// Stable backend name from configuration.
    pub name: String,

    /// Relative selection weight.
    pub weight: u32,

    /// Number of active requests observed by this router.
    pub inflight: usize,
}

/// Selects a backend for a new reserve_gas request.
///
/// Selection must not perform network or storage I/O.
///
/// Only eligible backends are provided as candidates:
/// - Enabled in configuration.
/// - Considered healthy.
/// - Not excluded by the current retry attempt.
///
/// Implementations must be safe for concurrent use.
///
/// The returned index refers to `candidates`, not to the
/// complete backend registry.
pub trait BackendSelector: Send + Sync {
    /// Selects a backend.
    ///
    /// Returns None if no candidates are available.
    fn select(&self, candidates: &[BackendCandidate]) -> Option<usize>;
}
