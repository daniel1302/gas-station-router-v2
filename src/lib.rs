pub mod router;

// Keep the existing library imports working during the module reorganization.
pub use router::{api, backend, config, metrics, routing, server, services, storage};
