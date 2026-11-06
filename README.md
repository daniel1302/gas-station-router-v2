# Gas Station Router v2

Minimal Rust service with a YAML config, a Clap CLI, separate RPC and metrics
HTTP servers and an in-memory implementation of the storage trait. RPC handlers
are still dummy handlers: routing, authentication and storage integration are not
implemented yet. Startup does not connect to Redis or the backends.

```sh
cargo run -- init                       # YAML on stdout
cargo run -- init --config-file config.yaml
# --output config.yaml is also accepted; existing files are never overwritten.

export GS_AUTH='example-router-token'
export REDIS_URL='redis://localhost:6379'
export GAS_STATION_B_TOKEN='example-backend-token'
cargo run -- start --config-file config.yaml
```

See [config.example.yaml](config.example.yaml) for the complete config. `start`
requires `--config-file`. The `memory` storage kind can omit the `redis` section;
the `redis` kind requires it.

The RPC token, Redis URL, backend tokens and backend RPC URLs accept whole-value
environment references: `${NAME}` or `$NAME`. Other values are used literally.
Missing variables fail config loading. Substitution happens after YAML parsing,
so environment values containing YAML syntax are preserved as strings. `init`
keeps the placeholders and does not require environment variables.

Durations use human-readable strings such as `3s`, `200ms`, `15m` and `24h`.
Unknown config fields are rejected. The example mirrors the proposed backend
addresses; backend A currently shares the router's default port, so change that
address before implementing forwarding.

| Listener | Method | Path | Dummy response |
| --- | --- | --- | --- |
| RPC (`0.0.0.0:9257`) | GET | `/health` | `200`, `{"status":"ok"}` |
| RPC | POST | `/v1/reserve_gas` | `501`, `{"error":"not implemented"}` |
| RPC | POST | `/v1/execute_transaction` | `501`, `{"error":"not implemented"}` |
| Metrics (`0.0.0.0:9184`) | GET | `/metrics` | Prometheus text, `gas_station_router_up 1` |

The config key is `routing.execute_tx`; the HTTP path is
`/v1/execute_transaction`. Both listeners bind before serving. SIGINT and SIGTERM
gracefully stop both servers.

`storage::memory::MemoryStorage::new()` implements `storage::Storage`. Cloning a
store shares its state. Entries expire by a monotonic clock and are cleaned on
access and writes. Transaction upgrades never extend TTL or downgrade local
execution confirmation. Execution claims, ownership checks, dispatch and atomic
completion use the same lock. Expired leases can only be reclaimed before
dispatch; completion is idempotent and preserves the execution record TTL.
Missing/expired execution records, invalid ownership and invalid transitions
return `StorageError::Conflict`. Reclaiming requires a fresh token and preserves
the original record TTL; a repeated live claim by its owner preserves both TTLs.

Memory storage is process-local and loses its state on restart. IDs are unique
across stores within one process, but there is no cross-process coordination or
restart persistence. The caller supplies finalized effects and independently
verifies local execution, as required by the trait.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```
