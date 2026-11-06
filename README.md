# Gas Station Router v2

Minimal Rust service with a YAML config, a Clap CLI, separate RPC and metrics
HTTP servers and an in-memory implementation of the storage trait.
`POST /v1/reserve_gas` routes reservations to enabled, healthy backends and stores
the mapping before returning a router-generated reservation ID. Transaction
execution is still a dummy handler.

`api` contains HTTP routing and handlers (`health`, `reservation`, `execution`).
`services::reservation::ReservationService` owns reservation selection, retry
policy and mapping storage. `services::execution::ExecutionService` is a
placeholder for future execution logic. `server` wires dependencies, starts
listeners and manages shutdown and backend health monitoring.

```sh
cargo run -- init                       # YAML on stdout
cargo run -- init --config-file config.yaml
# --output config.yaml is also accepted; existing files are never overwritten.

export GS_AUTH='example-router-token'
export IOTA_RPC_URL='http://localhost:9000'
export REDIS_URL='redis://localhost:6379'
export GAS_STATION_B_TOKEN='example-backend-token'
cargo run -- start --config-file config.yaml
```

See [config.example.yaml](config.example.yaml) for the complete config. `start`
requires `--config-file`. Use `storage.kind: memory`; Redis storage is not
implemented yet and selecting it fails startup. The `memory` kind can omit the
`redis` section. All environment references present in the file are resolved,
including those in an unused Redis section.

The RPC token, IOTA RPC URL, Redis URL, backend tokens and backend RPC URLs accept
whole-value environment references: `${NAME}` or `$NAME`. Other values are used
literally.
Missing variables fail config loading. Substitution happens after YAML parsing,
so environment values containing YAML syntax are preserved as strings. `init`
keeps the placeholders and does not require environment variables.

Durations use human-readable strings such as `3s`, `200ms`, `15m` and `24h`.
Unknown config fields are rejected. Backend RPC ports must differ from the
router's port when running on the same host.

| Listener | Method | Path | Response |
| --- | --- | --- | --- |
| RPC (`0.0.0.0:9257`) | GET | `/health` | `200`, `{"status":"ok"}` |
| RPC | POST | `/v1/reserve_gas` | Gas Station response with a router reservation ID |
| RPC | POST | `/v1/execute_transaction` | `501`, `{"error":"not implemented"}` |
| Metrics (`0.0.0.0:9184`) | GET | `/metrics` | Prometheus text, `gas_station_router_up 1` |

The config key is `routing.execute_tx`; the HTTP path is
`/v1/execute_transaction`. Both listeners bind before serving. SIGINT and SIGTERM
gracefully stop both servers.

Logging uses `tracing` with timestamps, levels, module targets and event fields.
Logs go to stderr; `init` keeps stdout as YAML only. The default level is `info`;
override it with `RUST_LOG`, for example:

```sh
RUST_LOG=gas_station_router_v2=debug cargo run -- start --config-file config.yaml
```

Startup, listener addresses, graceful shutdown and fatal command errors are
logged. Fatal errors exit with code 1. ANSI colors are enabled only when stderr
is a terminal.

Reservations accept `{"gas_budget": 1000000, "reserve_duration_secs": 60}`.
When `rpc.token` is nonempty, this endpoint requires `Authorization: Bearer <token>`.
The upstream receives its own configured backend token, not the client's token.
The Gas Station response envelope (`result` / `error`), sponsor, gas coins and
extra response fields are preserved. Only a successful `result.reservation_id`
is replaced; storage retains the original ID and backend name for execution.
The mapping uses `routing.ttl.reservation`.

The backend registry initially treats enabled backends as healthy. It probes each
enabled backend's base RPC URL (`GET /` for a normal Gas Station) at startup and
every five seconds, with `routing.reserve_gas.timeout`. A 2xx response makes the
backend eligible; this is an HTTP health check, not an execution canary. Set
`enabled: false` on a backend to exclude it from probes and new reservations;
omitting `enabled` defaults to true. Selection is round robin and performs no
network or storage I/O. Candidate `inflight` counts active backend requests.

The HTTP handler in `api::reservation` calls
`ReservationService::reserve_with_retry()` for selection, retries and mapping
storage; `reserve_once()` performs exactly one backend HTTP request.

```yaml
routing:
  reserve_gas:
    timeout: 3s
    retries:
      max_retries: 2
      backoff: 200ms
```

`max_retries` counts additional attempts: 2 allows at most 3 backend requests.
`ReserveError::is_retryable()` replaces the old `retry_on` config field:

| Error | Retry | Classification |
| --- | --- | --- |
| `ConnectionFailed` | Yes | Connection establishment failed, including a connect timeout; request not delivered. |
| `ReservationRejected` | Yes | Explicit negative `result: null` / nonempty `error` envelope with HTTP 2xx or 429, or a known safe Gas Station rejection below. |
| `UnknownOutcome` | No | Response/body timeout, interrupted/malformed response, or an unrecognized 5xx; reservation may exist. |
| `Permanent` | No | Invalid request, other HTTP 4xx, redirect or request construction failure. |

The current Gas Station uses string errors for safe rejections even with 5xx:
`Unable to reserve gas coins for the given budget.`,
`Gas station is in maintenance mode. Please try again later.`, and
`Gas usage exceeds daily cap`. Only exact matches in a negative envelope qualify;
generic 5xx and Redis errors remain ambiguous.

Retry selects another eligible backend, excludes the failed one for this request,
and waits `backoff` before sending. Connection failures also mark the backend
unhealthy until a successful probe. The timeout is per HTTP attempt, including
the response body. Redirects and automatic HTTP client retries are disabled.
Exhausting the retry budget or candidates returns the last failure. No initially
eligible backend or a failed mapping write returns 503; connection errors return
502 and timeouts return 504. Storage failures never trigger a new reservation;
upstream reservations then expire normally.

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

Metrics are process-wide and initialized on first use with `std::sync::LazyLock`.
The `prometheus` library provides atomic integer counters and text encoding.
The metrics module owns no storage instances; `/metrics` only gathers static
collectors. Scraping never calls storage or probes dependencies. Fixed labels
use bounded method/outcome/error names; backend labels use configured names,
never tokens, RPC URLs, reservation IDs or transaction digests.

All names below have the prefix `gas_station_router_`:

| Name | Labels | Meaning |
| --- | --- | --- |
| `storage_execution_phase_transitions_total` | `kind` | Entry into `claimed`, `dispatching`, `completed`; idempotent calls do not add transitions. |
| `storage_execution_claims_total` | `kind` | Claim results: `acquired`, `in_flight`, `possibly_submitted`, `completed`, `conflict`. |
| `storage_cached_transactions_total` | `backend` | New cache inserts, including inserts after TTL expiry; reads and upgrades are excluded. |
| `storage_errors_total` | `kind` | Storage `Err` results: `backend`, `invalid_ttl`, `id_exhausted`, `conflict`. |
| `storage_calls_total` | `method` | All storage method calls, including `health_check` and failed calls. |
| `storage_health_checks_total` | `kind` | `healthy` / `unhealthy` health check outcomes. |
| `backend_selector_calls_total` | `algorithm`, `backend_name` | Each selection, including retries; empty backend name means no candidate was selected. |
| `backend_registry_candidates` | none | Gauge: the latest candidate count after enabled/health/retry filtering. |
| `reserve_gas_requests_total` | none | Incoming reserve HTTP calls, including auth/validation rejections; retries do not add requests. |
| `reserve_gas_errors_total` | none | Failed client responses, once per call; includes business errors returned with HTTP 200. |

These storage counters describe events, not live cache sizes or current phase
populations. `storage_health_checks_total` is a result counter, not a health
gauge. A claim conflict is an ordinary claim outcome; it increments
`storage_errors_total` only when an operation actually returns `Err`.
The existing `gas_station_router_up` gauge is also retained. Backend label series
appear on first use; fixed label sets are initialized to zero.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```
