# Gas Station Router v2

Minimal Rust service with a YAML config, a Clap CLI, separate RPC and metrics
HTTP servers and an in-memory implementation of the storage trait.
`POST /v1/reserve_gas` routes reservations to enabled, healthy backends and stores
the mapping before returning a router-generated reservation ID. Transaction
execution always uses the backend recorded by the reservation.

Local Redis + three Gas Stations and incremental Rust e2e tests:
[docker/README.md](docker/README.md). The `reserve` test works with the current
router; `transactions` submits signed transactions and verifies their effects.

`api` contains HTTP routing and handlers (`health`, `reservation`, `execution`).
`services::reservation::ReservationService` owns reservation selection, retry
policy and mapping storage. `services::execution::ExecutionService` owns execution,
deduplication, effects caching and RPC recovery. `server` wires dependencies, starts
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
| RPC | POST | `/v1/execute_tx` | Transaction effects and local execution confirmation |
| RPC | POST | `/v1/execute_transaction` | Alias of `/v1/execute_tx` |
| Metrics (`0.0.0.0:9184`) | GET | `/metrics` | Prometheus text, `gas_station_router_up 1` |

The execution config key is `routing.execute_tx`. Both listeners bind before serving. SIGINT and SIGTERM
gracefully stop both servers.

Logging uses `tracing` with timestamps, levels, module targets and event fields.
Logs go to stderr; `init` keeps stdout as YAML only. The default level is `info`;
override it with `RUST_LOG`, for example:

```sh
RUST_LOG=gas_station_router_v2=debug cargo run -- start --config-file config.yaml
```

Set `LOG_FORMAT=json` for one JSON object per log line, with structured event fields
under `fields`. `LOG_FORMAT=text` is the default when the variable is absent.
Other values fail startup. `RUST_LOG` controls filtering in both formats:

```sh
LOG_FORMAT=json RUST_LOG=info cargo run -- start --config-file config.yaml
# Or, from docker/:
LOG_FORMAT=json RUST_LOG=info ./start-router.sh
```

Startup, listener addresses, graceful shutdown and fatal command errors are
logged. Fatal errors exit with code 1. In text mode, ANSI colors are enabled only
when stderr is a terminal. JSON logs never contain ANSI colors. Both formats use
stderr, so `init` output remains YAML on stdout.

Reservations accept `{"gas_budget": 1000000, "reserve_duration_secs": 60}`.
When `rpc.token` is nonempty, this endpoint requires `Authorization: Bearer <token>`.
The upstream receives its own configured backend token, not the client's token.
The Gas Station response envelope (`result` / `error`), sponsor, gas coins and
extra response fields are preserved. Only a successful `result.reservation_id`
is replaced; storage retains the original ID and backend name for execution.
The mapping uses `routing.ttl.reservation`.

The backend registry initially treats enabled backends as healthy.
`backend::health::BackgroundHealthChecker` runs as a separate Tokio task, with
parallel `GET /` probes of enabled Gas Stations. The first round starts immediately,
then repeats at `health_checks.interval` (default `5s`). Each probe uses
`health_checks.timeout` (default `1500ms`), independently of reservation timeouts.
HTTP `200` is a success; other statuses, connection errors and timeouts are failures.
Probes do not follow redirects or retry automatically.

Counters track consecutive outcomes per backend. A success resets failures;
a failure resets successes. After `unhealthy_threshold` failures (default `3`)
the backend is excluded from new reservations. After `healthy_threshold` successes
(default `2`) it becomes eligible again. Probing continues for unhealthy backends.
Health changes do not remove existing reservation mappings. Each backend has its
own worker and schedule, so a slow node cannot delay other nodes' probes. Probes
for the same backend do not overlap and missed interval ticks are skipped.
Shutdown cancels every worker and its outstanding probe.

`health_checks.enabled: false` disables the background checker. Set
`enabled: false` on a backend to exclude it from probes and new reservations;
omitting that backend option defaults to true. Omitting `health_checks` uses the
example config's defaults. Enabled checks require nonzero intervals, timeouts and
thresholds. The `deep_check` settings are reserved for future work.
Selection is round robin and performs no network or storage I/O. Candidate
`inflight` counts active reservation requests, excluding health probes.

Passive checks observe each upstream reservation attempt when
`health_checks.passive.enabled` is true (default). They keep a separate reservation
failure counter per backend. A valid successful reservation resets it, even if
the router subsequently fails to store its mapping. Connection failures/timeouts,
response timeouts and generic HTTP `500/502/503/504` responses increment it once.
After `passive.failure_threshold` failures (default `3`), the backend becomes
`Unhealthy` and is excluded from new reservations. The active checker restores
it after `healthy_threshold` successes. Active probes do not reset the reservation
failure counter: only a successful reservation establishes that reservations work.
With background checks disabled, an `Unhealthy` backend needs an explicit health
reset or a router restart to become eligible again.

HTTP `400`, rate limiting, maintenance and daily-cap rejections do not count as
backend faults. Neutral responses leave the counter unchanged. HTTP `401/403`
responses emit an `ERROR` log with `alert=backend_reservation_authorization` and
increment `gas_station_router_backend_authorization_errors_total{backend,status}`.
These authorization alerts remain enabled when passive state updates are disabled.
Only configured backend names and status codes are logged; tokens and response
bodies are excluded. Client authorization failures at the router do not trigger
backend authorization alerts.

The exact negative-envelope error `Unable to reserve gas coins for the given budget.`
marks the backend `Degraded`, without counting it as a backend failure. The first
cooldown lasts `passive.initial_cooldown` (default `15s`). Each subsequent rejection
multiplies the last duration by the integer `passive.backoff` (default `2`), capped at
`passive.max_cooldown` (default `60s`): `15s`, `30s`, `60s`, `60s`, and so on.
Each backend tracks its own cooldown. A further rejection from an already in-flight
request also increases the duration and restarts the timer. Multiplication saturates
before applying the cap, so large values cannot overflow.

During cooldown the backend receives no new reservations. Successful active probes
or already in-flight reservations cannot shorten cooldown or reset its backoff.
After it expires, the backend is eligible again if active checks have not marked it
`Unhealthy`. Expiry alone does not reset backoff: a successful reservation after
expiry resets it, so the next cooldown starts at `initial_cooldown` again. Existing
reservation mappings are preserved.

Enabled passive checks require a positive failure threshold and `initial_cooldown`,
`backoff` of at least `1`, and `max_cooldown` at least as long as `initial_cooldown`.
Use `backoff: 1` for a fixed cooldown. The old `passive.cooldown` key has been renamed
to `passive.initial_cooldown`; update existing configs and add `backoff` and
`max_cooldown`. The old key is rejected as an unknown field.

Health transitions emit `Backend health changed` with `backend`, `previous_health`,
`health`, `source`, and `reason`. The reason identifies the observation that triggered
the transition. HTTP observations include `http_status`; transport failures use
`connection_refused`, `connection_timeout`, `request_timeout`, `connection_error`,
or `transport_error`, without logging raw errors or request URLs.

The `source` is `background`, `passive`, `cooldown`, or `manual`. Further reasons
include `http_error`, `http_ok`, `no_gas_coins`, `cooldown_expired`, and
`manual_override`. Logs include active success/failure counts, the reservation
failure count, the applicable threshold, and cooldown duration/remaining time in
milliseconds when available. Transitions to `Degraded` or `Unhealthy` use `WARN`;
transitions to `Healthy` use `INFO`.

```text
WARN Backend health changed backend=gas_station_1 previous_health=Healthy health=Unhealthy source="background" reason="http_error" http_status=503 active_success_count=0 active_failure_count=3 reservation_failure_count=0 threshold=3
```

Unchanged health does not emit a transition log. Cooldown expiry is logged once,
when next observed by routing, an explicit health read, or an active/passive check.
It is recorded before applying a new observation, so an unrelated HTTP response is
not attributed as the cause of cooldown recovery. Expiry while the backend remains
`Unhealthy` does not produce a false recovery event. Authorization alerts retain
their separate per-failure `ERROR` log and metric.

Every failed active probe also emits `WARN Backend health check failed`, even
before the unhealthy threshold is reached or when the backend is already
unhealthy. It includes the backend, reason, HTTP status when available, current
failure count, and threshold. Each failed upstream reservation attempt emits
`WARN Backend reservation attempt failed` with its attempt number, failure kind,
and whether the retry policy permits another attempt. A separate
`WARN Retrying reservation on another backend` identifies actual retries and
the backend selected for the next attempt.

Execution failures emit `WARN Execution request failed` with a bounded reason,
HTTP status, reservation ID, and transaction digest when available. An execution
span carries the reservation ID and digest through the owned background task.
Uncertain backend responses and failed IOTA RPC calls emit their own warnings,
including backend, reason, HTTP status or JSON-RPC error code when available,
and the next action. RPC recovery retries are logged as well. A failed transaction
in finalized effects emits a warning even when its HTTP response is 200.
Logs omit tokens, signatures, transaction bytes, raw upstream error text, response
bodies, and request URLs. These warnings are available in both text and JSON
logging; the default `INFO` filter includes `WARN`.

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
and waits `backoff` before sending. Passive health observations do not change
retry safety: an ambiguous timeout or generic 5xx is never automatically retried.
The timeout is per HTTP attempt, including
the response body. Redirects and automatic HTTP client retries are disabled.
Exhausting the retry budget or candidates returns the last failure. No initially
eligible backend or a failed mapping write returns 503; connection errors return
502 and timeouts return 504. Storage failures never trigger a new reservation;
upstream reservations then expire normally.

Execution accepts the router reservation ID, base64 BCS transaction bytes, the
base64 user signature, and an optional request type:

```json
{
  "reservation_id": 123,
  "tx_bytes": "<base64 BCS Transaction>",
  "user_sig": "<base64 UserSignature>",
  "request_type": "waitForLocalExecution"
}
```

`request_type` defaults to `waitForEffectsCert`; `requestType` is also accepted.
Execution requires the same router Bearer token as reservations. Invalid JSON,
transaction BCS or signature encoding returns `400` before storage is modified.
The digest is calculated from the decoded transaction using the pinned IOTA SDK.

`ExecutionService` exposes only `new()` and `execute()`. Its `executor`, `inflight`
and `recovery` modules are private.

**Execution flow**

```text
                         POST /v1/execute_tx
                   (alias: /v1/execute_transaction)
                                  |
                                  v
                   HTTP authorization + JSON validation
                                  |
                                  v
                       ExecutionService.execute()
                                  |
                                  v
             Decode transaction/signature, calculate TransactionDigest
                                  |
                                  v
                   Owned task + overall execution timeout
                                  |
                                  v
                  Storage.get_execution(reservation_id)
                                  |
              Existing record with a different digest? -- yes --> 409 Conflict
                                  |
                                  no
                                  |
           +----------------------+-----------------------+
           |                      |                       |
           v                      v                       v
     Missing / Claimed        Dispatching              Completed
           |                      |                       |
           v                      v                       v
 Storage.try_claim_execution() Recovery [B]    Storage.get_transaction(digest)
           |                                          /       \
     +-----+------+                                 Found    Missing
     |            |                                   |         |
     v            v                                   v         v
  Acquired     InFlight                        Request type [C] Recovery [B]
     |            |
     v            v
 Dispatch [A]  Wait 100 ms, reread execution state


 Dispatch [A]: execution lease acquired
                     |
                     v
          Storage.get_reservation()
                     |
                     v
       Find the original configured backend
                     |
                     v
       Storage.get_transaction(digest)
                     |
                     v
 Check cached backend matches / build upstream request
                     |
                     v
   Storage.mark_execution_dispatching()
                     |
          +----------+--------------------+
          |                               |
     Cache exists                     Cache missing
          |                               |
          |                               v
          |             POST Gas Station /v1/execute_tx
          |              (original backend, upstream ID)
          |                               |
          |                     +---------+----------+
          |                     |                    |
          |              Validated effects     Uncertain result
          |                     |                    |
          +----------+----------+                    v
                     |                         Recovery [B]
                     |                      (keep current lease)
                     v
        Storage.complete_execution()
        (cache effects + Completed,
         preserve record TTL, release lease)
                     |
                     v
              Request type [C]
                     |
                     v
                  Response
```

If `try_claim_execution()` observes a concurrent transition and returns
`PossiblySubmitted` or `Completed`, the service rereads execution state. A claim
conflict returns `409`. Pre-dispatch mapping/backend errors cancel the claim.
Every wait and RPC operation is bounded by the overall execution timeout.

Only the original reservation backend receives the POST, using its own token and
upstream reservation ID. Health filtering applies to new reservations, not execution
of existing ones. Execution is never failed over to another Gas Station. A different
digest for an existing execution returns `409`. Missing reservations return `404`;
missing backend configuration or unavailable storage returns `503`.

`Dispatching` is persisted before sending. A failed or uncertain storage transition
prevents the POST. After dispatch, timeouts, redirects, error responses and malformed
effects lead only to read-only RPC recovery. Neither lease expiry nor a lookup that
returns not found authorizes another POST. Recovery uses an atomic storage lease,
fences the previous owner, and preserves the execution record's phase and TTL.
Completion releases the lease. Backend metadata is retained in the execution record
so recovery survives reservation mapping expiry or removal of the backend config.

**Recovery flow [B]**

```text
       Existing Dispatching / Completed with missing cache
                              |
                              v
                 Storage.try_claim_recovery()
                              |
             +----------------+----------------+
             |                |                |
             v                v                v
          Acquired         InFlight         Conflict
             |                |                |
             |          Wait 100 ms,        409 Conflict
             |          reread state
             |
             +<--- Current dispatch owner after an uncertain POST
             |     (already owns the lease; no new claim needed)
             v
     IOTA RPC: iota_getTransactionBlock(showEffects: true)
             |
     +-------+-----------------------------------+
     |                                           |
     v                                           v
 Matching, validated effects          Not found / RPC error / invalid result
     |                                           |
     v                                           v
 Storage.complete_execution()             Wait 200 ms, retry lookup
 (cache effects + Completed,                      |
  preserve record TTL, release lease)             v
     |                                   Overall deadline reached?
     v                                     /              \
 Request type [C]                         no                yes
     |                                    |                 |
     v                                    v                 v
  Response                           RPC lookup again   504 Still unknown
                                                       (phase unchanged)
```

Recovery only performs RPC lookups; it never submits the transaction again.
For an already-owned dispatch lease, recovery runs within the original request's
remaining time budget. A new request waits for or acquires the recovery lease.
When recovery saves effects, the response still passes through request-type
handling: `Completed` alone does not establish local execution confirmation.

RPC recovery polls `iota_getTransactionBlock` with `showEffects: true`. The digest,
effects identity, final execution status and required effects fields are checked.
Both successful and failed finalized transactions are cached. Empty optional lists
and object-reference version representations are normalized between Gas Station and
JSON-RPC responses. Pending, mismatched and invalid results are never cached.

**Request-type handling [C]**

| Cache | Request type | Behavior |
| --- | --- | --- |
| Effects + local confirmation `false` | `waitForEffectsCert` | Return cache |
| Effects + local confirmation `true` | `waitForEffectsCert` | Return cache |
| Effects + local confirmation `true` | `waitForLocalExecution` | Return cache |
| Effects + local confirmation `false` | `waitForLocalExecution` | Verify local execution through RPC, upgrade cache, return effects |

Local execution is independently confirmed through RPC: an explicit
`confirmedLocalExecution: true`, or `iota_isTransactionIndexedOnNode: true` after
retrieving matching effects. A Gas Station success or the mere presence of effects
in a remote transaction store is insufficient. `iota.rpc_url` must point to the
fullnode used by the Gas Stations; local confirmation refers to that configured
node. RPC requests never carry router/backend authorization headers. See the
[IOTA RPC reference](https://docs.iota.org/iota-api-ref#iota_gettransactionblock).

The response contains `effects`, `error`, and `confirmedLocalExecution`. Local
confirmation can only be upgraded from false to true and never refreshes cache TTL.
If local confirmation is unavailable, effects remain cached and the request returns
`504` at its deadline. `waitForEffectsCert` can still read those cached effects.
Concurrent execution requests wait for the stored result. Dropping the client
connection does not cancel the owned execution task.

`routing.execute_tx.timeout` bounds the entire operation, including waiting and
recovery. RPC calls additionally use `iota.timeout`; polling waits 100 ms for another
owner and 200 ms between RPC lookups. The execution lease must exceed the overall
timeout. Record TTL must exceed the lease and be at least the reservation TTL.
Effects TTL must be positive. Existing configs already satisfy these constraints.
On `504`, retry the same reservation ID and transaction bytes. Recovery is driven
by requests and only runs while a request is active. Retention is bounded by the
configured TTLs; in-memory storage loses execution state on restart.

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
| `backend_authorization_errors_total` | `backend`, `status` | Backend reservation HTTP 401/403 responses, once per attempt; status is bounded to 401/403. |
| `backend_health_transitions_total` | `backend`, `previous_health`, `health` | Effective state changes, including active/passive checks, cooldown expiry, and manual overrides; states are `healthy`, `degraded`, `unhealthy`. |
| `backend_health_check_failures_total` | `backend`, `reason` | Every failed active probe, including failures before the threshold and while already unhealthy. |
| `reserve_gas_requests_total` | none | Incoming reserve HTTP calls, including auth/validation rejections; retries do not add requests. |
| `reserve_gas_errors_total` | none | Failed client responses, once per call; includes business errors returned with HTTP 200. |
| `reserve_gas_successes_total` | none | Successful client responses after storing the reservation mapping, once per call; successful failover counts once. |
| `reserve_gas_attempt_failures_total` | `backend`, `reason` | Every failed upstream reservation attempt, including business rejections and attempts followed by successful failover. |
| `reserve_gas_retries_total` | `backend`, `reason` | Additional upstream attempts actually started; labels identify the failed backend and reason that caused the retry. |
| `execute_tx_errors_total` | `reason` | Failed execution results, once per call, including auth/validation errors, storage failures, conflicts, timeouts, and finalized transaction failures returned with HTTP 200. |
| `execute_tx_successes_total` | none | Successful execution results, once per call, including successful recovery and cache reads; finalized failed effects are excluded. |
| `execute_tx_recovery_retries_total` | `backend`, `stage` | Additional RPC lookup rounds actually started after an inconclusive result; `stage` is `recovery` or `local_execution`. |

Health transition counters increment only when the effective state changes. Failed
active probes and passive reservation failures have separate counters. A cooldown
expiry is counted when first observed, following the same rules as its transition
log; scraping alone does not observe expiry or change counters.

Reservation retry counters exclude the initial attempt, exhausted retry budgets,
and retryable failures with no eligible next backend. Unsafe or ambiguous failures
never authorize a retry. The retry counter increments after backoff, immediately
before the additional upstream attempt begins.

RPC retry counters exclude the initial lookup and increment when the next lookup
round starts after backoff. A round may check both transaction effects and local
indexing. They count read-only recovery/polling, including pending local execution,
and never imply another Gas Station execution POST.

Execution errors include both execution endpoint aliases. Successful recovery does
not increment the final error counter, although intermediate failures still emit
warnings. Each response containing finalized failed effects counts, including
cache reads. The owned execution task records its final result even if the client
disconnects. Invalid HTTP requests are counted by the API handler; service results
are counted by the service, so a single request is not counted twice.

Success counters are initialized to zero and count successful request results,
not unique on-chain transactions. A reservation counts only after its mapping is
stored and the router returns a successful response without a business error.
An execution counts when it returns successful effects and satisfies the requested
confirmation mode. Each successful cache read counts as one success; retries
within a single request do not add successes. Finalized failed effects returned
with HTTP 200 increment only the execution error counter.

Health probe reasons are `http_error`, `connection_refused`, `connection_timeout`,
`request_timeout`, `connection_error`, and `transport_error`. Reservation reasons
also distinguish `invalid_response`, `redirect`, `authorization_error`,
`invalid_request`, `rate_limited`, `no_gas_coins`, `maintenance`, `daily_cap`, and
`business_rejection`. Reasons are fixed categories, never upstream error strings.

These storage counters describe events, not live cache sizes or current phase
populations. `storage_health_checks_total` is a result counter, not a health
gauge. A claim conflict is an ordinary claim outcome; it increments
`storage_errors_total` only when an operation actually returns `Err`.
The existing `gas_station_router_up` gauge is also retained. Health, authorization,
reservation attempt/retry, and execution recovery retry series are initialized to
zero for each configured backend and their bounded labels, so the first event is
observable as an increment. Other backend label series appear on first use; fixed
label sets are initialized to zero. Log filters do not affect metric increments.

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```
