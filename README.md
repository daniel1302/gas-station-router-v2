# Gas Station Router v2

A Rust router for IOTA Gas Stations. It distributes gas reservations across healthy
backends and sends each transaction to the station that reserved its gas.

For a local setup with Redis, three Gas Stations and e2e tests, see
[docker/README.md](docker/README.md).

## Quick start

Requires Rust 1.95 or newer.

```sh
cargo run -- init --config-file config.yaml
```

Edit the backend URLs and tokens in `config.yaml`, then set the variables used by
[the example config](config.example.yaml):

```sh
export GS_AUTH='example-router-token'
export IOTA_RPC_URL='http://localhost:9000'
export REDIS_URL='redis://localhost:6379'
export GAS_STATION_B_TOKEN='example-backend-token'

cargo run -- start --config-file config.yaml
```

Use `storage.kind: memory`. Redis storage for the router is not implemented yet.
Memory storage loses reservations and execution state on restart.

Tokens and RPC/Redis URLs can reference environment variables as `${NAME}` or `$NAME`.
Missing variables fail startup, including references in an unused Redis section.
Durations accept values such as `3s`, `200ms` and `15m`. Unknown fields are rejected.

`init` creates a new file without overwriting an existing one. Run `cargo run -- init`
to print the example config instead. Stop the router with `Ctrl+C` or `SIGTERM`.

## HTTP API

The default RPC port is `9257`; metrics use `9184`.

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/health` | Router health: `{"status":"ok"}` |
| POST | `/v1/reserve_gas` | Reserve gas coins |
| POST | `/v1/execute_tx` | Execute a transaction |
| POST | `/v1/execute_transaction` | Alias of `/v1/execute_tx` |
| GET | `/metrics` | Prometheus metrics, on the metrics port |

When `rpc.token` is set, both POST endpoints require `Authorization: Bearer <token>`.
The router uses each backend's own token when forwarding requests.

## Reservations

Send a gas budget and reservation duration to `/v1/reserve_gas`:

```json
{"gas_budget": 1000000, "reserve_duration_secs": 60}
```

The router selects an eligible backend using round robin and stores its reservation
ID. The response keeps the sponsor and gas coins, but replaces `reservation_id`
with the router's ID. Use that ID for execution. Mappings expire after
`routing.ttl.reservation`.

Retries use `routing.reserve_gas.retries`. With `max_retries: 2`, the router makes
at most three attempts, waiting `backoff` before each retry and choosing a different
eligible backend.

| Failure | Retry? |
| --- | --- |
| Connection failure or connect timeout before delivery | Yes |
| Explicit rejection known not to have created a reservation | Yes |
| Response timeout, malformed reply or generic 5xx | No: a reservation may already exist |
| Invalid request, authorization error or redirect | No |

Known safe rejections include insufficient gas coins, maintenance and the daily
cap, even when returned as 5xx. The router recognizes exact Gas Station errors;
an arbitrary 5xx never makes a retry safe. A mapping storage failure also stops
without creating another reservation.

## Backend health

Enabled backends start as healthy. The background checker probes each station's
`GET /` immediately, then every `5s`, with a `1500ms` timeout. Probes run in parallel.
Only HTTP 200 counts as success.

Three consecutive failures mark a backend `Unhealthy`; two consecutive successes
restore it. Configure these values under `health_checks`. Unhealthy stations keep
receiving probes.

| State | New reservations |
| --- | --- |
| `Healthy` | Allowed |
| `Degraded` | Paused during cooldown |
| `Unhealthy` | Excluded until recovery |

Passive checks also observe reservation results:

| Result | Action |
| --- | --- |
| Success | Reset the reservation failure count |
| Connection failure, timeout or generic 500/502/503/504 | Increment the count; mark `Unhealthy` at `passive.failure_threshold` |
| HTTP 400 or other ordinary request rejection | Leave health unchanged |
| HTTP 401/403 | Log an authorization alert at `ERROR` and increment its metric |
| Rate limiting, maintenance or daily cap | Leave health unchanged |
| Insufficient gas coins | Mark `Degraded` and start cooldown |

Cooldown starts at `passive.initial_cooldown`, grows by the `passive.backoff`
multiplier and stops at `passive.max_cooldown`. Defaults give `15s`, `30s`, `60s`,
then `60s` again. A successful reservation after cooldown resets the backoff.
Successful probes cannot shorten cooldown; a backend still marked `Unhealthy`
remains excluded after cooldown expires.

Set `health_checks.enabled: false` to stop background probes, or
`health_checks.passive.enabled: false` to stop passive state updates. With background
checks disabled, an `Unhealthy` backend needs an explicit reset or a router restart.
A backend with `enabled: false` gets neither probes nor new reservations.
`deep_check` is reserved for future work.

Health changes affect new reservations. Existing reservations still execute on
their original backend.

## Execution

Send the router reservation ID, base64 transaction bytes and user signature to
`/v1/execute_tx`:

```json
{
  "reservation_id": 123,
  "tx_bytes": "<base64 BCS Transaction>",
  "user_sig": "<base64 UserSignature>",
  "request_type": "waitForLocalExecution"
}
```

`request_type` defaults to `waitForEffectsCert`; `requestType` is also accepted.
Invalid input returns `400`. A different transaction for an existing execution
returns `409`; a missing reservation returns `404`.

Concurrent requests for the same execution wait for one result. Disconnecting the
client does not cancel the router's execution task.

### Execution flow

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

The router records `Dispatching` before sending to the original Gas Station. If
that write fails, it does not send the request. Once dispatch may have happened,
an uncertain result is recovered through RPC without submitting again.

### Recovery flow [B]

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

Recovery uses a storage lease so only one worker performs it at a time. It looks up
`iota_getTransactionBlock` and caches matching, validated effects, including finalized
transaction failures. Backend metadata stays with the execution record so recovery
can continue after the reservation mapping expires.

`routing.execute_tx.timeout` covers the whole request, including waiting and
recovery. On `504`, retry with the same reservation ID and transaction bytes.

### Request-type handling [C]

| Cache | Request type | Behavior |
| --- | --- | --- |
| Effects + local confirmation `false` | `waitForEffectsCert` | Return cache |
| Effects + local confirmation `true` | `waitForEffectsCert` | Return cache |
| Effects + local confirmation `true` | `waitForLocalExecution` | Return cache |
| Effects + local confirmation `false` | `waitForLocalExecution` | Confirm through RPC, upgrade cache, return effects |

`iota.rpc_url` must point to the fullnode used by the Gas Stations. Local execution
requires RPC confirmation: `confirmedLocalExecution: true`, or matching effects
and `iota_isTransactionIndexedOnNode: true`. Finding effects alone is insufficient.

Responses contain `effects`, `error` and `confirmedLocalExecution`. If local
confirmation times out, effects remain cached for later requests. Upgrading local
confirmation does not extend the cache TTL.

Set the execution lease longer than the request timeout. Record TTL must exceed
the lease and be at least as long as reservation TTL. Effects TTL must be positive.
The example config provides these settings.

## Logging

Logs go to stderr. The default format is text and the default level is `info`.
Use JSON logs with:

```sh
LOG_FORMAT=json RUST_LOG=info cargo run -- start --config-file config.yaml
```

Use `RUST_LOG=gas_station_router_v2=debug` for more detail. The same variables work
with `docker/start-router.sh`.

Failed probes, reservations, execution and RPC calls emit `WARN`, including the
reason, backend and request context where available. Health transitions include
the previous and new state. Recovery to `Healthy` uses `INFO`; backend authorization
alerts use `ERROR`. Tokens, signatures, transaction bytes and raw upstream responses
are omitted from these logs.

## Metrics

Scrape `http://localhost:9184/metrics`. All names below start with
`gas_station_router_`.

| Metric | Labels | Counts |
| --- | --- | --- |
| `reserve_gas_requests_total` | — | Incoming reservation requests |
| `reserve_gas_successes_total` | — | Successful reservation responses |
| `reserve_gas_errors_total` | — | Failed reservation responses, including business errors |
| `reserve_gas_attempt_failures_total` | `backend`, `reason` | Failed upstream attempts |
| `reserve_gas_retries_total` | `backend`, `reason` | Started retries, labeled by the previous failure |
| `execute_tx_successes_total` | — | Successful execution results, including cache reads |
| `execute_tx_errors_total` | `reason` | Failed execution results, including failed effects with HTTP 200 |
| `execute_tx_recovery_retries_total` | `backend`, `stage` | Repeated RPC lookup rounds: `recovery` or `local_execution` |
| `backend_health_transitions_total` | `backend`, `previous_health`, `health` | Health state changes |
| `backend_health_check_failures_total` | `backend`, `reason` | Failed active probes, even before the unhealthy threshold |
| `backend_authorization_errors_total` | `backend`, `status` | Backend HTTP 401/403 responses |
| `backend_selector_calls_total` | `algorithm`, `backend_name` | Selections; empty backend means no candidate |
| `storage_calls_total` | `method` | Storage calls |
| `storage_errors_total` | `kind` | Storage errors |
| `storage_health_checks_total` | `kind` | Storage health-check results |
| `storage_execution_phase_transitions_total` | `kind` | Entries into each execution phase |
| `storage_execution_claims_total` | `kind` | Claim outcomes |
| `storage_cached_transactions_total` | `backend` | New cache inserts |

`backend_registry_candidates` is a gauge of the latest eligible candidate count;
`up` is set to 1 while the router's HTTP servers are running.

Successes and errors count once per request result. A successful retry or recovery
counts as one success. Each cache read counts as another result, so these counters
do not represent unique on-chain transactions. Failed finalized effects count as
errors even with HTTP 200.

Retry counters increase only when another attempt starts. Scraping metrics does
not run checks or change state. Labels use fixed categories and configured backend
names.

## Development

```sh
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
```
