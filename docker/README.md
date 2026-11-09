# E2E tests

Requires Linux, Docker Compose and Rust 1.95. Run commands from the repository root.

```sh
./docker/e2e.sh              # router + supervisor, no keys or Testnet needed
./docker/e2e.sh router       # two routers, Nginx, Redis and three mock stations
./docker/e2e.sh supervisor   # supervisor managing a mock Gas Station process
```

The runner builds the binaries, allocates free ports and prints the setup: station
addresses and public sponsors, router URLs, metrics, Nginx and test auth. It saves
logs and JSON reports in `docker/.runtime/`. Each test owns its processes and Docker
project; cleanup removes only that project's resources.

| Test | Checks |
| --- | --- |
| Router | Both routers serve reservations and execution through Nginx; Redis shares the execution cache; each transaction reaches a station once |
| Failures | A stopped station is excluded and readmitted; Redis failure returns errors; cached results survive its restart |
| TTL | Reservation, execution and effects keys all expire naturally; reads do not renew TTL; no evictions; only the persistent ID counter needs removal |
| Supervisor | Readiness and crash restarts, refresh before rebuild, manual approval, activity/network guards, lock failures, configured escalation, inherited flag isolation and child shutdown |

Mock tests never use real sponsor keys or contact IOTA. The mock implements the
Gas Station CLI flags and a small HTTP API. `control.json` controls its behavior;
`events.jsonl` records launches, flags, refreshes and executions. These files belong
to the test fixture, not the real Gas Station API. The router scenario takes about
a minute, including a 40-second TTL drain. The supervisor scenario takes seconds.

## Testnet

Keep a Gas Station checkout next to this repository, then:

```sh
./docker/e2e.sh init          # once; creates docker/.env and prints four sponsors
./docker/e2e.sh sponsors     # print the public funding addresses again
# Fund the first three sponsors with Testnet IOTA.
./docker/e2e.sh testnet       # full transaction, failure and TTL scenario
```

Use the [Foundation Testnet faucet](https://faucet.testnet.iota.cafe/), for example
10 IOTA per sponsor. Private keys stay in the ignored `docker/.env`. Foundation
Testnet JSON-RPC and gRPC endpoints are the defaults. `GAS_STATION_SOURCE` can
override the checkout path; use an absolute path in `.env`.

This scenario runs real signed transactions for roughly five minutes, plus builds,
startup, recovery and TTL expiry. It stops and restores one station and Redis,
checks both routers and verifies transaction effects on Testnet. All three router
data types must expire without deletion: reservations after 20s, execution records
after 40s, and effects after 30s. The final report lists keys seen, expired and
remaining, sampled TTL ranges, renewals, evictions and cleanup results.

The full test uses three stations. The manual Compose setup retains a fourth
station, sharing station 1's Redis registry for failover experiments.

## Manual runs

```sh
./docker/router/services.sh       # Redis, four real stations and Nginx
./docker/start-router.sh          # build once and start two routers
```

In another terminal:

```sh
./docker/e2e.sh reserve
./docker/e2e.sh transactions --count 9
./docker/router/services.sh down  # preserves the local Redis volume
```

Nginx listens on `9257`, routers on `9261`/`9262`, and metrics on `9185`/`9186`.
`ROUTER_URL` overrides the incremental tests' default `http://127.0.0.1:9257`.
`Ctrl+C` stops both router processes.

For a supervisor demo using the mock:

```sh
./docker/supervisor/start.sh
```

The station listens on `19527`, metrics on `19184`. Status is written to
`docker/.runtime/supervisor-demo/status.json`. See
[supervisor recovery settings](../docs/supervisor.md) for real stations.

## Layout and CI

`router/` contains the real Testnet Compose stack, router config and Nginx template.
`supervisor/` contains the mock demo. `mock/` contains the isolated Redis/Nginx
Compose stack. Shared Bash helpers and checks live in `scripts/`.

GitHub Actions runs formatting, ShellCheck, YAML checks, Clippy, Rust tests, both
mock E2E suites and the real Testnet scenario. The Testnet job uses
`GAS_STATION_1_PRIVATE_KEY` through `GAS_STATION_4_PRIVATE_KEY` and `GS_AUTH` from
repository secrets. Keys stay in the job environment; the sponsors must be funded.
Testnet runs are serialized because they share on-chain coins. Fork PRs run the
mock suites; the secrets-dependent job runs only for this repository's branches.
