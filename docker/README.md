# Local E2E tests

Requires Linux, Docker Compose, Rust 1.95, and a `gas-station` checkout next to this
repository. Nginx uses host networking to reach the two router binaries.

## Fund the sponsors once

```sh
cd docker
./e2e.sh init       # only when .env does not exist
./e2e.sh sponsors
```

Fund the three printed addresses with Testnet IOTA, for example 10 IOTA each from
<https://faucet.testnet.iota.cafe/>. Private keys stay in `.env` (ignored by Git).

## Run the full test

```sh
./full_e2e.sh
```

This builds the router, starts Redis and three Gas Stations, and launches two
router processes behind a separate Nginx container on free local ports. Builds
and startup happen before the scenario begins. Allow roughly five minutes, plus
builds, startup, service recovery, and the final TTL drain.

The startup summary prints the actual load balancer, both routers, metrics and
status URLs, and configured Gas Station URLs. It includes the full test
`Authorization: Bearer ...` headers for the routers and stations. `/status` and
`/health` on the routers are public. Full e2e uses free ports, so use the printed
addresses when inspecting a running test.

This setup is saved in `.runtime/full-*/setup.json` and the final report's `setup`
field, including test auth tokens. Sponsor private keys stay in `.env`.

The scenario sends signed transactions through Nginx, verifies them on Foundation
Testnet, and replays each request to check the shared cache. It stops and restores
one Gas Station, then Redis. Redis outages must return errors; after restart the
same transaction must still be cached without another dispatch.

Both routers must serve reservations and execution, report the health transitions,
and observe Redis failures. Every router data key must have TTL, reads must not
renew it, and no keys may be evicted. The final stage stops new traffic and waits
for all three data types to expire naturally. Its deadline follows the latest
observed expiry, with 30 seconds of grace; there is no fixed five-minute cutoff.
Only then are the owned routers stopped
and the test's ID counter removed. There is no `FLUSHDB` or data-key cleanup.

`PASS` is printed only after these checks and cleanup succeed. Logs and
`report.json` are saved under `.runtime/full-*/`. On failure or interruption, the
test restores Redis and the Gas Station and stops its own routers and proxy.
The existing local Compose data volume is preserved.

The final summary and JSON report show each key type separately:

| Router key type | Test TTL | Verified at the end |
| --- | --- | --- |
| Reservation | 20s | Every observed key expired, none remain |
| Execution record | 40s | Every observed key expired, none remain |
| Transaction effects cache | 30s | Every observed key expired, none remain |
| Reservation ID counter | Persistent | Kept during the test, removed after both routers stop |

For each data type, the summary includes keys seen, expired and remaining, TTL
checks, and the sampled TTL range. Missing, excessive or renewed TTLs fail the
test, as do keys disappearing before their expiry. A type with no observed keys
cannot pass. The report also includes per-router traffic, cache reads, Redis
restarts and evictions, and whether cleanup left the test namespace empty.
These checks cover the router's isolated namespace, not Gas Station databases.

## Start two routers for manual testing

```sh
docker compose up -d --build
./start-router.sh
```

The launcher builds once and starts two copies of the binary. Wait for `Two routers
ready`, then use another terminal:

```sh
./e2e.sh reserve
./e2e.sh transactions --count 9
```

Nginx listens on `9257` and forwards to routers on `9261` and `9262`. Each response
includes `X-Router-Upstream`. Router metrics use `9185` and `9186`; router logs are
in `.runtime/cluster/`. `Ctrl+C` stops both router processes.
The launcher also prints the topology and test auth, and saves
`.runtime/cluster/setup.json`.

`ROUTER_URL` defaults to `http://127.0.0.1:9257` for incremental tests. For example:

```sh
ROUTER_URL=http://127.0.0.1:9257 ./e2e.sh reserve --count 20
./e2e.sh reserve --count 3 --min-sponsors 1
```

Incremental tests print their selected target and auth, plus the Gas Station
addresses and auth from `router.config.yaml`. With a custom `ROUTER_URL`, that local
station configuration may differ from the remote router's setup.

Redis uses AOF with `appendfsync always` and `noeviction`. Router data uses DB `0`;
station data uses DBs `1–3`. The default host Redis port is `6380`;
`ROUTER_REDIS_PORT` changes it and `ROUTER_REDIS_URL` overrides the manual routers'
connection. Full e2e always targets the local Compose Redis and its own namespace.

`.env` selects Foundation Testnet through `IOTA_RPC_URL` and `IOTA_GRPC_URL`.
Set `GAS_STATION_SOURCE` there to change the station checkout path (default
`../../gas-station`). The two-process launcher saves JSON logs by default.

To stop the Docker services, run `docker compose down`; it preserves Redis data.
