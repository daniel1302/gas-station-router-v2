# Simple E2E setup

Requirements: Docker Compose, Rust 1.95 and a `gas-station` checkout next to this
repository. The first build takes a few minutes.

## 1. Generate keys and fund sponsors

From the repository root:

```sh
cd docker
./e2e.sh init       # only if .env does not exist yet
./e2e.sh sponsors   # show the addresses to fund
```

Fund **all 3 addresses**, for example with 10 Testnet IOTA each, using
<https://faucet.testnet.iota.cafe/>. Transaction senders do not need funding.
Keys are stored in `.env` (permissions `600`, ignored by Git).

## 2. Start the stations and router

Still in `docker/`:

```sh
docker compose up -d --build
./start-router.sh
```

Keep this terminal open. The stations use ports `9527–9529`; the router uses `9257`.
After funding, wait for coin initialization: `docker compose logs -f`.

## 3. Run the test in a second terminal

From `docker/`:

```sh
./e2e.sh reserve
```

`PASS` means 9 reservations, valid coins, unique IDs and at least 3 sponsors.
Budgets are random and reservations expire after 5 seconds. Failures return a nonzero exit code.

Optional:

```sh
ROUTER_URL=http://127.0.0.1:9257 ./e2e.sh reserve --count 20
./e2e.sh reserve --count 3 --min-sponsors 1   # test a single backend
./e2e.sh transactions --count 9             # submit and verify full transactions
```

`transactions` submits signed transactions with random senders and budgets through
`/v1/execute_transaction` (an alias of `/v1/execute_tx`). Each transaction reads the
IOTA clock 1–5 times, and its result is verified independently on the fullnode.
If execution times out, retry the same reservation and transaction to recover its
result. The router never resubmits an execution whose outcome is unknown.

Default `ROUTER_URL`: `http://127.0.0.1:9257`. `.env` provides `GS_AUTH`,
`IOTA_RPC_URL=https://api.testnet.iota.cafe` and
`IOTA_GRPC_URL=https://grpc.testnet.iota.cafe` for Foundation Testnet.
Shell environment variables take precedence. Set `GAS_STATION_SOURCE` in `.env`
to use a different station source path (default: `../../gas-station`, relative to `docker/`).

Redis stores station data in separate databases `1–3`; the router uses RAM
and loses reservation mappings on restart. Do not change keys while reusing existing Redis data.
To stop: press `Ctrl+C` for the router and run `docker compose down` for the stations (preserves data).
