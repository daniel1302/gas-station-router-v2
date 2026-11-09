# Gas Station Router

A router and process supervisor for IOTA Gas Stations. Requires Linux and Rust 1.95.

## Router

```sh
cargo run -- init --config-file config.yaml
# Edit config.yaml with your backend URLs, tokens and storage settings.
cargo run -- start --config-file config.yaml
```

The router distributes reservations across healthy stations and sends each
execution to its original station. Use Redis to share reservations and execution
results between router instances. `GET /status` shows backend health and cooldowns.

[Router configuration and flow diagrams](docs/router.md) ·
[Local E2E tests](docker/README.md)

## Supervisor

```sh
cargo run -- supervisor \
  --gas-station-binary /path/to/gas-station \
  --gas-station-config /path/to/gas-station.yaml
```

The supervisor starts one station, checks its HTTP endpoint and pool metrics,
restarts it after a crash, and stops it on shutdown. Full rescans require manual
approval by default and respect Gas Station locks.

For custom settings, copy [supervisor.example.yaml](supervisor.example.yaml), edit
the process paths, and run:

```sh
cargo run -- supervisor --config-file supervisor.yaml
```

[Recovery settings and approval](docs/supervisor.md)

## Development

```sh
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo fmt --all --check
```

Redis storage tests start their own Docker containers. Logs use `RUST_LOG=info`;
set `LOG_FORMAT=json` for JSON output.
