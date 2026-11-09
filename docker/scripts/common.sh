#!/usr/bin/env bash

REPO_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

build_mock_tests() {
  cargo build --locked --bin gas-station-router-v2 --example mock-gas-station --example mock-e2e
}

example_binary() {
  printf '%s/debug/examples/%s\n' "${CARGO_TARGET_DIR:-$REPO_ROOT/target}" "$1"
}

require_testnet_env() {
  if [[ ! -f "$REPO_ROOT/docker/.env" ]]; then
    echo 'Run ./docker/e2e.sh init, then fund the printed sponsors.' >&2
    exit 1
  fi
}
