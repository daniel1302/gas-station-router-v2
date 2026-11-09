#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=docker/scripts/common.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/../scripts/common.sh"
cd "$REPO_ROOT"
cargo build --locked --bin gas-station-router-v2 --example mock-gas-station
exec "${CARGO_TARGET_DIR:-target}/debug/gas-station-router-v2" supervisor \
  --config-file docker/supervisor/config.yaml \
  --gas-station-binary "$(example_binary mock-gas-station)" "$@"
