#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=docker/scripts/common.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/../scripts/common.sh"
cd "$REPO_ROOT"

lock_cluster() {
  mkdir -p docker/.runtime
  exec 9>docker/.runtime/cluster.lock
  flock -n 9 || {
    echo 'The router cluster is already running.' >&2
    exit 1
  }
}

require_testnet_env
lock_cluster
cargo build --locked --bin gas-station-router-v2 --example e2e
docker compose --env-file docker/.env -f docker/router/docker-compose.yml up -d nginx
exec "$(example_binary e2e)" routers
