#!/bin/sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
if [ ! -f .env ]; then
  echo 'Run ./e2e.sh init first.' >&2
  exit 1
fi
set -a
. ./.env
set +a
export ROUTER_REDIS_URL="${ROUTER_REDIS_URL:-redis://127.0.0.1:${ROUTER_REDIS_PORT:-6380}/0}"
cd ..
mkdir -p docker/.runtime
exec 9>docker/.runtime/cluster.lock
flock -n 9 || { echo 'The router cluster is already running.' >&2; exit 1; }
cargo build --locked --bin gas-station-router-v2 --example e2e
docker compose -f docker/docker-compose.yml up -d nginx
exec "${CARGO_TARGET_DIR:-target}/debug/examples/e2e" routers
