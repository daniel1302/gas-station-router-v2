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
cd ..
exec cargo run --locked --bin gas-station-router-v2 -- start --config-file docker/router.config.yaml
