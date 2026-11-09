#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=docker/scripts/common.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/../scripts/common.sh"
cd "$REPO_ROOT"
require_testnet_env
if [[ "$#" -eq 0 ]]; then set -- up -d --build; fi
exec docker compose --env-file docker/.env -f docker/router/docker-compose.yml "$@"
