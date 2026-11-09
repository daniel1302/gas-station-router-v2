#!/usr/bin/env bash
set -euo pipefail

exec "$(dirname -- "${BASH_SOURCE[0]}")/e2e.sh" testnet full-e2e "$@"
