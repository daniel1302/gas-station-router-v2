#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=docker/scripts/common.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/scripts/common.sh"
cd "$REPO_ROOT"

run_mock_tests() {
  build_mock_tests
  exec "$(example_binary mock-e2e)" "$@"
}

run_testnet_tests() {
  exec cargo run --locked --example e2e -- "$@"
}

case "${1:-all}" in
  all | router | supervisor)
    run_mock_tests "${1:-all}"
    ;;
  testnet)
    shift
    if [[ "$#" -eq 0 ]]; then set -- full-e2e; fi
    run_testnet_tests "$@"
    ;;
  init | sponsors | reserve | transactions | routers | full-e2e | full_e2e | ful_e2e)
    run_testnet_tests "$@"
    ;;
  *)
    echo 'Usage: ./docker/e2e.sh [all|router|supervisor|testnet|init|sponsors|reserve|transactions]' >&2
    exit 2
    ;;
esac
