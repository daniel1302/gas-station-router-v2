#!/usr/bin/env bash
set -euo pipefail

# shellcheck source=docker/scripts/common.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/common.sh"
cd "$REPO_ROOT"

check_shell() {
  mapfile -t scripts < <(git ls-files --cached --others --exclude-standard 'docker/*.sh' 'docker/**/*.sh')
  shellcheck -x "${scripts[@]}"
  shfmt -d -i 2 -ci "${scripts[@]}"
}

check_yaml() {
  mapfile -t configs < <(git ls-files --cached --others --exclude-standard '*.yaml' '*.yml')
  yamllint -d '{extends: default, rules: {document-start: disable, line-length: disable, truthy: {check-keys: false}}}' "${configs[@]}"
}

cargo fmt --all --check
check_shell
check_yaml
