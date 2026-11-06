#!/bin/sh
set -eu
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
if [ "$#" -eq 0 ]; then set -- reserve; fi
exec cargo run --locked --example e2e -- "$@"
