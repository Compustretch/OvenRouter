#!/usr/bin/env bash
#
# Compatibility entry point for the arithmetic/ClickHouse smoke test.
# The maintained test lives at scripts/smoke-jev-ch.sh.
#
set -Eeuo pipefail
ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
exec bash "${ROOT_DIR}/scripts/smoke-jev-ch.sh" "$@"
