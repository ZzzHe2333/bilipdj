#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCH="$(uname -m)"
[[ "$ARCH" == "arm64" || "$ARCH" == "aarch64" ]] || { echo "arm64 build requires an arm64 runner; detected: $ARCH" >&2; exit 2; }
exec "$ROOT/package-macos-local.sh" "$@"
