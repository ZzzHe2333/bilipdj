#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ARCH="$(uname -m)"
[[ "$ARCH" == "x86_64" || "$ARCH" == "amd64" ]] || { echo "amd64 build requires an amd64 runner; detected: $ARCH" >&2; exit 2; }
exec "$ROOT/package-macos-local.sh" "$@"
