#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

INSTALL_DEPS=0
EXPECTED_ARCH=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --install-deps)
      INSTALL_DEPS=1
      shift
      ;;
    --arch)
      EXPECTED_ARCH="${2:-}"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

ACTUAL_ARCH="$(uname -m)"
case "$ACTUAL_ARCH" in
  arm64|aarch64) NORMALIZED_ARCH="arm64" ;;
  x86_64|amd64) NORMALIZED_ARCH="x86_64" ;;
  *) NORMALIZED_ARCH="$ACTUAL_ARCH" ;;
esac

if [[ -n "$EXPECTED_ARCH" && "$NORMALIZED_ARCH" != "$EXPECTED_ARCH" ]]; then
  echo "macOS build architecture mismatch: expected=$EXPECTED_ARCH actual=$NORMALIZED_ARCH" >&2
  exit 2
fi

for shadow in "http.py" "http/__init__.py"; do
  if [[ -f "$shadow" ]]; then
    echo "ERROR: Found shadowing file: $shadow" >&2
    exit 1
  fi
done

if [[ "$INSTALL_DEPS" -eq 1 ]]; then
  python3 -m pip install --upgrade pip
  python3 -m pip install -r requirements.txt
  python3 -m pip uninstall -y http 2>/dev/null || true
  python3 -m pip install pyinstaller
fi

rm -rf build dist
python3 -m PyInstaller --noconfirm --clean apps/windows/bilipdj_onedir_mac.spec
python3 -m PyInstaller --noconfirm --clean apps/windows/paiduijitm_mac.spec

[[ -d "dist/bilipdj/core/cd" ]] && rm -rf "dist/bilipdj/core/cd"
cp "dist/paiduijitm" "dist/bilipdj/paiduijitm"

for required in "dist/bilipdj/main" "dist/bilipdj/paiduijitm"; do
  if [[ ! -f "$required" ]]; then
    echo "ERROR: Build output missing: $required" >&2
    exit 1
  fi
done

echo "macOS build architecture: $NORMALIZED_ARCH"
echo "Main panel executable : dist/bilipdj/main"
echo "Overlay executable    : dist/bilipdj/paiduijitm"
