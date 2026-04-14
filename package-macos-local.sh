#!/usr/bin/env bash
# package-macos-local.sh — 本地 macOS 打包脚本
# 用法：./package-macos-local.sh [--install-deps]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

INSTALL_DEPS=0
for arg in "$@"; do
    [[ "$arg" == "--install-deps" ]] && INSTALL_DEPS=1
done

# 检查是否存在会遮蔽标准库的 http 模块
for shadow in "http.py" "http/__init__.py"; do
    if [[ -f "$shadow" ]]; then
        echo "ERROR: Found shadowing file: $shadow" >&2
        exit 1
    fi
done

if [[ "$INSTALL_DEPS" -eq 1 ]]; then
    python3 -m pip install --upgrade pip
    if [[ -f requirements.txt ]]; then
        python3 -m pip install -r requirements.txt
    fi
    python3 -m pip uninstall -y http 2>/dev/null || true
    python3 -m pip install pyinstaller 'qrcode[pil]' brotli psutil
fi

# 清理旧产物
rm -rf build dist

python3 -m PyInstaller --noconfirm --clean bilipdj_onedir_mac.spec
python3 -m PyInstaller --noconfirm --clean paiduijitm_mac.spec

# 清理 PyInstaller 有时会生成的 cd 目录
[[ -d "dist/bilipdj/core/cd" ]] && rm -rf "dist/bilipdj/core/cd"

# 把 overlay 可执行文件合并到主目录
cp "dist/paiduijitm" "dist/bilipdj/paiduijitm"

if [[ ! -f "dist/bilipdj/main" ]]; then
    echo "ERROR: Build output missing: dist/bilipdj/main" >&2
    exit 1
fi

if [[ ! -f "dist/bilipdj/paiduijitm" ]]; then
    echo "ERROR: Build output missing: dist/bilipdj/paiduijitm" >&2
    exit 1
fi

echo "Main panel executable : dist/bilipdj/main"
echo "Overlay executable    : dist/bilipdj/paiduijitm"
