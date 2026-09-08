from __future__ import annotations

import shutil
from pathlib import Path

APP_DIR = Path(__file__).resolve().parent
SOURCE_DIR = APP_DIR / "static"
DIST_DIR = APP_DIR / "dist"


def build() -> Path:
    if not SOURCE_DIR.is_dir():
        raise FileNotFoundError(f"Web source directory not found: {SOURCE_DIR}")
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    shutil.copytree(SOURCE_DIR, DIST_DIR)
    required = (
        "index.html",
        "config.html",
        "myjs.js",
        "moren.css",
        "control.html",
        "control.js",
        "support_us.js",
        "WxZSM.png",
    )
    missing = [name for name in required if not (DIST_DIR / name).is_file()]
    if missing:
        raise RuntimeError(f"Web build is incomplete; missing: {', '.join(missing)}")
    print(f"Web build complete: {DIST_DIR}")
    return DIST_DIR


if __name__ == "__main__":
    build()
