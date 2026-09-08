from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.server import configure_runtime_paths, server as backend  # noqa: E402

SOURCE_WEB_DIR = REPO_ROOT / "apps" / "web" / "static"
BUNDLED_WEB_DIR = Path(getattr(sys, "_MEIPASS", REPO_ROOT)) / "apps" / "web" / "static"


def _resolve_default_web_dir() -> Path:
    for candidate in (SOURCE_WEB_DIR, BUNDLED_WEB_DIR, backend.UI_DIR, backend.BUNDLE_UI_DIR):
        try:
            path = Path(candidate).resolve()
        except Exception:
            continue
        if path.is_dir():
            return path
    return SOURCE_WEB_DIR.resolve()


def configure_web_assets(web_dir: str | os.PathLike[str] | None = None) -> Path:
    """Point the backend at the canonical Web assets for source or packaged runs."""
    configure_runtime_paths(backend)
    target = Path(web_dir).expanduser().resolve() if web_dir else _resolve_default_web_dir()
    if not target.is_dir():
        raise FileNotFoundError(f"Web assets directory does not exist: {target}")
    backend.UI_DIR = target
    backend.BUNDLE_UI_DIR = target
    backend.LIVE_STYLE_CSS_PATH = target / "moren.css"
    return target


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="BiliPDJ standalone backend server")
    parser.add_argument("--host", default=None, help="listen host; defaults to config.yaml")
    parser.add_argument("--port", type=int, default=None, help="listen port; defaults to config.yaml")
    parser.add_argument(
        "--web-dir",
        default=os.getenv("BILIPDJ_WEB_DIR", ""),
        help="Web static directory; defaults to apps/web/static",
    )
    args = parser.parse_args(argv)

    web_dir = configure_web_assets(args.web_dir or None)
    config = backend.load_config()
    server_config = backend.normalize_server_config(config.get("server", {}))
    host = args.host or os.getenv("DANMUJI_BACKEND_HOST") or str(server_config["host"])
    port = args.port or int(os.getenv("DANMUJI_BACKEND_PORT", int(server_config["port"])))

    print(f"[bilipdj-server] web={web_dir}")
    backend.run_server(host=host, port=port)


if __name__ == "__main__":
    main()
