from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core import server as backend  # noqa: E402

DEFAULT_WEB_DIR = REPO_ROOT / "apps" / "web" / "static"


def configure_web_assets(web_dir: str | os.PathLike[str] | None = None) -> Path:
    """Point the legacy-compatible backend at the canonical Web app assets."""
    target = Path(web_dir).expanduser().resolve() if web_dir else DEFAULT_WEB_DIR.resolve()
    if not target.exists():
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
