from __future__ import annotations

from apps.web import web_updater
from apps.web.issue187_runtime_guard import patch_web_updater


def main() -> int:
    patch_web_updater(web_updater)
    return int(web_updater.main())


if __name__ == "__main__":
    raise SystemExit(main())
