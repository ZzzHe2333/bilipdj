from __future__ import annotations

from apps.web import web_updater
from apps.web.issue187_runtime_guard import patch_web_updater
from apps.web.issue222_update_workspace import install_issue222_update_workspace


def main() -> int:
    # Install the app-local update engine first. The issue187 handoff guard then
    # wraps these final handlers so the page must be visible before processes stop.
    install_issue222_update_workspace(web_updater)
    patch_web_updater(web_updater)
    return int(web_updater.main())


if __name__ == "__main__":
    raise SystemExit(main())
