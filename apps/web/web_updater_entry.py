from __future__ import annotations

from apps.web import web_updater
from apps.web.issue187_runtime_guard import patch_web_updater
from apps.web.issue222_update_workspace import install_issue222_update_workspace
from apps.web.issue226_update_safety import install_issue226_web_update_safety


def main() -> int:
    # Install the app-local update engine first, then the issue-226 failure
    # recovery wrapper. The issue187 handoff guard wraps the resulting handlers
    # so the update page is visible before processes stop.
    install_issue222_update_workspace(web_updater)
    install_issue226_web_update_safety(web_updater)
    patch_web_updater(web_updater)
    return int(web_updater.main())


if __name__ == "__main__":
    raise SystemExit(main())
