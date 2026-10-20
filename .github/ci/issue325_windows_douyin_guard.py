"""Issue #325: regression for Windows --backend and local Douyin login."""
from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


def check_entry() -> None:
    entry = (ROOT / "apps/windows/main.py").read_text(encoding="utf-8")
    assert entry.index('if __name__ == "__main__" and "--backend" in sys.argv[1:]:') < entry.index("from apps.windows.dpi_awareness import")
    assert "backend_main([])" in entry
    gui = (ROOT / "apps/windows/control_panel.py").read_text(encoding="utf-8")
    for needle in ("BILIPDJ_UNATTENDED_BACKEND", "DANMUJI_BACKEND_PORT",
                   "DANMUJI_BACKEND_HOST", "ProxyHandler({})",
                   "抖音扫码启动失败", "后端未启动：请先点击"):
        assert needle in gui, needle
    workflow = (ROOT / ".github/workflows/package-windows-x64.yml").read_text(encoding="utf-8")
    assert "issue325_frozen_backend_smoke.py" in workflow


def check_safe_migration() -> None:
    # Direct file-loader avoids initializing the live backend package.
    p = ROOT / "apps/server/user_data_paths.py"
    spec = importlib.util.spec_from_file_location("issue325_user_data_paths", p)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    fake_conflicts = [(Path("project-example.yaml"), Path("private-example.yaml"))]
    with patch.dict(os.environ, {"BILIPDJ_UNATTENDED_BACKEND": "1"}, clear=False):
        os.environ.pop("BILIPDJ_MIGRATION_CHOICE", None)
        try:
            module.ask_preference(fake_conflicts)
            raise AssertionError("Unattended backend must never open modal conflict dialog")
        except module.DataConflictError as exc:
            assert "冲突" in str(exc)
        with patch.dict(os.environ, {"BILIPDJ_MIGRATION_CHOICE": "user"}):
            assert module.ask_preference(fake_conflicts) == "user"
        with patch.dict(os.environ, {"BILIPDJ_MIGRATION_CHOICE": "project"}):
            assert module.ask_preference(fake_conflicts) == "project"


def check_douyin_browser_discovery() -> None:
    # Includes per-user Chrome, not just Program Files installations.
    source = (ROOT / "apps/server/douyin_login.py").read_text(encoding="utf-8")
    assert 'os.environ.get("LOCALAPPDATA", "")' in source
    assert "Storage.getCookies" in source and "Network.getAllCookies" in source
    assert "_allowed_cookie" in source


def main() -> None:
    check_entry()
    check_safe_migration()
    check_douyin_browser_discovery()
    print("Issue #325 backend, safe migration and QR login guard: OK")


if __name__ == "__main__":
    main()
