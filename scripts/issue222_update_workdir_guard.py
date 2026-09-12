from __future__ import annotations

import importlib.util
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_sources() -> None:
    manifest = read("apps/windows/update_manifest.py")
    desktop_spec = read("apps/windows/bilipdj_onedir.spec")
    desktop_main = read("apps/windows/main.py")
    updater_spec = read("apps/windows/updater.spec")
    windows_workspace = read("apps/windows/issue222_update_workspace.py")
    windows_apply = read("apps/windows/issue222_update_apply.py")
    web_guard = read("apps/server/issue187_web_update_guard.py")
    web_entry = read("apps/web/web_updater_entry.py")
    web_workspace = read("apps/web/issue222_update_workspace.py")
    core_init = read("core/__init__.py")

    assert '"update/"' in manifest
    assert "runtime_hooks=[]" in desktop_spec
    assert "issue222_runtime_hook.py" not in desktop_spec
    assert "install_windows_update_workspace" in desktop_main
    assert desktop_main.index("install_windows_update_workspace()") < desktop_main.index("patch_update_ui(update_ui)")
    assert "updater_issue222_entry.py" in updater_spec
    assert "allocate_update_session" in windows_workspace
    assert "windows-full" in windows_workspace and "windows-incremental" in windows_workspace
    assert "windows-restore" in windows_workspace
    assert "validate_update_session" in windows_apply
    assert 'in {"backup", "update"}' in windows_apply
    assert "allocate_update_session(app_dir" in web_guard
    assert "tempfile.mkdtemp" not in web_guard
    assert web_entry.index("install_issue222_update_workspace") < web_entry.index("patch_web_updater(web_updater)")
    assert 'module.PRESERVED_PREFIXES = tuple(module.PRESERVED_PREFIXES) + ("update/",)' in web_workspace
    assert "validate_update_session" in web_workspace
    assert 'not in {"backup", "update"}' in web_workspace
    assert "schedule_local_update_cleanup" in core_init


def _load_workspace_module():
    path = ROOT / "core" / "update_workspace.py"
    spec = importlib.util.spec_from_file_location("bilipdj_issue222_update_workspace_helper", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_allocator() -> None:
    workspace = _load_workspace_module()
    with tempfile.TemporaryDirectory(prefix="bilipdj-issue222-") as raw:
        app = Path(raw) / "bilipdj"
        app.mkdir()
        session = workspace.allocate_update_session(app, "full")
        assert session.parent == app.resolve() / "update"
        assert workspace.validate_update_session(app, session) == session.resolve()
        marker = session / "payload.bin"
        marker.write_bytes(b"ok")
        assert marker.is_file()
        assert workspace.cleanup_update_session(app, session)
        assert not session.exists()
        try:
            workspace.validate_update_session(app, Path(raw) / "outside")
        except ValueError:
            pass
        else:
            raise AssertionError("outside cleanup target must be rejected")


def main() -> None:
    check_sources()
    check_allocator()
    print("issue222 update workdir guard: OK")


if __name__ == "__main__":
    main()
