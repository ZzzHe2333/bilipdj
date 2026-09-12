from __future__ import annotations

import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_sources() -> None:
    manifest = read("apps/windows/update_manifest.py")
    desktop_spec = read("apps/windows/bilipdj_onedir.spec")
    updater_spec = read("apps/windows/updater.spec")
    windows_workspace = read("apps/windows/issue222_update_workspace.py")
    windows_apply = read("apps/windows/issue222_update_apply.py")
    web_guard = read("apps/server/issue187_web_update_guard.py")
    web_entry = read("apps/web/web_updater_entry.py")
    web_workspace = read("apps/web/issue222_update_workspace.py")
    core_init = read("core/__init__.py")

    assert '"update/"' in manifest
    assert "issue222_runtime_hook.py" in desktop_spec
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


def check_allocator() -> None:
    from core.update_workspace import allocate_update_session, cleanup_update_session, validate_update_session

    with tempfile.TemporaryDirectory(prefix="bilipdj-issue222-") as raw:
        app = Path(raw) / "bilipdj"
        app.mkdir()
        session = allocate_update_session(app, "full")
        assert session.parent == app.resolve() / "update"
        assert validate_update_session(app, session) == session.resolve()
        marker = session / "payload.bin"
        marker.write_bytes(b"ok")
        assert marker.is_file()
        assert cleanup_update_session(app, session)
        assert not session.exists()
        try:
            validate_update_session(app, Path(raw) / "outside")
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
