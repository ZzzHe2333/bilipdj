from __future__ import annotations

import tempfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_windows_transaction_boundary() -> None:
    source = read("apps/windows/issue226_update_safety.py")
    snapshot_at = source.index("snapshot = apply._create_snapshot")
    first_copy_at = source.index("apply._copy_with_retry(source, destination)")
    first_remove_at = source.index("incremental_apply._remove_target(destination)")
    assert snapshot_at < first_copy_at
    assert snapshot_at < first_remove_at
    assert "incremental-rollback" not in source
    assert "原文件保持不变" in source
    assert "apply._restore_snapshot(app_dir, snapshot)" in source
    assert 'status = "preflight_failed"' in source


def check_single_workspace_implementation() -> None:
    canonical = read("apps/update_workspace.py")
    compat = read("core/update_workspace.py")
    cleanup = read("core/issue222_update_cleanup.py")
    assert "def allocate_update_session" in canonical
    assert "def allocate_update_session" not in compat
    assert "from apps.update_workspace import" in compat
    assert "from apps.update_workspace import cleanup_update_session, validate_update_session" in cleanup


def check_web_relaunch_wrapper() -> None:
    from apps.web.issue226_update_safety import install_issue226_web_update_safety

    with tempfile.TemporaryDirectory(prefix="bilipdj-issue226-") as raw:
        app_dir = Path(raw)
        (app_dir / "main.exe").write_bytes(b"placeholder")
        calls: list[str] = []

        def stop_app(_request, _state):
            calls.append("stop")

        def launch_main(_app_dir, _main_exe):
            calls.append("launch")
            return object()

        def failing_incremental(request, state, work):
            module._stop_app(request, state)
            raise OSError("snapshot preparation failed")

        module = SimpleNamespace(
            _incremental_update=failing_incremental,
            _stop_app=stop_app,
            _launch_main=launch_main,
        )
        assert install_issue226_web_update_safety(module)
        try:
            module._incremental_update(
                {"app_dir": str(app_dir), "main_exe": "main.exe"},
                object(),
                app_dir / "update" / "probe",
            )
        except OSError:
            pass
        else:
            raise AssertionError("expected injected Web update failure")
        assert calls == ["stop", "launch"], calls

        calls.clear()

        def handled_incremental(request, state, work):
            module._stop_app(request, state)
            module._launch_main(Path(request["app_dir"]), request["main_exe"])
            raise OSError("failure after internal relaunch")

        # Reinstall around a fresh fake module to verify the outer safety layer
        # does not launch a second instance when the inner rollback already did.
        module2 = SimpleNamespace(
            _incremental_update=handled_incremental,
            _stop_app=stop_app,
            _launch_main=launch_main,
        )
        assert install_issue226_web_update_safety(module2)
        try:
            module2._incremental_update(
                {"app_dir": str(app_dir), "main_exe": "main.exe"},
                object(),
                app_dir / "update" / "probe2",
            )
        except OSError:
            pass
        else:
            raise AssertionError("expected injected handled Web update failure")
        assert calls == ["stop", "launch"], calls


def check_entry_wiring() -> None:
    windows_entry = read("apps/windows/updater_issue222_entry.py")
    web_entry = read("apps/web/web_updater_entry.py")
    assert "patch_issue226_windows_update_safety" in windows_entry
    assert windows_entry.index("patch_updater_v2") < windows_entry.index("patch_issue226_windows_update_safety()")
    assert "install_issue226_web_update_safety" in web_entry
    assert web_entry.index("install_issue222_update_workspace(web_updater)") < web_entry.index("install_issue226_web_update_safety(web_updater)")


def main() -> None:
    check_windows_transaction_boundary()
    check_single_workspace_implementation()
    check_web_relaunch_wrapper()
    check_entry_wiring()
    print("issue226 update failure guard: OK")


if __name__ == "__main__":
    main()
