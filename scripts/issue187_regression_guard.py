from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_version_semantics() -> None:
    from apps.windows.update_channel import select_channel_release, version_key

    assert version_key("3.0.4-test") < version_key("3.0.4")
    assert version_key("3.0.4-test") != version_key("3.0.4-test2")
    assert version_key("3.0.5-beta.1") > version_key("3.0.4")
    assert version_key("v3.0.4+build.7") == version_key("3.0.4")

    selected = select_channel_release(
        [
            {"tag_name": "v3.0.2", "draft": False, "prerelease": False},
            {"tag_name": "v3.0.4-test", "draft": False, "prerelease": True},
            {"tag_name": "v9.9.9", "draft": True, "prerelease": False},
        ]
    )
    assert selected["tag_name"] == "v3.0.4-test"
    stable_same_core = select_channel_release(
        [
            {"tag_name": "v3.0.4-test", "draft": False, "prerelease": True},
            {"tag_name": "v3.0.4", "draft": False, "prerelease": False},
        ]
    )
    assert stable_same_core["tag_name"] == "v3.0.4"


def check_desktop_guards() -> None:
    source = read("apps/windows/update_stability.py")
    assert "threading.Thread" in source
    assert "panel_class.stop_server = stop_server_async" in source
    assert "panel_class.on_close = on_close_async" in source
    assert "_launch_prepared_full = launch_full" in source
    assert "_launch_prepared_incremental = launch_incremental" in source
    assert "self.root.after(45, flush)" in source

    runtime = read("apps/windows/desktop_runtime.py")
    assert "install_update_channel_guard()" in runtime
    assert "patch_control_panel_issue187(panel_class)" in runtime


def check_web_page_handoff() -> None:
    from apps.web.issue187_runtime_guard import patch_web_updater

    calls: list[str] = []

    class PageSeen:
        def wait(self, timeout: float) -> bool:
            assert timeout == 8
            return False

    class State:
        page_seen = PageSeen()

        def set(self, **_kwargs) -> None:
            pass

    def original(*_args, **_kwargs) -> None:
        calls.append("called")

    module = SimpleNamespace(WebUpdaterError=RuntimeError, _full_update=original, _incremental_update=original, _restore_update=original)
    assert patch_web_updater(module)
    for name in ("_full_update", "_incremental_update", "_restore_update"):
        try:
            getattr(module, name)({}, State(), object())
        except RuntimeError as exc:
            assert "主 Web 服务保持运行" in str(exc)
        else:
            raise AssertionError(f"{name} should reject missing page handoff")
    assert not calls

    entry = read("apps/web/web_updater_entry.py")
    assert "patch_web_updater(web_updater)" in entry
    spec = read("apps/web/web_updater.spec")
    assert "web_updater_entry.py" in spec


def check_web_portable_shutdown() -> None:
    source = read("apps/web/issue187_runtime_guard.py")
    assert "bilipdj-web-portable-stop" in source
    assert "threading.Thread" in source
    entry = read("apps/web/portable_launcher_entry.py")
    assert "patch_portable_launcher(portable_launcher.WebPortableLauncher)" in entry
    spec = read("apps/web/web_portable.spec")
    assert "portable_launcher_entry.py" in spec


def check_failed_launch_cleanup() -> None:
    source = read("apps/server/issue187_web_update_guard.py")
    assert "_stop_process(process)" in source
    assert "cleanup_update_session(app_dir, session_dir)" in source
    assert "等待独立 Web 更新器启动超时" in source
    main = read("apps/server/main.py")
    assert "install_issue187_web_update_guard(backend)" in main


def main() -> None:
    check_version_semantics()
    check_desktop_guards()
    check_web_page_handoff()
    check_web_portable_shutdown()
    check_failed_launch_cleanup()
    print("issue #187 stability/update regression guard: OK")


if __name__ == "__main__":
    main()
