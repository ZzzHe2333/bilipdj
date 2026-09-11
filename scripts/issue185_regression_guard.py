from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_web_settings_observer() -> None:
    source = read("apps/web/static/issue167_control_extensions.js")
    assert "text.textContent !== translated" in source
    assert "queueMicrotask" in source
    assert "Web 便携版暂不支持" not in source
    assert "当前 Release 未提供增量资源" in source


def load_runtime_guard():
    path = ROOT / "apps" / "server" / "issue185_runtime_guard.py"
    spec = importlib.util.spec_from_file_location("issue185_runtime_guard_probe", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def check_release_channel_selection() -> None:
    module = load_runtime_guard()
    calls: list[str] = []

    def fake_request(url: str, *, timeout: float = 10.0):
        del timeout
        calls.append(url)
        if url == module.LATEST_RELEASE_API:
            return {"tag_name": "v3.0.2", "draft": False, "prerelease": False}
        if url == module.RELEASES_API:
            return [
                {"tag_name": "v3.0.4-test", "draft": False, "prerelease": True},
                {"tag_name": "v3.0.2", "draft": False, "prerelease": False},
            ]
        raise AssertionError(url)

    original = module._request_json
    module._request_json = fake_request
    try:
        stable = module._select_release("3.0.2")
        assert stable["tag_name"] == "v3.0.2"
        assert calls[-1] == module.LATEST_RELEASE_API
        prerelease = module._select_release("3.0.4-test")
        assert prerelease["tag_name"] == "v3.0.4-test"
        assert calls[-1] == module.RELEASES_API
    finally:
        module._request_json = original


def check_server_installation() -> None:
    source = read("apps/server/main.py")
    assert "from apps.server.issue185_runtime_guard import install_issue185_runtime_guards" in source
    assert "install_issue185_runtime_guards(backend)" in source
    guard = read("apps/server/issue185_runtime_guard.py")
    assert "web_update_api._load_manifest = load_manifest_for_channel" in guard
    assert "update_estimate_api._fetch_latest_release = fetch_release_for_estimate" in guard
    assert "web_control_guard._update_payload = update_payload_for_channel" in guard
    assert "update-manifest.json" in guard


def check_desktop_stability() -> None:
    source = read("apps/windows/issue185_stability.py")
    assert "def _stop_server_nonblocking" in source
    assert "threading.Thread(target=worker" in source
    assert "panel.root.after(0" in source
    assert "canvas.after_idle(flush)" in source
    assert "abs(width - int(state[\"width\"])) > 1" in source
    bootstrap = read("apps/windows/control_panel_bootstrap.py")
    assert "from .issue185_stability import patch_control_panel_issue185" in bootstrap
    assert "patch_control_panel_issue185(cls)" in bootstrap


def main() -> None:
    check_web_settings_observer()
    check_release_channel_selection()
    check_server_installation()
    check_desktop_stability()
    from issue187_regression_guard import main as issue187_main
    from issue189_regression_guard import main as issue189_main

    issue187_main()
    issue189_main()
    print("issue #185 Web/desktop regression guard: OK")


if __name__ == "__main__":
    main()
