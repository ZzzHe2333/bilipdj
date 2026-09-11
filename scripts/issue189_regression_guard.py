from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_windows_release_choice() -> None:
    from apps.windows.issue189_release_selector import _newest_release

    releases = [
        {"tag_name": "v3.0.4-test", "draft": False, "prerelease": True},
        {"tag_name": "v3.0.2", "draft": False, "prerelease": False},
        {"tag_name": "v3.0.3", "draft": False, "prerelease": False},
        {"tag_name": "v3.0.5-beta.1", "draft": False, "prerelease": True},
        {"tag_name": "v9.9.9", "draft": True, "prerelease": False},
    ]
    assert _newest_release(releases, prerelease=False)["tag_name"] == "v3.0.3"
    assert _newest_release(releases, prerelease=True)["tag_name"] == "v3.0.5-beta.1"

    source = read("apps/windows/issue189_release_selector.py")
    assert '"云端正式版"' in source
    assert '"云端测试版"' in source
    assert "_newest_release(releases, prerelease=False)" in source
    assert "_newest_release(releases, prerelease=True)" in source
    assert "default_release = releases[0]" in source

    bootstrap = read("apps/windows/control_panel_bootstrap.py")
    assert "install_issue189_release_selector()" in bootstrap
    spec = read("apps/windows/bilipdj_onedir.spec")
    assert "apps.windows.issue189_release_selector" in spec


def check_web_release_choice() -> None:
    from apps.server.issue189_release_selector import _latest

    releases = [
        {"tag_name": "v3.0.4-test", "draft": False, "prerelease": True},
        {"tag_name": "v3.0.2", "draft": False, "prerelease": False},
        {"tag_name": "v3.0.3", "draft": False, "prerelease": False},
        {"tag_name": "v3.0.5-beta.1", "draft": False, "prerelease": True},
    ]
    assert _latest(releases, prerelease=False)["tag_name"] == "v3.0.3"
    assert _latest(releases, prerelease=True)["tag_name"] == "v3.0.5-beta.1"

    source = read("apps/server/issue189_release_selector.py")
    assert 'chosen = [_latest(releases, prerelease=False), _latest(releases, prerelease=True)]' in source
    assert 'result["default_tag"] = default["tag_name"]' in source
    assert 'target_tag = str(payload.get("target_tag"' in source
    assert 'web_update_api._load_manifest = lambda: manifest' in source
    assert 'web_control_guard._update_payload = stable_update_payload' in source

    main = read("apps/server/main.py")
    assert "install_issue189_release_selector(backend)" in main
    spec = read("apps/web/web_portable.spec")
    assert "apps.server.issue189_release_selector" in spec


def check_web_frontend() -> None:
    source = read("apps/web/static/web_updater_control.js")
    assert "state?.releases" in source
    assert "云端正式版" in source
    assert "云端测试版" in source
    assert "state?.default_tag" in source
    assert "payload.target_tag" in source
    assert "默认选择正式版" in source


def main() -> None:
    check_windows_release_choice()
    check_web_release_choice()
    check_web_frontend()
    print("issue #189 release selector regression guard: OK")


if __name__ == "__main__":
    main()
