from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_windows_release_choice() -> None:
    from apps.windows.release_selector import ALL_RELEASE_LIMIT, CHANNEL_ORDER, RELEASE_ONLY_LIMIT
    from apps.windows.update_channel import select_channel_release

    assert CHANNEL_ORDER == ("发行包", "全部")
    assert RELEASE_ONLY_LIMIT == 3
    assert ALL_RELEASE_LIMIT == 10
    releases = [
        {"tag_name": "v3.0.17", "draft": False, "prerelease": True, "published_at": "2026-09-17T00:00:00Z"},
        {"tag_name": "v3.0.16", "draft": False, "prerelease": False, "published_at": "2026-09-16T00:00:00Z"},
        {"tag_name": "v3.0.15-test", "draft": False, "prerelease": True, "published_at": "2026-09-15T00:00:00Z"},
    ]
    assert select_channel_release(releases)["tag_name"] == "v3.0.16"

    source = read("apps/windows/release_selector.py")
    assert 'CHANNEL_ORDER = ("发行包", "全部")' in source
    assert 'all_raw = releases[:ALL_RELEASE_LIMIT]' in source
    assert 'not bool(raw.get("prerelease"))' in source
    assert 'update_channel_var.set("发行包")' in source
    assert "def on_channel_selected" in source

    runtime = read("apps/windows/desktop_runtime.py")
    assert "install_release_selector()" in runtime
    spec = read("apps/windows/bilipdj_onedir.spec")
    assert "apps.windows.release_selector" in spec


def check_web_release_choice() -> None:
    source = read("apps/server/issue189_release_selector.py")
    assert "RELEASE_ONLY_LIMIT = 3" in source
    assert "ALL_RELEASE_LIMIT = 10" in source
    assert 'all_recent = releases[:ALL_RELEASE_LIMIT]' in source
    assert 'stable_recent = [release for release in releases if not bool(release.get("prerelease"))][:RELEASE_ONLY_LIMIT]' in source
    assert 'result["stable_releases"]' in source
    assert 'result["default_tag"] = default["tag_name"]' in source
    assert 'selected = _latest_stable_entry(entries)' in source
    assert 'web_control_guard._update_payload = stable_update_payload' in source

    main = read("apps/server/main.py")
    assert "install_issue189_release_selector(backend)" in main
    spec = read("apps/web/web_portable.spec")
    assert "apps.server.issue189_release_selector" in spec


def check_web_frontend() -> None:
    source = read("apps/web/static/web_updater_control.js")
    assert "web-update-filter" in source
    assert "发行包" in source
    assert "预发行包" in source
    assert "stable_releases" in source
    assert "slice(0, 3)" in source
    assert "slice(0, 10)" in source
    assert "state?.default_tag" in source
    assert "payload.target_tag" in source
    assert "默认以最新发行包为更新目标" in source


def main() -> None:
    check_windows_release_choice()
    check_web_release_choice()
    check_web_frontend()
    print("issue #189 release selector regression guard: OK")


if __name__ == "__main__":
    main()
