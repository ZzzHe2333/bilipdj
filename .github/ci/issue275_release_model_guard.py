from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_version_model() -> None:
    version = read("VERSION").strip()
    assert re.fullmatch(r"\d+\.\d+\.\d+", version), version
    version_py = read("apps/windows/version.py")
    assert r'^\d+\.\d+\.\d+$' in version_py


def check_windows_updater() -> None:
    selector = read("apps/windows/release_selector.py")
    channel = read("apps/windows/update_channel.py")
    page = read("apps/windows/update_page.py")
    assert 'CHANNEL_ORDER = ("发行包", "全部")' in selector
    assert "RELEASE_ONLY_LIMIT = 3" in selector
    assert "ALL_RELEASE_LIMIT = 10" in selector
    assert 'all_raw = releases[:ALL_RELEASE_LIMIT]' in selector
    assert 'not bool(raw.get("prerelease"))' in selector
    assert 'RELEASE_CHANNELS = ("发行包", "全部")' in page
    assert 'tk.StringVar(value="发行包")' in page
    assert "bool(raw.get(\"prerelease\"))" in channel
    assert "return candidates[0]" in channel


def check_web_updater() -> None:
    server = read("apps/server/issue189_release_selector.py")
    runtime = read("apps/server/issue185_runtime_guard.py")
    web = read("apps/web/static/web_updater_control.js")
    assert "RELEASE_ONLY_LIMIT = 3" in server
    assert "ALL_RELEASE_LIMIT = 10" in server
    assert 'stable_releases' in server
    assert '_latest_stable_entry' in server
    assert 'LATEST_RELEASE_API' in runtime
    assert '_is_prerelease_version' not in runtime
    assert 'id="web-update-filter"' in web
    assert "slice(0, 3)" in web and "slice(0, 10)" in web
    assert "预发行包" in web and "发行包" in web


def check_publish_policy() -> None:
    win = read(".github/workflows/package-windows-x64.yml")
    mac = read(".github/workflows/package-macos.yml")
    bridge = read(".github/workflows/release-dispatch-bridge.yml")
    promote = read(".github/workflows/promote-existing-release.yml")

    for workflow in (win, mac):
        assert "release_status:" in workflow
        assert "default: prerelease" in workflow
        assert "- prerelease" in workflow and "- release" in workflow
        assert "prerelease:" in workflow
    assert "contains(needs.build-portable-bundles.outputs.version, '-')" not in win
    assert "release-publish-stable/v*" in bridge
    assert '-f release_status="$RELEASE_STATUS"' in bridge
    assert "release-promote/v*" in promote
    assert "-F prerelease=false" in promote
    assert "rebuild" not in promote.lower()


def main() -> None:
    check_version_model()
    check_windows_updater()
    check_web_updater()
    check_publish_policy()
    print("issue #275 unified release model guard: OK")


if __name__ == "__main__":
    main()
