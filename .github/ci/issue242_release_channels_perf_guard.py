from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_release_channels() -> None:
    from apps.windows.release_selector import (
        ALL_RELEASE_LIMIT,
        CHANNEL_ORDER,
        RELEASE_ONLY_LIMIT,
        display_version,
        is_deprecated_version,
        release_channel,
    )

    assert CHANNEL_ORDER == ("发行包", "全部")
    assert RELEASE_ONLY_LIMIT == 3
    assert ALL_RELEASE_LIMIT == 10
    # A version string no longer defines a channel; GitHub prerelease is source of truth.
    assert release_channel("3.0.15") == "全部"
    assert release_channel("3.0.15-test") == "全部"
    assert display_version("3.0.15-test") == "3.0.15-test"
    for patch in range(7, 12):
        assert is_deprecated_version(f"3.0.{patch}-test")

    source = read("apps/windows/release_selector.py")
    assert 'CHANNEL_ORDER = ("发行包", "全部")' in source
    assert "RELEASE_ONLY_LIMIT = 3" in source
    assert "ALL_RELEASE_LIMIT = 10" in source
    assert 'raw.get("prerelease")' in source
    assert '_RELEASE_TAGS_BY_FILTER["发行包"]' in source
    assert 'update_channel_var.set("发行包")' in source
    assert "MAX_RELEASE_SCAN = 100" in source


def check_update_page_policy() -> None:
    source = read("apps/windows/release_selector.py")
    page = read("apps/windows/update_page.py")
    assert "update_page.RELEASE_CHANNELS = CHANNEL_ORDER" in source
    assert "def _install_update_page_policy" in source
    assert "最近 3 个发行包" in source
    assert "最近 10 个 Release" in source
    assert 'text="版本类型"' in page
    assert "_update_channel_combo" in page
    assert "_update_version_combo" in page


def check_performance_monitor() -> None:
    source = read("apps/windows/performance_monitor.py")
    runtime = read("apps/windows/desktop_runtime.py")
    assert "DEFAULT_REFRESH_SECONDS = 2.0" in source
    assert "MIN_REFRESH_SECONDS = 0.5" in source
    assert "MAX_REFRESH_SECONDS = 60.0" in source
    assert "BooleanVar(value=False)" in source
    assert 'StringVar(value="2")' in source
    assert "if enabled:" in source
    assert "psutil.cpu_percent" in source
    assert "psutil.virtual_memory" in source
    assert "process.memory_info().rss" in source
    assert "_directory_size(app_dir)" in source
    assert "panel.root.after(100, panel._refresh_perf)" in source
    assert "install_performance_monitor(panel_class)" in runtime
    assert runtime.index("patch_control_panel_issue180(panel_class)") < runtime.index("install_performance_monitor(panel_class)")


def main() -> None:
    check_release_channels()
    check_update_page_policy()
    check_performance_monitor()
    print("issue242 release state/performance guard: OK")


if __name__ == "__main__":
    main()
