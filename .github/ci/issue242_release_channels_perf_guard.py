from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_release_channels() -> None:
    from apps.windows.release_selector import CHANNEL_ORDER, display_version, is_deprecated_version, release_channel

    assert CHANNEL_ORDER == ("正式版", "公测版", "创新版", "内测版", "废弃版")
    assert release_channel("3.0.6") == "正式版"
    assert release_channel("3.1.0-gc") == "公测版"
    assert release_channel("3.1.0-g") == "公测版"
    assert release_channel("3.1.0-cx") == "创新版"
    assert release_channel("3.1.0-c") == "创新版"
    assert release_channel("3.1.0-text") == "内测版"
    assert release_channel("3.1.0-t") == "内测版"
    assert release_channel("3.1.0-dev") == "内测版"
    assert release_channel("3.1.0-feiqi") == "废弃版"
    assert release_channel("3.1.0-loss") == "废弃版"
    # Compatibility: current test suffix remains internal unless it belongs to
    # the explicitly deprecated 3.0.7..3.0.11 UI migration window.
    assert release_channel("3.0.12-test") == "内测版"
    for patch in range(7, 12):
        version = f"3.0.{patch}-test"
        assert is_deprecated_version(version)
        assert display_version(version) == f"3.0.{patch}-feiqi"

    source = read("apps/windows/release_selector.py")
    assert "RECENT_RELEASE_LIMIT = 10" in source
    assert "MAX_RELEASE_SCAN = 100" in source
    assert "counts = {channel: 0 for channel in CHANNEL_ORDER}" in source
    assert "full_body = str(raw.get(\"body\"" in source
    assert "【废弃版本：不可使用】" in source
    assert "full_button.configure(text=\"废弃版本\", state=\"disabled\")" in source


def check_update_page() -> None:
    source = read("apps/windows/update_page.py")
    assert 'RELEASE_CHANNELS = ("正式版", "公测版", "创新版", "内测版", "废弃版")' in source
    assert 'text="版本类型"' in source
    assert 'text="版本"' in source
    assert "_update_channel_combo" in source
    assert "_update_version_combo" in source
    assert 'tk.StringVar(value="正式版")' in source
    assert "on_channel_selected" in source


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
    check_update_page()
    check_performance_monitor()
    print("issue242 release channels/performance guard: OK")


if __name__ == "__main__":
    main()
