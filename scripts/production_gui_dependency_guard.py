from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WINDOWS = ROOT / "apps" / "windows"

PRODUCTION_ENTRY_FILES = [
    WINDOWS / "main.py",
    WINDOWS / "control_panel_bootstrap.py",
    WINDOWS / "desktop_runtime.py",
    WINDOWS / "bilipdj_onedir.spec",
    WINDOWS / "updater_entry.py",
    WINDOWS / "updater.spec",
]

REMOVED_RUNTIME_ISSUE_MODULES = [
    "issue79_features.py",
    "issue167_command_console.py",
    "issue167_update_estimate.py",
    "issue180_windows_ui.py",
    "issue185_stability.py",
    "issue187_stability.py",
    "issue187_update_channel.py",
    "issue189_release_selector.py",
    "issue194_fixed_window.py",
    "issue196_log_toolbar.py",
    "issue209_nav_stability.py",
    "issue222_runtime_hook.py",
    "issue222_update_apply.py",
    "issue222_update_workspace.py",
    "issue226_update_safety.py",
    "updater_issue222_entry.py",
]

REQUIRED_PRODUCTION_MODULES = [
    "desktop_runtime.py",
    "platform_features.py",
    "command_console_ui.py",
    "update_estimate_ui.py",
    "windows_ui.py",
    "gui_stability.py",
    "update_stability.py",
    "update_channel.py",
    "release_selector.py",
    "window_policy.py",
    "log_toolbar.py",
    "navigation_layout.py",
    "updater_apply.py",
    "update_workspace_runtime.py",
    "updater_safety.py",
    "updater_entry.py",
]


def main() -> None:
    issue_import = re.compile(r"(?:from|import)\s+apps\.windows\.issue\d|from\s+\.issue\d|apps\.windows\.issue\d")
    for path in PRODUCTION_ENTRY_FILES:
        text = path.read_text(encoding="utf-8")
        match = issue_import.search(text)
        assert match is None, f"production entry still depends on issue module: {path}: {match.group(0) if match else ''}"

    for name in REMOVED_RUNTIME_ISSUE_MODULES:
        assert not (WINDOWS / name).exists(), f"historical runtime issue module still exists: {name}"

    for name in REQUIRED_PRODUCTION_MODULES:
        assert (WINDOWS / name).is_file(), f"missing production module: {name}"

    main_source = (WINDOWS / "main.py").read_text(encoding="utf-8")
    bootstrap = (WINDOWS / "control_panel_bootstrap.py").read_text(encoding="utf-8")
    runtime = (WINDOWS / "desktop_runtime.py").read_text(encoding="utf-8")
    platform = (WINDOWS / "platform_features.py").read_text(encoding="utf-8")
    spec = (WINDOWS / "bilipdj_onedir.spec").read_text(encoding="utf-8")

    assert "install_desktop_runtime(control_panel.ControlPanelApp)" in main_source
    assert "from .desktop_runtime import install_desktop_runtime" in bootstrap
    assert "install_platform_features(panel_class)" in runtime
    assert "lambda exc=exc" in platform
    assert "issue79_features" not in spec
    assert "apps.windows.desktop_runtime" in spec

    print("production GUI dependency guard: OK")


if __name__ == "__main__":
    main()
