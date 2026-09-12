from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WINDOWS = ROOT / "apps" / "windows"
COMPONENTS = WINDOWS / "components"

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

REQUIRED_COMPONENTS = [
    "__init__.py",
    "base.py",
    "registry.py",
    "log_page.py",
    "settings_page.py",
    "update_page.py",
]


def _production_python_files() -> list[Path]:
    return sorted(
        path
        for path in WINDOWS.rglob("*.py")
        if not path.name.startswith("issue") and "__pycache__" not in path.parts
    )


def main() -> None:
    issue_import = re.compile(r"(?:from|import)\s+apps\.windows\.issue\d|from\s+\.issue\d|apps\.windows\.issue\d")
    for path in [*_production_python_files(), *PRODUCTION_ENTRY_FILES]:
        text = path.read_text(encoding="utf-8")
        match = issue_import.search(text)
        assert match is None, f"production Windows module still depends on issue runtime: {path.relative_to(ROOT)}: {match.group(0) if match else ''}"

    for name in REMOVED_RUNTIME_ISSUE_MODULES:
        assert not (WINDOWS / name).exists(), f"historical runtime issue module still exists: {name}"

    for name in REQUIRED_PRODUCTION_MODULES:
        assert (WINDOWS / name).is_file(), f"missing production module: {name}"
    for name in REQUIRED_COMPONENTS:
        assert (COMPONENTS / name).is_file(), f"missing GUI component module: {name}"

    main_source = (WINDOWS / "main.py").read_text(encoding="utf-8")
    bootstrap = (WINDOWS / "control_panel_bootstrap.py").read_text(encoding="utf-8")
    runtime = (WINDOWS / "desktop_runtime.py").read_text(encoding="utf-8")
    component_base = (COMPONENTS / "base.py").read_text(encoding="utf-8")
    component_registry = (COMPONENTS / "registry.py").read_text(encoding="utf-8")
    log_component = (COMPONENTS / "log_page.py").read_text(encoding="utf-8")
    settings_component = (COMPONENTS / "settings_page.py").read_text(encoding="utf-8")
    platform = (WINDOWS / "platform_features.py").read_text(encoding="utf-8")
    huya = (WINDOWS / "huya_control_guard.py").read_text(encoding="utf-8")
    redtv = (WINDOWS / "redtv_control_guard.py").read_text(encoding="utf-8")
    purple = (WINDOWS / "purple_mouse_control_guard.py").read_text(encoding="utf-8")
    spec = (WINDOWS / "bilipdj_onedir.spec").read_text(encoding="utf-8")
    core_init = (ROOT / "core" / "__init__.py").read_text(encoding="utf-8")

    assert "install_desktop_runtime(control_panel.ControlPanelApp)" in main_source
    assert main_source.index("install_desktop_runtime(control_panel.ControlPanelApp)") < main_source.index("app = control_panel.ControlPanelApp(root)")

    assert "__build_class__" not in bootstrap
    assert "import builtins" not in bootstrap
    assert "install_control_panel_class_hook()" not in core_init
    assert "install_control_panel_runtime" in bootstrap

    assert "from .components import install_page_components" in runtime
    assert "install_page_components(panel_class)" in runtime
    assert runtime.index("_install_compatibility_runtime(panel_class)") < runtime.index("install_page_components(panel_class)")
    assert "patch_control_panel_issue180(panel_class)" not in runtime
    assert "patch_control_panel_issue196(panel_class)" not in runtime

    assert "class PageComponent" in component_base
    assert "CompatFrame" in component_base
    assert "LogPageComponent" in component_registry
    assert "SettingsPageComponent" in component_registry
    assert "UpdatePageComponent" in component_registry
    assert 'setattr(panel_class, "_build_log_tab"' in component_registry
    assert 'setattr(panel_class, "_build_settings_tab"' in component_registry
    assert 'setattr(panel_class, "_build_quanxian_tab"' in component_registry
    assert 'setattr(panel_class, "_build_perf_tab"' in component_registry
    assert "update_page_module.build_update_tab =" in component_registry
    assert "_install_stable_update_layout()" in component_registry
    assert "compact_log_toolbar(host)" in log_component
    assert "_build_plugin_manager_tab(panel, module)" in settings_component

    assert "install_platform_features(panel_class)" in runtime
    assert "lambda exc=exc" in platform
    for source in (platform, huya, redtv, purple):
        assert "_issue79_platform_vars" not in source
        assert "_issue79_platform_status_var" not in source
    assert "issue79_features" not in spec
    assert "apps.windows.desktop_runtime" in spec

    print("production GUI dependency/component guard: OK")


if __name__ == "__main__":
    main()
