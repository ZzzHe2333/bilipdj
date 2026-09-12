from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_source_wiring() -> None:
    requirements = read("requirements.txt")
    custom = read("apps/windows/customtk_ui.py")
    main = read("apps/windows/main.py")
    bootstrap = read("apps/windows/control_panel_bootstrap.py")
    runtime = read("apps/windows/desktop_runtime.py")
    navigation = read("apps/windows/navigation_layout.py")
    spec = read("apps/windows/bilipdj_onedir.spec")
    quality = read(".github/workflows/quality.yml")

    assert "customtkinter>=5.2,<6" in requirements
    assert "import customtkinter as ctk" in custom
    assert "NAV_WIDTH = 178" in custom
    assert "shell.grid_columnconfigure(0, weight=0, minsize=NAV_WIDTH)" in custom
    assert "nav.grid_propagate(False)" in custom
    assert "nav.pack_propagate(False)" in custom
    assert "winfo_reqwidth" not in custom
    assert "class BiliPDJCTk(ctk.CTk)" in custom

    assert "install_desktop_runtime(control_panel.ControlPanelApp)" in main
    assert "from .desktop_runtime import install_desktop_runtime" in bootstrap
    runtime_custom = runtime.index("patch_control_panel_customtkinter(panel_class)")
    runtime_legacy = runtime.index("patch_control_panel_issue209(panel_class)")
    assert runtime_custom < runtime_legacy
    assert "_issue209_superseded_by_customtkinter" in navigation
    assert "_bilipdj_customtkinter_ui_installed" in navigation

    assert 'collect_data_files("customtkinter")' in spec
    assert 'collect_submodules("customtkinter")' in spec
    assert '"apps.windows.customtk_ui"' in spec
    assert '"apps.windows.desktop_runtime"' in spec
    assert "python scripts/issue217_customtkinter_guard.py" in quality


def check_patch_interop() -> None:
    import customtkinter as ctk

    from apps.windows.customtk_ui import BiliPDJCTk, CompatButton, CompatFrame, CompatLabel, NAV_WIDTH, patch_control_panel_customtkinter
    from apps.windows.navigation_layout import patch_control_panel_issue209

    assert NAV_WIDTH == 178
    assert issubclass(BiliPDJCTk, ctk.CTk)
    assert issubclass(CompatFrame, ctk.CTkFrame)
    assert issubclass(CompatButton, ctk.CTkButton)
    assert issubclass(CompatLabel, ctk.CTkLabel)

    module_name = "customtk_fake_control_panel"
    fake_module = types.ModuleType(module_name)
    fake_module.__file__ = str(ROOT / "apps" / "windows" / "control_panel.py")
    sys.modules[module_name] = fake_module

    class FakePanel:
        __module__ = module_name
        def _build_ui(self):
            return "legacy"
        def _apply_theme(self, dark: bool = True):
            return dark
        def _apply_ui_font_size(self):
            return None

    fake_module.ControlPanelApp = FakePanel
    try:
        assert patch_control_panel_customtkinter(FakePanel)
        assert getattr(FakePanel, "_bilipdj_customtkinter_ui_installed", False)
        assert patch_control_panel_issue209(FakePanel)
        assert getattr(FakePanel, "_issue209_superseded_by_customtkinter", False)
        assert getattr(FakePanel, "_issue209_nav_stability_installed", False)
    finally:
        sys.modules.pop(module_name, None)


def main() -> None:
    check_source_wiring()
    check_patch_interop()
    print("CustomTkinter desktop UI guard: OK")


if __name__ == "__main__":
    main()
