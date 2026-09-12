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
    issue209 = read("apps/windows/issue209_nav_stability.py")
    spec = read("apps/windows/bilipdj_onedir.spec")
    quality = read(".github/workflows/quality.yml")

    assert "customtkinter>=5.2,<6" in requirements
    assert "import customtkinter as ctk" in custom
    assert "NAV_WIDTH = 178" in custom
    assert "shell.grid_columnconfigure(0, weight=0, minsize=NAV_WIDTH)" in custom
    assert "nav.grid_propagate(False)" in custom
    assert "nav.pack_propagate(False)" in custom
    assert "winfo_reqwidth" not in custom, "CTk navigation must not derive width from requested label geometry"
    assert "class BiliPDJCTk(ctk.CTk)" in custom
    assert "def run_control_panel(module: Any)" in custom

    main_custom = main.index("patch_control_panel_customtkinter(control_panel.ControlPanelApp)")
    main_legacy = main.index("patch_control_panel_issue209(control_panel.ControlPanelApp)")
    assert main_custom < main_legacy
    boot_custom = bootstrap.index("patch_control_panel_customtkinter(cls)")
    boot_legacy = bootstrap.index("patch_control_panel_issue209(cls)")
    assert boot_custom < boot_legacy
    assert "_issue209_superseded_by_customtkinter" in issue209
    assert "_bilipdj_customtkinter_ui_installed" in issue209

    assert 'collect_data_files("customtkinter")' in spec
    assert 'collect_submodules("customtkinter")' in spec
    assert '"apps.windows.customtk_ui"' in spec
    assert "python scripts/issue217_customtkinter_guard.py" in quality


def check_patch_interop() -> None:
    import customtkinter as ctk

    from apps.windows.customtk_ui import (
        BiliPDJCTk,
        CompatButton,
        CompatFrame,
        CompatLabel,
        NAV_WIDTH,
        patch_control_panel_customtkinter,
    )
    from apps.windows.issue209_nav_stability import patch_control_panel_issue209

    assert NAV_WIDTH == 178
    assert issubclass(BiliPDJCTk, ctk.CTk)
    assert issubclass(CompatFrame, ctk.CTkFrame)
    assert issubclass(CompatButton, ctk.CTkButton)
    assert issubclass(CompatLabel, ctk.CTkLabel)

    module_name = "issue217_fake_control_panel"
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
    print("issue #217 CustomTkinter desktop UI guard: OK")


if __name__ == "__main__":
    main()
