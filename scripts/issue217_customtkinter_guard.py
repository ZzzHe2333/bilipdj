from __future__ import annotations

import sys
import tkinter as tk
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def check_source_wiring() -> None:
    requirements = read("requirements.txt")
    main = read("apps/windows/main.py")
    runtime = read("apps/windows/desktop_runtime.py")
    navigation = read("apps/windows/navigation_layout.py")
    spec = read("apps/windows/bilipdj_onedir.spec")
    quality = read(".github/workflows/quality.yml")

    assert "customtkinter" not in requirements.lower()
    assert "import tkinter as tk" in main
    assert "root = tk.Tk()" in main
    assert "BiliPDJCTk" not in main
    assert "patch_control_panel_customtkinter" not in runtime
    assert "install_page_components" not in runtime
    assert "patch_control_panel_issue180(panel_class)" in runtime
    assert "patch_control_panel_issue196(panel_class)" in runtime
    assert "patch_control_panel_command_console(panel_class)" in runtime
    assert "_bilipdj_tk_ui_installed" in runtime

    # Navigation stays on the original Tk/ttk compatibility path. The legacy
    # CustomTkinter short-circuit may remain for source compatibility, but it is
    # never activated by the production runtime.
    assert "patch_control_panel_issue209" in runtime
    assert "_bilipdj_customtkinter_ui_installed" in navigation

    assert "customtkinter" not in spec.lower()
    assert 'excludes=["customtkinter"]' in spec
    assert '"apps.windows.desktop_runtime"' in spec
    assert "python scripts/issue217_customtkinter_guard.py" in quality


def check_runtime_marker() -> None:
    # Use a lightweight fake control-panel module to prove the production
    # runtime installs the native Tk path without importing CustomTkinter.
    from apps.windows.desktop_runtime import install_desktop_runtime

    module_name = "tk_fake_control_panel"
    fake_module = types.ModuleType(module_name)
    fake_module.__file__ = str(ROOT / "apps" / "windows" / "control_panel.py")
    fake_module.tk = tk
    sys.modules[module_name] = fake_module

    class FakePanel:
        __module__ = module_name

        def __init__(self, root=None):
            self.root = root
            self._nav_items = []
            self._content_pages = []

        def _build_ui(self):
            return None

        def _build_log_tab(self, frame):
            return None

        def _build_settings_tab(self, frame):
            return None

        def _build_quanxian_tab(self, frame):
            return None

        def _build_perf_tab(self, frame):
            return None

        def _apply_theme(self, dark=True):
            return dark

        def _apply_ui_font_size(self):
            return None

    fake_module.ControlPanelApp = FakePanel
    try:
        # Individual feature patches may decline an intentionally incomplete
        # fake panel, but installation must never import/require customtkinter.
        try:
            install_desktop_runtime(FakePanel)
        except Exception as exc:
            assert "customtkinter" not in repr(exc).lower(), repr(exc)
        assert "customtkinter" not in sys.modules
    finally:
        sys.modules.pop(module_name, None)


def main() -> None:
    check_source_wiring()
    check_runtime_marker()
    print("Tk/ttk desktop rollback guard: OK")


if __name__ == "__main__":
    main()
