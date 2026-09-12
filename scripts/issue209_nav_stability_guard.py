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
    patch = read("apps/windows/navigation_layout.py")
    runtime = read("apps/windows/desktop_runtime.py")
    quality = read(".github/workflows/quality.yml")

    assert "nav.pack_propagate(False)" in patch
    assert "shell.columnconfigure(0, weight=0, minsize=width)" in patch
    assert "_NAV_WIDTH_HEADROOM_PX = 12" in patch
    assert "from .navigation_layout import patch_control_panel_issue209" in runtime
    assert "patch_control_panel_issue209(panel_class)" in runtime
    assert "python scripts/issue209_nav_stability_guard.py" in quality


def check_patch_behavior() -> None:
    from apps.windows.navigation_layout import patch_control_panel_issue209

    module_name = "navigation_layout_fake_control_panel"
    fake_module = types.ModuleType(module_name)
    fake_module.__file__ = str(ROOT / "apps" / "windows" / "control_panel.py")
    sys.modules[module_name] = fake_module

    class FakeRoot:
        def __init__(self) -> None:
            self.update_calls = 0
        def update_idletasks(self) -> None:
            self.update_calls += 1

    class FakeShell:
        def __init__(self) -> None:
            self.columns = {}
        def columnconfigure(self, index: int, **kwargs) -> None:
            self.columns[index] = dict(kwargs)

    class FakeNav:
        def __init__(self, master: FakeShell) -> None:
            self.master = master
            self.width = 90
            self.propagate = True
        def pack_propagate(self, value: bool) -> None:
            self.propagate = bool(value)
        def configure(self, **kwargs) -> None:
            if "width" in kwargs:
                self.width = int(kwargs["width"])
        def winfo_width(self) -> int:
            return self.width

    class FakeRow:
        def __init__(self, requested_width: int) -> None:
            self.requested_width = requested_width
        def winfo_reqwidth(self) -> int:
            return self.requested_width

    shell = FakeShell()
    nav = FakeNav(shell)
    rows = [FakeRow(88), FakeRow(104), FakeRow(96)]

    def fake_build_ui(self):
        self._nav_frame = nav
        self._nav_items = [(row, object(), object()) for row in rows]
        return "built"

    def fake_show_page(self, index: int):
        self._active_page = index
        rows[index % len(rows)].requested_width = 150
        if nav.propagate:
            nav.width = max(row.winfo_reqwidth() for row in rows)
        return index

    FakePanel = type("ControlPanelApp", (), {"__module__": module_name, "_build_ui": fake_build_ui, "_show_page": fake_show_page})
    fake_module.ControlPanelApp = FakePanel

    try:
        assert patch_control_panel_issue209(FakePanel)
        panel = FakePanel()
        panel.root = FakeRoot()
        assert panel._build_ui() == "built"
        frozen = panel._issue209_nav_width
        assert frozen == 104 + 12
        assert nav.width == frozen
        assert nav.propagate is False
        assert shell.columns[0] == {"weight": 0, "minsize": frozen}
        assert panel._show_page(1) == 1
        assert rows[1].requested_width == 150
        assert nav.width == frozen
        assert patch_control_panel_issue209(FakePanel)
    finally:
        sys.modules.pop(module_name, None)


def main() -> None:
    check_source_wiring()
    check_patch_behavior()
    print("navigation stability guard: OK")


if __name__ == "__main__":
    main()
