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
    patch = read("apps/windows/window_policy.py")
    runtime = read("apps/windows/desktop_runtime.py")

    assert "WINDOW_WIDTH = 1180" in patch
    assert "WINDOW_HEIGHT = 720" in patch
    assert "root.minsize(WINDOW_WIDTH, WINDOW_HEIGHT)" in patch
    assert "root.maxsize(WINDOW_WIDTH, WINDOW_HEIGHT)" in patch
    assert "root.resizable(False, False)" in patch
    assert "root.geometry(WINDOW_GEOMETRY)" in patch
    assert "root.after_idle(enforce)" in patch
    assert "from .window_policy import patch_control_panel_issue194" in runtime
    assert "patch_control_panel_issue194(panel_class)" in runtime


def check_patch_behavior() -> None:
    from apps.windows.window_policy import WINDOW_HEIGHT, WINDOW_WIDTH, patch_control_panel_issue194

    module_name = "window_policy_fake_control_panel"
    fake_module = types.ModuleType(module_name)
    fake_module.__file__ = str(ROOT / "apps" / "windows" / "control_panel.py")
    sys.modules[module_name] = fake_module

    class FakeRoot:
        def __init__(self) -> None:
            self.width = 1
            self.height = 1
            self.min_size = None
            self.max_size = None
            self.resize_flags = None
            self.geometry_calls: list[str] = []

        def minsize(self, width: int, height: int) -> None:
            self.min_size = (width, height)

        def maxsize(self, width: int, height: int) -> None:
            self.max_size = (width, height)

        def resizable(self, width: bool, height: bool) -> None:
            self.resize_flags = (width, height)

        def geometry(self, geometry: str) -> None:
            self.geometry_calls.append(geometry)
            size = geometry.split("+", 1)[0]
            self.width, self.height = (int(part) for part in size.split("x", 1))

        def winfo_width(self) -> int:
            return self.width

        def winfo_height(self) -> int:
            return self.height

        def after_idle(self, callback):
            callback()
            return "idle-1"

    def fake_init(self, root) -> None:
        self.root = root

    FakePanel = type("ControlPanelApp", (), {"__module__": module_name, "__init__": fake_init})
    fake_module.ControlPanelApp = FakePanel

    try:
        assert patch_control_panel_issue194(FakePanel)
        root = FakeRoot()
        panel = FakePanel(root)
        assert root.min_size == (WINDOW_WIDTH, WINDOW_HEIGHT)
        assert root.max_size == (WINDOW_WIDTH, WINDOW_HEIGHT)
        assert root.resize_flags == (False, False)
        assert root.geometry_calls
        assert root.geometry_calls[-1] == f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}"
        assert panel._issue194_fixed_window_size == (WINDOW_WIDTH, WINDOW_HEIGHT)
        assert patch_control_panel_issue194(FakePanel)
    finally:
        sys.modules.pop(module_name, None)


def main() -> None:
    check_source_wiring()
    check_patch_behavior()
    print("fixed Windows window policy guard: OK")


if __name__ == "__main__":
    main()
