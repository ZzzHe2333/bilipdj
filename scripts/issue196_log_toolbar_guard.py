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
    patch = read("apps/windows/issue196_log_toolbar.py")
    main = read("apps/windows/main.py")
    bootstrap = read("apps/windows/control_panel_bootstrap.py")

    assert '_COMPACT_BUTTON_TEXTS = {"清空", "复制"}' in patch
    assert "_COMPACT_BUTTON_WIDTH = 5" in patch
    assert "padding=_COMPACT_BUTTON_PADDING" in patch
    assert "grid_configure(padx=_COMPACT_BUTTON_PADX)" in patch
    assert "from apps.windows.issue196_log_toolbar import patch_control_panel_issue196" in main
    assert "patch_control_panel_issue196(control_panel.ControlPanelApp)" in main
    assert "from .issue196_log_toolbar import patch_control_panel_issue196" in bootstrap
    assert "patch_control_panel_issue196(cls)" in bootstrap


def check_patch_behavior() -> None:
    from apps.windows.issue196_log_toolbar import patch_control_panel_issue196

    module_name = "issue196_fake_control_panel"
    fake_module = types.ModuleType(module_name)
    fake_module.__file__ = str(ROOT / "apps" / "windows" / "control_panel.py")
    sys.modules[module_name] = fake_module

    class FakeWidget:
        def __init__(self, widget_class: str, text: str = "", children=None) -> None:
            self.widget_class = widget_class
            self.text = text
            self.children = list(children or [])
            self.options = {}
            self.grid_options = {"row": 0, "column": 0}

        def winfo_children(self):
            return list(self.children)

        def winfo_class(self):
            return self.widget_class

        def cget(self, key: str):
            if key == "text":
                return self.text
            raise KeyError(key)

        def configure(self, **kwargs):
            self.options.update(kwargs)

        def grid_info(self):
            return dict(self.grid_options)

        def grid_configure(self, **kwargs):
            self.grid_options.update(kwargs)

    clear_button = FakeWidget("TButton", "清空")
    copy_button = FakeWidget("TButton", "复制")
    unrelated_button = FakeWidget("TButton", "发送")
    label = FakeWidget("TLabel", "清空")
    toolbar = FakeWidget("TFrame", children=[clear_button, copy_button, unrelated_button, label])
    frame = FakeWidget("TFrame", children=[toolbar])

    def fake_build_log_tab(self, passed_frame):
        assert passed_frame is frame
        return "ok"

    FakePanel = type(
        "ControlPanelApp",
        (),
        {
            "__module__": module_name,
            "_build_log_tab": fake_build_log_tab,
        },
    )
    fake_module.ControlPanelApp = FakePanel

    try:
        assert patch_control_panel_issue196(FakePanel)
        panel = FakePanel()
        assert panel._build_log_tab(frame) == "ok"
        assert clear_button.options["width"] == 5
        assert copy_button.options["width"] == 5
        assert clear_button.options["padding"] == (5, 5)
        assert copy_button.options["padding"] == (5, 5)
        assert clear_button.grid_options["padx"] == (2, 1)
        assert copy_button.grid_options["padx"] == (2, 1)
        assert unrelated_button.options == {}
        assert label.options == {}
        assert set(panel._issue196_compact_log_buttons) == {"清空", "复制"}
        assert patch_control_panel_issue196(FakePanel)
    finally:
        sys.modules.pop(module_name, None)


def main() -> None:
    check_source_wiring()
    check_patch_behavior()
    print("issue #196 compact log toolbar guard: OK")


if __name__ == "__main__":
    main()
