"""Issue #290: ensure update/download controls and network settings live in separate subtabs."""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.windows import update_page, update_version_selector, download_acceleration


class FakeVar:
    def __init__(self, *args, value=None, **kwargs):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class FakeWidget:
    def __init__(self, parent=None, *args, **kwargs):
        self.master = parent
        self.children = []
        self.text = kwargs.get("text", "")
        self.grid_args = None
        self.tabs = []
        if isinstance(parent, FakeWidget):
            parent.children.append(self)

    def columnconfigure(self, *args, **kwargs):
        pass

    def rowconfigure(self, *args, **kwargs):
        pass

    def grid(self, **kwargs):
        self.grid_args = kwargs

    def grid_propagate(self, *args):
        pass

    def pack(self, **kwargs):
        pass

    def place(self, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

    def insert(self, *args, **kwargs):
        pass

    def bind(self, *args, **kwargs):
        pass

    def configure(self, **kwargs):
        pass

    def create_window(self, *args, **kwargs):
        return 1

    def add(self, item, **kwargs):
        self.tabs.append((item, kwargs.get("text")))

    def yview(self, *args, **kwargs):
        pass

    def set(self, *args, **kwargs):
        pass


def belongs_to(widget, page):
    current = widget
    while current is not None:
        if current is page:
            return True
        current = getattr(current, "master", None)
    return False


def verify_layout():
    # Simulate Tk widgets without a desktop display, while executing the
    # actual update page and source-picker constructors (not only source grep).
    ui_names = ("Frame", "LabelFrame", "Label", "Scrollbar", "Entry",
                "Combobox", "Progressbar", "Button", "Checkbutton", "Notebook")
    with patch.multiple(update_page.tk, StringVar=FakeVar, BooleanVar=FakeVar,
                        DoubleVar=FakeVar, Canvas=FakeWidget, Text=FakeWidget), \
         patch.multiple(update_page.ttk, **{name: FakeWidget for name in ui_names}), \
         patch.object(update_page, "load_network_settings"), \
         patch.object(update_version_selector, "check_for_versions") as check:
        orig = update_page.build_update_tab
        try:
            download_acceleration._install_source_picker()
            app = type("FakeApp", (), {})()
            app.root = FakeWidget()
            app._all_text_widgets = []
            frame = FakeWidget()
            update_page.build_update_tab(app, frame, "弹幕排队姬", "3.0.18", ROOT)
            tabs = app._update_subtabs
            assert [name for _, name in tabs.tabs] == ["版本更新", "更新设置"]
            version_page, settings_page = [entry[0] for entry in tabs.tabs]
            assert belongs_to(app._update_progress, version_page)
            assert belongs_to(app._update_notes, version_page)
            assert belongs_to(app._update_channel_combo, version_page)
            assert belongs_to(app._update_version_combo, version_page)
            assert belongs_to(app._update_check_button, version_page)
            assert belongs_to(app._update_full_button, version_page)
            assert belongs_to(app._update_incremental_button, version_page)
            assert belongs_to(app._update_download_source_combo, settings_page)
            assert app._update_download_source_frame.grid_args["row"] == 0
            assert app._update_settings_content.grid_args is None
            assert all(belongs_to(entry, settings_page) for entry in app._update_proxy_entries)
            assert belongs_to(app._update_proxy_test_button, settings_page)
            source = app._update_download_source_combo
            assert source.master is app._update_download_source_frame
            check.assert_called_once_with(app, silent=True)
        finally:
            update_page.build_update_tab = orig


def check_source_layout():
    source = (ROOT / "apps/windows/update_page.py").read_text(encoding="utf-8")
    acceleration = (ROOT / "apps/windows/download_acceleration.py").read_text(encoding="utf-8")
    assert 'sub_tabs.add(versions_tab, text="版本更新")' in source
    assert 'sub_tabs.add(settings_tab, text="更新设置")' in source
    assert 'network.grid(row=1' in source
    assert 'source_frame.grid(row=0' in acceleration
    assert 'getattr(app, "_update_settings_content", None)' in acceleration
    assert "def _bind_stable_scroll_region" in source
    assert "def _scrollable_update_content" in source
    print("issue #290 two update subtabs and control parentage: OK")


if __name__ == "__main__":
    check_source_layout()
    verify_layout()
