from __future__ import annotations

import inspect
import unittest
from pathlib import Path
from unittest import mock

from apps.windows import bilibili_qr_dialog

ROOT = Path(__file__).resolve().parents[1]


class _Var:
    def __init__(self, value=""):
        self.value = value

    def get(self):
        return self.value

    def set(self, value):
        self.value = value


class NativeBilibiliQrLoginTests(unittest.TestCase):
    def test_patch_replaces_browser_login_and_applies_success(self) -> None:
        dialogs = []

        class FakeDialog:
            def __init__(self, parent, *, port, on_success):
                self.parent = parent
                self.port = port
                self.on_success = on_success
                self.window = None
                dialogs.append(self)
                on_success("SESSDATA=test-cookie", 123456, "Tester")

        class FakePanel:
            _FREE_NOTICE = "free"

            def __init__(self):
                self.root = object()
                self.port_var = _Var("9816")
                self.cookie_var = _Var("")
                self.uid_var = _Var("0")
                self._bilibili_fetch_status_var = _Var("")
                self.logs = []

            def _append_log(self, message, warn=False):
                self.logs.append((message, warn))

            def open_config(self):
                raise AssertionError("legacy browser login should be replaced")

        with mock.patch.object(bilibili_qr_dialog, "BilibiliQrLoginDialog", FakeDialog):
            self.assertTrue(bilibili_qr_dialog.patch_control_panel_qr_login(FakePanel))
            panel = FakePanel()
            panel.open_config()

        self.assertEqual(len(dialogs), 1)
        self.assertEqual(dialogs[0].port, "9816")
        self.assertEqual(panel.cookie_var.get(), "SESSDATA=test-cookie")
        self.assertEqual(panel.uid_var.get(), "123456")
        self.assertIn("扫码登录成功", panel._bilibili_fetch_status_var.get())
        self.assertTrue(any("Cookie 已由后端持久化" in message for message, _warn in panel.logs))

    def test_native_dialog_uses_existing_qr_api_off_main_thread(self) -> None:
        source = inspect.getsource(bilibili_qr_dialog)
        self.assertIn('"/api/bili/qr/start"', source)
        self.assertIn('"/api/bili/qr/poll"', source)
        self.assertIn("threading.Thread", source)
        self.assertNotIn("webbrowser.open", source)

    def test_bootstrap_installs_native_qr_patch(self) -> None:
        source = (ROOT / "apps" / "windows" / "control_panel_bootstrap.py").read_text(encoding="utf-8")
        self.assertIn("from .bilibili_qr_dialog import patch_control_panel_qr_login", source)
        self.assertIn("patch_control_panel_qr_login(cls)", source)

    def test_windows_spec_packages_native_qr_module(self) -> None:
        source = (ROOT / "apps" / "windows" / "bilipdj_onedir.spec").read_text(encoding="utf-8")
        self.assertIn('"apps.windows.bilibili_qr_dialog"', source)


if __name__ == "__main__":
    unittest.main()
