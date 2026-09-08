from __future__ import annotations

import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
WINDOWS_MAIN = ROOT / "apps" / "windows" / "main.py"
QR_DIALOG = ROOT / "apps" / "windows" / "bilibili_qr_dialog.py"
WEB_LAUNCHER = ROOT / "apps" / "web" / "portable_launcher.py"
WEB_SPEC = ROOT / "apps" / "web" / "web_portable.spec"
WEB_CONFIG = ROOT / "apps" / "web" / "static" / "config.html"


class NativeWindowsLoginEntryTests(unittest.TestCase):
    def test_windows_entry_explicitly_installs_native_qr_patch(self) -> None:
        source = WINDOWS_MAIN.read_text(encoding="utf-8")
        self.assertIn(
            "from apps.windows.bilibili_qr_dialog import patch_control_panel_qr_login",
            source,
        )
        self.assertIn(
            "patch_control_panel_qr_login(control_panel.ControlPanelApp)",
            source,
        )
        self.assertLess(
            source.index("patch_control_panel_qr_login(control_panel.ControlPanelApp)"),
            source.index("def main()"),
        )

    def test_native_patch_replaces_open_config_with_tk_dialog(self) -> None:
        source = QR_DIALOG.read_text(encoding="utf-8")
        self.assertIn('setattr(panel_class, "open_config", open_native_config)', source)
        self.assertIn("BilibiliQrLoginDialog(self.root", source)
        self.assertIn("self.cookie_var.set(cookie)", source)
        self.assertIn("self.uid_var.set(str(uid))", source)

    def test_web_qr_configuration_page_is_still_present(self) -> None:
        source = WEB_CONFIG.read_text(encoding="utf-8")
        self.assertIn('id="qr-btn"', source)
        self.assertIn('id="save-btn"', source)
        self.assertIn("扫码连接", source)


class WebServiceManagerTests(unittest.TestCase):
    def test_launcher_source_is_valid_python(self) -> None:
        ast.parse(WEB_LAUNCHER.read_text(encoding="utf-8"))

    def test_frozen_web_executable_is_windowed(self) -> None:
        source = WEB_SPEC.read_text(encoding="utf-8")
        self.assertIn("console=False", source)

    def test_windows_backend_child_has_double_hidden_window_guard(self) -> None:
        source = WEB_LAUNCHER.read_text(encoding="utf-8")
        self.assertIn("CREATE_NO_WINDOW", source)
        self.assertIn("STARTUPINFO", source)
        self.assertIn("STARTF_USESHOWWINDOW", source)
        self.assertIn("SW_HIDE", source)
        self.assertIn("_hidden_backend_process_options()", source)

    def test_launcher_auto_minimizes_after_browser_is_opened(self) -> None:
        source = WEB_LAUNCHER.read_text(encoding="utf-8")
        self.assertIn("self.root.after(1200, self._auto_minimize)", source)
        self.assertIn("self.root.iconify()", source)
        self.assertIn("打开 Web 控制台", source)
        self.assertIn("停止并退出", source)

    def test_failed_backend_restores_manager_and_shows_error(self) -> None:
        source = WEB_LAUNCHER.read_text(encoding="utf-8")
        self.assertIn("self.root.deiconify()", source)
        self.assertIn('messagebox.showerror("启动失败", error, parent=self.root)', source)
        self.assertIn("请检查端口占用或运行目录权限", source)


if __name__ == "__main__":
    unittest.main()
