from __future__ import annotations

import unittest
from pathlib import Path

from core.gui_log_sink import _is_error_message
from core.log_manager import DEFAULT_RETENTION_DAYS


ROOT = Path(__file__).resolve().parents[1]


class UpdateFeaturePackTests(unittest.TestCase):
    def test_update_page_is_separate_from_about(self) -> None:
        source = (ROOT / "core" / "control_panel_features.py").read_text(encoding="utf-8")
        page_source = (ROOT / "core" / "update_page.py").read_text(encoding="utf-8")
        self.assertIn('text="更新软件"', source)
        self.assertIn("build_update_tab", source)
        about = page_source.split("def build_about_tab", 1)[1]
        self.assertNotIn("下载并安装", about)
        self.assertNotIn("检查更新", about)

    def test_proxy_modes_default_off_and_tun_note_is_present(self) -> None:
        source = (ROOT / "core" / "update_network.py").read_text(encoding="utf-8")
        page = (ROOT / "core" / "update_page.py").read_text(encoding="utf-8")
        self.assertIn('"bypass_system_proxy": False', source)
        self.assertIn('"use_third_party_proxy": False', source)
        self.assertIn("TUN 模式", page)
        self.assertIn("暂未接入", page)

    def test_retention_defaults_to_31_days(self) -> None:
        self.assertEqual(DEFAULT_RETENTION_DAYS, 31)
        source = (ROOT / "core" / "control_panel_ui_finish.py").read_text(encoding="utf-8")
        self.assertIn('retention_var.set("31")', source)

    def test_warning_is_not_misclassified_as_error(self) -> None:
        self.assertFalse(_is_error_message("[GUI] 本软件完全免费", True))
        self.assertFalse(_is_error_message("普通警告提示", True))
        self.assertTrue(_is_error_message("保存失败：拒绝访问", False))
        self.assertTrue(_is_error_message("Traceback: test", False))

    def test_style_transport_uses_api_then_local_fallback(self) -> None:
        source = (ROOT / "core" / "style_save_transport.py").read_text(encoding="utf-8")
        self.assertLess(source.index("urllib.request.urlopen"), source.index("backend.save_style"))
        self.assertIn("saved_through_backend", source)
        self.assertIn('name="bilipdj-style-save"', source)

    def test_overlay_refresh_has_no_eager_tk_import(self) -> None:
        source = (ROOT / "core" / "overlay_refresh_guard.py").read_text(encoding="utf-8")
        prefix = source.split("def _create_font", 1)[0]
        self.assertNotIn("import tkinter", prefix)
        self.assertIn("from tkinter import font as tkfont", source)

    def test_regenerated_web_css_preserves_one_row_layout(self) -> None:
        source = (ROOT / "core" / "web_queue_layout.py").read_text(encoding="utf-8")
        script = (ROOT / "core" / "ui" / "myjs.js").read_text(encoding="utf-8")
        self.assertIn("44px minmax(0, 1fr)", source)
        self.assertIn('"minmax(0, 1fr)"', script)
        self.assertIn(".vText { display: none", source)

    def test_packaging_specs_register_dynamic_modules(self) -> None:
        for name in ("bilipdj_onedir.spec", "bilipdj_onedir_mac.spec"):
            source = (ROOT / name).read_text(encoding="utf-8")
            for module in (
                "core.update_page",
                "core.update_network",
                "core.log_manager",
                "core.style_save_transport",
                "core.web_queue_layout",
                "core.overlay_performance_guard",
            ):
                self.assertIn(f'"{module}"', source)


if __name__ == "__main__":
    unittest.main()
