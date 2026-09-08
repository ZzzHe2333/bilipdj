from __future__ import annotations

import json
import re
import shutil
import subprocess
import unittest
from pathlib import Path

import qrcode

ROOT = Path(__file__).resolve().parents[1]
PROMO_URL = "https://m.sdyuntuo.cn/ProductEn/Index/01929ef4362a8858"
PROMO_COPY = "项目免费开源使用，申请流量卡给开发者回回血。"
PROMO_TEXT = "【打扰一下】如果有需要正规大流量电话卡的，可以点击 支持我们-申请流量卡，自助申请哦。你的每张正常申请使用，都能给本项目带来持续的支持！"


class WindowsSupportUsTests(unittest.TestCase):
    def test_windows_has_support_page_and_application_button(self) -> None:
        source = (ROOT / "apps" / "windows" / "support_us.py").read_text(encoding="utf-8")
        self.assertIn(PROMO_URL, source)
        self.assertIn(PROMO_COPY, source)
        self.assertIn('text="支持我们"', source)
        self.assertIn('text="申请流量卡"', source)
        self.assertIn("qrcode.QRCode", source)
        self.assertIn("webbrowser.open(SUPPORT_URL)", source)

    def test_windows_guanggao_is_random_and_ui_only(self) -> None:
        source = (ROOT / "apps" / "windows" / "support_us.py").read_text(encoding="utf-8")
        self.assertIn('GUANGGAO_LEVEL = "GUANGGAO"', source)
        self.assertIn(PROMO_TEXT, source)
        self.assertIn("random.randint", source)
        self.assertIn("_render_log_record", source)
        self.assertNotIn("_append_log(", source)
        self.assertNotIn("log_manager", source)
        self.assertNotIn("logging.", source)

    def test_windows_bootstrap_and_portable_specs_include_support_module(self) -> None:
        bootstrap = (ROOT / "apps" / "windows" / "control_panel_bootstrap.py").read_text(encoding="utf-8")
        app_spec = (ROOT / "apps" / "windows" / "bilipdj_onedir.spec").read_text(encoding="utf-8")
        root_spec = (ROOT / "bilipdj_onedir.spec").read_text(encoding="utf-8")
        self.assertIn("from .support_us import patch_control_panel_support_us", bootstrap)
        self.assertIn("patch_control_panel_support_us(cls)", bootstrap)
        self.assertIn('"apps.windows.support_us"', app_spec)
        self.assertIn('"apps.windows.support_us"', root_spec)


class WebSupportUsTests(unittest.TestCase):
    def test_web_extension_loads_before_control_bindings(self) -> None:
        html = (ROOT / "apps" / "web" / "static" / "control.html").read_text(encoding="utf-8")
        self.assertIn('<script src="support_us.js"></script>', html)
        self.assertLess(html.index('src="support_us.js"'), html.index('src="control.js"'))

    def test_web_support_page_and_guanggao_content(self) -> None:
        source = (ROOT / "apps" / "web" / "static" / "support_us.js").read_text(encoding="utf-8")
        self.assertIn(PROMO_URL, source)
        self.assertIn(PROMO_COPY, source)
        self.assertIn(PROMO_TEXT, source)
        self.assertIn("button.textContent = '支持我们'", source)
        self.assertIn('id="support-apply"', source)
        self.assertIn('>申请流量卡</a>', source)
        self.assertIn("const GUANGGAO_LEVEL = 'GUANGGAO'", source)
        self.assertIn("Math.random()", source)
        self.assertIn("window.setTimeout", source)
        self.assertNotIn("fetch(", source)
        self.assertNotIn("/api/", source)

    def test_web_qr_matrix_matches_promotion_url(self) -> None:
        source = (ROOT / "apps" / "web" / "static" / "support_us.js").read_text(encoding="utf-8")
        match = re.search(r"const QR_MATRIX = (\[[\s\S]*?\]);", source)
        self.assertIsNotNone(match)
        actual_rows = json.loads(match.group(1))

        qr = qrcode.QRCode(
            error_correction=qrcode.constants.ERROR_CORRECT_M,
            box_size=1,
            border=4,
        )
        qr.add_data(PROMO_URL)
        qr.make(fit=True)
        expected_rows = [
            "".join("1" if cell else "0" for cell in row)
            for row in qr.get_matrix()
        ]
        self.assertEqual(actual_rows, expected_rows)

    def test_support_javascript_has_valid_syntax_when_node_is_available(self) -> None:
        node = shutil.which("node")
        if node is None:
            self.skipTest("node is not installed")
        script = ROOT / "apps" / "web" / "static" / "support_us.js"
        result = subprocess.run([node, "--check", str(script)], capture_output=True, text=True, check=False)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_web_build_requires_support_extension(self) -> None:
        build = (ROOT / "apps" / "web" / "build.py").read_text(encoding="utf-8")
        self.assertIn('"support_us.js"', build)


if __name__ == "__main__":
    unittest.main()
