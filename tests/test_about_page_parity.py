from __future__ import annotations

import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
WINDOWS_ABOUT = ROOT / "apps" / "windows" / "about_page.py"
WINDOWS_BOOTSTRAP = ROOT / "apps" / "windows" / "control_panel_bootstrap.py"
WINDOWS_MAIN = ROOT / "apps" / "windows" / "main.py"
WEB_ABOUT = ROOT / "apps" / "web" / "static" / "support_us.js"

COMMON_COPY = (
    "Bilibili 直播弹幕排队管理工具",
    "排队逻辑由 Python 后端统一处理，前端仅负责显示。",
    "本软件完全免费，源码公开，Github Action自动打包，无后台无病毒，不损害电脑。若有人向你收费获取此软件（亲手帮安装调试除外），请立刻退款并举报！",
    "【侵权/倒卖责任】",
    "• 民事责任：侵权方须停止侵权、赔偿损失（含维权合理费用）。",
    "• 刑事责任：以营利为目的的侵权行为，情节严重时可能被追究刑事责任。",
)


class AboutPageParityTests(unittest.TestCase):
    def test_windows_and_web_share_requested_copy(self) -> None:
        windows = WINDOWS_ABOUT.read_text(encoding="utf-8")
        web = WEB_ABOUT.read_text(encoding="utf-8")
        for line in COMMON_COPY:
            self.assertIn(line, windows)
            self.assertIn(line, web)

    def test_each_frontend_identifies_its_client_type(self) -> None:
        windows = WINDOWS_ABOUT.read_text(encoding="utf-8")
        web = WEB_ABOUT.read_text(encoding="utf-8")
        self.assertIn("您的版本是：Windows版", windows)
        self.assertIn("您的版本是：</strong>网页版", web)

    def test_version_is_dynamic_on_both_frontends(self) -> None:
        windows = WINDOWS_ABOUT.read_text(encoding="utf-8")
        web = WEB_ABOUT.read_text(encoding="utf-8")
        self.assertIn("APP_VERSION", windows)
        self.assertIn("当前版本号：{APP_VERSION}", windows)
        self.assertIn('id="about-version"', web)
        self.assertIn("document.getElementById('version')", web)
        self.assertIn("MutationObserver(syncVersion)", web)
        self.assertNotIn("fetch(", web)
        self.assertNotIn("/api/", web)
        self.assertNotIn("当前版本号：2.0.2", windows)
        self.assertNotIn("当前版本号：2.0.2", web)

    def test_windows_entry_points_install_about_after_feature_patch(self) -> None:
        bootstrap = WINDOWS_BOOTSTRAP.read_text(encoding="utf-8")
        main = WINDOWS_MAIN.read_text(encoding="utf-8")
        self.assertIn("from .about_page import patch_control_panel_about", bootstrap)
        self.assertLess(bootstrap.index("patch_control_panel_features(cls)"), bootstrap.index("patch_control_panel_about(cls)"))
        self.assertIn("from apps.windows.about_page import patch_control_panel_about", main)
        self.assertIn("patch_control_panel_about(control_panel.ControlPanelApp)", main)

    def test_web_extension_has_valid_javascript_when_node_is_available(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is not installed")
        result = subprocess.run(
            [node, "--check", str(WEB_ABOUT)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
