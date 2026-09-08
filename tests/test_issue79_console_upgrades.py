from __future__ import annotations

import importlib.util
import json
import shutil
import subprocess
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
WEB_JS = ROOT / "apps" / "web" / "static" / "control_issue79.js"
WEB_CSS = ROOT / "apps" / "web" / "static" / "control_issue79.css"
LAUNCHER = ROOT / "apps" / "web" / "portable_launcher.py"
WINDOWS_FEATURES = ROOT / "apps" / "windows" / "issue79_features.py"
UPDATE_CLIENT = ROOT / "apps" / "windows" / "update_client.py"
MANIFEST = ROOT / "update-manifest.json"
GUARD = ROOT / "apps" / "server" / "issue79_guard.py"


def load_guard():
    name = "issue79_guard_under_test"
    spec = importlib.util.spec_from_file_location(name, GUARD)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


class Issue79StaticContractTests(unittest.TestCase):
    def test_update_manifest_contains_exact_packages_and_sha256(self) -> None:
        payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
        self.assertEqual(payload["schema"], 1)
        packages = payload["packages"]
        for key in ("windows-tk-x64", "web-portable-x64"):
            package = packages[key]
            self.assertTrue(package["filename"].endswith(".zip"))
            self.assertTrue(package["url"].startswith("https://github.com/"))
            self.assertRegex(package["sha256"], r"^[0-9a-f]{64}$")
            self.assertGreater(int(package["size"]), 0)

    def test_windows_updater_uses_manifest_before_download(self) -> None:
        source = UPDATE_CLIENT.read_text(encoding="utf-8")
        self.assertIn("LATEST_MANIFEST_URL", source)
        self.assertIn("WINDOWS_PACKAGE_KEY = \"windows-tk-x64\"", source)
        self.assertIn("_release_from_manifest", source)
        self.assertIn("verify_sha256", source)
        self.assertIn("Windows-Tk-Portable-x64", source)

    def test_web_has_visual_style_editor_theme_and_two_field_queue(self) -> None:
        source = WEB_JS.read_text(encoding="utf-8")
        css = WEB_CSS.read_text(encoding="utf-8")
        for token in (
            "queue-username",
            "填写用户名（必填）",
            "填写排队内容（可选）",
            "settings-active-platforms",
            "/api/platforms/active",
            "issue79-style-editor",
            "applyStylePreview",
            "settings-theme",
            "跟随系统",
            "白天模式",
            "夜晚模式",
            "关于项目",
            "Releases 发行包",
        ):
            self.assertIn(token, source)
        self.assertIn(':root[data-theme="light"]', css)
        self.assertIn(':root[data-theme="dark"]', css)

    def test_web_javascript_syntax(self) -> None:
        node = shutil.which("node")
        if not node:
            self.skipTest("node is unavailable")
        completed = subprocess.run(
            [node, "--check", str(WEB_JS)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_backend_launcher_is_server_system_with_tray_warning(self) -> None:
        source = LAUNCHER.read_text(encoding="utf-8")
        self.assertIn("后端服务器系统", source)
        self.assertIn("隐藏到右下角", source)
        self.assertIn("pystray", source)
        self.assertIn("关闭后端服务器后", source)
        self.assertIn("停止后端并退出", source)

    def test_windows_has_required_username_dialog_and_correct_navigation(self) -> None:
        source = WINDOWS_FEATURES.read_text(encoding="utf-8")
        self.assertIn("填写用户名", source)
        self.assertIn("填写排队内容（可选）", source)
        self.assertIn('7: "关于项目"', source)
        self.assertIn('8: "支持我们"', source)
        self.assertIn("激活平台", source)
        self.assertIn(REPO_URL := "https://github.com/ZzzHe2333/bilipdj", source)
        self.assertIn(f"{REPO_URL}/releases", source)


class Issue79RelayTests(unittest.TestCase):
    def setUp(self) -> None:
        self.guard = load_guard()

    def test_explicit_active_platforms_are_deduplicated_and_can_be_empty(self) -> None:
        module = SimpleNamespace(_get_runtime_platform=lambda cfg: cfg.get("platform", "bilibili"))
        self.assertEqual(
            self.guard._normalize_active_platforms(module, {"active_platforms": ["bilibili", "douyin", "bilibili"]}),
            ("bilibili", "douyin"),
        )
        self.assertEqual(self.guard._normalize_active_platforms(module, {"active_platforms": []}), ())
        self.assertEqual(self.guard._normalize_active_platforms(module, {"platform": "douyin"}), ("douyin",))

    def test_multi_platform_manager_gives_each_relay_its_own_runtime_view(self) -> None:
        created = []

        class Relay:
            def __init__(self, proxy):
                self.proxy = proxy
                self.started = False
                self.reconnects = 0

            def start(self):
                self.started = True

            def stop(self):
                return None

            def join(self, timeout=None):
                return timeout

            def request_reconnect(self):
                self.reconnects += 1

            def get_runtime_status(self):
                return {"connected": self.started, "platform": self.proxy.runtime_config["platform"]}

        def make_relay(proxy):
            relay = Relay(proxy)
            created.append(relay)
            return relay

        server = SimpleNamespace(
            runtime_config={
                "platform": "bilibili",
                "bilibili": {"roomid": 111},
                "douyin": {"live_id": "222", "enabled": True},
            },
            logger=SimpleNamespace(info=lambda *_args, **_kwargs: None),
        )
        manager = self.guard.MultiPlatformRelayManager(
            SimpleNamespace(_create_danmu_relay=make_relay),
            server,
            ("bilibili", "douyin"),
        )
        manager.start()
        self.assertEqual([item.proxy.runtime_config["platform"] for item in created], ["bilibili", "douyin"])
        self.assertTrue(all(item.started for item in created))
        status = manager.get_runtime_status()
        self.assertEqual(status["active_platforms"], ["bilibili", "douyin"])
        self.assertEqual(set(status["platforms"]), {"bilibili", "douyin"})

        server.runtime_config["bilibili"]["roomid"] = 333
        manager.request_reconnect()
        self.assertEqual(created[0].proxy.runtime_config["bilibili"]["roomid"], 333)
        self.assertEqual([item.reconnects for item in created], [1, 1])


if __name__ == "__main__":
    unittest.main()
