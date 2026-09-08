from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from scripts import generate_api_docs

ROOT = Path(__file__).resolve().parents[1]
STATIC = ROOT / "apps" / "web" / "static"


class WebControlPanelTests(unittest.TestCase):
    def test_control_panel_exposes_requested_sections(self) -> None:
        html = (STATIC / "control.html").read_text(encoding="utf-8")
        for label in ("运行日志", "当前排队", "设置", "透明窗口 / OBS", "权限", "性能", "更新软件", "关于"):
            self.assertIn(label, html)
        self.assertIn('src="/config"', html)
        self.assertIn('src="/index"', html)

    def test_control_script_reuses_server_management_apis(self) -> None:
        source = (STATIC / "control.js").read_text(encoding="utf-8")
        for route in (
            "/api/control/logs",
            "/api/control/performance",
            "/api/control/update",
            "/api/queue/state",
            "/api/queue/delete",
            "/api/queue/move",
            "/api/queue/insert",
            "/api/queue/update",
            "/api/queue/clear",
            "/api/quanxian",
            "/api/kaiguan",
            "/api/blacklist/state",
            "/api/style",
            "/api/config",
        ):
            self.assertIn(route, source)

    def test_control_backend_is_local_only_and_root_maps_to_control(self) -> None:
        source = (ROOT / "apps" / "server" / "web_control_guard.py").read_text(encoding="utf-8")
        self.assertIn('{"/", "/control", "/control/", "/control.html"}', source)
        self.assertIn("self._require_loopback()", source)
        self.assertIn('"/api/control/logs"', source)
        self.assertIn('"/api/control/performance"', source)
        self.assertIn('"/api/control/update"', source)

    def test_web_portable_opens_control_panel_by_default(self) -> None:
        source = (ROOT / "apps" / "web" / "portable_launcher.py").read_text(encoding="utf-8")
        self.assertIn('webbrowser.open(f"http://127.0.0.1:{self.port}/control")', source)
        self.assertIn('text="打开 Web 控制台"', source)


class GeneratedApiReferenceTests(unittest.TestCase):
    def test_api_directory_is_gitignored(self) -> None:
        lines = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
        self.assertIn("/api/", lines)

    def test_every_server_api_route_is_registered(self) -> None:
        gap = generate_api_docs.coverage_gap(ROOT)
        self.assertEqual(gap, set(), f"undocumented server routes: {sorted(gap)}")

    def test_generator_creates_ignored_reference_with_examples(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            target = generate_api_docs.generate(ROOT, Path(temp_dir) / "api")
            readme = (target / "README.md").read_text(encoding="utf-8")
            http = (target / "http.md").read_text(encoding="utf-8")
            websocket = (target / "websocket.md").read_text(encoding="utf-8")
            self.assertIn("被仓库根 `.gitignore`", readme)
            self.assertIn("GET /api/queue/state", http)
            self.assertIn("POST /api/bili/qr/poll", http)
            self.assertIn("POST /api/backup/settings/run", http)
            self.assertIn("GET /api/control/logs", http)
            self.assertIn("curl", http)
            self.assertIn("WS /ws", websocket)
            self.assertIn("WS /danmu/sub", websocket)


if __name__ == "__main__":
    unittest.main()
