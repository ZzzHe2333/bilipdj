from __future__ import annotations

import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PortableReleaseV201Tests(unittest.TestCase):
    def test_version_is_v201(self) -> None:
        self.assertEqual((ROOT / "VERSION").read_text(encoding="utf-8").strip(), "2.0.1")

    def test_tk_portable_forces_backend_autostart_when_frozen(self) -> None:
        main_source = (ROOT / "apps" / "windows" / "main.py").read_text(encoding="utf-8")
        patch_source = (ROOT / "apps" / "windows" / "portable_autostart.py").read_text(encoding="utf-8")
        bootstrap_source = (ROOT / "apps" / "windows" / "control_panel_bootstrap.py").read_text(encoding="utf-8")
        self.assertIn("BILIPDJ_PORTABLE_AUTO_BACKEND", main_source)
        self.assertIn("getattr(sys, \"frozen\", False)", main_source)
        self.assertIn("self.auto_start_var.set(True)", patch_source)
        self.assertIn("patch_control_panel_portable_autostart", bootstrap_source)
        self.assertIn("module.APP_VERSION = APP_VERSION", patch_source)

    def test_web_portable_self_hosts_backend_and_opens_browser(self) -> None:
        source = (ROOT / "apps" / "web" / "portable_launcher.py").read_text(encoding="utf-8")
        self.assertIn('"--backend"', source)
        self.assertIn("subprocess.Popen", source)
        self.assertIn("server_main.main([])", source)
        self.assertIn("/health", source)
        self.assertIn("webbrowser.open", source)
        self.assertIn("process.terminate()", source)

    def test_web_portable_spec_bundles_server_and_web_assets(self) -> None:
        source = (ROOT / "apps" / "web" / "web_portable.spec").read_text(encoding="utf-8")
        self.assertIn('"apps.server.server"', source)
        self.assertIn('"apps/web/static"', source)
        self.assertIn('name="BiliPDJ-Web"', source)
        self.assertIn('name="bilipdj-web"', source)

    def test_windows_spec_bundles_portable_autostart(self) -> None:
        source = (ROOT / "apps" / "windows" / "bilipdj_onedir.spec").read_text(encoding="utf-8")
        self.assertIn('"apps.windows.portable_autostart"', source)
        self.assertIn('"apps.server.server"', source)
        self.assertIn('"apps/web/static"', source)

    def test_release_workflow_emits_both_customer_bundles(self) -> None:
        source = (ROOT / ".github" / "workflows" / "package-windows-x64.yml").read_text(encoding="utf-8")
        self.assertIn("BiliPDJ-v$VERSION-Windows-Tk-Portable-x64.zip", source)
        self.assertIn("BiliPDJ-v$VERSION-Web-Portable-x64.zip", source)
        self.assertIn("apps\\windows\\package.ps1", source)
        self.assertIn("apps\\web\\package-portable.ps1", source)
        self.assertIn("Create GitHub Release", source)


if __name__ == "__main__":
    unittest.main()
