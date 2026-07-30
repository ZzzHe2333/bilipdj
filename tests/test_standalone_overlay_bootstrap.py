from __future__ import annotations

import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class StandaloneOverlayBootstrapTests(unittest.TestCase):
    def test_overlay_specs_install_runtime_hook(self) -> None:
        for name in ("paiduijitm.spec", "paiduijitm_mac.spec"):
            source = (ROOT / name).read_text(encoding="utf-8")
            self.assertIn("pyi_overlay_runtime_hook.py", source)
            self.assertIn('"core.overlay_bootstrap"', source)
            self.assertIn('"core.overlay_performance_guard"', source)

    def test_runtime_hook_installs_class_patch(self) -> None:
        source = (ROOT / "core" / "pyi_overlay_runtime_hook.py").read_text(encoding="utf-8")
        bootstrap = (ROOT / "core" / "overlay_bootstrap.py").read_text(encoding="utf-8")
        self.assertIn("install_overlay_class_hook()", source)
        self.assertIn('name != "OverlayHostApp"', bootstrap)
        self.assertIn("patch_overlay_module(module)", bootstrap)


if __name__ == "__main__":
    unittest.main()
