from __future__ import annotations

import sys
import types
import unittest

from core.style_option_guard import (
    DISPLAY_STYLE_DEFAULTS,
    install_style_persistence_guard,
    patch_style_module,
)


class StyleOptionGuardTests(unittest.TestCase):
    def _build_style_module(self, name: str):
        module = types.ModuleType(name)
        module.DEFAULT_STYLE = {"bg1": "old"}
        module.DEFAULT_CONFIG = {"style": {"bg1": "old"}}
        module.saved = None
        module.current = {
            "bg1": "old",
            "auto_scroll": True,
            "show_sequence": True,
            "future_option": "keep-me",
        }

        def load_style():
            return dict(module.current)

        def save_style(data):
            module.saved = dict(data)
            module.current = dict(data)

        module.load_style = load_style
        module.save_style = save_style
        sys.modules[name] = module
        return module

    def test_patch_adds_defaults_and_preserves_unknown_fields(self) -> None:
        name = "style_guard_test_module"
        module = self._build_style_module(name)
        try:
            self.assertTrue(patch_style_module(module))
            self.assertEqual(
                {key: module.DEFAULT_STYLE[key] for key in DISPLAY_STYLE_DEFAULTS},
                DISPLAY_STYLE_DEFAULTS,
            )
            module.save_style({"bg1": "new"})
            self.assertEqual(module.saved["bg1"], "new")
            self.assertTrue(module.saved["auto_scroll"])
            self.assertTrue(module.saved["show_sequence"])
            self.assertEqual(module.saved["future_option"], "keep-me")
        finally:
            sys.modules.pop(name, None)

    def test_queue_manager_constructor_installs_patch_after_module_load(self) -> None:
        name = "style_guard_queue_module"
        module = self._build_style_module(name)
        try:
            class QueueManager:
                def __init__(self):
                    self.ready = True

            QueueManager.__module__ = name
            module.QueueManager = QueueManager
            self.assertTrue(install_style_persistence_guard(QueueManager))
            manager = QueueManager()
            self.assertTrue(manager.ready)
            self.assertTrue(
                getattr(module.save_style, "_bilipdj_preserves_style_options", False)
            )
        finally:
            sys.modules.pop(name, None)


if __name__ == "__main__":
    unittest.main()
