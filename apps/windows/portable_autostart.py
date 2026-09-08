from __future__ import annotations

import functools
import os
from pathlib import Path
from typing import Any

from .version import APP_VERSION

PORTABLE_AUTO_BACKEND_ENV = "BILIPDJ_PORTABLE_AUTO_BACKEND"


def _portable_autostart_enabled() -> bool:
    return os.getenv(PORTABLE_AUTO_BACKEND_ENV, "").strip().lower() in {"1", "true", "yes", "on"}


def patch_control_panel_portable_autostart(panel_class: type[Any]) -> bool:
    """Force backend auto-start only for packaged portable desktop builds."""
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    # Keep every desktop surface on the canonical VERSION file, including the
    # main title and update/about pages that still read module.APP_VERSION.
    module.APP_VERSION = APP_VERSION

    if bool(getattr(panel_class, "_bilipdj_portable_autostart_installed", False)):
        return True
    original_load = getattr(panel_class, "load_from_file", None)
    if not callable(original_load):
        return False

    @functools.wraps(original_load)
    def load_with_portable_autostart(self: Any, *args: Any, **kwargs: Any) -> Any:
        result = original_load(self, *args, **kwargs)
        if _portable_autostart_enabled() and hasattr(self, "auto_start_var"):
            self.auto_start_var.set(True)
        return result

    setattr(panel_class, "load_from_file", load_with_portable_autostart)
    setattr(panel_class, "_bilipdj_portable_autostart_installed", True)
    return True


__all__ = [
    "PORTABLE_AUTO_BACKEND_ENV",
    "patch_control_panel_portable_autostart",
]
