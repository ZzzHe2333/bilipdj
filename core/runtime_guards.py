"""Small process-wide guards that do not require GUI or network access."""
from __future__ import annotations

import builtins
import functools
import json
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path
from typing import Any, Callable

_GUARD_LOCK = threading.RLock()
_CLEANUP_TARGETS: set[str] = set()
_OVERLAY_IMPORT_ORIGINAL: Callable[..., Any] | None = None
_OVERLAY_IMPORT_WRAPPER: Callable[..., Any] | None = None
_OVERLAY_IMPORT_TIMER: threading.Timer | None = None


def install_openai_protocol_guard(module: Any | None = None) -> bool:
    """Keep a custom client's selected API protocol when only its model changes."""

    if module is None:
        from . import openai_api as module

    client_class = getattr(module, "OpenAIAnswerClient", None)
    if not isinstance(client_class, type):
        return False
    original = getattr(client_class, "ask", None)
    if not callable(original):
        return False
    if bool(getattr(original, "_bilipdj_custom_style_guard", False)):
        return True

    @functools.wraps(original)
    def ask_with_sticky_custom_style(
        self: Any,
        prompt: str,
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        if (
            str(getattr(self, "provider", "")) == "custom"
            and kwargs.get("model") is not None
            and kwargs.get("api_style") is None
        ):
            kwargs["api_style"] = getattr(self, "api_style", None)
        return original(self, prompt, *args, **kwargs)

    setattr(ask_with_sticky_custom_style, "_bilipdj_custom_style_guard", True)
    setattr(client_class, "ask", ask_with_sticky_custom_style)
    return True


def _restore_overlay_import_guard() -> None:
    global _OVERLAY_IMPORT_ORIGINAL, _OVERLAY_IMPORT_WRAPPER, _OVERLAY_IMPORT_TIMER
    with _GUARD_LOCK:
        wrapper = _OVERLAY_IMPORT_WRAPPER
        original = _OVERLAY_IMPORT_ORIGINAL
        if wrapper is not None and original is not None and builtins.__import__ is wrapper:
            builtins.__import__ = original
        timer = _OVERLAY_IMPORT_TIMER
        _OVERLAY_IMPORT_ORIGINAL = None
        _OVERLAY_IMPORT_WRAPPER = None
        _OVERLAY_IMPORT_TIMER = None
        if timer is not None and timer is not threading.current_thread():
            timer.cancel()


def install_overlay_import_guard(*, timeout: float = 120.0) -> bool:
    """Patch ``core.overlay_host`` after import and before its caller gets control."""

    global _OVERLAY_IMPORT_ORIGINAL, _OVERLAY_IMPORT_WRAPPER, _OVERLAY_IMPORT_TIMER
    from .overlay_refresh_guard import patch_overlay_module

    for module_name in ("core.overlay_host", "overlay_host"):
        module = sys.modules.get(module_name)
        if module is not None and patch_overlay_module(module):
            return True

    with _GUARD_LOCK:
        if _OVERLAY_IMPORT_WRAPPER is not None:
            return True
        current_import: Callable[..., Any] = builtins.__import__

        @functools.wraps(current_import)
        def guarded_import(
            name: str,
            globals: dict[str, Any] | None = None,
            locals: dict[str, Any] | None = None,
            fromlist: tuple[str, ...] | list[str] = (),
            level: int = 0,
        ) -> Any:
            imported = current_import(name, globals, locals, fromlist, level)
            for module_name in ("core.overlay_host", "overlay_host"):
                module = sys.modules.get(module_name)
                if module is not None and patch_overlay_module(module):
                    _restore_overlay_import_guard()
                    break
            return imported

        _OVERLAY_IMPORT_ORIGINAL = current_import
        _OVERLAY_IMPORT_WRAPPER = guarded_import
        builtins.__import__ = guarded_import
        timer = threading.Timer(
            max(1.0, float(timeout)),
            _restore_overlay_import_guard,
        )
        timer.daemon = True
        _OVERLAY_IMPORT_TIMER = timer
        timer.start()
        return True


def _application_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def _validated_cleanup_target(app_dir: Path | None = None) -> Path | None:
    root = Path(app_dir) if app_dir is not None else _application_dir()
    marker = root / "update-result.json"
    try:
        payload = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None

    raw_target = str(payload.get("cleanup_dir", "") or "").strip()
    if not raw_target:
        return None
    target = Path(raw_target).resolve(strict=False)
    temp_root = Path(tempfile.gettempdir()).resolve(strict=False)
    if target.parent != temp_root:
        return None
    if not target.name.startswith("bilipdj-update-"):
        return None
    if target == target.parent or target == root.resolve(strict=False):
        return None
    return target


def cleanup_update_residue(
    app_dir: Path | None = None,
    *,
    attempts: int = 20,
    retry_delay: float = 1.0,
) -> bool:
    """Delete the exact update work directory recorded by the updater."""

    target = _validated_cleanup_target(app_dir)
    if target is None:
        return False

    for attempt in range(max(1, int(attempts))):
        try:
            if target.is_symlink() or target.is_file():
                target.unlink(missing_ok=True)
            elif target.exists():
                shutil.rmtree(target)
            return not target.exists()
        except OSError:
            if attempt + 1 >= max(1, int(attempts)):
                return False
            time.sleep(max(0.01, float(retry_delay)))
    return not target.exists()


def schedule_update_residue_cleanup(
    app_dir: Path | None = None,
    *,
    initial_delay: float = 12.0,
) -> bool:
    """Schedule cleanup after the detached updater has had time to exit."""

    target = _validated_cleanup_target(app_dir)
    if target is None:
        return False
    key = os.path.normcase(str(target))
    with _GUARD_LOCK:
        if key in _CLEANUP_TARGETS:
            return False
        _CLEANUP_TARGETS.add(key)

    def worker() -> None:
        try:
            time.sleep(max(0.0, float(initial_delay)))
            cleanup_update_residue(app_dir)
        finally:
            with _GUARD_LOCK:
                _CLEANUP_TARGETS.discard(key)

    threading.Thread(
        target=worker,
        name="bilipdj-update-residue-cleanup",
        daemon=True,
    ).start()
    return True


def install_runtime_guards() -> None:
    if os.environ.get("BILIPDJ_DISABLE_RUNTIME_GUARDS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return
    for installer in (
        install_openai_protocol_guard,
        install_overlay_import_guard,
        schedule_update_residue_cleanup,
    ):
        try:
            installer()
        except Exception:
            pass


__all__ = [
    "cleanup_update_residue",
    "install_openai_protocol_guard",
    "install_overlay_import_guard",
    "install_runtime_guards",
    "schedule_update_residue_cleanup",
]
