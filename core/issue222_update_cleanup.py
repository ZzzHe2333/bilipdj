from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

from apps.update_workspace import cleanup_update_session, validate_update_session

_LOCK = threading.RLock()
_ACTIVE: set[str] = set()


def _application_dir() -> Path:
    import sys

    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def _cleanup_target(app_dir: Path) -> Path | None:
    root = Path(app_dir).resolve()
    for marker in (root / "key" / "update-result.json", root / "update-result.json"):
        try:
            payload = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, TypeError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        raw = str(payload.get("cleanup_dir", "") or "").strip()
        if not raw:
            continue
        try:
            return validate_update_session(root, Path(raw))
        except ValueError:
            continue
    return None


def schedule_local_update_cleanup(
    app_dir: Path | None = None,
    *,
    initial_delay: float = 12.0,
    attempts: int = 90,
    retry_delay: float = 1.0,
) -> bool:
    root = Path(app_dir).resolve() if app_dir is not None else _application_dir()
    target = _cleanup_target(root)
    if target is None:
        return False
    key = os.path.normcase(str(target))
    with _LOCK:
        if key in _ACTIVE:
            return False
        _ACTIVE.add(key)

    def worker() -> None:
        try:
            time.sleep(max(0.0, float(initial_delay)))
            for index in range(max(1, int(attempts))):
                if cleanup_update_session(root, target):
                    break
                if index + 1 < max(1, int(attempts)):
                    time.sleep(max(0.05, float(retry_delay)))
        finally:
            with _LOCK:
                _ACTIVE.discard(key)

    threading.Thread(target=worker, name="bilipdj-local-update-cleanup", daemon=True).start()
    return True


__all__ = ["schedule_local_update_cleanup"]
