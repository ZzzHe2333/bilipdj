"""Allow the Douyin relay to use saved IDs when page parsing is incomplete.

Interactive parameter fetching remains strict. Lenient parsing is enabled only
inside a relay call that already has both a saved room id and a saved user id.
"""
from __future__ import annotations

import functools
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()
_THREAD_STATE = threading.local()

_ROOM_ID_PATTERNS = [
    r'\\"roomId\\":\\"(?P<value>\d+)\\"',
    r'"roomId"\s*:\s*"(?P<value>\d+)"',
    r'"room_id"\s*:\s*"(?P<value>\d+)"',
    r'\\"web_rid\\":\\"(?P<value>\d+)\\"',
    r'"web_rid"\s*:\s*"(?P<value>\d+)"',
]
_USER_ID_PATTERNS = [
    r'\\"user_unique_id\\":\\"(?P<value>\d+)\\"',
    r'"user_unique_id"\s*:\s*"(?P<value>\d+)"',
    r'"userUniqueId"\s*:\s*"(?P<value>\d+)"',
]
_ROOM_STATUS_PATTERNS = [
    r'"room_status"\s*:\s*"?(?P<value>\d+)"?',
    r'\\"liveStatus\\":\\"(?P<value>[^\"]+)\\"',
    r'"liveStatus"\s*:\s*"?(?P<value>[^\",}]+)"?',
    r'\\"live_status\\":\\"(?P<value>[^\"]+)\\"',
    r'"live_status"\s*:\s*"?(?P<value>[^\",}]+)"?',
    r'\\"room\\":\{[^}]{0,400}\\"status\\":(?P<value>\d+)[,}]',
    r'"room"\s*:\s*\{[^}]{0,400}"status"\s*:\s*(?P<value>\d+)[,}]',
]


def _saved_ids_available(cfg: Any) -> bool:
    if not isinstance(cfg, dict):
        return False
    room_id = str(cfg.get("preset_room_id", "") or "").strip()
    user_id = str(
        cfg.get("preset_user_unique_id", "")
        or cfg.get("preset_user_id", "")
        or ""
    ).strip()
    return bool(room_id and room_id != "0" and user_id and user_id != "0")


def patch_douyin_module(module: Any) -> bool:
    """Patch one loaded ``douyin_protocol`` module, idempotently."""

    if module is None:
        return False
    relay_class = getattr(module, "DouyinDanmuRelay", None)
    live_info_class = getattr(module, "DouyinLiveInfo", None)
    protocol_error = getattr(module, "DouyinProtocolError", None)
    search_patterns = getattr(module, "_search_patterns", None)
    original_parser = getattr(module, "parse_douyin_live_info_html", None)
    if (
        not isinstance(relay_class, type)
        or not isinstance(live_info_class, type)
        or not isinstance(protocol_error, type)
        or not callable(search_patterns)
        or not callable(original_parser)
    ):
        return False

    with _PATCH_LOCK:
        if bool(getattr(relay_class, "_bilipdj_preset_fallback_installed", False)):
            return True
        original_connect = getattr(relay_class, "_connect_and_stream", None)
        if not callable(original_connect):
            return False

        @functools.wraps(original_parser)
        def parser_with_relay_fallback(html_text: str, live_id: str):
            try:
                return original_parser(html_text, live_id)
            except protocol_error:
                if not bool(getattr(_THREAD_STATE, "allow_partial", False)):
                    raise
                room_id = str(search_patterns(html_text, _ROOM_ID_PATTERNS) or "")
                user_id = str(search_patterns(html_text, _USER_ID_PATTERNS) or "")
                room_status = str(
                    search_patterns(html_text, _ROOM_STATUS_PATTERNS) or ""
                )
                return live_info_class(
                    live_id=str(live_id),
                    room_id=room_id,
                    user_id=user_id,
                    user_unique_id=user_id,
                    room_status=room_status,
                    raw_html=html_text,
                )

        @functools.wraps(original_connect)
        def connect_with_saved_id_fallback(self: Any, *args: Any, **kwargs: Any):
            try:
                cfg = self._load_runtime_cfg()
            except Exception:
                cfg = {}
            if not _saved_ids_available(cfg):
                return original_connect(self, *args, **kwargs)

            previous = bool(getattr(_THREAD_STATE, "allow_partial", False))
            _THREAD_STATE.allow_partial = True
            try:
                return original_connect(self, *args, **kwargs)
            finally:
                _THREAD_STATE.allow_partial = previous

        setattr(module, "parse_douyin_live_info_html", parser_with_relay_fallback)
        setattr(relay_class, "_connect_and_stream", connect_with_saved_id_fallback)
        setattr(relay_class, "_bilipdj_preset_fallback_installed", True)
        return True


__all__ = ["patch_douyin_module"]
