"""Keep same-named users from different platforms distinct inside the live queue.

Queue display text remains backward compatible. A parallel in-memory key list
tracks the platform user identifier for entries created by live danmu commands.
Archive files remain readable by older versions; restored legacy rows simply have
no platform key until they are claimed by a live user again.
"""
from __future__ import annotations

import functools
import sys
import threading
from typing import Any

if __package__:
    from .multi_platform_danmu import platform_user_key
else:
    from multi_platform_danmu import platform_user_key


def _sync_keys(self: Any) -> list[str | None]:
    keys = getattr(self, "_platform_queue_keys", None)
    if not isinstance(keys, list):
        keys = []
        self._platform_queue_keys = keys
    persons = getattr(self, "_persons", [])
    missing = len(persons) - len(keys)
    if missing > 0:
        keys.extend([None] * missing)
    elif missing < 0:
        del keys[len(persons):]
    return keys


def _context(self: Any) -> threading.local:
    local = getattr(self, "_platform_identity_context", None)
    if not isinstance(local, threading.local):
        local = threading.local()
        self._platform_identity_context = local
    return local


def _payload_identity(payload: Any) -> tuple[str, str, str]:
    if not isinstance(payload, dict):
        return "bilibili", "", ""
    platform = str(payload.get("_pdj_platform", "bilibili") or "bilibili").strip().lower()
    info = payload.get("info", [])
    uid = 0
    uname = ""
    if isinstance(info, list) and len(info) > 2 and isinstance(info[2], (list, tuple)):
        user = info[2]
        try:
            uid = int(user[0] or 0) if len(user) > 0 else 0
        except (TypeError, ValueError):
            uid = 0
        uname = str(user[1] or "") if len(user) > 1 else ""
    meta = payload.get("_pdj_identity", {})
    sec_uid = str(meta.get("sec_uid", "") or "") if isinstance(meta, dict) else ""
    key = platform_user_key(platform, uid=uid, uname=uname, sec_uid=sec_uid)
    return platform, uname, key


def _item_identity(server_module: Any, item: Any) -> str:
    try:
        item_id, _content = server_module.queue_item_to_parts(item)
        return str(item_id or "").strip().lstrip("@")
    except Exception:
        text = str(item or "").strip()
        return text.split(" ", 1)[0].strip().lstrip("@") if text else ""


def patch_queue_manager_platform_identity(queue_manager_cls: type[Any]) -> bool:
    if not isinstance(queue_manager_cls, type):
        return False
    if bool(getattr(queue_manager_cls, "_bilipdj_platform_queue_identity_installed", False)):
        return True
    server_module = sys.modules.get(str(getattr(queue_manager_cls, "__module__", "") or ""))
    if server_module is None:
        return False

    original_process_danmu = getattr(queue_manager_cls, "process_danmu_json", None)
    original_find = getattr(queue_manager_cls, "_find_index", None)
    original_append = getattr(queue_manager_cls, "_append_queue_item_unlocked", None)
    original_insert = getattr(queue_manager_cls, "_insert_queue_item_unlocked", None)
    original_remove = getattr(queue_manager_cls, "_remove_queue_item_unlocked", None)
    original_set_entries = getattr(queue_manager_cls, "_set_queue_from_entries_unlocked", None)
    original_get_entries = getattr(queue_manager_cls, "_get_queue_entries_unlocked", None)
    original_move = getattr(queue_manager_cls, "move_item", None)
    original_clear = getattr(queue_manager_cls, "clear_queue", None)
    required = (
        original_process_danmu,
        original_find,
        original_append,
        original_insert,
        original_remove,
        original_set_entries,
        original_get_entries,
    )
    if not all(callable(item) for item in required):
        return False

    @functools.wraps(original_process_danmu)
    def process_with_identity_context(self: Any, payload: dict[str, Any]) -> None:
        platform, uname, user_key = _payload_identity(payload)
        local = _context(self)
        previous = (
            getattr(local, "platform", None),
            getattr(local, "uname", None),
            getattr(local, "user_key", None),
        )
        local.platform = platform
        local.uname = uname
        local.user_key = user_key
        try:
            return original_process_danmu(self, payload)
        finally:
            local.platform, local.uname, local.user_key = previous

    @functools.wraps(original_find)
    def find_platform_user(self: Any, uname: str) -> int:
        target = str(uname or "").strip()
        local = _context(self)
        current_key = str(getattr(local, "user_key", "") or "")
        if not current_key:
            return int(original_find(self, uname))

        keys = _sync_keys(self)
        legacy_match = -1
        for index, item in enumerate(getattr(self, "_persons", [])):
            if _item_identity(server_module, item) != target:
                continue
            row_key = keys[index] if index < len(keys) else None
            if row_key == current_key:
                return index
            if row_key is None and legacy_match < 0:
                legacy_match = index
        if legacy_match >= 0:
            keys[legacy_match] = current_key
        return legacy_match

    def _key_for_item(self: Any, item: Any) -> str | None:
        local = _context(self)
        user_key = str(getattr(local, "user_key", "") or "")
        uname = str(getattr(local, "uname", "") or "").strip()
        if not user_key or not uname:
            return None
        return user_key if _item_identity(server_module, item) == uname else None

    @functools.wraps(original_append)
    def append_with_key(self: Any, item: Any, *args: Any, **kwargs: Any) -> bool:
        before = len(getattr(self, "_persons", []))
        result = bool(original_append(self, item, *args, **kwargs))
        keys = _sync_keys(self)
        if result and len(getattr(self, "_persons", [])) == before + 1 and keys:
            keys[-1] = _key_for_item(self, item)
        return result

    @functools.wraps(original_insert)
    def insert_with_key(self: Any, pos: int, item: Any, *args: Any, **kwargs: Any) -> bool:
        before = len(getattr(self, "_persons", []))
        insert_pos = max(0, min(int(pos), before))
        keys = _sync_keys(self)
        keys.insert(insert_pos, None)
        try:
            result = bool(original_insert(self, pos, item, *args, **kwargs))
        except Exception:
            if insert_pos < len(keys):
                keys.pop(insert_pos)
            raise
        persons = getattr(self, "_persons", [])
        if not result or len(persons) != before + 1:
            if insert_pos < len(keys):
                keys.pop(insert_pos)
            _sync_keys(self)
            return result
        _sync_keys(self)
        if insert_pos < len(keys):
            keys[insert_pos] = _key_for_item(self, item)
        return result

    @functools.wraps(original_remove)
    def remove_with_key(self: Any, index: int, *args: Any, **kwargs: Any) -> bool:
        keys = _sync_keys(self)
        valid = 0 <= int(index) < len(getattr(self, "_persons", []))
        result = bool(original_remove(self, index, *args, **kwargs))
        if result and valid and 0 <= int(index) < len(keys):
            keys.pop(int(index))
        _sync_keys(self)
        return result

    @functools.wraps(original_set_entries)
    def set_entries_with_keys(self: Any, entries: list[dict[str, Any]]) -> None:
        original_set_entries(self, entries)
        keys: list[str | None] = []
        for entry in entries if isinstance(entries, list) else []:
            if not isinstance(entry, dict):
                continue
            raw_key = str(entry.get("platform_user_key", "") or "").strip()
            keys.append(raw_key or None)
        self._platform_queue_keys = keys
        _sync_keys(self)

    @functools.wraps(original_get_entries)
    def get_entries_with_keys(self: Any) -> list[dict[str, Any]]:
        entries = original_get_entries(self)
        keys = _sync_keys(self)
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict) or index >= len(keys) or not keys[index]:
                continue
            key = str(keys[index])
            entry["platform_user_key"] = key
            entry["source_platform"] = key.split(":", 1)[0]
        return entries

    if callable(original_move):
        @functools.wraps(original_move)
        def move_with_keys(self: Any, index: int, direction: str):
            with self._lock:
                count = len(getattr(self, "_persons", []))
                should_swap = (
                    (direction == "up" and 2 <= int(index) <= count)
                    or (direction == "down" and 1 <= int(index) <= count - 1)
                )
                if should_swap:
                    keys = _sync_keys(self)
                    left = int(index) - 2 if direction == "up" else int(index) - 1
                    right = int(index) - 1 if direction == "up" else int(index)
                    if 0 <= left < len(keys) and 0 <= right < len(keys):
                        keys[left], keys[right] = keys[right], keys[left]
            try:
                return original_move(self, index, direction)
            except Exception:
                if should_swap:
                    with self._lock:
                        keys = _sync_keys(self)
                        if 0 <= left < len(keys) and 0 <= right < len(keys):
                            keys[left], keys[right] = keys[right], keys[left]
                raise

        setattr(queue_manager_cls, "move_item", move_with_keys)

    if callable(original_clear):
        @functools.wraps(original_clear)
        def clear_with_keys(self: Any):
            result = original_clear(self)
            with self._lock:
                self._platform_queue_keys = []
            return result

        setattr(queue_manager_cls, "clear_queue", clear_with_keys)

    setattr(queue_manager_cls, "process_danmu_json", process_with_identity_context)
    setattr(queue_manager_cls, "_find_index", find_platform_user)
    setattr(queue_manager_cls, "_append_queue_item_unlocked", append_with_key)
    setattr(queue_manager_cls, "_insert_queue_item_unlocked", insert_with_key)
    setattr(queue_manager_cls, "_remove_queue_item_unlocked", remove_with_key)
    setattr(queue_manager_cls, "_set_queue_from_entries_unlocked", set_entries_with_keys)
    setattr(queue_manager_cls, "_get_queue_entries_unlocked", get_entries_with_keys)
    setattr(queue_manager_cls, "_bilipdj_platform_queue_identity_installed", True)
    return True


def install_queue_rank_identity_integration(queue_rank_module: Any) -> bool:
    current = getattr(queue_rank_module, "attach_queue_rank_query", None)
    if not callable(current):
        return False
    if bool(getattr(current, "_bilipdj_platform_queue_identity_bridge", False)):
        return True

    @functools.wraps(current)
    def attach_with_platform_identity(queue_manager_cls: type[Any]) -> bool:
        result = bool(current(queue_manager_cls))
        patch_queue_manager_platform_identity(queue_manager_cls)
        return result

    setattr(attach_with_platform_identity, "_bilipdj_platform_queue_identity_bridge", True)
    setattr(queue_rank_module, "attach_queue_rank_query", attach_with_platform_identity)
    return True


__all__ = [
    "install_queue_rank_identity_integration",
    "patch_queue_manager_platform_identity",
]
