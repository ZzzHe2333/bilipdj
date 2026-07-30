"""Targeted queue-logic compatibility fixes."""
from __future__ import annotations

import functools
import re
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()
_SUPER_QUEUE_PATTERN = re.compile(r"^\s*<([^<>]+)>([^<>]*)\s*$", re.DOTALL)
_WAITING_MARKERS = re.compile(r"⏳待确认|等待确认")


def _queue_identity(item: Any) -> str:
    text = str(item or "").strip()
    match = re.match(r"^(?:官|[Gg]|[Bb]|米|[Mm]|[Ss])\|([^ ]+)", text)
    if match:
        return match.group(1).strip()
    match = re.match(r"^<([^>]+)>", text)
    if match:
        return match.group(1).strip()
    return text.split(" ", 1)[0].strip() if text else ""


def _gift_only_queue_plan(self: Any, uid: int, uname: str, msg: str) -> list[str] | None:
    command = str(msg or "").strip()
    if not (command == "插队" or command.startswith("插队 ")):
        return None
    lock = getattr(self, "_lock", None)
    if lock is None:
        return None
    with lock:
        if int(uid or 0) not in getattr(self, "_gift_queue_credits", {}):
            return None
        if self._find_index(str(uname or "").strip()) >= 0:
            return None
        credit = max(0, int(getattr(self, "_gift_queue_credits", {}).get(int(uid), 0) or 0))
        requested = [
            value
            for value in re.split(r"[\s,，、]+", command[2:].strip())
            if value
        ] or [str(uname or "").strip()]
        selected = requested[: min(len(requested), credit)]
        if str(uname or "").strip() not in selected:
            return None
        result = list(getattr(self, "_persons", []))
        rank = max(0, int(getattr(self, "_gift_queue_insert_rank", 1) or 0))
        if rank <= 0:
            for value in selected:
                cleaned = self._strip_html(value)
                if cleaned:
                    result.append(cleaned)
        else:
            base_pos = min(len(result), max(0, rank - 1))
            for offset, value in enumerate(selected):
                cleaned = self._strip_html(value)
                if cleaned:
                    insert_pos = max(0, min(base_pos + offset, len(result)))
                    result.insert(insert_pos, cleaned)
        return result


def patch_queue_manager(queue_manager_cls: type[Any]) -> bool:
    """Install queue sanitizer and duplicate-insertion fixes."""

    if not isinstance(queue_manager_cls, type):
        return False
    with _PATCH_LOCK:
        if bool(getattr(queue_manager_cls, "_bilipdj_queue_logic_guard_installed", False)):
            return True
        original_strip = getattr(queue_manager_cls, "_strip_html", None)
        original_process = getattr(queue_manager_cls, "_process", None)
        if not callable(original_strip) or not callable(original_process):
            return False

        @functools.wraps(original_strip)
        def strip_preserving_super_marker(text: Any) -> str:
            raw = str(text or "")
            match = _SUPER_QUEUE_PATTERN.fullmatch(raw)
            if match:
                item_id = _WAITING_MARKERS.sub("", match.group(1)).strip()
                extra = _WAITING_MARKERS.sub("", match.group(2)).strip()
                if item_id:
                    return f"<{item_id}>{extra}" if extra else f"<{item_id}>"
            return str(original_strip(raw) or "").strip()

        @functools.wraps(original_process)
        def process_without_double_insertion(
            self: Any,
            uid: int,
            uname: str,
            msg: str,
            is_anchor: bool,
            is_admin: bool,
            is_guard: bool,
            guard_level: int,
        ):
            gift_only_plan = _gift_only_queue_plan(self, uid, uname, msg)
            result = original_process(
                self,
                uid,
                uname,
                msg,
                is_anchor,
                is_admin,
                is_guard,
                guard_level,
            )
            modified = bool(result[0]) if isinstance(result, tuple) and result else False
            if not modified or gift_only_plan is None:
                return result

            lock = getattr(self, "_lock", None)
            if lock is None:
                return result
            user_name = str(uname or "").strip()
            with lock:
                current = list(getattr(self, "_persons", []))
                if len(current) != len(gift_only_plan) + 1:
                    return result
                for index, item in enumerate(current):
                    if _queue_identity(item) != user_name:
                        continue
                    candidate = current[:index] + current[index + 1 :]
                    if candidate != gift_only_plan:
                        continue
                    self._remove_queue_item_unlocked(index)
                    logger = getattr(self, "_logger", None)
                    if logger is not None:
                        logger.warning(
                            "[队列修复] 已移除礼物插队与舰长插队叠加产生的重复项：%s",
                            user_name,
                        )
                    break
            return result

        setattr(queue_manager_cls, "_strip_html", staticmethod(strip_preserving_super_marker))
        setattr(queue_manager_cls, "_process", process_without_double_insertion)
        setattr(queue_manager_cls, "_bilipdj_queue_logic_guard_installed", True)
        return True


__all__ = ["patch_queue_manager"]
