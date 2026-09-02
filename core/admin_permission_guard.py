"""Granular permissions for named plugin administrators.

Legacy behavior is preserved: an administrator without an explicit override keeps
all operator permissions. An explicit empty override removes all operator
commands while keeping the user in the administrator role.
"""
from __future__ import annotations

import functools
import json
import os
import re
import sys
import threading
from pathlib import Path
from typing import Any

ADMIN_PERMISSION_DEFS: tuple[tuple[str, str], ...] = (
    ("queue_remove", "删除 / 完成队列项"),
    ("queue_add", "手动新增队列项"),
    ("queue_insert", "指定位置插队 / 无影插"),
    ("queue_control", "暂停 / 恢复排队功能"),
    ("queue_limit", "修改排队人数上限"),
    ("jianzhang_control", "开启 / 关闭舰长插队"),
    ("room_admin_control", "开启 / 关闭房管管理权限"),
    ("blacklist", "拉黑 / 取消拉黑"),
)
ADMIN_PERMISSION_KEYS = tuple(key for key, _label in ADMIN_PERMISSION_DEFS)
_PERMISSION_LABELS = dict(ADMIN_PERMISSION_DEFS)
_STORE_LOCK = threading.RLock()

_PERMISSION_ALIASES: dict[str, set[str]] = {
    "queue_remove": {"queue_remove", "删除", "完成", "移除"},
    "queue_add": {"queue_add", "新增", "添加", "手动新增"},
    "queue_insert": {"queue_insert", "插队", "指定插队", "位置插队", "无影插"},
    "queue_control": {"queue_control", "排队开关", "暂停恢复", "暂停", "恢复"},
    "queue_limit": {"queue_limit", "排队上限", "人数上限", "上限"},
    "jianzhang_control": {"jianzhang_control", "舰长插队开关", "舰长开关"},
    "room_admin_control": {"room_admin_control", "房管权限", "房管开关"},
    "blacklist": {"blacklist", "黑名单", "拉黑"},
}
_ALIAS_TO_KEY = {
    alias.casefold(): key
    for key, aliases in _PERMISSION_ALIASES.items()
    for alias in aliases
}


def _server_module_for_class(queue_manager_cls: type[Any]) -> Any | None:
    module = sys.modules.get(str(getattr(queue_manager_cls, "__module__", "") or ""))
    if module is not None:
        return module
    return None


def admin_permission_store_path(queue_manager_cls: type[Any] | None = None) -> Path:
    if queue_manager_cls is not None:
        module = _server_module_for_class(queue_manager_cls)
        yaml_dir = getattr(module, "_YAML_DIR", None) if module is not None else None
        if yaml_dir is not None:
            return Path(yaml_dir) / "admin_permissions.json"
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / "admin_permissions.json"
    return Path(__file__).resolve().parent / "admin_permissions.json"


def _normalize_permission_list(value: Any) -> list[str]:
    if isinstance(value, str):
        values = re.split(r"[\s,，、;；]+", value.strip())
    elif isinstance(value, (list, tuple, set)):
        values = [str(item).strip() for item in value]
    else:
        values = []
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        key = _ALIAS_TO_KEY.get(str(item).strip().casefold(), str(item).strip())
        if key not in ADMIN_PERMISSION_KEYS or key in seen:
            continue
        seen.add(key)
        result.append(key)
    return result


def load_admin_permission_store(path: Path | None = None) -> dict[str, list[str]]:
    target = Path(path) if path is not None else admin_permission_store_path()
    with _STORE_LOCK:
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            return {}
    raw_admins = payload.get("admins", {}) if isinstance(payload, dict) else {}
    if not isinstance(raw_admins, dict):
        return {}
    result: dict[str, list[str]] = {}
    for name, permissions in raw_admins.items():
        admin_name = str(name or "").strip()
        if not admin_name:
            continue
        result[admin_name] = _normalize_permission_list(permissions)
    return result


def save_admin_permission_store(
    admins: dict[str, Any],
    path: Path | None = None,
) -> dict[str, list[str]]:
    target = Path(path) if path is not None else admin_permission_store_path()
    normalized = {
        str(name).strip(): _normalize_permission_list(permissions)
        for name, permissions in admins.items()
        if str(name).strip()
    }
    payload = {"version": 1, "admins": normalized}
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f".{target.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with _STORE_LOCK:
        try:
            temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)
    return normalized


def set_admin_permissions(
    admin_name: str,
    permissions: Any,
    path: Path | None = None,
) -> dict[str, list[str]]:
    target = Path(path) if path is not None else admin_permission_store_path()
    current = load_admin_permission_store(target)
    name = str(admin_name or "").strip()
    if name:
        current[name] = _normalize_permission_list(permissions)
    return save_admin_permission_store(current, target)


def reset_admin_permissions(admin_name: str, path: Path | None = None) -> dict[str, list[str]]:
    target = Path(path) if path is not None else admin_permission_store_path()
    current = load_admin_permission_store(target)
    current.pop(str(admin_name or "").strip(), None)
    return save_admin_permission_store(current, target)


def prune_admin_permissions(valid_admins: list[str] | tuple[str, ...] | set[str], path: Path | None = None) -> dict[str, list[str]]:
    target = Path(path) if path is not None else admin_permission_store_path()
    valid = {str(name).strip() for name in valid_admins if str(name).strip()}
    current = load_admin_permission_store(target)
    filtered = {name: permissions for name, permissions in current.items() if name in valid}
    if filtered != current:
        return save_admin_permission_store(filtered, target)
    return current


def required_permission_for_command(message: str) -> str | None:
    msg = str(message or "").strip()
    if msg == "完成" or re.match(r"^(?:del|删除|完成)\s*\d+\s*$", msg):
        return "queue_remove"
    if msg.startswith(("add ", "新增 ", "添加 ")):
        return "queue_add"
    if msg.startswith("无影插 ") or re.match(r"^插队\s+\d+\s+.+", msg):
        return "queue_insert"
    if msg in {"暂停排队功能", "关闭自助排队", "恢复排队功能", "恢复自助排队"}:
        return "queue_control"
    if any(keyword in msg for keyword in ("设置排队人数", "设置排队上限")):
        return "queue_limit"
    if msg in {"开启舰长插队", "关闭舰长插队"}:
        return "jianzhang_control"
    if msg in {"允许房管成为插件管理员", "停止房管成为插件管理员"}:
        return "room_admin_control"
    if msg.startswith(("拉黑 ", "取消拉黑 ")):
        return "blacklist"
    return None


def _parse_permission_spec(spec: str) -> list[str]:
    text = str(spec or "").strip()
    if not text:
        return []
    if text.casefold() in {"all", "全部", "完整", "全权限"}:
        return list(ADMIN_PERMISSION_KEYS)
    if text.casefold() in {"none", "无", "空", "无权限"}:
        return []
    return _normalize_permission_list(text)


def _permission_summary(permissions: list[str]) -> str:
    if not permissions:
        return "无管理指令权限"
    if set(permissions) == set(ADMIN_PERMISSION_KEYS):
        return "全部管理权限"
    return "、".join(_PERMISSION_LABELS.get(key, key) for key in permissions)


def attach_admin_permission_guard(queue_manager_cls: type[Any]) -> bool:
    if not isinstance(queue_manager_cls, type):
        return False
    if bool(getattr(queue_manager_cls, "_bilipdj_admin_permission_guard_installed", False)):
        return True

    original_process = getattr(queue_manager_cls, "_process", None)
    if not callable(original_process):
        return False

    def _store_path(self: Any) -> Path:
        return admin_permission_store_path(type(self))

    def _permission_map(self: Any) -> dict[str, list[str]]:
        return load_admin_permission_store(_store_path(self))

    def _current_admin_names(self: Any) -> set[str]:
        lock = getattr(self, "_lock", None)
        if lock is not None:
            with lock:
                return {str(name) for name in getattr(self, "_admins", []) if str(name).strip()}
        return {str(name) for name in getattr(self, "_admins", []) if str(name).strip()}

    def _is_super(self: Any, uname: str, is_anchor: bool) -> bool:
        checker = getattr(self, "_has_super_admin", None)
        if callable(checker):
            try:
                return bool(checker(uname, is_anchor))
            except Exception:
                pass
        return bool(is_anchor or uname in getattr(self, "_super_admins", []))

    def _handle_permission_management(
        self: Any,
        uname: str,
        message: str,
        is_anchor: bool,
    ) -> tuple[bool, str] | None:
        msg = str(message or "").strip()
        prefixes = (
            "设置管理员权限 ",
            "清空管理员权限 ",
            "重置管理员权限 ",
            "查看管理员权限 ",
        )
        if not msg.startswith(prefixes):
            return None
        if not _is_super(self, uname, is_anchor):
            return True, "权限不足，仅最高管理员可调整管理员细分权限"

        current_admins = _current_admin_names(self)
        path = _store_path(self)

        if msg.startswith("设置管理员权限 "):
            rest = msg[len("设置管理员权限 "):].strip()
            parts = rest.split(maxsplit=1)
            if len(parts) < 2:
                return True, "格式：设置管理员权限 管理员名 权限1,权限2"
            target, spec = parts[0].strip(), parts[1].strip()
            if target not in current_admins:
                return True, f"{target} 当前不是管理员"
            permissions = _parse_permission_spec(spec)
            if spec and not permissions and spec.casefold() not in {"none", "无", "空", "无权限"}:
                return True, "未识别到有效权限名称"
            set_admin_permissions(target, permissions, path)
            return True, f"已设置 {target}：{_permission_summary(permissions)}"

        if msg.startswith("清空管理员权限 "):
            target = msg[len("清空管理员权限 "):].strip()
            if target not in current_admins:
                return True, f"{target} 当前不是管理员"
            set_admin_permissions(target, [], path)
            return True, f"已清空 {target} 的管理指令权限"

        if msg.startswith("重置管理员权限 "):
            target = msg[len("重置管理员权限 "):].strip()
            if target not in current_admins:
                return True, f"{target} 当前不是管理员"
            reset_admin_permissions(target, path)
            return True, f"已将 {target} 恢复为完整管理员权限"

        target = msg[len("查看管理员权限 "):].strip()
        if target not in current_admins:
            return True, f"{target} 当前不是管理员"
        mapping = load_admin_permission_store(path)
        if target not in mapping:
            return True, f"{target}：完整管理员权限（未设置单独限制）"
        return True, f"{target}：{_permission_summary(mapping[target])}"

    @functools.wraps(original_process)
    def process_with_admin_permissions(
        self: Any,
        uid: int,
        uname: str,
        msg: str,
        is_anchor: bool,
        is_admin: bool,
        is_guard: bool,
        guard_level: int,
    ) -> tuple[bool, str | None]:
        management = _handle_permission_management(self, uname, msg, is_anchor)
        if management is not None:
            _handled, note = management
            logger = getattr(self, "_logger", None)
            if logger is not None:
                logger.info("[管理员权限] actor=%s command=%r result=%s", uname, msg, note)
            return False, note

        required = required_permission_for_command(msg)
        if required and not _is_super(self, uname, is_anchor):
            named_admin = uname in _current_admin_names(self)
            if named_admin:
                mapping = _permission_map(self)
                if uname in mapping and required not in mapping[uname]:
                    return False, f"权限不足：未授予「{_PERMISSION_LABELS.get(required, required)}」"

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

        # Commands such as blacklisting or removing an administrator can change
        # the admin role inside the original queue engine. Drop stale overrides
        # immediately so re-adding that person later starts with legacy full
        # permissions instead of unexpectedly inheriting an old restriction.
        if msg.startswith(("拉黑 ", "取消管理员 ")):
            prune_admin_permissions(_current_admin_names(self), _store_path(self))
        return result

    setattr(queue_manager_cls, "_process", process_with_admin_permissions)
    setattr(queue_manager_cls, "get_admin_permission_overrides", _permission_map)
    setattr(queue_manager_cls, "_bilipdj_admin_permission_guard_installed", True)
    return True


__all__ = [
    "ADMIN_PERMISSION_DEFS",
    "ADMIN_PERMISSION_KEYS",
    "admin_permission_store_path",
    "attach_admin_permission_guard",
    "load_admin_permission_store",
    "prune_admin_permissions",
    "required_permission_for_command",
    "reset_admin_permissions",
    "save_admin_permission_store",
    "set_admin_permissions",
]
