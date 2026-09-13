from __future__ import annotations

import copy
import json
import math
import re
import threading
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .bilibili_gifts import GIFT_BATTERIES

_PATCH_LOCK = threading.RLock()
_PLATFORM_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,39}$")
_MAX_RULES = 500
_MAX_BODY_BYTES = 256 * 1024
_CONFIG_NAME = "gift_compatibility.json"


class GiftCompatibilityError(RuntimeError):
    pass


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _to_number(value: Any) -> float | int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0:
        return None
    if number.is_integer():
        return int(number)
    return number


def _normalize_rule(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise GiftCompatibilityError("礼物规则必须是对象")
    platform = str(raw.get("platform", "") or "").strip().lower()
    if not _PLATFORM_RE.fullmatch(platform):
        raise GiftCompatibilityError("平台标识格式无效")
    gift_id = str(raw.get("gift_id", "") or "").strip()
    gift_name = str(raw.get("gift_name", "") or "").strip()
    if not gift_id and not gift_name:
        raise GiftCompatibilityError("礼物 ID 和礼物名称至少填写一项")
    if len(gift_id) > 120 or len(gift_name) > 120:
        raise GiftCompatibilityError("礼物 ID 或名称过长")
    value = _to_number(raw.get("value"))
    if value is None:
        raise GiftCompatibilityError("礼物价值必须是大于等于 0 的有限数字")
    return {
        "platform": platform,
        "gift_id": gift_id,
        "gift_name": gift_name,
        "value": value,
        "enabled": bool(raw.get("enabled", True)),
    }


def _atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


class GiftCompatibilityService:
    """Persistent cross-platform gift-value mapping used by queue rules."""

    def __init__(self, server_module: Any) -> None:
        self.server = server_module
        self._lock = threading.RLock()
        self._cache_mtime_ns: int | None = None
        self._cache_rules: list[dict[str, Any]] = []

    @property
    def config_path(self) -> Path:
        return Path(getattr(self.server, "_YAML_DIR")) / _CONFIG_NAME

    def _read(self) -> list[dict[str, Any]]:
        path = self.config_path
        try:
            stat = path.stat()
        except OSError:
            self._cache_mtime_ns = None
            self._cache_rules = []
            return []
        if self._cache_mtime_ns == int(stat.st_mtime_ns):
            return copy.deepcopy(self._cache_rules)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise GiftCompatibilityError(f"礼物兼容性配置无法读取：{exc}") from exc
        rows = raw.get("rules", []) if isinstance(raw, dict) else []
        if not isinstance(rows, list):
            raise GiftCompatibilityError("礼物兼容性配置格式无效")
        normalized: list[dict[str, Any]] = []
        for row in rows[:_MAX_RULES]:
            normalized.append(_normalize_rule(row))
        self._cache_mtime_ns = int(stat.st_mtime_ns)
        self._cache_rules = normalized
        return copy.deepcopy(normalized)

    def list_rules(self) -> list[dict[str, Any]]:
        with self._lock:
            return self._read()

    def save_rules(self, rules: Any) -> list[dict[str, Any]]:
        if not isinstance(rules, list):
            raise GiftCompatibilityError("rules 必须是数组")
        if len(rules) > _MAX_RULES:
            raise GiftCompatibilityError(f"礼物规则最多 {_MAX_RULES} 条")
        normalized = [_normalize_rule(row) for row in rules]
        seen: set[tuple[str, str, str]] = set()
        for row in normalized:
            key = (row["platform"], row["gift_id"], row["gift_name"].casefold())
            if key in seen:
                raise GiftCompatibilityError("存在重复的礼物兼容性规则")
            seen.add(key)
        with self._lock:
            _atomic_json_write(self.config_path, {"version": 1, "rules": normalized})
            try:
                stat = self.config_path.stat()
                self._cache_mtime_ns = int(stat.st_mtime_ns)
            except OSError:
                self._cache_mtime_ns = None
            self._cache_rules = normalized
        return copy.deepcopy(normalized)

    def resolve(
        self,
        *,
        platform: str,
        gift_id: Any = "",
        gift_name: str = "",
        count: int = 1,
        raw_unit_value: Any = None,
    ) -> dict[str, Any]:
        normalized_platform = str(platform or "").strip().lower()
        normalized_id = str(gift_id or "").strip()
        normalized_name = str(gift_name or "").strip()
        qty = max(1, _to_int(count, 1))
        rules = self.list_rules()

        match: dict[str, Any] | None = None
        if normalized_id:
            for row in rules:
                if row["enabled"] and row["platform"] == normalized_platform and row["gift_id"] == normalized_id:
                    match = row
                    break
        if match is None and normalized_name:
            folded = normalized_name.casefold()
            for row in rules:
                if (
                    row["enabled"]
                    and row["platform"] == normalized_platform
                    and row["gift_name"]
                    and row["gift_name"].casefold() == folded
                ):
                    match = row
                    break

        unit_value: float | int | None = None
        source = "unresolved"
        if match is not None:
            unit_value = match["value"]
            source = "compatibility"
        elif normalized_platform == "bilibili" and normalized_name in GIFT_BATTERIES:
            unit_value = GIFT_BATTERIES[normalized_name]
            source = "builtin:bilibili"
        else:
            unit_value = _to_number(raw_unit_value)
            if unit_value is not None:
                source = "platform"

        total: float | int | None = None
        if unit_value is not None:
            total_num = float(unit_value) * qty
            total = int(total_num) if total_num.is_integer() else total_num
        return {
            "platform": normalized_platform,
            "gift_id": normalized_id,
            "gift_name": normalized_name,
            "count": qty,
            "unit_value": unit_value,
            "value": total,
            "source": source,
            "matched_rule": copy.deepcopy(match),
        }

    @staticmethod
    def builtin_catalog() -> list[dict[str, Any]]:
        return [
            {"platform": "bilibili", "gift_name": name, "gift_id": "", "value": value, "enabled": True}
            for name, value in sorted(GIFT_BATTERIES.items())
        ]


def _read_body(handler: Any) -> dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length", "0"))
    except (TypeError, ValueError):
        length = 0
    if length < 0 or length > _MAX_BODY_BYTES:
        raise GiftCompatibilityError("请求体过大")
    if length == 0:
        return {}
    try:
        payload = json.loads(handler.rfile.read(length).decode("utf-8", errors="replace"))
    except json.JSONDecodeError as exc:
        raise GiftCompatibilityError("请求体必须是有效 JSON") from exc
    if not isinstance(payload, dict):
        raise GiftCompatibilityError("请求体必须是 JSON 对象")
    return payload


def install_gift_compatibility(
    server_module: Any,
    plugin_manager_module: Any | None = None,
    backup_module: Any | None = None,
) -> bool:
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_gift_compatibility_installed", False)):
            return True

        service = GiftCompatibilityService(server_module)
        server_module.GiftCompatibilityService = GiftCompatibilityService
        server_module.gift_compatibility = service

        queue_class = getattr(server_module, "QueueManager", None)
        if not isinstance(queue_class, type):
            return False

        original_state = queue_class.get_gift_state

        def process_gift_event(self: Any, payload: dict[str, Any]) -> dict[str, Any]:
            if not isinstance(payload, dict):
                raise TypeError("gift event must be a dict")
            platform = str(payload.get("platform", "") or "").strip().lower()
            if not _PLATFORM_RE.fullmatch(platform):
                raise ValueError("gift event platform is invalid")
            gift = payload.get("gift", {})
            gift = gift if isinstance(gift, dict) else {}
            gift_name = str(gift.get("name", payload.get("gift_name", "")) or "").strip()
            gift_id_raw = gift.get("id", payload.get("gift_id", ""))
            gift_id = str(gift_id_raw or "").strip()
            count = max(1, _to_int(gift.get("count", payload.get("count", 1)), 1))
            uid = _to_int(payload.get("uid", payload.get("user_id", 0)), 0)
            uname = str(payload.get("uname", payload.get("user_name", "")) or "").strip()
            event_name = str(payload.get("event", "gift") or "gift")
            resolved = service.resolve(
                platform=platform,
                gift_id=gift_id,
                gift_name=gift_name,
                count=count,
                raw_unit_value=gift.get("value", payload.get("value")),
            )
            resolved_value = resolved["value"]
            event = {
                "type": "LIVE_GIFT_EVENT",
                "platform": platform,
                "event": event_name,
                "uid": uid,
                "uname": uname,
                "gift": {
                    "id": gift_id_raw,
                    "name": gift_name,
                    "count": count,
                    "unit_value": resolved["unit_value"],
                    "value": resolved_value,
                    "value_source": resolved["source"],
                    "batteries": resolved_value,
                    "coin_type": str(gift.get("coin_type", "") or ""),
                    "price": _to_int(gift.get("price", 0), 0),
                },
                "room_id": payload.get("room_id", payload.get("roomid", 0)),
                "ts": payload.get("ts"),
            }
            if isinstance(payload.get("raw"), dict):
                event["raw"] = copy.deepcopy(payload["raw"])

            grant_credit = bool(payload.get("grant_queue_credit", event_name == "gift"))
            with self._lock:
                catalog_key = f"{platform}:{gift_id or gift_name.casefold()}"
                self._gift_catalog[catalog_key] = {
                    "platform": platform,
                    "id": gift_id_raw,
                    "name": gift_name,
                    "value": resolved_value,
                    "unit_value": resolved["unit_value"],
                    "value_source": resolved["source"],
                    "batteries": resolved_value,
                    "price": _to_int(gift.get("price", 0), 0),
                    "coin_type": str(gift.get("coin_type", "") or ""),
                }
                self._last_gift_event = copy.deepcopy(event)
                matches_name = bool(gift_name and gift_name in self._gift_queue_names)
                matches_value = bool(
                    resolved_value is not None
                    and self._gift_queue_min_batteries > 0
                    and float(resolved_value) >= float(self._gift_queue_min_batteries)
                )
                qualifies = matches_name or matches_value
                can_repeat = self._gift_queue_allow_multiple or uid not in self._gift_queue_used_uids
                if self._gift_queue_enabled and grant_credit and uid > 0 and qualifies and can_repeat:
                    self._gift_queue_credits[uid] = self._gift_queue_credits.get(uid, 0) + self._gift_queue_slots_per_gift
                    if not self._gift_queue_allow_multiple:
                        self._gift_queue_used_uids.add(uid)
                    self._persist_myjs_state_unlocked()
            self._emit_event(event)
            self._log(
                f"礼物事件：{platform} {uname}({uid}) {gift_name or gift_id} x{count} "
                f"价值={resolved_value if resolved_value is not None else '未知'} 来源={resolved['source']}"
            )
            return event

        def process_live_event(self: Any, payload: dict[str, Any]) -> None:
            if not isinstance(payload, dict):
                return
            cmd = str(payload.get("cmd", "") or "").split(":", 1)[0]
            if cmd not in {"SEND_GIFT", "COMBO_SEND", "GUARD_BUY"}:
                return
            data = payload.get("data", {})
            data = data if isinstance(data, dict) else {}
            uid = _to_int(data.get("uid", data.get("user_id", 0)), 0)
            uname = str(data.get("uname", data.get("username", "")) or "")
            if cmd == "GUARD_BUY":
                gift_name = str(data.get("gift_name", data.get("role_name", "大航海")) or "大航海")
                gift_id = data.get("gift_id", data.get("guard_level", ""))
                count = max(1, _to_int(data.get("num", 1), 1))
                event_name = "guard_buy"
            else:
                gift_name = str(data.get("giftName", data.get("gift_name", "")) or "")
                gift_id = data.get("giftId", data.get("gift_id", ""))
                count = max(1, _to_int(data.get("num", data.get("combo_num", 1)), 1))
                event_name = "gift" if cmd == "SEND_GIFT" else "combo"
            self.process_gift_event({
                "platform": "bilibili",
                "event": event_name,
                "grant_queue_credit": cmd == "SEND_GIFT",
                "uid": uid,
                "uname": uname,
                "room_id": data.get("roomid", data.get("room_id", 0)),
                "gift": {
                    "id": gift_id,
                    "name": gift_name,
                    "count": count,
                    "coin_type": data.get("coin_type", ""),
                    "price": data.get("price", 0),
                },
                "raw": payload,
            })

        def get_gift_state(self: Any) -> dict[str, Any]:
            state = dict(original_state(self))
            state["value_unit"] = "queue_value"
            state["compatibility_rules"] = service.list_rules()
            state["builtin_catalog"] = service.builtin_catalog()
            catalog = state.get("catalog", [])
            if isinstance(catalog, list):
                enriched = []
                for item in catalog:
                    if isinstance(item, dict):
                        row = dict(item)
                        row.setdefault("platform", "bilibili")
                        row.setdefault("value", row.get("batteries"))
                        enriched.append(row)
                state["catalog"] = enriched
            return state

        queue_class.process_gift_event = process_gift_event
        queue_class.process_live_event = process_live_event
        queue_class.get_gift_state = get_gift_state

        if plugin_manager_module is not None:
            context_class = getattr(plugin_manager_module, "PluginContext", None)
            if isinstance(context_class, type):
                def process_plugin_gift_event(self: Any, payload: dict[str, Any]) -> None:
                    if not isinstance(payload, dict):
                        raise TypeError("gift event must be a dict")
                    event = dict(payload)
                    requested = str(event.get("platform", self.platform) or self.platform).strip().lower()
                    if requested != self.platform:
                        raise ValueError("plugin cannot emit a gift event for another platform")
                    event["platform"] = self.platform
                    manager = getattr(self._server, "queue_manager", None)
                    if manager is None or not hasattr(manager, "process_gift_event"):
                        raise RuntimeError("queue manager is unavailable")
                    manager.process_gift_event(event)
                context_class.process_gift_event = process_plugin_gift_event

        handler_class = getattr(server_module, "ApiHandler", None)
        if isinstance(handler_class, type):
            original_post = handler_class.do_POST

            def do_POST(self: Any) -> None:  # noqa: N802
                # GET /api/gifts/state already returns the runtime gift state.
                # POST on the same resource replaces only compatibility rules.
                if urlparse(self.path).path != "/api/gifts/state":
                    return original_post(self)
                if not self._require_loopback():
                    return
                try:
                    payload = _read_body(self)
                    rules = service.save_rules(payload.get("rules", []))
                    self._write_json({"status": "ok", "rules": rules})
                except GiftCompatibilityError as exc:
                    self._write_json({"status": "error", "message": str(exc)}, status=400)
                except Exception as exc:  # noqa: BLE001
                    self._write_json({"status": "error", "message": f"礼物兼容性保存失败：{exc}"}, status=500)

            handler_class.do_POST = do_POST

        if backup_module is not None:
            files = tuple(getattr(backup_module, "SETTINGS_FILES", ()))
            if _CONFIG_NAME not in files:
                backup_module.SETTINGS_FILES = files + (_CONFIG_NAME,)
            backup_service = getattr(backup_module, "SettingsBackupService", None)
            if isinstance(backup_service, type) and not bool(getattr(backup_service, "_bilipdj_gift_compat_paths", False)):
                original_paths = backup_service.settings_paths

                def settings_paths(self: Any) -> dict[str, Path]:
                    paths = dict(original_paths(self))
                    paths[_CONFIG_NAME] = Path(getattr(self.server, "_YAML_DIR")) / _CONFIG_NAME
                    return paths

                backup_service.settings_paths = settings_paths
                backup_service._bilipdj_gift_compat_paths = True

        server_module._gift_compatibility_installed = True
        return True


__all__ = [
    "GiftCompatibilityError",
    "GiftCompatibilityService",
    "install_gift_compatibility",
]
