"""Multi-platform danmu runtime and platform-aware user identities.

The legacy single-platform relay remains the default. When multi-platform mode is
explicitly enabled and at least two implemented platform streams are configured,
a composite relay starts them in parallel.
"""
from __future__ import annotations

import copy
import datetime as dt
import functools
import hashlib
import json
import os
import sys
import threading
from pathlib import Path
from typing import Any

MULTI_PLATFORM_CONFIG_KEY = "multi_platform_danmu"
DEFAULT_MULTI_PLATFORM_DANMU: dict[str, Any] = {
    "enabled": False,
    "platforms": {
        "bilibili": True,
        "douyin": True,
    },
}
IMPLEMENTED_STREAM_PLATFORMS = ("bilibili", "douyin")
PLATFORM_LOG_LABELS = {
    "bilibili": "B站",
    "douyin": "抖音",
}
_CONFIG_LOCK = threading.RLock()
_PATCH_LOCK = threading.RLock()


def normalize_multi_platform_config(raw: Any) -> dict[str, Any]:
    source = raw if isinstance(raw, dict) else {}
    raw_platforms = source.get("platforms", {})
    if not isinstance(raw_platforms, dict):
        raw_platforms = {}
    return {
        "enabled": bool(source.get("enabled", False)),
        "platforms": {
            platform: bool(
                raw_platforms.get(
                    platform,
                    DEFAULT_MULTI_PLATFORM_DANMU["platforms"][platform],
                )
            )
            for platform in IMPLEMENTED_STREAM_PLATFORMS
        },
    }


def _config_path(server_module: Any | None = None) -> Path:
    if server_module is not None:
        yaml_dir = getattr(server_module, "_YAML_DIR", None)
        if yaml_dir is not None:
            return Path(yaml_dir) / "multi_platform_danmu.json"
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / "multi_platform_danmu.json"
    return Path(__file__).resolve().parent / "multi_platform_danmu.json"


def load_multi_platform_config(path: Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else _config_path()
    with _CONFIG_LOCK:
        try:
            payload = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError, TypeError):
            return copy.deepcopy(DEFAULT_MULTI_PLATFORM_DANMU)
    return normalize_multi_platform_config(payload)


def save_multi_platform_config(config: Any, path: Path | None = None) -> dict[str, Any]:
    target = Path(path) if path is not None else _config_path()
    normalized = normalize_multi_platform_config(config)
    payload = {
        "version": 1,
        **normalized,
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_name(f".{target.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    with _CONFIG_LOCK:
        try:
            temp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            os.replace(temp, target)
        finally:
            temp.unlink(missing_ok=True)
    return normalized


def multi_platform_enabled(config: Any) -> bool:
    if not isinstance(config, dict):
        return False
    return normalize_multi_platform_config(config.get(MULTI_PLATFORM_CONFIG_KEY, {}))["enabled"]


def enabled_stream_platforms(config: Any) -> list[str]:
    """Return implemented streams that are selected and sufficiently configured."""
    if not isinstance(config, dict):
        return []
    multi_cfg = normalize_multi_platform_config(config.get(MULTI_PLATFORM_CONFIG_KEY, {}))
    if not multi_cfg["enabled"]:
        return []

    selected = multi_cfg["platforms"]
    result: list[str] = []

    bilibili_cfg = config.get("bilibili", config.get("api", {}))
    if not isinstance(bilibili_cfg, dict):
        bilibili_cfg = {}
    try:
        bilibili_roomid = int(bilibili_cfg.get("roomid", 0) or 0)
    except (TypeError, ValueError):
        bilibili_roomid = 0
    if selected.get("bilibili", True) and bilibili_roomid > 0:
        result.append("bilibili")

    douyin_cfg = config.get("douyin", {})
    if not isinstance(douyin_cfg, dict):
        douyin_cfg = {}
    douyin_live_id = str(douyin_cfg.get("live_id", "") or "").strip()
    if selected.get("douyin", True) and bool(douyin_cfg.get("enabled", False)) and douyin_live_id:
        result.append("douyin")

    return result


def platform_user_key(
    platform: str,
    *,
    uid: Any = 0,
    uname: Any = "",
    sec_uid: Any = "",
) -> str:
    platform_name = str(platform or "unknown").strip().lower() or "unknown"
    sec_uid_text = str(sec_uid or "").strip()
    if sec_uid_text:
        return f"{platform_name}:sec_uid:{sec_uid_text}"
    try:
        numeric_uid = int(uid or 0)
    except (TypeError, ValueError):
        numeric_uid = 0
    if numeric_uid > 0:
        return f"{platform_name}:uid:{numeric_uid}"
    name = str(uname or "").strip()
    digest = hashlib.sha256(name.encode("utf-8")).hexdigest()[:24] if name else "anonymous"
    return f"{platform_name}:name:{digest}"


def platform_runtime_uid(platform: str, original_uid: Any, user_key: str) -> int:
    """Namespace non-Bilibili numeric IDs while preserving Bilibili gift-credit IDs."""
    try:
        uid = int(original_uid or 0)
    except (TypeError, ValueError):
        uid = 0
    if str(platform or "").strip().lower() == "bilibili" and uid > 0:
        return uid
    digest = hashlib.blake2b(str(user_key).encode("utf-8"), digest_size=8).digest()
    return max(1, int.from_bytes(digest, "big") & ((1 << 63) - 1))


def _platform_anchor_uid(server_module: Any, runtime: dict[str, Any], platform: str) -> int:
    try:
        if platform == "bilibili":
            cfg = server_module._get_bilibili_config(runtime)
            return int(cfg.get("uid", 0) or 0)
        if platform == "douyin":
            cfg = server_module._get_douyin_config(runtime)
            live_info = cfg.get("live_info", {}) if isinstance(cfg, dict) else {}
            if isinstance(live_info, dict):
                for key in ("anchor_id", "user_id", "user_unique_id"):
                    try:
                        value = int(live_info.get(key, 0) or 0)
                    except (TypeError, ValueError):
                        value = 0
                    if value > 0:
                        return value
    except Exception:
        pass
    return 0


def _patch_queue_manager(server_module: Any) -> bool:
    queue_manager_cls = getattr(server_module, "QueueManager", None)
    if not isinstance(queue_manager_cls, type):
        return False
    if bool(getattr(queue_manager_cls, "_bilipdj_multi_platform_identity_installed", False)):
        return True

    original = getattr(queue_manager_cls, "process_danmu_json", None)
    if not callable(original):
        return False

    @functools.wraps(original)
    def process_platform_danmu_json(self: Any, payload: dict[str, Any]) -> None:
        cmd = str(payload.get("cmd", "") or "").strip() if isinstance(payload, dict) else ""
        if not cmd.startswith("DANMU_MSG"):
            return original(self, payload)
        info = payload.get("info", []) if isinstance(payload, dict) else []
        if not isinstance(info, list) or len(info) < 3:
            return original(self, payload)

        platform = str(payload.get("_pdj_platform", "bilibili") or "bilibili").strip().lower()
        if platform not in IMPLEMENTED_STREAM_PLATFORMS:
            platform = "bilibili"
        platform_meta = payload.get("_pdj_identity", {})
        if not isinstance(platform_meta, dict):
            platform_meta = {}

        runtime = getattr(getattr(self, "_ws_hub", None), "server", None)
        runtime_config: dict[str, Any] = {}
        # QueueManager does not own the server object. Resolve the active server module
        # configuration through the runtime reload callback owner when possible; the
        # module-level load_config fallback is safe and local-only.
        try:
            runtime_config = server_module.load_config()
        except Exception:
            runtime_config = {}

        anchor_uid = _platform_anchor_uid(server_module, runtime_config, platform)
        identity = server_module.bilibili_protocol.parse_bilibili_danmu_identity(
            payload,
            anchor_uid=anchor_uid,
        )
        msg = str(info[1]) if len(info) > 1 else ""
        try:
            original_uid = int(identity.get("uid", 0) or 0)
        except (TypeError, ValueError):
            original_uid = 0
        uname = str(identity.get("uname", "") or "")
        sec_uid = str(platform_meta.get("sec_uid", "") or "")
        user_key = platform_user_key(
            platform,
            uid=original_uid,
            uname=uname,
            sec_uid=sec_uid,
        )
        runtime_uid = platform_runtime_uid(platform, original_uid, user_key)

        if not uname or not msg:
            return

        is_admin_flag = bool(identity.get("is_room_admin", False))
        is_anchor = bool(identity.get("is_anchor", False))
        try:
            guard_level = int(identity.get("guard_level", 0) or 0)
        except (TypeError, ValueError):
            guard_level = 0
        is_guard = bool(identity.get("is_guard", False))

        identity["platform"] = platform
        identity["platform_user_key"] = user_key
        identity["internal_user_id"] = user_key
        identity["original_uid"] = original_uid
        identity["runtime_uid"] = runtime_uid
        if sec_uid:
            identity["sec_uid"] = sec_uid

        event = {
            "type": "DANMU_EVENT",
            "platform": platform,
            "platform_user_key": user_key,
            "internal_user_id": user_key,
            "message": msg,
            "identity": identity,
            "received_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        }
        with self._lock:
            is_blacklisted = uname in self._blacklist
            self._last_danmu_event = copy.deepcopy(event)
            registry = getattr(self, "_platform_user_registry", None)
            if not isinstance(registry, dict):
                registry = {}
                self._platform_user_registry = registry
            registry[user_key] = {
                "platform": platform,
                "uid": original_uid,
                "runtime_uid": runtime_uid,
                "uname": uname,
                "sec_uid": sec_uid,
                "last_seen_at": event["received_at"],
            }
        self._ws_hub.broadcast_json(None, event)

        guard_name = str(identity.get("guard_name", "") or "")
        medal = identity.get("fan_medal", {})
        medal_text = (
            f" 粉丝牌={medal.get('name')}Lv.{medal.get('level')}"
            if identity.get("has_fan_medal") and isinstance(medal, dict)
            else ""
        )
        perm = (
            "黑名单"
            if is_blacklisted
            else (
                "主播"
                if is_anchor
                else (
                    "super_admin"
                    if uname in self._super_admins
                    else (
                        "房管"
                        if is_admin_flag
                        else (guard_name or ("管理员" if uname in self._admins else "普通用户"))
                    )
                )
            )
        )
        source = PLATFORM_LOG_LABELS.get(platform, platform)
        self._logger.info(
            "[弹幕][%s] %s(%s%s) user_key=%s: %s",
            source,
            uname,
            perm,
            medal_text,
            user_key,
            msg,
        )

        modified, note = self._process(
            runtime_uid,
            uname,
            msg,
            is_anchor,
            is_admin_flag,
            is_guard,
            guard_level,
        )
        if modified:
            self._broadcast_and_archive(f"{platform}:{uname}", msg)
            self._logger.info(
                "[触发指令][%s] uname=%s user_key=%s 权限=%s msg=%r → 队列变更，当前 %s 人",
                source,
                uname,
                user_key,
                perm,
                msg,
                len(self._persons),
            )
        elif note:
            self._logger.info(
                "[提示][%s] %s(%s) user_key=%s: %s",
                source,
                uname,
                perm,
                user_key,
                note,
            )

    def get_platform_user_registry(self: Any) -> dict[str, dict[str, Any]]:
        with self._lock:
            registry = getattr(self, "_platform_user_registry", {})
            return copy.deepcopy(registry) if isinstance(registry, dict) else {}

    setattr(queue_manager_cls, "process_danmu_json", process_platform_danmu_json)
    setattr(queue_manager_cls, "get_platform_user_registry", get_platform_user_registry)
    setattr(queue_manager_cls, "_bilipdj_multi_platform_identity_installed", True)
    return True


def _patch_bilibili_relay(server_module: Any) -> None:
    relay_cls = getattr(getattr(server_module, "bilibili_protocol", None), "BilibiliDanmuRelay", None)
    if not isinstance(relay_cls, type):
        return
    if not hasattr(relay_cls, "platform"):
        setattr(relay_cls, "platform", "bilibili")

    emit = getattr(relay_cls, "_emit_status", None)
    if callable(emit) and not bool(getattr(emit, "_bilipdj_platform_tagged", False)):
        @functools.wraps(emit)
        def emit_with_platform(self: Any, status: str, **extra: Any) -> Any:
            extra.setdefault("platform", "bilibili")
            return emit(self, status, **extra)

        setattr(emit_with_platform, "_bilipdj_platform_tagged", True)
        setattr(relay_cls, "_emit_status", emit_with_platform)

    get_status = getattr(relay_cls, "get_runtime_status", None)
    if callable(get_status) and not bool(getattr(get_status, "_bilipdj_platform_tagged", False)):
        @functools.wraps(get_status)
        def status_with_platform(self: Any) -> dict[str, Any]:
            payload = get_status(self)
            payload = dict(payload) if isinstance(payload, dict) else {}
            payload.setdefault("platform", "bilibili")
            return payload

        setattr(status_with_platform, "_bilipdj_platform_tagged", True)
        setattr(relay_cls, "get_runtime_status", status_with_platform)


def _patch_douyin_relay(server_module: Any) -> None:
    relay_cls = getattr(getattr(server_module, "douyin_protocol", None), "DouyinDanmuRelay", None)
    if not isinstance(relay_cls, type):
        return

    converter = getattr(relay_cls, "_to_bilibili_like_danmu_payload", None)
    if callable(converter) and not bool(getattr(converter, "_bilipdj_platform_tagged", False)):
        @functools.wraps(converter)
        def converter_with_platform(event: Any) -> dict[str, Any]:
            payload = converter(event)
            payload = dict(payload) if isinstance(payload, dict) else {}
            payload["_pdj_platform"] = "douyin"
            payload["_pdj_identity"] = {
                "sec_uid": str(getattr(event, "sec_uid", "") or ""),
                "original_uid": int(getattr(event, "uid", 0) or 0),
            }
            return payload

        setattr(converter_with_platform, "_bilipdj_platform_tagged", True)
        setattr(relay_cls, "_to_bilibili_like_danmu_payload", staticmethod(converter_with_platform))

    load_cfg = getattr(relay_cls, "_load_runtime_cfg", None)
    if callable(load_cfg) and not bool(getattr(load_cfg, "_bilipdj_multi_platform_enabled", False)):
        @functools.wraps(load_cfg)
        def load_cfg_multi(self: Any) -> dict[str, Any]:
            cfg = load_cfg(self)
            cfg = dict(cfg) if isinstance(cfg, dict) else {}
            runtime = getattr(getattr(self, "server", None), "runtime_config", {})
            if "douyin" in enabled_stream_platforms(runtime):
                cfg["platform"] = "douyin"
            return cfg

        setattr(load_cfg_multi, "_bilipdj_multi_platform_enabled", True)
        setattr(relay_cls, "_load_runtime_cfg", load_cfg_multi)


class MultiPlatformDanmuRelay:
    platform = "multi"

    def __init__(self, server: Any, server_module: Any, platforms: list[str]) -> None:
        self.server = server
        self.server_module = server_module
        self.platforms = tuple(platform for platform in platforms if platform in IMPLEMENTED_STREAM_PLATFORMS)
        self.relays: dict[str, Any] = {}
        for platform in self.platforms:
            if platform == "bilibili":
                relay = server_module.bilibili_protocol.BilibiliDanmuRelay(server)
            elif platform == "douyin":
                relay = server_module.douyin_protocol.DouyinDanmuRelay(server)
            else:
                continue
            self.relays[platform] = relay

    def start(self) -> None:
        for relay in self.relays.values():
            relay.start()

    def stop(self) -> None:
        for relay in self.relays.values():
            try:
                relay.stop()
            except Exception:
                pass
        for relay in self.relays.values():
            try:
                relay.join(timeout=1.5)
            except Exception:
                pass

    def join(self, timeout: float | None = None) -> None:
        per_relay = None if timeout is None else max(0.05, float(timeout) / max(1, len(self.relays)))
        for relay in self.relays.values():
            try:
                relay.join(timeout=per_relay)
            except Exception:
                pass

    def request_reconnect(self) -> None:
        for relay in self.relays.values():
            requester = getattr(relay, "request_reconnect", None)
            if callable(requester):
                requester()

    def get_runtime_status(self) -> dict[str, Any]:
        statuses: dict[str, dict[str, Any]] = {}
        for platform, relay in self.relays.items():
            getter = getattr(relay, "get_runtime_status", None)
            try:
                status = getter() if callable(getter) else {}
            except Exception as exc:
                status = {"connected": False, "last_disconnect_reason": str(exc)}
            status = dict(status) if isinstance(status, dict) else {}
            status.setdefault("platform", platform)
            statuses[platform] = status

        connected = [name for name, status in statuses.items() if bool(status.get("connected", False))]
        last_packet_at = max(
            (str(status.get("last_packet_at", "") or "") for status in statuses.values()),
            default="",
        )
        runtime = getattr(self.server, "runtime_config", {})
        primary_name = str(runtime.get("platform", "bilibili") or "bilibili") if isinstance(runtime, dict) else "bilibili"
        primary = statuses.get(primary_name) or next(iter(statuses.values()), {})
        result = dict(primary)
        result.update(
            {
                "platform": "multi",
                "connected": bool(connected),
                "connected_all": bool(statuses) and len(connected) == len(statuses),
                "connected_platforms": connected,
                "expected_platforms": list(statuses),
                "platforms": statuses,
                "last_packet_at": last_packet_at,
            }
        )
        return result


def _stop_relay(relay: Any) -> None:
    if relay is None:
        return
    try:
        relay.stop()
    except Exception:
        pass
    try:
        relay.join(timeout=2.0)
    except Exception:
        pass


def patch_server_module(server_module: Any) -> bool:
    """Patch a completed or partially completed core.server module idempotently."""
    if server_module is None:
        return False
    with _PATCH_LOCK:
        _patch_bilibili_relay(server_module)
        _patch_douyin_relay(server_module)
        _patch_queue_manager(server_module)

        load_config = getattr(server_module, "load_config", None)
        if callable(load_config) and not bool(getattr(load_config, "_bilipdj_multi_platform_config", False)):
            path = _config_path(server_module)

            @functools.wraps(load_config)
            def load_config_with_multi(*args: Any, **kwargs: Any) -> dict[str, Any]:
                config = load_config(*args, **kwargs)
                config = dict(config) if isinstance(config, dict) else {}
                config[MULTI_PLATFORM_CONFIG_KEY] = load_multi_platform_config(path)
                return config

            setattr(load_config_with_multi, "_bilipdj_multi_platform_config", True)
            setattr(server_module, "load_config", load_config_with_multi)

        save_config = getattr(server_module, "save_config", None)
        if callable(save_config) and not bool(getattr(save_config, "_bilipdj_multi_platform_config", False)):
            path = _config_path(server_module)

            @functools.wraps(save_config)
            def save_config_with_multi(config: dict[str, Any], *args: Any, **kwargs: Any) -> Any:
                if isinstance(config, dict):
                    current = load_multi_platform_config(path)
                    save_multi_platform_config(config.get(MULTI_PLATFORM_CONFIG_KEY, current), path)
                return save_config(config, *args, **kwargs)

            setattr(save_config_with_multi, "_bilipdj_multi_platform_config", True)
            setattr(server_module, "save_config", save_config_with_multi)

        ensure_relay = getattr(server_module, "_ensure_danmu_relay", None)
        if callable(ensure_relay) and not bool(getattr(ensure_relay, "_bilipdj_multi_platform_relay", False)):
            @functools.wraps(ensure_relay)
            def ensure_multi_relay(server: Any, *, reconnect: bool = False) -> None:
                runtime = getattr(server, "runtime_config", {})
                desired = enabled_stream_platforms(runtime)
                current = getattr(server, "danmu_relay", None)
                if len(desired) >= 2:
                    if not isinstance(current, MultiPlatformDanmuRelay) or set(current.platforms) != set(desired):
                        _stop_relay(current)
                        relay = MultiPlatformDanmuRelay(server, server_module, desired)
                        server.danmu_relay = relay
                        relay.start()
                        labels = [PLATFORM_LOG_LABELS.get(name, name) for name in desired]
                        server.logger.info("多平台弹幕监听已启动：%s", " + ".join(labels))
                        server.ws_hub.broadcast_json(
                            None,
                            {
                                "type": "PDJ_STATUS",
                                "status": "danmu_multi_platform_started",
                                "platform": "multi",
                                "platforms": desired,
                                "message": " + ".join(labels),
                            },
                        )
                        return
                    if reconnect:
                        current.request_reconnect()
                    return

                if isinstance(current, MultiPlatformDanmuRelay):
                    _stop_relay(current)
                    server.danmu_relay = None
                    server.logger.info("多平台弹幕监听已关闭，恢复单平台监听")
                return ensure_relay(server, reconnect=reconnect)

            setattr(ensure_multi_relay, "_bilipdj_multi_platform_relay", True)
            setattr(server_module, "_ensure_danmu_relay", ensure_multi_relay)

        setattr(server_module, "DEFAULT_MULTI_PLATFORM_DANMU", copy.deepcopy(DEFAULT_MULTI_PLATFORM_DANMU))
        setattr(server_module, "normalize_multi_platform_config", normalize_multi_platform_config)
        setattr(server_module, "load_multi_platform_config", lambda: load_multi_platform_config(_config_path(server_module)))
        setattr(server_module, "save_multi_platform_config", lambda data: save_multi_platform_config(data, _config_path(server_module)))
        setattr(server_module, "enabled_stream_platforms", enabled_stream_platforms)
        return True


def install_queue_rank_integration(queue_rank_module: Any) -> bool:
    """Reuse the existing QueueManager class hook instead of installing a competing hook."""
    if queue_rank_module is None:
        return False
    current = getattr(queue_rank_module, "attach_queue_rank_query", None)
    if not callable(current):
        return False
    if bool(getattr(current, "_bilipdj_multi_platform_bridge", False)):
        return True

    @functools.wraps(current)
    def attach_with_multi_platform(queue_manager_cls: type[Any]) -> bool:
        result = bool(current(queue_manager_cls))
        server_module = sys.modules.get(str(getattr(queue_manager_cls, "__module__", "") or ""))
        patch_server_module(server_module)
        return result

    setattr(attach_with_multi_platform, "_bilipdj_multi_platform_bridge", True)
    setattr(queue_rank_module, "attach_queue_rank_query", attach_with_multi_platform)
    return True


def patch_control_panel_multi_platform(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    if bool(getattr(panel_class, "_bilipdj_multi_platform_ui_installed", False)):
        return True
    module = sys.modules.get(str(getattr(panel_class, "__module__", "") or ""))
    if module is None:
        return False

    original_settings = getattr(panel_class, "_build_settings_tab", None)
    original_load = getattr(panel_class, "load_from_file", None)
    original_gather = getattr(panel_class, "gather_config", None)
    if not all(callable(item) for item in (original_settings, original_load, original_gather)):
        return False

    def _ensure_vars(self: Any) -> None:
        if not hasattr(self, "multi_platform_enabled_var"):
            self.multi_platform_enabled_var = module.tk.BooleanVar(value=False)
        if not hasattr(self, "multi_platform_bilibili_var"):
            self.multi_platform_bilibili_var = module.tk.BooleanVar(value=True)
        if not hasattr(self, "multi_platform_douyin_var"):
            self.multi_platform_douyin_var = module.tk.BooleanVar(value=True)

    @functools.wraps(original_settings)
    def build_settings_with_multi(self: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
        result = original_settings(self, frame, *args, **kwargs)
        _ensure_vars(self)
        notebook = getattr(self, "settings_notebook", None)
        add_page = getattr(self, "_add_scrollable_settings_page", None)
        if notebook is None or not callable(add_page):
            return result
        if bool(getattr(self, "_bilipdj_multi_platform_page_built", False)):
            return result

        inner = add_page(notebook, "多平台弹幕")
        inner.columnconfigure(0, weight=1)
        box = module.ttk.LabelFrame(inner, text="同时监听多个直播平台", padding=12)
        box.grid(row=0, column=0, sticky="ew")
        module.ttk.Checkbutton(
            box,
            text="启用多平台弹幕监听",
            variable=self.multi_platform_enabled_var,
        ).grid(row=0, column=0, columnspan=2, sticky="w", pady=(0, 8))
        module.ttk.Checkbutton(
            box,
            text="B站",
            variable=self.multi_platform_bilibili_var,
        ).grid(row=1, column=0, sticky="w", padx=(18, 28), pady=3)
        module.ttk.Checkbutton(
            box,
            text="抖音",
            variable=self.multi_platform_douyin_var,
        ).grid(row=1, column=1, sticky="w", pady=3)
        module.ttk.Label(
            box,
            text=(
                "启用后，当 B站房间号有效、且抖音已勾选“启用抖音配置”并填写 live_id 时，"
                "两路弹幕会同时连接并触发同一套排队指令。\n"
                "每条弹幕内部会生成平台用户标识符，日志栏显示 [B站] / [抖音] 来源。"
            ),
            wraplength=760,
        ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(10, 0))
        module.ttk.Label(
            box,
            text="当前已实现并行弹幕流：B站 + 抖音；其他预留平台以后接入 Relay 后可扩展。",
        ).grid(row=3, column=0, columnspan=2, sticky="w", pady=(8, 0))
        self._bilipdj_multi_platform_page_built = True
        return result

    @functools.wraps(original_load)
    def load_with_multi(self: Any, *args: Any, **kwargs: Any) -> Any:
        result = original_load(self, *args, **kwargs)
        _ensure_vars(self)
        try:
            backend = module.load_backend_server_module()
            config = backend.load_config()
            raw = config.get(MULTI_PLATFORM_CONFIG_KEY, {}) if isinstance(config, dict) else {}
        except Exception:
            raw = {}
        normalized = normalize_multi_platform_config(raw)
        self.multi_platform_enabled_var.set(bool(normalized["enabled"]))
        self.multi_platform_bilibili_var.set(bool(normalized["platforms"]["bilibili"]))
        self.multi_platform_douyin_var.set(bool(normalized["platforms"]["douyin"]))
        return result

    @functools.wraps(original_gather)
    def gather_with_multi(self: Any, *args: Any, **kwargs: Any) -> dict[str, Any]:
        payload = original_gather(self, *args, **kwargs)
        payload = dict(payload) if isinstance(payload, dict) else {}
        _ensure_vars(self)
        payload[MULTI_PLATFORM_CONFIG_KEY] = {
            "enabled": bool(self.multi_platform_enabled_var.get()),
            "platforms": {
                "bilibili": bool(self.multi_platform_bilibili_var.get()),
                "douyin": bool(self.multi_platform_douyin_var.get()),
            },
        }
        return payload

    setattr(panel_class, "_build_settings_tab", build_settings_with_multi)
    setattr(panel_class, "load_from_file", load_with_multi)
    setattr(panel_class, "gather_config", gather_with_multi)
    setattr(panel_class, "_bilipdj_multi_platform_ui_installed", True)
    return True


def install_control_panel_feature_patch() -> bool:
    """Wrap the existing feature-pack patch so the same class hook installs this UI."""
    try:
        if __package__:
            from . import control_panel_features
        else:
            import control_panel_features
    except Exception:
        return False
    current = getattr(control_panel_features, "patch_control_panel_features", None)
    if not callable(current):
        return False
    if bool(getattr(current, "_bilipdj_multi_platform_bridge", False)):
        return True

    @functools.wraps(current)
    def patch_features_with_multi(panel_class: type[Any]) -> bool:
        result = bool(current(panel_class))
        patch_control_panel_multi_platform(panel_class)
        return result

    setattr(patch_features_with_multi, "_bilipdj_multi_platform_bridge", True)
    setattr(control_panel_features, "patch_control_panel_features", patch_features_with_multi)
    return True


__all__ = [
    "DEFAULT_MULTI_PLATFORM_DANMU",
    "IMPLEMENTED_STREAM_PLATFORMS",
    "MULTI_PLATFORM_CONFIG_KEY",
    "MultiPlatformDanmuRelay",
    "enabled_stream_platforms",
    "install_control_panel_feature_patch",
    "install_queue_rank_integration",
    "load_multi_platform_config",
    "multi_platform_enabled",
    "normalize_multi_platform_config",
    "patch_control_panel_multi_platform",
    "patch_server_module",
    "platform_runtime_uid",
    "platform_user_key",
    "save_multi_platform_config",
]
