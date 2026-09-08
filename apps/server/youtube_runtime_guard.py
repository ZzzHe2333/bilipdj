"""Red TV runtime integration for the existing multi-platform backend."""
from __future__ import annotations

import copy
import json
import threading
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from . import youtube_protocol

_PATCH_LOCK = threading.RLock()


def _append_unique(values: Any, item: str) -> tuple[str, ...]:
    result: list[str] = []
    for value in values if isinstance(values, (list, tuple)) else ():
        text = str(value or "").strip().lower()
        if text and text not in result:
            result.append(text)
    if item not in result:
        result.append(item)
    return tuple(result)


def _install_relay_factory(server_module: Any) -> None:
    original = getattr(server_module, "_create_danmu_relay", None)
    if not callable(original) or bool(getattr(original, "_bilipdj_redtv_wrapped", False)):
        return

    def create_danmu_relay_with_redtv(server: Any) -> Any:
        try:
            platform = str(
                server_module._get_runtime_platform(getattr(server, "runtime_config", {})) or ""
            ).strip().lower()
        except Exception:
            platform = str(getattr(server, "runtime_config", {}).get("platform", "") or "").strip().lower()
        if platform == "youtube":
            return youtube_protocol.YoutubeDanmuRelay(server)
        return original(server)

    create_danmu_relay_with_redtv._bilipdj_redtv_wrapped = True  # type: ignore[attr-defined]
    server_module._create_danmu_relay = create_danmu_relay_with_redtv


def _patch_issue79(issue79_module: Any) -> None:
    if issue79_module is None:
        return

    issue79_module.SUPPORTED_ACTIVE_PLATFORMS = _append_unique(
        getattr(issue79_module, "SUPPORTED_ACTIVE_PLATFORMS", ("bilibili", "douyin")),
        "youtube",
    )

    original_runtime = getattr(issue79_module, "_runtime_for_platform", None)
    if callable(original_runtime) and not bool(getattr(original_runtime, "_bilipdj_redtv_wrapped", False)):
        def runtime_for_platform_with_redtv(server: Any, platform: str) -> dict[str, Any]:
            runtime = original_runtime(server, platform)
            if str(platform).strip().lower() == "youtube":
                section = dict(runtime.get("youtube", {}) or {})
                section["enabled"] = True
                runtime["youtube"] = section
            return runtime

        runtime_for_platform_with_redtv._bilipdj_redtv_wrapped = True  # type: ignore[attr-defined]
        issue79_module._runtime_for_platform = runtime_for_platform_with_redtv

    original_writer = getattr(issue79_module, "_write_control_html", None)
    if callable(original_writer) and not bool(getattr(original_writer, "_bilipdj_redtv_wrapped", False)):
        def write_control_html_with_redtv(self: Any, server_module: Any) -> None:
            if not self._require_loopback():
                return
            file_path = Path(server_module.UI_DIR) / "control.html"
            if not file_path.is_file():
                self._write_json({"status": "error", "message": "Web control panel is missing"}, status=404)
                return
            text = file_path.read_text(encoding="utf-8")
            css_tag = '<link rel="stylesheet" href="/control_issue79.css">'
            issue_js_tag = '<script src="/control_issue79.js"></script>'
            huya_js_tag = '<script src="/control_huya.js"></script>'
            redtv_js_tag = '<script src="/control_redtv.js"></script>'
            if css_tag not in text:
                text = text.replace("</head>", f"  {css_tag}\n</head>")
            if issue_js_tag not in text:
                text = text.replace("</body>", f"  {issue_js_tag}\n</body>")
            if (Path(server_module.UI_DIR) / "control_huya.js").is_file() and huya_js_tag not in text:
                text = text.replace("</body>", f"  {huya_js_tag}\n</body>")
            if redtv_js_tag not in text:
                text = text.replace("</body>", f"  {redtv_js_tag}\n</body>")
            body = text.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        write_control_html_with_redtv._bilipdj_redtv_wrapped = True  # type: ignore[attr-defined]
        issue79_module._write_control_html = write_control_html_with_redtv


def _patch_runtime_status(server_module: Any) -> None:
    """Keep `/api/runtime-status` compatible with string room identifiers.

    The legacy endpoint coerces relay_status['roomid'] to int. YouTube uses an
    11-character Video ID, so intercept only this endpoint and emit the same
    payload shape with a safe numeric compatibility field plus `danmu_room_id`.
    """
    handler_class = getattr(server_module, "ApiHandler", None)
    if not isinstance(handler_class, type):
        return
    original_get = getattr(handler_class, "do_GET", None)
    if not callable(original_get) or bool(getattr(original_get, "_bilipdj_redtv_status_wrapped", False)):
        return

    def do_GET_with_redtv_status(self: Any) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path != "/api/runtime-status":
            return original_get(self)

        relay_status = (
            self.server.danmu_relay.get_runtime_status()
            if hasattr(self.server, "danmu_relay")
            else {}
        )
        if not isinstance(relay_status, dict):
            relay_status = {}
        runtime_platform = server_module._get_runtime_platform(self.server.runtime_config)
        relay_platform = str(relay_status.get("platform", runtime_platform) or runtime_platform)
        raw_room_id = relay_status.get("roomid", relay_status.get("room_id", 0))
        room_id = str(raw_room_id or "")
        try:
            roomid_compat = int(raw_room_id or 0)
        except (TypeError, ValueError):
            roomid_compat = 0

        self._write_json(
            {
                "status": "ok",
                "ws_clients": self.server.ws_hub.client_count,
                "danmu_stream_active": bool(relay_status.get("connected", False)),
                "danmu_platform": relay_platform,
                "last_message_at": self.server.ws_hub.last_message_at,
                "danmu_connected": bool(relay_status.get("connected", False)),
                "danmu_last_packet_at": str(relay_status.get("last_packet_at", "") or ""),
                "danmu_last_connect_at": str(relay_status.get("last_connect_at", "") or ""),
                "danmu_last_disconnect_at": str(relay_status.get("last_disconnect_at", "") or ""),
                "danmu_last_disconnect_reason": str(relay_status.get("last_disconnect_reason", "") or ""),
                "danmu_idle_seconds": relay_status.get("idle_seconds"),
                "danmu_roomid": roomid_compat,
                "danmu_room_id": room_id,
                "danmu_host": str(relay_status.get("host", "") or ""),
                "danmu_port": int(relay_status.get("port", 0) or 0),
                "danmu_transport": str(relay_status.get("transport", "") or ""),
                "danmu_auth_uid": int(relay_status.get("auth_uid", 0) or 0),
                "danmu_live_status": str(relay_status.get("live_status", "") or ""),
                "danmu_live_id": str(relay_status.get("live_id", "") or ""),
                "danmu_anchor_nickname": str(relay_status.get("anchor_nickname", "") or ""),
                "danmu_last_chat_seen_at": str(relay_status.get("last_chat_seen_at", "") or ""),
            }
        )

    do_GET_with_redtv_status._bilipdj_redtv_status_wrapped = True  # type: ignore[attr-defined]
    handler_class.do_GET = do_GET_with_redtv_status


def _patch_active_platform_persistence(server_module: Any, issue79_module: Any) -> None:
    """Persist active platform selection without changing the legacy YAML writer.

    Store the active list in the active platform slot's `youtube.extra` section.
    This keeps existing config-schema compatibility while making the selection
    survive save -> reload -> restart. The issue79 normalizer is wrapped to read
    that stored value when the transient top-level key is absent.
    """
    if issue79_module is None:
        return
    original_normalize = getattr(issue79_module, "_normalize_active_platforms", None)
    if not callable(original_normalize) or bool(getattr(original_normalize, "_bilipdj_redtv_persist_wrapped", False)):
        return

    def normalize_active_platforms_with_persist(module: Any, config: Any) -> tuple[str, ...]:
        cfg = config if isinstance(config, dict) else {}
        if not isinstance(cfg.get("active_platforms"), (list, tuple)):
            youtube = cfg.get("youtube", {})
            if isinstance(youtube, dict):
                extra = youtube.get("extra", {})
                if isinstance(extra, dict):
                    stored = extra.get("active_platforms")
                    if isinstance(stored, str):
                        try:
                            decoded = json.loads(stored)
                        except json.JSONDecodeError:
                            decoded = None
                        if isinstance(decoded, list):
                            cfg = dict(cfg)
                            cfg["active_platforms"] = decoded
        return original_normalize(module, cfg)

    normalize_active_platforms_with_persist._bilipdj_redtv_persist_wrapped = True  # type: ignore[attr-defined]
    issue79_module._normalize_active_platforms = normalize_active_platforms_with_persist

    handler_class = getattr(server_module, "ApiHandler", None)
    original_post = getattr(handler_class, "do_POST", None) if isinstance(handler_class, type) else None
    if not callable(original_post) or bool(getattr(original_post, "_bilipdj_redtv_active_wrapped", False)):
        return

    def do_POST_with_active_persistence(self: Any) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path != "/api/platforms/active":
            return original_post(self)
        if not self._require_loopback():
            return

        payload = issue79_module._read_json_body(self)
        raw = payload.get("active", [])
        active: list[str] = []
        supported = tuple(getattr(issue79_module, "SUPPORTED_ACTIVE_PLATFORMS", ()))
        if isinstance(raw, (list, tuple)):
            for value in raw:
                name = str(value or "").strip().lower()
                if name in supported and name not in active:
                    active.append(name)

        config = server_module.load_config()
        config["active_platforms"] = active
        if active:
            config["platform"] = active[0]
        douyin = dict(config.get("douyin", {}) or {})
        douyin["enabled"] = "douyin" in active
        config["douyin"] = douyin

        youtube = dict(config.get("youtube", {}) or {})
        youtube_extra = dict(youtube.get("extra", {}) or {})
        youtube_extra["active_platforms"] = json.dumps(active, ensure_ascii=False, separators=(",", ":"))
        youtube["extra"] = youtube_extra
        config["youtube"] = youtube

        server_module.save_config(config)
        self.server.runtime_config = server_module.load_config()
        # Keep the in-memory explicit value too, including the intentionally empty list.
        self.server.runtime_config["active_platforms"] = list(active)
        server_module._ensure_danmu_relay(self.server, reconnect=True)
        self._write_json(issue79_module._platform_payload(server_module, self.server))

    do_POST_with_active_persistence._bilipdj_redtv_active_wrapped = True  # type: ignore[attr-defined]
    handler_class.do_POST = do_POST_with_active_persistence


def install_youtube_runtime_guard(server_module: Any, issue79_module: Any | None = None) -> bool:
    if server_module is None:
        return False

    with _PATCH_LOCK:
        if bool(getattr(server_module, "_redtv_runtime_guard_installed", False)):
            return True

        server_module.SUPPORTED_RUNTIME_PLATFORMS = _append_unique(
            getattr(server_module, "SUPPORTED_RUNTIME_PLATFORMS", ("bilibili", "douyin")),
            "youtube",
        )
        server_module.RESERVED_RUNTIME_PLATFORMS = _append_unique(
            getattr(server_module, "RESERVED_RUNTIME_PLATFORMS", ()),
            "youtube",
        )
        all_platforms = list(getattr(server_module, "SUPPORTED_RUNTIME_PLATFORMS", ()))
        all_platforms.extend(getattr(server_module, "RESERVED_RUNTIME_PLATFORMS", ()))
        server_module.ALL_RUNTIME_PLATFORMS = tuple(dict.fromkeys(str(value) for value in all_platforms))

        display_names = getattr(server_module, "PLATFORM_DISPLAY_NAMES", None)
        if isinstance(display_names, dict):
            display_names["youtube"] = "红色小电视"

        defaults = getattr(server_module, "DEFAULT_CONFIG", None)
        section_default = getattr(server_module, "RESERVED_PLATFORM_SECTION_DEFAULT", {})
        if isinstance(defaults, dict) and "youtube" not in defaults:
            defaults["youtube"] = copy.deepcopy(section_default if isinstance(section_default, dict) else {})

        _install_relay_factory(server_module)
        _patch_issue79(issue79_module)
        _patch_runtime_status(server_module)
        _patch_active_platform_persistence(server_module, issue79_module)

        server_module.youtube_protocol = youtube_protocol
        server_module.YoutubeDanmuRelay = youtube_protocol.YoutubeDanmuRelay
        server_module._redtv_runtime_guard_installed = True
        return True


__all__ = ["install_youtube_runtime_guard"]
