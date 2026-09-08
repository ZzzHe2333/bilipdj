from __future__ import annotations

import logging
import re
import threading
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

_PATCH_LOCK = threading.RLock()
_SLOT_NAME_RE = re.compile(r"^platform-slot-(\d+)\.yaml$")
_PROBE_PATH = "/api/platforms/youtube/probe"


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return int(default)


def _patch_runtime_status_and_probe(server_module: Any, youtube_protocol: Any) -> None:
    handler_class = getattr(server_module, "ApiHandler", None)
    if not isinstance(handler_class, type):
        return
    original_get = getattr(handler_class, "do_GET", None)
    if not callable(original_get) or bool(getattr(original_get, "_bilipdj_issue123_wrapped", False)):
        return

    def do_GET_with_issue123(self: Any) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path == _PROBE_PATH:
            if not self._require_loopback():
                return
            allowed = bool(youtube_protocol.probe_google_access(timeout=4.0))
            self._write_json({"status": "ok", "allowed": allowed})
            return

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

        active_platforms = relay_status.get("active_platforms", [])
        if not isinstance(active_platforms, (list, tuple)):
            active_platforms = []
        platforms = relay_status.get("platforms", {})
        if not isinstance(platforms, dict):
            platforms = {}

        payload = {
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
            "danmu_roomid": _safe_int(raw_room_id, 0),
            "danmu_room_id": room_id,
            "danmu_room_url": str(relay_status.get("room_url", "") or ""),
            "danmu_host": str(relay_status.get("host", "") or ""),
            "danmu_port": _safe_int(relay_status.get("port", 0), 0),
            "danmu_transport": str(relay_status.get("transport", "") or ""),
            "danmu_auth_uid": _safe_int(relay_status.get("auth_uid", 0), 0),
            "danmu_live_status": str(relay_status.get("live_status", "") or ""),
            "danmu_live_id": str(relay_status.get("live_id", "") or ""),
            "danmu_anchor_nickname": str(relay_status.get("anchor_nickname", "") or ""),
            "danmu_last_chat_seen_at": str(relay_status.get("last_chat_seen_at", "") or ""),
            "danmu_title": str(relay_status.get("title", "") or ""),
            "danmu_channel": str(relay_status.get("channel", "") or ""),
            "danmu_chat_mode": str(relay_status.get("chat_mode", "") or ""),
            "danmu_proxy": str(relay_status.get("proxy", "") or ""),
            # MultiPlatformRelayManager-specific status must remain visible to
            # callers instead of being lost while normalizing string room IDs.
            "active_platforms": list(active_platforms),
            "platforms": platforms,
        }
        self._write_json(payload)

    do_GET_with_issue123._bilipdj_issue123_wrapped = True  # type: ignore[attr-defined]
    handler_class.do_GET = do_GET_with_issue123


def _patch_youtube_end_state(youtube_protocol: Any) -> None:
    session_class = getattr(youtube_protocol, "YoutubeChatSession", None)
    relay_class = getattr(youtube_protocol, "YoutubeDanmuRelay", None)
    protocol_error = getattr(youtube_protocol, "YoutubeProtocolError", RuntimeError)
    if not isinstance(session_class, type) or not isinstance(relay_class, type):
        return

    ended_class = getattr(youtube_protocol, "YoutubeChatEnded", None)
    if not isinstance(ended_class, type):
        class YoutubeChatEnded(protocol_error):
            pass

        ended_class = YoutubeChatEnded
        youtube_protocol.YoutubeChatEnded = ended_class

    original_fetch = getattr(session_class, "fetch_once", None)
    if callable(original_fetch) and not bool(getattr(original_fetch, "_bilipdj_issue123_end_wrapped", False)):
        def fetch_once_with_end_state(self: Any):
            try:
                return original_fetch(self)
            except protocol_error as exc:
                if bool(getattr(self, "_bilipdj_continuation_exhausted", False)):
                    raise ended_class("红色小电视聊天已结束") from exc
                raise

        fetch_once_with_end_state._bilipdj_issue123_end_wrapped = True  # type: ignore[attr-defined]
        session_class.fetch_once = fetch_once_with_end_state

    original_run = getattr(relay_class, "run", None)
    if not callable(original_run) or bool(getattr(original_run, "_bilipdj_issue123_end_wrapped", False)):
        return

    def run_without_reconnect_after_end(self: Any) -> None:
        reconnect_default = float(getattr(youtube_protocol, "DEFAULT_RECONNECT_DELAY_SECONDS", 2.0) or 2.0)
        while not self._stop_event.is_set():
            cfg = self._load_runtime_cfg()
            self._reconnect_event.clear()
            try:
                self._connect_once(cfg)
            except ended_class as exc:
                self._mark_disconnected(str(exc))
                self._emit_status("danmu_ended", message=str(exc), platform="youtube")
                self.logger.info("红色小电视聊天已结束，停止自动重连")
                break
            except Exception as exc:  # noqa: BLE001
                self._mark_disconnected(str(exc))
                self._emit_status("danmu_disconnected", error=str(exc), platform="youtube")
                self.logger.warning("红色小电视连接错误: %s", exc)
            if self._stop_event.is_set() or not cfg.get("auto_reconnect", True):
                break
            self._stop_event.wait(float(cfg.get("reconnect_delay_seconds", reconnect_default)))

    run_without_reconnect_after_end._bilipdj_issue123_end_wrapped = True  # type: ignore[attr-defined]
    relay_class.run = run_without_reconnect_after_end


def _patch_settings_backup(backup_module: Any, server_module: Any) -> None:
    service_class = getattr(backup_module, "SettingsBackupService", None)
    if not isinstance(service_class, type):
        return
    if bool(getattr(service_class, "_bilipdj_issue123_slots_wrapped", False)):
        return

    max_slots = max(1, _safe_int(getattr(server_module, "MAX_QUEUE_ARCHIVE_SLOTS", 10), 10))
    slot_names = tuple(f"platform-slot-{slot}.yaml" for slot in range(1, max_slots + 1))
    current_files = tuple(getattr(backup_module, "SETTINGS_FILES", ()))
    backup_module.SETTINGS_FILES = tuple(dict.fromkeys((*current_files, *slot_names)))

    original_paths = service_class.settings_paths
    original_restore = service_class.restore_settings_zip

    def settings_paths_with_slots(self: Any) -> dict[str, Path]:
        paths = dict(original_paths(self))
        path_factory = getattr(self.server, "platform_config_path", None)
        if not callable(path_factory):
            return paths
        local_max = max(1, _safe_int(getattr(self.server, "MAX_QUEUE_ARCHIVE_SLOTS", max_slots), max_slots))
        for slot in range(1, local_max + 1):
            paths[f"platform-slot-{slot}.yaml"] = Path(path_factory(slot))
        return paths

    def restore_settings_zip_with_old_slot_compat(self: Any, data: bytes, *, httpd: Any | None = None):
        restored = self.validate_settings_zip(data)
        paths = self.settings_paths()
        slot_paths = {
            name: path
            for name, path in paths.items()
            if _SLOT_NAME_RE.fullmatch(str(name or ""))
        }
        has_slot_payload = any(name in restored for name in slot_paths)
        # Backups created before platform slots existed contain config.yaml but
        # no slot files. Clear stale local slots first; otherwise load_config()
        # immediately overlays the restored config with unrelated newer data.
        clear_legacy_slots = "config.yaml" in restored and not has_slot_payload
        if not clear_legacy_slots:
            return original_restore(self, data, httpd=httpd)

        snapshots: dict[str, bytes | None] = {}
        for name, path in slot_paths.items():
            try:
                snapshots[name] = path.read_bytes() if path.is_file() else None
            except OSError:
                snapshots[name] = None
        try:
            for path in slot_paths.values():
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass
            return original_restore(self, data, httpd=httpd)
        except Exception:
            for name, path in slot_paths.items():
                previous = snapshots.get(name)
                try:
                    if previous is None:
                        path.unlink(missing_ok=True)
                    else:
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(previous)
                except OSError:
                    pass
            raise

    service_class.settings_paths = settings_paths_with_slots
    service_class.restore_settings_zip = restore_settings_zip_with_old_slot_compat
    service_class._bilipdj_issue123_slots_wrapped = True


def _patch_control_writer(issue79_module: Any, server_module: Any) -> None:
    if issue79_module is None:
        return
    current = getattr(issue79_module, "_write_control_html", None)
    if not callable(current) or bool(getattr(current, "_bilipdj_issue123_writer", False)):
        return

    def write_control_html_with_issue123(self: Any, module: Any) -> None:
        if not self._require_loopback():
            return
        file_path = Path(module.UI_DIR) / "control.html"
        if not file_path.is_file():
            self._write_json({"status": "error", "message": "Web control panel is missing"}, status=404)
            return
        text = file_path.read_text(encoding="utf-8")
        css_tags = ('<link rel="stylesheet" href="/control_issue79.css">',)
        js_files = (
            "control_issue79.js",
            "control_huya.js",
            "control_redtv.js",
            "control_purple_mouse.js",
            "control_issue123.js",
        )
        for tag in css_tags:
            if tag not in text:
                text = text.replace("</head>", f"  {tag}\n</head>")
        for filename in js_files:
            if not (Path(module.UI_DIR) / filename).is_file():
                continue
            tag = f'<script src="/{filename}"></script>'
            if tag not in text:
                text = text.replace("</body>", f"  {tag}\n</body>")
        body = text.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    write_control_html_with_issue123._bilipdj_issue123_writer = True  # type: ignore[attr-defined]
    issue79_module._write_control_html = write_control_html_with_issue123


def install_issue123_guard(
    server_module: Any,
    issue79_module: Any,
    youtube_protocol: Any,
    backup_module: Any,
) -> bool:
    if server_module is None:
        return False
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_issue123_guard_installed", False)):
            return True
        _patch_runtime_status_and_probe(server_module, youtube_protocol)
        _patch_youtube_end_state(youtube_protocol)
        _patch_settings_backup(backup_module, server_module)
        _patch_control_writer(issue79_module, server_module)
        server_module._issue123_guard_installed = True
        return True


__all__ = ["install_issue123_guard"]
