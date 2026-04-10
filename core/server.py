from __future__ import annotations

import base64
import copy
import csv
import datetime as dt
import hashlib
import json
import logging
import os
import random
import re
import socket
import struct
import sys
import threading
import time
import urllib.error
import urllib.request
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

if __package__:
    from . import bilibili_protocol
else:
    import bilibili_protocol

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9816
MAX_QUEUE_ARCHIVE_SLOTS = 10
DEFAULT_PLATFORM = "bilibili"

REPO_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = Path(__file__).resolve().parent  # bilipdj/core/
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", REPO_DIR))
APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else REPO_DIR
_YAML_DIR = APP_DIR if getattr(sys, "frozen", False) else CORE_DIR
BUNDLE_CORE_DIR = BUNDLE_DIR / "core"
RUNTIME_CORE_DIR = APP_DIR / "core" if getattr(sys, "frozen", False) else CORE_DIR
BUNDLE_UI_DIR = BUNDLE_CORE_DIR / "ui"
UI_DIR = RUNTIME_CORE_DIR / "ui"
CONFIG_PATH = _YAML_DIR / "config.yaml"
LOG_DIR = APP_DIR / "log"
PD_DIR = APP_DIR / "core" / "cd"
QUEUE_STATE_PATH = PD_DIR / "queue_archive_state.json"
BLACKLIST_PATH = PD_DIR / "blacklist.csv"
QUANXIAN_PATH = _YAML_DIR / "quanxian.yaml"
KAIGUAN_PATH = _YAML_DIR / "kaiguan.yaml"
STYLE_PATH = _YAML_DIR / "style.json"
LIVE_STYLE_CSS_PATH = UI_DIR / "moren.css"

WS_MAGIC_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


STYLE_CSS_VAR_MAP: dict[str, str] = {
    "bg1": "--bg1",
    "bg2": "--bg2",
    "bg3": "--bg3",
    "text_color": "--text-color",
    "queue_font_size": "--queue-font-size",
    "queue_font_weight": "--queue-font-weight",
    "queue_font_style": "--queue-font-style",
    "text_grad_start": "--text-grad-start",
    "text_grad_end": "--text-grad-end",
    "text_stroke_color": "--text-stroke",
    "text_stroke_enabled": "--text-stroke-enabled",
}


class BackendServer(ThreadingHTTPServer):
    daemon_threads = True
    runtime_config: dict[str, Any]
    logger: logging.Logger
    queue_archive: "QueueArchiveManager"
    queue_manager: "QueueManager"
    ws_hub: "WebSocketHub"
    danmu_relay: "BilibiliDanmuRelay"


class WebSocketHub:
    def __init__(self, logger: logging.Logger) -> None:
        self.logger = logger
        self._clients: set[socket.socket] = set()
        self._lock = threading.Lock()
        self.last_message_at: str = ""

    @property
    def client_count(self) -> int:
        with self._lock:
            return len(self._clients)

    def register(self, conn: socket.socket) -> None:
        with self._lock:
            self._clients.add(conn)
            count = len(self._clients)
        self.logger.info("WebSocket 客户端已连接，当前 %s 个", count)

    def unregister(self, conn: socket.socket) -> None:
        with self._lock:
            self._clients.discard(conn)
            count = len(self._clients)
        self.logger.info("WebSocket 客户端已断开，当前 %s 个", count)

    def broadcast_json(self, sender: socket.socket | None, payload: dict[str, Any]) -> None:
        text = json.dumps(payload, ensure_ascii=False)
        self.broadcast_text(sender, text)

    def broadcast_text(self, sender: socket.socket | None, text: str) -> None:
        dead: list[socket.socket] = []
        with self._lock:
            targets = list(self._clients)

        for conn in targets:
            if sender is not None and conn is sender:
                continue
            try:
                _ws_send_text(conn, text)
            except OSError:
                dead.append(conn)

        if dead:
            with self._lock:
                for conn in dead:
                    self._clients.discard(conn)

    def mark_message(self) -> None:
        self.last_message_at = dt.datetime.now(dt.timezone.utc).isoformat()


def _ws_send_text(conn: socket.socket, text: str, opcode: int = 0x1) -> None:
    payload = text.encode("utf-8")
    header = bytearray([0x80 | (opcode & 0x0F)])
    length = len(payload)

    if length <= 125:
        header.append(length)
    elif length <= 65535:
        header.append(126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(127)
        header.extend(struct.pack("!Q", length))

    conn.sendall(bytes(header) + payload)



def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if value == "":
        return ""
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if value.lower() in {"null", "none"}:
        return None
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def _strip_inline_yaml_comment(value: str) -> str:
    in_single = False
    in_double = False
    escaped = False
    result_chars: list[str] = []
    for ch in value:
        if escaped:
            result_chars.append(ch)
            escaped = False
            continue
        if ch == "\\":
            result_chars.append(ch)
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
            result_chars.append(ch)
            continue
        if ch == '"' and not in_single:
            in_double = not in_double
            result_chars.append(ch)
            continue
        if ch == "#" and not in_single and not in_double:
            break
        result_chars.append(ch)
    return "".join(result_chars).rstrip()


def _next_meaningful_line(lines: list[str], start_index: int) -> tuple[int, str] | None:
    for idx in range(start_index, len(lines)):
        stripped = lines[idx].strip()
        if stripped and not stripped.startswith("#"):
            return idx, lines[idx]
    return None


def load_simple_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any] | list[Any]]] = [(-1, root)]
    lines = path.read_text(encoding="utf-8").splitlines()

    for index, raw_line in enumerate(lines):
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        while stack and indent <= stack[-1][0]:
            stack.pop()

        current = stack[-1][1] if stack else root
        if stripped.startswith("- "):
            if not isinstance(current, list):
                continue
            item_value = stripped[2:].strip()
            if item_value == "":
                child: dict[str, Any] = {}
                current.append(child)
                stack.append((indent, child))
            else:
                current.append(_parse_scalar(item_value))
            continue

        if ":" not in stripped or not isinstance(current, dict):
            continue

        key, value = stripped.split(":", 1)
        key = key.strip()
        value = _strip_inline_yaml_comment(value.strip())

        if value == "":
            next_line = _next_meaningful_line(lines, index + 1)
            child: dict[str, Any] | list[Any]
            if next_line is not None:
                next_raw = next_line[1]
                next_indent = len(next_raw) - len(next_raw.lstrip(" "))
                next_stripped = next_raw.strip()
                if next_indent > indent and next_stripped.startswith("- "):
                    child = []
                else:
                    child = {}
            else:
                child = {}
            current[key] = child
            stack.append((indent, child))
        else:
            current[key] = _parse_scalar(value)

    return root


def _merge_config(defaults: dict[str, Any], custom: dict[str, Any]) -> dict[str, Any]:
    merged = dict(defaults)
    for key, value in custom.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_config(merged[key], value)
        else:
            merged[key] = value
    return merged


MYJS_LIST_KEYS = {"admins", "ban_admins", "jianzhang"}


def _normalize_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, dict) and not value:
        return []
    if value is None:
        return []

    text = str(value).strip()
    if not text:
        return []

    try:
        decoded = json.loads(text)
    except json.JSONDecodeError:
        decoded = None

    if isinstance(decoded, list):
        return [str(item).strip() for item in decoded if str(item).strip()]

    parts = text.replace(",", "\n").splitlines()
    return [part.strip() for part in parts if part.strip()]


def _normalize_myjs_config(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}

    normalized: dict[str, Any] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        if key in MYJS_LIST_KEYS:
            normalized[key] = _normalize_string_list(value)
        else:
            normalized[key] = value
    return normalized


def _dedupe_string_list(value: Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for item in _normalize_string_list(value):
        if item in seen:
            continue
        seen.add(item)
        result.append(item)
    return result


def _yaml_quote_string(value: Any) -> str:
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


def _read_raw_config() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    return load_simple_yaml(CONFIG_PATH)


ARCHIVE_HEADER_SEQ = "序号"
ARCHIVE_HEADER_ID = "id"
ARCHIVE_HEADER_CONTENT = "内容"
ARCHIVE_HEADER_LAST_OPERATION_AT = "最后操作时间"
ARCHIVE_META_TIMESTAMP = "最后操作时间"
ARCHIVE_META_ACTOR = "操作人"
ARCHIVE_META_MESSAGE = "操作说明"


def _format_archive_timestamp(value: dt.datetime | str | None = None) -> str:
    if isinstance(value, dt.datetime):
        return value.isoformat(sep=" ", timespec="seconds")
    text = str(value or "").strip()
    if text:
        return text
    return dt.datetime.now().isoformat(sep=" ", timespec="seconds")


def _queue_labeled_content(label: str, extra: str) -> str:
    extra_text = str(extra).strip()
    return label if not extra_text else f"{label} {extra_text}"


def queue_item_to_parts(item: Any) -> tuple[str, str]:
    text = str(item or "").strip()
    if not text:
        return "", ""

    patterns = (
        (r"^(?:官|[Gg])\|([^ ]+)(?:\s+(.*))?$", "[官服]"),
        (r"^[Bb]\|([^ ]+)(?:\s+(.*))?$", "[B服]"),
        (r"^(?:米|[Mm])\|([^ ]+)(?:\s+(.*))?$", "[米服]"),
        (r"^[Ss]\|([^ ]+)(?:\s+(.*))?$", "[超级]"),
    )
    for pattern, label in patterns:
        match = re.match(pattern, text)
        if match:
            return match.group(1).strip(), _queue_labeled_content(label, match.group(2) or "")

    super_match = re.match(r"^<([^>]+)>(.*)$", text)
    if super_match:
        return super_match.group(1).strip(), _queue_labeled_content("[超级]", super_match.group(2) or "")

    parts = text.split(" ", 1)
    item_id = parts[0].strip()
    content = parts[1].strip() if len(parts) > 1 else ""
    return item_id, content


def queue_parts_to_item(item_id: Any, content: Any) -> str:
    item_id_text = str(item_id or "").strip()
    content_text = str(content or "").strip()

    if not item_id_text:
        return content_text

    labels = (
        ("[官服]", "官|"),
        ("[B服]", "B|"),
        ("[米服]", "米|"),
    )
    for label, prefix in labels:
        if content_text == label or content_text.startswith(f"{label} "):
            extra = content_text[len(label):].strip()
            return f"{prefix}{item_id_text} {extra}".rstrip()

    if content_text == "[超级]" or content_text.startswith("[超级] "):
        extra = content_text[len("[超级]"):].strip()
        return f"<{item_id_text}>{extra}" if extra else f"<{item_id_text}>"

    return f"{item_id_text} {content_text}".rstrip()


def build_queue_entry(item_id: Any, content: Any, last_operation_at: Any = "") -> dict[str, str]:
    timestamp = str(last_operation_at or "").strip()
    return {
        "id": str(item_id or "").strip(),
        "content": str(content or "").strip(),
        "last_operation_at": _format_archive_timestamp(timestamp) if timestamp else "",
    }


def queue_item_to_entry(item: Any, last_operation_at: Any = "") -> dict[str, str]:
    if isinstance(item, dict):
        return build_queue_entry(
            item.get("id", ""),
            item.get("content", ""),
            item.get("last_operation_at", last_operation_at),
        )
    item_id, content = queue_item_to_parts(item)
    return build_queue_entry(item_id, content, last_operation_at)


def queue_items_to_entries(queue_items: list[Any], last_operation_at: Any = "") -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    for item in queue_items:
        entry = queue_item_to_entry(item, last_operation_at=last_operation_at)
        if entry.get("id") or entry.get("content"):
            entries.append(entry)
    return entries


def queue_entries_to_items(entries: list[dict[str, Any]]) -> list[str]:
    items: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        item = queue_parts_to_item(entry.get("id", ""), entry.get("content", ""))
        if item.strip():
            items.append(item)
    return items


def parse_queue_archive_rows(rows: list[list[str]]) -> tuple[dict[str, str], list[dict[str, str]]]:
    meta = {"timestamp": "", "actor": "", "message": ""}
    meta_aliases = {
        "timestamp": "timestamp",
        "last_operation_at": "timestamp",
        ARCHIVE_META_TIMESTAMP: "timestamp",
        "actor": "actor",
        ARCHIVE_META_ACTOR: "actor",
        "message": "message",
        ARCHIVE_META_MESSAGE: "message",
    }
    entries: list[dict[str, str]] = []

    for row in rows:
        if not row or not any(str(cell).strip() for cell in row):
            continue

        first = str(row[0]).strip()
        lowered = first.lower()

        alias = meta_aliases.get(first) or meta_aliases.get(lowered)
        if alias in meta:
            meta[alias] = str(row[1]).strip() if len(row) > 1 else ""
            continue

        if first == ARCHIVE_HEADER_SEQ or lowered in {"position", "seq"}:
            continue

        if first.isdigit():
            if len(row) >= 3:
                item_id = str(row[1]).strip()
                content = str(row[2]).strip()
                last_operation_at = str(row[3]).strip() if len(row) > 3 else ""
            elif len(row) >= 2:
                item_id, content = queue_item_to_parts(row[1])
                last_operation_at = ""
            else:
                continue
            entries.append(build_queue_entry(item_id, content, last_operation_at))

    fallback_timestamp = str(meta.get("timestamp", "")).strip()
    if fallback_timestamp:
        for entry in entries:
            if not entry.get("last_operation_at", ""):
                entry["last_operation_at"] = fallback_timestamp

    return meta, entries


def latest_queue_entry_timestamp(entries: list[dict[str, Any]]) -> str:
    latest_text = ""
    latest_value: dt.datetime | None = None
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("last_operation_at", "") or "").strip()
        if not text:
            continue
        try:
            candidate = dt.datetime.fromisoformat(text)
        except ValueError:
            if not latest_text:
                latest_text = text
            continue
        if latest_value is None or candidate > latest_value:
            latest_value = candidate
            latest_text = _format_archive_timestamp(candidate)
    return latest_text


def read_queue_archive_entries(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.reader(f))
    except OSError:
        return []
    return parse_queue_archive_rows(rows)[1]


def write_queue_archive_entries(path: Path, entries: list[dict[str, Any]], meta: dict[str, Any] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([ARCHIVE_HEADER_SEQ, ARCHIVE_HEADER_ID, ARCHIVE_HEADER_CONTENT, ARCHIVE_HEADER_LAST_OPERATION_AT])
        for idx, entry in enumerate(entries, start=1):
            normalized = queue_item_to_entry(entry)
            writer.writerow(
                [
                    idx,
                    normalized.get("id", ""),
                    normalized.get("content", ""),
                    normalized.get("last_operation_at", ""),
                ]
            )


def blacklist_names_to_entries(names: list[Any]) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    seen: set[str] = set()
    for name in names:
        text = str(name or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        entries.append({"id": text, "content": ""})
    return entries


def blacklist_entries_to_names(entries: list[dict[str, Any]]) -> list[str]:
    names: list[str] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        text = str(entry.get("id", "") or entry.get("content", "")).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        names.append(text)
    return names


def read_blacklist_entries(path: Path = BLACKLIST_PATH) -> list[dict[str, str]]:
    return blacklist_names_to_entries(blacklist_entries_to_names(read_queue_archive_entries(path)))


def write_blacklist_entries(path: Path, entries: list[dict[str, Any]]) -> None:
    write_queue_archive_entries(path, blacklist_names_to_entries(blacklist_entries_to_names(entries)))


def ensure_queue_archive_row_timestamps(path: Path) -> None:
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.reader(f))
    except OSError:
        return

    meta, entries = parse_queue_archive_rows(rows)
    header_row = next(
        (row for row in rows if row and str(row[0]).strip() == ARCHIVE_HEADER_SEQ),
        [],
    )
    has_last_operation_column = (
        len(header_row) > 3 and str(header_row[3]).strip() == ARCHIVE_HEADER_LAST_OPERATION_AT
    )
    has_file_level_timestamp = any(
        row and str(row[0]).strip() == ARCHIVE_META_TIMESTAMP and str(row[0]).strip() != ARCHIVE_HEADER_SEQ
        for row in rows
    )
    missing_entry_timestamp = any(not str(entry.get("last_operation_at", "")).strip() for entry in entries)

    if not (has_file_level_timestamp or missing_entry_timestamp or not has_last_operation_column):
        return

    fallback_timestamp = str(meta.get("timestamp", "")).strip()
    if not fallback_timestamp:
        fallback_timestamp = _format_archive_timestamp(dt.datetime.fromtimestamp(path.stat().st_mtime))
    normalized_entries = [
        build_queue_entry(
            entry.get("id", ""),
            entry.get("content", ""),
            entry.get("last_operation_at", "") or fallback_timestamp,
        )
        for entry in entries
    ]
    write_queue_archive_entries(
        path,
        normalized_entries,
        meta={"actor": meta.get("actor", ""), "message": meta.get("message", "")},
    )


DEFAULT_CONFIG: dict[str, Any] = {
    "server": {"host": DEFAULT_HOST, "port": DEFAULT_PORT},
    "platform": DEFAULT_PLATFORM,
    "bilibili": {"roomid": 3049445, "uid": 0, "cookie": ""},
    "douyin": {
        "enabled": False,
        "live_id": "",
        "cookie": "",
        "signature": "",
        "bootstrap": {"cursor": "", "internal_ext": ""},
        "ws": {
            "auto_reconnect": True,
            "heartbeat_interval_seconds": 5.0,
            "reconnect_delay_seconds": 2.0,
        },
        "live_info": {
            "room_id": "",
            "user_id": "",
            "user_unique_id": "",
            "anchor_id": "",
            "sec_uid": "",
            "ttwid": "",
        },
        "extra_query": {},
    },
    "qr_login": {
        "last_success_at": "",
        "qrcode_key": "",
        "poll_code": -1,
        "message": "",
        "cookie": "",
    },
    "callback": {"enabled": False, "url": "", "auth_token": "", "timeout_seconds": 5},
    "myjs": {
        "paidui_list_length_max": 100,
        "all_suoyourenbukepaidui": False,
        "fangguan_can_doing": False,
        "jianzhangchadui": False,
        "admins": [],
        "ban_admins": [],
        "jianzhang": [],
    },
    "ui": {
        "startup_splash_seconds": 5,
        "auto_start_backend": False,
        "language": "中文",
        "overlay_window": {"width": 860, "height": 420, "scale": 100},
    },
    "logging": {"level": "INFO", "retention_days": 7},
    "queue_archive": {"enabled": True, "slots": MAX_QUEUE_ARCHIVE_SLOTS, "active_slot": 1},
    "platform_config_archive": {"slots": MAX_QUEUE_ARCHIVE_SLOTS, "active_slot": 1},
}


def _get_bilibili_config(config: dict[str, Any] | None) -> dict[str, Any]:
    defaults = dict(DEFAULT_CONFIG.get("bilibili", {}))
    if not isinstance(config, dict):
        return defaults

    merged = dict(defaults)
    legacy_api_cfg = config.get("api", {})
    bilibili_cfg = config.get("bilibili", {})
    if isinstance(legacy_api_cfg, dict):
        if "roomid" in legacy_api_cfg:
            merged["roomid"] = _to_int(legacy_api_cfg.get("roomid", merged.get("roomid", 0)), merged["roomid"])
        if "uid" in legacy_api_cfg:
            merged["uid"] = _to_int(legacy_api_cfg.get("uid", merged.get("uid", 0)), merged["uid"])
        if "cookie" in legacy_api_cfg:
            merged["cookie"] = str(legacy_api_cfg.get("cookie", merged.get("cookie", "")) or "")

    if isinstance(bilibili_cfg, dict):
        if "roomid" in bilibili_cfg:
            bilibili_roomid = _to_int(bilibili_cfg.get("roomid", defaults.get("roomid", 0)), defaults.get("roomid", 0))
            if bilibili_roomid != defaults.get("roomid", 0) or merged["roomid"] == defaults.get("roomid", 0) or not isinstance(legacy_api_cfg, dict) or "roomid" not in legacy_api_cfg:
                merged["roomid"] = bilibili_roomid
        if "uid" in bilibili_cfg:
            bilibili_uid = _to_int(bilibili_cfg.get("uid", defaults.get("uid", 0)), defaults.get("uid", 0))
            if bilibili_uid != defaults.get("uid", 0) or merged["uid"] == defaults.get("uid", 0) or not isinstance(legacy_api_cfg, dict) or "uid" not in legacy_api_cfg:
                merged["uid"] = bilibili_uid
        if "cookie" in bilibili_cfg:
            bilibili_cookie = str(bilibili_cfg.get("cookie", defaults.get("cookie", "")) or "")
            if bilibili_cookie != str(defaults.get("cookie", "") or "") or not str(merged.get("cookie", "") or "").strip() or not isinstance(legacy_api_cfg, dict) or "cookie" not in legacy_api_cfg:
                merged["cookie"] = bilibili_cookie
    return merged


def _normalize_bilibili_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(config)
    bilibili_cfg = _get_bilibili_config(normalized)
    normalized["bilibili"] = bilibili_cfg
    normalized["api"] = dict(bilibili_cfg)
    return normalized


def _normalize_platform_name(value: Any) -> str:
    platform = str(value or DEFAULT_PLATFORM).strip().lower()
    if platform not in {"bilibili", "douyin"}:
        return DEFAULT_PLATFORM
    return platform


def _get_douyin_config(config: dict[str, Any] | None) -> dict[str, Any]:
    defaults = copy.deepcopy(DEFAULT_CONFIG.get("douyin", {}))
    if not isinstance(config, dict):
        return defaults

    douyin_cfg = config.get("douyin", {})
    if not isinstance(douyin_cfg, dict):
        return defaults

    merged = _merge_config(defaults, douyin_cfg)
    bootstrap_cfg = merged.get("bootstrap", {})
    ws_cfg = merged.get("ws", {})
    live_info_cfg = merged.get("live_info", {})
    extra_query_cfg = merged.get("extra_query", {})

    merged["enabled"] = bool(merged.get("enabled", defaults.get("enabled", False)))
    merged["live_id"] = str(merged.get("live_id", defaults.get("live_id", "")) or "")
    merged["cookie"] = str(merged.get("cookie", defaults.get("cookie", "")) or "")
    merged["signature"] = str(merged.get("signature", defaults.get("signature", "")) or "")
    merged["bootstrap"] = {
        "cursor": str(bootstrap_cfg.get("cursor", defaults["bootstrap"].get("cursor", "")) or ""),
        "internal_ext": str(bootstrap_cfg.get("internal_ext", defaults["bootstrap"].get("internal_ext", "")) or ""),
    }
    merged["ws"] = {
        "auto_reconnect": bool(ws_cfg.get("auto_reconnect", defaults["ws"].get("auto_reconnect", True))),
        "heartbeat_interval_seconds": float(ws_cfg.get("heartbeat_interval_seconds", defaults["ws"].get("heartbeat_interval_seconds", 5.0)) or 5.0),
        "reconnect_delay_seconds": float(ws_cfg.get("reconnect_delay_seconds", defaults["ws"].get("reconnect_delay_seconds", 2.0)) or 2.0),
    }
    merged["live_info"] = {
        "room_id": str(live_info_cfg.get("room_id", defaults["live_info"].get("room_id", "")) or ""),
        "user_id": str(live_info_cfg.get("user_id", defaults["live_info"].get("user_id", "")) or ""),
        "user_unique_id": str(live_info_cfg.get("user_unique_id", defaults["live_info"].get("user_unique_id", "")) or ""),
        "anchor_id": str(live_info_cfg.get("anchor_id", defaults["live_info"].get("anchor_id", "")) or ""),
        "sec_uid": str(live_info_cfg.get("sec_uid", defaults["live_info"].get("sec_uid", "")) or ""),
        "ttwid": str(live_info_cfg.get("ttwid", defaults["live_info"].get("ttwid", "")) or ""),
    }
    merged["extra_query"] = dict(extra_query_cfg) if isinstance(extra_query_cfg, dict) else {}
    return merged


def _get_platform_config_archive(config: dict[str, Any] | None) -> dict[str, int]:
    defaults = dict(DEFAULT_CONFIG.get("platform_config_archive", {}))
    if not isinstance(config, dict):
        return defaults
    raw = config.get("platform_config_archive", {})
    if not isinstance(raw, dict):
        raw = {}
    active_slot = max(1, min(MAX_QUEUE_ARCHIVE_SLOTS, _to_int(raw.get("active_slot", defaults.get("active_slot", 1)), defaults.get("active_slot", 1))))
    return {
        "slots": MAX_QUEUE_ARCHIVE_SLOTS,
        "active_slot": active_slot,
    }


def _normalize_runtime_platform_config(config: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_bilibili_config(config)
    normalized["platform"] = _normalize_platform_name(normalized.get("platform", DEFAULT_PLATFORM))
    normalized["douyin"] = _get_douyin_config(normalized)
    normalized["platform_config_archive"] = _get_platform_config_archive(normalized)
    return normalized


def _needs_bilibili_config_migration(raw_config: dict[str, Any] | None) -> bool:
    if not isinstance(raw_config, dict):
        return False
    legacy_api_cfg = raw_config.get("api")
    return isinstance(legacy_api_cfg, dict)


def _should_preserve_legacy_api_schema(raw_config: dict[str, Any] | None) -> bool:
    if not isinstance(raw_config, dict):
        return False
    legacy_api_cfg = raw_config.get("api")
    bilibili_cfg = raw_config.get("bilibili")
    return isinstance(legacy_api_cfg, dict) and not isinstance(bilibili_cfg, dict)


def _migrate_legacy_bilibili_config_if_needed(
    config: dict[str, Any],
    *,
    logger: logging.Logger | None = None,
) -> bool:
    raw_config = _read_raw_config()
    if not _needs_bilibili_config_migration(raw_config):
        return False
    try:
        save_config(config, preserve_legacy_api_schema=False)
    except OSError as exc:
        if logger is not None:
            logger.warning("Failed to migrate legacy api config to bilibili config: %s", exc)
        return False
    if logger is not None:
        logger.info("Migrated legacy api config to bilibili config")
    return True


_ARCHIVE_SEED_NAMES = (
    "小艾",
    "阿星",
    "北海",
    "夜雨",
    "清风",
    "团子",
    "阿九",
    "若白",
    "南栀",
    "流云",
    "初夏",
    "长安",
)

_ARCHIVE_SEED_TASKS = (
    "修城墙",
    "清理仓库",
    "搬运补给",
    "巡逻东门",
    "整理账本",
    "训练新兵",
    "制作药剂",
    "检查农田",
    "准备晚饭",
    "修理马车",
    "统计材料",
    "加固护栏",
)


def _build_seed_archive_entries(slot: int) -> list[dict[str, str]]:
    rng = random.SystemRandom()
    names = list(rng.sample(_ARCHIVE_SEED_NAMES, 3))
    tasks = list(rng.sample(_ARCHIVE_SEED_TASKS, 3))
    base_time = dt.datetime.now().replace(microsecond=0) - dt.timedelta(minutes=max(0, slot - 1) * 7)
    entries: list[dict[str, str]] = []
    for idx, (name, task) in enumerate(zip(names, tasks, strict=False)):
        timestamp = _format_archive_timestamp(base_time - dt.timedelta(minutes=idx))
        entries.append(build_queue_entry(name, task, timestamp))
    return entries


def platform_config_path(slot: int) -> Path:
    normalized = max(1, min(MAX_QUEUE_ARCHIVE_SLOTS, int(slot)))
    return PD_DIR / f"pingtai_config_{normalized}.yaml"


def _default_platform_slot_payload(seed: dict[str, Any] | None = None) -> dict[str, Any]:
    base = {
        "platform": DEFAULT_PLATFORM,
        "bilibili": copy.deepcopy(DEFAULT_CONFIG.get("bilibili", {})),
        "douyin": copy.deepcopy(DEFAULT_CONFIG.get("douyin", {})),
    }
    merged_seed = _merge_config(base, seed) if isinstance(seed, dict) else base
    normalized = _normalize_runtime_platform_config(merged_seed)
    return {
        "platform": normalized["platform"],
        "bilibili": dict(normalized["bilibili"]),
        "douyin": copy.deepcopy(normalized["douyin"]),
    }


def _render_platform_slot_yaml(payload: dict[str, Any]) -> str:
    normalized = _default_platform_slot_payload(payload)
    bilibili_cfg = normalized.get("bilibili", {})
    douyin_cfg = normalized.get("douyin", {})
    douyin_bootstrap_cfg = douyin_cfg.get("bootstrap", {}) if isinstance(douyin_cfg, dict) else {}
    douyin_ws_cfg = douyin_cfg.get("ws", {}) if isinstance(douyin_cfg, dict) else {}
    douyin_live_info_cfg = douyin_cfg.get("live_info", {}) if isinstance(douyin_cfg, dict) else {}
    douyin_extra_query_cfg = douyin_cfg.get("extra_query", {}) if isinstance(douyin_cfg, dict) else {}

    douyin_extra_query_lines: list[str] = []
    if isinstance(douyin_extra_query_cfg, dict):
        for key, value in douyin_extra_query_cfg.items():
            if not isinstance(key, str):
                continue
            if isinstance(value, bool):
                rendered = "true" if value else "false"
            elif isinstance(value, (int, float)):
                rendered = str(value)
            elif value is None:
                rendered = "null"
            else:
                rendered = _yaml_quote_string(value)
            douyin_extra_query_lines.append(f"    {key}: {rendered}")
    douyin_extra_query_block = "\n".join(douyin_extra_query_lines)

    return f"""platform: {normalized.get('platform', DEFAULT_PLATFORM)}
bilibili:
  roomid: {int(bilibili_cfg.get('roomid', 0))}
  uid: {int(bilibili_cfg.get('uid', 0))}
  cookie: {_yaml_quote_string(bilibili_cfg.get('cookie', ''))}

douyin:
  enabled: {'true' if bool(douyin_cfg.get('enabled', False)) else 'false'}
  live_id: {_yaml_quote_string(douyin_cfg.get('live_id', ''))}
  cookie: {_yaml_quote_string(douyin_cfg.get('cookie', ''))}
  signature: {_yaml_quote_string(douyin_cfg.get('signature', ''))}
  bootstrap:
    cursor: {_yaml_quote_string(douyin_bootstrap_cfg.get('cursor', ''))}
    internal_ext: {_yaml_quote_string(douyin_bootstrap_cfg.get('internal_ext', ''))}
  ws:
    auto_reconnect: {'true' if bool(douyin_ws_cfg.get('auto_reconnect', True)) else 'false'}
    heartbeat_interval_seconds: {float(douyin_ws_cfg.get('heartbeat_interval_seconds', 5.0) or 5.0)}
    reconnect_delay_seconds: {float(douyin_ws_cfg.get('reconnect_delay_seconds', 2.0) or 2.0)}
  live_info:
    room_id: {_yaml_quote_string(douyin_live_info_cfg.get('room_id', ''))}
    user_id: {_yaml_quote_string(douyin_live_info_cfg.get('user_id', ''))}
    user_unique_id: {_yaml_quote_string(douyin_live_info_cfg.get('user_unique_id', ''))}
    anchor_id: {_yaml_quote_string(douyin_live_info_cfg.get('anchor_id', ''))}
    sec_uid: {_yaml_quote_string(douyin_live_info_cfg.get('sec_uid', ''))}
    ttwid: {_yaml_quote_string(douyin_live_info_cfg.get('ttwid', ''))}
  extra_query:
{douyin_extra_query_block if douyin_extra_query_block else '    # optional websocket query overrides'}
"""


def ensure_platform_config_archives() -> None:
    PD_DIR.mkdir(parents=True, exist_ok=True)
    raw_config = _normalize_runtime_platform_config(_merge_config(DEFAULT_CONFIG, _read_raw_config()))
    seed_payload = _default_platform_slot_payload(raw_config)
    archive_paths = [platform_config_path(slot) for slot in range(1, MAX_QUEUE_ARCHIVE_SLOTS + 1)]
    has_any_archive = any(path.exists() for path in archive_paths)

    if not has_any_archive:
        for slot_path in archive_paths:
            slot_path.write_text(_render_platform_slot_yaml(seed_payload), encoding="utf-8")
        return

    for slot_path in archive_paths:
        try:
            if slot_path.exists() and slot_path.read_text(encoding="utf-8").strip():
                continue
        except OSError:
            pass
        slot_path.write_text(_render_platform_slot_yaml(seed_payload), encoding="utf-8")


def load_platform_config_slot(slot: int) -> dict[str, Any]:
    ensure_platform_config_archives()
    path = platform_config_path(slot)
    raw = load_simple_yaml(path)
    return _default_platform_slot_payload(raw)


def save_platform_config_slot(slot: int, data: dict[str, Any]) -> dict[str, Any]:
    normalized = _default_platform_slot_payload(data)
    PD_DIR.mkdir(parents=True, exist_ok=True)
    path = platform_config_path(slot)
    path.write_text(_render_platform_slot_yaml(normalized), encoding="utf-8")
    return normalized


def ensure_runtime_layout() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PD_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
    if not QUANXIAN_PATH.exists():
        _write_quanxian_file(_normalize_quanxian_config(DEFAULT_QUANXIAN))
    if not KAIGUAN_PATH.exists():
        _write_kaiguan_file(dict(DEFAULT_KAIGUAN))

    raw_config = _merge_config(DEFAULT_CONFIG, _read_raw_config())
    queue_archive_cfg = raw_config.get("queue_archive", {})
    active_slot = max(1, min(MAX_QUEUE_ARCHIVE_SLOTS, int(queue_archive_cfg.get("active_slot", 1) or 1)))
    if not QUEUE_STATE_PATH.exists():
        QUEUE_STATE_PATH.write_text(
            json.dumps({"next_slot": 1, "active_slot": active_slot}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    archive_paths = [PD_DIR / f"queue_archive_slot_{slot}.csv" for slot in range(1, MAX_QUEUE_ARCHIVE_SLOTS + 1)]
    has_any_archive = any(path.exists() for path in archive_paths)
    if not has_any_archive:
        for slot, slot_file in enumerate(archive_paths, start=1):
            write_queue_archive_entries(slot_file, _build_seed_archive_entries(slot))
    else:
        for slot_file in archive_paths:
            if not slot_file.exists():
                write_queue_archive_entries(slot_file, [])
            else:
                ensure_queue_archive_row_timestamps(slot_file)
    if not BLACKLIST_PATH.exists():
        write_blacklist_entries(BLACKLIST_PATH, blacklist_names_to_entries(load_quanxian().get("blacklist", [])))
    ensure_platform_config_archives()
    ensure_style_css_archives()


def load_config() -> dict[str, Any]:
    ensure_runtime_layout()
    raw_config = _read_raw_config()
    merged = _normalize_runtime_platform_config(_merge_config(DEFAULT_CONFIG, raw_config))
    platform_archive_cfg = merged.get("platform_config_archive", {})
    active_platform_slot = max(1, min(MAX_QUEUE_ARCHIVE_SLOTS, _to_int(platform_archive_cfg.get("active_slot", 1), 1)))
    slot_payload = load_platform_config_slot(active_platform_slot)
    merged["platform"] = slot_payload["platform"]
    merged["bilibili"] = dict(slot_payload["bilibili"])
    merged["douyin"] = copy.deepcopy(slot_payload["douyin"])
    merged["api"] = dict(slot_payload["bilibili"])
    merged["platform_config_archive"] = {"slots": MAX_QUEUE_ARCHIVE_SLOTS, "active_slot": active_platform_slot}
    merged["myjs"] = _normalize_myjs_config(merged.get("myjs", {}))
    return merged


def save_config(config: dict[str, Any], *, preserve_legacy_api_schema: bool | None = None) -> None:
    config = _normalize_runtime_platform_config(config)
    if preserve_legacy_api_schema is None:
        preserve_legacy_api_schema = _should_preserve_legacy_api_schema(_read_raw_config())
    server = config.get("server", {})
    platform_name = _normalize_platform_name(config.get("platform", DEFAULT_PLATFORM))
    bilibili_cfg = config.get("bilibili", {})
    douyin_cfg = config.get("douyin", {})
    qr_login = config.get("qr_login", {})
    callback_cfg = config.get("callback", {})
    myjs_cfg = config.get("myjs", {})
    ui_cfg = config.get("ui", {})
    overlay_cfg = ui_cfg.get("overlay_window", {}) if isinstance(ui_cfg, dict) else {}
    logging_cfg = config.get("logging", {})
    queue_archive = config.get("queue_archive", {})
    platform_archive = config.get("platform_config_archive", {})
    quanxian_cfg = config.get("quanxian", {})
    kaiguan_cfg = config.get("kaiguan", {})
    style_cfg = config.get("style", {})

    myjs_lines = []
    if isinstance(myjs_cfg, dict):
        for key, value in myjs_cfg.items():
            if not isinstance(key, str):
                continue
            if isinstance(value, list):
                if value:
                    myjs_lines.append(f"  {key}:")
                    for item in value:
                        item_text = str(item).strip()
                        if item_text:
                            myjs_lines.append(f"    - {_yaml_quote_string(item_text)}")
                else:
                    myjs_lines.append(f"  {key}: []")
            elif isinstance(value, bool):
                myjs_lines.append(f"  {key}: {'true' if value else 'false'}")
            elif isinstance(value, (int, float)):
                myjs_lines.append(f"  {key}: {value}")
            elif value is None:
                myjs_lines.append(f"  {key}: null")
            else:
                myjs_lines.append(f"  {key}: {_yaml_quote_string(value)}")
    myjs_block = "\n".join(myjs_lines) if myjs_lines else "  # 可在此覆盖前端 myjs.js 配置"

    quanxian_lines = []
    if isinstance(quanxian_cfg, dict):
        for key, default_values in DEFAULT_QUANXIAN.items():
            raw_values = quanxian_cfg.get(key, default_values)
            values = [str(item).strip() for item in raw_values if str(item).strip()] if isinstance(raw_values, list) else list(default_values)
            if values:
                quanxian_lines.append(f"  {key}:")
                for item in values:
                    quanxian_lines.append(f"    - {_yaml_quote_string(item)}")
            else:
                quanxian_lines.append(f"  {key}: []")
    quanxian_block = "\n".join(quanxian_lines) if quanxian_lines else "  # 权限配置"

    kaiguan_lines = []
    if isinstance(kaiguan_cfg, dict):
        for key, default in DEFAULT_KAIGUAN.items():
            value = bool(kaiguan_cfg.get(key, default))
            kaiguan_lines.append(f"  {key}: {'true' if value else 'false'}")
    kaiguan_block = "\n".join(kaiguan_lines) if kaiguan_lines else "  # 功能开关"

    style_lines = []
    if isinstance(style_cfg, dict):
        for key, default in DEFAULT_STYLE.items():
            value = style_cfg.get(key, default)
            if isinstance(default, int):
                style_lines.append(f"  {key}: {_to_int(value, int(default))}")
            else:
                style_lines.append(f"  {key}: {_yaml_quote_string(value)}")
    style_block = "\n".join(style_lines) if style_lines else "  # 样式配置"

    cookie_text = _yaml_quote_string(bilibili_cfg.get("cookie", ""))
    douyin_bootstrap_cfg = douyin_cfg.get("bootstrap", {}) if isinstance(douyin_cfg, dict) else {}
    douyin_ws_cfg = douyin_cfg.get("ws", {}) if isinstance(douyin_cfg, dict) else {}
    douyin_live_info_cfg = douyin_cfg.get("live_info", {}) if isinstance(douyin_cfg, dict) else {}
    douyin_extra_query_cfg = douyin_cfg.get("extra_query", {}) if isinstance(douyin_cfg, dict) else {}
    douyin_extra_query_lines: list[str] = []
    if isinstance(douyin_extra_query_cfg, dict):
        for key, value in douyin_extra_query_cfg.items():
            if not isinstance(key, str):
                continue
            if isinstance(value, bool):
                rendered = "true" if value else "false"
            elif isinstance(value, (int, float)):
                rendered = str(value)
            elif value is None:
                rendered = "null"
            else:
                rendered = _yaml_quote_string(value)
            douyin_extra_query_lines.append(f"    {key}: {rendered}")
    douyin_extra_query_block = "\n".join(douyin_extra_query_lines)
    douyin_live_id = _yaml_quote_string(douyin_cfg.get("live_id", "") if isinstance(douyin_cfg, dict) else "")
    douyin_cookie = _yaml_quote_string(douyin_cfg.get("cookie", "") if isinstance(douyin_cfg, dict) else "")
    douyin_signature = _yaml_quote_string(douyin_cfg.get("signature", "") if isinstance(douyin_cfg, dict) else "")
    douyin_cursor = _yaml_quote_string(douyin_bootstrap_cfg.get("cursor", "") if isinstance(douyin_bootstrap_cfg, dict) else "")
    douyin_internal_ext = _yaml_quote_string(douyin_bootstrap_cfg.get("internal_ext", "") if isinstance(douyin_bootstrap_cfg, dict) else "")
    douyin_room_id = _yaml_quote_string(douyin_live_info_cfg.get("room_id", "") if isinstance(douyin_live_info_cfg, dict) else "")
    douyin_user_id = _yaml_quote_string(douyin_live_info_cfg.get("user_id", "") if isinstance(douyin_live_info_cfg, dict) else "")
    douyin_user_unique_id = _yaml_quote_string(douyin_live_info_cfg.get("user_unique_id", "") if isinstance(douyin_live_info_cfg, dict) else "")
    douyin_anchor_id = _yaml_quote_string(douyin_live_info_cfg.get("anchor_id", "") if isinstance(douyin_live_info_cfg, dict) else "")
    douyin_sec_uid = _yaml_quote_string(douyin_live_info_cfg.get("sec_uid", "") if isinstance(douyin_live_info_cfg, dict) else "")
    douyin_ttwid = _yaml_quote_string(douyin_live_info_cfg.get("ttwid", "") if isinstance(douyin_live_info_cfg, dict) else "")
    qr_last_success_at = _yaml_quote_string(qr_login.get("last_success_at", ""))
    qr_qrcode_key = _yaml_quote_string(qr_login.get("qrcode_key", ""))
    qr_message = _yaml_quote_string(qr_login.get("message", ""))
    qr_cookie = _yaml_quote_string(qr_login.get("cookie", ""))
    callback_url = _yaml_quote_string(callback_cfg.get("url", ""))
    callback_auth_token = _yaml_quote_string(callback_cfg.get("auth_token", ""))
    overlay_width = max(320, _to_int(overlay_cfg.get("width", 860), 860))
    overlay_height = max(180, _to_int(overlay_cfg.get("height", 420), 420))
    overlay_scale = max(40, min(250, _to_int(overlay_cfg.get("scale", 100), 100)))

    bilibili_section_name = "api" if preserve_legacy_api_schema else "bilibili"

    content = f"""# Danmuji 全局配置
server:
  host: {server.get('host', DEFAULT_HOST)}
  port: {int(server.get('port', DEFAULT_PORT))}

platform: {platform_name}

{bilibili_section_name}:
  roomid: {int(bilibili_cfg.get('roomid', 0))}
  uid: {int(bilibili_cfg.get('uid', 0))}
  cookie: {cookie_text}

douyin:
  enabled: {'true' if bool(douyin_cfg.get('enabled', False)) else 'false'}
  live_id: {douyin_live_id}
  cookie: {douyin_cookie}
  signature: {douyin_signature}
  bootstrap:
    cursor: {douyin_cursor}
    internal_ext: {douyin_internal_ext}
  ws:
    auto_reconnect: {'true' if bool(douyin_ws_cfg.get('auto_reconnect', True)) else 'false'}
    heartbeat_interval_seconds: {float(douyin_ws_cfg.get('heartbeat_interval_seconds', 5.0) or 5.0)}
    reconnect_delay_seconds: {float(douyin_ws_cfg.get('reconnect_delay_seconds', 2.0) or 2.0)}
  live_info:
    room_id: {douyin_room_id}
    user_id: {douyin_user_id}
    user_unique_id: {douyin_user_unique_id}
    anchor_id: {douyin_anchor_id}
    sec_uid: {douyin_sec_uid}
    ttwid: {douyin_ttwid}
  extra_query:
{douyin_extra_query_block if douyin_extra_query_block else '    # optional websocket query overrides'}

qr_login:
  # 最近一次扫码成功信息（由 /api/bili/qr/poll 自动写入）
  last_success_at: {qr_last_success_at}
  qrcode_key: {qr_qrcode_key}
  poll_code: {int(qr_login.get('poll_code', -1))}
  message: {qr_message}
  cookie: {qr_cookie}

# 前端 myjs.js 可覆盖配置（如需扩展可继续加键值）
myjs:
{myjs_block}

ui:
  # 页面启动提示层展示时长（秒）
  startup_splash_seconds: {max(0, int(ui_cfg.get('startup_splash_seconds', 5)))}
  # GUI 启动时是否自动拉起后端
  auto_start_backend: {'true' if bool(ui_cfg.get('auto_start_backend', False)) else 'false'}
  # GUI 当前语言
  language: {_yaml_quote_string(ui_cfg.get('language', '中文'))}
  overlay_window:
    width: {overlay_width}
    height: {overlay_height}
    scale: {overlay_scale}

logging:
  # 支持 DEBUG / INFO / WARNING / ERROR / CRITICAL
  level: {str(logging_cfg.get('level', 'INFO')).upper()}
  # 每次启动默认清理多少天前日志
  retention_days: {int(logging_cfg.get('retention_days', 7))}

queue_archive:
  enabled: {'true' if bool(queue_archive.get('enabled', True)) else 'false'}
  # 最大存档位数（固定为 {MAX_QUEUE_ARCHIVE_SLOTS}，勿修改）
  slots: {MAX_QUEUE_ARCHIVE_SLOTS}
  # 当前活动存档槽（1~{MAX_QUEUE_ARCHIVE_SLOTS}，由 GUI 写入）
  active_slot: {min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(queue_archive.get('active_slot', 1))))}

platform_config_archive:
  slots: {MAX_QUEUE_ARCHIVE_SLOTS}
  active_slot: {min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(platform_archive.get('active_slot', 1))))}

callback:
  enabled: {'true' if bool(callback_cfg.get('enabled', False)) else 'false'}
  url: {callback_url}
  auth_token: {callback_auth_token}
  timeout_seconds: {max(1, int(callback_cfg.get('timeout_seconds', 5)))}

quanxian:
{quanxian_block}

kaiguan:
{kaiguan_block}

style:
{style_block}
"""
    CONFIG_PATH.write_text(content, encoding="utf-8")


def _cleanup_old_logs(retention_days: int) -> None:
    if retention_days <= 0:
        return
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=retention_days)
    for log_file in LOG_DIR.glob("*.log"):
        modified = dt.datetime.fromtimestamp(log_file.stat().st_mtime, dt.timezone.utc)
        if modified < cutoff:
            log_file.unlink(missing_ok=True)


def setup_logging(config: dict[str, Any]) -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    retention_days = int(config.get("logging", {}).get("retention_days", 7))
    _cleanup_old_logs(retention_days)

    level_name = str(config.get("logging", {}).get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    log_path = LOG_DIR / f"backend_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    stream_handler = logging.StreamHandler()
    stream_handler.stream = open(stream_handler.stream.fileno(), mode="w", encoding="utf-8", buffering=1, closefd=False)  # type: ignore[assignment]
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            stream_handler,
        ],
    )
    logger = logging.getLogger("danmuji.backend")
    logger.info("日志已初始化，文件：%s", log_path)
    logger.info("日志自动清理：保留最近 %s 天", retention_days)
    return logger


class QueueArchiveManager:
    def __init__(self, slots: int = 3, enabled: bool = True) -> None:
        self.slots = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(slots)))
        self.enabled = enabled
        PD_DIR.mkdir(parents=True, exist_ok=True)

    def _read_state(self) -> dict[str, int]:
        if not QUEUE_STATE_PATH.exists():
            return {"next_slot": 1}
        try:
            return json.loads(QUEUE_STATE_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"next_slot": 1}

    def _write_state(self, state: dict[str, int]) -> None:
        QUEUE_STATE_PATH.write_text(
            json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def _slot_file(self, slot: int) -> Path:
        return PD_DIR / f"queue_archive_slot_{slot}.csv"

    def _empty_snapshot(self) -> dict[str, Any]:
        return {
            "slot": 0,
            "path": "",
            "timestamp": "",
            "last_operation_at": "",
            "actor": "",
            "message": "",
            "queue": [],
            "entries": [],
        }

    def get_active_slot(self) -> int:
        state = self._read_state()
        slot = int(state.get("active_slot", 1))
        return max(1, min(self.slots, slot))

    def set_active_slot(self, slot: int) -> None:
        state = self._read_state()
        state["active_slot"] = max(1, min(self.slots, slot))
        self._write_state(state)

    def write_snapshot(self, actor: str, message: str, queue_items: list[Any]) -> Path | None:
        if not self.enabled:
            return None

        slot = self.get_active_slot()
        out = self._slot_file(slot)
        default_timestamp = _format_archive_timestamp()
        entries = [
            build_queue_entry(
                entry.get("id", ""),
                entry.get("content", ""),
                entry.get("last_operation_at", "") or default_timestamp,
            )
            for entry in queue_items_to_entries(queue_items)
        ]
        write_queue_archive_entries(
            out,
            entries,
            meta={
                "actor": actor,
                "message": message,
            },
        )
        return out

    def write_blank_snapshot(self, actor: str, message: str) -> Path | None:
        if not self.enabled:
            return None
        slot = self.get_active_slot()
        out = self._slot_file(slot)
        write_queue_archive_entries(
            out,
            [],
            meta={
                "actor": actor,
                "message": message,
            },
        )
        return out

    def _read_snapshot(self, slot: int) -> dict[str, Any] | None:
        path = self._slot_file(slot)
        if not path.exists():
            return None

        snapshot: dict[str, Any] = {
            "slot": slot,
            "path": str(path),
            "timestamp": "",
            "last_operation_at": "",
            "actor": "",
            "message": "",
            "queue": [],
            "entries": [],
        }

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                rows = list(csv.reader(f))
        except OSError:
            return None

        meta, entries = parse_queue_archive_rows(rows)
        snapshot_last_operation_at = latest_queue_entry_timestamp(entries)
        snapshot["timestamp"] = meta.get("timestamp", "") or snapshot_last_operation_at
        snapshot["actor"] = meta.get("actor", "")
        snapshot["message"] = meta.get("message", "")
        snapshot["entries"] = entries
        snapshot["queue"] = queue_entries_to_items(entries)

        modified = dt.datetime.fromtimestamp(path.stat().st_mtime)
        if not snapshot["timestamp"]:
            snapshot["timestamp"] = _format_archive_timestamp(modified)
        snapshot["last_operation_at"] = snapshot_last_operation_at or _format_archive_timestamp(modified)
        try:
            sort_key = dt.datetime.fromisoformat(str(snapshot["last_operation_at"]))
        except ValueError:
            sort_key = modified
        snapshot["_sort_key"] = sort_key.isoformat()
        return snapshot

    def read_snapshot_by_slot(self, slot: int) -> dict[str, Any] | None:
        return self._read_snapshot(slot)

    def read_latest_snapshot(self) -> dict[str, Any]:
        if not self.enabled:
            return self._empty_snapshot()
        snapshots = [
            snapshot
            for slot in range(1, self.slots + 1)
            for snapshot in [self._read_snapshot(slot)]
            if snapshot is not None
        ]
        if not snapshots:
            return self._empty_snapshot()
        snapshots.sort(key=lambda item: str(item.get("_sort_key", "")), reverse=True)
        latest = dict(snapshots[0])
        latest.pop("_sort_key", None)
        return latest


class QueueManager:
    """Thread-safe server-side queue manager.

    Receives parsed danmu events and processes queue commands,
    then broadcasts ``QUEUE_UPDATE`` to all WebSocket clients.
    """

    def __init__(
        self,
        ws_hub: "WebSocketHub",
        queue_archive: "QueueArchiveManager",
        logger: logging.Logger,
        runtime_reload_callback: Callable[[], None] | None = None,
    ) -> None:
        self._ws_hub = ws_hub
        self._queue_archive = queue_archive
        self._logger = logger
        self._runtime_reload_callback = runtime_reload_callback
        self._lock = threading.Lock()
        self._persons: list[str] = []
        self._entry_timestamps: list[str] = []
        self._admins: list[str] = []
        self._blacklist: list[str] = []
        self._jianzhang: list[str] = []
        self._anchor_uid: int = 0
        self._max_length: int = 100
        self._fangguan_can_doing: bool = False
        self._jianzhangchadui: bool = False
        self._all_disabled: bool = False
        self._super_admins: list[str] = []
        self._kaiguan: dict[str, bool] = dict(DEFAULT_KAIGUAN)

    def load_config(self, myjs_cfg: dict[str, Any], anchor_uid: int = 0) -> None:
        with self._lock:
            if isinstance(myjs_cfg.get("admins"), list):
                self._admins = [str(x) for x in myjs_cfg["admins"] if x]
            if isinstance(myjs_cfg.get("ban_admins"), list):
                self._blacklist = [str(x) for x in myjs_cfg["ban_admins"] if x]
            if isinstance(myjs_cfg.get("jianzhang"), list):
                self._jianzhang = [str(x) for x in myjs_cfg["jianzhang"] if x]
            if myjs_cfg.get("paidui_list_length_max") is not None:
                self._max_length = max(1, _to_int(myjs_cfg["paidui_list_length_max"], 100))
            if isinstance(myjs_cfg.get("all_suoyourenbukepaidui"), bool):
                self._all_disabled = myjs_cfg["all_suoyourenbukepaidui"]
            if isinstance(myjs_cfg.get("fangguan_can_doing"), bool):
                self._fangguan_can_doing = myjs_cfg["fangguan_can_doing"]
            if isinstance(myjs_cfg.get("jianzhangchadui"), bool):
                self._jianzhangchadui = myjs_cfg["jianzhangchadui"]
            if anchor_uid > 0:
                self._anchor_uid = anchor_uid

    def load_quanxian(self, quanxian: dict[str, Any]) -> None:
        with self._lock:
            normalized = _normalize_quanxian_config(quanxian)
            self._super_admins = list(normalized.get("super_admin", []))
            self._admins = list(normalized.get("admin", []))
            self._jianzhang = list(normalized.get("jianzhang", []))
            self._blacklist = list(normalized.get("blacklist", []))

    def load_kaiguan(self, kaiguan: dict[str, bool]) -> None:
        with self._lock:
            self._kaiguan = {**DEFAULT_KAIGUAN, **{k: v for k, v in kaiguan.items() if isinstance(v, bool)}}
            self._jianzhangchadui = self._kaiguan.get("jianzhang_chadui", False)
            self._fangguan_can_doing = self._kaiguan.get("fangguan_op", False)
            self._all_disabled = not self._kaiguan.get("paidui", True)

    def _reload_runtime_config(self) -> None:
        if not self._runtime_reload_callback:
            return
        try:
            self._runtime_reload_callback()
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("刷新运行时配置失败: %s", exc)

    def _persist_myjs_state_unlocked(self) -> None:
        current = load_config()
        myjs_cfg = _normalize_myjs_config(current.get("myjs", {}))
        myjs_cfg["paidui_list_length_max"] = self._max_length
        myjs_cfg["all_suoyourenbukepaidui"] = self._all_disabled
        myjs_cfg["fangguan_can_doing"] = self._fangguan_can_doing
        myjs_cfg["jianzhangchadui"] = self._jianzhangchadui
        myjs_cfg["admins"] = list(self._admins)
        myjs_cfg["ban_admins"] = list(self._blacklist)
        myjs_cfg["jianzhang"] = list(self._jianzhang)
        current["myjs"] = _normalize_myjs_config(myjs_cfg)
        save_config(current)
        self._reload_runtime_config()

    def _persist_quanxian_state_unlocked(self) -> None:
        current_quanxian = load_quanxian()
        payload = {
            "super_admin": list(self._super_admins),
            "admin": list(self._admins),
            "jianzhang": list(self._jianzhang),
            "member": list(current_quanxian.get("member", [])),
            "blacklist": list(self._blacklist),
        }
        save_quanxian(payload)
        persisted = load_quanxian()
        self._super_admins = list(persisted.get("super_admin", []))
        self._admins = list(persisted.get("admin", []))
        self._jianzhang = list(persisted.get("jianzhang", []))
        self._blacklist = list(persisted.get("blacklist", []))
        self._reload_runtime_config()

    def _persist_kaiguan_state_unlocked(self) -> None:
        payload = dict(self._kaiguan)
        payload["paidui"] = not self._all_disabled
        payload["jianzhang_chadui"] = self._jianzhangchadui
        payload["fangguan_op"] = self._fangguan_can_doing
        save_kaiguan(payload)
        self._kaiguan = load_kaiguan()
        self._jianzhangchadui = self._kaiguan.get("jianzhang_chadui", False)
        self._fangguan_can_doing = self._kaiguan.get("fangguan_op", False)
        self._all_disabled = not self._kaiguan.get("paidui", True)
        self._reload_runtime_config()

    def restore_from_archive(self) -> None:
        slot = self._queue_archive.get_active_slot()
        snapshot = self._queue_archive.read_snapshot_by_slot(slot)
        if snapshot is None:
            return
        with self._lock:
            self._set_queue_from_entries_unlocked(snapshot.get("entries", []))
        self._logger.info(
            "[队列] 已从存档恢复 %s 人（槽位 %s，存档时间：%s）",
            len(self._persons), snapshot.get("slot", "?"), snapshot.get("timestamp", "?"),
        )

    @staticmethod
    def _strip_html(text: str) -> str:
        cleaned = re.sub(r"<[^>]*>", "", str(text))
        cleaned = re.sub(r"⏳待确认|等待确认", "", cleaned)
        return cleaned.strip()

    def _now_queue_timestamp(self) -> str:
        return _format_archive_timestamp()

    def _sync_entry_timestamps_unlocked(self) -> None:
        missing = len(self._persons) - len(self._entry_timestamps)
        if missing > 0:
            self._entry_timestamps.extend([""] * missing)
        elif missing < 0:
            del self._entry_timestamps[len(self._persons):]

    def _set_queue_from_entries_unlocked(self, entries: list[dict[str, Any]]) -> None:
        persons: list[str] = []
        timestamps: list[str] = []
        for entry in entries:
            normalized = queue_item_to_entry(entry)
            item = self._strip_html(
                queue_parts_to_item(
                    normalized.get("id", ""),
                    normalized.get("content", ""),
                )
            )
            if not item:
                continue
            persons.append(item)
            timestamps.append(str(normalized.get("last_operation_at", "") or "").strip())
        self._persons = persons
        self._entry_timestamps = timestamps
        self._sync_entry_timestamps_unlocked()

    def _get_queue_entries_unlocked(self) -> list[dict[str, str]]:
        self._sync_entry_timestamps_unlocked()
        entries: list[dict[str, str]] = []
        for idx, item in enumerate(self._persons):
            item_text = str(item or "").strip()
            if not item_text:
                continue
            entries.append(queue_item_to_entry(item_text, self._entry_timestamps[idx]))
        return entries

    def _append_queue_item_unlocked(self, item: Any, last_operation_at: Any | None = None) -> bool:
        item_text = self._strip_html(item)
        if not item_text:
            return False
        timestamp = str(last_operation_at or "").strip() or self._now_queue_timestamp()
        self._persons.append(item_text)
        self._entry_timestamps.append(_format_archive_timestamp(timestamp))
        return True

    def _insert_queue_item_unlocked(self, pos: int, item: Any, last_operation_at: Any | None = None) -> bool:
        item_text = self._strip_html(item)
        if not item_text:
            return False
        self._sync_entry_timestamps_unlocked()
        insert_pos = max(0, min(pos, len(self._persons)))
        timestamp = str(last_operation_at or "").strip() or self._now_queue_timestamp()
        self._persons.insert(insert_pos, item_text)
        self._entry_timestamps.insert(insert_pos, _format_archive_timestamp(timestamp))
        return True

    def _replace_queue_item_unlocked(self, index: int, item: Any) -> bool:
        if not (0 <= index < len(self._persons)):
            return False
        item_text = self._strip_html(item)
        if not item_text:
            return False
        self._persons[index] = item_text
        self._sync_entry_timestamps_unlocked()
        self._entry_timestamps[index] = self._now_queue_timestamp()
        return True

    def _remove_queue_item_unlocked(self, index: int) -> bool:
        if not (0 <= index < len(self._persons)):
            return False
        self._sync_entry_timestamps_unlocked()
        self._persons.pop(index)
        if 0 <= index < len(self._entry_timestamps):
            self._entry_timestamps.pop(index)
        else:
            self._sync_entry_timestamps_unlocked()
        return True

    def _touch_queue_items_unlocked(self, *indices: int) -> None:
        self._sync_entry_timestamps_unlocked()
        timestamp = self._now_queue_timestamp()
        for index in dict.fromkeys(indices):
            if 0 <= index < len(self._entry_timestamps):
                self._entry_timestamps[index] = timestamp

    def get_queue(self) -> list[str]:
        with self._lock:
            return list(self._persons)

    def get_queue_entries(self) -> list[dict[str, str]]:
        with self._lock:
            return self._get_queue_entries_unlocked()

    def get_blacklist(self) -> list[str]:
        with self._lock:
            return list(self._blacklist)

    def get_blacklist_entries(self) -> list[dict[str, str]]:
        return blacklist_names_to_entries(self.get_blacklist())

    def add_blacklist_item(self, name: str) -> list[dict[str, str]]:
        target = str(name or "").strip()
        if not target:
            return self.get_blacklist_entries()
        with self._lock:
            if target not in self._blacklist:
                self._blacklist.append(target)
            self._super_admins = [item for item in self._super_admins if item != target]
            self._admins = [item for item in self._admins if item != target]
            self._jianzhang = [item for item in self._jianzhang if item != target]
            self._persist_quanxian_state_unlocked()
        self._logger.info("[黑名单] 已新增：%s", target)
        return self.get_blacklist_entries()

    def delete_blacklist_item(self, index: int) -> list[dict[str, str]]:
        removed = ""
        with self._lock:
            if 1 <= index <= len(self._blacklist):
                removed = self._blacklist.pop(index - 1)
                self._persist_quanxian_state_unlocked()
        if removed:
            self._logger.info("[黑名单] 已删除：%s", removed)
        return self.get_blacklist_entries()

    def clear_blacklist(self) -> list[dict[str, str]]:
        with self._lock:
            if not self._blacklist:
                return []
            previous = list(self._blacklist)
            self._blacklist.clear()
            self._persist_quanxian_state_unlocked()
        self._logger.info("[黑名单] 已清空，共移除 %s 人: %s", len(previous), json.dumps(previous, ensure_ascii=False))
        return []

    def send_current_to(self, conn: socket.socket) -> None:
        try:
            entries = self.get_queue_entries()
            _ws_send_text(
                conn,
                json.dumps(
                    {
                        "type": "QUEUE_UPDATE",
                        "queue": queue_entries_to_items(entries),
                        "entries": entries,
                    },
                    ensure_ascii=False,
                ),
            )
        except OSError:
            pass

    def delete_item(self, index: int) -> list[str]:
        """删除第 index 项（1-based），越界静默忽略。"""
        with self._lock:
            if 1 <= index <= len(self._persons):
                self._remove_queue_item_unlocked(index - 1)
        self._broadcast_and_archive("gui", f"delete_{index}")
        return self.get_queue()

    def move_item(self, index: int, direction: str) -> list[str]:
        """上移或下移第 index 项（1-based）。direction: 'up' | 'down'。"""
        with self._lock:
            n = len(self._persons)
            if direction == "up" and 2 <= index <= n:
                self._persons[index - 2], self._persons[index - 1] = (
                    self._persons[index - 1],
                    self._persons[index - 2],
                )
                self._sync_entry_timestamps_unlocked()
                self._entry_timestamps[index - 2], self._entry_timestamps[index - 1] = (
                    self._entry_timestamps[index - 1],
                    self._entry_timestamps[index - 2],
                )
                self._touch_queue_items_unlocked(index - 2, index - 1)
            elif direction == "down" and 1 <= index <= n - 1:
                self._persons[index - 1], self._persons[index] = (
                    self._persons[index],
                    self._persons[index - 1],
                )
                self._sync_entry_timestamps_unlocked()
                self._entry_timestamps[index - 1], self._entry_timestamps[index] = (
                    self._entry_timestamps[index],
                    self._entry_timestamps[index - 1],
                )
                self._touch_queue_items_unlocked(index - 1, index)
        self._broadcast_and_archive("gui", f"move_{direction}_{index}")
        return self.get_queue()

    def insert_item(self, after_index: int, entry: str) -> list[str]:
        """在 after_index 之后插入 entry（after_index=0 插到最前面）。"""
        entry = entry.strip()
        if not entry:
            return self.get_queue()
        with self._lock:
            pos = max(0, min(after_index, len(self._persons)))
            self._insert_queue_item_unlocked(pos, entry)
        self._broadcast_and_archive("gui", f"insert_{after_index}")
        return self.get_queue()

    def update_item_content(self, index: int, content: str) -> list[str]:
        """按序号修改排队内容，保留原条目的 id。"""
        with self._lock:
            if 1 <= index <= len(self._persons):
                item_id, _old_content = queue_item_to_parts(self._persons[index - 1])
                self._replace_queue_item_unlocked(index - 1, queue_parts_to_item(item_id, content))
        self._broadcast_and_archive("gui", f"update_{index}")
        return self.get_queue()

    def clear_queue(self) -> list[str]:
        """清空队列。"""
        with self._lock:
            previous_queue = list(self._persons)
            self._persons.clear()
            self._entry_timestamps.clear()

        if previous_queue:
            self._logger.info(
                "[队列] 一键清空前原始队列（%s 人）: %s",
                len(previous_queue),
                json.dumps(previous_queue, ensure_ascii=False),
            )
        else:
            self._logger.info("[队列] 一键清空时原始队列为空")

        self._ws_hub.broadcast_json(None, {"type": "QUEUE_UPDATE", "queue": [], "entries": []})
        self._queue_archive.write_blank_snapshot("gui", "clear")
        self._logger.info("[队列] 当前槽位 %s 已恢复为空白存档", self._queue_archive.get_active_slot())
        return []

    def switch_to_slot(self, slot: int) -> list[str]:
        """Load queue from a specific archive slot, update in-memory queue, and broadcast."""
        old_slot = self._queue_archive.get_active_slot()
        reconcile_live_css_with_archive(old_slot)
        self._queue_archive.set_active_slot(slot)
        apply_css_archive_to_live(slot, force=True)
        snapshot = self._queue_archive.read_snapshot_by_slot(slot)
        if snapshot is None:
            self._logger.info("[队列] 切换到槽位 %s：槽位文件不存在，保持当前队列不变", slot)
            return self.get_queue()
        with self._lock:
            self._set_queue_from_entries_unlocked(snapshot.get("entries", []))
            items = list(self._persons)
        self._logger.info(
            "[队列] 已切换到槽位 %s，加载 %s 人（存档时间：%s）",
            slot, len(items), snapshot.get("timestamp", "?"),
        )
        self._ws_hub.broadcast_json(
            None,
            {"type": "STYLE_UPDATE", "slot": slot, "css": LIVE_STYLE_CSS_PATH.name, "reason": "switch_slot"},
        )
        self._broadcast_and_archive("system", f"switch_slot_{slot}")
        return list(items)

    def _has_super_admin(self, uname: str, is_anchor: bool) -> bool:
        return is_anchor or uname in self._super_admins

    def _has_op_permission(self, uname: str, is_anchor: bool, is_admin: bool) -> bool:
        if self._has_super_admin(uname, is_anchor):
            return True
        return (uname in self._admins) or (is_admin and self._fangguan_can_doing)

    def _find_index(self, uname: str) -> int:
        for i, item in enumerate(self._persons):
            if uname in item:
                return i
        return -1

    def _broadcast_and_archive(self, actor: str, msg: str) -> None:
        queue_entries = self.get_queue_entries()
        queue_snapshot = queue_entries_to_items(queue_entries)
        self._ws_hub.broadcast_json(
            None,
            {"type": "QUEUE_UPDATE", "queue": queue_snapshot, "entries": queue_entries},
        )
        self._queue_archive.write_snapshot(actor, msg, queue_entries)

    def process_danmu_json(self, payload: dict[str, Any]) -> None:
        cmd = str(payload.get("cmd", "") or "").strip()
        if not cmd.startswith("DANMU_MSG"):
            return

        info = payload.get("info", [])
        if not isinstance(info, list) or len(info) < 3:
            return

        msg = str(info[1]) if len(info) > 1 else ""
        user_info = info[2] if len(info) > 2 and isinstance(info[2], list) else []
        uid = _to_int(user_info[0]) if len(user_info) > 0 else 0
        uname = str(user_info[1]) if len(user_info) > 1 else ""
        is_admin_flag = _to_int(user_info[2]) == 1 if len(user_info) > 2 else False

        with self._lock:
            anchor_uid = self._anchor_uid
            is_blacklisted = uname in self._blacklist
        is_anchor = uid > 0 and anchor_uid > 0 and uid == anchor_uid

        medal_info = info[3] if len(info) > 3 else None
        guard_level = 0
        if isinstance(medal_info, list):
            for idx in (10, 11, 12):
                if idx < len(medal_info):
                    gl = _to_int(medal_info[idx])
                    if 1 <= gl <= 3:
                        guard_level = gl
                        break
        is_guard = guard_level > 0

        if not uname or not msg:
            return

        # 所有弹幕打印到 INFO
        perm = "黑名单" if is_blacklisted else ("主播" if is_anchor else ("super_admin" if uname in self._super_admins else ("管理员" if uname in self._admins or (is_admin_flag and self._fangguan_can_doing) else ("舰长" if is_guard else "普通用户"))))
        self._logger.info("[弹幕] %s(%s): %s", uname, perm, msg)

        modified, note = self._process(uname, msg, is_anchor, is_admin_flag, is_guard, guard_level)
        if modified:
            self._broadcast_and_archive(uname, msg)
            self._logger.info(
                "[触发指令] uname=%s 权限=%s msg=%r → 队列变更，当前 %s 人",
                uname, perm, msg, len(self._persons),
            )
        elif note:
            self._logger.info("[提示] %s(%s): %s", uname, perm, note)

    # 判断弹幕是否属于"排队类指令"（不含管理员专属指令）
    _JOIN_CMD_PATTERNS = (
        "排队", "官服排", "排官服", "官服排队", "排队官服",
        "B服排", "b服排", "排b服", "排B服", "B服排队", "排队B服", "b服排队", "排队b服",
        "超级排", "超级排队", "小米排", "排小米", "排米服", "插队",
    )

    def _is_join_cmd(self, msg: str) -> bool:
        """判断消息是否看起来像排队入队指令（用于已在队列时提示）。"""
        if msg in self._JOIN_CMD_PATTERNS:
            return True
        for prefix in ("排队 ", "官服排队 ", "官服排 ", "B服排 ", "b服排 ", "超级排队 ", "超级排 ", "米服排 "):
            if msg.startswith(prefix):
                return True
        return False

    def _is_non_admin_command(self, msg: str) -> bool:
        if self._is_join_cmd(msg):
            return True
        if msg in ("取消排队", "排队取消", "我确认我取消排队", "替换", "修改", "内容洗白"):
            return True
        return msg.startswith(("替换 ", "修改 "))

    def _is_admin_command(self, msg: str) -> bool:
        if any(msg.startswith(p) for p in ("del", "删除", "完成", "add ", "新增 ", "添加 ", "无影插 ", "插队 ")):
            return True
        if msg in (
            "暂停排队功能",
            "关闭自助排队",
            "恢复排队功能",
            "恢复自助排队",
            "开启舰长插队",
            "关闭舰长插队",
            "允许房管成为插件管理员",
            "停止房管成为插件管理员",
        ):
            return True
        if any(kw in msg for kw in ("设置排队人数", "设置排队上限")):
            return True
        if msg.startswith(("拉黑 ", "取消拉黑 ", "添加管理员 ", "取消管理员 ")):
            return True
        return False

    def _is_command_like(self, msg: str) -> bool:
        return self._is_non_admin_command(msg) or self._is_admin_command(msg)

    def _process(
        self,
        uname: str,
        msg: str,
        is_anchor: bool,
        is_admin: bool,
        is_guard: bool,
        guard_level: int,
    ) -> tuple[bool, str | None]:
        with self._lock:
            has_op = self._has_op_permission(uname, is_anchor, is_admin)
            kg = self._kaiguan

            if uname in self._blacklist:
                if self._is_command_like(msg):
                    self._logger.warning("[黑名单拦截] uname=%s msg=%r", uname, msg)
                    return False, "黑名单用户指令已拦截"
                return False, None

            if (self._all_disabled or not kg.get("paidui", True)) and not has_op:
                if self._is_non_admin_command(msg):
                    return False, "排队功能已暂停，权限不足"
                return False, None

            if is_guard and guard_level > 0 and uname not in self._jianzhang:
                self._jianzhang.append(uname)

            index = self._find_index(uname)
            is_jianzhang = uname in self._jianzhang
            modified = False

            # --- Join commands (not yet in queue) ---
            if index < 0:
                can_join = len(self._persons) < self._max_length or has_op
                new_item: str | None = None

                join_master_enabled = kg.get("paidui", True)
                if msg == "排队" and join_master_enabled:
                    new_item = uname
                elif msg in ("官服排", "排官服", "官服排队", "排队官服") and join_master_enabled and kg.get("guanfu_paidui", True):
                    new_item = f"官|{uname}"
                elif msg in ("B服排", "b服排", "排b服", "排B服", "B服排队", "排队B服", "b服排队", "排队b服") and join_master_enabled and kg.get("bfu_paidui", True):
                    new_item = f"B|{uname}"
                elif msg in ("超级排", "超级排队") and join_master_enabled and kg.get("chaoji_paidui", True):
                    new_item = f"<{uname}>"
                elif msg in ("小米排", "排小米", "排米服") and join_master_enabled and kg.get("mifu_paidui", True):
                    new_item = f"米|{uname}"
                elif msg.startswith("排队 ") and join_master_enabled:
                    extra = msg[3:].strip()
                    new_item = f"{uname} {extra}" if extra else uname
                elif (re.match(r"^官服排队?\s", msg) or re.match(r"^官服排\s", msg)) and join_master_enabled and kg.get("guanfu_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    new_item = f"官|{uname} {extra}".rstrip()
                elif re.match(r"^[Bb]服排\s", msg) and join_master_enabled and kg.get("bfu_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    new_item = f"B|{uname} {extra}".rstrip()
                elif re.match(r"^超级排队?\s", msg) and join_master_enabled and kg.get("chaoji_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    new_item = f"<{uname}>{extra}" if extra else f"<{uname}>"
                elif msg.startswith("米服排 ") and join_master_enabled and kg.get("mifu_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    new_item = f"M|{uname} {extra}".rstrip()
                elif msg == "插队" and is_jianzhang and kg.get("jianzhang_chadui", False):
                    if len(self._persons) == 0 or not self._jianzhangchadui:
                        self._append_queue_item_unlocked(uname)
                    else:
                        insert_pos = len(self._persons)
                        while insert_pos > 0 and any(
                            j in self._persons[insert_pos - 1] for j in self._jianzhang
                        ):
                            insert_pos -= 1
                        self._insert_queue_item_unlocked(insert_pos, uname)
                    modified = True

                if new_item is not None and can_join:
                    self._append_queue_item_unlocked(new_item)
                    modified = True
                elif new_item is not None and not can_join and not has_op:
                    return False, "队列已满，无法加入排队"

            # --- 已在队列时再次发排队指令 ---
            if index >= 0 and self._is_join_cmd(msg) and not has_op:
                return False, f"已在队列第 {index + 1} 位，无法重复排队"

            # --- Self-service cancel/replace (already in queue) ---
            if index >= 0:
                if msg in ("取消排队", "排队取消", "我确认我取消排队") and self._kaiguan.get("quxiao_paidui", True):
                    self._remove_queue_item_unlocked(index)
                    modified = True
                elif msg in ("替换", "修改", "内容洗白") and self._kaiguan.get("xiugai_paidui", True):
                    self._replace_queue_item_unlocked(index, uname)
                    modified = True
                elif (msg.startswith("替换 ") or msg.startswith("修改 ")) and self._kaiguan.get("xiugai_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    self._replace_queue_item_unlocked(index, f"{uname} {extra}".rstrip())
                    modified = True

            # --- Operator/admin commands ---
            if not has_op and any(
                msg.startswith(p) for p in ("del ", "删除 ", "完成 ", "add ", "新增 ", "添加 ", "无影插 ", "插队 ")
            ):
                return False, "权限不足，该指令需要管理员权限"

            note: str | None = None
            if has_op:
                for kw in ("del", "删除", "完成"):
                    if kw in msg:
                        nums = re.sub(r"[^0-9]", "", msg)
                        kw_only = re.sub(r"[\d\s]+", "", msg)
                        if kw_only == kw and nums:
                            n = int(nums)
                            if 1 <= n <= len(self._persons):
                                self._remove_queue_item_unlocked(n - 1)
                                modified = True
                        break

                for prefix in ("add ", "新增 ", "添加 "):
                    if msg.startswith(prefix):
                        text = msg[len(prefix):].strip()
                        if text:
                            self._append_queue_item_unlocked(text)
                            modified = True
                        break

                m = re.match(r"^无影插\s+(\d+)\s+(.+)", msg)
                if m:
                    pos, text = int(m.group(1)), m.group(2).strip()
                    if text and 1 <= pos <= 20:
                        self._insert_queue_item_unlocked(pos - 1, text)
                        modified = True

                m2 = re.match(r"^插队\s+(\d+)\s+(.+)", msg)
                if m2:
                    pos, text = int(m2.group(1)), m2.group(2).strip()
                    if text and 1 <= pos <= 30:
                        self._insert_queue_item_unlocked(pos - 1, f"@{text}")
                        modified = True

                if msg == "暂停排队功能" or msg == "关闭自助排队":
                    self._kaiguan["paidui"] = False
                    self._all_disabled = True
                    self._persist_kaiguan_state_unlocked()
                elif msg == "恢复排队功能" or msg == "恢复自助排队":
                    self._kaiguan["paidui"] = True
                    self._all_disabled = False
                    self._persist_kaiguan_state_unlocked()
                elif msg == "开启舰长插队":
                    self._kaiguan["jianzhang_chadui"] = True
                    self._jianzhangchadui = True
                    self._persist_kaiguan_state_unlocked()
                elif msg == "关闭舰长插队":
                    self._kaiguan["jianzhang_chadui"] = False
                    self._jianzhangchadui = False
                    self._persist_kaiguan_state_unlocked()
                elif msg == "允许房管成为插件管理员":
                    self._kaiguan["fangguan_op"] = True
                    self._fangguan_can_doing = True
                    self._persist_kaiguan_state_unlocked()
                elif msg == "停止房管成为插件管理员":
                    self._kaiguan["fangguan_op"] = False
                    self._fangguan_can_doing = False
                    self._persist_kaiguan_state_unlocked()

                if any(kw in msg for kw in ("设置排队人数", "设置排队上限")):
                    nums = re.sub(r"[^0-9]", "", msg)
                    kw_only = re.sub(r"[\d\s]+", "", msg)
                    if kw_only in ("设置排队人数", "设置排队上限", "设置排队人数上限") and nums:
                        self._max_length = max(1, int(nums))
                        self._persist_myjs_state_unlocked()

                if msg.startswith("拉黑 "):
                    target = msg[3:].strip()
                    if target and target not in self._blacklist:
                        self._blacklist.append(target)
                        self._super_admins = [item for item in self._super_admins if item != target]
                        self._admins = [item for item in self._admins if item != target]
                        self._jianzhang = [item for item in self._jianzhang if item != target]
                        self._persist_quanxian_state_unlocked()
                elif msg.startswith("取消拉黑 "):
                    target = msg[5:].strip()
                    if target in self._blacklist:
                        self._blacklist.remove(target)
                        self._persist_quanxian_state_unlocked()

                if self._has_super_admin(uname, is_anchor):
                    if msg.startswith("添加管理员 "):
                        target = msg[6:].strip()
                        if target and target in self._blacklist:
                            note = "黑名单用户不能设为管理员"
                        elif target and target not in self._admins:
                            self._admins.append(target)
                            self._persist_quanxian_state_unlocked()
                    elif msg.startswith("取消管理员 "):
                        target = msg[6:].strip()
                        if target in self._admins:
                            self._admins.remove(target)
                            self._persist_quanxian_state_unlocked()

            return modified, note


DEFAULT_QUANXIAN: dict[str, Any] = {
    "super_admin": ["一纸轻予梦"],
    "admin": [],
    "jianzhang": [],
    "member": [],
    "blacklist": [],
}


def _normalize_quanxian_config(raw: Any) -> dict[str, list[str]]:
    normalized: dict[str, list[str]] = {key: list(values) for key, values in DEFAULT_QUANXIAN.items()}
    if isinstance(raw, dict):
        for key in DEFAULT_QUANXIAN:
            normalized[key] = _dedupe_string_list(raw.get(key, normalized[key]))

    blacklist = set(normalized.get("blacklist", []))
    for key in ("super_admin", "admin", "jianzhang", "member"):
        normalized[key] = [name for name in normalized.get(key, []) if name not in blacklist]
    return normalized


def _sync_quanxian_to_myjs(myjs_cfg: Any, quanxian_cfg: dict[str, Any]) -> dict[str, Any]:
    merged = _normalize_myjs_config(myjs_cfg)
    merged["admins"] = list(quanxian_cfg.get("admin", []))
    merged["ban_admins"] = list(quanxian_cfg.get("blacklist", []))
    merged["jianzhang"] = list(quanxian_cfg.get("jianzhang", []))
    return merged


def _sync_kaiguan_to_myjs(myjs_cfg: Any, kaiguan_cfg: dict[str, bool]) -> dict[str, Any]:
    merged = _normalize_myjs_config(myjs_cfg)
    merged["all_suoyourenbukepaidui"] = not bool(kaiguan_cfg.get("paidui", True))
    merged["fangguan_can_doing"] = bool(kaiguan_cfg.get("fangguan_op", False))
    merged["jianzhangchadui"] = bool(kaiguan_cfg.get("jianzhang_chadui", False))
    return merged

DEFAULT_KAIGUAN: dict[str, bool] = {
    "paidui": True,
    "guanfu_paidui": True,
    "bfu_paidui": True,
    "chaoji_paidui": True,
    "mifu_paidui": True,
    "quxiao_paidui": True,
    "xiugai_paidui": True,
    "jianzhang_chadui": False,
    "fangguan_op": False,
}


def load_quanxian() -> dict[str, Any]:
    raw_config = _read_raw_config()
    config_section = raw_config.get("quanxian", {})
    raw_file = load_simple_yaml(QUANXIAN_PATH)
    result: dict[str, Any] = {k: list(v) for k, v in DEFAULT_QUANXIAN.items()}
    for key in DEFAULT_QUANXIAN:
        if isinstance(config_section, dict) and isinstance(config_section.get(key), list):
            result[key] = _dedupe_string_list(config_section[key])
        elif isinstance(raw_file.get(key), list):
            result[key] = _dedupe_string_list(raw_file[key])
    return _normalize_quanxian_config(result)


def _write_quanxian_file(normalized: dict[str, Any]) -> None:
    labels = {
        "super_admin": "最高管理员：拥有所有权限，包括新增/删除管理员",
        "admin": "管理员：拥有除新增/删除管理员以外的所有操作权限",
        "jianzhang": "舰长：仅拥有「插队」命令权限",
        "member": "成员：普通观众",
        "blacklist": "黑名单：禁止触发任何弹幕指令，且不能同时是最高管理员/管理员",
    }
    lines: list[str] = ["# 权限配置\n"]
    for key in ("super_admin", "admin", "jianzhang", "member", "blacklist"):
        lines.append(f"# {labels[key]}\n{key}:\n")
        for item in normalized.get(key, []):
            escaped = str(item).replace('"', '\\"')
            lines.append(f'  - "{escaped}"\n')
        lines.append("\n")
    QUANXIAN_PATH.write_text("".join(lines), encoding="utf-8")
    write_blacklist_entries(BLACKLIST_PATH, blacklist_names_to_entries(normalized.get("blacklist", [])))


def save_quanxian(config: dict[str, Any]) -> None:
    normalized = _normalize_quanxian_config(config)
    _write_quanxian_file(normalized)

    current = _merge_config(DEFAULT_CONFIG, _read_raw_config())
    current["quanxian"] = normalized
    current["myjs"] = _sync_quanxian_to_myjs(current.get("myjs", {}), normalized)
    save_config(current)


def load_kaiguan() -> dict[str, bool]:
    raw_config = _read_raw_config()
    config_section = raw_config.get("kaiguan", {})
    raw_file = load_simple_yaml(KAIGUAN_PATH)
    result: dict[str, bool] = dict(DEFAULT_KAIGUAN)
    for key in DEFAULT_KAIGUAN:
        if isinstance(config_section, dict) and isinstance(config_section.get(key), bool):
            result[key] = config_section[key]
        elif isinstance(raw_file.get(key), bool):
            result[key] = raw_file[key]
    return result

def _write_kaiguan_file(normalized: dict[str, bool]) -> None:
    comments = {
        "paidui": "排队总开关：关闭后普通/官服/B服/超级/米服排队全部关闭",
        "guanfu_paidui": "官服排队（需总开关开启）",
        "bfu_paidui": "B服排队（需总开关开启）",
        "chaoji_paidui": "超级排队（需总开关开启）",
        "mifu_paidui": "米服排队（需总开关开启）",
        "quxiao_paidui": "取消排队",
        "xiugai_paidui": "修改/替换排队内容",
        "jianzhang_chadui": "舰长插队",
        "fangguan_op": "允许B站房管执行管理员命令",
    }
    lines: list[str] = ["# 功能开关（true=启用，false=禁用）\n"]
    for key, default in DEFAULT_KAIGUAN.items():
        value = normalized.get(key, default)
        value_str = "true" if value else "false"
        comment = comments.get(key, key)
        lines.append(f"{key}: {value_str}              # {comment}\n")
    KAIGUAN_PATH.write_text("".join(lines), encoding="utf-8")


def save_kaiguan(config: dict[str, bool]) -> None:
    normalized: dict[str, bool] = dict(DEFAULT_KAIGUAN)
    for key in DEFAULT_KAIGUAN:
        if isinstance(config.get(key), bool):
            normalized[key] = bool(config.get(key))

    _write_kaiguan_file(normalized)

    current = _merge_config(DEFAULT_CONFIG, _read_raw_config())
    current["kaiguan"] = normalized
    current["myjs"] = _sync_kaiguan_to_myjs(current.get("myjs", {}), normalized)
    save_config(current)


DEFAULT_STYLE: dict[str, Any] = {
    "bg1": "#0e2036",
    "bg2": "#060b14",
    "bg3": "#020409",
    "text_color": "#eaf6ff",
    "queue_font_size": 50,
    "queue_font_weight": "700",
    "queue_font_style": "italic",
    "text_grad_start": "#f7f7f7",
    "text_grad_end": "rgba(255,255,255,0.6)",
    "text_stroke_color": "#000000",
    "text_stroke_enabled": True,
}

DEFAULT_CONFIG["quanxian"] = {key: list(values) for key, values in DEFAULT_QUANXIAN.items()}
DEFAULT_CONFIG["kaiguan"] = dict(DEFAULT_KAIGUAN)
DEFAULT_CONFIG["style"] = dict(DEFAULT_STYLE)


def css_archive_path(slot: int) -> Path:
    normalized = max(1, min(MAX_QUEUE_ARCHIVE_SLOTS, int(slot)))
    return PD_DIR / f"cdang_{normalized}.css"


def _unique_paths(*paths: Path) -> list[Path]:
    result: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = os.path.normcase(str(path))
        if key in seen:
            continue
        seen.add(key)
        result.append(path)
    return result


def _legacy_css_archive_paths(slot: int) -> list[Path]:
    normalized = max(1, min(MAX_QUEUE_ARCHIVE_SLOTS, int(slot)))
    name = f"cdang_{normalized}.css"
    return _unique_paths(
        UI_DIR / name,
        BUNDLE_UI_DIR / name,
        CORE_DIR / "ui" / name,
    )


def _load_text_from_candidates(paths: list[Path]) -> str:
    for path in paths:
        try:
            if path.exists():
                return path.read_text(encoding="utf-8")
        except OSError:
            continue
    return ""


def _resolve_static_file(relative_path: str) -> Path | None:
    relative = Path(relative_path)
    for base_dir in _unique_paths(UI_DIR, BUNDLE_UI_DIR):
        target = (base_dir / relative).resolve()
        try:
            target.relative_to(base_dir.resolve())
        except ValueError:
            continue
        if target.is_file():
            return target
    return None


def build_index_css(style: dict[str, Any] | None = None) -> str:
    merged = dict(DEFAULT_STYLE)
    if isinstance(style, dict):
        merged.update(style)
    stroke_enabled = str(merged.get("text_stroke_enabled", True)).strip().lower() not in {"0", "false", "no", "off"}
    try:
        queue_font_size = max(8, int(merged.get("queue_font_size", 50)))
    except (TypeError, ValueError):
        queue_font_size = 50
    queue_font_weight = str(merged.get("queue_font_weight", DEFAULT_STYLE["queue_font_weight"]) or DEFAULT_STYLE["queue_font_weight"]).strip()
    queue_font_style = str(merged.get("queue_font_style", DEFAULT_STYLE["queue_font_style"]) or DEFAULT_STYLE["queue_font_style"]).strip()
    text_stroke = merged.get("text_stroke_color", DEFAULT_STYLE["text_stroke_color"]) if stroke_enabled else "transparent"
    return (
        ":root {\n"
        f"    --bg1: {merged.get('bg1', DEFAULT_STYLE['bg1'])};\n"
        f"    --bg2: {merged.get('bg2', DEFAULT_STYLE['bg2'])};\n"
        f"    --bg3: {merged.get('bg3', DEFAULT_STYLE['bg3'])};\n"
        f"    --text-color: {merged.get('text_color', DEFAULT_STYLE['text_color'])};\n"
        f"    --queue-font-size: {queue_font_size}px;\n"
        f"    --queue-font-weight: {queue_font_weight};\n"
        f"    --queue-font-style: {queue_font_style};\n"
        f"    --text-grad-start: {merged.get('text_grad_start', DEFAULT_STYLE['text_grad_start'])};\n"
        f"    --text-grad-end: {merged.get('text_grad_end', DEFAULT_STYLE['text_grad_end'])};\n"
        f"    --text-stroke: {text_stroke};\n"
        f"    --text-stroke-enabled: {1 if stroke_enabled else 0};\n"
        "}\n"
        "html, body { margin: 0; background: transparent !important; color: var(--text-color); }\n"
        ".wk { width: 100%; height: 80%; text-overflow: ellipsis; white-space: nowrap; }\n"
        ".div { width: 60%; height: 90%; line-height: 1.3; font-size: var(--queue-font-size); font-weight: var(--queue-font-weight); font-style: var(--queue-font-style); float: left; margin-top: 5%; text-align: left; overflow: hidden; }\n"
        ".div span { -webkit-text-stroke: 2px var(--text-stroke); color: var(--text-color); }\n"
        ".div span.pdj-confirm-pending { animation: pdjPendingBlink 0.8s infinite alternate; filter: brightness(1.35); }\n"
        "@keyframes pdjPendingBlink { from { opacity: 1; transform: scale(1); } to { opacity: 0.5; transform: scale(1.02); } }\n"
        ".vText { float: left; width: 10%; height: 100%; overflow: hidden; white-space: nowrap; font-weight: var(--queue-font-weight); font-style: var(--queue-font-style); text-align: center; font-size: var(--queue-font-size); }\n"
        ".vText div { height: 20%; padding-top: 20%; writing-mode: tb-rl; display: inline-block; }\n"
        ".vText span { -webkit-text-stroke: 3px var(--text-stroke); color: var(--text-color); }\n"
    )


def parse_style_from_css_text(css_text: str) -> dict[str, Any]:
    if not css_text:
        return {}
    result: dict[str, Any] = {}
    for key, css_var in STYLE_CSS_VAR_MAP.items():
        pattern = re.compile(rf"{re.escape(css_var)}\s*:\s*([^;]+);")
        match = pattern.search(css_text)
        if not match:
            continue
        value = match.group(1).strip()
        if key == "queue_font_size":
            if value.lower().endswith("px"):
                value = value[:-2].strip()
            try:
                result[key] = max(8, int(float(value)))
            except ValueError:
                continue
        elif key == "text_stroke_enabled":
            result[key] = str(value).strip() not in {"0", "false", "False", "no", "off"}
        else:
            result[key] = value
    return result


def ensure_style_css_archives() -> None:
    UI_DIR.mkdir(parents=True, exist_ok=True)
    PD_DIR.mkdir(parents=True, exist_ok=True)
    seed_style = dict(DEFAULT_STYLE)
    raw_config = _read_raw_config()
    config_style = raw_config.get("style", {})
    if isinstance(config_style, dict):
        seed_style.update(config_style)
    if STYLE_PATH.exists():
        try:
            style_json = json.loads(STYLE_PATH.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            style_json = {}
        if isinstance(style_json, dict):
            seed_style.update(style_json)

    active_slot = _current_style_slot(raw_config)
    active_archive_path = css_archive_path(active_slot)
    default_css = (
        _load_text_from_candidates([active_archive_path, *_legacy_css_archive_paths(active_slot)])
        or _load_text_from_candidates([BUNDLE_UI_DIR / LIVE_STYLE_CSS_PATH.name, CORE_DIR / "ui" / LIVE_STYLE_CSS_PATH.name])
        or build_index_css(seed_style)
    )
    if not LIVE_STYLE_CSS_PATH.exists():
        LIVE_STYLE_CSS_PATH.write_text(default_css, encoding="utf-8")
    live_css = LIVE_STYLE_CSS_PATH.read_text(encoding="utf-8")

    for slot in range(1, MAX_QUEUE_ARCHIVE_SLOTS + 1):
        archive_path = css_archive_path(slot)
        if not archive_path.exists():
            for legacy_path in _legacy_css_archive_paths(slot):
                if not legacy_path.exists():
                    continue
                archive_path.write_text(legacy_path.read_text(encoding="utf-8"), encoding="utf-8")
                try:
                    legacy_path.unlink()
                except OSError:
                    pass
                break
        if not archive_path.exists():
            archive_path.write_text(live_css, encoding="utf-8")


def _current_style_slot(raw_config: dict[str, Any] | None = None) -> int:
    config = raw_config if isinstance(raw_config, dict) else _read_raw_config()
    queue_archive = config.get("queue_archive", {})
    try:
        slot = int(queue_archive.get("active_slot", 1))
    except (TypeError, ValueError):
        slot = 1
    return max(1, min(MAX_QUEUE_ARCHIVE_SLOTS, slot))


def reconcile_live_css_with_archive(slot: int) -> str:
    ensure_style_css_archives()
    archive_path = css_archive_path(slot)
    live_text = LIVE_STYLE_CSS_PATH.read_text(encoding="utf-8") if LIVE_STYLE_CSS_PATH.exists() else ""
    archive_text = archive_path.read_text(encoding="utf-8") if archive_path.exists() else ""

    if not live_text and archive_text:
        LIVE_STYLE_CSS_PATH.write_text(archive_text, encoding="utf-8")
        return "archive"
    if live_text and not archive_text:
        archive_path.write_text(live_text, encoding="utf-8")
        return "live"
    if live_text == archive_text:
        return "same"

    live_mtime = LIVE_STYLE_CSS_PATH.stat().st_mtime if LIVE_STYLE_CSS_PATH.exists() else 0.0
    archive_mtime = archive_path.stat().st_mtime if archive_path.exists() else 0.0
    if live_mtime >= archive_mtime:
        archive_path.write_text(live_text, encoding="utf-8")
        return "live"
    LIVE_STYLE_CSS_PATH.write_text(archive_text, encoding="utf-8")
    return "archive"


def apply_css_archive_to_live(slot: int, *, force: bool = False) -> bool:
    ensure_style_css_archives()
    archive_path = css_archive_path(slot)
    if not archive_path.exists():
        archive_path.write_text(build_index_css(DEFAULT_STYLE), encoding="utf-8")
    archive_text = archive_path.read_text(encoding="utf-8")
    live_text = LIVE_STYLE_CSS_PATH.read_text(encoding="utf-8") if LIVE_STYLE_CSS_PATH.exists() else ""
    if not force and live_text == archive_text:
        return False
    LIVE_STYLE_CSS_PATH.write_text(archive_text, encoding="utf-8")
    return True


def load_style() -> dict[str, Any]:
    ensure_style_css_archives()
    raw_config = _read_raw_config()
    result = dict(DEFAULT_STYLE)
    config_section = raw_config.get("style", {})
    if isinstance(config_section, dict):
        result.update(config_section)

    if STYLE_PATH.exists():
        try:
            data = json.loads(STYLE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                result.update(data)
        except (json.JSONDecodeError, OSError):
            pass

    try:
        css_values = parse_style_from_css_text(LIVE_STYLE_CSS_PATH.read_text(encoding="utf-8"))
    except OSError:
        css_values = {}
    if css_values:
        result.update(css_values)
    return result


def save_style(data: dict[str, Any]) -> None:
    merged = dict(DEFAULT_STYLE)
    merged.update(data)
    STYLE_PATH.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    current = _merge_config(DEFAULT_CONFIG, _read_raw_config())
    current["style"] = merged
    save_config(current)
    css_text = build_index_css(merged)
    ensure_style_css_archives()
    LIVE_STYLE_CSS_PATH.write_text(css_text, encoding="utf-8")
    css_archive_path(_current_style_slot(current)).write_text(css_text, encoding="utf-8")


def load_model() -> dict[str, Any]:
    return bilibili_protocol.get_initial_model()



def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default



def _dispatch_login_callback(
    callback_cfg: dict[str, Any],
    *,
    cookie: str,
    bilibili_data: dict[str, Any],
    logger: logging.Logger,
) -> tuple[bool, str]:
    if not bool(callback_cfg.get("enabled", False)):
        return False, "callback disabled"

    callback_url = str(callback_cfg.get("url", "")).strip()
    if not callback_url:
        return False, "callback url is empty"

    payload = {
        "event": "bilibili_qr_login_success",
        "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
        "cookie": cookie,
        "bilibili": bilibili_data,
    }
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    timeout_seconds = max(1, int(callback_cfg.get("timeout_seconds", 5)))
    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "User-Agent": "DanmujiBackend/0.3",
    }
    auth_token = str(callback_cfg.get("auth_token", "")).strip()
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"

    req = urllib.request.Request(callback_url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            status = int(getattr(resp, "status", 200))
            if 200 <= status < 300:
                return True, f"callback ok (status={status})"
            return False, f"callback failed (status={status})"
    except urllib.error.URLError as exc:
        logger.warning("扫码回调失败: %s", exc)
        return False, f"callback failed ({exc})"


def _safe_static_path(request_path: str) -> Path | None:
    parsed = urlparse(request_path)
    path = parsed.path
    if path in {"/", ""}:
        path = "/config"
    if path == "/config":
        path = "/config.html"
    if path == "/index":
        path = "/index.html"
    if path == "/cookie-login":
        path = "/cookie_login.html"
    if path == f"/{LIVE_STYLE_CSS_PATH.name}":
        ensure_style_css_archives()
        return LIVE_STYLE_CSS_PATH

    return _resolve_static_file(path.lstrip("/"))


def _guess_content_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".html", ".htm"}:
        return "text/html; charset=utf-8"
    if suffix == ".js":
        return "application/javascript; charset=utf-8"
    if suffix == ".json":
        return "application/json; charset=utf-8"
    if suffix == ".css":
        return "text/css; charset=utf-8"
    return "application/octet-stream"


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "DanmujiBackend/0.3"

    def log_message(self, format: str, *args: Any) -> None:
        message = "%s - %s" % (self.address_string(), format % args)
        self.server.logger.debug(message)

    def _write_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve_static_file(self, file_path: Path) -> None:
        if file_path.name in {"index.html", LIVE_STYLE_CSS_PATH.name}:
            reconcile_live_css_with_archive(_current_style_slot())
            if file_path.name == LIVE_STYLE_CSS_PATH.name:
                file_path = LIVE_STYLE_CSS_PATH
        body = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", _guess_content_type(file_path))
        if file_path.suffix.lower() == ".css":
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _handle_websocket_upgrade(self) -> None:
        key = self.headers.get("Sec-WebSocket-Key", "")
        if not key:
            self._write_json(
                {"status": "error", "message": "Missing Sec-WebSocket-Key"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        accept = base64.b64encode(
            hashlib.sha1(f"{key}{WS_MAGIC_GUID}".encode("utf-8")).digest()
        ).decode("utf-8")

        self.send_response(HTTPStatus.SWITCHING_PROTOCOLS)
        self.send_header("Upgrade", "websocket")
        self.send_header("Connection", "Upgrade")
        self.send_header("Sec-WebSocket-Accept", accept)
        self.end_headers()

        client = self.connection
        client.settimeout(120)
        hub = self.server.ws_hub
        hub.register(client)
        self._ws_send_json(
            client,
            {
                "type": "PDJ_STATUS",
                "status": "connected",
                "message": "ws://127.0.0.1:9816/ws is ready",
            },
        )
        if hasattr(self.server, "queue_manager"):
            self.server.queue_manager.send_current_to(client)

        try:
            while True:
                message = self._ws_recv_text(client)
                if message is None:
                    break
                if message == "":
                    continue

                hub.mark_message()
                hub.broadcast_text(client, message)
        finally:
            hub.unregister(client)

    def _ws_recv_text(self, conn: socket.socket) -> str | None:
        try:
            head = conn.recv(2)
            if not head or len(head) < 2:
                return None

            b1, b2 = head
            opcode = b1 & 0x0F
            masked = (b2 >> 7) & 1
            payload_len = b2 & 0x7F

            if opcode == 0x8:
                return None
            if opcode == 0x9:  # ping
                _ws_send_text(conn, "", opcode=0xA)
                return ""
            if opcode != 0x1:
                return ""

            if payload_len == 126:
                payload_len = struct.unpack("!H", conn.recv(2))[0]
            elif payload_len == 127:
                payload_len = struct.unpack("!Q", conn.recv(8))[0]

            mask_key = conn.recv(4) if masked else b""
            payload = b""
            remaining = payload_len
            while remaining > 0:
                chunk = conn.recv(remaining)
                if not chunk:
                    return None
                payload += chunk
                remaining -= len(chunk)

            if masked:
                payload = bytes(
                    b ^ mask_key[i % 4] for i, b in enumerate(payload)
                )

            decoded = payload.decode("utf-8", errors="replace")
            return decoded
        except (ConnectionError, OSError, TimeoutError):
            return None

    def _ws_send_text(self, conn: socket.socket, text: str) -> None:
        _ws_send_text(conn, text)

    def _ws_send_json(self, conn: socket.socket, payload: dict[str, Any]) -> None:
        self._ws_send_text(conn, json.dumps(payload, ensure_ascii=False))

    def _build_basic_config_payload(self) -> dict[str, Any]:
        cfg = self.server.runtime_config
        bilibili_cfg = _get_bilibili_config(cfg)
        return {
            "roomid": int(bilibili_cfg.get("roomid", 0)),
            "uid": int(bilibili_cfg.get("uid", 0)),
            "bilibili": bilibili_cfg,
        }

    def _build_login_config_payload(self) -> dict[str, Any]:
        payload = self._build_basic_config_payload()
        payload["cookie"] = str(_get_bilibili_config(self.server.runtime_config).get("cookie", ""))
        payload["qr_login"] = self.server.runtime_config.get("qr_login", {})
        return payload

    def _save_login_config(self, *, uid: int, cookie: str) -> dict[str, Any]:
        login_state = bilibili_protocol.resolve_bilibili_login(
            cookie,
            fallback_uid=uid,
            logger=self.server.logger if cookie else None,
        )
        resolved_uid = _to_int(login_state.get("uid", 0))
        if cookie and resolved_uid > 0 and resolved_uid != uid:
            self.server.logger.info(
                "Auto-correcting Bilibili login UID during login save: %s -> %s (source=%s)",
                uid,
                resolved_uid,
                login_state.get("uid_source", "unknown"),
            )
            uid = resolved_uid

        updated = _merge_config(
            self.server.runtime_config,
            {
                "bilibili": {
                    "uid": uid,
                    "cookie": cookie,
                }
            },
        )
        save_config(updated)
        updated = load_config()
        self.server.runtime_config = updated

        if hasattr(self.server, "danmu_relay"):
            self.server.danmu_relay.request_reconnect()
        if hasattr(self.server, "queue_manager"):
            self.server.queue_manager.load_config(
                updated.get("myjs", {}),
                anchor_uid=_to_int(_get_bilibili_config(updated).get("uid", 0)),
            )
            self.server.queue_manager.load_quanxian(updated.get("quanxian", {}))

        self.server.logger.info("登录配置已更新，触发弹幕重连 uid=%s", uid)
        return {
            "status": "ok",
            "roomid": int(_get_bilibili_config(updated).get("roomid", 0)),
            "uid": uid,
            "cookie": cookie,
            "uname": str(login_state.get("uname", "") or ""),
            "uid_source": str(login_state.get("uid_source", "") or ""),
            "is_login": bool(login_state.get("is_login", False)),
        }

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path in {"/ws", "/danmu/sub"}:
            self._handle_websocket_upgrade()
            return

        if parsed.path == "/health":
            self._write_json(
                {
                    "status": "ok",
                    "service": "danmuji-python-backend",
                    "port": self.server.server_port,
                }
            )
            return

        if parsed.path == "/model":
            try:
                model = load_model()
                self._write_json({"status": "ok", "model": model})
            except FileNotFoundError as exc:
                self._write_json(
                    {
                        "status": "error",
                        "message": str(exc),
                    },
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            except json.JSONDecodeError as exc:
                self._write_json(
                    {
                        "status": "error",
                        "message": f"Model JSON is invalid: {exc}",
                    },
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return

        if parsed.path == "/":
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", "/config")
            self.end_headers()
            return

        static_path = _safe_static_path(self.path)
        if static_path:
            self._serve_static_file(static_path)
            return

        if parsed.path == "/api/config/basic":
            self._write_json(self._build_basic_config_payload())
            return
        if parsed.path == "/api/config/login":
            self._write_json(self._build_login_config_payload())
            return
        if parsed.path == "/api/config":
            cfg = self.server.runtime_config
            bilibili_cfg = _get_bilibili_config(cfg)
            self._write_json(
                {
                    "roomid": int(bilibili_cfg.get("roomid", 0)),
                    "uid": int(bilibili_cfg.get("uid", 0)),
                    "cookie": str(bilibili_cfg.get("cookie", "")),
                    "bilibili": bilibili_cfg,
                    "qr_login": cfg.get("qr_login", {}),
                    "callback": cfg.get("callback", {}),
                    "myjs": cfg.get("myjs", {}),
                    "ui": cfg.get("ui", {}),
                    "quanxian": cfg.get("quanxian", {}),
                    "kaiguan": cfg.get("kaiguan", {}),
                    "style": cfg.get("style", {}),
                }
            )
            return
        if parsed.path == "/api/runtime-status":
            relay_status = (
                self.server.danmu_relay.get_runtime_status()
                if hasattr(self.server, "danmu_relay")
                else {}
            )
            self._write_json(
                {
                    "status": "ok",
                    "ws_clients": self.server.ws_hub.client_count,
                    "danmu_stream_active": bool(relay_status.get("connected", False)),
                    "last_message_at": self.server.ws_hub.last_message_at,
                    "danmu_connected": bool(relay_status.get("connected", False)),
                    "danmu_last_packet_at": str(relay_status.get("last_packet_at", "") or ""),
                    "danmu_last_connect_at": str(relay_status.get("last_connect_at", "") or ""),
                    "danmu_last_disconnect_at": str(relay_status.get("last_disconnect_at", "") or ""),
                    "danmu_last_disconnect_reason": str(relay_status.get("last_disconnect_reason", "") or ""),
                    "danmu_idle_seconds": relay_status.get("idle_seconds"),
                    "danmu_roomid": int(relay_status.get("roomid", 0) or 0),
                    "danmu_host": str(relay_status.get("host", "") or ""),
                    "danmu_port": int(relay_status.get("port", 0) or 0),
                    "danmu_transport": str(relay_status.get("transport", "") or ""),
                    "danmu_auth_uid": int(relay_status.get("auth_uid", 0) or 0),
                }
            )
            return

        if parsed.path == "/api/queue/state":
            qm = getattr(self.server, "queue_manager", None)
            current = qm.get_queue() if qm is not None else []
            entries = qm.get_queue_entries() if qm is not None else queue_items_to_entries(current)
            self._write_json(
                {
                    "status": "ok",
                    "queue": current,
                    "entries": entries,
                    "size": len(current),
                }
            )
            return

        if parsed.path == "/api/blacklist/state":
            entries = (
                self.server.queue_manager.get_blacklist_entries()
                if hasattr(self.server, "queue_manager")
                else read_blacklist_entries()
            )
            self._write_json(
                {
                    "status": "ok",
                    "entries": entries,
                    "size": len(entries),
                }
            )
            return

        if parsed.path == "/api/queue/archive":
            snapshot = self.server.queue_archive.read_latest_snapshot()
            self._write_json(
                {
                    "status": "ok",
                    "enabled": bool(getattr(self.server.queue_archive, "enabled", True)),
                    "slot": int(snapshot.get("slot", 0) or 0),
                    "path": str(snapshot.get("path", "") or ""),
                    "timestamp": str(snapshot.get("timestamp", "") or ""),
                    "last_operation_at": str(snapshot.get("last_operation_at", "") or ""),
                    "actor": str(snapshot.get("actor", "") or ""),
                    "message": str(snapshot.get("message", "") or ""),
                    "queue": snapshot.get("queue", []),
                    "entries": snapshot.get("entries", []),
                }
            )
            return

        if parsed.path == "/api/quanxian":
            quanxian = load_quanxian()
            self._write_json({"status": "ok", **quanxian})
            return

        if parsed.path == "/api/kaiguan":
            kaiguan = load_kaiguan()
            self._write_json({"status": "ok", **kaiguan})
            return

        if parsed.path == "/api/style":
            self._write_json({"status": "ok", **load_style()})
            return

        if parsed.path == "/api/bili/qr/start":
            try:
                payload = bilibili_protocol.bilibili_qr_generate()
            except urllib.error.URLError as exc:
                self._write_json(
                    {"status": "error", "message": f"Bilibili 接口访问失败: {exc}"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            except json.JSONDecodeError:
                self._write_json(
                    {"status": "error", "message": "Bilibili 返回了无效 JSON"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return

            data = payload.get("data", {})
            if isinstance(data, dict):
                qr_url = str(data.get("url", "")).strip()
                qr_base64, qr_error = bilibili_protocol.build_qr_png_base64(qr_url)
                if qr_base64:
                    data["qr_image_base64"] = qr_base64
                if qr_error:
                    data["qr_image_error"] = qr_error
                payload["data"] = data
            self._write_json(payload)
            return

        self._write_json(
            {"status": "error", "message": f"Path not found: {self.path}"},
            status=HTTPStatus.NOT_FOUND,
        )

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/api/config/login":
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0:
                self._write_json(
                    {"status": "error", "message": "Empty request body"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return

            raw = self.rfile.read(length).decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self._write_json(
                    {"status": "error", "message": "Body must be valid JSON"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return

            uid = int(payload.get("uid", 0))
            cookie = str(payload.get("cookie", "")).strip()
            self._write_json(self._save_login_config(uid=uid, cookie=cookie))
            return

        if parsed.path == "/api/config":
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0:
                self._write_json(
                    {"status": "error", "message": "Empty request body"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return

            raw = self.rfile.read(length).decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self._write_json(
                    {"status": "error", "message": "Body must be valid JSON"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return

            roomid = int(payload.get("roomid", 0))
            uid = int(payload.get("uid", 0))
            cookie = str(payload.get("cookie", "")).strip()
            callback_payload = payload.get("callback", {})
            callback_enabled = bool(callback_payload.get("enabled", False)) if isinstance(callback_payload, dict) else False
            callback_url = str(callback_payload.get("url", "")).strip() if isinstance(callback_payload, dict) else ""
            callback_auth_token = str(callback_payload.get("auth_token", "")).strip() if isinstance(callback_payload, dict) else ""
            callback_timeout = int(callback_payload.get("timeout_seconds", 5)) if isinstance(callback_payload, dict) else 5
            incoming_myjs = payload.get("myjs", {})
            myjs_payload = _normalize_myjs_config(incoming_myjs) if isinstance(incoming_myjs, dict) else None
            incoming_quanxian = payload.get("quanxian", {})
            quanxian_payload = None
            if isinstance(incoming_quanxian, dict):
                base_quanxian = load_quanxian()
                for key in ("super_admin", "admin", "jianzhang", "member", "blacklist"):
                    if key in incoming_quanxian:
                        base_quanxian[key] = incoming_quanxian.get(key, [])
                quanxian_payload = _normalize_quanxian_config(base_quanxian)
            elif myjs_payload is not None and any(
                key in myjs_payload for key in ("admins", "ban_admins", "jianzhang")
            ):
                quanxian_payload = _normalize_quanxian_config(
                    {
                        **load_quanxian(),
                        "admin": myjs_payload.get("admins", []),
                        "jianzhang": myjs_payload.get("jianzhang", []),
                        "blacklist": myjs_payload.get("ban_admins", []),
                    }
                )
            login_state = bilibili_protocol.resolve_bilibili_login(
                cookie,
                fallback_uid=uid,
                logger=self.server.logger if cookie else None,
            )
            resolved_uid = _to_int(login_state.get("uid", 0))
            if cookie and resolved_uid > 0 and resolved_uid != uid:
                self.server.logger.info(
                    "Auto-correcting Bilibili login UID during config save: %s -> %s (source=%s)",
                    uid,
                    resolved_uid,
                    login_state.get("uid_source", "unknown"),
                )
                uid = resolved_uid

            updated = _merge_config(
                self.server.runtime_config,
                {
                    "bilibili": {"roomid": roomid, "uid": uid, "cookie": cookie},
                    "callback": {
                        "enabled": callback_enabled,
                        "url": callback_url,
                        "auth_token": callback_auth_token,
                        "timeout_seconds": max(1, callback_timeout),
                    },
                    **({"myjs": myjs_payload} if myjs_payload is not None else {}),
                },
            )
            updated["myjs"] = _normalize_myjs_config(updated.get("myjs", {}))
            save_config(updated)
            if quanxian_payload is not None:
                save_quanxian(quanxian_payload)
            updated = load_config()
            self.server.runtime_config = updated
            if hasattr(self.server, "danmu_relay"):
                self.server.danmu_relay.request_reconnect()
            if hasattr(self.server, "queue_manager"):
                self.server.queue_manager.load_config(
                    updated.get("myjs", {}),
                    anchor_uid=_to_int(_get_bilibili_config(updated).get("uid", 0)),
                )
                self.server.queue_manager.load_quanxian(updated.get("quanxian", {}))
            self.server.logger.info("配置已更新，触发直播间弹幕重连 roomid=%s uid=%s", roomid, uid)
            self._write_json(
                {
                    "status": "ok",
                    "roomid": roomid,
                    "uid": uid,
                    "uname": str(login_state.get("uname", "") or ""),
                    "uid_source": str(login_state.get("uid_source", "") or ""),
                    "is_login": bool(login_state.get("is_login", False)),
                    "myjs": updated.get("myjs", {}),
                    "quanxian": updated.get("quanxian", {}),
                }
            )
            return

        if parsed.path == "/api/queue/reload":
            if hasattr(self.server, "queue_manager"):
                self.server.queue_manager.restore_from_archive()
                current = self.server.queue_manager.get_queue()
                entries = self.server.queue_manager.get_queue_entries()
                self.server.queue_manager._broadcast_and_archive("gui", "reload")
            else:
                current = []
                entries = []
            self._write_json(
                {
                    "status": "ok",
                    "queue": current,
                    "entries": entries,
                    "size": len(current),
                }
            )
            return

        if parsed.path in {
            "/api/queue/delete",
            "/api/queue/move",
            "/api/queue/insert",
            "/api/queue/update",
            "/api/queue/clear",
        }:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="replace") if length > 0 else "{}"
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {}
            qm = getattr(self.server, "queue_manager", None)
            if qm is None:
                self._write_json({"status": "error", "message": "queue_manager not ready"})
                return
            if parsed.path == "/api/queue/delete":
                idx = _to_int(payload.get("index", 0), 0)
                current = qm.delete_item(idx)
            elif parsed.path == "/api/queue/move":
                idx = _to_int(payload.get("index", 0), 0)
                direction = str(payload.get("direction", "up"))
                current = qm.move_item(idx, direction)
            elif parsed.path == "/api/queue/insert":
                after = _to_int(payload.get("after", 0), 0)
                entry = str(payload.get("entry", ""))
                current = qm.insert_item(after, entry)
            elif parsed.path == "/api/queue/update":
                idx = _to_int(payload.get("index", 0), 0)
                content = str(payload.get("content", ""))
                current = qm.update_item_content(idx, content)
            else:  # /api/queue/clear
                current = qm.clear_queue()
            entries = qm.get_queue_entries()
            self._write_json(
                {
                    "status": "ok",
                    "queue": current,
                    "entries": entries,
                    "size": len(current),
                }
            )
            return

        if parsed.path in {
            "/api/blacklist/add",
            "/api/blacklist/delete",
            "/api/blacklist/clear",
        }:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="replace") if length > 0 else "{}"
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {}
            qm = getattr(self.server, "queue_manager", None)
            if qm is None:
                self._write_json({"status": "error", "message": "queue_manager not ready"})
                return
            if parsed.path == "/api/blacklist/add":
                name = str(payload.get("name", "")).strip()
                entries = qm.add_blacklist_item(name)
            elif parsed.path == "/api/blacklist/delete":
                idx = _to_int(payload.get("index", 0), 0)
                entries = qm.delete_blacklist_item(idx)
            else:
                entries = qm.clear_blacklist()
            self.server.runtime_config = load_config()
            self._write_json(
                {
                    "status": "ok",
                    "entries": entries,
                    "size": len(entries),
                }
            )
            return

        if parsed.path == "/api/style":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="replace") if length > 0 else "{}"
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {}
            if isinstance(payload, dict):
                save_style(payload)
                self.server.runtime_config = _merge_config(self.server.runtime_config, {"style": load_style()})
                active_slot = _current_style_slot()
                self.server.ws_hub.broadcast_json(
                    None,
                    {"type": "STYLE_UPDATE", "slot": active_slot, "css": LIVE_STYLE_CSS_PATH.name, "reason": "save_style"},
                )
            self._write_json({"status": "ok"})
            return

        if parsed.path == "/api/queue/switch":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="replace") if length > 0 else "{}"
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                payload = {}
            slot = _to_int(payload.get("slot", 1), 1)
            slot = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, slot))
            if hasattr(self.server, "queue_manager"):
                current = self.server.queue_manager.switch_to_slot(slot)
                entries = self.server.queue_manager.get_queue_entries()
            else:
                current = []
                entries = []
            self.server.runtime_config = load_config()
            self._write_json({"status": "ok", "slot": slot, "queue": current, "entries": entries, "size": len(current)})
            return

        if parsed.path == "/api/quanxian":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="replace") if length > 0 else "{}"
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self._write_json({"status": "error", "message": "Body must be valid JSON"}, status=HTTPStatus.BAD_REQUEST)
                return
            save_quanxian(payload)
            saved_quanxian = load_quanxian()
            self.server.runtime_config = load_config()
            if hasattr(self.server, "queue_manager"):
                self.server.queue_manager.load_quanxian(saved_quanxian)
            self._write_json({"status": "ok"})
            return

        if parsed.path == "/api/kaiguan":
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode("utf-8", errors="replace") if length > 0 else "{}"
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self._write_json({"status": "error", "message": "Body must be valid JSON"}, status=HTTPStatus.BAD_REQUEST)
                return
            save_kaiguan(payload)
            saved_kaiguan = load_kaiguan()
            self.server.runtime_config = load_config()
            if hasattr(self.server, "queue_manager"):
                self.server.queue_manager.load_kaiguan(saved_kaiguan)
            self._write_json({"status": "ok"})
            return

        if parsed.path == "/api/bili/qr/poll":
            length = int(self.headers.get("Content-Length", "0"))
            if length <= 0:
                self._write_json(
                    {"status": "error", "message": "Empty request body"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return

            raw = self.rfile.read(length).decode("utf-8", errors="replace")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self._write_json(
                    {"status": "error", "message": "Body must be valid JSON"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return

            qrcode_key = str(payload.get("qrcode_key", "")).strip()
            if not qrcode_key:
                self._write_json(
                    {"status": "error", "message": "qrcode_key is required"},
                    status=HTTPStatus.BAD_REQUEST,
                )
                return

            try:
                bilibili_payload, cookie_text = bilibili_protocol.bilibili_qr_poll(qrcode_key)
            except urllib.error.URLError as exc:
                self._write_json(
                    {"status": "error", "message": f"Bilibili 接口访问失败: {exc}"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            except json.JSONDecodeError:
                self._write_json(
                    {"status": "error", "message": "Bilibili 返回了无效 JSON"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return

            data = bilibili_payload.get("data", {})
            if isinstance(data, dict):
                data["cookie"] = cookie_text
                bilibili_payload["data"] = data

                try:
                    poll_code = int(data.get("code", -1))
                except (TypeError, ValueError):
                    poll_code = -1

                if poll_code == 0 and cookie_text:
                    login_state = bilibili_protocol.resolve_bilibili_login(
                        cookie_text,
                        fallback_uid=int(_get_bilibili_config(self.server.runtime_config).get("uid", 0)),
                        logger=self.server.logger,
                    )
                    resolved_uid = _to_int(login_state.get("uid", 0))
                    resolved_uname = str(login_state.get("uname", "") or "")
                    if resolved_uid > 0:
                        data["uid"] = resolved_uid
                    if resolved_uname:
                        data["uname"] = resolved_uname
                    data["uid_source"] = str(login_state.get("uid_source", "") or "")
                    data["is_login"] = bool(login_state.get("is_login", False))
                    success_time = dt.datetime.now(dt.timezone.utc).isoformat()
                    bilibili_update: dict[str, Any] = {"cookie": cookie_text}
                    if resolved_uid > 0:
                        bilibili_update["uid"] = resolved_uid
                    updated = _merge_config(
                        self.server.runtime_config,
                        {
                            "bilibili": bilibili_update,
                            "qr_login": {
                                "last_success_at": success_time,
                                "qrcode_key": qrcode_key,
                                "poll_code": poll_code,
                                "message": str(data.get("message", "")),
                                "cookie": cookie_text,
                            },
                        },
                    )
                    save_config(updated)
                    self.server.runtime_config = load_config()
                    if hasattr(self.server, "danmu_relay"):
                        self.server.danmu_relay.request_reconnect()
                    callback_ok, callback_message = _dispatch_login_callback(
                        self.server.runtime_config.get("callback", {}),
                        cookie=cookie_text,
                        bilibili_data=data,
                        logger=self.server.logger,
                    )
                    self.server.logger.info(
                        "Bilibili QR login succeeded; cookie saved and danmu reconnect requested uid_present=%s uname_present=%s",
                        resolved_uid > 0,
                        bool(resolved_uname),
                    )
                    data["callback"] = {
                        "attempted": True,
                        "ok": callback_ok,
                        "message": callback_message,
                    }
                    self.server.logger.info("Bilibili 扫码成功，Cookie 已自动写入 config.yaml")
            self._write_json(bilibili_payload)
            return

        if parsed.path != "/api/queue/log":
            self._write_json(
                {"status": "error", "message": f"Path not found: {self.path}"},
                status=HTTPStatus.NOT_FOUND,
            )
            return

        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            self._write_json(
                {"status": "error", "message": "Empty request body"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        raw = self.rfile.read(length).decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            self._write_json(
                {"status": "error", "message": "Body must be valid JSON"},
                status=HTTPStatus.BAD_REQUEST,
            )
            return

        actor = str(payload.get("actor", "unknown"))
        message = str(payload.get("message", ""))
        queue_items = payload.get("queue", [])
        if not isinstance(queue_items, list):
            queue_items = []
        queue_items = [str(item) for item in queue_items]

        archive_path = self.server.queue_archive.write_snapshot(actor, message, queue_items)
        self.server.logger.info(
            "[queue] actor_present=%s message_chars=%s queue_size=%s archive=%s",
            bool(str(actor).strip()),
            len(str(message)),
            len(queue_items),
            archive_path,
        )
        self._write_json(
            {
                "status": "ok",
                "archive": str(archive_path) if archive_path else None,
                "queue_size": len(queue_items),
            }
        )


def run_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    runtime_config = load_config()
    ensure_runtime_layout()
    logger = setup_logging(runtime_config)
    archive_cfg = runtime_config.get("queue_archive", {})

    httpd = BackendServer((host, port), ApiHandler)
    httpd.runtime_config = runtime_config
    httpd.logger = logger
    httpd.queue_archive = QueueArchiveManager(
        slots=MAX_QUEUE_ARCHIVE_SLOTS,
        enabled=bool(archive_cfg.get("enabled", True)),
    )
    cfg_active_slot = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(archive_cfg.get("active_slot", 1))))
    httpd.queue_archive.set_active_slot(cfg_active_slot)
    reconcile_live_css_with_archive(cfg_active_slot)
    httpd.ws_hub = WebSocketHub(logger)
    httpd.queue_manager = QueueManager(
        ws_hub=httpd.ws_hub,
        queue_archive=httpd.queue_archive,
        logger=logger,
        runtime_reload_callback=lambda: setattr(httpd, "runtime_config", load_config()),
    )
    httpd.queue_manager.load_config(
        runtime_config.get("myjs", {}),
        anchor_uid=_to_int(_get_bilibili_config(runtime_config).get("uid", 0)),
    )
    httpd.queue_manager.load_quanxian(load_quanxian())
    httpd.queue_manager.load_kaiguan(load_kaiguan())
    httpd.queue_manager.restore_from_archive()
    httpd.danmu_relay = bilibili_protocol.BilibiliDanmuRelay(httpd)
    httpd.danmu_relay.start()
    if _migrate_legacy_bilibili_config_if_needed(runtime_config, logger=logger):
        runtime_config = load_config()
        httpd.runtime_config = runtime_config

    logger.info("后端已启动，地址：http://%s:%s", host, port)
    logger.info("配置页：http://127.0.0.1:%s/config", port)
    logger.info("排队展示页：http://127.0.0.1:%s/index", port)
    logger.info("WebSocket：ws://127.0.0.1:%s/ws（别名：/danmu/sub）", port)

    # 打印非敏感配置
    srv_cfg = runtime_config.get("server", {})
    bilibili_cfg = _get_bilibili_config(runtime_config)
    log_cfg = runtime_config.get("logging", {})
    qa_cfg = runtime_config.get("queue_archive", {})
    logger.info(
        "[config] server=%s:%s  roomid=%s  uid=%s  log_level=%s  retention=%sd  "
        "archive_enabled=%s  active_slot=%s",
        srv_cfg.get("host", host), srv_cfg.get("port", port),
        bilibili_cfg.get("roomid", 0),
        bilibili_cfg.get("uid", 0),
        log_cfg.get("level", "INFO"),
        log_cfg.get("retention_days", 7),
        qa_cfg.get("enabled", True),
        cfg_active_slot,
    )
    try:
        httpd.serve_forever()
    finally:
        httpd.danmu_relay.stop()


def _warn_use_gui_startup() -> None:
    message = "请使用 GUI 控制台启动后端：python core/control_panel.py"
    print(message)
    if sys.platform != "win32":
        return
    try:
        import ctypes  # noqa: PLC0415

        ctypes.windll.user32.MessageBoxW(0, message, "弹幕排队姬", 0x40)
    except Exception:
        pass


if __name__ == "__main__":
    if os.getenv("DANMUJI_LAUNCHED_BY_GUI", "").strip() != "1":
        _warn_use_gui_startup()
        raise SystemExit(0)
    config = load_config()
    host = os.getenv("DANMUJI_BACKEND_HOST", str(config.get("server", {}).get("host", DEFAULT_HOST)))
    port = int(os.getenv("DANMUJI_BACKEND_PORT", int(config.get("server", {}).get("port", DEFAULT_PORT))))
    run_server(host=host, port=port)
