from __future__ import annotations

import base64
import csv
import datetime as dt
import hashlib
import io
import json
import logging
import os
import random
import re
import socket
import ssl
import struct
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

try:
    import qrcode
except ImportError:  # pragma: no cover - 运行时环境可选依赖
    qrcode = None

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9816
MAX_QUEUE_ARCHIVE_SLOTS = 10
DANMU_HEARTBEAT_INTERVAL_SECONDS = 30
DANMU_IDLE_RECONNECT_SECONDS = 90

REPO_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = Path(__file__).resolve().parent  # bilipdj/core/
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", REPO_DIR))
APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else REPO_DIR
_YAML_DIR = APP_DIR if getattr(sys, "frozen", False) else CORE_DIR

MODEL_JSON_PATH = BUNDLE_DIR / "core" / "danmuji_initial_model.json"
UI_DIR = BUNDLE_DIR / "core" / "ui"
CONFIG_PATH = _YAML_DIR / "config.yaml"
LOG_DIR = APP_DIR / "log"
PD_DIR = APP_DIR / "core" / "cd"
QUEUE_STATE_PATH = PD_DIR / "queue_archive_state.json"
QUANXIAN_PATH = _YAML_DIR / "quanxian.yaml"
KAIGUAN_PATH = _YAML_DIR / "kaiguan.yaml"
STYLE_PATH = _YAML_DIR / "style.json"

WS_MAGIC_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
MAX_SAFE_INTEGER = (1 << 53) - 1
BILIBILI_QR_GENERATE_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
BILIBILI_QR_POLL_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
BILIBILI_NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
BILIBILI_DANMU_CONF_URL = "https://api.live.bilibili.com/room/v1/Danmu/getConf"
BILIBILI_DANMU_INFO_URL = "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo"
BILIBILI_ROOM_INIT_URL = "https://api.live.bilibili.com/room/v1/Room/room_init"

try:
    import brotli
except ImportError:  # pragma: no cover - 可选依赖
    brotli = None


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


def _ws_recv_exact(conn: socket.socket, size: int) -> bytes | None:
    chunks = bytearray()
    while len(chunks) < size:
        try:
            chunk = conn.recv(size - len(chunks))
        except (TimeoutError, socket.timeout):
            raise TimeoutError("socket recv timeout")
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)


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


def queue_item_to_entry(item: Any) -> dict[str, str]:
    item_id, content = queue_item_to_parts(item)
    return {"id": item_id, "content": content}


def queue_items_to_entries(queue_items: list[Any]) -> list[dict[str, str]]:
    return [queue_item_to_entry(item) for item in queue_items if str(item or "").strip()]


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
    entries: list[dict[str, str]] = []

    for row in rows:
        if not row or not any(str(cell).strip() for cell in row):
            continue

        first = str(row[0]).strip()
        lowered = first.lower()

        if first in meta:
            meta[first] = str(row[1]).strip() if len(row) > 1 else ""
            continue

        if first == ARCHIVE_HEADER_SEQ or lowered in {"position", "seq"}:
            continue

        if first.isdigit():
            if len(row) >= 3:
                item_id = str(row[1]).strip()
                content = str(row[2]).strip()
            elif len(row) >= 2:
                item_id, content = queue_item_to_parts(row[1])
            else:
                continue
            entries.append({"id": item_id, "content": content})

    return meta, entries


def read_queue_archive_entries(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as f:
            rows = list(csv.reader(f))
    except OSError:
        return []
    return parse_queue_archive_rows(rows)[1]


def write_queue_archive_entries(path: Path, entries: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([ARCHIVE_HEADER_SEQ, ARCHIVE_HEADER_ID, ARCHIVE_HEADER_CONTENT])
        for idx, entry in enumerate(entries, start=1):
            if not isinstance(entry, dict):
                continue
            item_id = str(entry.get("id", "")).strip()
            content = str(entry.get("content", "")).strip()
            writer.writerow([idx, item_id, content])


DEFAULT_CONFIG: dict[str, Any] = {
    "server": {"host": DEFAULT_HOST, "port": DEFAULT_PORT},
    "api": {"roomid": 3049445, "uid": 0, "cookie": ""},
    "qr_login": {
        "last_success_at": "",
        "qrcode_key": "",
        "poll_code": -1,
        "message": "",
        "cookie": "",
    },
    "callback": {"enabled": False, "url": "", "auth_token": "", "timeout_seconds": 5},
    "myjs": {},
    "ui": {"startup_splash_seconds": 5, "auto_start_backend": False, "language": "中文"},
    "logging": {"level": "INFO", "retention_days": 7},
    "queue_archive": {"enabled": True, "slots": MAX_QUEUE_ARCHIVE_SLOTS, "active_slot": 1},
}


def ensure_runtime_layout() -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PD_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
    if not QUANXIAN_PATH.exists():
        save_quanxian(DEFAULT_QUANXIAN)
    if not KAIGUAN_PATH.exists():
        save_kaiguan(DEFAULT_KAIGUAN)

    for slot in range(1, MAX_QUEUE_ARCHIVE_SLOTS + 1):
        slot_file = PD_DIR / f"queue_archive_slot_{slot}.csv"
        if not slot_file.exists():
            write_queue_archive_entries(slot_file, [])


def load_config() -> dict[str, Any]:
    ensure_runtime_layout()
    merged = _merge_config(DEFAULT_CONFIG, _read_raw_config())
    merged["myjs"] = _normalize_myjs_config(merged.get("myjs", {}))
    return merged


def save_config(config: dict[str, Any]) -> None:
    server = config.get("server", {})
    api = config.get("api", {})
    qr_login = config.get("qr_login", {})
    callback_cfg = config.get("callback", {})
    myjs_cfg = config.get("myjs", {})
    ui_cfg = config.get("ui", {})
    logging_cfg = config.get("logging", {})
    queue_archive = config.get("queue_archive", {})
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

    cookie_text = _yaml_quote_string(api.get("cookie", ""))
    qr_last_success_at = _yaml_quote_string(qr_login.get("last_success_at", ""))
    qr_qrcode_key = _yaml_quote_string(qr_login.get("qrcode_key", ""))
    qr_message = _yaml_quote_string(qr_login.get("message", ""))
    qr_cookie = _yaml_quote_string(qr_login.get("cookie", ""))
    callback_url = _yaml_quote_string(callback_cfg.get("url", ""))
    callback_auth_token = _yaml_quote_string(callback_cfg.get("auth_token", ""))

    content = f"""# Danmuji 全局配置
server:
  host: {server.get('host', DEFAULT_HOST)}
  port: {int(server.get('port', DEFAULT_PORT))}

api:
  roomid: {int(api.get('roomid', 0))}
  uid: {int(api.get('uid', 0))}
  cookie: {cookie_text}

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

    def write_snapshot(self, actor: str, message: str, queue_items: list[str]) -> Path | None:
        if not self.enabled:
            return None

        slot = self.get_active_slot()
        out = self._slot_file(slot)
        write_queue_archive_entries(out, queue_items_to_entries(queue_items))
        return out

    def write_blank_snapshot(self, actor: str, message: str) -> Path | None:
        if not self.enabled:
            return None
        slot = self.get_active_slot()
        out = self._slot_file(slot)
        write_queue_archive_entries(out, [])
        return out

    def _read_snapshot(self, slot: int) -> dict[str, Any] | None:
        path = self._slot_file(slot)
        if not path.exists():
            return None

        snapshot: dict[str, Any] = {
            "slot": slot,
            "path": str(path),
            "timestamp": "",
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
        snapshot["timestamp"] = meta.get("timestamp", "")
        snapshot["actor"] = meta.get("actor", "")
        snapshot["message"] = meta.get("message", "")
        snapshot["entries"] = entries
        snapshot["queue"] = queue_entries_to_items(entries)

        modified = dt.datetime.fromtimestamp(path.stat().st_mtime)
        if not snapshot["timestamp"]:
            snapshot["timestamp"] = modified.isoformat(timespec="seconds")
        try:
            sort_key = dt.datetime.fromisoformat(str(snapshot["timestamp"]))
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
    ) -> None:
        self._ws_hub = ws_hub
        self._queue_archive = queue_archive
        self._logger = logger
        self._lock = threading.Lock()
        self._persons: list[str] = []
        self._admins: list[str] = []
        self._ban_admins: list[str] = []
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
                self._ban_admins = [str(x) for x in myjs_cfg["ban_admins"] if x]
            if isinstance(myjs_cfg.get("jianzhang"), list):
                self._jianzhang = [str(x) for x in myjs_cfg["jianzhang"] if x]
            if myjs_cfg.get("paidui_list_length_max") is not None:
                self._max_length = max(1, _to_int(myjs_cfg["paidui_list_length_max"], 100))
            if isinstance(myjs_cfg.get("fangguan_can_doing"), bool):
                self._fangguan_can_doing = myjs_cfg["fangguan_can_doing"]
            if isinstance(myjs_cfg.get("jianzhangchadui"), bool):
                self._jianzhangchadui = myjs_cfg["jianzhangchadui"]
            if anchor_uid > 0:
                self._anchor_uid = anchor_uid

    def load_quanxian(self, quanxian: dict[str, Any]) -> None:
        with self._lock:
            self._super_admins = [str(x) for x in quanxian.get("super_admin", []) if x]
            self._admins = [str(x) for x in quanxian.get("admin", []) if x]
            self._jianzhang = [str(x) for x in quanxian.get("jianzhang", []) if x]

    def load_kaiguan(self, kaiguan: dict[str, bool]) -> None:
        with self._lock:
            self._kaiguan = {**DEFAULT_KAIGUAN, **{k: v for k, v in kaiguan.items() if isinstance(v, bool)}}
            self._jianzhangchadui = self._kaiguan.get("jianzhang_chadui", False)
            self._fangguan_can_doing = self._kaiguan.get("fangguan_op", False)

    def restore_from_archive(self) -> None:
        slot = self._queue_archive.get_active_slot()
        snapshot = self._queue_archive.read_snapshot_by_slot(slot)
        if snapshot is None:
            return
        with self._lock:
            self._persons = [self._strip_html(item) for item in snapshot.get("queue", []) if item]
        self._logger.info(
            "[队列] 已从存档恢复 %s 人（槽位 %s，存档时间：%s）",
            len(self._persons), snapshot.get("slot", "?"), snapshot.get("timestamp", "?"),
        )

    @staticmethod
    def _strip_html(text: str) -> str:
        cleaned = re.sub(r"<[^>]*>", "", str(text))
        cleaned = re.sub(r"⏳待确认|等待确认", "", cleaned)
        return cleaned.strip()

    def get_queue(self) -> list[str]:
        with self._lock:
            return list(self._persons)

    def get_queue_entries(self) -> list[dict[str, str]]:
        return queue_items_to_entries(self.get_queue())

    def send_current_to(self, conn: socket.socket) -> None:
        try:
            _ws_send_text(
                conn,
                json.dumps({"type": "QUEUE_UPDATE", "queue": self.get_queue()}, ensure_ascii=False),
            )
        except OSError:
            pass

    def delete_item(self, index: int) -> list[str]:
        """删除第 index 项（1-based），越界静默忽略。"""
        with self._lock:
            if 1 <= index <= len(self._persons):
                self._persons.pop(index - 1)
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
            elif direction == "down" and 1 <= index <= n - 1:
                self._persons[index - 1], self._persons[index] = (
                    self._persons[index],
                    self._persons[index - 1],
                )
        self._broadcast_and_archive("gui", f"move_{direction}_{index}")
        return self.get_queue()

    def insert_item(self, after_index: int, entry: str) -> list[str]:
        """在 after_index 之后插入 entry（after_index=0 插到最前面）。"""
        entry = entry.strip()
        if not entry:
            return self.get_queue()
        with self._lock:
            pos = max(0, min(after_index, len(self._persons)))
            self._persons.insert(pos, entry)
        self._broadcast_and_archive("gui", f"insert_{after_index}")
        return self.get_queue()

    def update_item_content(self, index: int, content: str) -> list[str]:
        """按序号修改排队内容，保留原条目的 id。"""
        with self._lock:
            if 1 <= index <= len(self._persons):
                item_id, _old_content = queue_item_to_parts(self._persons[index - 1])
                self._persons[index - 1] = queue_parts_to_item(item_id, content)
        self._broadcast_and_archive("gui", f"update_{index}")
        return self.get_queue()

    def clear_queue(self) -> list[str]:
        """清空队列。"""
        with self._lock:
            previous_queue = list(self._persons)
            self._persons.clear()

        if previous_queue:
            self._logger.info(
                "[队列] 一键清空前原始队列（%s 人）: %s",
                len(previous_queue),
                json.dumps(previous_queue, ensure_ascii=False),
            )
        else:
            self._logger.info("[队列] 一键清空时原始队列为空")

        self._ws_hub.broadcast_json(None, {"type": "QUEUE_UPDATE", "queue": []})
        self._queue_archive.write_blank_snapshot("gui", "clear")
        self._logger.info("[队列] 当前槽位 %s 已恢复为空白存档", self._queue_archive.get_active_slot())
        return []

    def switch_to_slot(self, slot: int) -> list[str]:
        """Load queue from a specific archive slot, update in-memory queue, and broadcast."""
        self._queue_archive.set_active_slot(slot)
        snapshot = self._queue_archive.read_snapshot_by_slot(slot)
        if snapshot is None:
            self._logger.info("[队列] 切换到槽位 %s：槽位文件不存在，保持当前队列不变", slot)
            return self.get_queue()
        items = [self._strip_html(item) for item in snapshot.get("queue", []) if item]
        with self._lock:
            self._persons = items
        self._logger.info(
            "[队列] 已切换到槽位 %s，加载 %s 人（存档时间：%s）",
            slot, len(items), snapshot.get("timestamp", "?"),
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
        queue_snapshot = self.get_queue()
        self._ws_hub.broadcast_json(None, {"type": "QUEUE_UPDATE", "queue": queue_snapshot})
        self._queue_archive.write_snapshot(actor, msg, queue_snapshot)

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
        perm = "主播" if is_anchor else ("super_admin" if uname in self._super_admins else ("管理员" if uname in self._admins or (is_admin_flag and self._fangguan_can_doing) else ("舰长" if is_guard else "普通用户")))
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

            if uname in self._ban_admins:
                return False, None

            if self._all_disabled and not has_op:
                if self._is_join_cmd(msg):
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

                kg = self._kaiguan
                if msg == "排队" and kg.get("paidui", True):
                    new_item = uname
                elif msg in ("官服排", "排官服", "官服排队", "排队官服") and kg.get("guanfu_paidui", True):
                    new_item = f"官|{uname}"
                elif msg in ("B服排", "b服排", "排b服", "排B服", "B服排队", "排队B服", "b服排队", "排队b服") and kg.get("bfu_paidui", True):
                    new_item = f"B|{uname}"
                elif msg in ("超级排", "超级排队") and kg.get("chaoji_paidui", True):
                    new_item = f"<{uname}>"
                elif msg in ("小米排", "排小米", "排米服") and kg.get("mifu_paidui", True):
                    new_item = f"米|{uname}"
                elif msg.startswith("排队 ") and kg.get("paidui", True):
                    extra = msg[3:].strip()
                    new_item = f"{uname} {extra}" if extra else uname
                elif (re.match(r"^官服排队?\s", msg) or re.match(r"^官服排\s", msg)) and kg.get("guanfu_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    new_item = f"官|{uname} {extra}".rstrip()
                elif re.match(r"^[Bb]服排\s", msg) and kg.get("bfu_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    new_item = f"B|{uname} {extra}".rstrip()
                elif re.match(r"^超级排队?\s", msg) and kg.get("chaoji_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    new_item = f"<{uname}>{extra}" if extra else f"<{uname}>"
                elif msg.startswith("米服排 ") and kg.get("mifu_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    new_item = f"M|{uname} {extra}".rstrip()
                elif msg == "插队" and is_jianzhang and kg.get("jianzhang_chadui", False):
                    if len(self._persons) == 0 or not self._jianzhangchadui:
                        self._persons.append(uname)
                    else:
                        insert_pos = len(self._persons)
                        while insert_pos > 0 and any(
                            j in self._persons[insert_pos - 1] for j in self._jianzhang
                        ):
                            insert_pos -= 1
                        self._persons.insert(insert_pos, uname)
                    modified = True

                if new_item is not None and can_join:
                    self._persons.append(new_item)
                    modified = True
                elif new_item is not None and not can_join and not has_op:
                    return False, "队列已满，无法加入排队"

            # --- 已在队列时再次发排队指令 ---
            if index >= 0 and self._is_join_cmd(msg) and not has_op:
                return False, f"已在队列第 {index + 1} 位，无法重复排队"

            # --- Self-service cancel/replace (already in queue) ---
            if index >= 0:
                if msg in ("取消排队", "排队取消", "我确认我取消排队") and self._kaiguan.get("quxiao_paidui", True):
                    self._persons.pop(index)
                    modified = True
                elif msg in ("替换", "修改", "内容洗白") and self._kaiguan.get("xiugai_paidui", True):
                    self._persons[index] = uname
                    modified = True
                elif (msg.startswith("替换 ") or msg.startswith("修改 ")) and self._kaiguan.get("xiugai_paidui", True):
                    extra = msg.split(" ", 1)[1].strip() if " " in msg else ""
                    self._persons[index] = f"{uname} {extra}".rstrip()
                    modified = True

            # --- Operator/admin commands ---
            if not has_op and any(
                msg.startswith(p) for p in ("del ", "删除 ", "完成 ", "add ", "新增 ", "添加 ", "无影插 ", "插队 ")
            ):
                return False, "权限不足，该指令需要管理员权限"

            if has_op:
                for kw in ("del", "删除", "完成"):
                    if kw in msg:
                        nums = re.sub(r"[^0-9]", "", msg)
                        kw_only = re.sub(r"[\d\s]+", "", msg)
                        if kw_only == kw and nums:
                            n = int(nums)
                            if 1 <= n <= len(self._persons):
                                self._persons.pop(n - 1)
                                modified = True
                        break

                for prefix in ("add ", "新增 ", "添加 "):
                    if msg.startswith(prefix):
                        text = msg[len(prefix):].strip()
                        if text:
                            self._persons.append(text)
                            modified = True
                        break

                m = re.match(r"^无影插\s+(\d+)\s+(.+)", msg)
                if m:
                    pos, text = int(m.group(1)), m.group(2).strip()
                    if text and 1 <= pos <= 20:
                        self._persons.insert(pos - 1, text)
                        modified = True

                m2 = re.match(r"^插队\s+(\d+)\s+(.+)", msg)
                if m2:
                    pos, text = int(m2.group(1)), m2.group(2).strip()
                    if text and 1 <= pos <= 30:
                        self._persons.insert(pos - 1, f"@{text}")
                        modified = True

                if msg == "暂停排队功能" or msg == "关闭自助排队":
                    self._all_disabled = True
                elif msg == "恢复排队功能" or msg == "恢复自助排队":
                    self._all_disabled = False
                elif msg == "开启舰长插队":
                    self._jianzhangchadui = True
                elif msg == "关闭舰长插队":
                    self._jianzhangchadui = False
                elif msg == "允许房管成为插件管理员":
                    self._fangguan_can_doing = True
                elif msg == "停止房管成为插件管理员":
                    self._fangguan_can_doing = False

                if any(kw in msg for kw in ("设置排队人数", "设置排队上限")):
                    nums = re.sub(r"[^0-9]", "", msg)
                    kw_only = re.sub(r"[\d\s]+", "", msg)
                    if kw_only in ("设置排队人数", "设置排队上限", "设置排队人数上限") and nums:
                        self._max_length = max(1, int(nums))

                if msg.startswith("拉黑 "):
                    target = msg[3:].strip()
                    if target and target not in self._ban_admins:
                        self._ban_admins.append(target)
                elif msg.startswith("取消拉黑 "):
                    target = msg[5:].strip()
                    if target in self._ban_admins:
                        self._ban_admins.remove(target)

                if self._has_super_admin(uname, is_anchor):
                    if msg.startswith("添加管理员 "):
                        target = msg[6:].strip()
                        if target and target not in self._admins:
                            self._admins.append(target)
                    elif msg.startswith("取消管理员 "):
                        target = msg[6:].strip()
                        if target in self._admins:
                            self._admins.remove(target)

            return modified, None


DEFAULT_QUANXIAN: dict[str, Any] = {
    "super_admin": ["一纸轻予梦"],
    "admin": [],
    "jianzhang": [],
    "member": [],
}

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
            result[key] = [str(x) for x in config_section[key] if x]
        elif isinstance(raw_file.get(key), list):
            result[key] = [str(x) for x in raw_file[key] if x]
    return result


def save_quanxian(config: dict[str, Any]) -> None:
    normalized: dict[str, Any] = {k: list(v) for k, v in DEFAULT_QUANXIAN.items()}
    for key in DEFAULT_QUANXIAN:
        value = config.get(key, [])
        if isinstance(value, list):
            normalized[key] = [str(item).strip() for item in value if str(item).strip()]

    labels = {
        "super_admin": "最高管理员：拥有所有权限，包括新增/删除管理员",
        "admin": "管理员：拥有除新增/删除管理员以外的所有操作权限",
        "jianzhang": "舰长：仅拥有「插队」命令权限",
        "member": "成员：普通观众",
    }
    lines: list[str] = ["# 权限配置\n"]
    for key in ("super_admin", "admin", "jianzhang", "member"):
        lines.append(f"# {labels[key]}\n{key}:\n")
        for item in normalized.get(key, []):
            escaped = str(item).replace('"', '\\"')
            lines.append(f'  - "{escaped}"\n')
        lines.append("\n")
    QUANXIAN_PATH.write_text("".join(lines), encoding="utf-8")

    current = _merge_config(DEFAULT_CONFIG, _read_raw_config())
    current["quanxian"] = normalized
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


def save_kaiguan(config: dict[str, bool]) -> None:
    normalized: dict[str, bool] = dict(DEFAULT_KAIGUAN)
    for key in DEFAULT_KAIGUAN:
        if isinstance(config.get(key), bool):
            normalized[key] = bool(config.get(key))

    comments = {
        "paidui": "普通排队（排队 / 排队 xxx）",
        "guanfu_paidui": "官服排队",
        "bfu_paidui": "B服排队",
        "chaoji_paidui": "超级排队",
        "mifu_paidui": "米服排队",
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

    current = _merge_config(DEFAULT_CONFIG, _read_raw_config())
    current["kaiguan"] = normalized
    save_config(current)


DEFAULT_STYLE: dict[str, Any] = {
    "bg1": "#0e2036",
    "bg2": "#060b14",
    "bg3": "#020409",
    "text_color": "#eaf6ff",
    "queue_font_size": 50,
    "text_grad_start": "#f7f7f7",
    "text_grad_end": "rgba(255,255,255,0.6)",
    "text_stroke_color": "#000000",
}

DEFAULT_CONFIG["quanxian"] = {key: list(values) for key, values in DEFAULT_QUANXIAN.items()}
DEFAULT_CONFIG["kaiguan"] = dict(DEFAULT_KAIGUAN)
DEFAULT_CONFIG["style"] = dict(DEFAULT_STYLE)


def load_style() -> dict[str, Any]:
    raw_config = _read_raw_config()
    config_section = raw_config.get("style", {})
    if isinstance(config_section, dict) and config_section:
        result = dict(DEFAULT_STYLE)
        result.update(config_section)
        return result

    if STYLE_PATH.exists():
        try:
            data = json.loads(STYLE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                result = dict(DEFAULT_STYLE)
                result.update(data)
                return result
        except (json.JSONDecodeError, OSError):
            pass
    return dict(DEFAULT_STYLE)


def save_style(data: dict[str, Any]) -> None:
    merged = dict(DEFAULT_STYLE)
    merged.update(data)
    STYLE_PATH.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    current = _merge_config(DEFAULT_CONFIG, _read_raw_config())
    current["style"] = merged
    save_config(current)


def load_model() -> dict[str, Any]:
    with MODEL_JSON_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _extract_cookie_string(set_cookie_headers: list[str]) -> str:
    cookie_pairs: list[str] = []
    for header in set_cookie_headers:
        first_part = header.split(";", 1)[0].strip()
        if "=" not in first_part:
            continue
        cookie_pairs.append(first_part)
    return "; ".join(cookie_pairs)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _parse_cookie_pairs(cookie_text: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for item in str(cookie_text or "").split(";"):
        part = item.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if not key:
            continue
        pairs[key] = value.strip()
    return pairs


def _extract_uid_from_cookie(cookie_text: str) -> int:
    return _to_int(_parse_cookie_pairs(cookie_text).get("DedeUserID", 0))


def _is_plausible_bilibili_uid(uid: int) -> bool:
    return 0 < uid <= MAX_SAFE_INTEGER


def _build_bilibili_www_headers(cookie: str = "") -> dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.bilibili.com/",
        "Origin": "https://www.bilibili.com",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _build_bilibili_live_headers(roomid: int = 0, cookie: str = "") -> dict[str, str]:
    referer = "https://live.bilibili.com/"
    if roomid > 0:
        referer = f"https://live.bilibili.com/{roomid}"

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": referer,
        "Origin": "https://live.bilibili.com",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _bilibili_qr_generate() -> dict[str, Any]:
    req = urllib.request.Request(
        BILIBILI_QR_GENERATE_URL,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bilibili.com/",
            "Origin": "https://www.bilibili.com",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    return payload


def _bilibili_qr_poll(qrcode_key: str) -> tuple[dict[str, Any], str]:
    query = urllib.parse.urlencode({"qrcode_key": qrcode_key})
    req = urllib.request.Request(
        f"{BILIBILI_QR_POLL_URL}?{query}",
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bilibili.com/",
            "Origin": "https://www.bilibili.com",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
        raw_cookie_headers = resp.headers.get_all("Set-Cookie") or []
    return payload, _extract_cookie_string(raw_cookie_headers)


def _bilibili_get_nav_info(cookie: str) -> dict[str, Any]:
    req = urllib.request.Request(BILIBILI_NAV_URL, headers=_build_bilibili_www_headers(cookie))
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _resolve_bilibili_login(
    cookie: str,
    fallback_uid: int = 0,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    resolved_uid = max(0, _to_int(fallback_uid))
    if not _is_plausible_bilibili_uid(resolved_uid):
        resolved_uid = 0
    resolved = {
        "uid": resolved_uid,
        "uname": "",
        "is_login": False,
        "uid_source": "config" if resolved_uid > 0 else "",
    }

    cookie_uid = _extract_uid_from_cookie(cookie)
    if _is_plausible_bilibili_uid(cookie_uid):
        resolved["uid"] = cookie_uid
        resolved["uid_source"] = "cookie"
    elif cookie_uid > 0 and logger is not None:
        logger.warning("Ignoring implausible DedeUserID from cookie: %s", cookie_uid)

    if not str(cookie or "").strip():
        return resolved

    try:
        payload = _bilibili_get_nav_info(cookie)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        if logger is not None:
            logger.warning("Bilibili nav 查询失败，将回退到本地 Cookie/UID: %s", exc)
        return resolved

    if _to_int(payload.get("code", -1), -1) != 0:
        if logger is not None:
            logger.warning("Bilibili nav 返回异常，将回退到本地 Cookie/UID: %s", payload)
        return resolved

    data = payload.get("data", {})
    if not isinstance(data, dict):
        return resolved

    nav_uid = _to_int(data.get("mid", 0))
    if _is_plausible_bilibili_uid(nav_uid):
        resolved["uid"] = nav_uid
        resolved["uid_source"] = "nav"
    elif nav_uid > 0 and logger is not None:
        logger.warning("Ignoring implausible Bilibili nav uid: %s", nav_uid)
    resolved["uname"] = str(data.get("uname", "") or "")
    resolved["is_login"] = bool(data.get("isLogin", False))
    return resolved


def _build_qr_png_base64(text: str) -> tuple[str, str]:
    if not text:
        return "", "二维码内容为空"
    if qrcode is None:
        return "", "缺少依赖 qrcode，请先安装：pip install qrcode[pil]"

    qr = qrcode.QRCode(version=1, box_size=10, border=2)
    qr.add_data(text)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")

    buf = io.BytesIO()
    img.save(buf, format="PNG")
    encoded = base64.b64encode(buf.getvalue()).decode("ascii")
    return encoded, ""


def _bilibili_get_danmu_info(roomid: int, cookie: str = "") -> dict[str, Any]:
    query = urllib.parse.urlencode({"id": roomid, "type": 0})
    req = urllib.request.Request(
        f"{BILIBILI_DANMU_INFO_URL}?{query}",
        headers=_build_bilibili_live_headers(roomid, cookie),
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _bilibili_room_init(roomid: int, cookie: str = "") -> dict[str, Any]:
    query = urllib.parse.urlencode({"id": roomid})
    req = urllib.request.Request(
        f"{BILIBILI_ROOM_INIT_URL}?{query}",
        headers=_build_bilibili_live_headers(roomid, cookie),
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _bilibili_get_danmu_conf(roomid: int, cookie: str = "") -> dict[str, Any]:
    query = urllib.parse.urlencode({"room_id": roomid, "platform": "pc", "player": "web"})
    req = urllib.request.Request(
        f"{BILIBILI_DANMU_CONF_URL}?{query}",
        headers=_build_bilibili_live_headers(roomid, cookie),
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _bilibili_get_danmu_server_info(roomid: int, cookie: str = "") -> dict[str, Any]:
    payload = _bilibili_get_danmu_conf(roomid, cookie)
    if _to_int(payload.get("code", -1), -1) == 0:
        return payload

    legacy_payload = _bilibili_get_danmu_info(roomid, cookie)
    if _to_int(legacy_payload.get("code", -1), -1) != 0:
        return payload

    legacy_data = legacy_payload.get("data", {}) if isinstance(legacy_payload, dict) else {}
    if not isinstance(legacy_data, dict):
        return payload

    normalized_data = dict(legacy_data)
    if "host_server_list" not in normalized_data and isinstance(normalized_data.get("host_list"), list):
        normalized_data["host_server_list"] = normalized_data.get("host_list", [])
    return {"code": 0, "msg": "ok", "message": "ok", "data": normalized_data}


class BilibiliDanmuRelay(threading.Thread):
    def __init__(self, server: BackendServer) -> None:
        super().__init__(name="bilibili-danmu-relay", daemon=True)
        self.server = server
        self.logger = server.logger
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        self._seen_event_cmds: set[str] = set()
        self._status_lock = threading.Lock()
        self._connected = False
        self._last_packet_monotonic = 0.0
        self._last_packet_at = ""
        self._last_connect_at = ""
        self._last_disconnect_at = ""
        self._last_disconnect_reason = ""
        self._current_roomid = 0
        self._current_host = ""
        self._current_port = 0
        self._current_transport = ""
        self._current_auth_uid = 0

    def stop(self) -> None:
        self._stop_event.set()
        self._reconnect_event.set()

    def request_reconnect(self) -> None:
        self._reconnect_event.set()

    def _emit_status(self, status: str, **extra: Any) -> None:
        payload = {"type": "PDJ_STATUS", "status": status}
        payload.update(extra)
        self.server.ws_hub.broadcast_json(None, payload)

    def _pack_packet(self, body: bytes, operation: int, version: int = 1) -> bytes:
        header_len = 16
        packet_len = header_len + len(body)
        return struct.pack("!IHHII", packet_len, header_len, version, operation, 1) + body

    def _send_auth(self, conn: socket.socket, roomid: int, uid: int, token: str) -> None:
        protover = 3 if brotli is not None else 2
        auth_payload = {
            "uid": uid,
            "roomid": roomid,
            "protover": protover,
            "platform": "web",
            "type": 2,
            "key": token,
        }
        body = json.dumps(auth_payload, ensure_ascii=False).encode("utf-8")
        conn.sendall(self._pack_packet(body, operation=7, version=1))

    def _send_heartbeat(self, conn: socket.socket) -> None:
        conn.sendall(self._pack_packet(b"[object Object]", operation=2, version=1))

    def _mark_packet(self) -> None:
        with self._status_lock:
            self._last_packet_monotonic = time.monotonic()
            self._last_packet_at = dt.datetime.now(dt.timezone.utc).isoformat()

    def _mark_connected(
        self,
        *,
        roomid: int,
        host: str,
        port: int,
        transport: str,
        auth_uid: int,
    ) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with self._status_lock:
            self._connected = True
            self._last_connect_at = now
            self._last_disconnect_reason = ""
            self._current_roomid = roomid
            self._current_host = host
            self._current_port = port
            self._current_transport = transport
            self._current_auth_uid = auth_uid
        self._mark_packet()

    def _mark_disconnected(self, reason: str = "") -> None:
        with self._status_lock:
            self._connected = False
            self._last_disconnect_at = dt.datetime.now(dt.timezone.utc).isoformat()
            if reason:
                self._last_disconnect_reason = reason

    def get_runtime_status(self) -> dict[str, Any]:
        with self._status_lock:
            connected = self._connected
            last_packet_monotonic = self._last_packet_monotonic
            last_packet_at = self._last_packet_at
            last_connect_at = self._last_connect_at
            last_disconnect_at = self._last_disconnect_at
            last_disconnect_reason = self._last_disconnect_reason
            roomid = self._current_roomid
            host = self._current_host
            port = self._current_port
            transport = self._current_transport
            auth_uid = self._current_auth_uid
        return {
            "connected": connected,
            "last_packet_at": last_packet_at,
            "last_connect_at": last_connect_at,
            "last_disconnect_at": last_disconnect_at,
            "last_disconnect_reason": last_disconnect_reason,
            "idle_seconds": max(0.0, time.monotonic() - last_packet_monotonic) if last_packet_monotonic else None,
            "roomid": roomid,
            "host": host,
            "port": port,
            "transport": transport,
            "auth_uid": auth_uid,
        }

    def _log_business_message(self, text: str) -> None:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return

        if not isinstance(payload, dict):
            return

        cmd = str(payload.get("cmd", "") or "").strip()
        if not cmd:
            return

        if cmd.startswith("DANMU_MSG"):
            # 弹幕由 QueueManager.process_danmu_json 负责打印，此处跳过
            return

        if cmd in self._seen_event_cmds:
            return
        self._seen_event_cmds.add(cmd)

        data = payload.get("data", {})
        if isinstance(data, dict) and isinstance(data.get("pb"), str):
            self.logger.info("收到直播间事件 cmd=%s（protobuf 格式）", cmd)
            return

        self.logger.info("收到直播间事件 cmd=%s", cmd)

    def _iter_business_messages(self, packet_data: bytes) -> list[str]:
        messages: list[str] = []
        offset = 0
        total = len(packet_data)
        while offset + 16 <= total:
            packet_len, header_len, version, operation, _ = struct.unpack(
                "!IHHII", packet_data[offset : offset + 16]
            )
            if packet_len <= 0 or offset + packet_len > total:
                break
            body = packet_data[offset + header_len : offset + packet_len]
            offset += packet_len

            if operation != 5:
                continue
            if version == 0 or version == 1:
                text = body.decode("utf-8", errors="replace").strip()
                if text:
                    messages.append(text)
            elif version == 2:
                try:
                    messages.extend(self._iter_business_messages(zlib.decompress(body)))
                except zlib.error:
                    self.logger.debug("弹幕包 zlib 解压失败")
            elif version == 3:
                if brotli is None:
                    self.logger.debug("收到 brotli 包，但环境未安装 brotli")
                    continue
                try:
                    messages.extend(self._iter_business_messages(brotli.decompress(body)))
                except Exception:
                    self.logger.debug("弹幕包 brotli 解压失败")
        return messages

    def _wait_auth_result(self, conn: socket.socket) -> None:
        header = _ws_recv_exact(conn, 16)
        if not header:
            raise ConnectionError("danmu auth connection closed")

        packet_len, header_len, _, operation, _ = struct.unpack("!IHHII", header)
        if packet_len < header_len or header_len < 16:
            raise ConnectionError("invalid danmu auth packet")

        body = _ws_recv_exact(conn, packet_len - 16)
        if body is None:
            raise ConnectionError("empty danmu auth packet")

        if operation != 8:
            raise ConnectionError(f"unexpected danmu auth op={operation}")

        payload = json.loads(body.decode("utf-8", errors="replace") or "{}")
        if _to_int(payload.get("code", -1), -1) != 0:
            raise ConnectionError(f"danmu auth failed: {payload}")
        self.logger.info("直播间鉴权成功")
        self._emit_status("danmu_auth_ok")

    def _iter_auth_uid_candidates(self, auth_uid: int, configured_uid: int) -> list[int]:
        candidates: list[int] = []
        for value in [auth_uid, configured_uid, 0]:
            uid = _to_int(value, 0)
            if uid < 0:
                continue
            if uid != 0 and not _is_plausible_bilibili_uid(uid):
                continue
            if uid not in candidates:
                candidates.append(uid)
        return candidates or [0]

    def _normalize_host_candidates(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        seen: set[tuple[str, int, str]] = set()
        for key in ("server_list", "host_server_list", "host_list"):
            items = data.get(key, [])
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                host = str(item.get("host", "")).strip()
                if not host:
                    continue
                for transport_key, transport_name in (("port", "tcp"), ("wss_port", "tls"), ("ws_port", "tcp")):
                    port = _to_int(item.get(transport_key, 0), 0)
                    if port <= 0:
                        continue
                    dedupe_key = (host, port, transport_name)
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    candidates.append({"host": host, "port": port, "transport": transport_name})

        random.shuffle(candidates)
        candidates.sort(
            key=lambda item: (
                0 if item.get("transport") == "tcp" else 1,
                0 if _to_int(item.get("port", 0), 0) == 80 else 1,
                0 if _to_int(item.get("port", 0), 0) == 443 else 1,
            )
        )
        return candidates

    def _open_danmu_socket(self, host: str, port: int, transport: str) -> socket.socket:
        raw_conn = socket.create_connection((host, port), timeout=10)
        raw_conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if transport == "tls":
            context = ssl.create_default_context()
            return context.wrap_socket(raw_conn, server_hostname=host)
        return raw_conn

    def _connect_and_stream_v2(
        self,
        *,
        roomid: int,
        configured_uid: int,
        cookie: str,
        initial_auth_uid: int,
    ) -> None:
        self.logger.info("开始获取弹幕服务器信息，roomid=%s", roomid)
        real_room_id = roomid
        for candidate_room_id, candidate_cookie, mode in [
            (roomid, cookie, "room_init+cookie"),
            (roomid, "", "room_init+no_cookie"),
        ]:
            try:
                room_init_payload = _bilibili_room_init(candidate_room_id, candidate_cookie)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("room_init failed (%s): %s", mode, exc)
                continue
            room_init_data = room_init_payload.get("data", {}) if isinstance(room_init_payload, dict) else {}
            init_real_room_id = _to_int(room_init_data.get("room_id", candidate_room_id), candidate_room_id)
            if init_real_room_id > 0:
                real_room_id = init_real_room_id
                if real_room_id != roomid:
                    self.logger.info("room_init resolved real room id: %s -> %s", roomid, real_room_id)
                break

        if cookie:
            request_candidates: list[tuple[int, str, str]] = [(roomid, cookie, "id+cookie")]
            if real_room_id != roomid:
                request_candidates.append((real_room_id, cookie, "real_id+cookie"))
            request_candidates.append((roomid, "", "id+no_cookie"))
            if real_room_id != roomid:
                request_candidates.append((real_room_id, "", "real_id+no_cookie"))
        else:
            request_candidates = [(roomid, "", "id+no_cookie")]
            if real_room_id != roomid:
                request_candidates.append((real_room_id, "", "real_id+no_cookie"))

        payload: dict[str, Any] | None = None
        discovery_candidates: list[dict[str, Any]] = []

        for candidate_room_id, candidate_cookie, mode in request_candidates:
            try:
                payload = _bilibili_get_danmu_server_info(candidate_room_id, candidate_cookie)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("danmu server info request failed (%s): %s", mode, exc)
                continue

            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            code = _to_int(payload.get("code", -1), -1) if isinstance(payload, dict) else -1
            candidate_token = str(data.get("token", "")) if isinstance(data, dict) else ""
            candidate_hosts = self._normalize_host_candidates(data) if isinstance(data, dict) else []
            if code == 0 and candidate_token and candidate_hosts:
                discovery_candidates.append(
                    {
                        "token": candidate_token,
                        "host_candidates": candidate_hosts,
                        "room_id": _to_int(data.get("room_id", candidate_room_id), candidate_room_id),
                        "mode": mode,
                    }
                )
                continue
            self.logger.warning("danmu server info failed (%s): %s", mode, payload)

        if not discovery_candidates:
            raise RuntimeError(f"danmu server info failed: {payload}")

        auth_uid_candidates = self._iter_auth_uid_candidates(initial_auth_uid, configured_uid)
        conn: socket.socket | None = None
        host = ""
        port = 0
        transport = ""
        last_error: Exception | None = None
        selected_auth_uid = 0
        selected_room_id = real_room_id

        for discovery in discovery_candidates:
            token = str(discovery.get("token", "") or "")
            host_candidates = list(discovery.get("host_candidates", []))
            selected_room_id = _to_int(discovery.get("room_id", real_room_id), real_room_id)
            discovery_mode = str(discovery.get("mode", "") or "")

            if discovery_mode.endswith("no_cookie"):
                self.logger.warning("Danmu server discovery fell back to no-cookie mode")
            else:
                self.logger.info("Danmu server discovery using authenticated mode: %s", discovery_mode)

            for candidate in host_candidates:
                host = str(candidate.get("host", "")).strip()
                port = _to_int(candidate.get("port", 0), 0)
                transport = str(candidate.get("transport", "tcp") or "tcp")
                if not host or port <= 0:
                    continue

                self._emit_status(
                    "danmu_connecting",
                    roomid=selected_room_id,
                    host=host,
                    port=port,
                    transport=transport,
                )

                for candidate_auth_uid in auth_uid_candidates:
                    try:
                        self.logger.info(
                            "Trying danmu server %s://%s:%s with auth uid=%s discovery=%s",
                            transport,
                            host,
                            port,
                            candidate_auth_uid,
                            discovery_mode or "unknown",
                        )
                        conn = self._open_danmu_socket(host, port, transport)
                        conn.settimeout(5.0)
                        self._send_auth(conn, selected_room_id, candidate_auth_uid, token)
                        self._wait_auth_result(conn)
                        selected_auth_uid = candidate_auth_uid
                        conn.settimeout(1.0)
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_error = exc
                        self.logger.warning(
                            "danmu connect/auth failed %s://%s:%s uid=%s discovery=%s: %s",
                            transport,
                            host,
                            port,
                            candidate_auth_uid,
                            discovery_mode or "unknown",
                            exc,
                        )
                        if conn is not None:
                            try:
                                conn.close()
                            except OSError:
                                pass
                        conn = None
                if conn is not None:
                    break
            if conn is not None:
                break

        if conn is None:
            raise ConnectionError(f"all danmu server candidates failed: {last_error}")

        try:
            self._send_heartbeat(conn)
            self._mark_connected(
                roomid=selected_room_id,
                host=host,
                port=port,
                transport=transport,
                auth_uid=selected_auth_uid,
            )
            self._emit_status(
                "danmu_connected",
                roomid=selected_room_id,
                host=host,
                port=port,
                transport=transport,
                auth_uid=selected_auth_uid,
            )
            self.logger.info(
                "Danmu connected roomid=%s transport=%s host=%s port=%s auth_uid=%s",
                selected_room_id,
                transport,
                host,
                port,
                selected_auth_uid,
            )
            next_heartbeat = time.time() + DANMU_HEARTBEAT_INTERVAL_SECONDS

            while not self._stop_event.is_set():
                if self._reconnect_event.is_set():
                    self._reconnect_event.clear()
                    self.logger.info("Received reconnect signal; restarting danmu stream")
                    break
                if time.time() >= next_heartbeat:
                    self._send_heartbeat(conn)
                    next_heartbeat = time.time() + DANMU_HEARTBEAT_INTERVAL_SECONDS
                ok = self._recv_and_handle(conn)
                if not ok:
                    self._mark_disconnected("danmu stream disconnected")
                    raise ConnectionError("danmu stream disconnected")
                idle_seconds = self.get_runtime_status().get("idle_seconds")
                if idle_seconds is not None and idle_seconds >= DANMU_IDLE_RECONNECT_SECONDS:
                    self._mark_disconnected(
                        f"danmu stream idle timeout ({int(idle_seconds)}s without packets)"
                    )
                    raise ConnectionError(
                        f"danmu stream idle timeout ({int(idle_seconds)}s without packets)"
                    )
        finally:
            self._mark_disconnected("connection closed")
            try:
                conn.close()
            except OSError:
                pass

    def _recv_and_handle(self, conn: socket.socket) -> bool:
        try:
            header = _ws_recv_exact(conn, 16)
        except TimeoutError:
            return True
        if not header:
            return False
        packet_len, header_len, _, operation, _ = struct.unpack("!IHHII", header)
        if packet_len < header_len or header_len < 16:
            return False
        try:
            body = _ws_recv_exact(conn, packet_len - 16)
        except TimeoutError:
            return True
        if body is None:
            return False
        packet_data = header + body
        self._mark_packet()

        if operation == 8:
            self.logger.info("直播间弹幕鉴权成功")
            self._emit_status("danmu_auth_ok")
            return True
        if operation == 3 and len(body) >= 4:
            popularity = struct.unpack("!I", body[:4])[0]
            self.logger.info("实时人气值：%s", popularity)
            self._emit_status("popularity", popularity=popularity)
            return True

        for text in self._iter_business_messages(packet_data):
            self._log_business_message(text)
            self.server.ws_hub.mark_message()
            try:
                parsed_msg = json.loads(text)
            except json.JSONDecodeError:
                self.server.ws_hub.broadcast_text(None, text)
                continue
            cmd = str(parsed_msg.get("cmd", "") or "").strip() if isinstance(parsed_msg, dict) else ""
            if cmd.startswith("DANMU_MSG"):
                # Route through queue manager; QUEUE_UPDATE is broadcast on change
                if hasattr(self.server, "queue_manager"):
                    self.server.queue_manager.process_danmu_json(parsed_msg)
            else:
                self.server.ws_hub.broadcast_text(None, text)
        return True

    def _connect_and_stream(self) -> None:
        cfg = self.server.runtime_config.get("api", {})
        roomid = int(cfg.get("roomid", 0))
        uid = int(cfg.get("uid", 0))
        cookie = str(cfg.get("cookie", "")).strip()
        login_state = _resolve_bilibili_login(cookie, fallback_uid=uid, logger=self.logger if cookie else None)
        auth_uid = _to_int(login_state.get("uid", 0))
        if cookie and auth_uid > 0 and auth_uid != uid:
            cfg["uid"] = auth_uid
            self.logger.info(
                "Detected logged-in Bilibili UID=%s (source=%s); using it for danmu auth",
                auth_uid,
                login_state.get("uid_source", "unknown"),
            )
        elif cookie and auth_uid <= 0 and uid <= 0:
            self.logger.warning("Cookie is configured but login UID could not be resolved; falling back to guest/default UID")
        if roomid > 0:
            return self._connect_and_stream_v2(
                roomid=roomid,
                configured_uid=uid,
                cookie=cookie,
                initial_auth_uid=auth_uid,
            )
        if roomid <= 0:
            self.logger.info("直播间未配置（roomid=0），跳过弹幕连接")
            self._emit_status("danmu_waiting_config", message="roomid 未配置")
            time.sleep(3)
            return

        self.logger.info("开始获取直播间弹幕 ws 地址，roomid=%s", roomid)
        real_room_id = roomid
        try:
            room_init_payload = _bilibili_room_init(roomid, cookie)
            room_init_data = room_init_payload.get("data", {}) if isinstance(room_init_payload, dict) else {}
            init_real_room_id = int(room_init_data.get("room_id", roomid) or roomid)
            if init_real_room_id > 0:
                real_room_id = init_real_room_id
                if real_room_id != roomid:
                    self.logger.info("room_init 已解析真实房间号：%s -> %s", roomid, real_room_id)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("room_init 查询失败，将使用原房间号继续: %s", exc)

        request_candidates: list[tuple[int, str, str]] = [
            (roomid, cookie, "id+cookie"),
        ]
        if real_room_id != roomid:
            request_candidates.append((real_room_id, cookie, "real_id+cookie"))
        if cookie:
            request_candidates.append((roomid, "", "id+no_cookie"))
            if real_room_id != roomid:
                request_candidates.append((real_room_id, "", "real_id+no_cookie"))

        payload: dict[str, Any] | None = None
        token = ""
        host_list: list[dict[str, Any]] = []
        selected_room_id = real_room_id

        for candidate_room_id, candidate_cookie, mode in request_candidates:
            payload = _bilibili_get_danmu_info(candidate_room_id, candidate_cookie)
            code = int(payload.get("code", -1)) if isinstance(payload, dict) else -1
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            candidate_token = str(data.get("token", ""))
            candidate_hosts = data.get("host_list", [])
            if code == 0 and candidate_token and isinstance(candidate_hosts, list) and candidate_hosts:
                token = candidate_token
                host_list = candidate_hosts
                selected_room_id = int(data.get("room_id", candidate_room_id) or candidate_room_id)
                if mode.endswith("no_cookie"):
                    self.logger.warning("getDanmuInfo 在带 Cookie 模式失败，已回退到无 Cookie 模式")
                break
            self.logger.warning("getDanmuInfo 失败(%s): %s", mode, payload)

        if not token or not host_list:
            raise RuntimeError(f"getDanmuInfo 返回异常: {payload}")

        candidate = random.choice(host_list)
        host = str(candidate.get("host", "")).strip()
        wss_port = int(candidate.get("wss_port", 443) or 443)
        if not host:
            raise RuntimeError(f"getDanmuInfo host 为空: {payload}")

        self.logger.info("直播间弹幕服务地址：wss://%s:%s/sub", host, wss_port)
        self._emit_status("danmu_connecting", roomid=selected_room_id, host=host, port=wss_port)

        raw_conn = socket.create_connection((host, wss_port), timeout=10)
        context = ssl.create_default_context()
        conn = context.wrap_socket(raw_conn, server_hostname=host)
        conn.settimeout(1.0)
        try:
            self._send_auth(conn, selected_room_id, auth_uid, token)
            self._send_heartbeat(conn)
            self._emit_status("danmu_connected", roomid=selected_room_id, host=host)
            self.logger.info("已连接直播间弹幕流，roomid=%s", selected_room_id)
            next_heartbeat = time.time() + 30

            while not self._stop_event.is_set():
                if self._reconnect_event.is_set():
                    self._reconnect_event.clear()
                    self.logger.info("收到重连信号，准备重新连接直播间弹幕流")
                    break
                if time.time() >= next_heartbeat:
                    self._send_heartbeat(conn)
                    next_heartbeat = time.time() + 30
                ok = self._recv_and_handle(conn)
                if not ok:
                    raise ConnectionError("直播间弹幕连接中断")
        finally:
            try:
                conn.close()
            except OSError:
                pass

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._connect_and_stream()
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("直播间弹幕连接异常：%s", exc)
                self._emit_status("danmu_disconnected", error=str(exc))
                time.sleep(2)


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

    target = (UI_DIR / path.lstrip("/")).resolve()
    try:
        target.relative_to(UI_DIR.resolve())
    except ValueError:
        return None

    if target.is_file():
        return target
    return None


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
        body = file_path.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", _guess_content_type(file_path))
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
            except FileNotFoundError:
                self._write_json(
                    {
                        "status": "error",
                        "message": f"Model file not found: {MODEL_JSON_PATH}",
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

        if parsed.path == "/api/config":
            cfg = self.server.runtime_config
            self._write_json(
                {
                    "roomid": int(cfg.get("api", {}).get("roomid", 0)),
                    "uid": int(cfg.get("api", {}).get("uid", 0)),
                    "cookie": str(cfg.get("api", {}).get("cookie", "")),
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
            current = self.server.queue_manager.get_queue() if hasattr(self.server, "queue_manager") else []
            self._write_json(
                {
                    "status": "ok",
                    "queue": current,
                    "entries": queue_items_to_entries(current),
                    "size": len(current),
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
                payload = _bilibili_qr_generate()
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
                qr_base64, qr_error = _build_qr_png_base64(qr_url)
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
            login_state = _resolve_bilibili_login(cookie, fallback_uid=uid, logger=self.server.logger if cookie else None)
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
                    "api": {"roomid": roomid, "uid": uid, "cookie": cookie},
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
            self.server.runtime_config = updated
            if hasattr(self.server, "danmu_relay"):
                self.server.danmu_relay.request_reconnect()
            if hasattr(self.server, "queue_manager"):
                self.server.queue_manager.load_config(
                    updated.get("myjs", {}),
                    anchor_uid=_to_int(updated.get("api", {}).get("uid", 0)),
                )
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
                }
            )
            return

        if parsed.path == "/api/queue/reload":
            if hasattr(self.server, "queue_manager"):
                self.server.queue_manager.restore_from_archive()
                current = self.server.queue_manager.get_queue()
                self.server.queue_manager._broadcast_and_archive("gui", "reload")
            else:
                current = []
            self._write_json(
                {
                    "status": "ok",
                    "queue": current,
                    "entries": queue_items_to_entries(current),
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
            self._write_json(
                {
                    "status": "ok",
                    "queue": current,
                    "entries": queue_items_to_entries(current),
                    "size": len(current),
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
            else:
                current = []
            self._write_json({"status": "ok", "slot": slot, "queue": current, "size": len(current)})
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
            self.server.runtime_config = _merge_config(self.server.runtime_config, {"quanxian": load_quanxian()})
            if hasattr(self.server, "queue_manager"):
                self.server.queue_manager.load_quanxian(payload)
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
            self.server.runtime_config = _merge_config(self.server.runtime_config, {"kaiguan": load_kaiguan()})
            if hasattr(self.server, "queue_manager"):
                self.server.queue_manager.load_kaiguan(payload)
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
                bilibili_payload, cookie_text = _bilibili_qr_poll(qrcode_key)
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
                    login_state = _resolve_bilibili_login(
                        cookie_text,
                        fallback_uid=int(self.server.runtime_config.get("api", {}).get("uid", 0)),
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
                    api_update: dict[str, Any] = {"cookie": cookie_text}
                    if resolved_uid > 0:
                        api_update["uid"] = resolved_uid
                    updated = _merge_config(
                        self.server.runtime_config,
                        {
                            "api": api_update,
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
                    self.server.runtime_config = updated
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
    httpd.ws_hub = WebSocketHub(logger)
    httpd.queue_manager = QueueManager(
        ws_hub=httpd.ws_hub,
        queue_archive=httpd.queue_archive,
        logger=logger,
    )
    httpd.queue_manager.load_config(
        runtime_config.get("myjs", {}),
        anchor_uid=_to_int(runtime_config.get("api", {}).get("uid", 0)),
    )
    httpd.queue_manager.load_quanxian(load_quanxian())
    httpd.queue_manager.load_kaiguan(load_kaiguan())
    httpd.queue_manager.restore_from_archive()
    httpd.danmu_relay = BilibiliDanmuRelay(httpd)
    httpd.danmu_relay.start()

    logger.info("后端已启动，地址：http://%s:%s", host, port)
    logger.info("配置页：http://127.0.0.1:%s/config", port)
    logger.info("排队展示页：http://127.0.0.1:%s/index", port)
    logger.info("WebSocket：ws://127.0.0.1:%s/ws（别名：/danmu/sub）", port)

    # 打印非敏感配置
    srv_cfg = runtime_config.get("server", {})
    api_cfg = runtime_config.get("api", {})
    log_cfg = runtime_config.get("logging", {})
    qa_cfg = runtime_config.get("queue_archive", {})
    logger.info(
        "[config] server=%s:%s  roomid=%s  uid=%s  log_level=%s  retention=%sd  "
        "archive_enabled=%s  active_slot=%s",
        srv_cfg.get("host", host), srv_cfg.get("port", port),
        api_cfg.get("roomid", 0),
        api_cfg.get("uid", 0),
        log_cfg.get("level", "INFO"),
        log_cfg.get("retention_days", 7),
        qa_cfg.get("enabled", True),
        cfg_active_slot,
    )
    try:
        httpd.serve_forever()
    finally:
        httpd.danmu_relay.stop()


if __name__ == "__main__":
    config = load_config()
    host = os.getenv("DANMUJI_BACKEND_HOST", str(config.get("server", {}).get("host", DEFAULT_HOST)))
    port = int(os.getenv("DANMUJI_BACKEND_PORT", int(config.get("server", {}).get("port", DEFAULT_PORT))))
    run_server(host=host, port=port)
