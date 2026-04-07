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
MAX_QUEUE_ARCHIVE_SLOTS = 5
DANMU_HEARTBEAT_INTERVAL_SECONDS = 30
DANMU_IDLE_RECONNECT_SECONDS = 90

REPO_DIR = Path(__file__).resolve().parent.parent
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", REPO_DIR))
APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else REPO_DIR

MODEL_JSON_PATH = BUNDLE_DIR / "models" / "danmuji_initial_model.json"
TOGUI_DIR = BUNDLE_DIR / "toGUI"
CONFIG_PATH = APP_DIR / "config.yaml"
LOG_DIR = APP_DIR / "log"
PD_DIR = APP_DIR / "pd"
QUEUE_STATE_PATH = PD_DIR / "queue_archive_state.json"

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
        self.logger.info("WebSocket client connected, total=%s", count)

    def unregister(self, conn: socket.socket) -> None:
        with self._lock:
            self._clients.discard(conn)
            count = len(self._clients)
        self.logger.info("WebSocket client disconnected, total=%s", count)

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
        value = value.strip()

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


DEFAULT_CONFIG: dict[str, Any] = {
    "server": {"host": DEFAULT_HOST, "port": DEFAULT_PORT},
    "api": {"roomid": 0, "uid": 0, "cookie": ""},
    "qr_login": {
        "last_success_at": "",
        "qrcode_key": "",
        "poll_code": -1,
        "message": "",
        "cookie": "",
    },
    "callback": {"enabled": False, "url": "", "auth_token": "", "timeout_seconds": 5},
    "myjs": {},
    "ui": {"startup_splash_seconds": 5},
    "logging": {"level": "INFO", "retention_days": 15},
    "queue_archive": {"enabled": True, "slots": 3},
}


def ensure_runtime_layout(config_slots: int = 3) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    PD_DIR.mkdir(parents=True, exist_ok=True)
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)

    slots = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(config_slots)))
    for slot in range(1, slots + 1):
        slot_file = PD_DIR / f"queue_archive_slot_{slot}.csv"
        if not slot_file.exists():
            slot_file.write_text("position,queue_item\n", encoding="utf-8-sig")


def load_config() -> dict[str, Any]:
    ensure_runtime_layout(int(DEFAULT_CONFIG.get("queue_archive", {}).get("slots", 3)))
    merged = _merge_config(DEFAULT_CONFIG, load_simple_yaml(CONFIG_PATH))
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

logging:
  # 支持 DEBUG / INFO / WARNING / ERROR / CRITICAL
  level: {str(logging_cfg.get('level', 'INFO')).upper()}
  # 每次启动默认清理多少天前日志
  retention_days: {int(logging_cfg.get('retention_days', 15))}

queue_archive:
  enabled: {'true' if bool(queue_archive.get('enabled', True)) else 'false'}
  # 存档位（1~5）
  slots: {min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(queue_archive.get('slots', 3))))}

callback:
  enabled: {'true' if bool(callback_cfg.get('enabled', False)) else 'false'}
  url: {callback_url}
  auth_token: {callback_auth_token}
  timeout_seconds: {max(1, int(callback_cfg.get('timeout_seconds', 5)))}
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
    retention_days = int(config.get("logging", {}).get("retention_days", 15))
    _cleanup_old_logs(retention_days)

    level_name = str(config.get("logging", {}).get("level", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)
    log_path = LOG_DIR / f"backend_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logger = logging.getLogger("danmuji.backend")
    logger.info("Logging initialized at %s", log_path)
    logger.info("Log cleanup retention_days=%s", retention_days)
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
        return {"slot": 0, "path": "", "timestamp": "", "actor": "", "message": "", "queue": []}

    def write_snapshot(self, actor: str, message: str, queue_items: list[str]) -> Path | None:
        if not self.enabled:
            return None

        state = self._read_state()
        slot = int(state.get("next_slot", 1))
        slot = ((slot - 1) % self.slots) + 1
        out = self._slot_file(slot)

        now = dt.datetime.now().isoformat(timespec="seconds")
        with out.open("w", encoding="utf-8-sig", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", now])
            writer.writerow(["actor", actor])
            writer.writerow(["message", message])
            writer.writerow([])
            writer.writerow(["position", "queue_item"])
            for idx, item in enumerate(queue_items, start=1):
                writer.writerow([idx, item])

        state["next_slot"] = (slot % self.slots) + 1
        self._write_state(state)
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
        }

        try:
            with path.open("r", encoding="utf-8-sig", newline="") as f:
                rows = list(csv.reader(f))
        except OSError:
            return None

        for row in rows:
            if not row:
                continue
            key = str(row[0]).strip()
            if key == "timestamp":
                snapshot["timestamp"] = str(row[1]).strip() if len(row) > 1 else ""
            elif key == "actor":
                snapshot["actor"] = str(row[1]).strip() if len(row) > 1 else ""
            elif key == "message":
                snapshot["message"] = str(row[1]).strip() if len(row) > 1 else ""
            elif key == "position":
                continue
            elif key.isdigit() and len(row) > 1:
                snapshot["queue"].append(str(row[1]))

        if not snapshot["timestamp"] and not snapshot["actor"] and not snapshot["message"] and not snapshot["queue"]:
            return None

        try:
            sort_key = dt.datetime.fromisoformat(str(snapshot["timestamp"]))
        except ValueError:
            sort_key = dt.datetime.fromtimestamp(path.stat().st_mtime)
        snapshot["_sort_key"] = sort_key.isoformat()
        return snapshot

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
            info = payload.get("info", [])
            message = ""
            uname = ""
            uid = 0
            if isinstance(info, list):
                if len(info) > 1 and isinstance(info[1], str):
                    message = info[1]
                if len(info) > 2 and isinstance(info[2], list):
                    user_info = info[2]
                    if len(user_info) > 0:
                        uid = _to_int(user_info[0], 0)
                    if len(user_info) > 1 and isinstance(user_info[1], str):
                        uname = user_info[1]
            self.logger.debug(
                "Danmu message received cmd=%s user_present=%s uid_present=%s msg_chars=%s",
                cmd,
                bool(uname),
                uid > 0,
                len(message),
            )
            return

        if cmd in self._seen_event_cmds:
            return
        self._seen_event_cmds.add(cmd)

        data = payload.get("data", {})
        if isinstance(data, dict) and isinstance(data.get("pb"), str):
            self.logger.info("Danmu event received cmd=%s (protobuf-wrapped)", cmd)
            return

        self.logger.info("Danmu event received cmd=%s", cmd)

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
        self.logger.info("Danmu auth succeeded")
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
        self.logger.info("Starting danmu server discovery, roomid=%s", roomid)
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

    target = (TOGUI_DIR / path.lstrip("/")).resolve()
    try:
        target.relative_to(TOGUI_DIR.resolve())
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
        path = urlparse(getattr(self, "path", "")).path
        message = "%s - %s" % (self.address_string(), format % args)
        if path in {
            "/api/runtime-status",
            "/favicon.ico",
            "/.well-known/appspecific/com.chrome.devtools.json",
        }:
            self.server.logger.debug(message)
            return
        self.server.logger.info(message)

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
                }
            )
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
    ensure_runtime_layout(int(runtime_config.get("queue_archive", {}).get("slots", 3)))
    logger = setup_logging(runtime_config)
    archive_cfg = runtime_config.get("queue_archive", {})

    httpd = BackendServer((host, port), ApiHandler)
    httpd.runtime_config = runtime_config
    httpd.logger = logger
    httpd.queue_archive = QueueArchiveManager(
        slots=int(archive_cfg.get("slots", 3)),
        enabled=bool(archive_cfg.get("enabled", True)),
    )
    httpd.ws_hub = WebSocketHub(logger)
    httpd.danmu_relay = BilibiliDanmuRelay(httpd)
    httpd.danmu_relay.start()

    logger.info("Danmuji backend started on http://%s:%s", host, port)
    logger.info("Backend config page: http://127.0.0.1:%s/config", port)
    logger.info("Index page: http://127.0.0.1:%s/index", port)
    logger.info("WebSocket: ws://127.0.0.1:%s/ws (alias: /danmu/sub)", port)
    try:
        httpd.serve_forever()
    finally:
        httpd.danmu_relay.stop()


if __name__ == "__main__":
    config = load_config()
    host = os.getenv("DANMUJI_BACKEND_HOST", str(config.get("server", {}).get("host", DEFAULT_HOST)))
    port = int(os.getenv("DANMUJI_BACKEND_PORT", int(config.get("server", {}).get("port", DEFAULT_PORT))))
    run_server(host=host, port=port)
