from __future__ import annotations

import base64
import csv
import datetime as dt
import hashlib
import io
import json
import logging
import os
import socket
import struct
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
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
BILIBILI_QR_GENERATE_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
BILIBILI_QR_POLL_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"


class BackendServer(ThreadingHTTPServer):
    daemon_threads = True
    runtime_config: dict[str, Any]
    logger: logging.Logger
    queue_archive: "QueueArchiveManager"
    ws_hub: "WebSocketHub"


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


def load_simple_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}

    root: dict[str, Any] = {}
    stack: list[tuple[int, dict[str, Any]]] = [(-1, root)]

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        if ":" not in stripped:
            continue

        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()

        while stack and indent <= stack[-1][0]:
            stack.pop()

        current = stack[-1][1] if stack else root
        if value == "":
            child: dict[str, Any] = {}
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

    slots = max(1, int(config_slots))
    for slot in range(1, slots + 1):
        slot_file = PD_DIR / f"queue_archive_slot_{slot}.csv"
        if not slot_file.exists():
            slot_file.write_text("position,queue_item\n", encoding="utf-8-sig")


def load_config() -> dict[str, Any]:
    ensure_runtime_layout(int(DEFAULT_CONFIG.get("queue_archive", {}).get("slots", 3)))
    return _merge_config(DEFAULT_CONFIG, load_simple_yaml(CONFIG_PATH))


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
            if isinstance(value, bool):
                myjs_lines.append(f"  {key}: {'true' if value else 'false'}")
            elif isinstance(value, (int, float)):
                myjs_lines.append(f"  {key}: {value}")
            elif value is None:
                myjs_lines.append(f"  {key}: null")
            else:
                text = str(value).replace("\\", "\\\\").replace('"', '\\"')
                myjs_lines.append(f'  {key}: "{text}"')
    myjs_block = "\n".join(myjs_lines) if myjs_lines else "  # 可在此覆盖前端 myjs.js 配置"

    content = f"""# Danmuji 全局配置
server:
  host: {server.get('host', DEFAULT_HOST)}
  port: {int(server.get('port', DEFAULT_PORT))}

api:
  roomid: {int(api.get('roomid', 0))}
  uid: {int(api.get('uid', 0))}
  cookie: "{str(api.get('cookie', '')).replace('\\"', '\\\\"')}"

qr_login:
  # 最近一次扫码成功信息（由 /api/bili/qr/poll 自动写入）
  last_success_at: "{str(qr_login.get('last_success_at', '')).replace('\\"', '\\\\"')}"
  qrcode_key: "{str(qr_login.get('qrcode_key', '')).replace('\\"', '\\\\"')}"
  poll_code: {int(qr_login.get('poll_code', -1))}
  message: "{str(qr_login.get('message', '')).replace('\\"', '\\\\"')}"
  cookie: "{str(qr_login.get('cookie', '')).replace('\\"', '\\\\"')}"

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
  # 三个存档位（像游戏存档）
  slots: {int(queue_archive.get('slots', 3))}

callback:
  enabled: {'true' if bool(callback_cfg.get('enabled', False)) else 'false'}
  url: "{str(callback_cfg.get('url', '')).replace('\\"', '\\\\"')}"
  auth_token: "{str(callback_cfg.get('auth_token', '')).replace('\\"', '\\\\"')}"
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
        self.slots = max(1, int(slots))
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
        self.server.logger.info("%s - %s", self.address_string(), format % args)

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
            self._write_json(
                {
                    "status": "ok",
                    "ws_clients": self.server.ws_hub.client_count,
                    "danmu_stream_active": bool(self.server.ws_hub.last_message_at),
                    "last_message_at": self.server.ws_hub.last_message_at,
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
            cookie = str(payload.get("cookie", ""))
            callback_payload = payload.get("callback", {})
            callback_enabled = bool(callback_payload.get("enabled", False)) if isinstance(callback_payload, dict) else False
            callback_url = str(callback_payload.get("url", "")).strip() if isinstance(callback_payload, dict) else ""
            callback_auth_token = str(callback_payload.get("auth_token", "")).strip() if isinstance(callback_payload, dict) else ""
            callback_timeout = int(callback_payload.get("timeout_seconds", 5)) if isinstance(callback_payload, dict) else 5

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
                },
            )
            save_config(updated)
            self.server.runtime_config = updated
            self._write_json(
                {
                    "status": "ok",
                    "roomid": roomid,
                    "uid": uid,
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
                    success_time = dt.datetime.now(dt.timezone.utc).isoformat()
                    updated = _merge_config(
                        self.server.runtime_config,
                        {
                            "api": {"cookie": cookie_text},
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
                    callback_ok, callback_message = _dispatch_login_callback(
                        self.server.runtime_config.get("callback", {}),
                        cookie=cookie_text,
                        bilibili_data=data,
                        logger=self.server.logger,
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
            "[queue] actor=%s message=%s queue_size=%s archive=%s",
            actor,
            message,
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

    logger.info("Danmuji backend started on http://%s:%s", host, port)
    logger.info("Backend config page: http://127.0.0.1:%s/config", port)
    logger.info("Index page: http://127.0.0.1:%s/index", port)
    logger.info("WebSocket: ws://127.0.0.1:%s/ws (alias: /danmu/sub)", port)
    httpd.serve_forever()


if __name__ == "__main__":
    config = load_config()
    host = os.getenv("DANMUJI_BACKEND_HOST", str(config.get("server", {}).get("host", DEFAULT_HOST)))
    port = int(os.getenv("DANMUJI_BACKEND_PORT", int(config.get("server", {}).get("port", DEFAULT_PORT))))
    run_server(host=host, port=port)
