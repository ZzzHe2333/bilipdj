"""Process-isolated QuickJS runtime for JavaScript .bilipdj-plugin packages (Issue #130)."""
from __future__ import annotations

import base64
import json
import multiprocessing as mp
import socket
import subprocess
import threading
import time
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse

from .danmu_plugins import DanmuPlugin, PLUGIN_API_VERSION, PLUGIN_TYPE
from .plugin_data_quota import read_plugin_data, write_plugin_data
from .plugin_http_request import perform_plugin_http_request, prepare_plugin_http_request

JS_MEMORY_LIMIT = 16 * 1024 * 1024
JS_TIME_LIMIT_SECONDS = 0.5
JS_STACK_LIMIT = 512 * 1024
JS_TICK_INTERVAL_SECONDS = 0.02
MAX_WS_MESSAGE_BYTES = 4 * 1024 * 1024
MAX_HOST_JSON_BYTES = 1024 * 1024

_JS_BRIDGE = r"""
const __bilipdjHost = Object.freeze({
  getConfig() { return JSON.parse(__host_get_config()); },
  getSecret(key) { return __host_get_secret(String(key)); },
  emit(payload) { return __host_emit(JSON.stringify(payload ?? {})); },
  processDanmu(payload) { return __host_process_danmu(JSON.stringify(payload ?? {})); },
  processDanmuEvent(payload) { return __host_process_danmu_event(JSON.stringify(payload ?? {})); },
  setStatus(payload) { return __host_set_status(JSON.stringify(payload ?? {})); },
  httpRequest(url, options = {}) {
    return JSON.parse(__host_http_request(String(url), JSON.stringify(options ?? {})));
  },
  readData(path) { return __host_read_data(String(path)); },
  writeData(path, base64Data) { return __host_write_data(String(path), String(base64Data)); },
  runProcess(argv, timeoutSeconds = 15) {
    return JSON.parse(__host_run_process(JSON.stringify(argv ?? []), Number(timeoutSeconds) || 15));
  },
  wsConnect(url, headers = {}) {
    return __host_ws_connect(String(url), JSON.stringify(headers ?? {}));
  },
  wsRecv(handle, timeoutMs = 100) {
    return JSON.parse(__host_ws_recv(Number(handle), Number(timeoutMs) || 100));
  },
  wsSend(handle, data, binary = false) {
    return __host_ws_send(Number(handle), String(data ?? ''), Boolean(binary));
  },
  wsClose(handle) { return __host_ws_close(Number(handle)); },
  sleep(ms) { return __host_sleep(Math.max(0, Number(ms) || 0)); },
  isStopping() { return Boolean(__host_is_stopping()); },
  consumeReconnect() { return Boolean(__host_consume_reconnect()); },
  base64ToBytes(value) {
    const alphabet = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
    const clean = String(value || '').replace(/=+$/, '');
    const out = [];
    let buffer = 0, bits = 0;
    for (const ch of clean) {
      const idx = alphabet.indexOf(ch);
      if (idx < 0) continue;
      buffer = (buffer << 6) | idx;
      bits += 6;
      if (bits >= 8) {
        bits -= 8;
        out.push((buffer >> bits) & 255);
      }
    }
    return new Uint8Array(out);
  }
});
let __bilipdjRelay = null;
function __bilipdj_init(configJson) {
  if (typeof createRelay !== 'function') throw new Error('JavaScript plugin must define createRelay(config, host)');
  __bilipdjRelay = createRelay(JSON.parse(configJson), __bilipdjHost);
  if (!__bilipdjRelay || typeof __bilipdjRelay !== 'object') throw new Error('createRelay must return an object');
  return true;
}
function __bilipdj_call(name) {
  if (!__bilipdjRelay) throw new Error('plugin relay is not initialized');
  const fn = __bilipdjRelay[name];
  if (typeof fn !== 'function') return JSON.stringify(null);
  const value = fn.call(__bilipdjRelay);
  return JSON.stringify(value === undefined ? null : value);
}
function __bilipdj_has(name) {
  return Boolean(__bilipdjRelay && typeof __bilipdjRelay[name] === 'function');
}
"""


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _send(conn: Any, payload: dict[str, Any]) -> None:
    try:
        conn.send(payload)
    except (BrokenPipeError, EOFError, OSError):
        raise SystemExit(0)


def _phase(conn: Any, state: str, name: str = "") -> None:
    _send(conn, {"type": "phase", "state": state, "name": name})


def _blocking(conn: Any, state: str, timeout: float = 0.0) -> None:
    _send(conn, {"type": "blocking", "state": state, "timeout": float(timeout)})


def _require_permission(permissions: frozenset[str], permission: str) -> None:
    if permission not in permissions:
        raise PermissionError(f"plugin permission not granted: {permission}")


def _safe_data_path(data_root: str, relative: str) -> Path:
    raw = str(relative or "").replace("\\", "/")
    path = PurePosixPath(raw)
    if (
        not raw
        or raw.startswith("/")
        or path.is_absolute()
        or ":" in raw
        or any(part in {"", ".", ".."} for part in path.parts)
        or len(path.parts) > 12
        or len(raw) > 240
    ):
        raise PermissionError("invalid plugin data path")
    reserved = {"con", "prn", "aux", "nul", *(f"com{i}" for i in range(1, 10)), *(f"lpt{i}" for i in range(1, 10))}
    for part in path.parts:
        stem = part.split(".", 1)[0].strip().lower()
        if stem in reserved:
            raise PermissionError("invalid Windows-reserved plugin data path")
    base = Path(data_root).resolve()
    target = base.joinpath(*path.parts).resolve()
    if target != base and base not in target.parents:
        raise PermissionError("path escapes plugin data directory")
    return target


def _parse_host_json(raw: str, label: str) -> Any:
    encoded = str(raw or "").encode("utf-8")
    if len(encoded) > MAX_HOST_JSON_BYTES:
        raise ValueError(f"{label} exceeds host message limit")
    return json.loads(str(raw or ""))


def _javascript_worker_main(
    conn: Any,
    stop_event: Any,
    reconnect_event: Any,
    source: str,
    config: dict[str, Any],
    secret_config: dict[str, Any],
    permissions_raw: tuple[str, ...],
    data_root: str,
) -> None:
    import quickjs
    import websocket

    permissions = frozenset(str(item) for item in permissions_raw)
    websockets: dict[int, Any] = {}
    ws_next = 1

    def emit(raw: str) -> bool:
        payload = _parse_host_json(raw, "emit payload")
        if not isinstance(payload, dict):
            raise TypeError("emit payload must be an object")
        _send(conn, {"type": "emit", "payload": payload})
        return True

    def process_danmu(raw: str) -> bool:
        payload = _parse_host_json(raw, "danmu payload")
        if not isinstance(payload, dict):
            raise TypeError("danmu payload must be an object")
        _send(conn, {"type": "danmu", "payload": payload})
        return True

    def process_danmu_event(raw: str) -> bool:
        payload = _parse_host_json(raw, "danmu event")
        if not isinstance(payload, dict):
            raise TypeError("danmu event must be an object")
        _send(conn, {"type": "danmu_event", "payload": payload})
        return True

    def set_status(raw: str) -> bool:
        payload = _parse_host_json(raw, "status payload")
        if not isinstance(payload, dict):
            raise TypeError("status must be an object")
        _send(conn, {"type": "status", "payload": payload})
        return True

    def get_secret(key: str) -> str:
        _require_permission(permissions, "secrets")
        return str(secret_config.get(str(key), "") or "")

    def http_request(url: str, options_raw: str) -> str:
        _require_permission(permissions, "network")
        options = _parse_host_json(options_raw or "{}", "http options")
        prepared = prepare_plugin_http_request(str(url or ""), options)
        _blocking(conn, "start", prepared.timeout + 1.0)
        try:
            result = perform_plugin_http_request(prepared)
        finally:
            _blocking(conn, "end")
        return _json(result)

    def read_data(relative: str) -> str:
        _require_permission(permissions, "filesystem_read")
        root = Path(data_root).resolve()
        target = _safe_data_path(data_root, str(relative))
        data = read_plugin_data(root, target)
        return base64.b64encode(data).decode("ascii")

    def write_data(relative: str, encoded: str) -> bool:
        _require_permission(permissions, "filesystem_write")
        data = base64.b64decode(str(encoded), validate=True)
        root = Path(data_root).resolve()
        target = _safe_data_path(data_root, str(relative))
        write_plugin_data(root, target, data)
        return True

    def run_process(argv_raw: str, timeout: float) -> str:
        _require_permission(permissions, "subprocess")
        argv = _parse_host_json(argv_raw, "process argv")
        if not isinstance(argv, list) or not argv or any(not isinstance(item, str) or not item for item in argv):
            raise ValueError("argv must be a non-empty string array")
        effective = max(1.0, min(60.0, float(timeout or 15)))
        _blocking(conn, "start", effective + 1.0)
        try:
            result = subprocess.run(
                argv,
                shell=False,
                check=False,
                capture_output=True,
                text=True,
                timeout=effective,
            )
        finally:
            _blocking(conn, "end")
        return _json({
            "returncode": int(result.returncode),
            "stdout": result.stdout[-1024 * 1024:],
            "stderr": result.stderr[-1024 * 1024:],
        })

    def ws_connect(url: str, headers_raw: str) -> int:
        nonlocal ws_next
        _require_permission(permissions, "network")
        parsed = urlparse(str(url or ""))
        if parsed.scheme not in {"ws", "wss"} or not parsed.hostname:
            raise ValueError("only ws/wss URLs are allowed")
        headers = _parse_host_json(headers_raw or "{}", "websocket headers")
        header_list = [f"{key}: {value}" for key, value in headers.items()] if isinstance(headers, dict) else []
        _blocking(conn, "start", 11.0)
        try:
            ws = websocket.create_connection(str(url), header=header_list, timeout=10)
        finally:
            _blocking(conn, "end")
        ws.settimeout(0.1)
        handle = ws_next
        ws_next += 1
        websockets[handle] = ws
        return handle

    def get_ws(handle: int) -> Any:
        ws = websockets.get(int(handle))
        if ws is None:
            raise ValueError("invalid websocket handle")
        return ws

    def ws_recv(handle: int, timeout_ms: float) -> str:
        _require_permission(permissions, "network")
        ws = get_ws(handle)
        timeout = min(max(float(timeout_ms), 1.0), 1000.0) / 1000.0
        ws.settimeout(timeout)
        _blocking(conn, "start", timeout + 0.5)
        try:
            try:
                value = ws.recv()
            except (websocket.WebSocketTimeoutException, socket.timeout):
                return _json({"type": "timeout"})
        finally:
            _blocking(conn, "end")
        if value is None:
            return _json({"type": "closed"})
        if isinstance(value, bytes):
            if len(value) > MAX_WS_MESSAGE_BYTES:
                raise ValueError("websocket message exceeds plugin safety limit")
            return _json({"type": "binary", "data_base64": base64.b64encode(value).decode("ascii")})
        text = str(value)
        if len(text.encode("utf-8")) > MAX_WS_MESSAGE_BYTES:
            raise ValueError("websocket message exceeds plugin safety limit")
        return _json({"type": "text", "data": text})

    def ws_send(handle: int, data: str, binary: bool) -> bool:
        _require_permission(permissions, "network")
        ws = get_ws(handle)
        if binary:
            payload = base64.b64decode(str(data), validate=True)
            if len(payload) > MAX_WS_MESSAGE_BYTES:
                raise ValueError("websocket message exceeds plugin safety limit")
            ws.send(payload, opcode=websocket.ABNF.OPCODE_BINARY)
        else:
            text = str(data)
            if len(text.encode("utf-8")) > MAX_WS_MESSAGE_BYTES:
                raise ValueError("websocket message exceeds plugin safety limit")
            ws.send(text)
        return True

    def ws_close(handle: int) -> bool:
        ws = websockets.pop(int(handle), None)
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        return True

    def host_sleep(ms: float) -> None:
        seconds = min(max(float(ms), 0.0), 1000.0) / 1000.0
        _blocking(conn, "start", seconds + 0.5)
        try:
            stop_event.wait(seconds)
        finally:
            _blocking(conn, "end")

    def consume_reconnect() -> bool:
        value = reconnect_event.is_set()
        if value:
            reconnect_event.clear()
        return value

    ctx = quickjs.Context()
    ctx.set_memory_limit(JS_MEMORY_LIMIT)
    ctx.set_max_stack_size(JS_STACK_LIMIT)
    ctx.add_callable("__host_get_config", lambda: _json(config))
    ctx.add_callable("__host_get_secret", get_secret)
    ctx.add_callable("__host_emit", emit)
    ctx.add_callable("__host_process_danmu", process_danmu)
    ctx.add_callable("__host_process_danmu_event", process_danmu_event)
    ctx.add_callable("__host_set_status", set_status)
    ctx.add_callable("__host_http_request", http_request)
    ctx.add_callable("__host_read_data", read_data)
    ctx.add_callable("__host_write_data", write_data)
    ctx.add_callable("__host_run_process", run_process)
    ctx.add_callable("__host_ws_connect", ws_connect)
    ctx.add_callable("__host_ws_recv", ws_recv)
    ctx.add_callable("__host_ws_send", ws_send)
    ctx.add_callable("__host_ws_close", ws_close)
    ctx.add_callable("__host_sleep", host_sleep)
    ctx.add_callable("__host_is_stopping", stop_event.is_set)
    ctx.add_callable("__host_consume_reconnect", consume_reconnect)

    def js_eval(script: str, phase_name: str) -> Any:
        _phase(conn, "start", phase_name)
        try:
            return ctx.eval(script)
        finally:
            _phase(conn, "end", phase_name)

    try:
        js_eval(source + "\n" + _JS_BRIDGE, "load")
        config_json = _json(config)
        js_eval(f"__bilipdj_init({_json(config_json)})", "init")
        js_eval("__bilipdj_call('start')", "start")
        _send(conn, {"type": "running"})
        while not stop_event.is_set():
            if reconnect_event.is_set():
                if bool(js_eval("__bilipdj_has('requestReconnect')", "probe-reconnect")):
                    reconnect_event.clear()
                    js_eval("__bilipdj_call('requestReconnect')", "reconnect")
            if bool(js_eval("__bilipdj_has('tick')", "probe-tick")):
                js_eval("__bilipdj_call('tick')", "tick")
            if bool(js_eval("__bilipdj_has('getRuntimeStatus')", "probe-status")):
                raw = js_eval("__bilipdj_call('getRuntimeStatus')", "status")
                if isinstance(raw, str):
                    try:
                        value = json.loads(raw)
                    except json.JSONDecodeError:
                        value = None
                    if isinstance(value, dict):
                        _send(conn, {"type": "status", "payload": value})
            stop_event.wait(JS_TICK_INTERVAL_SECONDS)
        try:
            if bool(js_eval("__bilipdj_has('stop')", "probe-stop")):
                js_eval("__bilipdj_call('stop')", "stop")
        except Exception:
            pass
        _send(conn, {"type": "stopped"})
    except BaseException as exc:  # noqa: BLE001
        _send(conn, {"type": "error", "message": str(exc)})
    finally:
        for ws in list(websockets.values()):
            try:
                ws.close()
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass


class JavascriptRelay:
    def __init__(self, manager: Any, record: Any, active_server: Any, context: Any) -> None:
        self.manager = manager
        self.record = record
        self.server = active_server
        self.context = context
        self.platform = record.platform
        self._thread: threading.Thread | None = None
        self._mp_context = mp.get_context("spawn")
        self._stop_event: Any = None
        self._reconnect_event: Any = None
        self._process: Any = None
        self._parent_conn: Any = None
        self._stop_requested = threading.Event()
        self._reconnect_requested = threading.Event()
        self._status_lock = threading.RLock()
        self._status: dict[str, Any] = {
            "platform": self.platform,
            "plugin_id": record.plugin_id,
            "connected": False,
            "runtime": "javascript",
            "state": "idle",
        }

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_requested.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"bilipdj-js-plugin-{self.platform}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_requested.set()
        event = self._stop_event
        if event is not None:
            event.set()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def request_reconnect(self) -> None:
        self._reconnect_requested.set()
        event = self._reconnect_event
        if event is not None:
            event.set()

    def get_runtime_status(self) -> dict[str, Any]:
        with self._status_lock:
            return dict(self._status)

    def _set_status(self, payload: dict[str, Any]) -> None:
        with self._status_lock:
            self._status.update(payload)
            self._status.setdefault("platform", self.platform)
            self._status.setdefault("plugin_id", self.record.plugin_id)
            self._status["runtime"] = "javascript"

    def _set_error(self, message: str) -> None:
        self._set_status({"connected": False, "state": "error", "error": str(message)})
        hub = getattr(self.server, "ws_hub", None)
        if hub is not None and hasattr(hub, "broadcast_json"):
            hub.broadcast_json(None, {
                "type": "PDJ_STATUS",
                "status": "danmu_error",
                "platform": self.platform,
                "plugin_id": self.record.plugin_id,
                "message": str(message),
            })

    def _read_source(self) -> str:
        entry = str(self.record.manifest.get("entry", "") or "")
        source_path = self.record.root.joinpath(*entry.split("/"))
        return source_path.read_text(encoding="utf-8")

    def _secret_config(self) -> dict[str, Any]:
        if "secrets" not in set(self.record.permissions):
            return {}
        server = getattr(self.context, "_server", None)
        runtime = getattr(server, "runtime_config", {}) if server is not None else {}
        section = runtime.get(self.platform, {}) if isinstance(runtime, dict) else {}
        return dict(section) if isinstance(section, dict) else {}

    def _handle_message(self, message: dict[str, Any]) -> tuple[str, float | None]:
        kind = str(message.get("type", "") or "")
        if kind == "status":
            payload = message.get("payload")
            if isinstance(payload, dict):
                self._set_status(payload)
            return "message", None
        if kind == "emit":
            payload = message.get("payload")
            if isinstance(payload, dict):
                self.context.emit(payload)
            return "message", None
        if kind == "danmu":
            payload = message.get("payload")
            if isinstance(payload, dict):
                self.context.process_danmu_json(payload)
            return "message", None
        if kind == "danmu_event":
            payload = message.get("payload")
            if isinstance(payload, dict):
                self.context.process_danmu_event(payload)
            return "message", None
        if kind == "running":
            self._set_status({"state": "running"})
            return "message", None
        if kind == "stopped":
            self._set_status({"connected": False, "state": "stopped"})
            return "stopped", None
        if kind == "error":
            self._set_error(str(message.get("message", "JavaScript plugin failed")))
            return "error", None
        if kind == "phase":
            return str(message.get("state", "")), None
        if kind == "blocking":
            timeout = max(0.0, min(65.0, float(message.get("timeout", 0.0) or 0.0)))
            return "blocking-" + str(message.get("state", "")), timeout
        return "message", None

    def _terminate_worker(self) -> None:
        process = self._process
        if process is not None and process.is_alive():
            process.terminate()
            process.join(timeout=1.0)
            if process.is_alive() and hasattr(process, "kill"):
                process.kill()
                process.join(timeout=1.0)

    def _run(self) -> None:
        source = self._read_source()
        config = self.context.get_config()
        secret_config = self._secret_config()
        data_root = str((self.manager.data_root / self.record.plugin_id).resolve())
        Path(data_root).mkdir(parents=True, exist_ok=True)
        parent_conn, child_conn = self._mp_context.Pipe(duplex=False)
        self._parent_conn = parent_conn
        self._stop_event = self._mp_context.Event()
        self._reconnect_event = self._mp_context.Event()
        if self._stop_requested.is_set():
            self._stop_event.set()
        if self._reconnect_requested.is_set():
            self._reconnect_event.set()
            self._reconnect_requested.clear()
        process = self._mp_context.Process(
            target=_javascript_worker_main,
            args=(
                child_conn,
                self._stop_event,
                self._reconnect_event,
                source,
                config,
                secret_config,
                tuple(self.record.permissions),
                data_root,
            ),
            name=f"bilipdj-js-worker-{self.platform}",
            daemon=True,
        )
        self._process = process
        phase_deadline: float | None = None
        blocking_deadline: float | None = None
        finished = False
        try:
            process.start()
            child_conn.close()
            while process.is_alive() or parent_conn.poll():
                if parent_conn.poll(0.02):
                    try:
                        message = parent_conn.recv()
                    except EOFError:
                        break
                    if not isinstance(message, dict):
                        continue
                    state, timeout = self._handle_message(message)
                    now = time.monotonic()
                    if state == "start":
                        phase_deadline = now + JS_TIME_LIMIT_SECONDS
                        blocking_deadline = None
                    elif state == "end":
                        phase_deadline = None
                        blocking_deadline = None
                    elif state == "blocking-start":
                        blocking_deadline = now + max(JS_TIME_LIMIT_SECONDS, float(timeout or 0.0))
                    elif state == "blocking-end":
                        blocking_deadline = None
                        phase_deadline = now + JS_TIME_LIMIT_SECONDS
                    elif state in {"stopped", "error"}:
                        finished = True
                now = time.monotonic()
                effective_deadline = blocking_deadline if blocking_deadline is not None else phase_deadline
                if effective_deadline is not None and now > effective_deadline:
                    self._set_error("JavaScript plugin execution time limit exceeded")
                    self._terminate_worker()
                    finished = True
                    break
            process.join(timeout=0.2)
            if not finished and self.get_runtime_status().get("state") not in {"error", "stopped"}:
                if self._stop_event.is_set():
                    self._set_status({"connected": False, "state": "stopped"})
                elif process.exitcode not in (0, None):
                    self._set_error(f"JavaScript plugin worker exited unexpectedly ({process.exitcode})")
        except Exception as exc:  # noqa: BLE001
            self._set_error(str(exc))
            self._terminate_worker()
        finally:
            try:
                parent_conn.close()
            except Exception:
                pass
            self._parent_conn = None
            self._process = None


def create_javascript_danmu_plugin(manager: Any, record: Any) -> DanmuPlugin:
    def create_relay(active_server: Any) -> JavascriptRelay:
        context = manager.create_context(active_server, record)
        return JavascriptRelay(manager, record, active_server, context)

    create_relay.__name__ = f"create_external_{record.platform}_javascript_relay"
    return DanmuPlugin(
        plugin_id=record.plugin_id,
        platform=record.platform,
        name=record.name,
        relay_factory=create_relay,
        version=record.version,
        plugin_api=PLUGIN_API_VERSION,
        plugin_type=PLUGIN_TYPE,
        source="external",
        config_section=str(record.manifest.get("config_section", record.platform) or record.platform),
        capabilities=tuple(str(item) for item in record.manifest.get("capabilities", []) or []),
        available=True,
    )


__all__ = [
    "JS_MEMORY_LIMIT", "JS_STACK_LIMIT", "JS_TIME_LIMIT_SECONDS",
    "JavascriptRelay", "create_javascript_danmu_plugin",
]
