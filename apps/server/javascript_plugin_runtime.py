"""QuickJS runtime for JavaScript .bilipdj-plugin packages (Issue #130)."""
from __future__ import annotations

import base64
import json
import socket
import threading
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import quickjs
import websocket

from .danmu_plugins import DanmuPlugin, PLUGIN_API_VERSION, PLUGIN_TYPE

JS_MEMORY_LIMIT = 16 * 1024 * 1024
JS_TIME_LIMIT_SECONDS = 0.5
JS_STACK_LIMIT = 512 * 1024
JS_TICK_INTERVAL_SECONDS = 0.02
MAX_WS_MESSAGE_BYTES = 4 * 1024 * 1024

_JS_BRIDGE = r'''
const __bilipdjHost = Object.freeze({
  getConfig() { return JSON.parse(__host_get_config()); },
  getSecret(key) { return __host_get_secret(String(key)); },
  emit(payload) { return __host_emit(JSON.stringify(payload ?? {})); },
  processDanmu(payload) { return __host_process_danmu(JSON.stringify(payload ?? {})); },
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
'''


class JavascriptRelay:
    def __init__(self, manager: Any, record: Any, active_server: Any, context: Any) -> None:
        self.manager = manager
        self.record = record
        self.server = active_server
        self.context = context
        self.platform = record.platform
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        self._status_lock = threading.RLock()
        self._status: dict[str, Any] = {
            "platform": self.platform,
            "plugin_id": record.plugin_id,
            "connected": False,
            "runtime": "javascript",
            "state": "idle",
        }
        self._websockets: dict[int, websocket.WebSocket] = {}
        self._ws_next = 1
        self._ws_lock = threading.RLock()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name=f"bilipdj-js-plugin-{self.platform}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        self._close_all_websockets()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    def request_reconnect(self) -> None:
        self._reconnect_event.set()

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

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    def _run(self) -> None:
        ctx = quickjs.Context()
        ctx.set_memory_limit(JS_MEMORY_LIMIT)
        ctx.set_time_limit(JS_TIME_LIMIT_SECONDS)
        ctx.set_max_stack_size(JS_STACK_LIMIT)
        self._install_host_api(ctx)
        try:
            source = self._read_source()
            ctx.eval(source + "\n" + _JS_BRIDGE)
            config_json = self._json(self.context.get_config())
            ctx.eval(f"__bilipdj_init({self._json(config_json)})")
            ctx.eval("__bilipdj_call('start')")
            self._set_status({"state": "running"})
            while not self._stop_event.is_set():
                if self._reconnect_event.is_set():
                    self._reconnect_event.clear()
                    ctx.eval("__bilipdj_call('requestReconnect')")
                if bool(ctx.eval("__bilipdj_has('tick')")):
                    ctx.eval("__bilipdj_call('tick')")
                if bool(ctx.eval("__bilipdj_has('getRuntimeStatus')")):
                    raw = ctx.eval("__bilipdj_call('getRuntimeStatus')")
                    if isinstance(raw, str):
                        try:
                            value = json.loads(raw)
                        except json.JSONDecodeError:
                            value = None
                        if isinstance(value, dict):
                            self._set_status(value)
                self._stop_event.wait(JS_TICK_INTERVAL_SECONDS)
            try:
                ctx.eval("__bilipdj_call('stop')")
            except Exception:
                pass
            self._set_status({"connected": False, "state": "stopped"})
        except Exception as exc:  # noqa: BLE001
            self._set_error(str(exc))
        finally:
            self._close_all_websockets()

    def _install_host_api(self, ctx: quickjs.Context) -> None:
        ctx.add_callable("__host_get_config", lambda: self._json(self.context.get_config()))
        ctx.add_callable("__host_get_secret", lambda key: self.context.get_secret(str(key)))
        ctx.add_callable("__host_emit", self._host_emit)
        ctx.add_callable("__host_process_danmu", self._host_process_danmu)
        ctx.add_callable("__host_set_status", self._host_set_status)
        ctx.add_callable("__host_http_request", self._host_http_request)
        ctx.add_callable("__host_read_data", self._host_read_data)
        ctx.add_callable("__host_write_data", self._host_write_data)
        ctx.add_callable("__host_run_process", self._host_run_process)
        ctx.add_callable("__host_ws_connect", self._host_ws_connect)
        ctx.add_callable("__host_ws_recv", self._host_ws_recv)
        ctx.add_callable("__host_ws_send", self._host_ws_send)
        ctx.add_callable("__host_ws_close", self._host_ws_close)
        ctx.add_callable("__host_sleep", lambda ms: time.sleep(min(max(float(ms), 0.0), 1000.0) / 1000.0))
        ctx.add_callable("__host_is_stopping", self._stop_event.is_set)
        ctx.add_callable("__host_consume_reconnect", self._consume_reconnect)

    def _host_emit(self, raw: str) -> bool:
        payload = json.loads(raw)
        self.context.emit(payload)
        return True

    def _host_process_danmu(self, raw: str) -> bool:
        payload = json.loads(raw)
        self.context.process_danmu_json(payload)
        return True

    def _host_set_status(self, raw: str) -> bool:
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise TypeError("status must be an object")
        self._set_status(payload)
        return True

    def _host_http_request(self, url: str, options_raw: str) -> str:
        options = json.loads(options_raw) if options_raw else {}
        headers = options.get("headers", {}) if isinstance(options, dict) else {}
        timeout = options.get("timeout", 10) if isinstance(options, dict) else 10
        data = self.context.http_request(
            str(url), timeout=float(timeout or 10),
            headers={str(k): str(v) for k, v in headers.items()} if isinstance(headers, dict) else {},
        )
        return self._json({"data_base64": base64.b64encode(data).decode("ascii")})

    def _host_read_data(self, relative: str) -> str:
        data = self.context.read_data(str(relative))
        return base64.b64encode(data).decode("ascii")

    def _host_write_data(self, relative: str, encoded: str) -> bool:
        data = base64.b64decode(str(encoded), validate=True)
        self.context.write_data(str(relative), data)
        return True

    def _host_run_process(self, argv_raw: str, timeout: float) -> str:
        argv = json.loads(argv_raw)
        result = self.context.run_process(argv, timeout=float(timeout or 15))
        return self._json({
            "returncode": int(result.returncode),
            "stdout": result.stdout[-1024 * 1024:],
            "stderr": result.stderr[-1024 * 1024:],
        })

    def _require_network(self) -> None:
        self.context._require("network")

    def _host_ws_connect(self, url: str, headers_raw: str) -> int:
        self._require_network()
        parsed = urlparse(str(url or ""))
        if parsed.scheme not in {"ws", "wss"} or not parsed.hostname:
            raise ValueError("only ws/wss URLs are allowed")
        headers = json.loads(headers_raw) if headers_raw else {}
        header_list = [f"{key}: {value}" for key, value in headers.items()] if isinstance(headers, dict) else []
        ws = websocket.create_connection(url, header=header_list, timeout=10)
        ws.settimeout(0.1)
        with self._ws_lock:
            handle = self._ws_next
            self._ws_next += 1
            self._websockets[handle] = ws
        return handle

    def _get_ws(self, handle: int) -> websocket.WebSocket:
        with self._ws_lock:
            ws = self._websockets.get(int(handle))
        if ws is None:
            raise ValueError("invalid websocket handle")
        return ws

    def _host_ws_recv(self, handle: int, timeout_ms: float) -> str:
        self._require_network()
        ws = self._get_ws(handle)
        ws.settimeout(min(max(float(timeout_ms), 1.0), 1000.0) / 1000.0)
        try:
            value = ws.recv()
        except (websocket.WebSocketTimeoutException, socket.timeout):
            return self._json({"type": "timeout"})
        if value is None:
            return self._json({"type": "closed"})
        if isinstance(value, bytes):
            if len(value) > MAX_WS_MESSAGE_BYTES:
                raise ValueError("websocket message exceeds plugin safety limit")
            return self._json({"type": "binary", "data_base64": base64.b64encode(value).decode("ascii")})
        text = str(value)
        if len(text.encode("utf-8")) > MAX_WS_MESSAGE_BYTES:
            raise ValueError("websocket message exceeds plugin safety limit")
        return self._json({"type": "text", "data": text})

    def _host_ws_send(self, handle: int, data: str, binary: bool) -> bool:
        self._require_network()
        ws = self._get_ws(handle)
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

    def _host_ws_close(self, handle: int) -> bool:
        with self._ws_lock:
            ws = self._websockets.pop(int(handle), None)
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass
        return True

    def _close_all_websockets(self) -> None:
        with self._ws_lock:
            items = list(self._websockets.items())
            self._websockets.clear()
        for _, ws in items:
            try:
                ws.close()
            except Exception:
                pass

    def _consume_reconnect(self) -> bool:
        value = self._reconnect_event.is_set()
        if value:
            self._reconnect_event.clear()
        return value


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
