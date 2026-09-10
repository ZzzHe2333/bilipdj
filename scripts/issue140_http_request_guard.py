from __future__ import annotations

import base64
import hashlib
import io
import json
import logging
import sys
import tempfile
import threading
import time
import zipfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import plugin_manager as pm  # noqa: E402
from apps.server import plugin_runtime_dual  # noqa: E402
from apps.server.danmu_plugins import DanmuPluginRegistry  # noqa: E402
from apps.server.plugin_http_request import (  # noqa: E402
    MAX_HTTP_REQUEST_BODY_BYTES,
    perform_plugin_http_request,
    prepare_plugin_http_request,
)

plugin_runtime_dual.install_dual_runtime_support()

SOURCE = b'''\
function createRelay(config, host) {
  let state = {connected: false};
  return {
    start() {
      const legacy = host.httpRequest(config.base_url + '/legacy', {
        headers: {'X-Probe': 'legacy'}, timeout: 5
      });
      const textPost = host.httpRequest(config.base_url + '/echo', {
        method: 'POST',
        headers: {'Content-Type': 'text/plain; charset=utf-8'},
        body: 'hello-\\u4e16\\u754c',
        timeout: 5
      });
      const binaryPost = host.httpRequest(config.base_url + '/echo', {
        method: 'POST', body_base64: 'AAEC/w==', timeout: 5
      });
      const limited = host.httpRequest(config.base_url + '/limited', {timeout: 5});
      host.emit({
        type: 'HTTP_REQUEST_PROBE', legacy, textPost, binaryPost, limited
      });
      state = {connected: true, complete: true};
      host.setStatus(state);
    },
    tick() {},
    stop() { state.connected = false; host.setStatus(state); },
    getRuntimeStatus() { return state; }
  };
}
'''


class ProbeHandler(BaseHTTPRequestHandler):
    server_version = "BiliPDJIssue140/1"

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
        return

    def _send(self, status: int, body: bytes, *, extra: dict[str, str] | None = None) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Set-Cookie", "session=must-not-leak; HttpOnly")
        self.send_header("X-Private-Probe", "must-not-leak")
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/legacy":
            self._send(200, b"legacy-get", extra={"ETag": '"probe"'})
            return
        if self.path == "/limited":
            self._send(429, b"slow-down", extra={"Retry-After": "7"})
            return
        self._send(404, b"not-found")

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        body = self.rfile.read(length)
        if self.path != "/echo":
            self._send(404, b"not-found")
            return
        self._send(201, body, extra={"Location": "/created"})


def package() -> bytes:
    manifest = {
        "schema": 1,
        "id": "example.http.request.probe",
        "name": "HTTP Request Probe",
        "version": "1.0.0",
        "plugin_api": 1,
        "type": "danmu_source",
        "platform": "http_request_probe",
        "runtime": "javascript",
        "entry": "plugin.js",
        "min_bilipdj_version": "2.0.0",
        "permissions": ["network"],
        "capabilities": ["danmu"],
        "files": {"plugin.js": hashlib.sha256(SOURCE).hexdigest()},
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest).encode("utf-8"))
        archive.writestr("plugin.js", SOURCE)
    return out.getvalue()


def _expect_value_error(fn: Any, contains: str) -> None:
    try:
        fn()
    except ValueError as exc:
        assert contains.lower() in str(exc).lower(), str(exc)
    else:
        raise AssertionError(f"expected ValueError containing {contains!r}")


def test_option_validation() -> None:
    base = "http://127.0.0.1:1/"
    assert prepare_plugin_http_request(base).request.get_method() == "GET"
    assert prepare_plugin_http_request(base, {"method": "post"}).request.get_method() == "POST"
    assert prepare_plugin_http_request(base, {"timeout": 0}).timeout == 1.0
    assert prepare_plugin_http_request(base, {"timeout": 99}).timeout == 30.0

    _expect_value_error(lambda: prepare_plugin_http_request(base, {"method": "DELETE"}), "GET or POST")
    _expect_value_error(lambda: prepare_plugin_http_request(base, {"body": "x"}), "GET request body")
    _expect_value_error(
        lambda: prepare_plugin_http_request(base, {"method": "POST", "body": "x", "body_base64": "eA=="}),
        "mutually exclusive",
    )
    _expect_value_error(
        lambda: prepare_plugin_http_request(base, {"method": "POST", "body_base64": "%%%"}),
        "valid Base64",
    )
    _expect_value_error(
        lambda: prepare_plugin_http_request(base, {"method": "POST", "body": "x" * (MAX_HTTP_REQUEST_BODY_BYTES + 1)}),
        "exceeds",
    )
    _expect_value_error(
        lambda: prepare_plugin_http_request(base, {"headers": {"X-Test": "ok\r\nInjected: yes"}}),
        "line break",
    )
    _expect_value_error(lambda: prepare_plugin_http_request("file:///tmp/x"), "http/https")


def test_transport_direct(base_url: str) -> None:
    prepared = prepare_plugin_http_request(base_url + "/limited", {"timeout": 5})
    result = perform_plugin_http_request(prepared)
    assert result["status"] == 429, result
    assert base64.b64decode(result["data_base64"]) == b"slow-down"
    assert result["headers"].get("retry-after") == "7"
    assert "set-cookie" not in result["headers"]
    assert "x-private-probe" not in result["headers"]


def test_quickjs_host_roundtrip(base_url: str) -> None:
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        module = SimpleNamespace(
            APP_DIR=root,
            BUNDLE_DIR=ROOT,
            REPO_DIR=ROOT,
            DEFAULT_PLATFORM="bilibili",
            RESERVED_RUNTIME_PLATFORMS=(),
            ALL_RUNTIME_PLATFORMS=(),
            PLATFORM_DISPLAY_NAMES={},
            _get_runtime_platform=lambda cfg: str((cfg or {}).get("platform", "bilibili")),
        )
        registry = DanmuPluginRegistry()
        manager = pm.PluginManager(module, registry)
        manager.install_bytes(package(), filename="http-request-probe.bilipdj-plugin", allow_unsigned=True)
        manager.set_enabled("example.http.request.probe", True)

        events: list[dict[str, Any]] = []
        active = SimpleNamespace(
            runtime_config={
                "platform": "http_request_probe",
                "http_request_probe": {"base_url": base_url},
            },
            logger=logging.getLogger("issue140-http-probe"),
            ws_hub=SimpleNamespace(broadcast_json=lambda _sender, payload: events.append(dict(payload))),
            queue_manager=SimpleNamespace(process_danmu_json=lambda payload: None),
        )
        relay = registry.create_relay("http_request_probe", active)
        relay.start()
        deadline = time.monotonic() + 5.0
        probe: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            probe = next((item for item in events if item.get("type") == "HTTP_REQUEST_PROBE"), None)
            if probe is not None or relay.get_runtime_status().get("state") == "error":
                break
            time.sleep(0.02)

        status = relay.get_runtime_status()
        assert status.get("state") == "running", status
        assert probe is not None, events

        legacy = probe["legacy"]
        assert legacy["status"] == 200
        assert base64.b64decode(legacy["data_base64"]) == b"legacy-get"
        assert legacy["headers"].get("etag") == '"probe"'
        assert "set-cookie" not in legacy["headers"]
        assert "x-private-probe" not in legacy["headers"]

        text_post = probe["textPost"]
        assert text_post["status"] == 201
        assert base64.b64decode(text_post["data_base64"]).decode("utf-8") == "hello-世界"
        assert text_post["headers"].get("location") == "/created"

        binary_post = probe["binaryPost"]
        assert binary_post["status"] == 201
        assert base64.b64decode(binary_post["data_base64"]) == b"\x00\x01\x02\xff"

        limited = probe["limited"]
        assert limited["status"] == 429
        assert base64.b64decode(limited["data_base64"]) == b"slow-down"
        assert limited["headers"].get("retry-after") == "7"
        assert "set-cookie" not in limited["headers"]

        relay.stop()
        relay.join(timeout=2)
        stopped = relay.get_runtime_status()
        assert stopped.get("state") == "stopped", stopped


def main() -> None:
    test_option_validation()
    server = ThreadingHTTPServer(("127.0.0.1", 0), ProbeHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    base_url = f"http://127.0.0.1:{server.server_port}"
    try:
        test_transport_direct(base_url)
        test_quickjs_host_roundtrip(base_url)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
    print("issue #140 JavaScript host.httpRequest guard: OK")


if __name__ == "__main__":
    main()
