"""Local Douyin web login and non-secret live-room parameter discovery.

Login uses the official Douyin page in an isolated Chrome/Edge profile.
Cookies are only stored under the user's private key directory; never sent
back to HTTP clients or written into Git-tracked files.
"""
from __future__ import annotations

import copy
import datetime as dt
import functools
import json
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
from http import HTTPStatus
from pathlib import Path
from typing import Any

START_PATH = "/api/douyin/login/start"
STATUS_PATH = "/api/douyin/login/status"
PARAMS_PATH = "/api/douyin/login/params"
_PRIVATE_NAMES = frozenset({
    "sessionid", "sessionid_ss", "ttwid", "msToken", "sid_guard",
    "sid_tt", "uid_tt", "passport_csrf_token", "odin_tt",
    "csrf_session_id", "passport_auth_status", "passport_auth_status_ss",
})
_MAX_LOGIN_SECONDS = 300
_PATCH_LOCK = threading.RLock()


def _browser_binary() -> str:
    explicit = os.environ.get("BILIPDJ_DOUYIN_BROWSER", "").strip()
    if explicit:
        path = Path(explicit)
        if path.is_file():
            return str(path)
        raise RuntimeError("Configured Chrome/Edge path does not exist")
    candidates = [
        shutil.which(name)
        for name in ("google-chrome", "google-chrome-stable", "chromium",
                     "chromium-browser", "microsoft-edge", "msedge")
    ]
    if sys.platform == "win32":
        root = os.environ.get("PROGRAMFILES", r"C:\Program Files")
        root_x86 = os.environ.get("PROGRAMFILES(X86)", r"C:\Program Files (x86)")
        candidates.extend([
            str(Path(root) / "Google/Chrome/Application/chrome.exe"),
            str(Path(root_x86) / "Microsoft/Edge/Application/msedge.exe"),
            str(Path(root) / "Microsoft/Edge/Application/msedge.exe"),
        ])
    elif sys.platform == "darwin":
        candidates.extend([
            "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
            "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
        ])
    for candidate in candidates:
        if candidate and Path(candidate).is_file():
            return str(candidate)
    raise RuntimeError("Chrome/Edge not found on the backend host")


def _allowed_cookie(record: Any) -> bool:
    if not isinstance(record, dict):
        return False
    domain = str(record.get("domain", "")).lstrip(".").lower()
    return (domain == "douyin.com" or domain.endswith(".douyin.com")) and (
        record.get("name") in _PRIVATE_NAMES
    ) and isinstance(record.get("value"), str)


def _redacted_cookie_header(records: Any) -> str:
    """Only returns a header for INTERNAL backend calls. Never serialize to HTTP."""
    if not isinstance(records, list):
        return ""
    pairs: dict[str, str] = {}
    for record in records:
        if _allowed_cookie(record):
            name, value = record["name"], record["value"]
            if name and value and len(value) <= 4096 and "\r" not in value and "\n" not in value:
                pairs[name] = value
    return "; ".join(f"{key}={value}" for key, value in pairs.items())


def _write_private_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if os.name != "nt":
        os.chmod(path.parent, 0o700)
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(raw)
        os.replace(tmp, path)
        if os.name != "nt":
            os.chmod(path, 0o600)
    finally:
        tmp.unlink(missing_ok=True)


class _CDP:
    def __init__(self, port: int) -> None:
        import websocket  # installed project dependency: websocket-client
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=2) as response:
            info = json.load(response)
        remote_url = info.get("webSocketDebuggerUrl", "")
        parsed = urllib.parse.urlsplit(remote_url)
        if parsed.scheme != "ws" or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}:
            raise RuntimeError("Chrome DevTools did not expose a local endpoint")
        self.socket = websocket.create_connection(remote_url, timeout=8, suppress_origin=True)
        self._next_id = 0

    def call(self, method: str, params: dict[str, Any] | None = None,
             session: str = "") -> dict[str, Any]:
        self._next_id += 1
        request: dict[str, Any] = {"id": self._next_id, "method": method, "params": params or {}}
        if session:
            request["sessionId"] = session
        self.socket.send(json.dumps(request))
        while True:
            msg = json.loads(self.socket.recv())
            if msg.get("id") != self._next_id:
                continue
            if msg.get("error"):
                raise RuntimeError("Chrome DevTools operation failed: " + method)
            return msg.get("result", {})

    def close(self) -> None:
        self.socket.close()


class DouyinLoginManager:
    def __init__(self, server_module: Any) -> None:
        self.module = server_module
        base = Path(getattr(server_module, "KEY_DIR", getattr(server_module, "_YAML_DIR")))
        self.profile_dir = base / "douyin-browser-profile"
        self.secret_path = base / "douyin-browser-session.json"
        self._lock = threading.RLock()
        self._state = "idle"
        self._error = ""
        self._start_time = 0.0
        self._backend: Any = None

    def cookie_header(self) -> str:
        try:
            data = json.loads(self.secret_path.read_text(encoding="utf-8"))
            return _redacted_cookie_header(data.get("cookies", []))
        except (OSError, ValueError, TypeError):
            return ""

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "status": "ok",
                "state": self._state,
                "logged_in": bool(self.cookie_header()),
                "error": self._error,
                "elapsed_seconds": round(time.monotonic() - self._start_time)
                    if self._state == "waiting_scan" else None,
                "method": "official_browser_qr",
            }

    def start_login(self, backend: Any) -> dict[str, Any]:
        with self._lock:
            if self._state == "waiting_scan":
                return self.status()
            _browser_binary()  # fail immediately on hosts without a desktop browser
            self._backend = backend
            self._state = "waiting_scan"
            self._start_time = time.monotonic()
            self._error = ""
            threading.Thread(target=self._login_worker, daemon=True,
                             name="douyin-official-qr-login").start()
        return self.status()

    def _login_worker(self) -> None:
        proc: subprocess.Popen[bytes] | None = None
        cdp: _CDP | None = None
        try:
            self.profile_dir.mkdir(parents=True, exist_ok=True)
            if os.name != "nt":
                os.chmod(self.profile_dir, 0o700)
            port_file = self.profile_dir / "DevToolsActivePort"
            port_file.unlink(missing_ok=True)
            argv = [
                _browser_binary(), "--remote-debugging-address=127.0.0.1",
                "--remote-debugging-port=0",
                f"--user-data-dir={self.profile_dir}",
                "--no-first-run", "--no-default-browser-check",
                "--window-size=1100,800",
                "https://live.douyin.com/",
            ]
            proc = subprocess.Popen(
                argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            port = None
            for _ in range(100):
                if proc.poll() is not None:
                    raise RuntimeError("Chrome/Edge exited before login page loaded")
                if port_file.is_file():
                    try:
                        first = port_file.read_text(encoding="utf-8").splitlines()[0]
                        maybe_port = int(first)
                        if 0 < maybe_port <= 65535:
                            port = maybe_port
                            break
                    except (OSError, ValueError, IndexError):
                        pass
                time.sleep(0.2)
            if port is None:
                raise RuntimeError("Chrome/Edge DevTools startup timed out")
            cdp = _CDP(port)
            target = cdp.call("Target.createTarget", {"url": "https://live.douyin.com/"})
            attach = cdp.call("Target.attachToTarget",
                              {"targetId": target["targetId"], "flatten": True})
            sid = attach["sessionId"]
            cdp.call("Network.enable", session=sid)
            deadline = time.monotonic() + _MAX_LOGIN_SECONDS
            while time.monotonic() < deadline:
                if proc.poll() is not None:
                    raise RuntimeError("Login window closed before completing scan")
                all_cookies = cdp.call("Network.getAllCookies", session=sid).get("cookies", [])
                cookies = [dict(name=c["name"], value=c["value"], domain=c["domain"])
                           for c in all_cookies if _allowed_cookie(c)]
                names = {c["name"] for c in cookies if c["value"]}
                if "sessionid" in names or "sessionid_ss" in names:
                    _write_private_json(self.secret_path, {
                        "schema": 1, "cookies": cookies,
                        "updated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                    })
                    with self._lock:
                        self._state = "success"
                        self._error = ""
                    backend = self._backend
                    if backend is not None:
                        relay = getattr(backend, "danmu_relay", None)
                        if relay is not None and hasattr(relay, "request_reconnect"):
                            relay.request_reconnect()
                    return
                time.sleep(2)
            raise RuntimeError("QR scan timed out; retry login")
        except Exception as exc:
            with self._lock:
                self._state = "failed"
                # Do not include CDP/URL/cookie details from a low-level exception.
                msg = str(exc)
                self._error = msg if msg in {
                    "Chrome/Edge exited before login page loaded",
                    "Chrome/Edge DevTools startup timed out",
                    "Login window closed before completing scan",
                    "QR scan timed out; retry login",
                } else "Browser login failed; check Chrome/Edge and retry"
        finally:
            if cdp is not None:
                try:
                    cdp.call("Browser.close")
                except Exception:
                    pass
                try:
                    cdp.close()
                except Exception:
                    pass
            if proc is not None and proc.poll() is None:
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except Exception:
                    proc.kill()

    def fetch_params(self, raw_live_id: str, *, backend: Any, apply: bool = True) -> dict[str, Any]:
        from .douyin_protocol import fetch_douyin_live_info
        from .douyin_protocol import map_room_status
        live_id = str(raw_live_id or "").strip()
        if not live_id or len(live_id) > 512:
            raise ValueError("Douyin live room URL or web_rid required")
        cookie = self.cookie_header()
        if not cookie:
            raise ValueError("Login required: scan with Douyin App first")
        info = fetch_douyin_live_info(live_id, cookie=cookie)
        public = {
            "live_id": str(info.live_id),
            "room_id": str(info.room_id),
            "room_status": map_room_status(info.room_status),
            "title": str(info.room_title or "")[:160],
        }
        if apply:
            module = self.module
            current = module.load_config()
            updated = copy.deepcopy(current)
            dy = dict(updated.get("douyin", {}))
            dy["enabled"] = True
            dy["live_id"] = public["live_id"]
            live_info = dict(dy.get("live_info", {}))
            live_info.update({
                "room_id": str(info.room_id),
                "user_id": str(info.user_id),
                "user_unique_id": str(info.user_unique_id),
                "anchor_id": str(info.anchor_id),
            })
            # Keep raw ttwid, sessionid, msToken, sec_uid out of the new API
            # response; the fresh browser credentials remain private on disk.
            dy["live_info"] = live_info
            updated["douyin"] = dy
            archive = updated.get("platform_config_archive", {})
            slot = max(1, min(10, int(archive.get("active_slot", 1))))
            module.save_platform_config_slot(slot, module._build_platform_config_payload(updated))
            module.save_config(updated)
            backend.runtime_config = module.load_config()
            try:
                module._ensure_danmu_relay(backend, reconnect=True)
            except Exception:
                backend.logger.exception("Douyin login room reconnect failed")
        return {"status": "ok", "applied": bool(apply), "room": public}


def install_douyin_login(server_module: Any) -> bool:
    """Install local-only session routes after the existing security wrappers."""
    with _PATCH_LOCK:
        if getattr(server_module, "_douyin_login_installed", False):
            return True
        manager = DouyinLoginManager(server_module)
        server_module.douyin_login = manager
        relay_cls = server_module.douyin_protocol.DouyinDanmuRelay
        original_load_cfg = relay_cls._load_runtime_cfg

        @functools.wraps(original_load_cfg)
        def load_cfg_with_browser_cookie(self: Any) -> dict[str, Any]:
            data = original_load_cfg(self)
            if not data.get("cookie"):
                data["cookie"] = manager.cookie_header()
            return data

        relay_cls._load_runtime_cfg = load_cfg_with_browser_cookie
        handler = server_module.ApiHandler
        original_get, original_post = handler.do_GET, handler.do_POST

        @functools.wraps(original_get)
        def do_GET(self: Any) -> None:  # noqa: N802
            if urllib.parse.urlparse(self.path).path != STATUS_PATH:
                return original_get(self)
            if not self._require_loopback():
                return
            self._write_json(manager.status())

        @functools.wraps(original_post)
        def do_POST(self: Any) -> None:  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            if path not in {START_PATH, PARAMS_PATH}:
                return original_post(self)
            if not self._require_loopback():
                return
            from .server_runtime_guard import _same_host_origin
            site = str(self.headers.get("Sec-Fetch-Site", "")).lower()
            if site == "cross-site" or not _same_host_origin(self.headers):
                self._write_json({"status": "error", "message": "Invalid management origin"},
                                 status=HTTPStatus.FORBIDDEN)
                return
            media = str(self.headers.get("Content-Type", "")).split(";", 1)[0].strip().lower()
            if media != "application/json":
                self._write_json({"status": "error", "message": "JSON required"},
                                 status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
                return
            try:
                size = int(self.headers.get("Content-Length", "-1"))
                if not 0 < size <= 4096:
                    raise ValueError("Invalid request body size")
                body = json.loads(self.rfile.read(size).decode("utf-8"))
                if not isinstance(body, dict):
                    raise ValueError("Expected JSON object")
                if path == START_PATH:
                    payload = manager.start_login(self.server)
                else:
                    payload = manager.fetch_params(
                        body.get("live_id", ""), backend=self.server,
                        apply=body.get("apply", True) is True)
                self._write_json(payload)
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                self._write_json({"status": "error", "message": str(exc)},
                                 status=HTTPStatus.BAD_REQUEST)
            except RuntimeError as exc:
                self._write_json({"status": "error", "message": str(exc)},
                                 status=HTTPStatus.SERVICE_UNAVAILABLE)
            except Exception:
                self.server.logger.exception("Douyin local login operation failed")
                self._write_json({"status": "error", "message": "Douyin operation failed"},
                                 status=HTTPStatus.BAD_GATEWAY)

        handler.do_GET = do_GET
        handler.do_POST = do_POST
        server_module._douyin_login_installed = True
        return True


__all__ = ["DouyinLoginManager", "install_douyin_login", "START_PATH", "STATUS_PATH", "PARAMS_PATH"]
