"""Optional headless Douyin WSS collector using the operator's local login profile.

No signatures or session cookies are exposed over HTTP. This collector uses
the existing gift-hook JavaScript and calls the #319 gift bridge directly.
Only one live room is supported, matching the Python relay configuration.
"""
from __future__ import annotations

import copy
import json
import os
import subprocess
import threading
import time
from pathlib import Path
from typing import Any

from .douyin_login import _CDP, _browser_binary


class DouyinHeadlessCollector:
    def __init__(self, module: Any, login: Any) -> None:
        self.module = module
        self.login = login
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = "stopped"
        self._last_error = ""
        self._observed = 0
        self._accepted = 0
        self._last_frame_at = ""
        self._target_room = ""

    def _hook_source(self) -> str:
        roots = (
            Path(getattr(self.module, "REPO_DIR", Path("."))),
            Path(getattr(self.module, "BUNDLE_DIR", Path("."))),
            Path(getattr(self.module, "APP_DIR", Path("."))),
        )
        rel = Path("tools") / "douyin" / "douyin_live_gift_hook.user.js"
        for root in roots:
            path = root / rel
            if path.is_file():
                raw = path.read_text(encoding="utf-8")
                # Internal CDP drain, not cross-origin messaging or a public API.
                hook = raw + """
;(function () {
  window.__bilipdjPendingGifts = [];
  window.addEventListener('douyin-gift', function (ev) {
    if (!ev.detail || ev.detail.type !== 'WebcastGiftMessage') return;
    window.__bilipdjPendingGifts.push(ev.detail);
    if (window.__bilipdjPendingGifts.length > 500)
      window.__bilipdjPendingGifts.splice(0, window.__bilipdjPendingGifts.length - 500);
  });
})();
"""
                return hook
        raise RuntimeError("Packaged Douyin WebSocket hook script is missing")

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "status": "ok", "state": self._state,
                "live_id": self._target_room,
                "observed": self._observed, "accepted": self._accepted,
                "last_error": self._last_error,
                "last_frame_at": self._last_frame_at,
            }

    def start(self, backend: Any) -> dict[str, Any]:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return self.status()
            if self.login.status()["state"] == "waiting_scan":
                raise ValueError("Close the QR login flow before starting headless collection")
            if not self.login.cookie_header():
                raise ValueError("Douyin QR login is required before headless collection")
            dy = getattr(backend, "runtime_config", {}).get("douyin", {})
            if not isinstance(dy, dict) or not dy.get("enabled"):
                raise ValueError("Enable Douyin and fetch live room parameters first")
            live_id = str(dy.get("live_id", "")).strip()
            if not live_id.isdigit() or not 5 <= len(live_id) <= 24:
                raise ValueError("Configured Douyin web_rid must be numeric")
            info = dy.get("live_info", {})
            if not isinstance(info, dict) or not str(info.get("room_id", "")).isdigit():
                raise ValueError("Internal Douyin room_id is not configured")
            if getattr(self.module, "douyin_gift_bridge", None) is None:
                raise RuntimeError("Douyin gift bridge must be installed first")
            self._hook_source()
            _browser_binary()
            self._stop.clear()
            self._last_error = ""
            self._target_room = live_id
            self._state = "starting"
            self._thread = threading.Thread(
                target=self._supervise, args=(backend, live_id),
                daemon=True, name="douyin-headless-gift-collector",
            )
            self._thread.start()
            return self.status()

    def stop(self) -> dict[str, Any]:
        with self._lock:
            self._stop.set()
            if self._state not in {"stopped", "failed"}:
                self._state = "stopping"
            return self.status()

    def wait_for_stop(self, seconds: float = 12.0) -> None:
        self.stop()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=seconds)

    def _supervise(self, backend: Any, live_id: str) -> None:
        try:
            while not self._stop.is_set():
                try:
                    self._run_once(backend, live_id)
                except Exception as exc:
                    with self._lock:
                        self._last_error = "Browser collector error; retrying locally"
                        self._state = "reconnecting"
                    if self._stop.wait(4):
                        break
            with self._lock:
                self._state = "stopped"
        except Exception:
            with self._lock:
                self._state = "failed"
                self._last_error = "Headless collector stopped unexpectedly"

    def _run_once(self, backend: Any, live_id: str) -> None:
        proc: subprocess.Popen[bytes] | None = None
        cdp: _CDP | None = None
        # The login window uses the same profile. Never launch both at once.
        try:
            profile = self.login.profile_dir
            profile.mkdir(parents=True, exist_ok=True)
            if os.name != "nt":
                os.chmod(profile, 0o700)
            port_file = profile / "DevToolsActivePort"
            port_file.unlink(missing_ok=True)
            proc = subprocess.Popen(
                [
                    _browser_binary(), "--remote-debugging-address=127.0.0.1",
                    "--remote-debugging-port=0",
                    f"--user-data-dir={profile}",
                    "--headless=new", "--disable-gpu",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    "--no-first-run", "--no-default-browser-check",
                    "about:blank",
                ],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            port = 0
            for _ in range(100):
                if self._stop.is_set():
                    return
                if proc.poll() is not None:
                    raise RuntimeError("Headless Chrome exited")
                if port_file.exists():
                    try:
                        port = int(port_file.read_text(encoding="utf-8").splitlines()[0])
                        if 0 < port <= 65535:
                            break
                    except (ValueError, IndexError, OSError):
                        pass
                time.sleep(0.2)
            if port < 1:
                raise RuntimeError("Headless Chrome startup timeout")
            cdp = _CDP(port)
            target = cdp.call("Target.createTarget", {"url": "about:blank"})
            attached = cdp.call("Target.attachToTarget", {
                "targetId": target["targetId"], "flatten": True,
            })
            sid = attached["sessionId"]
            cdp.call("Page.enable", session=sid)
            cdp.call("Runtime.enable", session=sid)
            cdp.call("Page.addScriptToEvaluateOnNewDocument",
                     {"source": self._hook_source()}, session=sid)
            cdp.call("Page.navigate",
                     {"url": f"https://live.douyin.com/{live_id}"}, session=sid)
            with self._lock:
                self._state = "running"
                self._last_error = ""
            tick = 0
            while not self._stop.wait(1):
                if proc.poll() is not None:
                    raise RuntimeError("Browser exited")
                result = cdp.call("Runtime.evaluate", {
                    "expression": """(function () {
                      var q = window.__bilipdjPendingGifts || [];
                      window.__bilipdjPendingGifts = [];
                      return JSON.stringify({
                        ready: !!window.__douyinGiftHookInstalled,
                        gifts: q, methods: window.__douyinMsgCounts || {}
                      });
                    })()""",
                    "returnByValue": True,
                }, session=sid)
                payload = json.loads(result.get("result", {}).get("value", "{}"))
                if not payload.get("ready"):
                    tick += 1
                    if tick > 30:
                        raise RuntimeError("WebSocket hook did not load")
                    continue
                received = payload.get("gifts", [])
                if not isinstance(received, list):
                    continue
                with self._lock:
                    self._observed += len(received)
                    if any(payload.get("methods", {}).values()):
                        self._last_frame_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                for raw in received:
                    try:
                        self.module.douyin_gift_bridge.handle(
                            raw, getattr(backend, "runtime_config", {}), backend,
                        )
                        with self._lock:
                            self._accepted += 1
                    except (ValueError, TypeError):
                        # Malformed/mismatched room frame; do not log private
                        # user fields from the untrusted browser payload.
                        pass
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


__all__ = ["DouyinHeadlessCollector"]
