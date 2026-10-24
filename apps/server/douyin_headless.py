"""Optional headless Douyin WSS collector using the operator's local login profile.

No signatures or session cookies are exposed over HTTP. Frames are captured at
the browser level through the DevTools protocol (``Network.webSocketFrameReceived``)
and decoded by :mod:`apps.server.douyin_ws_frames`; page JavaScript is never
patched, so Douyin's own IM client stays untouched. Only one live room is
supported, matching the Python relay configuration.
"""
from __future__ import annotations

import base64
import json
import os
import subprocess
import threading
import time
import urllib.parse
from pathlib import Path
from typing import Any

from .douyin_login import _CDP, _browser_binary
from .douyin_ws_frames import FrameDecodeError, decode_frame_gifts, iter_messages

IM_READY_SECONDS = 60.0
MAX_IM_RELOADS = 3
IDLE_RECONNECT_SECONDS = 180.0
WARMUP_SECONDS = 8.0
MAX_TAPPED_FRAMES = 128
MAX_TAPPED_BUFFER_BYTES = 16 * 1024 * 1024
# Douyin serves a degraded page (logged-out UI, and often no IM socket at all)
# when the user agent literally says "HeadlessChrome". Present a normal Chrome
# UA so the logged-in profile is honoured inside the headless browser.
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"
)

_SAFE_ERRORS = frozenset({
    "Headless Chrome exited",
    "Headless Chrome startup timeout",
    "Douyin live page navigation failed",
    "Browser exited",
    "Douyin IM channel did not open",
})


def _safe_error(exc: BaseException) -> str:
    """Only whitelisted, non-sensitive reasons reach the status endpoint."""
    text = str(exc)
    return text if text in _SAFE_ERRORS else "Browser collector error; retrying locally"


def _kill_tree(proc: subprocess.Popen[bytes]) -> None:
    """Terminate Chrome *and its children*; a surviving child keeps the
    profile lock and every later launch then fails with "profile in use"."""
    if proc.poll() is None:
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except Exception:  # noqa: BLE001
            try:
                proc.kill()
            except Exception:  # noqa: BLE001
                pass
    if os.name == "nt":
        try:
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                           stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=8, check=False)
        except Exception:  # noqa: BLE001
            pass


def _release_profile(profile: Path) -> None:
    """Clear only an orphaned lock; never kill an unrelated browser process.

    Chrome's profile is also used by the interactive QR login window. A
    concurrent active browser must be rejected rather than forcibly killed,
    even when its lock file can be unlinked on the local filesystem.
    """
    lock = profile / "lockfile"
    try:
        import psutil
    except ImportError:
        if lock.exists():
            raise RuntimeError("Douyin browser profile appears in use")
        return
    target = str(profile.resolve()).casefold()
    for candidate in psutil.process_iter(["name", "cmdline"]):
        try:
            name = str(candidate.info.get("name") or "").casefold()
            if not name.startswith(("chrome", "msedge", "chromium")):
                continue
            argv = [str(a).casefold() for a in (candidate.info.get("cmdline") or [])]
            if any(a.startswith("--user-data-dir=") and
                   a.partition("=")[2].strip('"').casefold() == target for a in argv):
                raise RuntimeError("Douyin browser profile appears in use")
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue
    try:
        lock.unlink(missing_ok=True)
    except OSError as exc:
        raise RuntimeError("Douyin browser profile lock cannot be cleared") from exc

class _FrameTap:
    """Bounded buffer fed by ``_CDP`` protocol events."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._frames: list[bytes] = []
        self._buffered_bytes = 0
        self.frames = 0
        self.messages = 0
        self.gifts = 0
        self.methods: dict[str, int] = {}
        self.im_session_identified = False

    def __call__(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        if method == "Network.webSocketCreated":
            url = str((message.get("params") or {}).get("url") or "")
            if "/webcast/im/" in url:
                # Diagnostic only: the id tells a logged-in session (the
                # account's user_unique_id) from an anonymous one.
                try:
                    query = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
                    with self._lock:
                        self.im_session_identified = bool(
                            (query.get("user_unique_id") or [""])[0]
                        )
                except (ValueError, TypeError):
                    pass
            return
        if method != "Network.webSocketFrameReceived":
            return
        response = (message.get("params") or {}).get("response") or {}
        if response.get("opcode") != 2:
            return
        data = str(response.get("payloadData") or "")
        if not data:
            return
        try:
            raw = base64.b64decode(data, validate=False)
        except (ValueError, TypeError):
            return
        if not raw or len(raw) > 4 * 1024 * 1024:
            return
        with self._lock:
            self.frames += 1
            self._frames.append(raw)
            self._buffered_bytes += len(raw)
            while (len(self._frames) > MAX_TAPPED_FRAMES or
                   self._buffered_bytes > MAX_TAPPED_BUFFER_BYTES):
                old = self._frames.pop(0)
                self._buffered_bytes -= len(old)

    def drain(self) -> list[bytes]:
        with self._lock:
            frames, self._frames = self._frames, []
            self._buffered_bytes = 0
        return frames

    def note_methods(self, names: list[str]) -> None:
        if not names:
            return
        with self._lock:
            self.messages += len(names)
            for name in names:
                self.methods[name] = self.methods.get(name, 0) + 1

    def note_gifts(self, count: int) -> None:
        with self._lock:
            self.gifts += max(0, int(count))

    def top_methods(self, limit: int = 12) -> dict[str, int]:
        with self._lock:
            return dict(sorted(self.methods.items(), key=lambda kv: -kv[1])[:limit])


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
        self._rejected = 0
        self._frames = 0
        self._messages = 0
        self._page_seconds = 0.0
        self._last_frame_at = ""
        self._target_room = ""
        self._methods: dict[str, int] = {}
        self._im_session_identified = False
        self._browser_logged_in = False

    def status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "status": "ok", "state": self._state,
                "live_id": self._target_room,
                "observed": self._observed, "accepted": self._accepted,
                "rejected": self._rejected,
                "frames": self._frames, "messages": self._messages,
                "methods": dict(self._methods),
                "im_session_identified": self._im_session_identified,
                "browser_logged_in": self._browser_logged_in,
                "page_video_seconds": self._page_seconds,
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
            if not isinstance(info, dict) or not str(info.get("room_id", "")).isdigit() or int(info["room_id"]) <= 0:
                raise ValueError("Internal Douyin room_id is not configured")
            if getattr(self.module, "douyin_gift_bridge", None) is None:
                raise RuntimeError("Douyin gift bridge must be installed first")
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
                except Exception as exc:  # noqa: BLE001
                    with self._lock:
                        self._last_error = _safe_error(exc)
                        self._state = "reconnecting"
                    if self._stop.wait(4):
                        break
            with self._lock:
                self._state = "stopped"
        except Exception:
            with self._lock:
                self._state = "failed"
                self._last_error = "Headless collector stopped unexpectedly"

    def _accept(self, raw: Any, backend: Any) -> None:
        try:
            self.module.douyin_gift_bridge.handle(
                raw, getattr(backend, "runtime_config", {}), backend,
            )
            with self._lock:
                self._accepted += 1
        except (ValueError, TypeError):
            # Malformed/mismatched room frame; do not log private user fields
            # from the untrusted browser payload.
            with self._lock:
                self._rejected += 1

    def _drain(self, tap: _FrameTap, backend: Any) -> int:
        frames = tap.drain()
        if not frames:
            return 0
        names: list[str] = []
        gifts = 0
        for frame in frames:
            try:
                for method, _payload in iter_messages(frame):
                    names.append(method)
            except FrameDecodeError:
                continue
            for event in decode_frame_gifts(frame):
                gifts += 1
                self._accept(event, backend)
        tap.note_methods(names)
        tap.note_gifts(gifts)
        with self._lock:
            self._frames = tap.frames
            self._messages = tap.messages
            self._observed = tap.gifts
            self._methods = tap.top_methods()
            self._im_session_identified = tap.im_session_identified
        return len(names)

    def _warm_up(self, cdp: _CDP, session: str) -> None:
        """Load douyin.com first: the live room only opens its IM socket once the
        browser holds the visitor cookies (ttwid/msToken) the site sets there."""
        try:
            cdp.call("Page.navigate", {"url": "https://www.douyin.com/"}, session=session)
        except Exception:
            return
        deadline = time.monotonic() + WARMUP_SECONDS
        while time.monotonic() < deadline and not self._stop.is_set():
            if self._stop.wait(0.5):
                return
            try:
                result = cdp.call("Network.getAllCookies", {}, session=session)
            except Exception:
                continue
            names = {
                str(cookie.get("name"))
                for cookie in (result.get("cookies") or [])
                if isinstance(cookie, dict)
            }
            if "ttwid" in names:
                # Cookies are set; give the page a moment to settle.
                self._stop.wait(1.5)
                return

    @staticmethod
    def _has_session_cookie(cdp: _CDP, session: str) -> bool:
        """True when this browser profile really carries the Douyin login.

        Gift messages are only pushed to sessions Douyin considers signed in,
        so this is the first thing to check when the collector sees no gifts.
        """
        try:
            result = cdp.call("Network.getAllCookies", {}, session=session)
        except Exception:  # noqa: BLE001
            return False
        for cookie in result.get("cookies") or []:
            if not isinstance(cookie, dict):
                continue
            domain = str(cookie.get("domain", "")).lstrip(".").lower()
            if domain == "douyin.com" or domain.endswith(".douyin.com"):
                if cookie.get("name") in {"sessionid", "sessionid_ss"} and cookie.get("value"):
                    return True
        return False

    def _apply_session_cookies(self, cdp: _CDP, session: str) -> int:
        """Replay the stored Douyin session into the headless browser.

        The QR login captures cookies through CDP, which does not guarantee
        they are flushed into the Chrome profile on disk. Without this replay
        the headless page loads signed-out and Douyin never pushes gift
        messages. Values stay in this process; nothing is exposed over HTTP.
        """
        getter = getattr(self.login, "cookie_records", None)
        if not callable(getter):
            return 0
        try:
            records = getter()
        except Exception:  # noqa: BLE001
            return 0
        applied = 0
        for record in records:
            if not isinstance(record, dict):
                continue
            name = str(record.get("name") or "")
            value = str(record.get("value") or "")
            domain = str(record.get("domain") or "")
            if not name or not value or not domain:
                continue
            try:
                cdp.call("Network.setCookie", {
                    "name": name, "value": value, "domain": domain,
                    "path": "/", "secure": True,
                }, session=session)
                applied += 1
            except Exception:  # noqa: BLE001 - one bad cookie must not abort
                continue
        return applied

    def _run_once(self, backend: Any, live_id: str) -> None:
        proc: subprocess.Popen[bytes] | None = None
        cdp: _CDP | None = None
        tap = _FrameTap()
        # The login window uses the same profile. Never launch both at once.
        try:
            profile = self.login.profile_dir
            profile.mkdir(parents=True, exist_ok=True)
            if os.name != "nt":
                os.chmod(profile, 0o700)
            port_file = profile / "DevToolsActivePort"
            port_file.unlink(missing_ok=True)
            _release_profile(profile)
            proc = subprocess.Popen(
                [
                    _browser_binary(), "--remote-debugging-address=127.0.0.1",
                    "--remote-debugging-port=0",
                    f"--user-data-dir={profile}",
                    "--headless=new", "--disable-gpu",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-renderer-backgrounding",
                    # Headless Chrome pauses autoplay unless told otherwise; a
                    # paused player never opens the IM socket, so no gifts.
                    "--autoplay-policy=no-user-gesture-required",
                    f"--user-agent={_USER_AGENT}",
                    "--mute-audio",
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
            cdp = _CDP(port, event_sink=tap)
            target = cdp.call("Target.createTarget", {"url": "about:blank"})
            attached = cdp.call("Target.attachToTarget", {
                "targetId": target["targetId"], "flatten": True,
            })
            sid = attached["sessionId"]
            cdp.call("Page.enable", session=sid)
            cdp.call("Runtime.enable", session=sid)
            # Browser-level frame capture: no page JavaScript is patched.
            cdp.call("Network.enable", session=sid)
            with self._lock:
                self._browser_logged_in = self._has_session_cookie(cdp, sid)
            if not self._browser_logged_in:
                # Profile cookies are missing (CDP-captured login was never
                # flushed to disk): replay the stored session before any
                # navigation, then re-check.
                self._apply_session_cookies(cdp, sid)
                with self._lock:
                    self._browser_logged_in = self._has_session_cookie(cdp, sid)
            self._warm_up(cdp, sid)
            navigation = cdp.call("Page.navigate",
                                  {"url": f"https://live.douyin.com/{live_id}"}, session=sid)
            if navigation.get("errorText"):
                raise RuntimeError("Douyin live page navigation failed")
            with self._lock:
                self._state = "running"
                self._last_error = ""
            started = time.monotonic()
            last_im_at = started
            nav_at = started
            reloads = 0
            last_message_count = 0
            while not self._stop.wait(1):
                if proc.poll() is not None:
                    raise RuntimeError("Browser exited")
                self._drain(tap, backend)
                try:
                    probe = cdp.call("Runtime.evaluate", {
                        "expression": """(function () {
                          var v = document.querySelector('video');
                          return JSON.stringify({
                            t: document.title,
                            v: v ? Math.round(v.currentTime) : 0,
                          });
                        })()""",
                        "returnByValue": True,
                    }, session=sid)
                    page = json.loads(probe.get("result", {}).get("value", "{}"))
                except (ValueError, TypeError):
                    page = {}
                except Exception:
                    page = {}
                with self._lock:
                    self._page_seconds = float(page.get("v") or 0)
                if tap.messages > last_message_count:
                    last_message_count = tap.messages
                    last_im_at = time.monotonic()
                    nav_at = last_im_at
                    with self._lock:
                        self._last_frame_at = time.strftime(
                            "%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                elif tap.messages == 0 and time.monotonic() - nav_at > IM_READY_SECONDS:
                    # The live page sometimes declines to open its IM socket on
                    # the first load (its own client retries with backoff). Reload
                    # a few times before giving up instead of idling silently.
                    reloads += 1
                    if reloads > MAX_IM_RELOADS:
                        raise RuntimeError("Douyin IM channel did not open")
                    cdp.call("Page.navigate", {
                        "url": f"https://live.douyin.com/{live_id}",
                    }, session=sid)
                    nav_at = time.monotonic()
                elif tap.messages and time.monotonic() - last_im_at > IDLE_RECONNECT_SECONDS:
                    # A tab suspended by Chrome can remain open while its IM
                    # channel has died. Reconnect without losing the profile.
                    cdp.call("Page.navigate", {
                        "url": f"https://live.douyin.com/{live_id}",
                    }, session=sid)
                    last_im_at = time.monotonic()
                    last_message_count = tap.messages
                    nav_at = last_im_at
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
            if proc is not None:
                _kill_tree(proc)


__all__ = ["DouyinHeadlessCollector"]

