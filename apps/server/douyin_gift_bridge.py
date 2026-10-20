"""Authenticated local bridge for Douyin browser WebSocket gift observations.

This is NOT a native Douyin client. It accepts data decoded by an explicitly
installed browser hook; the browser payload is untrusted and cannot establish
payment or purchase evidence.
"""
from __future__ import annotations

import datetime as dt
import hmac
import json
import os
import secrets
import threading
import time
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .danmu_event import DanmuEvent

API_PATH = "/api/douyin/gift-hook"
STATUS_PATH = "/api/douyin/gift-hook/status"
MAX_BYTES = 16 * 1024
COMBO_IDLE_SECONDS = 5.0
MESSAGE_TTL_SECONDS = 600.0
_ALLOWED_ORIGINS = {"https://live.douyin.com", "https://webcast.amemv.com"}


def _integer(value: Any, maximum: int = 1_000_000) -> int:
    if isinstance(value, bool):
        return 0
    try:
        result = int(value)
    except (ValueError, TypeError, OverflowError):
        return 0
    return result if 0 < result <= maximum else 0


def _string(value: Any, limit: int) -> str:
    result = str(value if value is not None else "").strip()
    if len(result) > limit:
        raise ValueError("field too long")
    return result


def _created_ms(value: Any) -> int:
    timestamp = _integer(value, 10**16)
    return timestamp * 1000 if timestamp and timestamp < 10**12 else timestamp


class GiftComboTracker:
    """Finish a combo exactly once; repeated counts are cumulative, not deltas."""

    def __init__(self, idle_seconds: float = COMBO_IDLE_SECONDS) -> None:
        self.idle_seconds = idle_seconds
        self._lock = threading.RLock()
        self._seen: dict[str, float] = {}
        self._pending: dict[tuple[str, ...], dict[str, Any]] = {}
        self._closed: dict[tuple[str, ...], tuple[int, int, float]] = {}

    def accept(self, raw: dict[str, Any], now: float | None = None) -> list[dict[str, Any]]:
        clock = time.monotonic() if now is None else now
        if not isinstance(raw, dict) or raw.get("type") != "WebcastGiftMessage":
            raise ValueError("unsupported message")
        msg_id = _string(raw.get("msgId"), 80)
        room = _string(raw.get("roomId"), 80)
        user = raw.get("user")
        gift = raw.get("gift")
        if not isinstance(user, dict) or not isinstance(gift, dict):
            raise ValueError("user/gift object required")
        uid = _string(user.get("id"), 80)
        name = _string(user.get("nickname"), 256)
        gift_id = _string(raw.get("giftId") or gift.get("id"), 80)
        gift_name = _string(gift.get("name"), 120)
        unit = _integer(gift.get("diamondCount"), 10_000_000)
        count = _integer(raw.get("repeatCount") or raw.get("sendCount"), 1_000_000)
        group = _string(raw.get("groupId") or "", 80)
        created = _created_ms(raw.get("createTime"))
        if not all((msg_id, room, uid, name, gift_id, gift_name, unit, count)):
            raise ValueError("incomplete gift message")
        key = (room, uid, gift_id, group)
        ended = raw.get("repeatEnd") == 1
        with self._lock:
            self._prune(clock)
            if msg_id in self._seen:
                return []
            self._seen[msg_id] = clock
            previous = self._closed.get(key)
            if previous and created and created <= previous[1] and count <= previous[0]:
                return []
            state = self._pending.get(key)
            if state is None:
                state = {
                    "room": room, "user_id": uid, "username": name,
                    "gift_id": gift_id, "gift_name": gift_name,
                    "unit_diamond": unit, "count": count,
                    "created_ms": created, "updated": clock,
                }
                self._pending[key] = state
            else:
                state["count"] = max(state["count"], count)
                state["created_ms"] = max(state["created_ms"], created)
                state["updated"] = clock
                state["username"] = name
            if ended:
                return [self._finish(key, clock)]
            return []

    def flush_due(self, now: float | None = None) -> list[dict[str, Any]]:
        clock = time.monotonic() if now is None else now
        with self._lock:
            self._prune(clock)
            keys = [key for key, value in self._pending.items()
                    if clock - value["updated"] >= self.idle_seconds]
            return [self._finish(key, clock) for key in keys]

    def _finish(self, key: tuple[str, ...], clock: float) -> dict[str, Any]:
        state = dict(self._pending.pop(key))
        self._closed[key] = (state["count"], state["created_ms"], clock)
        state.pop("updated", None)
        state["total_diamond"] = state["count"] * state["unit_diamond"]
        return state

    def _prune(self, clock: float) -> None:
        if len(self._seen) > 10000 or len(self._closed) > 10000:
            self._seen = {k: v for k, v in self._seen.items()
                          if clock - v < MESSAGE_TTL_SECONDS}
            self._closed = {k: v for k, v in self._closed.items()
                            if clock - v[2] < MESSAGE_TTL_SECONDS}


class DouyinGiftBridge:
    def __init__(self, server_module: Any, *, token: str | None = None,
                 combo_idle_seconds: float = COMBO_IDLE_SECONDS) -> None:
        self.server = server_module
        self._tracker = GiftComboTracker(combo_idle_seconds)
        self._lock = threading.RLock()
        self.received = 0
        self.settled = 0
        self.last_event_at = ""
        self.token_path = Path(getattr(server_module, "KEY_DIR", getattr(server_module, "_YAML_DIR"))) / "douyin-gift-bridge-token.txt"
        self.token = token if token is not None else self._load_token()
        self._stop = threading.Event()

    def _load_token(self) -> str:
        env = os.environ.get("BILIPDJ_DOUYIN_GIFT_TOKEN", "").strip()
        if len(env) >= 32:
            return env
        try:
            self.token_path.parent.mkdir(parents=True, exist_ok=True)
            if not self.token_path.exists():
                try:
                    fd = os.open(self.token_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                except FileExistsError:
                    pass
                else:
                    with os.fdopen(fd, "w", encoding="ascii") as stream:
                        stream.write(secrets.token_urlsafe(32))
            return self.token_path.read_text(encoding="ascii").strip()
        except OSError:
            return ""  # fail closed, no anonymous ingestion

    def authorize(self, header: str) -> bool:
        if len(self.token) < 32 or not header.startswith("Bearer "):
            return False
        return hmac.compare_digest(header[7:], self.token)

    @staticmethod
    def origin_allowed(header: str) -> bool:
        origin = str(header or "").strip().rstrip("/")
        # Tampermonkey GM_xmlhttpRequest may omit Origin; the Bearer token
        # is always required independently of the Origin header.
        return not origin or origin in _ALLOWED_ORIGINS or origin.startswith(
            ("chrome-extension://", "moz-extension://")
        )

    @staticmethod
    def expected_room(runtime: Any) -> str:
        if not isinstance(runtime, dict):
            return ""
        dy = runtime.get("douyin", {})
        if not isinstance(dy, dict) or not dy.get("enabled"):
            return ""
        info = dy.get("live_info", {})
        if not isinstance(info, dict):
            return ""
        return str(info.get("room_id") or "").strip()

    def handle(self, raw: dict[str, Any], runtime: Any) -> dict[str, Any]:
        expected = self.expected_room(runtime)
        if not expected or expected == "0":
            raise ValueError("Douyin enabled/live_info.room_id is required")
        if _string(raw.get("roomId"), 80) != expected:
            raise ValueError("roomId does not match configured Douyin room")
        finished = self._tracker.accept(raw)
        with self._lock:
            self.received += 1
            self.last_event_at = dt.datetime.now(dt.timezone.utc).isoformat()
        self._publish(finished)
        return {"status": "ok", "accepted": True, "settled": len(finished)}

    def _publish(self, combos: list[dict[str, Any]]) -> None:
        manager = getattr(self.server, "queue_manager", None)
        if manager is None or not callable(getattr(manager, "process_gift_event", None)):
            return
        # Browser events are not reliable payment proofs. Never grant credit
        # unless a local operator explicitly opts into this untrusted input.
        grant_credit = os.environ.get("BILIPDJ_DOUYIN_GIFT_QUEUE_CREDIT", "") == "1"
        for item in combos:
            uid = DanmuEvent(platform="douyin", user_id=item["user_id"],
                             username=item["username"], content="gift").legacy_uid()
            event = manager.process_gift_event({
                "platform": "douyin", "event": "gift",
                "uid": uid, "uname": item["username"],
                "room_id": item["room"],
                "grant_queue_credit": grant_credit,
                "gift": {
                    "id": item["gift_id"], "name": item["gift_name"],
                    "count": item["count"], "value": item["unit_diamond"],
                    "coin_type": "diamond",
                },
                "raw": {"source": "browser-wss-hook", "user_id": item["user_id"],
                        "total_diamond": item["total_diamond"]},
            })
            event["gift"]["value_unit"] = "diamond"
            with self._lock:
                self.settled += 1

    def start(self) -> None:
        worker = threading.Thread(target=self._run, daemon=True,
                                  name="douyin-gift-combo-flush")
        worker.start()

    def _run(self) -> None:
        while not self._stop.wait(1):
            try:
                self._publish(self._tracker.flush_due())
            except Exception:
                # Keep the daemon alive; emit diagnostics without dumping tokens.
                logger = getattr(self.server, "logger", None)
                if logger:
                    logger.exception("Douyin gift combo background flush failed")

    def status(self, runtime: Any) -> dict[str, Any]:
        with self._lock:
            return {
                "status": "ok", "enabled": bool(self.token),
                "configured_room_id": self.expected_room(runtime),
                "token_file": str(self.token_path) if self.token else "",
                "received": self.received, "settled": self.settled,
                "last_event_at": self.last_event_at,
                "queue_credit_enabled": os.environ.get("BILIPDJ_DOUYIN_GIFT_QUEUE_CREDIT", "") == "1",
                "transport": "browser-wss-hook",
            }


def install_douyin_gift_bridge(server_module: Any) -> bool:
    """Install outermost route handlers; other management routes are untouched."""
    if getattr(server_module, "_douyin_gift_bridge_installed", False):
        return True
    handler_class = getattr(server_module, "ApiHandler", None)
    if not isinstance(handler_class, type):
        return False
    service = DouyinGiftBridge(server_module)
    server_module.douyin_gift_bridge = service
    original_post, original_get = handler_class.do_POST, handler_class.do_GET

    def do_POST(self: Any) -> None:  # noqa: N802
        if urlparse(self.path).path != API_PATH:
            return original_post(self)
        if not self._require_loopback():
            return
        if not service.authorize(str(self.headers.get("Authorization", ""))):
            self._write_json({"status": "error", "message": "Invalid bridge token"},
                             status=HTTPStatus.UNAUTHORIZED)
            return
        if not service.origin_allowed(self.headers.get("Origin", "")):
            self._write_json({"status": "error", "message": "Invalid Origin"},
                             status=HTTPStatus.FORBIDDEN)
            return
        if str(self.headers.get("Content-Type", "")).split(";", 1)[0].strip().lower() != "application/json":
            self._write_json({"status": "error", "message": "JSON required"},
                             status=HTTPStatus.UNSUPPORTED_MEDIA_TYPE)
            return
        try:
            size = int(self.headers.get("Content-Length", "-1"))
            if not 0 < size <= MAX_BYTES:
                self._write_json({"status": "error", "message": "Invalid body size"},
                                 status=HTTPStatus.REQUEST_ENTITY_TOO_LARGE)
                return
            body = json.loads(self.rfile.read(size).decode("utf-8"))
            result = service.handle(body, getattr(self.server, "runtime_config", {}))
            self._write_json(result)
        except (ValueError, TypeError, UnicodeError, json.JSONDecodeError) as exc:
            self._write_json({"status": "error", "message": str(exc)},
                             status=HTTPStatus.BAD_REQUEST)
        except Exception:
            self.server.logger.exception("Douyin gift bridge dispatch failed")
            self._write_json({"status": "error", "message": "Internal bridge error"},
                             status=HTTPStatus.INTERNAL_SERVER_ERROR)

    def do_GET(self: Any) -> None:  # noqa: N802
        if urlparse(self.path).path != STATUS_PATH:
            return original_get(self)
        if not self._require_loopback():
            return
        self._write_json(service.status(getattr(self.server, "runtime_config", {})))

    handler_class.do_POST = do_POST
    handler_class.do_GET = do_GET
    server_module._douyin_gift_bridge_installed = True
    service.start()
    return True


__all__ = ["API_PATH", "STATUS_PATH", "DouyinGiftBridge",
           "GiftComboTracker", "install_douyin_gift_bridge"]
