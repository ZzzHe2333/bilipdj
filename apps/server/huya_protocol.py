from __future__ import annotations

import datetime as dt
import hashlib
import html
import json
import re
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

try:
    from .danmu_event import DanmuEvent
except ImportError:  # standalone compatibility
    from danmu_event import DanmuEvent

if __package__:
    from .huya_tars import TarsReader, TarsType, TarsWriter, tars_parse
else:
    from huya_tars import TarsReader, TarsType, TarsWriter, tars_parse

HUYA_WS_URL = "wss://cdnws.api.huya.com/"
HUYA_DESKTOP_URL = "https://www.huya.com/{slug}"
HUYA_MOBILE_URL = "https://m.huya.com/{slug}"
HUYA_PROFILE_API = "https://mp.huya.com/cache.php?m=Live&do=profileRoom&roomid={room_id}"
HUYA_LIVE_CACHE_API = "https://www.huya.com/cache.php?m=Live"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
)
DEFAULT_MOBILE_USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/152.0.0.0 Mobile Safari/537.36"
)
HEARTBEAT_INTERVAL_SECONDS = 55.0
DEFAULT_RECONNECT_DELAY_SECONDS = 2.0

# Public Huya web-client application heartbeat. It is protocol framing, not
# account authentication material.
HUYA_HEARTBEAT = bytes.fromhex(
    "00031d0000690000006910032c3c4c56086f6e6c696e657569660f4f6e557365724865617274426561747d"
    "00003c0800010604745265711d00002f0a0a0c1600260036076164725f77617046000b1203aef00f2203aef0"
    "0f3c426d5202605c60017c82000bb01f9cac0b8c980ca80c"
)


class HuyaProtocolError(RuntimeError):
    pass


@dataclass(slots=True)
class HuyaLiveInfo:
    room_url: str
    slug: str
    room_id: str
    anchor_uid: str
    anchor_nickname: str = ""
    room_title: str = ""
    live_status: str = "unknown"


@dataclass(slots=True)
class HuyaChatEvent:
    uid: int
    nickname: str
    content: str
    color: str = "#FFFFFF"
    recv_time: str = ""


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _stable_uid(text: str) -> int:
    digest = hashlib.blake2b(str(text or "").encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFF_FFFF_FFFF_FFFF


def _first_regex(text: str, patterns: list[str]) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match:
            return str(match.group(1) or "").strip()
    return ""


def _http_get_text(url: str, *, cookie: str = "", mobile: bool = False, timeout: float = 12.0) -> str:
    headers = {
        "User-Agent": DEFAULT_MOBILE_USER_AGENT if mobile else DEFAULT_USER_AGENT,
        "Referer": "https://www.huya.com/",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
        "Accept": "text/html,application/json;q=0.9,*/*;q=0.8",
        "Connection": "close",
    }
    if str(cookie or "").strip():
        headers["Cookie"] = str(cookie).strip()
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
    return raw.decode(charset, errors="replace")


def normalize_huya_target(raw_value: Any) -> tuple[str, str, str]:
    """Return canonical URL, URL slug and direct numeric room id if present."""
    text = str(raw_value or "").strip()
    if not text:
        return "", "", ""
    if text.isdigit():
        return HUYA_DESKTOP_URL.format(slug=text), text, text
    if "://" not in text:
        text = "https://www.huya.com/" + text if "/" not in text else "https://" + text.lstrip("/")
    parsed = urllib.parse.urlparse(text)
    if "huya.com" not in parsed.netloc.lower():
        raise HuyaProtocolError("Only huya.com room URLs/aliases are supported")
    parts = [urllib.parse.unquote(value) for value in parsed.path.split("/") if value]
    if not parts:
        raise HuyaProtocolError("Huya URL does not contain a room alias/id")
    slug = parts[0]
    canonical = HUYA_DESKTOP_URL.format(slug=urllib.parse.quote(slug, safe=""))
    return canonical, slug, slug if slug.isdigit() else ""


def _extract_page_ids(page: str) -> tuple[str, str]:
    room_id = _first_regex(page, [r'"lProfileRoom"\s*:\s*"?(\d+)"?', r'"profileRoom"\s*:\s*"?(\d+)"?'])
    anchor_uid = _first_regex(
        page,
        [r'"lUid"\s*:\s*"?(\d+)"?', r'"ayyuid"\s*:\s*"?(\d+)"?', r'"yyuid"\s*:\s*"?(\d+)"?'],
    )
    return room_id, anchor_uid


def _extract_title(page: str) -> str:
    value = _first_regex(page, [r"<title[^>]*>(.*?)</title>"])
    return re.sub(r"\s+", " ", html.unescape(value)).strip()


def _deep_pick(data: Any, paths: list[tuple[str, ...]]) -> str:
    for path in paths:
        current = data
        for key in path:
            if not isinstance(current, dict):
                current = None
                break
            current = current.get(key)
        value = str(current or "").strip()
        if value.isdigit() and int(value) > 0:
            return value
    return ""


def _pick_uid(data: Any) -> str:
    return _deep_pick(
        data,
        [
            ("roomInfo", "tProfileInfo", "lUid"), ("roomInfo", "tProfileInfo", "uid"),
            ("liveData", "uid"), ("liveData", "lUid"), ("profileInfo", "uid"),
            ("profileInfo", "lUid"), ("uid",), ("lUid",),
        ],
    )


def _pick_room(data: Any) -> str:
    return _deep_pick(
        data,
        [
            ("roomInfo", "tProfileInfo", "lProfileRoom"), ("roomInfo", "tProfileInfo", "profileRoom"),
            ("liveData", "profileRoom"), ("profileInfo", "profileRoom"),
            ("lProfileRoom",), ("profileRoom",), ("roomId",),
        ],
    )


def fetch_huya_live_info(
    target: Any = "",
    *,
    room_id: Any = "",
    anchor_uid: Any = "",
    cookie: str = "",
    timeout: float = 12.0,
) -> HuyaLiveInfo:
    """Resolve public Huya URL/alias; saved room/anchor IDs are manual fallbacks."""
    preset_room = str(room_id or "").strip()
    preset_uid = str(anchor_uid or "").strip()
    if str(target or "").strip():
        room_url, slug, direct_room = normalize_huya_target(target)
    elif preset_room:
        room_url, slug, direct_room = normalize_huya_target(preset_room)
    else:
        raise HuyaProtocolError("huya.room_url or huya.room_id is required")

    resolved_room = preset_room or direct_room
    resolved_uid = preset_uid
    nickname = ""
    title = ""
    live_status = "unknown"

    if not resolved_room or not resolved_uid:
        try:
            desktop = _http_get_text(room_url, cookie=cookie, timeout=timeout)
        except Exception:
            desktop = ""
        if desktop:
            page_room, page_uid = _extract_page_ids(desktop)
            resolved_room = resolved_room or page_room
            resolved_uid = resolved_uid or page_uid
            title = _extract_title(desktop)

    if not resolved_room or not resolved_uid:
        try:
            mobile = _http_get_text(
                HUYA_MOBILE_URL.format(slug=urllib.parse.quote(slug, safe="")),
                cookie=cookie,
                mobile=True,
                timeout=timeout,
            )
            page_room, page_uid = _extract_page_ids(mobile)
            resolved_room = resolved_room or page_room
            resolved_uid = resolved_uid or page_uid
        except Exception:
            pass

    if resolved_room:
        try:
            profile = json.loads(
                _http_get_text(
                    HUYA_PROFILE_API.format(room_id=urllib.parse.quote(resolved_room, safe="")),
                    cookie=cookie,
                    mobile=True,
                    timeout=timeout,
                )
            )
            data = profile.get("data") if isinstance(profile, dict) else None
            if isinstance(data, dict):
                resolved_uid = resolved_uid or _pick_uid(data)
                resolved_room = resolved_room or _pick_room(data)
                live_data = data.get("liveData") if isinstance(data.get("liveData"), dict) else {}
                profile_info = data.get("profileInfo") if isinstance(data.get("profileInfo"), dict) else {}
                nickname = str(live_data.get("nick") or profile_info.get("nick") or "").strip()
                raw_status = str(data.get("liveStatus") or live_data.get("liveStatus") or "").strip().upper()
                if raw_status in {"ON", "1", "LIVE"}:
                    live_status = "live"
                elif raw_status in {"OFF", "0", "REPLAY"}:
                    live_status = "offline"
        except Exception:
            pass

    # Useful for aliases such as /lpl. The cache title is deliberately ignored:
    # it is known to lag behind the current tournament page.
    if not resolved_room or not resolved_uid or not nickname:
        try:
            cache = json.loads(_http_get_text(HUYA_LIVE_CACHE_API, cookie=cookie, timeout=timeout))
            if isinstance(cache, list):
                for item in cache:
                    if not isinstance(item, dict):
                        continue
                    profile_room = str(item.get("profileRoom") or "").strip()
                    private_host = str(item.get("privateHost") or "").strip()
                    if private_host.casefold() != slug.casefold() and (not resolved_room or profile_room != resolved_room):
                        continue
                    if not resolved_room and profile_room.isdigit():
                        resolved_room = profile_room
                    cache_uid = str(item.get("uid") or "").strip()
                    if not resolved_uid and cache_uid.isdigit():
                        resolved_uid = cache_uid
                    nickname = nickname or str(item.get("nick") or "").strip()
                    try:
                        live_status = "live" if int(item.get("liveState", 0) or 0) == 1 else "offline"
                    except (TypeError, ValueError):
                        pass
                    break
        except Exception:
            pass

    if not resolved_room:
        raise HuyaProtocolError("Failed to resolve Huya room id; fill huya.room_id manually")
    if not resolved_uid:
        raise HuyaProtocolError("Failed to resolve Huya anchor uid; fill huya.anchor_id manually")
    return HuyaLiveInfo(room_url, slug, resolved_room, resolved_uid, nickname, title, live_status)


def make_huya_register_packet(anchor_uid: int) -> bytes:
    uid = int(anchor_uid)
    if uid <= 0:
        raise HuyaProtocolError("Huya anchor uid must be a positive integer")
    register = TarsWriter()
    register.write_string_list(0, [f"live:{uid}", f"chat:{uid}"])
    register.write_string(1, "")
    command = TarsWriter()
    command.write_int(0, 16)  # EWSCmd_RegisterGroupReq
    command.write_bytes(1, register.data())
    return command.data()


def decode_huya_chat_message(data: bytes) -> HuyaChatEvent | None:
    """Decode one binary frame; ordinary chat is WSPushMessage iUri=1400."""
    outer = tars_parse(data)
    if _to_int(outer.get(0), -1) != 7:
        return None
    push_raw = outer.get(1)
    if not isinstance(push_raw, (bytes, bytearray)):
        return None
    push = tars_parse(bytes(push_raw))
    if _to_int(push.get(1), -1) != 1400:
        return None
    notice_raw = push.get(2)
    if not isinstance(notice_raw, (bytes, bytearray)):
        return None
    notice = tars_parse(bytes(notice_raw))
    sender = notice.get(0)
    uid = 0
    nickname = ""
    if isinstance(sender, dict):
        uid = _to_int(sender.get(0), 0)
        nickname = str(sender.get(2) or "").strip()
    content = str(notice.get(3) or "")
    if not content:
        return None
    if uid <= 0:
        uid = _stable_uid(nickname or content[:64])
    color = 0xFFFFFF
    bullet = notice.get(6)
    if isinstance(bullet, dict):
        parsed = _to_int(bullet.get(0), -1)
        if parsed >= 0:
            color = parsed
    return HuyaChatEvent(
        uid=uid,
        nickname=nickname,
        content=content,
        color=f"#{color & 0xFFFFFF:06X}",
        recv_time=dt.datetime.now(dt.timezone.utc).isoformat(),
    )


def is_huya_system_chat(event: HuyaChatEvent) -> bool:
    nickname = str(event.nickname or "").strip().casefold()
    if nickname in {"系统消息", "系统提示", "system", "system message"}:
        return True
    content = str(event.content or "")
    return nickname.startswith("系统") and ("刚刚下单" in content or "下单了" in content)


class HuyaDanmuRelay(threading.Thread):
    platform = "huya"

    def __init__(self, server: Any) -> None:
        super().__init__(name="huya-danmu-relay", daemon=True)
        self.server = server
        self.logger = server.logger
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        self._status_lock = threading.Lock()
        self._ws_lock = threading.Lock()
        self._ws: Any = None
        self._connected = False
        self._last_packet_monotonic = 0.0
        self._last_packet_at = ""
        self._last_connect_at = ""
        self._last_disconnect_at = ""
        self._last_disconnect_reason = ""
        self._last_chat_seen_at = ""
        self._current_room_id = ""
        self._current_anchor_uid = ""
        self._current_room_url = ""
        self._current_anchor_nickname = ""
        self._current_room_title = ""
        self._current_live_status = "unknown"

    def stop(self) -> None:
        self._stop_event.set()
        self._reconnect_event.set()
        self._close_ws()

    def request_reconnect(self) -> None:
        self._reconnect_event.set()
        self._close_ws()

    def _close_ws(self) -> None:
        with self._ws_lock:
            ws = self._ws
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def _emit_status(self, status: str, **extra: Any) -> None:
        payload = {"type": "PDJ_STATUS", "status": status, "platform": "huya"}
        payload.update(extra)
        self.server.ws_hub.broadcast_json(None, payload)

    def _mark_packet(self) -> None:
        with self._status_lock:
            self._last_packet_monotonic = time.monotonic()
            self._last_packet_at = dt.datetime.now(dt.timezone.utc).isoformat()

    def _mark_connected(self, info: HuyaLiveInfo) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with self._status_lock:
            self._connected = True
            self._last_connect_at = now
            self._last_disconnect_reason = ""
            self._current_room_id = info.room_id
            self._current_anchor_uid = info.anchor_uid
            self._current_room_url = info.room_url
            self._current_anchor_nickname = info.anchor_nickname
            self._current_room_title = info.room_title
            self._current_live_status = info.live_status
        self._mark_packet()

    def _mark_disconnected(self, reason: str = "") -> None:
        with self._status_lock:
            self._connected = False
            self._last_disconnect_at = dt.datetime.now(dt.timezone.utc).isoformat()
            if reason:
                self._last_disconnect_reason = reason

    def get_runtime_status(self) -> dict[str, Any]:
        with self._status_lock:
            last_packet_monotonic = self._last_packet_monotonic
            return {
                "platform": "huya",
                "connected": self._connected,
                "last_packet_at": self._last_packet_at,
                "last_connect_at": self._last_connect_at,
                "last_disconnect_at": self._last_disconnect_at,
                "last_disconnect_reason": self._last_disconnect_reason,
                "idle_seconds": max(0.0, time.monotonic() - last_packet_monotonic) if last_packet_monotonic else None,
                "roomid": self._current_room_id,
                "room_url": self._current_room_url,
                "anchor_uid": self._current_anchor_uid,
                "anchor_nickname": self._current_anchor_nickname,
                "room_title": self._current_room_title,
                "live_status": self._current_live_status,
                "last_chat_seen_at": self._last_chat_seen_at,
                "host": "cdnws.api.huya.com",
                "port": 443,
                "transport": "wss",
            }

    def _load_runtime_cfg(self) -> dict[str, Any]:
        runtime = getattr(self.server, "runtime_config", {})
        if not isinstance(runtime, dict):
            runtime = {}
        huya = runtime.get("huya", {})
        if not isinstance(huya, dict):
            huya = {}
        extra = huya.get("extra", {})
        if not isinstance(extra, dict):
            extra = {}
        try:
            reconnect_delay = max(0.5, float(extra.get("reconnect_delay_seconds", DEFAULT_RECONNECT_DELAY_SECONDS)))
        except (TypeError, ValueError):
            reconnect_delay = DEFAULT_RECONNECT_DELAY_SECONDS
        return {
            "platform": str(runtime.get("platform", "bilibili") or "").strip().lower(),
            "enabled": bool(huya.get("enabled", True)),
            "room_id": str(huya.get("room_id", "") or "").strip(),
            "room_url": str(huya.get("room_url", "") or "").strip(),
            "anchor_uid": str(huya.get("anchor_id", "") or "").strip(),
            "cookie": str(huya.get("cookie", "") or "").strip(),
            "auto_reconnect": bool(extra.get("auto_reconnect", True)),
            "reconnect_delay_seconds": reconnect_delay,
        }

    def _sync_runtime_info(self, info: HuyaLiveInfo) -> None:
        runtime = getattr(self.server, "runtime_config", None)
        if not isinstance(runtime, dict):
            return
        huya = runtime.get("huya")
        if not isinstance(huya, dict):
            huya = {}
            runtime["huya"] = huya
        huya.update({"room_id": info.room_id, "room_url": info.room_url, "anchor_id": info.anchor_uid})

    def _forward_chat_event(self, event: HuyaChatEvent) -> None:
        if is_huya_system_chat(event):
            self.logger.debug("Huya system notice filtered: %s: %s", event.nickname, event.content[:120])
            return
        self.server.ws_hub.mark_message()
        self._last_chat_seen_at = event.recv_time
        self.server.ws_hub.broadcast_json(
            None,
            {
                "type": "HUYA_DANMU",
                "uid": int(event.uid),
                "nickname": event.nickname,
                "content": event.content,
                "color": event.color,
                "time": event.recv_time,
                "platform": "huya",
                "room_id": self._current_room_id,
            },
        )
        if hasattr(self.server, "queue_manager"):
            self.server.queue_manager.process_danmu_event(
                DanmuEvent(
                    platform="huya",
                    user_id=str(event.uid),
                    username=event.nickname,
                    content=event.content,
                    received_at=event.recv_time,
                    metadata={"numeric_uid": int(event.uid), "color": event.color, "room_id": self._current_room_id},
                )
            )

    @staticmethod
    def _send_binary(ws: Any, payload: bytes) -> None:
        import websocket
        ws.send(payload, opcode=websocket.ABNF.OPCODE_BINARY)

    def _heartbeat_loop(self, ws: Any) -> None:
        while not self._stop_event.wait(HEARTBEAT_INTERVAL_SECONDS):
            if self._reconnect_event.is_set():
                return
            with self._ws_lock:
                if ws is not self._ws:
                    return
            try:
                self._send_binary(ws, HUYA_HEARTBEAT)
            except Exception:
                return

    def _connect_once(self, cfg: dict[str, Any]) -> None:
        try:
            import websocket
        except ImportError as exc:
            raise HuyaProtocolError("websocket-client is required for Huya runtime") from exc
        if cfg["platform"] != "huya":
            self._emit_status("danmu_waiting_platform", message="platform is not huya")
            self._stop_event.wait(1.5)
            return
        if not cfg["enabled"]:
            self._emit_status("danmu_waiting_config", message="huya.enabled=false")
            self._stop_event.wait(2.0)
            return
        if not cfg["room_url"] and not cfg["room_id"]:
            self._emit_status("danmu_waiting_config", message="huya.room_url/room_id not configured")
            self._stop_event.wait(2.0)
            return

        info = fetch_huya_live_info(
            cfg["room_url"] or cfg["room_id"],
            room_id=cfg["room_id"],
            anchor_uid=cfg["anchor_uid"],
            cookie=cfg["cookie"],
        )
        self._sync_runtime_info(info)
        if hasattr(self.server, "queue_manager"):
            try:
                self.server.queue_manager.load_config(
                    getattr(self.server, "runtime_config", {}).get("myjs", {}),
                    anchor_uid=_to_int(info.anchor_uid, 0),
                )
            except Exception:
                pass

        def on_open(ws: Any) -> None:
            self._send_binary(ws, make_huya_register_packet(_to_int(info.anchor_uid, 0)))
            self._send_binary(ws, HUYA_HEARTBEAT)
            self._mark_connected(info)
            self._emit_status(
                "danmu_connected",
                roomid=info.room_id,
                room_url=info.room_url,
                anchor_uid=info.anchor_uid,
                anchor_nickname=info.anchor_nickname,
                live_status=info.live_status,
                host="cdnws.api.huya.com",
                port=443,
                transport="wss",
            )
            self.logger.info(
                "Huya connected room_id=%s anchor_uid=%s anchor=%s",
                info.room_id, info.anchor_uid, info.anchor_nickname or "",
            )
            threading.Thread(target=self._heartbeat_loop, args=(ws,), name="huya-heartbeat", daemon=True).start()

        def on_message(_ws: Any, message: Any) -> None:
            if not isinstance(message, (bytes, bytearray)):
                return
            self._mark_packet()
            try:
                event = decode_huya_chat_message(bytes(message))
            except Exception as exc:
                self.logger.debug("Ignore unknown Huya websocket frame: %s", exc)
                return
            if event is not None:
                self._forward_chat_event(event)

        def on_error(_ws: Any, error: Any) -> None:
            if not self._stop_event.is_set() and not self._reconnect_event.is_set():
                self.logger.warning("Huya websocket error: %s", error)

        def on_close(_ws: Any, status_code: Any, message: Any) -> None:
            self._mark_disconnected(f"websocket closed code={status_code or '-'} message={message or ''}".strip())

        ws = websocket.WebSocketApp(HUYA_WS_URL, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)
        with self._ws_lock:
            self._ws = ws
        try:
            ws.run_forever(ping_interval=30, ping_timeout=10, origin="https://www.huya.com")
        finally:
            with self._ws_lock:
                if self._ws is ws:
                    self._ws = None

    def run(self) -> None:
        while not self._stop_event.is_set():
            cfg = self._load_runtime_cfg()
            self._reconnect_event.clear()
            try:
                self._connect_once(cfg)
            except Exception as exc:
                self._mark_disconnected(str(exc))
                self._emit_status("danmu_disconnected", error=str(exc), platform="huya")
                self.logger.warning("Huya connection error: %s", exc)
            if self._stop_event.is_set() or not cfg.get("auto_reconnect", True):
                break
            self._stop_event.wait(float(cfg.get("reconnect_delay_seconds", DEFAULT_RECONNECT_DELAY_SECONDS)))


__all__ = [
    "HUYA_HEARTBEAT", "HUYA_WS_URL", "HuyaChatEvent", "HuyaDanmuRelay", "HuyaLiveInfo",
    "HuyaProtocolError", "TarsReader", "TarsType", "TarsWriter", "decode_huya_chat_message",
    "fetch_huya_live_info", "is_huya_system_chat", "make_huya_register_packet",
    "normalize_huya_target", "tars_parse",
]
