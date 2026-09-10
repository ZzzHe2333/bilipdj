from __future__ import annotations

import datetime as dt
import hashlib
import random
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

TWITCH_WS_URL = "wss://irc-ws.chat.twitch.tv:443"
TWITCH_ROOM_URL = "https://www.twitch.tv/{channel}"
TWITCH_WS_HOST = "irc-ws.chat.twitch.tv"
DEFAULT_RECONNECT_DELAY_SECONDS = 2.0
DEFAULT_PROBE_TIMEOUT_SECONDS = 4.0


class TwitchProtocolError(RuntimeError):
    pass


@dataclass(slots=True)
class TwitchLiveInfo:
    room_url: str
    channel: str


@dataclass(slots=True)
class TwitchChatEvent:
    uid: int
    user_id: str
    login: str
    nickname: str
    content: str
    badges: str = ""
    color: str = ""
    emotes: str = ""
    message_id: str = ""
    is_mod: bool = False
    is_subscriber: bool = False
    first_msg: bool = False
    recv_time: str = ""


def _stable_uid(text: str) -> int:
    digest = hashlib.blake2b(str(text or "").encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFF_FFFF_FFFF_FFFF


def normalize_twitch_target(raw_value: Any) -> TwitchLiveInfo:
    text = str(raw_value or "").strip()
    if not text:
        raise TwitchProtocolError("twitch.room_url or twitch.room_id is required")

    if "://" not in text and "/" not in text:
        channel = text.lstrip("#").strip().lower()
    else:
        if "://" not in text:
            text = "https://" + text.lstrip("/")
        parsed = urllib.parse.urlparse(text)
        host = parsed.netloc.casefold()
        if not (host == "twitch.tv" or host.endswith(".twitch.tv")):
            raise TwitchProtocolError("Only twitch.tv channel URLs/names are supported")
        parts = [urllib.parse.unquote(value) for value in parsed.path.split("/") if value]
        if not parts:
            raise TwitchProtocolError("Twitch URL does not contain a channel name")
        channel = parts[0].lower()

    reserved = {
        "directory", "downloads", "jobs", "p", "search", "settings",
        "subscriptions", "turbo", "videos", "wallet",
    }
    if channel in reserved or not re.fullmatch(r"[a-z0-9_]{1,25}", channel, flags=re.IGNORECASE):
        raise TwitchProtocolError(f"Invalid Twitch channel name: {channel or '-'}")
    return TwitchLiveInfo(TWITCH_ROOM_URL.format(channel=channel), channel)


def canonical_room_url(raw_value: Any) -> str:
    return normalize_twitch_target(raw_value).room_url


def extract_channel(raw_value: Any) -> str:
    return normalize_twitch_target(raw_value).channel


def _decode_irc_tag_value(value: str) -> str:
    out: list[str] = []
    index = 0
    escapes = {"s": " ", ":": ";", "\\": "\\", "r": "\r", "n": "\n"}
    while index < len(value):
        ch = value[index]
        if ch == "\\" and index + 1 < len(value):
            index += 1
            out.append(escapes.get(value[index], value[index]))
        else:
            out.append(ch)
        index += 1
    return "".join(out)


def parse_irc_tags(raw: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in str(raw or "").split(";"):
        if not item:
            continue
        key, sep, value = item.partition("=")
        result[key] = _decode_irc_tag_value(value if sep else "")
    return result


def parse_twitch_privmsg(line: str) -> TwitchChatEvent | None:
    text = str(line or "").rstrip("\r\n")
    tags: dict[str, str] = {}
    rest = text
    if rest.startswith("@"):
        raw_tags, sep, remainder = rest.partition(" ")
        if not sep:
            return None
        tags = parse_irc_tags(raw_tags[1:])
        rest = remainder

    match = re.match(r":([^! ]+)!.*? PRIVMSG #([^ ]+) :(.*)$", rest, flags=re.DOTALL)
    if not match:
        return None

    login = match.group(1).strip()
    content = match.group(3)
    nickname = str(tags.get("display-name") or login).strip() or login
    user_id = str(tags.get("user-id") or "").strip()
    uid = int(user_id) if user_id.isdigit() else _stable_uid(login or nickname)
    return TwitchChatEvent(
        uid=uid,
        user_id=user_id,
        login=login,
        nickname=nickname,
        content=content,
        badges=str(tags.get("badges") or ""),
        color=str(tags.get("color") or ""),
        emotes=str(tags.get("emotes") or ""),
        message_id=str(tags.get("id") or ""),
        is_mod=str(tags.get("mod") or "0") == "1",
        is_subscriber=str(tags.get("subscriber") or "0") == "1",
        first_msg=str(tags.get("first-msg") or "0") == "1",
        recv_time=dt.datetime.now(dt.timezone.utc).isoformat(),
    )


def _parse_proxy_url(source: str, raw: str) -> dict[str, Any] | None:
    value = str(raw or "").strip()
    if not value:
        return None
    if "://" not in value:
        value = "http://" + value
    try:
        parsed = urllib.parse.urlparse(value)
    except Exception:
        return None
    if not parsed.hostname:
        return None

    scheme = (parsed.scheme or "http").casefold()
    if scheme in {"http", "https"}:
        proxy_type = "http"
        default_port = 8080
    elif scheme in {"socks5", "socks5h", "socks"}:
        proxy_type = "socks5h" if scheme == "socks5h" else "socks5"
        default_port = 1080
    elif scheme == "socks4":
        proxy_type = "socks4"
        default_port = 1080
    else:
        return None

    auth = None
    if parsed.username is not None:
        auth = (
            urllib.parse.unquote(parsed.username),
            urllib.parse.unquote(parsed.password or ""),
        )
    return {
        "source": source,
        "host": parsed.hostname,
        "port": parsed.port or default_port,
        "proxy_type": proxy_type,
        "auth": auth,
    }


def get_system_proxy() -> dict[str, Any] | None:
    """Return a websocket-client compatible system/environment proxy."""
    proxies = urllib.request.getproxies() or {}
    for key in ("https", "http", "all"):
        proxy = _parse_proxy_url(key, str(proxies.get(key) or ""))
        if proxy:
            return proxy
    return None


def _proxy_run_kwargs(proxy: dict[str, Any] | None) -> dict[str, Any]:
    if not proxy:
        return {}
    result: dict[str, Any] = {
        "http_proxy_host": proxy["host"],
        "http_proxy_port": int(proxy["port"]),
        "proxy_type": proxy["proxy_type"],
    }
    if proxy.get("auth"):
        result["http_proxy_auth"] = proxy["auth"]
    return result


def _proxy_summary(proxy: dict[str, Any] | None) -> str:
    if not proxy:
        return "direct"
    return f"{proxy.get('proxy_type', 'http')}://{proxy.get('host')}:{proxy.get('port')}"


def probe_twitch_access(timeout: float = DEFAULT_PROBE_TIMEOUT_SECONDS) -> bool:
    """Probe the same IRC-over-WebSocket transport used by the runtime."""
    try:
        import websocket
    except ImportError:
        return False

    proxy = get_system_proxy()
    conn = None
    try:
        conn = websocket.create_connection(
            TWITCH_WS_URL,
            timeout=max(1.0, float(timeout)),
            origin="https://www.twitch.tv",
            **_proxy_run_kwargs(proxy),
        )
        nick = f"justinfan{random.randint(10000, 99999)}"
        conn.send(f"PASS SCHMOOPIIE\r\nNICK {nick}\r\nCAP REQ :twitch.tv/tags twitch.tv/commands\r\n")
        deadline = time.monotonic() + max(0.5, float(timeout))
        while time.monotonic() < deadline:
            try:
                conn.settimeout(max(0.2, deadline - time.monotonic()))
                message = conn.recv()
            except Exception:
                break
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            text = str(message or "")
            if text.startswith("PING"):
                payload = text[4:].strip()
                conn.send(f"PONG {payload}\r\n")
                continue
            lowered = text.casefold()
            if "login authentication failed" in lowered or "improperly formatted auth" in lowered:
                return False
            if text.strip():
                return True
        # A successful websocket handshake is enough to prove reachability.
        return True
    except Exception:
        return False
    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


class TwitchDanmuRelay(threading.Thread):
    platform = "twitch"

    def __init__(self, server: Any) -> None:
        super().__init__(name="twitch-danmu-relay", daemon=True)
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
        self._current_channel = ""
        self._current_room_url = ""
        self._current_proxy = "direct"
        self._anonymous_nick = ""

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
        payload = {"type": "PDJ_STATUS", "status": status, "platform": "twitch"}
        payload.update(extra)
        self.server.ws_hub.broadcast_json(None, payload)

    def _mark_packet(self) -> None:
        with self._status_lock:
            self._last_packet_monotonic = time.monotonic()
            self._last_packet_at = dt.datetime.now(dt.timezone.utc).isoformat()

    def _mark_connected(self, info: TwitchLiveInfo, proxy: dict[str, Any] | None) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with self._status_lock:
            self._connected = True
            self._last_connect_at = now
            self._last_disconnect_reason = ""
            self._current_channel = info.channel
            self._current_room_url = info.room_url
            self._current_proxy = _proxy_summary(proxy)
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
                "platform": "twitch",
                "connected": self._connected,
                "last_packet_at": self._last_packet_at,
                "last_connect_at": self._last_connect_at,
                "last_disconnect_at": self._last_disconnect_at,
                "last_disconnect_reason": self._last_disconnect_reason,
                "idle_seconds": max(0.0, time.monotonic() - last_packet_monotonic) if last_packet_monotonic else None,
                "roomid": self._current_channel,
                "room_url": self._current_room_url,
                "last_chat_seen_at": self._last_chat_seen_at,
                "host": TWITCH_WS_HOST,
                "port": 443,
                "transport": "wss-irc",
                "proxy": self._current_proxy,
            }

    def _load_runtime_cfg(self) -> dict[str, Any]:
        runtime = getattr(self.server, "runtime_config", {})
        if not isinstance(runtime, dict):
            runtime = {}
        section = runtime.get("twitch", {})
        if not isinstance(section, dict):
            section = {}
        extra = section.get("extra", {})
        if not isinstance(extra, dict):
            extra = {}
        try:
            reconnect_delay = max(0.5, float(extra.get("reconnect_delay_seconds", DEFAULT_RECONNECT_DELAY_SECONDS)))
        except (TypeError, ValueError):
            reconnect_delay = DEFAULT_RECONNECT_DELAY_SECONDS
        return {
            "platform": str(runtime.get("platform", "bilibili") or "").strip().lower(),
            "enabled": bool(section.get("enabled", True)),
            "room_id": str(section.get("room_id", "") or "").strip(),
            "room_url": str(section.get("room_url", "") or "").strip(),
            "auto_reconnect": bool(extra.get("auto_reconnect", True)),
            "reconnect_delay_seconds": reconnect_delay,
        }

    def _sync_runtime_info(self, info: TwitchLiveInfo) -> None:
        runtime = getattr(self.server, "runtime_config", None)
        if not isinstance(runtime, dict):
            return
        section = runtime.get("twitch")
        if not isinstance(section, dict):
            section = {}
            runtime["twitch"] = section
        section.update({"room_id": info.channel, "room_url": info.room_url})

    def _forward_chat_event(self, event: TwitchChatEvent) -> None:
        self.server.ws_hub.mark_message()
        self._last_chat_seen_at = event.recv_time
        self.server.ws_hub.broadcast_json(
            None,
            {
                "type": "TWITCH_DANMU",
                "uid": int(event.uid),
                "user_id": event.user_id,
                "login": event.login,
                "nickname": event.nickname,
                "content": event.content,
                "badges": event.badges,
                "color": event.color,
                "emotes": event.emotes,
                "message_id": event.message_id,
                "is_mod": event.is_mod,
                "is_subscriber": event.is_subscriber,
                "first_msg": event.first_msg,
                "time": event.recv_time,
                "platform": "twitch",
                "room_id": self._current_channel,
            },
        )
        if hasattr(self.server, "queue_manager"):
            self.server.queue_manager.process_danmu_event(
                DanmuEvent(
                    platform="twitch",
                    user_id=event.user_id or str(event.uid),
                    username=event.nickname,
                    content=event.content,
                    is_room_admin=event.is_mod,
                    is_anchor="broadcaster/1" in event.badges,
                    received_at=event.recv_time,
                    metadata={
                        "numeric_uid": int(event.uid),
                        "login": event.login,
                        "badges": event.badges,
                        "color": event.color,
                        "emotes": event.emotes,
                        "message_id": event.message_id,
                        "is_subscriber": event.is_subscriber,
                        "first_msg": event.first_msg,
                        "room_id": self._current_channel,
                    },
                )
            )

    @staticmethod
    def _send_irc(ws: Any, text: str) -> None:
        ws.send(text if text.endswith("\r\n") else text + "\r\n")

    def _connect_once(self, cfg: dict[str, Any]) -> None:
        try:
            import websocket
        except ImportError as exc:
            raise TwitchProtocolError("websocket-client is required for purple-mouse runtime") from exc

        if cfg["platform"] != "twitch":
            self._emit_status("danmu_waiting_platform", message="platform is not twitch")
            self._stop_event.wait(1.5)
            return
        if not cfg["enabled"]:
            self._emit_status("danmu_waiting_config", message="twitch.enabled=false")
            self._stop_event.wait(2.0)
            return
        if not cfg["room_url"] and not cfg["room_id"]:
            self._emit_status("danmu_waiting_config", message="twitch.room_url/room_id not configured")
            self._stop_event.wait(2.0)
            return

        info = normalize_twitch_target(cfg["room_url"] or cfg["room_id"])
        self._sync_runtime_info(info)
        proxy = get_system_proxy()
        proxy_summary = _proxy_summary(proxy)
        self._anonymous_nick = f"justinfan{random.randint(10000, 99999)}"

        def on_open(ws: Any) -> None:
            self._send_irc(ws, "PASS SCHMOOPIIE")
            self._send_irc(ws, f"NICK {self._anonymous_nick}")
            self._send_irc(ws, "CAP REQ :twitch.tv/tags twitch.tv/commands")
            self._send_irc(ws, f"JOIN #{info.channel}")
            self.logger.info("Purple mouse websocket open channel=%s proxy=%s", info.channel, proxy_summary)

        def on_message(ws: Any, message: Any) -> None:
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            if not isinstance(message, str):
                return
            self._mark_packet()
            for raw_line in message.replace("\r\n", "\n").split("\n"):
                line = raw_line.rstrip("\r")
                if not line:
                    continue
                if line.startswith("PING"):
                    self._send_irc(ws, f"PONG {line[4:].strip()}")
                    continue
                if " RECONNECT" in line:
                    self.logger.info("Purple mouse requested reconnect")
                    self._reconnect_event.set()
                    try:
                        ws.close()
                    except Exception:
                        pass
                    continue
                if " ROOMSTATE #" in line:
                    if not self._connected:
                        self._mark_connected(info, proxy)
                        self._emit_status(
                            "danmu_connected",
                            roomid=info.channel,
                            room_url=info.room_url,
                            host=TWITCH_WS_HOST,
                            port=443,
                            transport="wss-irc",
                            proxy=proxy_summary,
                        )
                        self.logger.info("Purple mouse joined channel=%s", info.channel)
                    continue
                event = parse_twitch_privmsg(line)
                if event is not None:
                    if not self._connected:
                        self._mark_connected(info, proxy)
                    self._forward_chat_event(event)
                    continue
                lowered = line.casefold()
                if "login authentication failed" in lowered or "improperly formatted auth" in lowered:
                    raise TwitchProtocolError("anonymous Twitch IRC authentication rejected")

        def on_error(_ws: Any, error: Any) -> None:
            if not self._stop_event.is_set() and not self._reconnect_event.is_set():
                self.logger.warning("Purple mouse websocket error: %s", error)

        def on_close(_ws: Any, status_code: Any, message: Any) -> None:
            self._mark_disconnected(f"websocket closed code={status_code or '-'} message={message or ''}".strip())

        ws = websocket.WebSocketApp(
            TWITCH_WS_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close,
        )
        with self._ws_lock:
            self._ws = ws
        try:
            ws.run_forever(
                ping_interval=30,
                ping_timeout=10,
                origin="https://www.twitch.tv",
                **_proxy_run_kwargs(proxy),
            )
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
                self._emit_status("danmu_disconnected", error=str(exc), platform="twitch")
                self.logger.warning("Purple mouse connection error: %s", exc)
            if self._stop_event.is_set() or not cfg.get("auto_reconnect", True):
                break
            self._stop_event.wait(float(cfg.get("reconnect_delay_seconds", DEFAULT_RECONNECT_DELAY_SECONDS)))


__all__ = [
    "TWITCH_ROOM_URL", "TWITCH_WS_HOST", "TWITCH_WS_URL", "TwitchChatEvent", "TwitchDanmuRelay",
    "TwitchLiveInfo", "TwitchProtocolError", "canonical_room_url", "extract_channel", "get_system_proxy",
    "normalize_twitch_target", "parse_irc_tags", "parse_twitch_privmsg", "probe_twitch_access",
]
