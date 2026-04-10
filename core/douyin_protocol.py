from __future__ import annotations

import gzip
import html
import json
import logging
import re
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol, runtime_checkable

try:
    import websocket
except ImportError:  # pragma: no cover
    websocket = None

DOUYIN_LIVE_PAGE_URL = "https://live.douyin.com/{live_id}"
DOUYIN_WEBSOCKET_URL = "wss://webcast100-ws-web-hl.douyin.com/webcast/im/push/v2/"
DOUYIN_HEARTBEAT_INTERVAL_SECONDS = 5.0
DOUYIN_RECONNECT_DELAY_SECONDS = 2.0
WEBSOCKET_BINARY_OPCODE = 0x2

DEFAULT_DOUYIN_WS_QUERY: dict[str, str] = {
    "app_name": "douyin_web",
    "version_code": "180800",
    "webcast_sdk_version": "1.0.15",
    "update_version_code": "1.0.15",
    "compress": "gzip",
    "device_platform": "web",
    "cookie_enabled": "true",
    "screen_width": "1920",
    "screen_height": "1080",
    "browser_language": "zh-CN",
    "browser_platform": "Win32",
    "browser_name": "Mozilla",
    "browser_version": "5.0 (Windows NT 10.0; Win64; x64)",
    "browser_online": "true",
    "tz_name": "Etc/GMT-8",
    "host": "https://live.douyin.com",
    "aid": "6383",
    "live_id": "1",
    "did_rule": "3",
    "endpoint": "live_pc",
    "support_wrds": "1",
    "im_path": "/webcast/im/fetch/",
    "identity": "audience",
    "need_persist_msg_count": "15",
    "insert_task_id": "",
    "live_reason": "",
    "heartbeatDuration": "0",
}


class DouyinProtocolError(RuntimeError):
    pass


class DouyinCodecError(DouyinProtocolError):
    pass


@dataclass(slots=True)
class DouyinLiveInfo:
    live_id: str
    room_id: str
    user_id: str
    user_unique_id: str = ""
    anchor_id: str = ""
    sec_uid: str = ""
    ttwid: str = ""
    room_status: str = ""
    room_title: str = ""
    raw_html: str = ""


@dataclass(slots=True)
class DouyinBootstrapContext:
    cursor: str
    internal_ext: str


@dataclass(slots=True)
class DouyinPushFrame:
    payload: bytes
    log_id: int | str = 0
    payload_type: str = ""


@dataclass(slots=True)
class DouyinMessageEnvelope:
    method: str
    payload: bytes


@dataclass(slots=True)
class DouyinLiveResponsePayload:
    need_ack: bool
    internal_ext: str
    messages: list[DouyinMessageEnvelope] = field(default_factory=list)


@dataclass(slots=True)
class DouyinDanmuEvent:
    method: str
    event_type: str
    user_sec_uid: str = ""
    user_nickname: str = ""
    content: str = ""
    target_sec_uid: str = ""
    target_nickname: str = ""
    gift_name: str = ""
    combo_count: int = 0
    like_count: int = 0
    like_total: int = 0
    action: int = 0
    room_stats_text: str = ""
    raw: Any = None


@runtime_checkable
class DouyinSignatureProvider(Protocol):
    def generate_signature(self, live_info: DouyinLiveInfo) -> str: ...


@runtime_checkable
class DouyinBootstrapProvider(Protocol):
    def fetch_bootstrap(
        self,
        *,
        live_info: DouyinLiveInfo,
        cookie: str,
        headers: dict[str, str],
    ) -> DouyinBootstrapContext: ...


@runtime_checkable
class DouyinProtoCodec(Protocol):
    def parse_push_frame(self, payload: bytes) -> DouyinPushFrame: ...

    def parse_live_response(self, payload: bytes) -> DouyinLiveResponsePayload: ...

    def parse_message(self, method: str, payload: bytes) -> Any: ...

    def build_ack_frame(self, *, internal_ext: str, log_id: int | str) -> bytes: ...

    def build_heartbeat_frame(self) -> bytes: ...


@dataclass(slots=True)
class StaticSignatureProvider:
    signature: str

    def generate_signature(self, live_info: DouyinLiveInfo) -> str:
        return str(self.signature or "").strip()


@dataclass(slots=True)
class StaticBootstrapProvider:
    cursor: str
    internal_ext: str

    def fetch_bootstrap(
        self,
        *,
        live_info: DouyinLiveInfo,
        cookie: str,
        headers: dict[str, str],
    ) -> DouyinBootstrapContext:
        return DouyinBootstrapContext(cursor=str(self.cursor), internal_ext=str(self.internal_ext))


class UnsupportedDouyinProtoCodec:
    def _raise(self, action: str) -> None:
        raise DouyinCodecError(
            f"Douyin protobuf codec is not configured: {action}. "
            "You need to provide a codec that can parse PushFrame/LiveResponse "
            "and build heartbeat/ack frames."
        )

    def parse_push_frame(self, payload: bytes) -> DouyinPushFrame:
        self._raise("parse_push_frame")

    def parse_live_response(self, payload: bytes) -> DouyinLiveResponsePayload:
        self._raise("parse_live_response")

    def parse_message(self, method: str, payload: bytes) -> Any:
        self._raise(f"parse_message({method})")

    def build_ack_frame(self, *, internal_ext: str, log_id: int | str) -> bytes:
        self._raise("build_ack_frame")

    def build_heartbeat_frame(self) -> bytes:
        self._raise("build_heartbeat_frame")


def _decode_json_style_string(value: str) -> str:
    text = html.unescape(str(value or ""))
    try:
        return json.loads(f'"{text}"')
    except json.JSONDecodeError:
        return text


def _search_patterns(text: str, patterns: list[str]) -> str:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        value = match.group("value") if "value" in match.groupdict() else match.group(1)
        return _decode_json_style_string(value.strip())
    return ""


def _extract_cookie_string(set_cookie_headers: list[str]) -> str:
    cookie_pairs: list[str] = []
    for header in set_cookie_headers:
        part = str(header).split(";", 1)[0].strip()
        if "=" not in part:
            continue
        cookie_pairs.append(part)
    return "; ".join(cookie_pairs)


def extract_cookie_value(cookie_text: str, name: str) -> str:
    target = f"{name}="
    for item in str(cookie_text or "").split(";"):
        part = item.strip()
        if part.startswith(target):
            return part[len(target) :].strip()
    return ""


def merge_cookie_strings(*cookie_strings: str) -> str:
    merged: dict[str, str] = {}
    for cookie_text in cookie_strings:
        for item in str(cookie_text or "").split(";"):
            part = item.strip()
            if not part or "=" not in part:
                continue
            key, value = part.split("=", 1)
            merged[key.strip()] = value.strip()
    return "; ".join(f"{key}={value}" for key, value in merged.items())


def build_douyin_live_headers(cookie: str = "", *, referer: str = "https://live.douyin.com/") -> dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer,
        "Origin": "https://live.douyin.com",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def parse_douyin_live_info_html(html_text: str, live_id: str) -> DouyinLiveInfo:
    room_title = _search_patterns(
        html_text,
        [
            r'"title"\s*:\s*"(?P<value>(?:[^"\\]|\\.)*)"',
            r"<title>(?P<value>.*?)</title>",
        ],
    )
    info = DouyinLiveInfo(
        live_id=str(live_id),
        room_id=_search_patterns(html_text, [r'"roomId"\s*:\s*"?(?P<value>\d+)"?', r'"room_id"\s*:\s*"?(?P<value>\d+)"?']),
        user_id=_search_patterns(html_text, [r'"userId"\s*:\s*"?(?P<value>\d+)"?', r'"user_id"\s*:\s*"?(?P<value>\d+)"?']),
        user_unique_id=_search_patterns(
            html_text,
            [r'"user_unique_id"\s*:\s*"(?P<value>[^"]+)"', r'"userUniqueId"\s*:\s*"(?P<value>[^"]+)"'],
        ),
        anchor_id=_search_patterns(html_text, [r'"anchorId"\s*:\s*"?(?P<value>\d+)"?', r'"anchor_id"\s*:\s*"?(?P<value>\d+)"?']),
        sec_uid=_search_patterns(html_text, [r'"secUid"\s*:\s*"(?P<value>[^"]+)"', r'"sec_uid"\s*:\s*"(?P<value>[^"]+)"']),
        ttwid=_search_patterns(html_text, [r'"ttwid"\s*:\s*"(?P<value>[^"]+)"']),
        room_status=_search_patterns(
            html_text,
            [r'"roomStatus"\s*:\s*"?(?P<value>\d+)"?', r'"room_status"\s*:\s*"?(?P<value>\d+)"?'],
        ),
        room_title=room_title,
        raw_html=html_text,
    )
    if not info.room_id or not info.user_id:
        raise DouyinProtocolError("Failed to extract room_id or user_id from Douyin live page HTML")
    return info


def fetch_douyin_live_info(live_id: str, *, cookie: str = "", timeout: float = 10.0) -> DouyinLiveInfo:
    referer = "https://live.douyin.com/"
    url = DOUYIN_LIVE_PAGE_URL.format(live_id=urllib.parse.quote(str(live_id), safe=""))
    req = urllib.request.Request(url, headers=build_douyin_live_headers(cookie, referer=referer))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        html_text = resp.read().decode("utf-8", errors="replace")
        set_cookie_headers = resp.headers.get_all("Set-Cookie") or []
    info = parse_douyin_live_info_html(html_text, str(live_id))
    merged_cookie = merge_cookie_strings(cookie, _extract_cookie_string(set_cookie_headers))
    if not info.ttwid:
        info.ttwid = extract_cookie_value(merged_cookie, "ttwid")
    return info


def build_douyin_websocket_url(
    *,
    live_info: DouyinLiveInfo,
    bootstrap: DouyinBootstrapContext,
    signature: str,
    extra_query: dict[str, Any] | None = None,
) -> str:
    query = dict(DEFAULT_DOUYIN_WS_QUERY)
    query.update(
        {
            "room_id": live_info.room_id,
            "user_unique_id": live_info.user_unique_id,
            "cursor": bootstrap.cursor,
            "internal_ext": bootstrap.internal_ext,
            "signature": str(signature or "").strip(),
        }
    )
    if extra_query:
        query.update({str(key): "" if value is None else str(value) for key, value in extra_query.items()})
    if not query["signature"]:
        raise DouyinProtocolError("Empty Douyin websocket signature")
    return f"{DOUYIN_WEBSOCKET_URL}?{urllib.parse.urlencode(query)}"


def _read_path(data: Any, path: str, default: Any = "") -> Any:
    current = data
    for part in path.split("."):
        if current is None:
            return default
        if isinstance(current, dict):
            current = current.get(part, default)
            continue
        current = getattr(current, part, default)
    return current if current is not None else default


def normalize_douyin_message(method: str, message: Any) -> DouyinDanmuEvent:
    event = DouyinDanmuEvent(method=method, event_type="unknown", raw=message)
    if method == "WebcastChatMessage":
        event.event_type = "chat"
        event.user_sec_uid = str(_read_path(message, "user.sec_uid"))
        event.user_nickname = str(_read_path(message, "user.nickname"))
        event.content = str(_read_path(message, "content"))
        return event
    if method == "WebcastGiftMessage":
        event.event_type = "gift"
        event.user_sec_uid = str(_read_path(message, "user.sec_uid"))
        event.user_nickname = str(_read_path(message, "user.nickname"))
        event.target_sec_uid = str(_read_path(message, "toUser.sec_uid"))
        event.target_nickname = str(_read_path(message, "toUser.nickname"))
        event.gift_name = str(_read_path(message, "gift.name"))
        try:
            event.combo_count = int(_read_path(message, "comboCount", 0) or 0)
        except (TypeError, ValueError):
            event.combo_count = 0
        return event
    if method == "WebcastMemberMessage":
        event.event_type = "member"
        event.user_sec_uid = str(_read_path(message, "user.sec_uid"))
        event.user_nickname = str(_read_path(message, "user.nickname"))
        return event
    if method == "WebcastLikeMessage":
        event.event_type = "like"
        event.user_sec_uid = str(_read_path(message, "user.sec_uid"))
        event.user_nickname = str(_read_path(message, "user.nickname"))
        try:
            event.like_count = int(_read_path(message, "count", 0) or 0)
        except (TypeError, ValueError):
            event.like_count = 0
        try:
            event.like_total = int(_read_path(message, "total", 0) or 0)
        except (TypeError, ValueError):
            event.like_total = 0
        return event
    if method == "WebcastSocialMessage":
        try:
            event.action = int(_read_path(message, "action", 0) or 0)
        except (TypeError, ValueError):
            event.action = 0
        event.event_type = "follow" if event.action == 1 else "social"
        event.user_sec_uid = str(_read_path(message, "user.sec_uid"))
        event.user_nickname = str(_read_path(message, "user.nickname"))
        return event
    if method == "WebcastRoomStatsMessage":
        event.event_type = "room_stats"
        event.room_stats_text = str(_read_path(message, "displayLong"))
        return event
    return event


class DouyinDanmuListener(threading.Thread):
    def __init__(
        self,
        *,
        live_id: str,
        cookie: str,
        signature_provider: DouyinSignatureProvider | Callable[[DouyinLiveInfo], str],
        bootstrap_provider: DouyinBootstrapProvider
        | Callable[[DouyinLiveInfo, str, dict[str, str]], DouyinBootstrapContext],
        proto_codec: DouyinProtoCodec | None = None,
        logger: logging.Logger | None = None,
        message_callback: Callable[[DouyinDanmuEvent], None] | None = None,
        status_callback: Callable[[str, dict[str, Any]], None] | None = None,
        extra_query: dict[str, Any] | None = None,
        heartbeat_interval: float = DOUYIN_HEARTBEAT_INTERVAL_SECONDS,
        reconnect_delay: float = DOUYIN_RECONNECT_DELAY_SECONDS,
        auto_reconnect: bool = True,
    ) -> None:
        super().__init__(name="douyin-danmu-listener", daemon=True)
        self.live_id = str(live_id)
        self.cookie = str(cookie or "").strip()
        self.signature_provider = signature_provider
        self.bootstrap_provider = bootstrap_provider
        self.proto_codec = proto_codec or UnsupportedDouyinProtoCodec()
        self.logger = logger or logging.getLogger(__name__)
        self.message_callback = message_callback
        self.status_callback = status_callback
        self.extra_query = dict(extra_query or {})
        self.heartbeat_interval = max(1.0, float(heartbeat_interval))
        self.reconnect_delay = max(0.5, float(reconnect_delay))
        self.auto_reconnect = bool(auto_reconnect)
        self.live_info: DouyinLiveInfo | None = None
        self.bootstrap: DouyinBootstrapContext | None = None
        self.websocket_url: str = ""
        self._stop_event = threading.Event()
        self._ws: Any = None

    def _emit_status(self, status: str, **extra: Any) -> None:
        if self.status_callback is not None:
            self.status_callback(status, extra)

    def _resolve_signature(self, live_info: DouyinLiveInfo) -> str:
        provider = self.signature_provider
        if callable(provider) and not isinstance(provider, DouyinSignatureProvider):
            return str(provider(live_info) or "").strip()
        return str(provider.generate_signature(live_info) or "").strip()

    def _resolve_bootstrap(self, live_info: DouyinLiveInfo) -> DouyinBootstrapContext:
        headers = build_douyin_live_headers(
            self.cookie,
            referer=DOUYIN_LIVE_PAGE_URL.format(live_id=urllib.parse.quote(self.live_id, safe="")),
        )
        provider = self.bootstrap_provider
        if callable(provider) and not isinstance(provider, DouyinBootstrapProvider):
            result = provider(live_info, self.cookie, headers)
        else:
            result = provider.fetch_bootstrap(live_info=live_info, cookie=self.cookie, headers=headers)
        if not result.cursor or not result.internal_ext:
            raise DouyinProtocolError("Bootstrap provider returned empty cursor/internal_ext")
        return result

    def prepare(self) -> tuple[DouyinLiveInfo, DouyinBootstrapContext, str]:
        self.live_info = fetch_douyin_live_info(self.live_id, cookie=self.cookie)
        self.bootstrap = self._resolve_bootstrap(self.live_info)
        signature = self._resolve_signature(self.live_info)
        self.websocket_url = build_douyin_websocket_url(
            live_info=self.live_info,
            bootstrap=self.bootstrap,
            signature=signature,
            extra_query=self.extra_query,
        )
        return self.live_info, self.bootstrap, self.websocket_url

    def stop(self) -> None:
        self._stop_event.set()
        if self._ws is not None:
            try:
                self._ws.close()
            except Exception:  # noqa: BLE001
                pass

    def _ensure_websocket_dependency(self) -> None:
        if websocket is None:
            raise ModuleNotFoundError(
                "websocket-client is required for DouyinDanmuListener. "
                "Install it with: pip install websocket-client"
            )

    def _open_connection(self) -> Any:
        self._ensure_websocket_dependency()
        if not self.websocket_url:
            self.prepare()
        header_map = build_douyin_live_headers(
            self.cookie,
            referer=DOUYIN_LIVE_PAGE_URL.format(live_id=urllib.parse.quote(self.live_id, safe="")),
        )
        headers = [f"{key}: {value}" for key, value in header_map.items() if key.lower() != "cookie"]
        ws = websocket.create_connection(
            self.websocket_url,
            timeout=10,
            header=headers,
            cookie=self.cookie or None,
            enable_multithread=True,
        )
        ws.settimeout(1.0)
        return ws

    def _send_binary(self, payload: bytes) -> None:
        if self._ws is None:
            return
        self._ws.send(payload, opcode=WEBSOCKET_BINARY_OPCODE)

    def _handle_binary_message(self, payload: bytes) -> None:
        push_frame = self.proto_codec.parse_push_frame(payload)
        response_bytes = push_frame.payload
        try:
            response_bytes = gzip.decompress(response_bytes)
        except OSError:
            pass
        live_response = self.proto_codec.parse_live_response(response_bytes)
        if live_response.need_ack:
            ack_frame = self.proto_codec.build_ack_frame(
                internal_ext=live_response.internal_ext,
                log_id=push_frame.log_id,
            )
            self._send_binary(ack_frame)
        for item in live_response.messages:
            decoded_message = self.proto_codec.parse_message(item.method, item.payload)
            event = normalize_douyin_message(item.method, decoded_message)
            if self.message_callback is not None:
                self.message_callback(event)

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._ws = self._open_connection()
                self._emit_status("connected", url=self.websocket_url)
                next_heartbeat = time.monotonic() + self.heartbeat_interval
                while not self._stop_event.is_set():
                    if time.monotonic() >= next_heartbeat:
                        self._send_binary(self.proto_codec.build_heartbeat_frame())
                        next_heartbeat = time.monotonic() + self.heartbeat_interval
                    try:
                        payload = self._ws.recv()
                    except Exception as exc:  # noqa: BLE001
                        timeout_exc = getattr(websocket, "WebSocketTimeoutException", None) if websocket is not None else None
                        if timeout_exc is not None and isinstance(exc, timeout_exc):
                            continue
                        raise
                    if payload in {None, "", b""}:
                        raise DouyinProtocolError("Douyin websocket closed")
                    frame_bytes = payload if isinstance(payload, bytes) else str(payload).encode("utf-8", errors="replace")
                    self._handle_binary_message(frame_bytes)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Douyin danmu listener error: %s", exc)
                self._emit_status("disconnected", error=str(exc))
                if not self.auto_reconnect or self._stop_event.is_set():
                    break
                time.sleep(self.reconnect_delay)
            finally:
                if self._ws is not None:
                    try:
                        self._ws.close()
                    except Exception:  # noqa: BLE001
                        pass
                    self._ws = None


__all__ = [
    "DOUYIN_HEARTBEAT_INTERVAL_SECONDS",
    "DOUYIN_LIVE_PAGE_URL",
    "DOUYIN_WEBSOCKET_URL",
    "DEFAULT_DOUYIN_WS_QUERY",
    "DouyinBootstrapContext",
    "DouyinBootstrapProvider",
    "DouyinCodecError",
    "DouyinDanmuEvent",
    "DouyinDanmuListener",
    "DouyinLiveInfo",
    "DouyinLiveResponsePayload",
    "DouyinMessageEnvelope",
    "DouyinProtoCodec",
    "DouyinProtocolError",
    "DouyinPushFrame",
    "DouyinSignatureProvider",
    "StaticBootstrapProvider",
    "StaticSignatureProvider",
    "UnsupportedDouyinProtoCodec",
    "WEBSOCKET_BINARY_OPCODE",
    "build_douyin_live_headers",
    "build_douyin_websocket_url",
    "extract_cookie_value",
    "fetch_douyin_live_info",
    "merge_cookie_strings",
    "normalize_douyin_message",
    "parse_douyin_live_info_html",
]
