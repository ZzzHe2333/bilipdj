from __future__ import annotations

import datetime as dt
import html
import json
import logging
import random
import re
import string
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any

if __package__:
    from . import douyin_live_pb2
else:
    import douyin_live_pb2

DOUYIN_LIVE_PAGE_URL = "https://live.douyin.com/{live_id}"
DOUYIN_FETCH_URL = "https://live.douyin.com/webcast/im/fetch/"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/138.0.0.0 Safari/537.36"
)
DEFAULT_POLL_INTERVAL_SECONDS = 1.0


class DouyinProtocolError(RuntimeError):
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
    anchor_nickname: str = ""
    raw_html: str = ""


@dataclass(slots=True)
class DouyinChatEvent:
    uid: int
    sec_uid: str
    nickname: str
    content: str
    user_role: int = 0
    recv_time: str = ""


@dataclass(slots=True)
class DouyinPollResult:
    cursor: str
    internal_ext: str
    poll_interval_seconds: float
    chat_events: list[DouyinChatEvent] = field(default_factory=list)
    raw_size: int = 0


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if isinstance(value, bool):
            return int(value)
        if value is None:
            return default
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(str(value).strip())
    except (TypeError, ValueError):
        return default


def _safe_unescape(value: str) -> str:
    text = html.unescape(str(value or ""))
    try:
        return json.loads(f'"{text}"')
    except json.JSONDecodeError:
        return text


def _search_patterns(text: str, patterns: list[str]) -> str:
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE | re.DOTALL):
            value = match.group("value") if "value" in match.groupdict() else match.group(1)
            value = value.strip()
            if "\\u" in value or '\\"' in value or "\\/" in value:
                value = _safe_unescape(value)
            else:
                value = html.unescape(value)
            if value in {"", "$undefined", "undefined", "null", "$null", "None"}:
                continue
            return value
    return ""


def normalize_live_id(raw_value: Any) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return ""
    if text.isdigit():
        return text
    match = re.search(r"live\.douyin\.com/(\d+)", text, flags=re.IGNORECASE)
    if match:
        return match.group(1)
    match = re.search(r"web_rid(?:=|\\\":\\\")(?P<value>\d+)", text, flags=re.IGNORECASE)
    if match:
        return match.group("value")
    match = re.search(r"/(\d+)(?:\?|$)", text)
    if match:
        return match.group(1)
    return text


def _looks_like_http_url(text: str) -> bool:
    parsed = urllib.parse.urlparse(str(text or "").strip())
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _looks_like_douyin_short_url(text: str) -> bool:
    parsed = urllib.parse.urlparse(str(text or "").strip())
    host = parsed.netloc.lower()
    return host in {"v.douyin.com", "iesdouyin.com"} or host.endswith(".v.douyin.com") or host.endswith(".iesdouyin.com")


def _resolve_douyin_short_url(url: str, *, cookie: str = "", timeout: float = 12.0) -> str:
    referer = "https://live.douyin.com/?from_nav=1"
    req = urllib.request.Request(url, headers=build_douyin_live_headers(cookie, referer=referer))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return str(resp.geturl() or "").strip()


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
    return "; ".join(f"{k}={v}" for k, v in merged.items())


def _extract_cookie_string(set_cookie_headers: list[str]) -> str:
    pairs: list[str] = []
    for header in set_cookie_headers:
        pair = str(header).split(";", 1)[0].strip()
        if "=" in pair:
            pairs.append(pair)
    return "; ".join(pairs)


def map_room_status(room_status: Any) -> str:
    status = str(room_status or "").strip()
    lowered = status.lower()
    if status == "2" or lowered in {"live", "normal"}:
        return "live"
    if status == "4" or lowered in {"offline", "ended"}:
        return "offline"
    if status:
        return f"unknown:{status}"
    return "unknown"


def build_douyin_live_headers(cookie: str = "", *, referer: str = "https://live.douyin.com/") -> dict[str, str]:
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": referer,
        "Origin": "https://live.douyin.com",
    }
    if cookie.strip():
        headers["Cookie"] = cookie
    return headers


def parse_douyin_live_info_html(html_text: str, live_id: str) -> DouyinLiveInfo:
    room_id = _search_patterns(
        html_text,
        [
            r'\\"roomId\\":\\"(?P<value>\d+)\\"',
            r'"roomId"\s*:\s*"(?P<value>\d+)"',
            r'"room_id"\s*:\s*"(?P<value>\d+)"',
            r'\\"web_rid\\":\\"(?P<value>\d+)\\"',
            r'"web_rid"\s*:\s*"(?P<value>\d+)"',
        ],
    )
    user_unique_id = _search_patterns(
        html_text,
        [
            r'\\"user_unique_id\\":\\"(?P<value>\d+)\\"',
            r'"user_unique_id"\s*:\s*"(?P<value>\d+)"',
            r'"userUniqueId"\s*:\s*"(?P<value>\d+)"',
        ],
    )
    anchor_id = _search_patterns(
        html_text,
        [
            r'\\"anchor\\":\{\\"id_str\\":\\"(?P<value>\d+)\\"',
            r'"anchor"\s*:\s*\{"id_str"\s*:\s*"(?P<value>\d+)"',
        ],
    )
    sec_uid = _search_patterns(
        html_text,
        [
            r'\\"sec_uid\\":\\"(?P<value>[^"]+)\\"',
            r'"sec_uid"\s*:\s*"(?P<value>[^"]+)"',
        ],
    )
    anchor_nickname = _search_patterns(
        html_text,
        [
            r'\\"anchor\\":\{\\"id_str\\":\\"[^"]+\\",\\"sec_uid\\":\\"[^"]+\\",\\"nickname\\":\\"(?P<value>[^"]+)\\"',
            r'\\"owner\\":\{\\"id_str\\":\\"[^"]+\\",\\"sec_uid\\":\\"[^"]+\\",\\"nickname\\":\\"(?P<value>[^"]+)\\"',
            r'"owner"\s*:\s*\{"id_str"\s*:\s*"[^"]+",\s*"sec_uid"\s*:\s*"[^"]+",\s*"nickname"\s*:\s*"(?P<value>[^"]+)"',
            r'\\"sec_uid\\":\\"[^"]+\\"[^{}]{0,200}\\"nickname\\":\\"(?P<value>[^"]+)\\"',
        ],
    )
    room_status = _search_patterns(
        html_text,
        [
            r'\\"roomInfo\\":\{\\"room\\":\{\\"id_str\\":\\"[^"]+\\",\\"status\\":(?P<value>\d+),',
            r'"roomInfo"\s*:\s*\{"room"\s*:\s*\{"id_str"\s*:\s*"[^"]+",\s*"status"\s*:\s*(?P<value>\d+),',
            r'"room_status"\s*:\s*"?(?P<value>\d+)"?',
            # 宽松匹配：roomInfo 下 room 对象里的 status 字段（不要求 id_str 紧邻）
            r'\\"roomInfo\\":\{[^}]{0,200}\\"status\\":(?P<value>\d+)[,}]',
            r'"roomInfo"\s*:\s*\{[^}]{0,200}"status"\s*:\s*(?P<value>\d+)[,}]',
            # room 对象直接包含 status（各种嵌套写法）
            r'\\"room\\":\{[^}]{0,400}\\"status\\":(?P<value>\d+)[,}]',
            r'"room"\s*:\s*\{[^}]{0,400}"status"\s*:\s*(?P<value>\d+)[,}]',
            # liveStatus / live_status 兜底
            r'\\"liveStatus\\":\\"(?P<value>[^"]+)\\"',
            r'"liveStatus"\s*:\s*"(?P<value>[^"]+)"',
            r'"liveStatus"\s*:\s*"?(?P<value>\d+)"?',
            r'\\"live_status\\":\\"(?P<value>[^"]+)\\"',
            r'"live_status"\s*:\s*"(?P<value>[^"]+)"',
            r'"live_status"\s*:\s*"?(?P<value>\d+)"?',
        ],
    )
    room_title = _search_patterns(
        html_text,
        [
            r'\\"roomInfo\\":\{\\"room\\":\{\\"id_str\\":\\"[^"]+\\",\\"status\\":\d+,\\"status_str\\":\\"[^"]*\\",\\"title\\":\\"(?P<value>(?:[^"\\]|\\.)*)\\"',
            r"<title>(?P<value>.*?)</title>",
        ],
    )

    if not room_id or not user_unique_id:
        raise DouyinProtocolError("Failed to extract room_id/user_unique_id from Douyin live page")

    return DouyinLiveInfo(
        live_id=str(live_id),
        room_id=room_id,
        user_id=user_unique_id,
        user_unique_id=user_unique_id,
        anchor_id=anchor_id,
        sec_uid=sec_uid,
        room_status=room_status,
        room_title=room_title,
        anchor_nickname=anchor_nickname,
        raw_html=html_text,
    )


def fetch_douyin_live_info(live_id: str, *, cookie: str = "", timeout: float = 12.0) -> DouyinLiveInfo:
    raw_input = str(live_id or "").strip()
    resolved_short_url = ""
    resolved_sec_uid = ""
    normalized_live_id = normalize_live_id(raw_input)
    if _looks_like_douyin_short_url(raw_input):
        resolved_short_url = _resolve_douyin_short_url(raw_input, cookie=cookie, timeout=timeout)
        normalized_live_id = normalize_live_id(resolved_short_url)
        parsed_short_url = urllib.parse.urlparse(resolved_short_url)
        short_query = urllib.parse.parse_qs(parsed_short_url.query)
        for key in ("sec_user_id", "sec_uid", "secuid"):
            values = short_query.get(key) or []
            if values and str(values[0]).strip():
                resolved_sec_uid = str(values[0]).strip()
                break
    if not normalized_live_id:
        raise DouyinProtocolError("Empty Douyin live_id")
    referer = "https://live.douyin.com/?from_nav=1"
    url = DOUYIN_LIVE_PAGE_URL.format(live_id=urllib.parse.quote(normalized_live_id, safe=""))
    req = urllib.request.Request(url, headers=build_douyin_live_headers(cookie, referer=referer))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        html_text = resp.read().decode("utf-8", errors="replace")
        set_cookie_headers = resp.headers.get_all("Set-Cookie") or []
    info = parse_douyin_live_info_html(html_text, normalized_live_id)
    merged_cookie = merge_cookie_strings(cookie, _extract_cookie_string(set_cookie_headers))
    if not info.sec_uid and resolved_sec_uid:
        info.sec_uid = resolved_sec_uid
    if not info.ttwid:
        info.ttwid = extract_cookie_value(merged_cookie, "ttwid")
    return info


def _generate_ms_token(length: int = 107) -> str:
    alphabet = string.ascii_letters + string.digits + "="
    return "".join(random.choice(alphabet) for _ in range(max(16, int(length))))


def _random_a_bogus(length: int = 8) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(random.choice(alphabet) for _ in range(max(4, int(length))))


def _build_fetch_query(
    *,
    live_info: DouyinLiveInfo,
    cursor: str,
    internal_ext: str,
    ms_token: str,
    a_bogus: str,
    user_agent: str,
) -> dict[str, str]:
    return {
        "resp_content_type": "protobuf",
        "did_rule": "3",
        "device_id": "",
        "app_name": "douyin_web",
        "endpoint": "live_pc",
        "support_wrds": "1",
        "user_unique_id": str(live_info.user_unique_id or live_info.user_id or ""),
        "identity": "audience",
        "need_persist_msg_count": "15",
        "insert_task_id": "",
        "live_reason": "",
        "room_id": str(live_info.room_id or ""),
        "version_code": "180800",
        "last_rtt": "0",
        "live_id": "1",
        "aid": "6383",
        "fetch_rule": "1",
        "cursor": str(cursor or ""),
        "internal_ext": str(internal_ext or ""),
        "device_platform": "web",
        "cookie_enabled": "true",
        "screen_width": "1920",
        "screen_height": "1080",
        "browser_language": "zh-CN",
        "browser_platform": "Win32",
        "browser_name": "Mozilla",
        "browser_version": user_agent,
        "browser_online": "true",
        "tz_name": "Asia/Shanghai",
        "msToken": ms_token,
        # 实测此参数只要存在且非空即可返回 protobuf
        "a_bogus": a_bogus,
    }


def _parse_poll_messages(
    payload: bytes,
    *,
    fallback_cursor: str,
    fallback_internal_ext: str,
) -> DouyinPollResult:
    response = douyin_live_pb2.LiveResponse()
    response.ParseFromString(payload)
    cursor = str(response.cursor or fallback_cursor or "")
    internal_ext = str(response.internalExt or fallback_internal_ext or "")
    poll_interval_seconds = DEFAULT_POLL_INTERVAL_SECONDS
    if int(response.fetchInterval or 0) > 0:
        poll_interval_seconds = max(0.2, min(5.0, float(response.fetchInterval) / 1000.0))
    chat_events: list[DouyinChatEvent] = []
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    for item in response.messagesList:
        method = str(item.method or "")
        if method != "WebcastChatMessage":
            continue
        message = douyin_live_pb2.ChatMessage()
        try:
            message.ParseFromString(item.payload)
        except Exception:
            continue
        nickname = str(getattr(message.user, "nickname", "") or "").strip()
        content = str(getattr(message, "content", "") or "").strip()
        if not nickname or not content:
            continue
        chat_events.append(
            DouyinChatEvent(
                uid=_to_int(getattr(message.user, "id", 0), 0),
                sec_uid=str(getattr(message.user, "sec_uid", "") or ""),
                nickname=nickname,
                content=content,
                user_role=_to_int(getattr(message.user, "user_role", 0), 0),
                recv_time=now_iso,
            )
        )
    return DouyinPollResult(
        cursor=cursor,
        internal_ext=internal_ext,
        poll_interval_seconds=poll_interval_seconds,
        chat_events=chat_events,
        raw_size=len(payload),
    )


def fetch_douyin_poll_once(
    *,
    live_info: DouyinLiveInfo,
    cursor: str,
    internal_ext: str,
    cookie: str = "",
    ms_token: str = "",
    a_bogus: str = "",
    timeout: float = 12.0,
    user_agent: str = DEFAULT_USER_AGENT,
) -> DouyinPollResult:
    token = ms_token.strip() or extract_cookie_value(cookie, "msToken") or _generate_ms_token()
    bogus = a_bogus.strip() or _random_a_bogus()
    merged_cookie = cookie
    if live_info.ttwid and not extract_cookie_value(cookie, "ttwid"):
        merged_cookie = merge_cookie_strings(cookie, f"ttwid={live_info.ttwid}")

    query = _build_fetch_query(
        live_info=live_info,
        cursor=cursor,
        internal_ext=internal_ext,
        ms_token=token,
        a_bogus=bogus,
        user_agent=user_agent,
    )
    url = f"{DOUYIN_FETCH_URL}?{urllib.parse.urlencode(query)}"
    referer = DOUYIN_LIVE_PAGE_URL.format(live_id=urllib.parse.quote(live_info.live_id, safe=""))
    headers = {
        "User-Agent": user_agent,
        "Accept": "*/*",
        "Referer": referer,
        "Origin": "https://live.douyin.com",
    }
    if merged_cookie.strip():
        headers["Cookie"] = merged_cookie

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        payload = resp.read()

    if not payload:
        return DouyinPollResult(
            cursor=cursor,
            internal_ext=internal_ext,
            poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
            chat_events=[],
            raw_size=0,
        )
    return _parse_poll_messages(
        payload,
        fallback_cursor=cursor,
        fallback_internal_ext=internal_ext,
    )


class DouyinDanmuRelay(threading.Thread):
    platform = "douyin"

    def __init__(self, server: Any) -> None:
        super().__init__(name="douyin-danmu-relay", daemon=True)
        self.server = server
        self.logger = server.logger
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        self._status_lock = threading.Lock()
        self._connected = False
        self._last_packet_monotonic = 0.0
        self._last_packet_at = ""
        self._last_connect_at = ""
        self._last_disconnect_at = ""
        self._last_disconnect_reason = ""
        self._current_roomid = 0
        self._current_host = "live.douyin.com"
        self._current_port = 443
        self._current_transport = "https"
        self._current_auth_uid = 0
        self._current_live_id = ""
        self._current_live_status = ""
        self._current_anchor_nickname = ""
        self._last_chat_seen_at = ""

    def stop(self) -> None:
        self._stop_event.set()
        self._reconnect_event.set()

    def request_reconnect(self) -> None:
        self._reconnect_event.set()

    def _emit_status(self, status: str, **extra: Any) -> None:
        payload = {"type": "PDJ_STATUS", "status": status, "platform": "douyin"}
        payload.update(extra)
        self.server.ws_hub.broadcast_json(None, payload)

    def _mark_packet(self) -> None:
        with self._status_lock:
            self._last_packet_monotonic = time.monotonic()
            self._last_packet_at = dt.datetime.now(dt.timezone.utc).isoformat()

    def _mark_connected(self, *, live_info: DouyinLiveInfo) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        roomid = _to_int(live_info.room_id, 0)
        live_status = map_room_status(live_info.room_status)
        with self._status_lock:
            self._connected = True
            self._last_connect_at = now
            self._last_disconnect_reason = ""
            self._current_roomid = roomid
            self._current_live_id = live_info.live_id
            self._current_live_status = live_status
            self._current_anchor_nickname = str(live_info.anchor_nickname or "")
        self._mark_packet()

    def _mark_disconnected(self, reason: str = "") -> None:
        with self._status_lock:
            self._connected = False
            self._last_disconnect_at = dt.datetime.now(dt.timezone.utc).isoformat()
            if reason:
                self._last_disconnect_reason = reason

    def get_runtime_status(self) -> dict[str, Any]:
        with self._status_lock:
            connected = self._connected
            last_packet_monotonic = self._last_packet_monotonic
            last_packet_at = self._last_packet_at
            last_connect_at = self._last_connect_at
            last_disconnect_at = self._last_disconnect_at
            last_disconnect_reason = self._last_disconnect_reason
            roomid = self._current_roomid
            host = self._current_host
            port = self._current_port
            transport = self._current_transport
            auth_uid = self._current_auth_uid
            live_id = self._current_live_id
            live_status = self._current_live_status
            anchor_nickname = self._current_anchor_nickname
            last_chat_seen_at = self._last_chat_seen_at
        return {
            "platform": "douyin",
            "connected": connected,
            "last_packet_at": last_packet_at,
            "last_connect_at": last_connect_at,
            "last_disconnect_at": last_disconnect_at,
            "last_disconnect_reason": last_disconnect_reason,
            "idle_seconds": max(0.0, time.monotonic() - last_packet_monotonic) if last_packet_monotonic else None,
            "roomid": roomid,
            "host": host,
            "port": port,
            "transport": transport,
            "auth_uid": auth_uid,
            "live_id": live_id,
            "live_status": live_status,
            "anchor_nickname": anchor_nickname,
            "last_chat_seen_at": last_chat_seen_at,
        }

    def _load_runtime_cfg(self) -> dict[str, Any]:
        runtime = getattr(self.server, "runtime_config", {})
        if not isinstance(runtime, dict):
            runtime = {}
        douyin_cfg = runtime.get("douyin", {})
        if not isinstance(douyin_cfg, dict):
            douyin_cfg = {}
        ws_cfg = douyin_cfg.get("ws", {})
        if not isinstance(ws_cfg, dict):
            ws_cfg = {}
        bootstrap_cfg = douyin_cfg.get("bootstrap", {})
        if not isinstance(bootstrap_cfg, dict):
            bootstrap_cfg = {}
        extra_query = douyin_cfg.get("extra_query", {})
        if not isinstance(extra_query, dict):
            extra_query = {}
        live_info_cfg = douyin_cfg.get("live_info", {})
        if not isinstance(live_info_cfg, dict):
            live_info_cfg = {}
        return {
            "platform": str(runtime.get("platform", "bilibili") or "").strip().lower(),
            "enabled": bool(douyin_cfg.get("enabled", False)),
            "live_id": normalize_live_id(douyin_cfg.get("live_id", "")),
            "cookie": str(douyin_cfg.get("cookie", "") or "").strip(),
            "cursor": str(bootstrap_cfg.get("cursor", "") or ""),
            "internal_ext": str(bootstrap_cfg.get("internal_ext", "") or ""),
            "auto_reconnect": bool(ws_cfg.get("auto_reconnect", True)),
            "reconnect_delay_seconds": max(0.5, _to_float(ws_cfg.get("reconnect_delay_seconds", 2.0), 2.0)),
            "extra_query": dict(extra_query),
            # 预置 live_info 字段（由"获取参数"或手动填写写入 config）
            "preset_room_id": str(live_info_cfg.get("room_id", "") or "").strip(),
            "preset_user_id": str(live_info_cfg.get("user_id", "") or "").strip(),
            "preset_user_unique_id": str(live_info_cfg.get("user_unique_id", "") or "").strip(),
            "preset_anchor_id": str(live_info_cfg.get("anchor_id", "") or "").strip(),
            "preset_sec_uid": str(live_info_cfg.get("sec_uid", "") or "").strip(),
            "preset_ttwid": str(live_info_cfg.get("ttwid", "") or "").strip(),
        }

    def _sync_runtime_live_info(self, info: DouyinLiveInfo) -> None:
        runtime = getattr(self.server, "runtime_config", None)
        if not isinstance(runtime, dict):
            return
        douyin_cfg = runtime.get("douyin")
        if not isinstance(douyin_cfg, dict):
            return
        live_info_cfg = douyin_cfg.get("live_info")
        if not isinstance(live_info_cfg, dict):
            live_info_cfg = {}
            douyin_cfg["live_info"] = live_info_cfg
        live_info_cfg.update(
            {
                "room_id": info.room_id,
                "user_id": info.user_id,
                "user_unique_id": info.user_unique_id,
                "anchor_id": info.anchor_id,
                "sec_uid": info.sec_uid,
                "ttwid": info.ttwid,
            }
        )

    @staticmethod
    def _to_bilibili_like_danmu_payload(event: DouyinChatEvent) -> dict[str, Any]:
        is_admin_flag = 1 if event.user_role >= 3 else 0
        return {
            "cmd": "DANMU_MSG",
            "info": [
                [],
                event.content,
                [int(event.uid), event.nickname, is_admin_flag],
                [],
            ],
        }

    def _forward_chat_event(self, event: DouyinChatEvent) -> None:
        self.server.ws_hub.mark_message()
        self._last_chat_seen_at = event.recv_time
        self.server.ws_hub.broadcast_json(
            None,
            {
                "type": "DOUYIN_DANMU",
                "uid": int(event.uid),
                "sec_uid": event.sec_uid,
                "nickname": event.nickname,
                "content": event.content,
                "time": event.recv_time,
                "platform": "douyin",
            },
        )
        if hasattr(self.server, "queue_manager"):
            self.server.queue_manager.process_danmu_json(
                self._to_bilibili_like_danmu_payload(event)
            )

    def _connect_and_stream(self) -> None:
        cfg = self._load_runtime_cfg()
        if cfg["platform"] != "douyin":
            self._emit_status("danmu_waiting_platform", message="platform is not douyin")
            time.sleep(1.5)
            return
        if not cfg["enabled"]:
            self._emit_status("danmu_waiting_config", message="douyin.enabled=false")
            time.sleep(2.0)
            return
        if not cfg["live_id"]:
            self._emit_status("danmu_waiting_config", message="douyin.live_id not configured")
            time.sleep(2.0)
            return

        info = fetch_douyin_live_info(cfg["live_id"], cookie=cfg["cookie"])
        # 预置字段：config 里由"获取参数"或手动填写写入的值作为兜底
        # 判断 HTML 解析结果是否为空（空字符串或 "0" 均视为无效）
        def _empty(v: str) -> bool:
            return not v or v == "0"

        if cfg["preset_room_id"] and _empty(info.room_id):
            info.room_id = cfg["preset_room_id"]
        if cfg["preset_user_unique_id"] and _empty(info.user_unique_id):
            info.user_unique_id = cfg["preset_user_unique_id"]
        if cfg["preset_user_id"] and _empty(info.user_id):
            info.user_id = cfg["preset_user_id"]
        if cfg["preset_anchor_id"] and _empty(info.anchor_id):
            info.anchor_id = cfg["preset_anchor_id"]
        if cfg["preset_sec_uid"] and not info.sec_uid:
            info.sec_uid = cfg["preset_sec_uid"]
        if cfg["preset_ttwid"] and not info.ttwid:
            info.ttwid = cfg["preset_ttwid"]
        self._sync_runtime_live_info(info)
        if hasattr(self.server, "queue_manager"):
            try:
                self.server.queue_manager.load_config(
                    getattr(self.server, "runtime_config", {}).get("myjs", {}),
                    anchor_uid=_to_int(info.user_id, 0),
                )
            except Exception:  # noqa: BLE001
                pass
        live_status = map_room_status(info.room_status)
        with self._status_lock:
            self._current_roomid = _to_int(info.room_id, 0)
            self._current_live_id = info.live_id
            self._current_live_status = live_status
            self._current_anchor_nickname = str(info.anchor_nickname or "")
        # 仅在明确为 offline（status=4）时等待；unknown 状态仍尝试连接
        if live_status == "offline":
            self._mark_disconnected(f"douyin live status: {live_status}")
            self._emit_status(
                "danmu_waiting_live",
                live_id=info.live_id,
                roomid=_to_int(info.room_id, 0),
                live_status=live_status,
                anchor_nickname=info.anchor_nickname,
            )
            time.sleep(cfg["reconnect_delay_seconds"])
            return
        if live_status not in ("live", "offline"):
            self.logger.warning(
                "Douyin room status unknown (raw=%r); proceeding optimistically",
                info.room_status,
            )

        cursor = cfg["cursor"]
        internal_ext = cfg["internal_ext"]
        ms_token = extract_cookie_value(cfg["cookie"], "msToken") or _generate_ms_token()
        a_bogus = str(cfg["extra_query"].get("a_bogus", "") or "").strip() or _random_a_bogus()

        self._mark_connected(live_info=info)
        self._emit_status(
            "danmu_connected",
            live_id=info.live_id,
            roomid=_to_int(info.room_id, 0),
            host="live.douyin.com",
            port=443,
            transport="https",
            live_status=live_status,
            anchor_nickname=info.anchor_nickname,
        )
        self.logger.info(
            "Douyin connected live_id=%s room_id=%s status=%s anchor=%s",
            info.live_id,
            info.room_id,
            live_status,
            info.anchor_nickname or "",
        )

        next_refresh_live_info = time.monotonic() + 45.0
        while not self._stop_event.is_set():
            if self._reconnect_event.is_set():
                self._reconnect_event.clear()
                self.logger.info("Received reconnect signal; restarting Douyin danmu stream")
                break

            result = fetch_douyin_poll_once(
                live_info=info,
                cursor=cursor,
                internal_ext=internal_ext,
                cookie=cfg["cookie"],
                ms_token=ms_token,
                a_bogus=a_bogus,
            )
            cursor = result.cursor
            internal_ext = result.internal_ext
            if result.raw_size > 0:
                self._mark_packet()
            for event in result.chat_events:
                self._forward_chat_event(event)

            if time.monotonic() >= next_refresh_live_info:
                refreshed = fetch_douyin_live_info(info.live_id, cookie=cfg["cookie"])
                self._sync_runtime_live_info(refreshed)
                self._current_live_status = map_room_status(refreshed.room_status)
                self._current_anchor_nickname = str(refreshed.anchor_nickname or "")
                if self._current_live_status != "live":
                    self._mark_disconnected(f"douyin live status changed: {self._current_live_status}")
                    raise ConnectionError(f"douyin live status changed: {self._current_live_status}")
                next_refresh_live_info = time.monotonic() + 45.0

            time.sleep(max(0.15, result.poll_interval_seconds))

    def run(self) -> None:
        while not self._stop_event.is_set():
            cfg = self._load_runtime_cfg()
            try:
                self._connect_and_stream()
            except Exception as exc:  # noqa: BLE001
                self._mark_disconnected(str(exc))
                self._emit_status("danmu_disconnected", error=str(exc), platform="douyin")
                self.logger.warning("Douyin connection error: %s", exc)
            if self._stop_event.is_set():
                break
            if not cfg.get("auto_reconnect", True):
                break
            time.sleep(_to_float(cfg.get("reconnect_delay_seconds", 2.0), 2.0))


__all__ = [
    "DOUYIN_FETCH_URL",
    "DOUYIN_LIVE_PAGE_URL",
    "DouyinChatEvent",
    "DouyinDanmuRelay",
    "DouyinLiveInfo",
    "DouyinPollResult",
    "DouyinProtocolError",
    "build_douyin_live_headers",
    "extract_cookie_value",
    "fetch_douyin_live_info",
    "fetch_douyin_poll_once",
    "map_room_status",
    "merge_cookie_strings",
    "normalize_live_id",
    "parse_douyin_live_info_html",
]
