from __future__ import annotations

import datetime as dt
import gzip
import hashlib
import json
import re
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterator

try:
    from .danmu_event import DanmuEvent
except ImportError:  # standalone compatibility
    from danmu_event import DanmuEvent

DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
)
GOOGLE_PROBE_URL = "https://www.google.com/generate_204"
WATCH_URL = "https://www.youtube.com/watch?v={video_id}&hl=zh-CN&gl=US"
INNERTUBE_CHAT_URL = "https://www.youtube.com/youtubei/v1/live_chat/{endpoint}?key={api_key}&prettyPrint=false"
DEFAULT_RECONNECT_DELAY_SECONDS = 2.0


class YoutubeProtocolError(RuntimeError):
    pass


@dataclass(slots=True)
class YoutubeLiveInfo:
    room_url: str
    video_id: str
    title: str = ""
    channel: str = ""
    mode: str = "live"


@dataclass(slots=True)
class YoutubeChatEvent:
    uid: int
    nickname: str
    content: str
    event_id: str = ""
    kind: str = "chat"
    recv_time: str = ""


def _stable_uid(text: str) -> int:
    digest = hashlib.blake2b(str(text or "").encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big") & 0x7FFF_FFFF_FFFF_FFFF


def extract_video_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        raise YoutubeProtocolError("红色小电视直播链接或 Video ID 不能为空")
    if re.fullmatch(r"[A-Za-z0-9_-]{11}", text):
        return text

    if "://" not in text and ("youtube.com" in text or "youtu.be" in text):
        text = "https://" + text.lstrip("/")
    parsed = urllib.parse.urlparse(text)
    host = parsed.netloc.casefold()
    if host.endswith("youtu.be"):
        video_id = parsed.path.strip("/").split("/")[0]
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            return video_id
    if "youtube.com" in host:
        query = urllib.parse.parse_qs(parsed.query)
        video_id = str((query.get("v") or [""])[0])
        if re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
            return video_id
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) >= 2 and parts[0] in {"live", "shorts", "embed"}:
            video_id = parts[1]
            if re.fullmatch(r"[A-Za-z0-9_-]{11}", video_id):
                return video_id
    raise YoutubeProtocolError("无法从红色小电视链接中解析 Video ID")


def canonical_room_url(video_id: str) -> str:
    return f"https://www.youtube.com/watch?v={video_id}"


def probe_google_access(*, timeout: float = 4.0) -> bool:
    request = urllib.request.Request(
        GOOGLE_PROBE_URL,
        headers={
            "User-Agent": DEFAULT_USER_AGENT,
            "Accept": "*/*",
            "Connection": "close",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=max(0.5, float(timeout))) as response:
            return int(getattr(response, "status", 200) or 200) in {200, 204}
    except Exception:
        return False


def _http_text(url: str, *, cookie: str = "", timeout: float = 15.0) -> str:
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
        "Connection": "close",
    }
    if str(cookie or "").strip():
        headers["Cookie"] = str(cookie).strip()
    request = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(request, timeout=max(1.0, float(timeout))) as response:
        raw = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
    return raw.decode(charset, errors="replace")


def _http_json_post(
    url: str,
    payload: dict[str, Any],
    *,
    cookie: str = "",
    referer: str = "",
    timeout: float = 15.0,
) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.7",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Origin": "https://www.youtube.com",
        "Connection": "close",
    }
    if referer:
        headers["Referer"] = referer
    if str(cookie or "").strip():
        headers["Cookie"] = str(cookie).strip()
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=max(1.0, float(timeout))) as response:
        raw = response.read()
        if str(response.headers.get("Content-Encoding", "")).casefold() == "gzip":
            raw = gzip.decompress(raw)
    result = json.loads(raw.decode("utf-8", errors="replace"))
    if not isinstance(result, dict):
        raise YoutubeProtocolError("红色小电视聊天接口返回了无效数据")
    return result


def _extract_balanced_json(text: str, start_pos: int) -> str:
    index = start_pos
    while index < len(text) and text[index] not in "{[":
        index += 1
    if index >= len(text):
        raise ValueError("JSON start not found")
    opener = text[index]
    closer = "}" if opener == "{" else "]"
    depth = 0
    in_string = False
    escaped = False
    for end in range(index, len(text)):
        char = text[end]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
            continue
        if char == opener:
            depth += 1
        elif char == closer:
            depth -= 1
            if depth == 0:
                return text[index : end + 1]
    raise ValueError("JSON is not balanced")


def _find_json_after_markers(text: str, markers: tuple[str, ...]) -> Any:
    for marker in markers:
        pos = text.find(marker)
        if pos < 0:
            continue
        try:
            return json.loads(_extract_balanced_json(text, pos + len(marker)))
        except Exception:
            continue
    return None


def _extract_ytcfg(html: str) -> dict[str, Any]:
    config: dict[str, Any] = {}
    marker = "ytcfg.set("
    cursor = 0
    while True:
        pos = html.find(marker, cursor)
        if pos < 0:
            break
        try:
            raw = _extract_balanced_json(html, pos + len(marker))
            value = json.loads(raw)
            if isinstance(value, dict):
                config.update(value)
            cursor = pos + len(marker) + len(raw)
        except Exception:
            cursor = pos + len(marker)
    if "INNERTUBE_API_KEY" not in config:
        match = re.search(r'"INNERTUBE_API_KEY"\s*:\s*"([^"]+)"', html)
        if match:
            config["INNERTUBE_API_KEY"] = match.group(1)
    if "INNERTUBE_CLIENT_VERSION" not in config:
        match = re.search(r'"INNERTUBE_CLIENT_VERSION"\s*:\s*"([^"]+)"', html)
        if match:
            config["INNERTUBE_CLIENT_VERSION"] = match.group(1)
    return config


def _extract_initial_data(html: str) -> dict[str, Any] | None:
    value = _find_json_after_markers(
        html,
        (
            "var ytInitialData =",
            'window["ytInitialData"] =',
            "ytInitialData =",
        ),
    )
    return value if isinstance(value, dict) else None


def _recursive_find_key(obj: Any, key: str) -> Iterator[Any]:
    if isinstance(obj, dict):
        if key in obj:
            yield obj[key]
        for value in obj.values():
            yield from _recursive_find_key(value, key)
    elif isinstance(obj, list):
        for value in obj:
            yield from _recursive_find_key(value, key)


def _text_from_runs(obj: Any) -> str:
    if not isinstance(obj, dict):
        return ""
    simple = obj.get("simpleText")
    if isinstance(simple, str):
        return simple
    result: list[str] = []
    for run in obj.get("runs", []) if isinstance(obj.get("runs"), list) else []:
        if not isinstance(run, dict):
            continue
        text = run.get("text")
        if isinstance(text, str):
            result.append(text)
            continue
        emoji = run.get("emoji")
        if isinstance(emoji, dict):
            shortcuts = emoji.get("shortcuts")
            if isinstance(shortcuts, list) and shortcuts:
                result.append(str(shortcuts[0]))
            elif emoji.get("emojiId"):
                result.append(f"[emoji:{emoji['emojiId']}]")
    return "".join(result)


def _get_continuation(obj: Any) -> tuple[str, int]:
    if not isinstance(obj, dict):
        return "", 1000
    continuations = obj.get("continuations")
    if isinstance(continuations, list):
        for item in continuations:
            if not isinstance(item, dict):
                continue
            for key in (
                "invalidationContinuationData",
                "timedContinuationData",
                "reloadContinuationData",
                "liveChatReplayContinuationData",
            ):
                data = item.get(key)
                if isinstance(data, dict) and data.get("continuation"):
                    try:
                        timeout_ms = int(data.get("timeoutMs") or 1000)
                    except (TypeError, ValueError):
                        timeout_ms = 1000
                    return str(data["continuation"]), timeout_ms
    for value in _recursive_find_key(obj, "continuation"):
        if isinstance(value, str) and len(value) > 20:
            return value, 1000
    return "", 1000


def _find_live_chat_renderer(initial_data: dict[str, Any]) -> dict[str, Any] | None:
    for value in _recursive_find_key(initial_data, "liveChatRenderer"):
        if isinstance(value, dict):
            return value
    return None


def _find_text_renderer(initial_data: dict[str, Any], key: str) -> dict[str, Any] | None:
    for value in _recursive_find_key(initial_data, key):
        if isinstance(value, dict):
            return value
    return None


MESSAGE_RENDERERS = (
    "liveChatTextMessageRenderer",
    "liveChatPaidMessageRenderer",
    "liveChatPaidStickerRenderer",
    "liveChatMembershipItemRenderer",
)


def _renderers_from_action(action: Any) -> list[tuple[str, dict[str, Any]]]:
    if not isinstance(action, dict):
        return []
    replay = action.get("replayChatItemAction")
    if isinstance(replay, dict):
        result: list[tuple[str, dict[str, Any]]] = []
        for item in replay.get("actions", []) if isinstance(replay.get("actions"), list) else []:
            result.extend(_renderers_from_action(item))
        return result
    result = []
    for renderer_name in MESSAGE_RENDERERS:
        for value in _recursive_find_key(action, renderer_name):
            if isinstance(value, dict):
                result.append((renderer_name, value))
    return result


def _event_from_renderer(renderer_name: str, renderer: dict[str, Any]) -> YoutubeChatEvent | None:
    nickname = _text_from_runs(renderer.get("authorName") or {}) or "匿名"
    content = _text_from_runs(renderer.get("message") or {})
    kind = "chat"
    if renderer_name == "liveChatPaidMessageRenderer":
        kind = "superchat"
        amount = _text_from_runs(renderer.get("purchaseAmountText") or {})
        content = f"{amount} | {content}".strip(" |") if amount else content
    elif renderer_name == "liveChatPaidStickerRenderer":
        kind = "supersticker"
        amount = _text_from_runs(renderer.get("purchaseAmountText") or {})
        content = amount or "[Super Sticker]"
    elif renderer_name == "liveChatMembershipItemRenderer":
        kind = "membership"
        content = content or _text_from_runs(renderer.get("headerSubtext") or {}) or "[会员消息]"
    if not content:
        return None
    author_key = str(renderer.get("authorExternalChannelId") or nickname)
    return YoutubeChatEvent(
        uid=_stable_uid(author_key),
        nickname=nickname,
        content=content,
        event_id=str(renderer.get("id") or ""),
        kind=kind,
        recv_time=dt.datetime.now(dt.timezone.utc).isoformat(),
    )


class YoutubeChatSession:
    def __init__(self, target: Any, *, cookie: str = "", timeout: float = 15.0) -> None:
        self.video_id = extract_video_id(target)
        self.room_url = canonical_room_url(self.video_id)
        self.cookie = str(cookie or "").strip()
        self.timeout = max(2.0, float(timeout))
        self.api_key = ""
        self.context: dict[str, Any] = {}
        self.continuation = ""
        self.timeout_ms = 1000
        self.mode = "live"
        self.title = ""
        self.channel = ""

    def bootstrap(self) -> YoutubeLiveInfo:
        html = _http_text(
            WATCH_URL.format(video_id=urllib.parse.quote(self.video_id, safe="")),
            cookie=self.cookie,
            timeout=self.timeout,
        )
        config = _extract_ytcfg(html)
        initial = _extract_initial_data(html)
        if initial is None:
            raise YoutubeProtocolError("红色小电视页面没有返回可解析的直播数据")
        self.api_key = str(config.get("INNERTUBE_API_KEY") or "").strip()
        if not self.api_key:
            raise YoutubeProtocolError("红色小电视页面缺少聊天接口配置")

        context = config.get("INNERTUBE_CONTEXT")
        if isinstance(context, dict):
            self.context = context
        else:
            client_version = str(config.get("INNERTUBE_CLIENT_VERSION") or "2.20260908.00.00")
            self.context = {
                "client": {
                    "clientName": "WEB",
                    "clientVersion": client_version,
                    "hl": "zh-CN",
                    "gl": "US",
                }
            }

        renderer = _find_live_chat_renderer(initial)
        if renderer is None:
            raise YoutubeProtocolError("该直播没有可用聊天或聊天回放")
        dumped = json.dumps(renderer, ensure_ascii=False)
        self.mode = "replay" if renderer.get("isReplay") is True or "liveChatReplayContinuationData" in dumped else "live"
        self.continuation, self.timeout_ms = _get_continuation(renderer)
        if not self.continuation:
            raise YoutubeProtocolError("未获取到红色小电视聊天 continuation")

        primary = _find_text_renderer(initial, "videoPrimaryInfoRenderer")
        if primary:
            self.title = _text_from_runs(primary.get("title") or {})
        secondary = _find_text_renderer(initial, "videoSecondaryInfoRenderer")
        if secondary:
            owner = secondary.get("owner")
            if isinstance(owner, dict):
                video_owner = owner.get("videoOwnerRenderer")
                if isinstance(video_owner, dict):
                    self.channel = _text_from_runs(video_owner.get("title") or {})

        return YoutubeLiveInfo(
            room_url=self.room_url,
            video_id=self.video_id,
            title=self.title,
            channel=self.channel,
            mode=self.mode,
        )

    def fetch_once(self) -> tuple[list[YoutubeChatEvent], int]:
        if not self.api_key or not self.continuation:
            raise YoutubeProtocolError("红色小电视聊天会话尚未初始化")
        endpoint = "get_live_chat_replay" if self.mode == "replay" else "get_live_chat"
        url = INNERTUBE_CHAT_URL.format(
            endpoint=endpoint,
            api_key=urllib.parse.quote(self.api_key, safe=""),
        )
        payload: dict[str, Any] = {
            "context": self.context,
            "continuation": self.continuation,
        }
        if self.mode == "live":
            payload["webClientInfo"] = {"isDocumentHidden": False}
        result = _http_json_post(
            url,
            payload,
            cookie=self.cookie,
            referer=self.room_url,
            timeout=self.timeout,
        )
        contents = result.get("continuationContents")
        chat = contents.get("liveChatContinuation") if isinstance(contents, dict) else None
        if not isinstance(chat, dict):
            raise YoutubeProtocolError("红色小电视聊天 continuation 已失效")
        next_token, timeout_ms = _get_continuation(chat)
        if next_token:
            self.continuation = next_token
        self.timeout_ms = max(250, min(int(timeout_ms or 1000), 10000))

        events: list[YoutubeChatEvent] = []
        for action in chat.get("actions", []) if isinstance(chat.get("actions"), list) else []:
            for renderer_name, renderer in _renderers_from_action(action):
                event = _event_from_renderer(renderer_name, renderer)
                if event is not None:
                    events.append(event)
        return events, self.timeout_ms


class YoutubeDanmuRelay(threading.Thread):
    platform = "youtube"

    def __init__(self, server: Any) -> None:
        super().__init__(name="redtv-danmu-relay", daemon=True)
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
        self._last_chat_seen_at = ""
        self._video_id = ""
        self._room_url = ""
        self._title = ""
        self._channel = ""
        self._mode = ""
        self._seen: set[str] = set()

    def stop(self) -> None:
        self._stop_event.set()
        self._reconnect_event.set()

    def request_reconnect(self) -> None:
        self._reconnect_event.set()

    def _emit_status(self, status: str, **extra: Any) -> None:
        payload = {"type": "PDJ_STATUS", "status": status, "platform": "youtube", **extra}
        self.server.ws_hub.broadcast_json(None, payload)

    def _mark_connected(self, info: YoutubeLiveInfo) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with self._status_lock:
            self._connected = True
            self._last_connect_at = now
            self._last_disconnect_reason = ""
            self._video_id = info.video_id
            self._room_url = info.room_url
            self._title = info.title
            self._channel = info.channel
            self._mode = info.mode

    def _mark_packet(self) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with self._status_lock:
            self._last_packet_monotonic = time.monotonic()
            self._last_packet_at = now

    def _mark_disconnected(self, reason: str) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with self._status_lock:
            self._connected = False
            self._last_disconnect_at = now
            self._last_disconnect_reason = str(reason or "")

    def get_runtime_status(self) -> dict[str, Any]:
        with self._status_lock:
            idle = max(0.0, time.monotonic() - self._last_packet_monotonic) if self._last_packet_monotonic else None
            return {
                "platform": "youtube",
                "display_name": "红色小电视",
                "connected": self._connected,
                "last_packet_at": self._last_packet_at,
                "last_connect_at": self._last_connect_at,
                "last_disconnect_at": self._last_disconnect_at,
                "last_disconnect_reason": self._last_disconnect_reason,
                "idle_seconds": idle,
                "roomid": self._video_id,
                "room_url": self._room_url,
                "title": self._title,
                "channel": self._channel,
                "chat_mode": self._mode,
                "last_chat_seen_at": self._last_chat_seen_at,
                "host": "www.youtube.com",
                "port": 443,
                "transport": "https-poll",
            }

    def _load_runtime_cfg(self) -> dict[str, Any]:
        runtime = getattr(self.server, "runtime_config", {})
        if not isinstance(runtime, dict):
            runtime = {}
        section = runtime.get("youtube", {})
        if not isinstance(section, dict):
            section = {}
        extra = section.get("extra", {})
        if not isinstance(extra, dict):
            extra = {}
        try:
            reconnect_delay = max(0.5, float(extra.get("reconnect_delay_seconds", DEFAULT_RECONNECT_DELAY_SECONDS)))
        except (TypeError, ValueError):
            reconnect_delay = DEFAULT_RECONNECT_DELAY_SECONDS
        try:
            timeout = max(3.0, float(extra.get("timeout_seconds", 15.0)))
        except (TypeError, ValueError):
            timeout = 15.0
        return {
            "platform": str(runtime.get("platform", "") or "").strip().lower(),
            "enabled": bool(section.get("enabled", True)),
            "room_id": str(section.get("room_id", "") or "").strip(),
            "room_url": str(section.get("room_url", "") or "").strip(),
            "cookie": str(section.get("cookie", "") or "").strip(),
            "auto_reconnect": bool(extra.get("auto_reconnect", True)),
            "reconnect_delay_seconds": reconnect_delay,
            "timeout_seconds": timeout,
        }

    def _forward_chat_event(self, event: YoutubeChatEvent) -> None:
        fingerprint = event.event_id or f"{event.uid}:{event.nickname}:{event.content}"
        if fingerprint in self._seen:
            return
        self._seen.add(fingerprint)
        if len(self._seen) > 10000:
            self._seen.clear()
            self._seen.add(fingerprint)

        self.server.ws_hub.mark_message()
        self._last_chat_seen_at = event.recv_time
        self.server.ws_hub.broadcast_json(
            None,
            {
                "type": "REDTV_DANMU",
                "uid": int(event.uid),
                "nickname": event.nickname,
                "content": event.content,
                "kind": event.kind,
                "time": event.recv_time,
                "platform": "youtube",
                "room_id": self._video_id,
            },
        )
        if hasattr(self.server, "queue_manager"):
            self.server.queue_manager.process_danmu_event(
                DanmuEvent(
                    platform="youtube",
                    user_id=str(event.uid),
                    username=event.nickname,
                    content=event.content,
                    received_at=event.recv_time,
                    metadata={"event_id": event.event_id, "kind": event.kind, "room_id": self._video_id},
                )
            )

    def _connect_once(self, cfg: dict[str, Any]) -> None:
        if cfg["platform"] != "youtube":
            self._stop_event.wait(1.5)
            return
        if not cfg["enabled"]:
            self._emit_status("danmu_waiting_config", message="红色小电视配置未启用")
            self._stop_event.wait(2.0)
            return
        target = cfg["room_url"] or cfg["room_id"]
        if not target:
            self._emit_status("danmu_waiting_config", message="红色小电视尚未配置直播链接")
            self._stop_event.wait(2.0)
            return

        session = YoutubeChatSession(target, cookie=cfg["cookie"], timeout=cfg["timeout_seconds"])
        info = session.bootstrap()
        self._mark_connected(info)
        self._emit_status(
            "danmu_connected",
            roomid=info.video_id,
            room_url=info.room_url,
            title=info.title,
            channel=info.channel,
            chat_mode=info.mode,
            host="www.youtube.com",
            port=443,
            transport="https-poll",
        )
        self.logger.info(
            "红色小电视已连接 video_id=%s channel=%s mode=%s",
            info.video_id,
            info.channel,
            info.mode,
        )

        while not self._stop_event.is_set() and not self._reconnect_event.is_set():
            events, timeout_ms = session.fetch_once()
            self._mark_packet()
            for event in events:
                self._forward_chat_event(event)
            if self._stop_event.wait(max(0.25, min(timeout_ms / 1000.0, 10.0))):
                break

    def run(self) -> None:
        while not self._stop_event.is_set():
            cfg = self._load_runtime_cfg()
            self._reconnect_event.clear()
            try:
                self._connect_once(cfg)
            except Exception as exc:
                self._mark_disconnected(str(exc))
                self._emit_status("danmu_disconnected", error=str(exc), platform="youtube")
                self.logger.warning("红色小电视连接错误: %s", exc)
            if self._stop_event.is_set() or not cfg.get("auto_reconnect", True):
                break
            self._stop_event.wait(float(cfg.get("reconnect_delay_seconds", DEFAULT_RECONNECT_DELAY_SECONDS)))


__all__ = [
    "GOOGLE_PROBE_URL",
    "YoutubeChatEvent",
    "YoutubeChatSession",
    "YoutubeDanmuRelay",
    "YoutubeLiveInfo",
    "YoutubeProtocolError",
    "canonical_room_url",
    "extract_video_id",
    "probe_google_access",
]
