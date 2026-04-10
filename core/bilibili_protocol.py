from __future__ import annotations

import base64
from copy import deepcopy
import datetime as dt
import io
import json
import logging
import random
import socket
import ssl
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zlib
from typing import Any

try:
    import qrcode
except ImportError:  # pragma: no cover
    qrcode = None

try:
    import brotli
except ImportError:  # pragma: no cover
    brotli = None

BILIBILI_QR_GENERATE_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/generate"
BILIBILI_QR_POLL_URL = "https://passport.bilibili.com/x/passport-login/web/qrcode/poll"
BILIBILI_NAV_URL = "https://api.bilibili.com/x/web-interface/nav"
BILIBILI_DANMU_CONF_URL = "https://api.live.bilibili.com/room/v1/Danmu/getConf"
BILIBILI_DANMU_INFO_URL = "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo"
BILIBILI_ROOM_INIT_URL = "https://api.live.bilibili.com/room/v1/Room/room_init"

DANMU_HEARTBEAT_INTERVAL_SECONDS = 30
DANMU_IDLE_RECONNECT_SECONDS = 90
MAX_SAFE_INTEGER = (1 << 53) - 1

_BILIBILI_INITIAL_MODEL: dict[str, Any] = {
    "model_name": "Bilibili Danmuji Initial Domain Model",
    "source_document": "Bilibili_Danmuji_流程与接口整理 (1).docx",
    "layers": {
        "接入层": ["Websocket", "WebSocketProxy", "HeartByteThread", "ReConnThread"],
        "协议层": ["HandleWebsocketPackage"],
        "业务分发层": ["ParseMessageThread"],
        "规则与聚合层": [
            "BlackParseComponent",
            "ShieldGiftTools",
            "ParseThankGiftThread",
            "ParseThankFollowThread",
            "ParseThankWelcomeThread",
        ],
        "发送层": ["SendBarrageThread", "HttpUserData.httpPostSendBarrage"],
        "本地前端层": ["DanmuWebsocket(/danmu/sub)"],
    },
    "startup_flow": [
        "读取配置并判断是否自动连接",
        "room_init/getInfoByRoom 拉取房间上下文",
        "getDanmuInfo 获取 websocket host_list 与 token",
        "构造首包认证并建立 websocket",
        "发送首次心跳并启动核心线程",
        "接收二进制包并进行协议解码",
        "按 cmd 分发到业务模块",
        "所有可发送文本统一进入 barrageString",
        "SendBarrageThread 按长度分片并限速发送",
        "异常关闭触发 ReConnThread 重连",
    ],
    "queue_contracts": {
        "resultStrs": "协议层解包后的 JSON 文本队列，供 ParseMessageThread 消费",
        "barrageString": "统一待发送弹幕队列，供 SendBarrageThread 独占消费",
    },
    "message_routes": [
        {
            "cmd": "DANMU_MSG:*",
            "normalized_cmd": "DANMU_MSG",
            "handlers": ["弹幕展示", "日志落地", "自动回复匹配", "前端广播"],
            "output_queue": "barrageString",
        },
        {
            "cmd": "SEND_GIFT/GUARD_BUY/SUPER_CHAT_MESSAGE/POPULARITY_RED_POCKET_NEW",
            "normalized_cmd": "GIFT_RELATED",
            "handlers": ["礼物过滤", "礼物延时聚合"],
            "output_queue": "barrageString",
        },
        {
            "cmd": "INTERACT_WORD/INTERACT_WORD_V2",
            "normalized_cmd": "INTERACT",
            "handlers": ["关注感谢", "欢迎感谢"],
            "output_queue": "barrageString",
        },
        {
            "cmd": "LIVE/PREPARING",
            "normalized_cmd": "LIVE_STATUS",
            "handlers": ["更新直播状态", "收敛后台线程"],
            "output_queue": None,
        },
    ],
    "aggregations": [
        {
            "name": "礼物感谢聚合",
            "container": "thankGiftConcurrentHashMap",
            "key_strategy": "用户名 + 礼物名",
            "window_strategy": "延时窗口内累加数量/总额，新事件刷新时间戳",
            "output_template_modes": ["单人单种", "单人多种", "多人多种"],
        },
        {
            "name": "关注感谢聚合",
            "container": "interacts",
            "key_strategy": "顺序列表按批次切片",
            "window_strategy": "窗口结束后按 num 将多个用户合并成一条感谢",
            "output_template_modes": [],
        },
        {
            "name": "欢迎感谢聚合",
            "container": "interactWelcome",
            "key_strategy": "顺序列表按批次切片",
            "window_strategy": "窗口结束后按 num 合并欢迎文本",
            "output_template_modes": [],
        },
    ],
    "endpoints": [
        {
            "name": "房间初始化",
            "method": "GET",
            "url": "https://api.live.bilibili.com/room/v1/Room/room_init?id={roomid}",
            "purpose": "短号解析为真实 room_id，并读取直播状态等上下文",
            "tier": "主流程必经",
        },
        {
            "name": "WS 配置",
            "method": "GET",
            "url": "https://api.live.bilibili.com/xlive/web-room/v1/index/getDanmuInfo",
            "purpose": "获取 websocket host_list 与 token",
            "tier": "主流程必经",
        },
        {
            "name": "发送弹幕",
            "method": "POST",
            "url": "https://api.live.bilibili.com/msg/send",
            "purpose": "统一弹幕发送出口",
            "tier": "高频运行期",
        },
        {
            "name": "获取抽奖信息",
            "method": "GET",
            "url": "https://api.live.bilibili.com/xlive/lottery-interface/v1/lottery/getLotteryInfoWeb?roomid={roomid}",
            "purpose": "供红包/天选屏蔽逻辑使用",
            "tier": "高频运行期",
        },
        {
            "name": "签到",
            "method": "GET",
            "url": "https://api.live.bilibili.com/xlive/web-ucenter/v1/sign/DoSign",
            "purpose": "每日签到任务",
            "tier": "可选扩展",
        },
        {
            "name": "本地订阅 WS",
            "method": "WS",
            "url": "/danmu/sub",
            "purpose": "浏览器订阅处理结果并可回传文本代发弹幕",
            "tier": "主流程必经",
        },
    ],
    "threads": [
        {"name": "HeartByteThread", "trigger": "建链成功后", "responsibility": "每 30 秒发送心跳包"},
        {"name": "ParseMessageThread", "trigger": "建链成功后", "responsibility": "消费 resultStrs 并按 cmd 分发"},
        {"name": "SendBarrageThread", "trigger": "存在待发文本时", "responsibility": "消费 barrageString 并做分条限速发送"},
        {"name": "ParseThankGiftThread", "trigger": "礼物缓存有新数据", "responsibility": "在窗口结束后生成感谢弹幕"},
        {"name": "ParseThankFollowThread", "trigger": "关注缓存有新数据", "responsibility": "按人数分组输出关注感谢"},
        {"name": "ParseThankWelcomeThread", "trigger": "访客缓存有新数据", "responsibility": "按人数分组输出欢迎文本"},
        {"name": "AutoReplyThread", "trigger": "收到匹配型弹幕后", "responsibility": "按关键词规则生成自动回复"},
        {"name": "AdvertThread", "trigger": "广告功能开启后", "responsibility": "按固定/随机间隔投喂广告文案"},
        {"name": "ReConnThread", "trigger": "连接关闭后", "responsibility": "按重试策略执行重连"},
    ],
    "sending_policy": {
        "single_writer_thread": "SendBarrageThread",
        "rate_limit_interval_ms": 1455,
        "split_strategy": "按当前用户可发送最大弹幕长度分段",
        "risk_control_recommendation": "后续若做全局风控，优先在发送层统一实现",
    },
}


def get_initial_model() -> dict[str, Any]:
    return deepcopy(_BILIBILI_INITIAL_MODEL)


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _get_runtime_bilibili_config(runtime_config: Any) -> dict[str, Any]:
    defaults = {"roomid": 0, "uid": 0, "cookie": ""}
    if not isinstance(runtime_config, dict):
        return defaults

    merged = dict(defaults)
    legacy_api_cfg = runtime_config.get("api", {})
    bilibili_cfg = runtime_config.get("bilibili", {})
    if isinstance(legacy_api_cfg, dict):
        if "roomid" in legacy_api_cfg:
            merged["roomid"] = _to_int(legacy_api_cfg.get("roomid", merged["roomid"]), merged["roomid"])
        if "uid" in legacy_api_cfg:
            merged["uid"] = _to_int(legacy_api_cfg.get("uid", merged["uid"]), merged["uid"])
        if "cookie" in legacy_api_cfg:
            merged["cookie"] = str(legacy_api_cfg.get("cookie", merged["cookie"]) or "")
    if isinstance(bilibili_cfg, dict):
        if "roomid" in bilibili_cfg:
            bilibili_roomid = _to_int(bilibili_cfg.get("roomid", defaults["roomid"]), defaults["roomid"])
            if bilibili_roomid != defaults["roomid"] or merged["roomid"] == defaults["roomid"] or not isinstance(legacy_api_cfg, dict) or "roomid" not in legacy_api_cfg:
                merged["roomid"] = bilibili_roomid
        if "uid" in bilibili_cfg:
            bilibili_uid = _to_int(bilibili_cfg.get("uid", defaults["uid"]), defaults["uid"])
            if bilibili_uid != defaults["uid"] or merged["uid"] == defaults["uid"] or not isinstance(legacy_api_cfg, dict) or "uid" not in legacy_api_cfg:
                merged["uid"] = bilibili_uid
        if "cookie" in bilibili_cfg:
            bilibili_cookie = str(bilibili_cfg.get("cookie", defaults["cookie"]) or "")
            if bilibili_cookie != str(defaults["cookie"] or "") or not str(merged.get("cookie", "") or "").strip() or not isinstance(legacy_api_cfg, dict) or "cookie" not in legacy_api_cfg:
                merged["cookie"] = bilibili_cookie
    return merged


def _ws_recv_exact(conn: socket.socket, size: int) -> bytes | None:
    chunks = bytearray()
    while len(chunks) < size:
        try:
            chunk = conn.recv(size - len(chunks))
        except (TimeoutError, socket.timeout):
            raise TimeoutError("socket recv timeout")
        if not chunk:
            return None
        chunks.extend(chunk)
    return bytes(chunks)


def _extract_cookie_string(set_cookie_headers: list[str]) -> str:
    cookie_pairs: list[str] = []
    for header in set_cookie_headers:
        first_part = header.split(";", 1)[0].strip()
        if "=" not in first_part:
            continue
        cookie_pairs.append(first_part)
    return "; ".join(cookie_pairs)


def _parse_cookie_pairs(cookie_text: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for item in str(cookie_text or "").split(";"):
        part = item.strip()
        if not part or "=" not in part:
            continue
        key, value = part.split("=", 1)
        key = key.strip()
        if not key:
            continue
        pairs[key] = value.strip()
    return pairs


def _extract_uid_from_cookie(cookie_text: str) -> int:
    return _to_int(_parse_cookie_pairs(cookie_text).get("DedeUserID", 0))


def _is_plausible_bilibili_uid(uid: int) -> bool:
    return 0 < uid <= MAX_SAFE_INTEGER


def _build_bilibili_www_headers(cookie: str = "") -> dict[str, str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": "https://www.bilibili.com/",
        "Origin": "https://www.bilibili.com",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def _build_bilibili_live_headers(roomid: int = 0, cookie: str = "") -> dict[str, str]:
    referer = "https://live.bilibili.com/"
    if roomid > 0:
        referer = f"https://live.bilibili.com/{roomid}"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/135.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "Referer": referer,
        "Origin": "https://live.bilibili.com",
    }
    if cookie:
        headers["Cookie"] = cookie
    return headers


def bilibili_qr_generate() -> dict[str, Any]:
    req = urllib.request.Request(
        BILIBILI_QR_GENERATE_URL,
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bilibili.com/",
            "Origin": "https://www.bilibili.com",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    return payload


def bilibili_qr_poll(qrcode_key: str) -> tuple[dict[str, Any], str]:
    query = urllib.parse.urlencode({"qrcode_key": qrcode_key})
    req = urllib.request.Request(
        f"{BILIBILI_QR_POLL_URL}?{query}",
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://www.bilibili.com/",
            "Origin": "https://www.bilibili.com",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        payload = json.loads(resp.read().decode("utf-8", errors="replace"))
        raw_cookie_headers = resp.headers.get_all("Set-Cookie") or []
    return payload, _extract_cookie_string(raw_cookie_headers)


def _bilibili_get_nav_info(cookie: str) -> dict[str, Any]:
    req = urllib.request.Request(BILIBILI_NAV_URL, headers=_build_bilibili_www_headers(cookie))
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def resolve_bilibili_login(
    cookie: str,
    fallback_uid: int = 0,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    resolved_uid = max(0, _to_int(fallback_uid))
    if not _is_plausible_bilibili_uid(resolved_uid):
        resolved_uid = 0
    resolved = {
        "uid": resolved_uid,
        "uname": "",
        "is_login": False,
        "uid_source": "config" if resolved_uid > 0 else "",
    }
    cookie_uid = _extract_uid_from_cookie(cookie)
    if _is_plausible_bilibili_uid(cookie_uid):
        resolved["uid"] = cookie_uid
        resolved["uid_source"] = "cookie"
    elif cookie_uid > 0 and logger is not None:
        logger.warning("Ignoring implausible DedeUserID from cookie: %s", cookie_uid)
    if not str(cookie or "").strip():
        return resolved
    try:
        payload = _bilibili_get_nav_info(cookie)
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        if logger is not None:
            logger.warning("Bilibili nav lookup failed; falling back to local cookie/uid: %s", exc)
        return resolved
    if _to_int(payload.get("code", -1), -1) != 0:
        if logger is not None:
            logger.warning("Bilibili nav returned unexpected payload; falling back to local cookie/uid: %s", payload)
        return resolved
    data = payload.get("data", {})
    if not isinstance(data, dict):
        return resolved
    nav_uid = _to_int(data.get("mid", 0))
    if _is_plausible_bilibili_uid(nav_uid):
        resolved["uid"] = nav_uid
        resolved["uid_source"] = "nav"
    elif nav_uid > 0 and logger is not None:
        logger.warning("Ignoring implausible Bilibili nav uid: %s", nav_uid)
    resolved["uname"] = str(data.get("uname", "") or "")
    resolved["is_login"] = bool(data.get("isLogin", False))
    return resolved


def build_qr_png_base64(text: str) -> tuple[str, str]:
    if not text:
        return "", "二维码内容为空"
    if qrcode is None:
        return "", "缺少依赖 qrcode，请先安装：pip install qrcode[pil]"
    qr = qrcode.QRCode(version=1, box_size=10, border=2)
    qr.add_data(text)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white")
    buf = io.BytesIO()
    img.save(buf, format="PNG")
    encoded = base64.b64encode(buf.getvalue()).decode("ascii")
    return encoded, ""


def _bilibili_get_danmu_info(roomid: int, cookie: str = "") -> dict[str, Any]:
    query = urllib.parse.urlencode({"id": roomid, "type": 0})
    req = urllib.request.Request(
        f"{BILIBILI_DANMU_INFO_URL}?{query}",
        headers=_build_bilibili_live_headers(roomid, cookie),
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _bilibili_room_init(roomid: int, cookie: str = "") -> dict[str, Any]:
    query = urllib.parse.urlencode({"id": roomid})
    req = urllib.request.Request(
        f"{BILIBILI_ROOM_INIT_URL}?{query}",
        headers=_build_bilibili_live_headers(roomid, cookie),
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _bilibili_get_danmu_conf(roomid: int, cookie: str = "") -> dict[str, Any]:
    query = urllib.parse.urlencode({"room_id": roomid, "platform": "pc", "player": "web"})
    req = urllib.request.Request(
        f"{BILIBILI_DANMU_CONF_URL}?{query}",
        headers=_build_bilibili_live_headers(roomid, cookie),
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _bilibili_get_danmu_server_info(roomid: int, cookie: str = "") -> dict[str, Any]:
    payload = _bilibili_get_danmu_conf(roomid, cookie)
    if _to_int(payload.get("code", -1), -1) == 0:
        return payload
    legacy_payload = _bilibili_get_danmu_info(roomid, cookie)
    if _to_int(legacy_payload.get("code", -1), -1) != 0:
        return payload
    legacy_data = legacy_payload.get("data", {}) if isinstance(legacy_payload, dict) else {}
    if not isinstance(legacy_data, dict):
        return payload
    normalized_data = dict(legacy_data)
    if "host_server_list" not in normalized_data and isinstance(normalized_data.get("host_list"), list):
        normalized_data["host_server_list"] = normalized_data.get("host_list", [])
    return {"code": 0, "msg": "ok", "message": "ok", "data": normalized_data}


class BilibiliDanmuRelay(threading.Thread):
    def __init__(self, server: Any) -> None:
        super().__init__(name="bilibili-danmu-relay", daemon=True)
        self.server = server
        self.logger = server.logger
        self._stop_event = threading.Event()
        self._reconnect_event = threading.Event()
        self._seen_event_cmds: set[str] = set()
        self._status_lock = threading.Lock()
        self._connected = False
        self._last_packet_monotonic = 0.0
        self._last_packet_at = ""
        self._last_connect_at = ""
        self._last_disconnect_at = ""
        self._last_disconnect_reason = ""
        self._current_roomid = 0
        self._current_host = ""
        self._current_port = 0
        self._current_transport = ""
        self._current_auth_uid = 0

    def stop(self) -> None:
        self._stop_event.set()
        self._reconnect_event.set()

    def request_reconnect(self) -> None:
        self._reconnect_event.set()

    def _emit_status(self, status: str, **extra: Any) -> None:
        payload = {"type": "PDJ_STATUS", "status": status}
        payload.update(extra)
        self.server.ws_hub.broadcast_json(None, payload)

    def _pack_packet(self, body: bytes, operation: int, version: int = 1) -> bytes:
        header_len = 16
        packet_len = header_len + len(body)
        return struct.pack("!IHHII", packet_len, header_len, version, operation, 1) + body

    def _send_auth(self, conn: socket.socket, roomid: int, uid: int, token: str) -> None:
        protover = 3 if brotli is not None else 2
        auth_payload = {
            "uid": uid,
            "roomid": roomid,
            "protover": protover,
            "platform": "web",
            "type": 2,
            "key": token,
        }
        body = json.dumps(auth_payload, ensure_ascii=False).encode("utf-8")
        conn.sendall(self._pack_packet(body, operation=7, version=1))

    def _send_heartbeat(self, conn: socket.socket) -> None:
        conn.sendall(self._pack_packet(b"[object Object]", operation=2, version=1))

    def _mark_packet(self) -> None:
        with self._status_lock:
            self._last_packet_monotonic = time.monotonic()
            self._last_packet_at = dt.datetime.now(dt.timezone.utc).isoformat()

    def _mark_connected(
        self,
        *,
        roomid: int,
        host: str,
        port: int,
        transport: str,
        auth_uid: int,
    ) -> None:
        now = dt.datetime.now(dt.timezone.utc).isoformat()
        with self._status_lock:
            self._connected = True
            self._last_connect_at = now
            self._last_disconnect_reason = ""
            self._current_roomid = roomid
            self._current_host = host
            self._current_port = port
            self._current_transport = transport
            self._current_auth_uid = auth_uid
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
        return {
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
        }

    def _log_business_message(self, text: str) -> None:
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            return
        if not isinstance(payload, dict):
            return
        cmd = str(payload.get("cmd", "") or "").strip()
        if not cmd or cmd.startswith("DANMU_MSG"):
            return
        if cmd in self._seen_event_cmds:
            return
        self._seen_event_cmds.add(cmd)
        data = payload.get("data", {})
        if isinstance(data, dict) and isinstance(data.get("pb"), str):
            self.logger.info("Received live room event cmd=%s (protobuf payload)", cmd)
            return
        self.logger.info("Received live room event cmd=%s", cmd)

    def _iter_business_messages(self, packet_data: bytes) -> list[str]:
        messages: list[str] = []
        offset = 0
        total = len(packet_data)
        while offset + 16 <= total:
            packet_len, header_len, version, operation, _ = struct.unpack(
                "!IHHII", packet_data[offset : offset + 16]
            )
            if packet_len <= 0 or offset + packet_len > total:
                break
            body = packet_data[offset + header_len : offset + packet_len]
            offset += packet_len
            if operation != 5:
                continue
            if version in {0, 1}:
                text = body.decode("utf-8", errors="replace").strip()
                if text:
                    messages.append(text)
            elif version == 2:
                try:
                    messages.extend(self._iter_business_messages(zlib.decompress(body)))
                except zlib.error:
                    self.logger.debug("Danmu packet zlib decompress failed")
            elif version == 3:
                if brotli is None:
                    self.logger.debug("Received brotli danmu packet but brotli is not installed")
                    continue
                try:
                    messages.extend(self._iter_business_messages(brotli.decompress(body)))
                except Exception:
                    self.logger.debug("Danmu packet brotli decompress failed")
        return messages

    def _wait_auth_result(self, conn: socket.socket) -> None:
        header = _ws_recv_exact(conn, 16)
        if not header:
            raise ConnectionError("danmu auth connection closed")
        packet_len, header_len, _, operation, _ = struct.unpack("!IHHII", header)
        if packet_len < header_len or header_len < 16:
            raise ConnectionError("invalid danmu auth packet")
        body = _ws_recv_exact(conn, packet_len - 16)
        if body is None:
            raise ConnectionError("empty danmu auth packet")
        if operation != 8:
            raise ConnectionError(f"unexpected danmu auth op={operation}")
        payload = json.loads(body.decode("utf-8", errors="replace") or "{}")
        if _to_int(payload.get("code", -1), -1) != 0:
            raise ConnectionError(f"danmu auth failed: {payload}")
        self.logger.info("Danmu auth succeeded")
        self._emit_status("danmu_auth_ok")

    def _iter_auth_uid_candidates(self, auth_uid: int, configured_uid: int) -> list[int]:
        candidates: list[int] = []
        for value in [auth_uid, configured_uid, 0]:
            uid = _to_int(value, 0)
            if uid < 0:
                continue
            if uid != 0 and not _is_plausible_bilibili_uid(uid):
                continue
            if uid not in candidates:
                candidates.append(uid)
        return candidates or [0]

    def _normalize_host_candidates(self, data: dict[str, Any]) -> list[dict[str, Any]]:
        candidates: list[dict[str, Any]] = []
        seen: set[tuple[str, int, str]] = set()
        for key in ("server_list", "host_server_list", "host_list"):
            items = data.get(key, [])
            if not isinstance(items, list):
                continue
            for item in items:
                if not isinstance(item, dict):
                    continue
                host = str(item.get("host", "")).strip()
                if not host:
                    continue
                for transport_key, transport_name in (("port", "tcp"), ("wss_port", "tls"), ("ws_port", "tcp")):
                    port = _to_int(item.get(transport_key, 0), 0)
                    if port <= 0:
                        continue
                    dedupe_key = (host, port, transport_name)
                    if dedupe_key in seen:
                        continue
                    seen.add(dedupe_key)
                    candidates.append({"host": host, "port": port, "transport": transport_name})
        random.shuffle(candidates)
        candidates.sort(
            key=lambda item: (
                0 if item.get("transport") == "tcp" else 1,
                0 if _to_int(item.get("port", 0), 0) == 80 else 1,
                0 if _to_int(item.get("port", 0), 0) == 443 else 1,
            )
        )
        return candidates

    def _open_danmu_socket(self, host: str, port: int, transport: str) -> socket.socket:
        raw_conn = socket.create_connection((host, port), timeout=10)
        raw_conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if transport == "tls":
            context = ssl.create_default_context()
            return context.wrap_socket(raw_conn, server_hostname=host)
        return raw_conn

    def _connect_and_stream_v2(
        self,
        *,
        roomid: int,
        configured_uid: int,
        cookie: str,
        initial_auth_uid: int,
    ) -> None:
        self.logger.info("Starting danmu server discovery, roomid=%s", roomid)
        real_room_id = roomid
        for candidate_room_id, candidate_cookie, mode in [
            (roomid, cookie, "room_init+cookie"),
            (roomid, "", "room_init+no_cookie"),
        ]:
            try:
                room_init_payload = _bilibili_room_init(candidate_room_id, candidate_cookie)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("room_init failed (%s): %s", mode, exc)
                continue
            room_init_data = room_init_payload.get("data", {}) if isinstance(room_init_payload, dict) else {}
            init_real_room_id = _to_int(room_init_data.get("room_id", candidate_room_id), candidate_room_id)
            if init_real_room_id > 0:
                real_room_id = init_real_room_id
                if real_room_id != roomid:
                    self.logger.info("room_init resolved real room id: %s -> %s", roomid, real_room_id)
                break

        if cookie:
            request_candidates: list[tuple[int, str, str]] = [(roomid, cookie, "id+cookie")]
            if real_room_id != roomid:
                request_candidates.append((real_room_id, cookie, "real_id+cookie"))
            request_candidates.append((roomid, "", "id+no_cookie"))
            if real_room_id != roomid:
                request_candidates.append((real_room_id, "", "real_id+no_cookie"))
        else:
            request_candidates = [(roomid, "", "id+no_cookie")]
            if real_room_id != roomid:
                request_candidates.append((real_room_id, "", "real_id+no_cookie"))

        payload: dict[str, Any] | None = None
        discovery_candidates: list[dict[str, Any]] = []
        for candidate_room_id, candidate_cookie, mode in request_candidates:
            try:
                payload = _bilibili_get_danmu_server_info(candidate_room_id, candidate_cookie)
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("danmu server info request failed (%s): %s", mode, exc)
                continue
            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            code = _to_int(payload.get("code", -1), -1) if isinstance(payload, dict) else -1
            candidate_token = str(data.get("token", "")) if isinstance(data, dict) else ""
            candidate_hosts = self._normalize_host_candidates(data) if isinstance(data, dict) else []
            if code == 0 and candidate_token and candidate_hosts:
                discovery_candidates.append(
                    {
                        "token": candidate_token,
                        "host_candidates": candidate_hosts,
                        "room_id": _to_int(data.get("room_id", candidate_room_id), candidate_room_id),
                        "mode": mode,
                    }
                )
                continue
            self.logger.warning("danmu server info failed (%s): %s", mode, payload)

        if not discovery_candidates:
            raise RuntimeError(f"danmu server info failed: {payload}")

        auth_uid_candidates = self._iter_auth_uid_candidates(initial_auth_uid, configured_uid)
        conn: socket.socket | None = None
        host = ""
        port = 0
        transport = ""
        last_error: Exception | None = None
        selected_auth_uid = 0
        selected_room_id = real_room_id
        for discovery in discovery_candidates:
            token = str(discovery.get("token", "") or "")
            host_candidates = list(discovery.get("host_candidates", []))
            selected_room_id = _to_int(discovery.get("room_id", real_room_id), real_room_id)
            discovery_mode = str(discovery.get("mode", "") or "")
            if discovery_mode.endswith("no_cookie"):
                self.logger.warning("Danmu server discovery fell back to no-cookie mode")
            else:
                self.logger.info("Danmu server discovery using authenticated mode: %s", discovery_mode)
            for candidate in host_candidates:
                host = str(candidate.get("host", "")).strip()
                port = _to_int(candidate.get("port", 0), 0)
                transport = str(candidate.get("transport", "tcp") or "tcp")
                if not host or port <= 0:
                    continue
                self._emit_status(
                    "danmu_connecting",
                    roomid=selected_room_id,
                    host=host,
                    port=port,
                    transport=transport,
                )
                for candidate_auth_uid in auth_uid_candidates:
                    try:
                        self.logger.info(
                            "Trying danmu server %s://%s:%s with auth uid=%s discovery=%s",
                            transport,
                            host,
                            port,
                            candidate_auth_uid,
                            discovery_mode or "unknown",
                        )
                        conn = self._open_danmu_socket(host, port, transport)
                        conn.settimeout(5.0)
                        self._send_auth(conn, selected_room_id, candidate_auth_uid, token)
                        self._wait_auth_result(conn)
                        selected_auth_uid = candidate_auth_uid
                        conn.settimeout(1.0)
                        break
                    except Exception as exc:  # noqa: BLE001
                        last_error = exc
                        self.logger.warning(
                            "danmu connect/auth failed %s://%s:%s uid=%s discovery=%s: %s",
                            transport,
                            host,
                            port,
                            candidate_auth_uid,
                            discovery_mode or "unknown",
                            exc,
                        )
                        if conn is not None:
                            try:
                                conn.close()
                            except OSError:
                                pass
                        conn = None
                if conn is not None:
                    break
            if conn is not None:
                break

        if conn is None:
            raise ConnectionError(f"all danmu server candidates failed: {last_error}")

        try:
            self._send_heartbeat(conn)
            self._mark_connected(
                roomid=selected_room_id,
                host=host,
                port=port,
                transport=transport,
                auth_uid=selected_auth_uid,
            )
            self._emit_status(
                "danmu_connected",
                roomid=selected_room_id,
                host=host,
                port=port,
                transport=transport,
                auth_uid=selected_auth_uid,
            )
            self.logger.info(
                "Danmu connected roomid=%s transport=%s host=%s port=%s auth_uid=%s",
                selected_room_id,
                transport,
                host,
                port,
                selected_auth_uid,
            )
            next_heartbeat = time.time() + DANMU_HEARTBEAT_INTERVAL_SECONDS
            while not self._stop_event.is_set():
                if self._reconnect_event.is_set():
                    self._reconnect_event.clear()
                    self.logger.info("Received reconnect signal; restarting danmu stream")
                    break
                if time.time() >= next_heartbeat:
                    self._send_heartbeat(conn)
                    next_heartbeat = time.time() + DANMU_HEARTBEAT_INTERVAL_SECONDS
                ok = self._recv_and_handle(conn)
                if not ok:
                    self._mark_disconnected("danmu stream disconnected")
                    raise ConnectionError("danmu stream disconnected")
                idle_seconds = self.get_runtime_status().get("idle_seconds")
                if idle_seconds is not None and idle_seconds >= DANMU_IDLE_RECONNECT_SECONDS:
                    self._mark_disconnected(
                        f"danmu stream idle timeout ({int(idle_seconds)}s without packets)"
                    )
                    raise ConnectionError(
                        f"danmu stream idle timeout ({int(idle_seconds)}s without packets)"
                    )
        finally:
            self._mark_disconnected("connection closed")
            try:
                conn.close()
            except OSError:
                pass

    def _recv_and_handle(self, conn: socket.socket) -> bool:
        try:
            header = _ws_recv_exact(conn, 16)
        except TimeoutError:
            return True
        if not header:
            return False
        packet_len, header_len, _, operation, _ = struct.unpack("!IHHII", header)
        if packet_len < header_len or header_len < 16:
            return False
        try:
            body = _ws_recv_exact(conn, packet_len - 16)
        except TimeoutError:
            return True
        if body is None:
            return False
        packet_data = header + body
        self._mark_packet()
        if operation == 8:
            self.logger.info("Danmu auth succeeded")
            self._emit_status("danmu_auth_ok")
            return True
        if operation == 3 and len(body) >= 4:
            popularity = struct.unpack("!I", body[:4])[0]
            self.logger.info("Realtime popularity: %s", popularity)
            self._emit_status("popularity", popularity=popularity)
            return True
        for text in self._iter_business_messages(packet_data):
            self._log_business_message(text)
            self.server.ws_hub.mark_message()
            try:
                parsed_msg = json.loads(text)
            except json.JSONDecodeError:
                self.server.ws_hub.broadcast_text(None, text)
                continue
            cmd = str(parsed_msg.get("cmd", "") or "").strip() if isinstance(parsed_msg, dict) else ""
            if cmd.startswith("DANMU_MSG"):
                if hasattr(self.server, "queue_manager"):
                    self.server.queue_manager.process_danmu_json(parsed_msg)
            else:
                self.server.ws_hub.broadcast_text(None, text)
        return True

    def _connect_and_stream(self) -> None:
        cfg = _get_runtime_bilibili_config(getattr(self.server, "runtime_config", {}))
        roomid = int(cfg.get("roomid", 0))
        uid = int(cfg.get("uid", 0))
        cookie = str(cfg.get("cookie", "")).strip()
        login_state = resolve_bilibili_login(cookie, fallback_uid=uid, logger=self.logger if cookie else None)
        auth_uid = _to_int(login_state.get("uid", 0))
        if cookie and auth_uid > 0 and auth_uid != uid:
            cfg["uid"] = auth_uid
            self.logger.info(
                "Detected logged-in Bilibili UID=%s (source=%s); using it for danmu auth",
                auth_uid,
                login_state.get("uid_source", "unknown"),
            )
        elif cookie and auth_uid <= 0 and uid <= 0:
            self.logger.warning("Cookie is configured but login UID could not be resolved; falling back to guest/default UID")
        if roomid <= 0:
            self.logger.info("roomid is not configured yet; skipping danmu connection")
            self._emit_status("danmu_waiting_config", message="roomid not configured")
            time.sleep(3)
            return
        self._connect_and_stream_v2(
            roomid=roomid,
            configured_uid=uid,
            cookie=cookie,
            initial_auth_uid=auth_uid,
        )

    def run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._connect_and_stream()
            except Exception as exc:  # noqa: BLE001
                self.logger.warning("Danmu connection error: %s", exc)
                self._emit_status("danmu_disconnected", error=str(exc))
                time.sleep(2)


__all__ = [
    "BilibiliDanmuRelay",
    "bilibili_qr_generate",
    "bilibili_qr_poll",
    "build_qr_png_base64",
    "get_initial_model",
    "resolve_bilibili_login",
]
