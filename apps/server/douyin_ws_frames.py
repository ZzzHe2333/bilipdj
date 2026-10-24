"""Decode Douyin IM WebSocket frames into the same events the userscript emits.

The headless collector captures frames at the browser level (CDP
``Network.webSocketFrameReceived``), so it never has to patch page JavaScript:
Douyin's own IM socket is read as it is, and the payloads are decoded here.

Frame layout (as used by the web client):

    PushFrame.payload (field 8) -> gzip -> LiveResponse
    LiveResponse.messagesList (field 1) -> Message{method, payload}

Gift payloads (``WebcastGiftMessage``) are walked field by field because the
checked-in ``douyin_live_pb2`` schema only covers the chat message. The field
numbers below match ``tools/douyin/douyin_live_gift_hook.user.js`` so both
transports produce identical bridge events.
"""
from __future__ import annotations

import gzip
import time
from typing import Any, Iterator

from . import douyin_live_pb2

MAX_FRAME_BYTES = 4 * 1024 * 1024
MAX_PAYLOAD_BYTES = 16 * 1024 * 1024
MAX_DEPTH = 8


class FrameDecodeError(ValueError):
    """Raised when a frame is not a decodable Douyin IM frame."""


def _varint(buf: bytes, start: int) -> tuple[int, int]:
    value = 0
    shift = 0
    pos = start
    for _ in range(10):
        if pos >= len(buf):
            raise FrameDecodeError("truncated varint")
        byte = buf[pos]
        pos += 1
        value |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return value, pos
        shift += 7
    raise FrameDecodeError("varint too long")


def _text(part: bytes) -> str | None:
    if not part:
        return None
    try:
        decoded = part.decode("utf-8")
    except UnicodeDecodeError:
        return None
    for char in decoded:
        if ord(char) < 32 and char not in "\t\n\r":
            return None
    return decoded.rstrip("\x00") or None


def _walk(buf: bytes, depth: int = 0) -> list[tuple[int, int, Any]]:
    """Return (field, wire_type, value) triples; length-delimited nests eagerly."""
    fields: list[tuple[int, int, Any]] = []
    pos = 0
    size = len(buf)
    while pos < size:
        tag, pos = _varint(buf, pos)
        number, wire = tag >> 3, tag & 7
        if not number or number > 100000:
            raise FrameDecodeError("invalid field number")
        if wire == 0:
            value, pos = _varint(buf, pos)
            fields.append((number, wire, value))
        elif wire == 2:
            length, pos = _varint(buf, pos)
            if length < 0 or pos + length > size:
                raise FrameDecodeError("invalid length")
            part = buf[pos:pos + length]
            pos += length
            nested: list[tuple[int, int, Any]] | None = None
            if depth < MAX_DEPTH and part and _text(part) is None:
                try:
                    candidate = _walk(part, depth + 1)
                    if candidate:
                        nested = candidate
                except FrameDecodeError:
                    nested = None
            fields.append((number, wire, part if nested is None else nested))
        elif wire == 5:
            if pos + 4 > size:
                raise FrameDecodeError("truncated fixed32")
            pos += 4
        elif wire == 1:
            if pos + 8 > size:
                raise FrameDecodeError("truncated fixed64")
            pos += 8
        else:
            raise FrameDecodeError("unsupported wire type")
    return fields


def _sub(fields: list[tuple[int, int, Any]], number: int) -> list[tuple[int, int, Any]]:
    for field, wire, value in fields:
        if field == number and wire == 2 and isinstance(value, list):
            return value
    return []


def _raw(fields: list[tuple[int, int, Any]], number: int) -> bytes | None:
    for field, wire, value in fields:
        if field == number and wire == 2 and isinstance(value, bytes):
            return value
    return None


def _str(fields: list[tuple[int, int, Any]], number: int) -> str | None:
    for field, wire, value in fields:
        if field == number and wire == 2 and isinstance(value, bytes):
            return _text(value)
        if field == number and wire == 2 and isinstance(value, list):
            return None
    return None


def _int(fields: list[tuple[int, int, Any]], number: int) -> int | None:
    for field, wire, value in fields:
        if field == number and wire == 0 and isinstance(value, int):
            return value
    return None


def _positive_int(fields: list[tuple[int, int, Any]], *numbers: int) -> int | None:
    for number in numbers:
        value = _int(fields, number)
        if value is not None and 0 < value <= 10_000_000:
            return value
    return None


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def frame_methods(frame: bytes) -> list[str]:
    """Method names carried by one IM frame (diagnostics only)."""
    return [method for method, _payload in iter_messages(frame)]


def iter_messages(frame: bytes) -> Iterator[tuple[str, bytes]]:
    """Yield (method, payload) for every message in one PushFrame."""
    if not frame or len(frame) > MAX_FRAME_BYTES:
        return
    push = douyin_live_pb2.PushFrame()
    try:
        push.ParseFromString(frame)
    except Exception as exc:  # noqa: BLE001 - untrusted browser payload
        raise FrameDecodeError("not a PushFrame") from exc
    payload = bytes(push.payload or b"")
    if not payload:
        return
    if payload[:2] == b"\x1f\x8b":
        try:
            payload = gzip.decompress(payload)
        except OSError as exc:
            raise FrameDecodeError("gzip payload invalid") from exc
        if len(payload) > MAX_PAYLOAD_BYTES:
            raise FrameDecodeError("payload too large")
    response = douyin_live_pb2.LiveResponse()
    try:
        response.ParseFromString(payload)
    except Exception as exc:  # noqa: BLE001
        raise FrameDecodeError("not a LiveResponse") from exc
    for item in response.messagesList:
        method = str(item.method or "")
        if method:
            yield method, bytes(item.payload or b"")


_DESCRIBE_RE = None


def _describe_parts(description: str | None) -> tuple[str | None, int | None, str | None]:
    """Split "nickname:送给主播 5 个 礼物名" into (nick, count, gift name)."""
    global _DESCRIBE_RE
    if not description:
        return None, None, None
    if _DESCRIBE_RE is None:
        import re
        _DESCRIBE_RE = re.compile(r"^(.*?)[:：]送给主播\s*(\d+)\s*个\s*(.+)$")
    match = _DESCRIBE_RE.match(description)
    if not match:
        return None, None, None
    return match.group(1), int(match.group(2)), match.group(3)


def decode_gift_message(payload: bytes) -> dict[str, Any] | None:
    """Decode one WebcastGiftMessage payload into the bridge event shape."""
    if not payload:
        return None
    try:
        fields = _walk(payload)
    except FrameDecodeError:
        return None
    common = _sub(fields, 1)
    user = _sub(fields, 7)
    gift_info = _sub(fields, 15)
    description = _str(common, 7)
    nick_from_text, count_from_text, name_from_text = _describe_parts(description)
    msg_id = _int(common, 2)
    room_id = _int(common, 3)
    gift_id = _int(fields, 2) or _int(gift_info, 5)
    repeat_count = _positive_int(fields, 5, 4)
    diamond = _positive_int(gift_info, 12, 9)
    gift_name = _str(gift_info, 16) or name_from_text
    user_id = _int(user, 1)
    nickname = _str(user, 3) or nick_from_text
    total = diamond * repeat_count if diamond and repeat_count else None
    return {
        "type": "WebcastGiftMessage",
        "receivedAt": int(time.time() * 1000),
        "msgId": _string_or_none(msg_id),
        "roomId": _string_or_none(room_id),
        "createTime": _int(common, 4),
        "giftId": gift_id,
        "groupCount": _int(fields, 4),
        "repeatCount": repeat_count,
        "comboCount": _int(fields, 6),
        "repeatEnd": _int(fields, 9),
        "groupId": _string_or_none(_int(fields, 10)),
        "describe": description,
        "sendCount": count_from_text,
        "user": {"id": _string_or_none(user_id), "nickname": nickname, "avatar": None},
        "gift": {
            "id": gift_id,
            "name": gift_name,
            "describe": _str(gift_info, 2),
            "diamondCount": diamond,
            "duration": _int(gift_info, 4),
        },
        "totalDiamond": total,
    }


def decode_frame_gifts(frame: bytes) -> list[dict[str, Any]]:
    """All gift events in one frame; malformed frames yield an empty list."""
    try:
        messages = list(iter_messages(frame))
    except FrameDecodeError:
        return []
    events: list[dict[str, Any]] = []
    for method, payload in messages:
        if method != "WebcastGiftMessage":
            continue
        event = decode_gift_message(payload)
        if event is not None:
            events.append(event)
    return events


__all__ = [
    "FrameDecodeError",
    "decode_frame_gifts",
    "decode_gift_message",
    "frame_methods",
    "iter_messages",
]
