"""Platform-independent danmu event model shared by built-in and external relays."""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Mapping

_PLATFORM_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{1,39}$")
MAX_USER_ID_CHARS = 256
MAX_USERNAME_CHARS = 256
MAX_CONTENT_CHARS = 4096
MAX_METADATA_BYTES = 64 * 1024
MAX_LEGACY_UID = (1 << 63) - 1


def _bounded_text(value: Any, *, label: str, max_chars: int, required: bool = False) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise ValueError(f"{label} is required")
    if len(text) > max_chars:
        raise ValueError(f"{label} exceeds {max_chars} characters")
    return text


def _json_object(value: Any, *, label: str, max_bytes: int = MAX_METADATA_BYTES) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise TypeError(f"{label} must be an object")
    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label} must be JSON-serializable") from exc
    if len(encoded) > max_bytes:
        raise ValueError(f"{label} exceeds {max_bytes} bytes")
    return json.loads(encoded.decode("utf-8"))


def _legacy_uid(platform: str, user_id: str, username: str) -> int:
    if user_id.isdigit():
        numeric = int(user_id)
        if 0 < numeric <= MAX_LEGACY_UID:
            return numeric
    seed = f"{platform}\0{user_id or username}".encode("utf-8")
    digest = hashlib.blake2b(seed, digest_size=8).digest()
    value = int.from_bytes(digest, "big") & MAX_LEGACY_UID
    return value or 1


@dataclass(slots=True)
class DanmuEvent:
    platform: str
    user_id: str
    username: str
    content: str
    is_room_admin: bool = False
    is_anchor: bool = False
    is_guard: bool = False
    guard_level: int = 0
    guard_name: str = ""
    fan_medal: dict[str, Any] = field(default_factory=dict)
    received_at: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.platform = _bounded_text(self.platform, label="platform", max_chars=40, required=True).lower()
        if not _PLATFORM_RE.fullmatch(self.platform):
            raise ValueError("platform contains unsupported characters")
        self.user_id = _bounded_text(self.user_id, label="user_id", max_chars=MAX_USER_ID_CHARS)
        self.username = _bounded_text(self.username, label="username", max_chars=MAX_USERNAME_CHARS, required=True)
        self.content = _bounded_text(self.content, label="content", max_chars=MAX_CONTENT_CHARS, required=True)
        self.is_room_admin = bool(self.is_room_admin)
        self.is_anchor = bool(self.is_anchor)
        self.is_guard = bool(self.is_guard)
        try:
            self.guard_level = max(0, int(self.guard_level or 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("guard_level must be an integer") from exc
        self.guard_name = _bounded_text(self.guard_name, label="guard_name", max_chars=128)
        self.fan_medal = _json_object(self.fan_medal, label="fan_medal", max_bytes=16 * 1024)
        self.metadata = _json_object(self.metadata, label="metadata")
        self.received_at = _bounded_text(self.received_at, label="received_at", max_chars=128)
        if not self.received_at:
            self.received_at = dt.datetime.now(dt.timezone.utc).isoformat()

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any], *, default_platform: str = "") -> "DanmuEvent":
        if not isinstance(value, Mapping):
            raise TypeError("danmu event must be an object")
        return cls(
            platform=value.get("platform") or default_platform,
            user_id=str(value.get("user_id", value.get("uid", "")) or ""),
            username=value.get("username", value.get("uname", "")),
            content=value.get("content", value.get("message", "")),
            is_room_admin=value.get("is_room_admin", value.get("is_admin", False)),
            is_anchor=value.get("is_anchor", False),
            is_guard=value.get("is_guard", False),
            guard_level=value.get("guard_level", 0),
            guard_name=value.get("guard_name", ""),
            fan_medal=value.get("fan_medal", {}),
            received_at=value.get("received_at", value.get("time", "")),
            metadata=value.get("metadata", {}),
        )

    def legacy_uid(self) -> int:
        return _legacy_uid(self.platform, self.user_id, self.username)

    def identity_dict(self) -> dict[str, Any]:
        base = self.metadata.get("identity")
        identity = deepcopy(base) if isinstance(base, dict) else {}
        identity.update(
            {
                "uid": self.legacy_uid(),
                "user_id": self.user_id,
                "uname": self.username,
                "username": self.username,
                "is_room_admin": self.is_room_admin,
                "is_anchor": self.is_anchor,
                "is_guard": self.is_guard,
                "guard_level": self.guard_level,
                "guard_name": self.guard_name,
                "fan_medal": deepcopy(self.fan_medal),
                "has_fan_medal": bool(self.fan_medal),
            }
        )
        return identity

    def to_dict(self) -> dict[str, Any]:
        return {
            "platform": self.platform,
            "user_id": self.user_id,
            "username": self.username,
            "content": self.content,
            "is_room_admin": self.is_room_admin,
            "is_anchor": self.is_anchor,
            "is_guard": self.is_guard,
            "guard_level": self.guard_level,
            "guard_name": self.guard_name,
            "fan_medal": deepcopy(self.fan_medal),
            "received_at": self.received_at,
            "metadata": deepcopy(self.metadata),
        }


__all__ = ["DanmuEvent", "MAX_METADATA_BYTES"]
