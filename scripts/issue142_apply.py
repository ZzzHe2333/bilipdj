from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(path: str) -> str:
    return (ROOT / path).read_text(encoding="utf-8")


def write(path: str, text: str) -> None:
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text, encoding="utf-8")


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"{path}: expected one exact match, got {count}: {old[:120]!r}")
    write(path, text.replace(old, new, 1))


def regex_once(path: str, pattern: str, replacement: str) -> None:
    text = read(path)
    updated, count = re.subn(pattern, replacement, text, count=1, flags=re.DOTALL)
    if count != 1:
        raise RuntimeError(f"{path}: expected one regex match, got {count}: {pattern[:120]!r}")
    write(path, updated)


DANMU_EVENT_SOURCE = '''\
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
    seed = f"{platform}\\0{user_id or username}".encode("utf-8")
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
'''
write("apps/server/danmu_event.py", DANMU_EVENT_SOURCE)

# QueueManager: import the canonical model and make it the authoritative business path.
replace_once(
    "apps/server/server.py",
    '''if __package__:\n    from . import bilibili_protocol, douyin_protocol\n    from .bilibili_gifts import GIFT_BATTERIES, batteries_for_gift\nelse:\n    import bilibili_protocol\n    import douyin_protocol\n    from bilibili_gifts import GIFT_BATTERIES, batteries_for_gift\n''',
    '''if __package__:\n    from . import bilibili_protocol, douyin_protocol\n    from .bilibili_gifts import GIFT_BATTERIES, batteries_for_gift\n    from .danmu_event import DanmuEvent\nelse:\n    import bilibili_protocol\n    import douyin_protocol\n    from bilibili_gifts import GIFT_BATTERIES, batteries_for_gift\n    from danmu_event import DanmuEvent\n''',
)

QUEUE_EVENT_METHODS = '''\
    def process_danmu_event(self, event: DanmuEvent | dict[str, Any]) -> None:
        if isinstance(event, dict):
            event = DanmuEvent.from_mapping(event)
        if not isinstance(event, DanmuEvent):
            raise TypeError("event must be a DanmuEvent or dict")

        identity = event.identity_dict()
        msg = event.content
        uid = event.legacy_uid()
        uname = event.username
        is_admin_flag = event.is_room_admin
        is_anchor = event.is_anchor
        guard_level = event.guard_level
        is_guard = event.is_guard

        event_payload = {
            "type": "DANMU_EVENT",
            "platform": event.platform,
            "message": msg,
            "identity": identity,
            "received_at": event.received_at,
        }
        with self._lock:
            is_blacklisted = uname in self._blacklist
            self._last_danmu_event = copy.deepcopy(event_payload)
        self._ws_hub.broadcast_json(None, event_payload)

        guard_name = event.guard_name
        medal = event.fan_medal if isinstance(event.fan_medal, dict) else {}
        medal_text = f" 粉丝牌={medal.get('name')}Lv.{medal.get('level')}" if medal else ""
        perm = "黑名单" if is_blacklisted else ("主播" if is_anchor else ("super_admin" if uname in self._super_admins else ("房管" if is_admin_flag else (guard_name or ("管理员" if uname in self._admins else "普通用户")))))
        self._logger.info("[弹幕][%s] %s(%s%s): %s", event.platform, uname, perm, medal_text, msg)

        modified, note = self._process(uid, uname, msg, is_anchor, is_admin_flag, is_guard, guard_level)
        if modified:
            self._broadcast_and_archive(uname, msg)
            self._logger.info(
                "[触发指令] platform=%s uname=%s 权限=%s msg=%r → 队列变更，当前 %s 人",
                event.platform, uname, perm, msg, len(self._persons),
            )
        elif note:
            self._logger.info("[提示][%s] %s(%s): %s", event.platform, uname, perm, note)

    def process_danmu_json(self, payload: dict[str, Any]) -> None:
        """Bilibili DANMU_MSG compatibility adapter for legacy callers/plugins."""
        if not isinstance(payload, dict):
            raise TypeError("payload must be a dict")
        cmd = str(payload.get("cmd", "") or "").strip()
        if not cmd.startswith("DANMU_MSG"):
            return
        info = payload.get("info", [])
        if not isinstance(info, list) or len(info) < 3:
            return

        with self._lock:
            anchor_uid = self._anchor_uid
        identity = bilibili_protocol.parse_bilibili_danmu_identity(payload, anchor_uid=anchor_uid)
        msg = str(info[1]) if len(info) > 1 else ""
        uname = str(identity.get("uname", "") or "")
        if not uname or not msg:
            return
        medal = identity.get("fan_medal", {})
        self.process_danmu_event(
            DanmuEvent(
                platform="bilibili",
                user_id=str(identity.get("uid", "") or ""),
                username=uname,
                content=msg,
                is_room_admin=bool(identity.get("is_room_admin", False)),
                is_anchor=bool(identity.get("is_anchor", False)),
                is_guard=bool(identity.get("is_guard", False)),
                guard_level=_to_int(identity.get("guard_level", 0)),
                guard_name=str(identity.get("guard_name", "") or ""),
                fan_medal=dict(medal) if isinstance(medal, dict) else {},
                metadata={"identity": identity, "bilibili_cmd": cmd},
            )
        )

'''
regex_once(
    "apps/server/server.py",
    r"    def process_danmu_json\(self, payload: dict\[str, Any\]\) -> None:\n.*?\n    # 判断弹幕是否属于",
    QUEUE_EVENT_METHODS + "    # 判断弹幕是否属于",
)

# Shared import insertion for built-in non-Bilibili protocols.
def add_event_import(path: str) -> None:
    text = read(path)
    if "from .danmu_event import DanmuEvent" in text:
        return
    match = re.search(r"^from typing import [^\n]+\n", text, flags=re.MULTILINE)
    if not match:
        raise RuntimeError(f"{path}: typing import anchor missing")
    insertion = match.group(0) + '''\ntry:\n    from .danmu_event import DanmuEvent\nexcept ImportError:  # standalone compatibility\n    from danmu_event import DanmuEvent\n'''
    write(path, text[: match.start()] + insertion + text[match.end() :])


for protocol in (
    "apps/server/douyin_protocol.py",
    "apps/server/huya_protocol.py",
    "apps/server/twitch_protocol.py",
    "apps/server/youtube_protocol.py",
):
    add_event_import(protocol)

# Remove fake Bilibili payload adapters and route directly to DanmuEvent.
replace_once(
    "apps/server/huya_protocol.py",
    '''    @staticmethod\n    def _to_bilibili_like_danmu_payload(event: HuyaChatEvent) -> dict[str, Any]:\n        return {"cmd": "DANMU_MSG", "info": [[], event.content, [int(event.uid), event.nickname, 0], []]}\n\n''',
    "",
)
replace_once(
    "apps/server/huya_protocol.py",
    '''        if hasattr(self.server, "queue_manager"):\n            self.server.queue_manager.process_danmu_json(self._to_bilibili_like_danmu_payload(event))\n''',
    '''        if hasattr(self.server, "queue_manager"):\n            self.server.queue_manager.process_danmu_event(\n                DanmuEvent(\n                    platform="huya",\n                    user_id=str(event.uid),\n                    username=event.nickname,\n                    content=event.content,\n                    received_at=event.recv_time,\n                    metadata={"numeric_uid": int(event.uid), "color": event.color, "room_id": self._current_room_id},\n                )\n            )\n''',
)

replace_once(
    "apps/server/douyin_protocol.py",
    '''    @staticmethod\n    def _to_bilibili_like_danmu_payload(event: DouyinChatEvent) -> dict[str, Any]:\n        is_admin_flag = 1 if event.user_role >= 3 else 0\n        return {\n            "cmd": "DANMU_MSG",\n            "info": [\n                [],\n                event.content,\n                [int(event.uid), event.nickname, is_admin_flag],\n                [],\n            ],\n        }\n\n''',
    "",
)
replace_once(
    "apps/server/douyin_protocol.py",
    '''        if hasattr(self.server, "queue_manager"):\n            self.server.queue_manager.process_danmu_json(\n                self._to_bilibili_like_danmu_payload(event)\n            )\n''',
    '''        if hasattr(self.server, "queue_manager"):\n            self.server.queue_manager.process_danmu_event(\n                DanmuEvent(\n                    platform="douyin",\n                    user_id=event.sec_uid or str(event.uid),\n                    username=event.nickname,\n                    content=event.content,\n                    is_room_admin=event.user_role >= 3,\n                    received_at=event.recv_time,\n                    metadata={"numeric_uid": int(event.uid), "sec_uid": event.sec_uid, "user_role": int(event.user_role)},\n                )\n            )\n''',
)

replace_once(
    "apps/server/twitch_protocol.py",
    '''    @staticmethod\n    def _to_bilibili_like_danmu_payload(event: TwitchChatEvent) -> dict[str, Any]:\n        return {"cmd": "DANMU_MSG", "info": [[], event.content, [int(event.uid), event.nickname, 0], []]}\n\n''',
    "",
)
replace_once(
    "apps/server/twitch_protocol.py",
    '''        if hasattr(self.server, "queue_manager"):\n            self.server.queue_manager.process_danmu_json(self._to_bilibili_like_danmu_payload(event))\n''',
    '''        if hasattr(self.server, "queue_manager"):\n            self.server.queue_manager.process_danmu_event(\n                DanmuEvent(\n                    platform="twitch",\n                    user_id=event.user_id or str(event.uid),\n                    username=event.nickname,\n                    content=event.content,\n                    is_room_admin=event.is_mod,\n                    is_anchor="broadcaster/1" in event.badges,\n                    received_at=event.recv_time,\n                    metadata={\n                        "numeric_uid": int(event.uid),\n                        "login": event.login,\n                        "badges": event.badges,\n                        "color": event.color,\n                        "emotes": event.emotes,\n                        "message_id": event.message_id,\n                        "is_subscriber": event.is_subscriber,\n                        "first_msg": event.first_msg,\n                        "room_id": self._current_channel,\n                    },\n                )\n            )\n''',
)

replace_once(
    "apps/server/youtube_protocol.py",
    '''    @staticmethod\n    def _to_bilibili_like_danmu_payload(event: YoutubeChatEvent) -> dict[str, Any]:\n        return {"cmd": "DANMU_MSG", "info": [[], event.content, [int(event.uid), event.nickname, 0], []]}\n\n''',
    "",
)
replace_once(
    "apps/server/youtube_protocol.py",
    '''        if hasattr(self.server, "queue_manager"):\n            self.server.queue_manager.process_danmu_json(self._to_bilibili_like_danmu_payload(event))\n''',
    '''        if hasattr(self.server, "queue_manager"):\n            self.server.queue_manager.process_danmu_event(\n                DanmuEvent(\n                    platform="youtube",\n                    user_id=str(event.uid),\n                    username=event.nickname,\n                    content=event.content,\n                    received_at=event.recv_time,\n                    metadata={"event_id": event.event_id, "kind": event.kind, "room_id": self._video_id},\n                )\n            )\n''',
)

# Python plugin API: canonical event path, bound to the plugin's own platform.
replace_once(
    "apps/server/plugin_manager.py",
    "from .danmu_plugins import PLUGIN_API_VERSION, PLUGIN_TYPE, DanmuPlugin, DanmuPluginRegistry\n",
    "from .danmu_event import DanmuEvent\nfrom .danmu_plugins import PLUGIN_API_VERSION, PLUGIN_TYPE, DanmuPlugin, DanmuPluginRegistry\n",
)
replace_once(
    "apps/server/plugin_manager.py",
    '''    def process_danmu_json(self, payload: dict[str, Any]) -> None:\n        if not isinstance(payload, dict):\n            raise TypeError("payload must be a dict")\n        manager = getattr(self._server, "queue_manager", None)\n        if manager is None or not hasattr(manager, "process_danmu_json"):\n            raise RuntimeError("queue manager is unavailable")\n        manager.process_danmu_json(dict(payload))\n\n''',
    '''    def process_danmu_event(self, event: DanmuEvent | dict[str, Any]) -> None:\n        if isinstance(event, DanmuEvent):\n            normalized = event\n        elif isinstance(event, dict):\n            normalized = DanmuEvent.from_mapping(event, default_platform=self.platform)\n        else:\n            raise TypeError("event must be a DanmuEvent or dict")\n        if normalized.platform != self.platform:\n            raise ValueError("plugin cannot emit a danmu event for another platform")\n        manager = getattr(self._server, "queue_manager", None)\n        if manager is None or not hasattr(manager, "process_danmu_event"):\n            raise RuntimeError("queue manager is unavailable")\n        manager.process_danmu_event(normalized)\n\n    def process_danmu_json(self, payload: dict[str, Any]) -> None:\n        """Deprecated Bilibili-shaped compatibility API; use process_danmu_event()."""\n        if not isinstance(payload, dict):\n            raise TypeError("payload must be a dict")\n        manager = getattr(self._server, "queue_manager", None)\n        if manager is None or not hasattr(manager, "process_danmu_json"):\n            raise RuntimeError("queue manager is unavailable")\n        manager.process_danmu_json(dict(payload))\n\n''',
)

# JavaScript plugin API: add a separate canonical event IPC message; keep old Bilibili JSON API.
replace_once(
    "apps/server/javascript_plugin_runtime.py",
    "  processDanmu(payload) { return __host_process_danmu(JSON.stringify(payload ?? {})); },\n",
    "  processDanmu(payload) { return __host_process_danmu(JSON.stringify(payload ?? {})); },\n  processDanmuEvent(payload) { return __host_process_danmu_event(JSON.stringify(payload ?? {})); },\n",
)
replace_once(
    "apps/server/javascript_plugin_runtime.py",
    '''    def process_danmu(raw: str) -> bool:\n        payload = _parse_host_json(raw, "danmu payload")\n        if not isinstance(payload, dict):\n            raise TypeError("danmu payload must be an object")\n        _send(conn, {"type": "danmu", "payload": payload})\n        return True\n\n''',
    '''    def process_danmu(raw: str) -> bool:\n        payload = _parse_host_json(raw, "danmu payload")\n        if not isinstance(payload, dict):\n            raise TypeError("danmu payload must be an object")\n        _send(conn, {"type": "danmu", "payload": payload})\n        return True\n\n    def process_danmu_event(raw: str) -> bool:\n        payload = _parse_host_json(raw, "danmu event")\n        if not isinstance(payload, dict):\n            raise TypeError("danmu event must be an object")\n        _send(conn, {"type": "danmu_event", "payload": payload})\n        return True\n\n''',
)
replace_once(
    "apps/server/javascript_plugin_runtime.py",
    '    ctx.add_callable("__host_process_danmu", process_danmu)\n',
    '    ctx.add_callable("__host_process_danmu", process_danmu)\n    ctx.add_callable("__host_process_danmu_event", process_danmu_event)\n',
)
replace_once(
    "apps/server/javascript_plugin_runtime.py",
    '''        if kind == "danmu":\n            payload = message.get("payload")\n            if isinstance(payload, dict):\n                self.context.process_danmu_json(payload)\n            return "message", None\n''',
    '''        if kind == "danmu":\n            payload = message.get("payload")\n            if isinstance(payload, dict):\n                self.context.process_danmu_json(payload)\n            return "message", None\n        if kind == "danmu_event":\n            payload = message.get("payload")\n            if isinstance(payload, dict):\n                self.context.process_danmu_event(payload)\n            return "message", None\n''',
)

DOC_SECTION = r'''

## 平台无关 DanmuEvent（Issue #142）

新的获取弹幕插件应把平台消息规范化后交给统一事件入口，不再伪造 Bilibili `DANMU_MSG/info`：

Python：

```python
context.process_danmu_event({
    "user_id": "platform-native-user-id",
    "username": "Alice",
    "content": "排队",
    "is_room_admin": False,
    "metadata": {"message_id": "abc"},
})
```

`platform` 可以省略，`PluginContext` 会绑定为插件 manifest 自己声明的平台；插件不能替其他平台发事件。也可以直接传入 `apps.server.danmu_event.DanmuEvent`。

JavaScript：

```javascript
host.processDanmuEvent({
  user_id: "platform-native-user-id",
  username: "Alice",
  content: "排队",
  is_room_admin: false,
  metadata: {message_id: "abc"}
});
```

规范字段包括 `platform`、`user_id`、`username`、`content`、`is_room_admin`、`is_anchor`、`is_guard`、`guard_level`、`guard_name`、`fan_medal`、`received_at`、`metadata`。`user_id` 是字符串，可安全保存非数字平台 ID。

旧 Python `context.process_danmu_json(payload)` 和 JavaScript `host.processDanmu(payload)` 继续支持，但它们仅作为 Bilibili `DANMU_MSG` 兼容接口；新插件应使用 `process_danmu_event` / `processDanmuEvent`。
'''
docs = read("docs/PLUGIN_API_V1.md")
if "## 平台无关 DanmuEvent（Issue #142）" not in docs:
    write("docs/PLUGIN_API_V1.md", docs.rstrip() + DOC_SECTION + "\n")

GUARD_SOURCE = r'''\
from __future__ import annotations

import hashlib
import io
import json
import logging
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from apps.server import plugin_manager as pm  # noqa: E402
from apps.server import plugin_runtime_dual, server  # noqa: E402
from apps.server.danmu_event import DanmuEvent  # noqa: E402
from apps.server.danmu_plugins import DanmuPluginRegistry  # noqa: E402


class Hub:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def broadcast_json(self, _sender, payload) -> None:
        self.events.append(dict(payload))


class Archive:
    def write_snapshot(self, *_args, **_kwargs):
        return None


def check_model_and_queue() -> None:
    first = DanmuEvent(platform="kuaishou", user_id="ks:user:abc", username="Alice", content="hello")
    second = DanmuEvent(platform="kuaishou", user_id="ks:user:abc", username="Alice", content="hello")
    assert first.legacy_uid() == second.legacy_uid() > 0
    assert first.identity_dict()["user_id"] == "ks:user:abc"

    hub = Hub()
    manager = server.QueueManager(hub, Archive(), logging.getLogger("issue142"))
    calls = []
    manager._process = lambda uid, uname, msg, is_anchor, is_admin, is_guard, guard_level: (calls.append((uid, uname, msg, is_anchor, is_admin, is_guard, guard_level)) or (False, None))
    manager.process_danmu_event(first)
    assert calls[-1][0] == first.legacy_uid()
    assert calls[-1][1:3] == ("Alice", "hello")
    public = manager.get_last_danmu_event()
    assert public["platform"] == "kuaishou"
    assert public["identity"]["user_id"] == "ks:user:abc"

    manager.process_danmu_json({"cmd": "DANMU_MSG", "info": [[], "legacy", [12345, "LegacyUser", 0], []]})
    legacy = manager.get_last_danmu_event()
    assert legacy["platform"] == "bilibili"
    assert legacy["message"] == "legacy"
    assert legacy["identity"]["uid"] == 12345


def check_builtin_sources() -> None:
    for name in ("douyin_protocol.py", "huya_protocol.py", "twitch_protocol.py", "youtube_protocol.py"):
        source = (ROOT / "apps" / "server" / name).read_text(encoding="utf-8")
        assert "_to_bilibili_like_danmu_payload" not in source, name
        assert "process_danmu_event" in source, name
        assert "DanmuEvent(" in source, name
    douyin = (ROOT / "apps/server/douyin_protocol.py").read_text(encoding="utf-8")
    assert "is_room_admin=event.user_role >= 3" in douyin


def check_python_plugin_context() -> None:
    captured = []
    queue = SimpleNamespace(
        process_danmu_event=lambda event: captured.append(event),
        process_danmu_json=lambda payload: captured.append(dict(payload)),
    )
    active = SimpleNamespace(runtime_config={}, logger=logging.getLogger("issue142-plugin"), queue_manager=queue)
    record = pm.InstalledPluginRecord(
        plugin_id="example.event",
        root=ROOT,
        manifest={"platform": "kuaishou", "permissions": []},
        enabled=True,
        package_sha256="0" * 64,
        signature_status="unsigned",
    )
    ctx = pm.PluginContext(SimpleNamespace(data_root=ROOT), active, record)
    ctx.process_danmu_event({"user_id": "native:id", "username": "Alice", "content": "event"})
    assert isinstance(captured[-1], DanmuEvent)
    assert captured[-1].platform == "kuaishou"
    assert captured[-1].user_id == "native:id"
    try:
        ctx.process_danmu_event({"platform": "douyin", "user_id": "x", "username": "Alice", "content": "bad"})
    except ValueError:
        pass
    else:
        raise AssertionError("plugin platform spoof should be rejected")


JS_SOURCE = b'''\\
function createRelay(config, host) {
  let state = {connected: false};
  return {
    start() {
      state.connected = true;
      host.processDanmuEvent({user_id: 'native:js:id', username: 'JSUser', content: 'event-js'});
      host.processDanmu({cmd: 'DANMU_MSG', info: [[], 'legacy-js', [2468, 'LegacyJS', 0], []]});
      host.setStatus({connected: true});
    },
    tick() {},
    stop() { state.connected = false; host.setStatus({connected: false}); },
    getRuntimeStatus() { return state; }
  };
}
'''


def js_package() -> bytes:
    manifest = {
        "schema": 1,
        "id": "example.event.javascript",
        "name": "Event JS Probe",
        "version": "1.0.0",
        "plugin_api": 1,
        "type": "danmu_source",
        "platform": "js_event_probe",
        "runtime": "javascript",
        "entry": "plugin.js",
        "min_bilipdj_version": "2.0.0",
        "permissions": [],
        "capabilities": ["danmu"],
        "files": {"plugin.js": hashlib.sha256(JS_SOURCE).hexdigest()},
    }
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("manifest.json", json.dumps(manifest).encode())
        archive.writestr("plugin.js", JS_SOURCE)
    return out.getvalue()


def check_javascript_plugin_api() -> None:
    plugin_runtime_dual.install_dual_runtime_support()
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        module = SimpleNamespace(
            APP_DIR=root,
            BUNDLE_DIR=ROOT,
            REPO_DIR=ROOT,
            DEFAULT_PLATFORM="bilibili",
            RESERVED_RUNTIME_PLATFORMS=(),
            ALL_RUNTIME_PLATFORMS=(),
            PLATFORM_DISPLAY_NAMES={},
            _get_runtime_platform=lambda cfg: str((cfg or {}).get("platform", "bilibili")),
        )
        registry = DanmuPluginRegistry()
        manager = pm.PluginManager(module, registry)
        manager.install_bytes(js_package(), filename="event-probe.bilipdj-plugin", allow_unsigned=True)
        manager.set_enabled("example.event.javascript", True)
        canonical = []
        legacy = []
        active = SimpleNamespace(
            runtime_config={"platform": "js_event_probe", "js_event_probe": {}},
            logger=logging.getLogger("issue142-js"),
            ws_hub=SimpleNamespace(broadcast_json=lambda _sender, _payload: None),
            queue_manager=SimpleNamespace(
                process_danmu_event=lambda event: canonical.append(event),
                process_danmu_json=lambda payload: legacy.append(dict(payload)),
            ),
        )
        relay = registry.create_relay("js_event_probe", active)
        relay.start()
        deadline = time.monotonic() + 3.0
        while time.monotonic() < deadline and (not canonical or not legacy):
            status = relay.get_runtime_status()
            if status.get("state") == "error":
                raise AssertionError(status)
            time.sleep(0.02)
        assert canonical and isinstance(canonical[0], DanmuEvent), canonical
        assert canonical[0].platform == "js_event_probe"
        assert canonical[0].user_id == "native:js:id"
        assert legacy and legacy[0].get("cmd") == "DANMU_MSG", legacy
        relay.stop()
        relay.join(timeout=2)


def main() -> None:
    check_model_and_queue()
    check_builtin_sources()
    check_python_plugin_context()
    check_javascript_plugin_api()
    print("issue #142 DanmuEvent regression guard: OK")


if __name__ == "__main__":
    main()
'''
write("scripts/issue142_danmu_event_guard.py", GUARD_SOURCE)

WORKFLOW_SOURCE = '''\
name: Validate Platform-independent DanmuEvent

on:
  workflow_dispatch:
  pull_request:
    paths:
      - 'apps/server/danmu_event.py'
      - 'apps/server/server.py'
      - 'apps/server/douyin_protocol.py'
      - 'apps/server/huya_protocol.py'
      - 'apps/server/twitch_protocol.py'
      - 'apps/server/youtube_protocol.py'
      - 'apps/server/plugin_manager.py'
      - 'apps/server/javascript_plugin_runtime.py'
      - 'scripts/issue142_danmu_event_guard.py'
      - 'docs/PLUGIN_API_V1.md'
      - '.github/workflows/danmu-event.yml'
      - 'requirements.txt'
  push:
    branches: [now]
    paths:
      - 'apps/server/danmu_event.py'
      - 'apps/server/server.py'
      - 'apps/server/douyin_protocol.py'
      - 'apps/server/huya_protocol.py'
      - 'apps/server/twitch_protocol.py'
      - 'apps/server/youtube_protocol.py'
      - 'apps/server/plugin_manager.py'
      - 'apps/server/javascript_plugin_runtime.py'
      - 'scripts/issue142_danmu_event_guard.py'
      - 'docs/PLUGIN_API_V1.md'
      - '.github/workflows/danmu-event.yml'
      - 'requirements.txt'

permissions:
  contents: read

jobs:
  validate-danmu-event:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - name: Install project dependencies
        run: python -m pip install -r requirements.txt
      - name: Compile Issue 142 sources
        run: python -m py_compile apps/server/danmu_event.py apps/server/server.py apps/server/douyin_protocol.py apps/server/huya_protocol.py apps/server/twitch_protocol.py apps/server/youtube_protocol.py apps/server/plugin_manager.py apps/server/javascript_plugin_runtime.py scripts/issue142_danmu_event_guard.py
      - name: Run Issue 142 DanmuEvent regression guard
        run: python scripts/issue142_danmu_event_guard.py
      - name: Run existing QuickJS runtime probe
        run: python scripts/issue130_js_runtime_probe.py
'''
write(".github/workflows/danmu-event.yml", WORKFLOW_SOURCE)

# One-shot staging files must not enter the final PR.
(ROOT / "scripts/issue142_apply.py").unlink(missing_ok=True)
(ROOT / ".github/workflows/issue142-apply.yml").unlink(missing_ok=True)
print("Issue #142 source refactor applied")
