"""Issue #309: only administrator roles may use multi-platform permission scopes.

This is a standalone regression executable integrated into Quality CI. It
tests the real QueueManager._process decision path, not just UI strings.
"""
from __future__ import annotations

import base64
import importlib
import json
import sys
import tempfile
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
backend = importlib.import_module("apps.server.server")


def minimal_queue(permissions: dict):
    """Exercise real _process with a minimal queue/archive stub."""
    m = backend.QueueManager.__new__(backend.QueueManager)
    m._lock = threading.RLock()
    m._queue_origin_context = threading.local()
    m._persons = []
    m._blacklist = list(permissions["blacklist"])
    m._admins = list(permissions["admin"])
    m._super_admins = list(permissions["super_admin"])
    m._jianzhang = list(permissions["jianzhang"])
    m._scoped_permissions = backend._decode_scoped_permissions(permissions["scoped_entries"])
    m._fangguan_can_doing = False
    m._all_disabled = False
    m._kaiguan = {"paidui": True, "jianzhang_chadui": True}
    m._jianzhangchadui = True
    m._max_length = 100
    m._gift_queue_credits = {}
    m._gift_queue_only = False
    m._daily_queue_limit = 0
    m._find_index = lambda username: -1
    m._append_queue_item_unlocked = lambda name: m._persons.append(name)
    return m


def apply(m, *, platform: str, name: str, uid: str, message: str = "插队",
          guard: bool = False, level: int = 0):
    m._queue_origin_context.platform = platform
    m._queue_origin_context.user_id = uid
    return m._process(int(uid) if uid.isdigit() else 0, name, message,
                      False, False, guard, level)


def main():
    assert backend.SCOPED_PERMISSION_ROLES == ("super_admin", "admin")
    source = {
        "super_admin": ["legacy_owner"], "admin": ["legacy_admin"],
        "jianzhang": ["B站老舰长"], "member": ["普通成员"],
        "blacklist": [],
        "scoped_entries": [
            {"id": "42", "kind": "id", "role": "admin", "platforms": ["bilibili", "douyin"]},
            {"id": "77", "kind": "id", "role": "super_admin", "platforms": []},
            {"id": "91", "kind": "id", "role": "jianzhang", "platforms": ["bilibili"]},
            {"id": "92", "kind": "id", "role": "member", "platforms": ["douyin"]},
        ],
    }
    normalized = backend._normalize_quanxian_config(source)
    records = backend._decode_scoped_permissions(normalized["scoped_entries"])
    assert len(records) == 2 and {r["role"] for r in records} == {"admin", "super_admin"}
    m = minimal_queue(normalized)
    assert apply(m, platform="bilibili", name="普通成员", uid="11", message="排队")[0]
    assert apply(m, platform="douyin", name="另一个成员", uid="12", message="排队")[0]
    assert m._persons == ["普通成员", "另一个成员"], "Both platforms use one queue"
    assert not m._has_scoped_role("member", "普通成员")
    assert not m._has_op_permission("普通成员", False, False)

    # Scoped admins work only on selected sources; empty source=all platforms.
    m._queue_origin_context.platform, m._queue_origin_context.user_id = "bilibili", "42"
    assert m._has_op_permission("same_name", False, False)
    m._queue_origin_context.platform = "douyin"
    assert m._has_op_permission("same_name", False, False)
    m._queue_origin_context.platform = "huya"
    assert not m._has_op_permission("same_name", False, False)
    m._queue_origin_context.user_id = "77"
    assert m._has_super_admin("some_name", False)
    m._queue_origin_context.platform = "twitch"
    assert m._has_super_admin("some_name", False)
    assert not m._has_scoped_role("jianzhang", "anyone")

    # A cached Bilibili guard name MUST NOT grant priority on Douyin.
    m._persons.clear()
    changed, _ = apply(m, platform="douyin", name="B站老舰长", uid="33")
    assert not changed and not m._persons
    changed, _ = apply(m, platform="bilibili", name="B站老舰长", uid="33")
    assert changed and m._persons == ["B站老舰长"]
    m._persons.clear()
    # Native Bilibili guard is recognized and cached only on Bilibili.
    changed, _ = apply(m, platform="bilibili", name="新舰长", uid="44", guard=True, level=1)
    assert changed and "新舰长" in m._jianzhang
    m._persons.clear()
    changed, _ = apply(m, platform="douyin", name="新舰长", uid="44", guard=True, level=1)
    assert not changed and not m._persons
    assert "B站老舰长" in m._jianzhang

    # Backward-compatible YAML storage keeps the guard/member lists unchanged.
    original = {name: getattr(backend, name) for name in
                ("CONFIG_PATH", "QUANXIAN_PATH", "BLACKLIST_PATH", "_CONFIG_LOCK_PATH")}
    with tempfile.TemporaryDirectory(prefix="bilipdj-309-") as directory:
        try:
            base = Path(directory)
            backend.CONFIG_PATH = base / "config.yaml"
            backend.QUANXIAN_PATH = base / "quanxian.yaml"
            backend.BLACKLIST_PATH = base / "blacklist.csv"
            backend._CONFIG_LOCK_PATH = base / ".config.lock"
            backend.save_quanxian(source)
            saved = backend.load_quanxian()
            assert saved["jianzhang"] == ["B站老舰长"]
            assert saved["member"] == ["普通成员"]
            assert len(saved["entries"]) == 4, saved["entries"]
            assert {item["role"] for item in saved["entries"]} == {"super_admin", "admin"}
            assert len(backend._decode_scoped_permissions(saved["scoped_entries"])) == 2
            backend.save_quanxian({
                "super_admin": ["legacy_owner"], "admin": ["legacy_admin"],
                "jianzhang": saved["jianzhang"], "member": saved["member"],
                "blacklist": [], "scoped_entries": []
            })
            revoked = backend.load_quanxian()
            assert revoked["jianzhang"] == ["B站老舰长"]
            assert revoked["member"] == ["普通成员"]
            assert not revoked["scoped_entries"]
        finally:
            for key, value in original.items():
                setattr(backend, key, value)

    web = (ROOT / "apps/web/static/control.html").read_text(encoding="utf-8")
    js = (ROOT / "apps/web/static/control.js").read_text(encoding="utf-8")
    tk = (ROOT / "apps/windows/windows_ui.py").read_text(encoding="utf-8")
    panel = (ROOT / "apps/windows/control_panel.py").read_text(encoding="utf-8")
    role_select = web.split('<select id="perm-role">', 1)[1].split("</select>", 1)[0]
    assert 'value="super_admin"' in role_select and 'value="admin"' in role_select
    assert 'value="jianzhang"' not in role_select and 'value="member"' not in role_select
    for name in ("perm-legacy-jianzhang", "perm-legacy-member"):
        assert name in web and name in js
    assert "const permissionRoles = {super_admin: '最高管理员', admin: '管理员'}" in js
    assert 'role_labels = {"super_admin": "最高管理员", "admin": "管理员"}' in tk
    assert 'panel._quanxian_text[role]' in tk
    for name in ('"jianzhang"', '"member"'):
        assert name in panel and name in tk
    print("issue #309 admin-only scopes, Bilibili guards, ordinary cross-platform queue: PASS")


if __name__ == "__main__":
    main()
