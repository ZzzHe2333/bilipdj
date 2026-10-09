"""Issue #307: real scoped authorization and persistent role configuration regression."""
from __future__ import annotations

import importlib
import json
import sys
import tempfile
import threading
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
backend = importlib.import_module("apps.server.server")
PERMISSIONS = ("super_admin", "admin", "jianzhang", "member")


def _manager(raw: dict):
    manager = backend.QueueManager.__new__(backend.QueueManager)
    manager._queue_origin_context = threading.local()
    manager._super_admins = list(raw["super_admin"])
    manager._admins = list(raw["admin"])
    manager._jianzhang = list(raw["jianzhang"])
    manager._scoped_permissions = backend._decode_scoped_permissions(raw["scoped_entries"])
    manager._fangguan_can_doing = False
    return manager


def _event(manager, platform: str, user_id: str = "", username: str = "shared"):
    manager._queue_origin_context.platform = platform
    manager._queue_origin_context.user_id = user_id
    return manager._has_op_permission(username, False, False)


def main() -> None:
    source = {
        "super_admin": ["legacy_owner"], "admin": ["legacy_admin"],
        "jianzhang": [], "member": [], "blacklist": [],
        "scoped_entries": [
            {"id": "42", "kind": "id", "role": "admin", "platforms": ["douyin", "bilibili"]},
            {"id": "77", "kind": "id", "role": "super_admin", "platforms": []},
            {"id": "guest", "kind": "name", "role": "admin", "platforms": ["twitch"]},
            {"id": "91", "kind": "id", "role": "jianzhang", "platforms": ["huya"]},
        ],
    }
    normalized = backend._normalize_quanxian_config(source)
    mgr = _manager(normalized)
    assert _event(mgr, "bilibili", "42") and _event(mgr, "douyin", "42")
    assert not _event(mgr, "huya", "42") and not _event(mgr, "twitch", "42")
    assert not _event(mgr, "douyin", "100", "shared")  # Identical display name is not identity.
    assert _event(mgr, "twitch", "100", "guest") and not _event(mgr, "douyin", "100", "guest")
    assert _event(mgr, "youtube", "77") and _event(mgr, "huya", "77")
    assert _event(mgr, "youtube", "nope", "legacy_admin")  # Legacy roles are global.
    mgr._queue_origin_context.platform, mgr._queue_origin_context.user_id = "huya", "91"
    assert mgr._has_scoped_role("jianzhang", "other")
    mgr._queue_origin_context.platform = "bilibili"
    assert not mgr._has_scoped_role("jianzhang", "other")
    malicious = backend._normalize_quanxian_config({
        "super_admin": [], "admin": [], "jianzhang": [], "member": [], "blacklist": [],
        "scoped_entries": [{"id": "evil", "kind": "id", "role": "admin", "platforms": ["unknown-service"]}],
    })
    assert not malicious["scoped_entries"], "Invalid platform must not grant every platform"

    old = {k: getattr(backend, k) for k in ("CONFIG_PATH", "QUANXIAN_PATH", "BLACKLIST_PATH", "_CONFIG_LOCK_PATH")}
    with tempfile.TemporaryDirectory(prefix="bilipdj-issue307-") as folder:
        root = Path(folder)
        try:
            backend.CONFIG_PATH = root / "config.yaml"
            backend.QUANXIAN_PATH = root / "quanxian.yaml"
            backend.BLACKLIST_PATH = root / "blacklist.csv"
            backend._CONFIG_LOCK_PATH = root / ".config.lock"
            backend.save_quanxian(source)
            loaded = backend.load_quanxian()
            assert len(loaded["entries"]) == 6, loaded["entries"]
            assert backend._decode_scoped_permissions(loaded["scoped_entries"]) == backend._decode_scoped_permissions(normalized["scoped_entries"])
            assert "scoped_entries:" in backend.CONFIG_PATH.read_text(encoding="utf-8")
            assert "scoped_entries:" in backend.QUANXIAN_PATH.read_text(encoding="utf-8")
            # A legacy editor saving name-only roles must never delete scoped rules.
            backend.save_quanxian({k: loaded[k] for k in ("super_admin", "admin", "jianzhang", "member", "blacklist")})
            assert len(backend.load_quanxian()["entries"]) == 6
            # Explicit empty list revokes scopes.
            backend.save_quanxian({**{k: [] for k in ("super_admin", "admin", "jianzhang", "member", "blacklist")}, "scoped_entries": []})
            assert backend.load_quanxian()["entries"] == []
        finally:
            for key, value in old.items():
                setattr(backend, key, value)

    html = (ROOT / "apps/web/static/control.html").read_text(encoding="utf-8")
    js = (ROOT / "apps/web/static/control.js").read_text(encoding="utf-8")
    windows = (ROOT / "apps/windows/windows_ui.py").read_text(encoding="utf-8")
    for token in ("perm-add", "perm-list", "perm-dialog", "perm-role", "perm-kind", "perm-identity", 'name="perm-platform"'):
        assert token in html, token
    for token in ("editPermission(", "savePermissions(", "scoped_entries", "permissionRows", "showModal()"):
        assert token in js, token
    for token in ("ttk.Treeview", "tk.Toplevel", "platform_vars", "_refresh_permission_list", "_save_quanxian"):
        assert token in windows, token
    print("issue #307 scoped platform permissions: PASS")


if __name__ == "__main__":
    main()
