from __future__ import annotations

import sys
import tempfile
import threading
import types
import unittest
from pathlib import Path

from core.admin_permission_guard import (
    ADMIN_PERMISSION_KEYS,
    attach_admin_permission_guard,
    load_admin_permission_store,
    required_permission_for_command,
    save_admin_permission_store,
    set_admin_permissions,
)


class AdminPermissionStoreTests(unittest.TestCase):
    def test_store_preserves_explicit_empty_and_subset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "admin_permissions.json"
            saved = save_admin_permission_store(
                {
                    "admin-a": ["queue_remove", "blacklist"],
                    "admin-b": [],
                },
                path,
            )
            self.assertEqual(saved["admin-a"], ["queue_remove", "blacklist"])
            self.assertEqual(saved["admin-b"], [])
            self.assertEqual(load_admin_permission_store(path), saved)

    def test_command_to_permission_mapping(self) -> None:
        cases = {
            "完成": "queue_remove",
            "删除 3": "queue_remove",
            "add 用户A": "queue_add",
            "无影插 2 用户A": "queue_insert",
            "插队 2 用户A": "queue_insert",
            "暂停排队功能": "queue_control",
            "设置排队上限 50": "queue_limit",
            "开启舰长插队": "jianzhang_control",
            "允许房管成为插件管理员": "room_admin_control",
            "拉黑 用户A": "blacklist",
        }
        for command, expected in cases.items():
            with self.subTest(command=command):
                self.assertEqual(required_permission_for_command(command), expected)
        self.assertIsNone(required_permission_for_command("排队"))


class AdminPermissionGuardTests(unittest.TestCase):
    def _make_manager(self, temp_dir: str):
        module_name = f"tests._fake_permission_server_{id(self)}_{len(sys.modules)}"
        module = types.ModuleType(module_name)
        module._YAML_DIR = Path(temp_dir)
        sys.modules[module_name] = module

        class FakeQueueManager:
            def __init__(self) -> None:
                self._lock = threading.Lock()
                self._admins = ["admin-a", "admin-b"]
                self._super_admins = ["root"]
                self._logger = None
                self.original_calls: list[tuple[str, str]] = []

            def _has_super_admin(self, uname: str, is_anchor: bool) -> bool:
                return is_anchor or uname in self._super_admins

            def _process(
                self,
                uid: int,
                uname: str,
                msg: str,
                is_anchor: bool,
                is_admin: bool,
                is_guard: bool,
                guard_level: int,
            ) -> tuple[bool, str | None]:
                self.original_calls.append((uname, msg))
                return True, "original"

        FakeQueueManager.__module__ = module_name
        module.QueueManager = FakeQueueManager
        self.assertTrue(attach_admin_permission_guard(FakeQueueManager))
        return FakeQueueManager(), module_name

    def test_unconfigured_admin_keeps_legacy_full_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager, module_name = self._make_manager(temp_dir)
            try:
                result = manager._process(1, "admin-a", "暂停排队功能", False, False, False, 0)
                self.assertEqual(result, (True, "original"))
                self.assertEqual(manager.original_calls, [("admin-a", "暂停排队功能")])
            finally:
                sys.modules.pop(module_name, None)

    def test_explicit_subset_allows_only_selected_management_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager, module_name = self._make_manager(temp_dir)
            try:
                path = Path(temp_dir) / "admin_permissions.json"
                set_admin_permissions("admin-a", ["queue_remove", "blacklist"], path)

                self.assertEqual(
                    manager._process(1, "admin-a", "完成", False, False, False, 0),
                    (True, "original"),
                )
                self.assertEqual(
                    manager._process(1, "admin-a", "拉黑 用户A", False, False, False, 0),
                    (True, "original"),
                )
                denied = manager._process(1, "admin-a", "暂停排队功能", False, False, False, 0)
                self.assertFalse(denied[0])
                self.assertIn("未授予", str(denied[1]))
            finally:
                sys.modules.pop(module_name, None)

    def test_explicit_empty_keeps_admin_role_but_blocks_management_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager, module_name = self._make_manager(temp_dir)
            try:
                path = Path(temp_dir) / "admin_permissions.json"
                set_admin_permissions("admin-a", [], path)
                for command in ("完成", "add 用户A", "拉黑 用户A", "设置排队上限 50"):
                    with self.subTest(command=command):
                        result = manager._process(1, "admin-a", command, False, False, False, 0)
                        self.assertFalse(result[0])
                        self.assertIn("未授予", str(result[1]))
            finally:
                sys.modules.pop(module_name, None)

    def test_super_admin_can_assign_multiple_permissions_and_bypasses_limits(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manager, module_name = self._make_manager(temp_dir)
            try:
                result = manager._process(
                    1,
                    "root",
                    "设置管理员权限 admin-a 删除,黑名单",
                    False,
                    False,
                    False,
                    0,
                )
                self.assertFalse(result[0])
                self.assertIn("已设置 admin-a", str(result[1]))
                saved = load_admin_permission_store(Path(temp_dir) / "admin_permissions.json")
                self.assertEqual(saved["admin-a"], ["queue_remove", "blacklist"])

                super_result = manager._process(
                    1,
                    "root",
                    "暂停排队功能",
                    False,
                    False,
                    False,
                    0,
                )
                self.assertEqual(super_result, (True, "original"))
            finally:
                sys.modules.pop(module_name, None)

    def test_all_permission_keys_are_unique(self) -> None:
        self.assertEqual(len(ADMIN_PERMISSION_KEYS), len(set(ADMIN_PERMISSION_KEYS)))


if __name__ == "__main__":
    unittest.main()
