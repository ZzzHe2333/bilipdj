"""Control-panel editor for per-admin operator permissions."""
from __future__ import annotations

import functools
import sys
from pathlib import Path
from typing import Any

import tkinter as tk
from tkinter import ttk

from .admin_permission_guard import (
    ADMIN_PERMISSION_DEFS,
    admin_permission_store_path,
    load_admin_permission_store,
    prune_admin_permissions,
    reset_admin_permissions,
    set_admin_permissions,
)


def _backend_module(app: Any) -> Any | None:
    module = sys.modules.get(str(getattr(type(app), "__module__", "") or ""))
    loader = getattr(module, "load_backend_server_module", None) if module is not None else None
    if not callable(loader):
        return None
    try:
        return loader()
    except Exception:
        return None


def _store_path(app: Any) -> Path:
    backend = _backend_module(app)
    yaml_dir = getattr(backend, "_YAML_DIR", None) if backend is not None else None
    if yaml_dir is not None:
        return Path(yaml_dir) / "admin_permissions.json"
    return admin_permission_store_path()


def _admin_names(app: Any) -> list[str]:
    widget = getattr(app, "_quanxian_text", {}).get("admin")
    if widget is None:
        return []
    try:
        raw = widget.get("1.0", "end")
    except Exception:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for line in str(raw).splitlines():
        name = line.strip()
        if not name or name in seen:
            continue
        seen.add(name)
        result.append(name)
    return result


def _set_status(app: Any, text: str) -> None:
    variable = getattr(app, "_admin_permission_status_var", None)
    if variable is not None:
        try:
            variable.set(text)
        except Exception:
            pass
    logger = getattr(app, "_append_log", None)
    if callable(logger):
        logger(f"[GUI] {text}")


def _refresh_admin_choices(app: Any, *, preserve_selection: bool = True) -> None:
    combo = getattr(app, "_admin_permission_combo", None)
    variable = getattr(app, "_admin_permission_admin_var", None)
    if combo is None or variable is None:
        return
    names = _admin_names(app)
    try:
        previous = str(variable.get() or "").strip() if preserve_selection else ""
        combo.configure(values=names)
        selected = previous if previous in names else (names[0] if names else "")
        variable.set(selected)
    except Exception:
        return
    _load_selected_admin_permissions(app)


def _load_selected_admin_permissions(app: Any) -> None:
    variable = getattr(app, "_admin_permission_admin_var", None)
    permission_vars = getattr(app, "_admin_permission_vars", {})
    if variable is None or not permission_vars:
        return
    try:
        admin_name = str(variable.get() or "").strip()
    except Exception:
        admin_name = ""
    if not admin_name:
        for permission_var in permission_vars.values():
            permission_var.set(False)
        _set_status(app, "请先在管理员列表中添加管理员")
        return

    overrides = load_admin_permission_store(_store_path(app))
    if admin_name not in overrides:
        for key, permission_var in permission_vars.items():
            permission_var.set(True)
        _set_status(app, f"{admin_name}：完整管理员权限（未单独限制）")
        return

    allowed = set(overrides.get(admin_name, []))
    for key, permission_var in permission_vars.items():
        permission_var.set(key in allowed)
    if not allowed:
        _set_status(app, f"{admin_name}：已保留管理员身份，但没有管理指令权限")
    else:
        _set_status(app, f"{admin_name}：已加载 {len(allowed)} 项单独权限")


def _save_selected_admin_permissions(app: Any) -> None:
    variable = getattr(app, "_admin_permission_admin_var", None)
    permission_vars = getattr(app, "_admin_permission_vars", {})
    if variable is None:
        return
    admin_name = str(variable.get() or "").strip()
    names = _admin_names(app)
    if not admin_name or admin_name not in names:
        _set_status(app, "请选择有效管理员后再保存细分权限")
        return
    permissions = [key for key, permission_var in permission_vars.items() if bool(permission_var.get())]
    set_admin_permissions(admin_name, permissions, _store_path(app))
    if permissions:
        _set_status(app, f"已保存 {admin_name} 的 {len(permissions)} 项权限，立即生效")
    else:
        _set_status(app, f"已清空 {admin_name} 的管理指令权限，立即生效")


def _reset_selected_admin_permissions(app: Any) -> None:
    variable = getattr(app, "_admin_permission_admin_var", None)
    if variable is None:
        return
    admin_name = str(variable.get() or "").strip()
    if not admin_name or admin_name not in _admin_names(app):
        _set_status(app, "请选择有效管理员后再恢复")
        return
    reset_admin_permissions(admin_name, _store_path(app))
    for permission_var in getattr(app, "_admin_permission_vars", {}).values():
        permission_var.set(True)
    _set_status(app, f"已恢复 {admin_name} 的完整管理员权限，立即生效")


def _clear_permission_checks(app: Any) -> None:
    for permission_var in getattr(app, "_admin_permission_vars", {}).values():
        permission_var.set(False)
    _set_status(app, "已取消全部勾选；点击“保存该管理员权限”后生效")


def _select_all_permission_checks(app: Any) -> None:
    for permission_var in getattr(app, "_admin_permission_vars", {}).values():
        permission_var.set(True)
    _set_status(app, "已勾选全部权限；点击“保存该管理员权限”后生效")


def patch_admin_permission_ui(control_panel_cls: type[Any]) -> bool:
    """Extend the existing permission page with a per-admin permission matrix."""

    if not isinstance(control_panel_cls, type):
        return False
    if bool(getattr(control_panel_cls, "_bilipdj_admin_permission_ui_installed", False)):
        return True

    original_build = getattr(control_panel_cls, "_build_quanxian_tab", None)
    original_load = getattr(control_panel_cls, "_load_quanxian", None)
    original_save = getattr(control_panel_cls, "_save_quanxian", None)
    if not all(callable(method) for method in (original_build, original_load, original_save)):
        return False

    @functools.wraps(original_build)
    def build_with_admin_permissions(self: Any, frame: ttk.Frame) -> None:
        original_build(self, frame)

        panel = ttk.LabelFrame(frame, text="管理员细分权限（由最高管理员配置）", padding=10)
        panel.grid(row=11, column=0, sticky="ew", pady=(16, 8))
        panel.columnconfigure(1, weight=1)

        ttk.Label(panel, text="管理员").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=(0, 8))
        self._admin_permission_admin_var = tk.StringVar(value="")
        self._admin_permission_combo = ttk.Combobox(
            panel,
            textvariable=self._admin_permission_admin_var,
            values=(),
            state="readonly",
            width=28,
        )
        self._admin_permission_combo.grid(row=0, column=1, sticky="w", pady=(0, 8))
        self._admin_permission_combo.bind(
            "<<ComboboxSelected>>",
            lambda _event: _load_selected_admin_permissions(self),
        )

        ttk.Label(
            panel,
            text="未设置单独限制的管理员保持旧版完整权限；可只勾选 1 项或任意多项。取消全部后保存，可保留管理员身份但禁止所有管理指令。",
            wraplength=900,
        ).grid(row=1, column=0, columnspan=2, sticky="w", pady=(0, 8))

        permissions_frame = ttk.Frame(panel)
        permissions_frame.grid(row=2, column=0, columnspan=2, sticky="ew")
        permissions_frame.columnconfigure(0, weight=1)
        permissions_frame.columnconfigure(1, weight=1)
        self._admin_permission_vars: dict[str, tk.BooleanVar] = {}
        for index, (key, label) in enumerate(ADMIN_PERMISSION_DEFS):
            permission_var = tk.BooleanVar(value=True)
            self._admin_permission_vars[key] = permission_var
            ttk.Checkbutton(
                permissions_frame,
                text=label,
                variable=permission_var,
            ).grid(row=index // 2, column=index % 2, sticky="w", padx=(0, 18), pady=3)

        buttons = ttk.Frame(panel)
        buttons.grid(row=3, column=0, columnspan=2, sticky="w", pady=(12, 0))
        ttk.Button(
            buttons,
            text="保存该管理员权限",
            command=lambda: _save_selected_admin_permissions(self),
        ).pack(side="left", padx=(0, 8))
        ttk.Button(
            buttons,
            text="全选",
            command=lambda: _select_all_permission_checks(self),
        ).pack(side="left", padx=(0, 8))
        ttk.Button(
            buttons,
            text="全部取消",
            command=lambda: _clear_permission_checks(self),
        ).pack(side="left", padx=(0, 8))
        ttk.Button(
            buttons,
            text="恢复完整权限",
            command=lambda: _reset_selected_admin_permissions(self),
        ).pack(side="left")

        self._admin_permission_status_var = tk.StringVar(value="")
        ttk.Label(panel, textvariable=self._admin_permission_status_var).grid(
            row=4,
            column=0,
            columnspan=2,
            sticky="w",
            pady=(8, 0),
        )
        _refresh_admin_choices(self, preserve_selection=False)

    @functools.wraps(original_load)
    def load_with_admin_permissions(self: Any) -> None:
        original_load(self)
        if hasattr(self, "_admin_permission_combo"):
            _refresh_admin_choices(self)

    @functools.wraps(original_save)
    def save_with_admin_permissions(self: Any) -> None:
        original_save(self)
        names = _admin_names(self)
        prune_admin_permissions(names, _store_path(self))
        if hasattr(self, "_admin_permission_combo"):
            _refresh_admin_choices(self)

    setattr(control_panel_cls, "_build_quanxian_tab", build_with_admin_permissions)
    setattr(control_panel_cls, "_load_quanxian", load_with_admin_permissions)
    setattr(control_panel_cls, "_save_quanxian", save_with_admin_permissions)
    setattr(control_panel_cls, "_bilipdj_admin_permission_ui_installed", True)
    return True


__all__ = ["patch_admin_permission_ui"]
