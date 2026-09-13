from __future__ import annotations

import threading
from typing import Any

_PATCH_LOCK = threading.RLock()
_SCOPE_ATTRS = (
    ("_backup_include_config_var", "backup_config", "备份配置"),
    ("_backup_include_archive_var", "backup_archive", "备份存档"),
    ("_backup_include_style_var", "backup_style", "备份样式"),
)


def _walk_widgets(widget: Any):
    for child in list(widget.winfo_children()):
        yield child
        yield from _walk_widgets(child)


def _scope_enabled(panel: Any) -> bool:
    for attr, _key, _label in _SCOPE_ATTRS:
        variable = getattr(panel, attr, None)
        try:
            if variable is not None and bool(variable.get()):
                return True
        except Exception:
            continue
    return False


def patch_backup_options_ui() -> bool:
    """Patch the existing backup page with selectable backup scopes."""
    from . import webdav_backup_ui as ui

    with _PATCH_LOCK:
        if bool(getattr(ui, "_issue246_backup_options_installed", False)):
            return True

        original_config_payload = ui._config_payload
        original_apply_config = ui._apply_config
        original_build_panel = ui._build_backup_panel

        def config_payload(panel: Any) -> dict[str, Any]:
            payload = dict(original_config_payload(panel))
            for attr, key, _label in _SCOPE_ATTRS:
                variable = getattr(panel, attr, None)
                try:
                    payload[key] = bool(variable.get()) if variable is not None else True
                except Exception:
                    payload[key] = True
            return payload

        def apply_config(panel: Any, result: dict[str, Any]) -> None:
            original_apply_config(panel, result)
            cfg = result.get("config", {}) if isinstance(result, dict) else {}
            if not isinstance(cfg, dict):
                cfg = {}
            for attr, key, _label in _SCOPE_ATTRS:
                variable = getattr(panel, attr, None)
                if variable is not None:
                    variable.set(bool(cfg.get(key, True)))

        def backup_now(panel: Any) -> None:
            if not _scope_enabled(panel):
                message = "未选择任何备份内容，无法备份。请至少开启一项。"
                panel._backup_status_var.set(message)
                messagebox = getattr(panel, "_backup_messagebox", None)
                if messagebox is not None:
                    try:
                        messagebox.showwarning("无法备份", message, parent=panel.root)
                    except Exception:
                        pass
                return

            def worker() -> dict[str, Any]:
                saved = ui._save_config_sync(panel)
                backed_up = ui._request_json(
                    panel,
                    "/api/backup/settings/run",
                    method="POST",
                    payload={},
                    timeout=30.0,
                )
                return {"saved": saved, "backed_up": backed_up}

            def finish(result: dict[str, Any]) -> None:
                saved = result.get("saved", {})
                backed_up = result.get("backed_up", {})
                if isinstance(saved, dict):
                    ui._apply_config(panel, saved)
                included = backed_up.get("included", [])
                count = len(included) if isinstance(included, list) else 0
                suffix = f"（{count} 个文件）" if count else ""
                panel._backup_status_var.set(f"备份完成：{backed_up.get('name', '')}{suffix}")
                ui._refresh_backups(panel, quiet=True, sync_config=False)

            ui._run_async(panel, "正在备份……", worker, finish)

        def restore_selected(panel: Any, module: Any) -> None:
            selection = panel._backup_list.curselection()
            if not selection:
                panel._backup_status_var.set("请先选择要恢复的备份。")
                return
            if getattr(panel, "_backup_list_signature", None) != ui._target_signature(panel):
                ui._clear_backup_list(panel, "备份目标或路径已变化，请先刷新备份列表再恢复。")
                return
            index = int(selection[0])
            names = getattr(panel, "_backup_names", [])
            if index >= len(names):
                panel._backup_status_var.set("备份选择无效，请刷新列表。")
                return
            name = names[index]
            prompt = (
                f"确定从以下备份恢复数据吗？\n\n{name}\n\n"
                "将恢复该备份包中实际包含的配置、存档和/或样式。"
            )
            if not module.messagebox.askyesno("恢复备份", prompt):
                return

            def worker() -> dict[str, Any]:
                saved = ui._save_config_sync(panel)
                restored = ui._request_json(
                    panel,
                    "/api/backup/settings/restore",
                    method="POST",
                    payload={"name": name},
                    timeout=30.0,
                )
                return {"saved": saved, "restored": restored}

            def finish(result: dict[str, Any]) -> None:
                saved = result.get("saved", {})
                restored_result = result.get("restored", {})
                if isinstance(saved, dict):
                    ui._apply_config(panel, saved)
                restored = restored_result.get("restored", [])
                count = len(restored) if isinstance(restored, list) else 0
                panel._backup_status_var.set(f"备份恢复完成。已恢复 {count} 个文件。")
                try:
                    panel.load_from_file()
                except Exception:
                    pass
                ui._refresh_backups(panel, quiet=True, sync_config=False)

            ui._run_async(panel, f"正在恢复 {name}……", worker, finish)

        def build_backup_panel(panel: Any, frame: Any, module: Any) -> None:
            original_build_panel(panel, frame, module)
            panel._backup_messagebox = module.messagebox
            for attr, _key, _label in _SCOPE_ATTRS:
                if getattr(panel, attr, None) is None:
                    setattr(panel, attr, module.tk.BooleanVar(value=True))

            children = list(frame.winfo_children())
            if not children:
                return
            box = children[0]

            note_var_name = str(getattr(panel, "_backup_backend_note_var", ""))
            status_var_name = str(getattr(panel, "_backup_status_var", ""))
            for widget in list(_walk_widgets(box)):
                try:
                    text = str(widget.cget("text") or "")
                except Exception:
                    text = ""
                try:
                    textvariable = str(widget.cget("textvariable") or "")
                except Exception:
                    textvariable = ""
                if text.startswith("只备份 config.yaml") or (note_var_name and textvariable == note_var_name):
                    try:
                        widget.destroy()
                    except Exception:
                        pass
                    continue
                if text == "立即备份设置":
                    try:
                        widget.configure(text="立即备份")
                    except Exception:
                        pass
                elif text == "恢复选中设置":
                    try:
                        widget.configure(text="恢复选中备份")
                    except Exception:
                        pass
                if status_var_name and textvariable == status_var_name:
                    try:
                        widget.grid_configure(row=6)
                    except Exception:
                        pass

            scope = module.ttk.Frame(box)
            scope.grid(row=2, column=0, columnspan=2, sticky="w", pady=(4, 4))
            module.ttk.Label(scope, text="备份内容").grid(row=0, column=0, sticky="w", padx=(0, 10))
            for index, (attr, _key, label) in enumerate(_SCOPE_ATTRS, start=1):
                module.ttk.Checkbutton(scope, text=label, variable=getattr(panel, attr)).grid(
                    row=0,
                    column=index,
                    sticky="w",
                    padx=(0, 16),
                )
            panel._backup_scope_frame = scope

        ui._config_payload = config_payload
        ui._apply_config = apply_config
        ui._backup_now = backup_now
        ui._restore_selected = restore_selected
        ui._build_backup_panel = build_backup_panel
        ui._issue246_backup_options_installed = True
        return True


__all__ = ["patch_backup_options_ui"]
