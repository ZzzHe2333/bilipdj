from __future__ import annotations

import os
import threading
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

from . import update_version_selector, updater


@dataclass(frozen=True)
class BackupPackageRow:
    version: str
    path: Path
    size_bytes: int


def _directory_size(path: Path) -> int:
    total = 0
    stack = [Path(path)]
    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_symlink():
                            continue
                        if entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                        elif entry.is_file(follow_symlinks=False):
                            total += int(entry.stat(follow_symlinks=False).st_size)
                    except OSError:
                        continue
        except OSError:
            continue
    return total


def _format_size(size_bytes: int) -> str:
    value = float(max(0, int(size_bytes)))
    units = ("B", "KB", "MB", "GB", "TB")
    unit = units[0]
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            break
        value /= 1024.0
    if unit == "B":
        return f"{int(value)} {unit}"
    return f"{value:.1f} {unit}"


def collect_backup_packages(app_dir: Path) -> list[BackupPackageRow]:
    rows: list[BackupPackageRow] = []
    for backup in update_version_selector.discover_local_backups(Path(app_dir)):
        rows.append(
            BackupPackageRow(
                version=backup.version,
                path=backup.path,
                size_bytes=_directory_size(backup.path),
            )
        )
    return rows


def _validated_backup_path(app_dir: Path, candidate: Path) -> Path:
    backup_root = (Path(app_dir).resolve() / update_version_selector.BACKUP_DIR_NAME).resolve()
    target = Path(candidate).resolve()
    if target.parent != backup_root:
        raise ValueError("仅允许清理 backup 目录中的旧版包")
    if target.is_symlink():
        raise ValueError("不允许清理符号链接")
    valid_paths = {item.path.resolve() for item in update_version_selector.discover_local_backups(app_dir)}
    if target not in valid_paths:
        raise ValueError("该备份已不存在或不是可识别的旧版包")
    return target


def remove_backup_package(app_dir: Path, candidate: Path) -> None:
    target = _validated_backup_path(app_dir, candidate)
    updater.remove_path_with_retry(target)


def _refresh_version_picker_after_cleanup(app: Any) -> None:
    """Drop removed local candidates without forcing a network request."""

    mapping = getattr(app, "_update_version_candidates", {})
    if not isinstance(mapping, dict):
        return
    remaining = []
    for candidate in mapping.values():
        if getattr(candidate, "source", "") != "local":
            remaining.append(candidate)
            continue
        backup = getattr(candidate, "backup", None)
        path = getattr(backup, "path", None)
        if path is not None and Path(path).is_dir():
            remaining.append(candidate)
    try:
        update_version_selector._set_version_picker(app, remaining)  # noqa: SLF001
        if remaining:
            update_version_selector.on_version_selected(app)
        else:
            update_version_selector._configure_buttons_for_candidate(app, None)  # noqa: SLF001
    except Exception:
        pass


def open_backup_cleanup_window(app: Any) -> None:
    existing = getattr(app, "_issue268_backup_cleanup_window", None)
    try:
        if existing is not None and existing.winfo_exists():
            existing.deiconify()
            existing.lift()
            return
    except Exception:
        pass

    window = tk.Toplevel(app.root)
    app._issue268_backup_cleanup_window = window
    window.title("清理旧版包")
    window.geometry("560x390")
    window.minsize(500, 330)
    try:
        window.transient(app.root)
    except Exception:
        pass

    shell = ttk.Frame(window, padding=14)
    shell.pack(fill="both", expand=True)
    shell.columnconfigure(0, weight=1)
    shell.rowconfigure(0, weight=1)

    columns = ("version", "size")
    tree = ttk.Treeview(shell, columns=columns, show="headings", selectmode="extended", height=12)
    tree.heading("version", text="版本号")
    tree.heading("size", text="大小")
    tree.column("version", minwidth=220, width=320, anchor="w", stretch=True)
    tree.column("size", minwidth=120, width=150, anchor="e", stretch=False)
    scroll = ttk.Scrollbar(shell, orient="vertical", command=tree.yview)
    tree.configure(yscrollcommand=scroll.set)
    tree.grid(row=0, column=0, sticky="nsew")
    scroll.grid(row=0, column=1, sticky="ns")

    action = ttk.Frame(shell)
    action.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(12, 0))
    selected_button = ttk.Button(action, text="清理选中")
    all_button = ttk.Button(action, text="全部清理")
    close_button = ttk.Button(action, text="关闭", command=window.destroy)
    selected_button.pack(side="left")
    all_button.pack(side="left", padx=(8, 0))
    close_button.pack(side="right")

    row_paths: dict[str, Path] = {}
    state = {"busy": False}

    def set_busy(value: bool) -> None:
        state["busy"] = value
        widget_state = "disabled" if value else "normal"
        selected_button.configure(state=widget_state)
        all_button.configure(state=widget_state)

    def render(rows: list[BackupPackageRow]) -> None:
        if not window.winfo_exists():
            return
        for item in tree.get_children():
            tree.delete(item)
        row_paths.clear()
        for index, row in enumerate(rows):
            iid = f"backup-{index}"
            tree.insert("", "end", iid=iid, values=(f"v{row.version}", _format_size(row.size_bytes)))
            row_paths[iid] = row.path
        set_busy(False)

    def refresh() -> None:
        if state["busy"]:
            return
        set_busy(True)

        def worker() -> None:
            try:
                rows = collect_backup_packages(Path(app._update_app_dir))
            except Exception as exc:  # noqa: BLE001
                rows = []
                error = str(exc)
            else:
                error = ""

            def finish() -> None:
                if error:
                    set_busy(False)
                    messagebox.showerror("清理旧版包", error, parent=window)
                    return
                render(rows)

            try:
                app.root.after(0, finish)
            except Exception:
                pass

        threading.Thread(target=worker, name="bilipdj-backup-list", daemon=True).start()

    def remove_paths(paths: list[Path]) -> None:
        if state["busy"] or not paths:
            return
        set_busy(True)

        def worker() -> None:
            error = ""
            try:
                for path in paths:
                    remove_backup_package(Path(app._update_app_dir), path)
            except Exception as exc:  # noqa: BLE001
                error = str(exc)

            def finish() -> None:
                if error:
                    set_busy(False)
                    messagebox.showerror("清理旧版包", error, parent=window)
                    return
                _refresh_version_picker_after_cleanup(app)
                set_busy(False)
                refresh()

            try:
                app.root.after(0, finish)
            except Exception:
                pass

        threading.Thread(target=worker, name="bilipdj-backup-cleanup", daemon=True).start()

    selected_button.configure(command=lambda: remove_paths([row_paths[iid] for iid in tree.selection() if iid in row_paths]))
    all_button.configure(command=lambda: remove_paths(list(row_paths.values())))

    def on_destroy(_event: Any = None) -> None:
        try:
            if getattr(app, "_issue268_backup_cleanup_window", None) is window:
                app._issue268_backup_cleanup_window = None
        except Exception:
            pass

    window.bind("<Destroy>", on_destroy, add="+")
    refresh()


def install_backup_cleanup_ui() -> bool:
    """Add the cleanup button to the production update page builder."""

    from . import update_page

    original = getattr(update_page, "build_update_tab", None)
    if not callable(original) or bool(getattr(original, "_issue268_backup_cleanup", False)):
        return callable(original)

    def build_update_tab_with_cleanup(app: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
        result = original(app, frame, *args, **kwargs)
        if getattr(app, "_issue268_backup_cleanup_button", None) is not None:
            return result
        incremental = getattr(app, "_update_incremental_button", None)
        button_row = getattr(incremental, "master", None)
        if button_row is None:
            return result
        try:
            button_row.grid_configure(sticky="ew")
            button = ttk.Button(button_row, text="清理旧版包", command=lambda: open_backup_cleanup_window(app))
            button.pack(side="right")
            app._issue268_backup_cleanup_button = button
        except Exception:
            pass
        return result

    setattr(build_update_tab_with_cleanup, "_issue268_backup_cleanup", True)
    update_page.build_update_tab = build_update_tab_with_cleanup
    return True


__all__ = [
    "BackupPackageRow",
    "collect_backup_packages",
    "install_backup_cleanup_ui",
    "open_backup_cleanup_window",
    "remove_backup_package",
]
