from __future__ import annotations

import functools
import json
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()
_BACKEND_LABELS = {
    "webdav": "WebDAV",
    "local": "本地文件夹",
    "smb": "SMB / NAS",
}
_LABEL_BACKENDS = {value: key for key, value in _BACKEND_LABELS.items()}


def _port(panel: Any) -> str:
    try:
        value = str(panel.port_var.get()).strip()
    except Exception:
        value = ""
    return value or "9816"


def _request_json(
    panel: Any,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    timeout: float = 15.0,
) -> dict[str, Any]:
    data = None
    headers: dict[str, str] = {}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"http://127.0.0.1:{_port(panel)}{path}",
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        try:
            detail = json.loads(body)
            message = str(detail.get("message", "") or f"HTTP {exc.code}")
        except json.JSONDecodeError:
            message = f"HTTP {exc.code}"
        raise RuntimeError(message) from exc
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"无法连接本地后端：{exc}") from exc

    try:
        result = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("本地后端返回了无效 JSON") from exc
    if not isinstance(result, dict):
        raise RuntimeError("本地后端返回格式异常")
    if str(result.get("status", "ok")) == "error":
        raise RuntimeError(str(result.get("message", "操作失败")))
    return result


def _run_async(panel: Any, status_text: str, worker: Any, finish: Any | None = None) -> None:
    status_var = getattr(panel, "_backup_status_var", None)
    if status_var is not None:
        status_var.set(status_text)
    if bool(getattr(panel, "_backup_busy", False)):
        if status_var is not None:
            status_var.set("已有备份操作正在执行，请稍候。")
        return
    panel._backup_busy = True

    def task() -> None:
        result: Any = None
        error = ""
        try:
            result = worker()
        except Exception as exc:  # noqa: BLE001
            error = str(exc)

        def complete() -> None:
            panel._backup_busy = False
            if error:
                if status_var is not None:
                    status_var.set("操作失败：" + error)
                try:
                    panel._append_log(f"[备份] {error}", warn=True)
                except Exception:
                    pass
                return
            if callable(finish):
                try:
                    finish(result)
                except Exception as exc:  # noqa: BLE001
                    if status_var is not None:
                        status_var.set("界面更新失败：" + str(exc))

        try:
            panel.root.after(0, complete)
        except Exception:
            pass

    threading.Thread(target=task, name="bilipdj-backup-ui", daemon=True).start()


def _selected_backend(panel: Any) -> str:
    label = str(panel._backup_backend_var.get() or "WebDAV")
    return _LABEL_BACKENDS.get(label, label if label in _BACKEND_LABELS else "webdav")


def _config_payload(panel: Any) -> dict[str, Any]:
    payload = {
        "backend": _selected_backend(panel),
        "url": panel._webdav_url_var.get().strip(),
        "username": panel._webdav_username_var.get().strip(),
        "remote_dir": panel._webdav_remote_dir_var.get().strip() or "BiliPDJ_Backup",
        "local_dir": panel._local_backup_dir_var.get().strip(),
        "smb_path": panel._smb_backup_path_var.get().strip(),
        "auto_on_start": bool(panel._backup_auto_start_var.get()),
        "auto_on_exit": bool(panel._backup_auto_exit_var.get()),
        "keep_last": panel._backup_keep_last_var.get().strip() or "10",
    }
    password = panel._webdav_password_var.get()
    if password:
        payload["password"] = password
    return payload


def _save_config_sync(panel: Any) -> dict[str, Any]:
    return _request_json(
        panel,
        "/api/backup/settings/config",
        method="POST",
        payload={"config": _config_payload(panel)},
    )


def _update_backend_panels(panel: Any) -> None:
    backend = _selected_backend(panel)
    for key, frame in panel._backup_backend_frames.items():
        if key == backend:
            frame.grid()
        else:
            frame.grid_remove()
    notes = {
        "webdav": "WebDAV 通过 HTTPS/HTTP 远程保存；密码只保存在本机。",
        "local": "选择一个本地父目录，程序会在其中创建 BiliPDJ_Backup 子目录。",
        "smb": "填写已由系统认证/挂载的共享路径，如 \\\\NAS\\share\\BiliPDJ、Z:\\BiliPDJ 或 /mnt/nas/BiliPDJ。程序不保存 NAS 密码。",
    }
    panel._backup_backend_note_var.set(notes.get(backend, ""))


def _apply_config(panel: Any, result: dict[str, Any]) -> None:
    cfg = result.get("config", {}) if isinstance(result, dict) else {}
    if not isinstance(cfg, dict):
        cfg = {}
    backend = str(cfg.get("backend", "webdav") or "webdav")
    panel._backup_backend_var.set(_BACKEND_LABELS.get(backend, "WebDAV"))
    panel._webdav_url_var.set(str(cfg.get("url", "") or ""))
    panel._webdav_username_var.set(str(cfg.get("username", "") or ""))
    panel._webdav_remote_dir_var.set(str(cfg.get("remote_dir", "BiliPDJ_Backup") or "BiliPDJ_Backup"))
    panel._local_backup_dir_var.set(str(cfg.get("local_dir", "") or ""))
    panel._smb_backup_path_var.set(str(cfg.get("smb_path", "") or ""))
    panel._backup_keep_last_var.set(str(cfg.get("keep_last", 10) or 10))
    panel._backup_auto_start_var.set(bool(cfg.get("auto_on_start", False)))
    panel._backup_auto_exit_var.set(bool(cfg.get("auto_on_exit", False)))
    panel._webdav_password_var.set("")
    password_note = "已保存密码（留空表示保持不变）" if cfg.get("password_set") else "尚未保存密码"
    panel._webdav_password_note_var.set(password_note)
    _update_backend_panels(panel)


def _load_config(panel: Any) -> None:
    def finish(result: dict[str, Any]) -> None:
        _apply_config(panel, result)
        panel._backup_status_var.set("备份设置已加载。")

    _run_async(panel, "正在读取备份设置……", lambda: _request_json(panel, "/api/backup/settings/config"), finish)


def _save_config(panel: Any) -> None:
    def finish(result: dict[str, Any]) -> None:
        _apply_config(panel, result)
        panel._backup_status_var.set("备份设置已保存。")

    _run_async(panel, "正在保存备份设置……", lambda: _save_config_sync(panel), finish)


def _test_connection(panel: Any) -> None:
    def worker() -> dict[str, Any]:
        _save_config_sync(panel)
        return _request_json(panel, "/api/backup/settings/test", method="POST", payload={})

    def finish(result: dict[str, Any]) -> None:
        panel._webdav_password_var.set("")
        panel._backup_status_var.set(str(result.get("message", "备份目标可用")))

    _run_async(panel, "正在测试备份目标……", worker, finish)


def _backup_now(panel: Any) -> None:
    def worker() -> dict[str, Any]:
        _save_config_sync(panel)
        return _request_json(panel, "/api/backup/settings/run", method="POST", payload={}, timeout=30.0)

    def finish(result: dict[str, Any]) -> None:
        included = ", ".join(result.get("included", []))
        panel._webdav_password_var.set("")
        panel._backup_status_var.set(
            f"设置备份完成：{result.get('name', '')}" + (f"（{included}）" if included else "")
        )
        _refresh_backups(panel, quiet=True)

    _run_async(panel, "正在备份设置……", worker, finish)


def _refresh_backups(panel: Any, *, quiet: bool = False) -> None:
    def finish(result: dict[str, Any]) -> None:
        backups = result.get("backups", [])
        if not isinstance(backups, list):
            backups = []
        listbox = panel._backup_list
        listbox.delete(0, "end")
        panel._backup_names = []
        for item in backups:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "") or "")
            if not name:
                continue
            size = int(item.get("size", 0) or 0)
            modified = str(item.get("modified", "") or "")
            suffix = f"   {size / 1024:.1f} KB" + (f"   {modified}" if modified else "")
            listbox.insert("end", name + suffix)
            panel._backup_names.append(name)
        panel._backup_status_var.set(f"找到 {len(panel._backup_names)} 份设置备份。")

    _run_async(
        panel,
        "正在读取备份列表……" if not quiet else "正在刷新备份列表……",
        lambda: _request_json(panel, "/api/backup/settings/list", timeout=20.0),
        finish,
    )


def _restore_selected(panel: Any, module: Any) -> None:
    selection = panel._backup_list.curselection()
    if not selection:
        panel._backup_status_var.set("请先选择要恢复的设置备份。")
        return
    index = int(selection[0])
    names = getattr(panel, "_backup_names", [])
    if index >= len(names):
        panel._backup_status_var.set("备份选择无效，请刷新列表。")
        return
    name = names[index]
    if not module.messagebox.askyesno(
        "恢复设置",
        f"确定从以下备份恢复设置吗？\n\n{name}\n\n"
        "只会恢复 config.yaml、quanxian.yaml、kaiguan.yaml、style.json；"
        "队列、黑名单和日志不会变化。",
    ):
        return

    def finish(result: dict[str, Any]) -> None:
        restored = ", ".join(result.get("restored", []))
        panel._backup_status_var.set("设置恢复完成。" + (f" 已恢复：{restored}" if restored else ""))
        try:
            panel.load_from_file()
        except Exception:
            pass

    _run_async(
        panel,
        f"正在恢复 {name}……",
        lambda: _request_json(
            panel,
            "/api/backup/settings/restore",
            method="POST",
            payload={"name": name},
            timeout=30.0,
        ),
        finish,
    )


def _choose_directory(panel: Any, variable: Any) -> None:
    try:
        from tkinter import filedialog

        initial = str(variable.get() or "").strip() or None
        chosen = filedialog.askdirectory(parent=panel.root, initialdir=initial, mustexist=False)
        if chosen:
            variable.set(chosen)
    except Exception as exc:  # noqa: BLE001
        panel._backup_status_var.set(f"选择目录失败：{exc}")


def _build_backup_panel(panel: Any, frame: Any, module: Any) -> None:
    if bool(getattr(panel, "_backup_ui_built", False)):
        return

    panel._backup_backend_var = module.tk.StringVar(value="WebDAV")
    panel._webdav_url_var = module.tk.StringVar()
    panel._webdav_username_var = module.tk.StringVar()
    panel._webdav_password_var = module.tk.StringVar()
    panel._webdav_password_note_var = module.tk.StringVar(value="尚未读取密码状态")
    panel._webdav_remote_dir_var = module.tk.StringVar(value="BiliPDJ_Backup")
    panel._local_backup_dir_var = module.tk.StringVar()
    panel._smb_backup_path_var = module.tk.StringVar()
    panel._backup_keep_last_var = module.tk.StringVar(value="10")
    panel._backup_auto_start_var = module.tk.BooleanVar(value=False)
    panel._backup_auto_exit_var = module.tk.BooleanVar(value=False)
    panel._backup_backend_note_var = module.tk.StringVar()
    panel._backup_status_var = module.tk.StringVar(value="正在读取备份设置……")
    panel._backup_names = []
    panel._backup_busy = False

    box = module.ttk.LabelFrame(frame, text="设置备份（WebDAV / 本地 / SMB-NAS）", padding=10)
    box.grid(row=50, column=0, columnspan=4, sticky="ew", pady=(14, 0))
    box.columnconfigure(1, weight=1)

    module.ttk.Label(box, text="备份方式").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
    backend_combo = module.ttk.Combobox(
        box,
        textvariable=panel._backup_backend_var,
        values=list(_BACKEND_LABELS.values()),
        state="readonly",
        width=20,
    )
    backend_combo.grid(row=0, column=1, sticky="w", pady=4)
    backend_combo.bind("<<ComboboxSelected>>", lambda _event: _update_backend_panels(panel))

    webdav = module.ttk.Frame(box)
    webdav.grid(row=1, column=0, columnspan=4, sticky="ew")
    webdav.columnconfigure(1, weight=1)
    webdav.columnconfigure(3, weight=1)
    webdav_rows = (
        ("WebDAV 地址", panel._webdav_url_var, False, 0, 0),
        ("用户名", panel._webdav_username_var, False, 0, 2),
        ("密码", panel._webdav_password_var, True, 1, 0),
        ("远程目录", panel._webdav_remote_dir_var, False, 1, 2),
    )
    for label, var, secret, row, col in webdav_rows:
        module.ttk.Label(webdav, text=label).grid(row=row, column=col, sticky="w", padx=(0 if col == 0 else 16, 6), pady=4)
        module.ttk.Entry(webdav, textvariable=var, show="*" if secret else "", width=28).grid(
            row=row, column=col + 1, sticky="ew", pady=4
        )
    module.ttk.Label(webdav, textvariable=panel._webdav_password_note_var).grid(
        row=2, column=0, columnspan=4, sticky="w", pady=(2, 4)
    )

    local = module.ttk.Frame(box)
    local.grid(row=1, column=0, columnspan=4, sticky="ew")
    local.columnconfigure(1, weight=1)
    module.ttk.Label(local, text="本地父目录").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
    module.ttk.Entry(local, textvariable=panel._local_backup_dir_var).grid(row=0, column=1, sticky="ew", pady=4)
    module.ttk.Button(local, text="浏览…", command=lambda: _choose_directory(panel, panel._local_backup_dir_var)).grid(
        row=0, column=2, padx=(6, 0), pady=4
    )

    smb = module.ttk.Frame(box)
    smb.grid(row=1, column=0, columnspan=4, sticky="ew")
    smb.columnconfigure(1, weight=1)
    module.ttk.Label(smb, text="SMB / NAS 路径").grid(row=0, column=0, sticky="w", padx=(0, 6), pady=4)
    module.ttk.Entry(smb, textvariable=panel._smb_backup_path_var).grid(row=0, column=1, sticky="ew", pady=4)
    module.ttk.Button(smb, text="浏览…", command=lambda: _choose_directory(panel, panel._smb_backup_path_var)).grid(
        row=0, column=2, padx=(6, 0), pady=4
    )

    panel._backup_backend_frames = {"webdav": webdav, "local": local, "smb": smb}
    module.ttk.Label(box, textvariable=panel._backup_backend_note_var, wraplength=760).grid(
        row=2, column=0, columnspan=4, sticky="w", pady=(2, 6)
    )

    shared = module.ttk.Frame(box)
    shared.grid(row=3, column=0, columnspan=4, sticky="ew", pady=(2, 4))
    module.ttk.Label(shared, text="保留份数").pack(side="left")
    module.ttk.Entry(shared, textvariable=panel._backup_keep_last_var, width=7).pack(side="left", padx=(6, 16))
    module.ttk.Checkbutton(shared, text="后端启动时自动备份", variable=panel._backup_auto_start_var).pack(side="left", padx=(0, 14))
    module.ttk.Checkbutton(shared, text="后端退出时自动备份", variable=panel._backup_auto_exit_var).pack(side="left")

    actions = module.ttk.Frame(box)
    actions.grid(row=4, column=0, columnspan=4, sticky="w", pady=(7, 6))
    module.ttk.Button(actions, text="保存设置", command=lambda: _save_config(panel)).pack(side="left", padx=(0, 6))
    module.ttk.Button(actions, text="测试目标", command=lambda: _test_connection(panel)).pack(side="left", padx=(0, 6))
    module.ttk.Button(actions, text="立即备份设置", command=lambda: _backup_now(panel)).pack(side="left", padx=(0, 6))
    module.ttk.Button(actions, text="刷新备份列表", command=lambda: _refresh_backups(panel)).pack(side="left", padx=(0, 6))
    module.ttk.Button(actions, text="恢复选中设置", command=lambda: _restore_selected(panel, module)).pack(side="left")

    panel._backup_list = module.tk.Listbox(box, height=6, exportselection=False)
    panel._backup_list.grid(row=5, column=0, columnspan=4, sticky="ew", pady=(4, 4))
    module.ttk.Label(
        box,
        text="只备份 config.yaml / quanxian.yaml / kaiguan.yaml / style.json，并保留各文件最后修改时间；不备份队列、黑名单和日志。",
        wraplength=760,
    ).grid(row=6, column=0, columnspan=4, sticky="w", pady=(4, 2))
    module.ttk.Label(box, textvariable=panel._backup_status_var, wraplength=760).grid(
        row=7, column=0, columnspan=4, sticky="w", pady=(2, 0)
    )

    _update_backend_panels(panel)
    panel._backup_ui_built = True
    try:
        panel.root.after(120, lambda: _load_config(panel))
    except Exception:
        pass


def patch_control_panel_webdav_backup(panel_class: type[Any]) -> bool:
    """Compatibility name: patch the unified settings backup UI."""
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_bilipdj_webdav_backup_patch", False)):
            return True
        original_settings_tab = getattr(panel_class, "_build_settings_tab", None)
        if not callable(original_settings_tab):
            return False

        @functools.wraps(original_settings_tab)
        def build_settings_with_backup(self: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_settings_tab(self, frame, *args, **kwargs)
            _build_backup_panel(self, frame, module)
            return result

        panel_class._build_settings_tab = build_settings_with_backup
        panel_class._bilipdj_webdav_backup_patch = True
        return True


__all__ = ["patch_control_panel_webdav_backup"]