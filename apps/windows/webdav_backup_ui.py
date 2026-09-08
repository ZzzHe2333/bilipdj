from __future__ import annotations

import functools
import json
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()


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
    status_var = getattr(panel, "_webdav_status_var", None)
    if status_var is not None:
        status_var.set(status_text)
    if bool(getattr(panel, "_webdav_busy", False)):
        if status_var is not None:
            status_var.set("已有 WebDAV 操作正在执行，请稍候。")
        return
    panel._webdav_busy = True

    def task() -> None:
        result: Any = None
        error = ""
        try:
            result = worker()
        except Exception as exc:  # noqa: BLE001
            error = str(exc)

        def complete() -> None:
            panel._webdav_busy = False
            if error:
                if status_var is not None:
                    status_var.set("操作失败：" + error)
                try:
                    panel._append_log(f"[WebDAV] {error}", warn=True)
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

    threading.Thread(target=task, name="bilipdj-webdav-ui", daemon=True).start()


def _config_payload(panel: Any) -> dict[str, Any]:
    payload = {
        "url": panel._webdav_url_var.get().strip(),
        "username": panel._webdav_username_var.get().strip(),
        "remote_dir": panel._webdav_remote_dir_var.get().strip() or "BiliPDJ_Backup",
        "auto_on_start": bool(panel._webdav_auto_start_var.get()),
        "auto_on_exit": bool(panel._webdav_auto_exit_var.get()),
        "keep_last": panel._webdav_keep_last_var.get().strip() or "10",
    }
    password = panel._webdav_password_var.get()
    if password:
        payload["password"] = password
    return payload


def _save_config_sync(panel: Any) -> dict[str, Any]:
    return _request_json(
        panel,
        "/api/backup/webdav/config",
        method="POST",
        payload={"config": _config_payload(panel)},
    )


def _apply_config(panel: Any, result: dict[str, Any]) -> None:
    cfg = result.get("config", {}) if isinstance(result, dict) else {}
    if not isinstance(cfg, dict):
        cfg = {}
    panel._webdav_url_var.set(str(cfg.get("url", "") or ""))
    panel._webdav_username_var.set(str(cfg.get("username", "") or ""))
    panel._webdav_remote_dir_var.set(str(cfg.get("remote_dir", "BiliPDJ_Backup") or "BiliPDJ_Backup"))
    panel._webdav_keep_last_var.set(str(cfg.get("keep_last", 10) or 10))
    panel._webdav_auto_start_var.set(bool(cfg.get("auto_on_start", False)))
    panel._webdav_auto_exit_var.set(bool(cfg.get("auto_on_exit", False)))
    panel._webdav_password_var.set("")
    password_note = "已保存密码（留空表示保持不变）" if cfg.get("password_set") else "尚未保存密码"
    panel._webdav_password_note_var.set(password_note)


def _load_config(panel: Any) -> None:
    def finish(result: dict[str, Any]) -> None:
        _apply_config(panel, result)
        panel._webdav_status_var.set("WebDAV 设置已加载。")

    _run_async(
        panel,
        "正在读取 WebDAV 设置……",
        lambda: _request_json(panel, "/api/backup/webdav/config"),
        finish,
    )


def _save_config(panel: Any) -> None:
    def finish(result: dict[str, Any]) -> None:
        _apply_config(panel, result)
        panel._webdav_status_var.set("WebDAV 设置已保存。")

    _run_async(panel, "正在保存 WebDAV 设置……", lambda: _save_config_sync(panel), finish)


def _test_connection(panel: Any) -> None:
    def worker() -> dict[str, Any]:
        _save_config_sync(panel)
        return _request_json(panel, "/api/backup/webdav/test", method="POST", payload={})

    def finish(result: dict[str, Any]) -> None:
        panel._webdav_password_var.set("")
        panel._webdav_password_note_var.set("密码已按当前设置保存")
        panel._webdav_status_var.set(str(result.get("message", "WebDAV 连接成功")))

    _run_async(panel, "正在测试 WebDAV 连接……", worker, finish)


def _backup_now(panel: Any) -> None:
    def worker() -> dict[str, Any]:
        _save_config_sync(panel)
        return _request_json(panel, "/api/backup/webdav/run", method="POST", payload={}, timeout=30.0)

    def finish(result: dict[str, Any]) -> None:
        included = ", ".join(result.get("included", []))
        panel._webdav_password_var.set("")
        panel._webdav_status_var.set(
            f"设置备份完成：{result.get('name', '')}"
            + (f"（{included}）" if included else "")
        )
        _refresh_backups(panel, quiet=True)

    _run_async(panel, "正在备份设置到 WebDAV……", worker, finish)


def _refresh_backups(panel: Any, *, quiet: bool = False) -> None:
    def finish(result: dict[str, Any]) -> None:
        backups = result.get("backups", [])
        if not isinstance(backups, list):
            backups = []
        listbox = panel._webdav_backup_list
        listbox.delete(0, "end")
        panel._webdav_backup_names = []
        for item in backups:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name", "") or "")
            if not name:
                continue
            size = int(item.get("size", 0) or 0)
            listbox.insert("end", f"{name}   {size / 1024:.1f} KB")
            panel._webdav_backup_names.append(name)
        panel._webdav_status_var.set(f"找到 {len(panel._webdav_backup_names)} 份设置备份。")

    _run_async(
        panel,
        "正在读取备份列表……" if not quiet else "正在刷新备份列表……",
        lambda: _request_json(panel, "/api/backup/webdav/list", timeout=20.0),
        finish,
    )


def _restore_selected(panel: Any, module: Any) -> None:
    selection = panel._webdav_backup_list.curselection()
    if not selection:
        panel._webdav_status_var.set("请先选择要恢复的设置备份。")
        return
    index = int(selection[0])
    names = getattr(panel, "_webdav_backup_names", [])
    if index >= len(names):
        panel._webdav_status_var.set("备份选择无效，请刷新列表。")
        return
    name = names[index]
    if not module.messagebox.askyesno(
        "恢复设置",
        f"确定从以下 WebDAV 备份恢复设置吗？\n\n{name}\n\n"
        "只会恢复 config.yaml、quanxian.yaml、kaiguan.yaml、style.json；"
        "队列、黑名单和日志不会变化。",
    ):
        return

    def finish(result: dict[str, Any]) -> None:
        restored = ", ".join(result.get("restored", []))
        panel._webdav_status_var.set(
            "设置恢复完成。" + (f" 已恢复：{restored}" if restored else "")
        )
        try:
            panel.load_from_file()
        except Exception:
            pass

    _run_async(
        panel,
        f"正在恢复 {name}……",
        lambda: _request_json(
            panel,
            "/api/backup/webdav/restore",
            method="POST",
            payload={"name": name},
            timeout=30.0,
        ),
        finish,
    )


def _build_webdav_panel(panel: Any, frame: Any, module: Any) -> None:
    if bool(getattr(panel, "_webdav_backup_ui_built", False)):
        return

    panel._webdav_url_var = module.tk.StringVar()
    panel._webdav_username_var = module.tk.StringVar()
    panel._webdav_password_var = module.tk.StringVar()
    panel._webdav_password_note_var = module.tk.StringVar(value="尚未读取密码状态")
    panel._webdav_remote_dir_var = module.tk.StringVar(value="BiliPDJ_Backup")
    panel._webdav_keep_last_var = module.tk.StringVar(value="10")
    panel._webdav_auto_start_var = module.tk.BooleanVar(value=False)
    panel._webdav_auto_exit_var = module.tk.BooleanVar(value=False)
    panel._webdav_status_var = module.tk.StringVar(value="正在读取 WebDAV 设置……")
    panel._webdav_backup_names = []
    panel._webdav_busy = False

    box = module.ttk.LabelFrame(frame, text="WebDAV 设置备份", padding=10)
    box.grid(row=50, column=0, columnspan=4, sticky="ew", pady=(14, 0))
    box.columnconfigure(1, weight=1)
    box.columnconfigure(3, weight=1)

    rows = (
        ("WebDAV 地址", panel._webdav_url_var, False, 0, 0),
        ("用户名", panel._webdav_username_var, False, 0, 2),
        ("密码", panel._webdav_password_var, True, 1, 0),
        ("远程目录", panel._webdav_remote_dir_var, False, 1, 2),
        ("保留份数", panel._webdav_keep_last_var, False, 2, 0),
    )
    for label, var, secret, row, col in rows:
        module.ttk.Label(box, text=label).grid(row=row, column=col, sticky="w", padx=(0 if col == 0 else 16, 6), pady=4)
        entry = module.ttk.Entry(box, textvariable=var, show="*" if secret else "", width=28)
        entry.grid(row=row, column=col + 1, sticky="ew", pady=4)

    module.ttk.Label(box, textvariable=panel._webdav_password_note_var).grid(
        row=2, column=2, columnspan=2, sticky="w", padx=(16, 0), pady=4
    )

    options = module.ttk.Frame(box)
    options.grid(row=3, column=0, columnspan=4, sticky="w", pady=(7, 3))
    module.ttk.Checkbutton(
        options, text="后端启动时自动备份", variable=panel._webdav_auto_start_var
    ).pack(side="left", padx=(0, 14))
    module.ttk.Checkbutton(
        options, text="后端退出时自动备份", variable=panel._webdav_auto_exit_var
    ).pack(side="left")

    actions = module.ttk.Frame(box)
    actions.grid(row=4, column=0, columnspan=4, sticky="w", pady=(7, 6))
    module.ttk.Button(actions, text="保存设置", command=lambda: _save_config(panel)).pack(side="left", padx=(0, 6))
    module.ttk.Button(actions, text="测试连接", command=lambda: _test_connection(panel)).pack(side="left", padx=(0, 6))
    module.ttk.Button(actions, text="立即备份设置", command=lambda: _backup_now(panel)).pack(side="left", padx=(0, 6))
    module.ttk.Button(actions, text="刷新备份列表", command=lambda: _refresh_backups(panel)).pack(side="left", padx=(0, 6))
    module.ttk.Button(actions, text="恢复选中设置", command=lambda: _restore_selected(panel, module)).pack(side="left")

    panel._webdav_backup_list = module.tk.Listbox(box, height=5, exportselection=False)
    panel._webdav_backup_list.grid(row=5, column=0, columnspan=4, sticky="ew", pady=(4, 4))
    module.ttk.Label(
        box,
        text="仅备份 config.yaml / quanxian.yaml / kaiguan.yaml / style.json；不备份队列、黑名单和日志。",
        wraplength=760,
    ).grid(row=6, column=0, columnspan=4, sticky="w", pady=(4, 2))
    module.ttk.Label(box, textvariable=panel._webdav_status_var, wraplength=760).grid(
        row=7, column=0, columnspan=4, sticky="w", pady=(2, 0)
    )

    panel._webdav_backup_ui_built = True
    try:
        panel.root.after(120, lambda: _load_config(panel))
    except Exception:
        pass


def patch_control_panel_webdav_backup(panel_class: type[Any]) -> bool:
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
        def build_settings_with_webdav(self: Any, frame: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_settings_tab(self, frame, *args, **kwargs)
            _build_webdav_panel(self, frame, module)
            return result

        panel_class._build_settings_tab = build_settings_with_webdav
        panel_class._bilipdj_webdav_backup_patch = True
        return True


__all__ = ["patch_control_panel_webdav_backup"]
