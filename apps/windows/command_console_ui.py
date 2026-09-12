from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


def _api_url(panel: Any) -> str:
    port_var = getattr(panel, "port_var", None)
    port = str(port_var.get() if port_var is not None else "9816").strip() or "9816"
    return f"http://127.0.0.1:{port}/api/control/command"


def _post_command(panel: Any, command: str) -> dict[str, Any]:
    request = urllib.request.Request(
        _api_url(panel),
        data=json.dumps({"command": command}, ensure_ascii=False).encode("utf-8"),
        headers={"Accept": "application/json", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=5.0) as response:
            payload = json.loads(response.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        try:
            payload = json.loads(exc.read().decode("utf-8", errors="replace"))
            message = str(payload.get("message", "") or f"HTTP {exc.code}")
        except Exception:
            message = f"HTTP {exc.code}"
        raise RuntimeError(message) from exc
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"无法连接后端：{exc}") from exc
    if not isinstance(payload, dict) or payload.get("status") != "ok":
        raise RuntimeError(str(payload.get("message", "后端返回无效响应")) if isinstance(payload, dict) else "后端返回无效响应")
    return payload


def _install_console_row(panel: Any) -> None:
    if getattr(panel, "_issue167_console_row", None) is not None:
        return
    log_text = getattr(panel, "log_text", None)
    if log_text is None:
        return

    import tkinter as tk
    from tkinter import ttk

    log_frame = log_text.master
    try:
        log_frame.rowconfigure(0, weight=1)
        log_frame.columnconfigure(0, weight=1)
    except Exception:
        return

    row = ttk.LabelFrame(log_frame, text="后端指令", padding=(10, 8))
    row.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(10, 0))
    row.columnconfigure(1, weight=1)

    ttk.Label(row, text="指令").grid(row=0, column=0, padx=(0, 8), sticky="w")
    panel.backend_command_var = tk.StringVar(value="")
    panel.backend_command_status_var = tk.StringVar(
        value="与直播弹幕共用同一套后端命令处理链；不会执行系统 Shell。"
    )
    entry = ttk.Entry(row, textvariable=panel.backend_command_var)
    entry.grid(row=0, column=1, sticky="ew")
    button = ttk.Button(row, text="发送")
    button.grid(row=0, column=2, padx=(8, 0), sticky="e")
    status = ttk.Label(
        row,
        textvariable=panel.backend_command_status_var,
        justify="left",
        anchor="w",
        wraplength=720,
    )
    status.grid(row=1, column=1, columnspan=2, sticky="ew", pady=(5, 0))

    panel._issue167_console_button = button
    panel._issue167_console_entry = entry
    panel._issue167_console_status = status
    panel._issue167_console_row = row

    def finish_ok(command: str, payload: dict[str, Any]) -> None:
        button.configure(state="normal")
        panel.backend_command_status_var.set(str(payload.get("message", "指令已发送。")))
        if panel.backend_command_var.get().strip() == command:
            panel.backend_command_var.set("")

    def finish_error(message: str) -> None:
        button.configure(state="normal")
        panel.backend_command_status_var.set(f"发送失败：{message}")

    def submit(_event: Any = None) -> str:
        command = panel.backend_command_var.get().strip()
        if not command:
            panel.backend_command_status_var.set("请输入后端指令。")
            entry.focus_set()
            return "break"
        if len(command) > 500:
            panel.backend_command_status_var.set("指令不能超过 500 个字符。")
            entry.focus_set()
            return "break"
        button.configure(state="disabled")
        panel.backend_command_status_var.set("正在送入后端弹幕流…")

        def worker() -> None:
            try:
                payload = _post_command(panel, command)
            except Exception as exc:  # noqa: BLE001
                panel.root.after(0, lambda text=str(exc): finish_error(text))
            else:
                panel.root.after(0, lambda: finish_ok(command, payload))

        threading.Thread(target=worker, daemon=True).start()
        return "break"

    button.configure(command=submit)
    entry.bind("<Return>", submit)


def patch_control_panel_command_console(panel_class: type[Any]) -> bool:
    module_name = str(getattr(panel_class, "__module__", "") or "")
    if not module_name.endswith("control_panel"):
        return False
    if bool(getattr(panel_class, "_issue167_command_console_patched", False)):
        return True
    original_init = panel_class.__init__

    def __init__(self: Any, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        _install_console_row(self)

    panel_class.__init__ = __init__
    panel_class._issue167_command_console_patched = True
    return True


__all__ = ["patch_control_panel_command_console"]
