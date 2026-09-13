from __future__ import annotations

import functools
import threading
import time
import tkinter as tk
from tkinter import ttk
from typing import Any

WARNING_TITLE = "警告"
WARNING_TEXT = "是否需要清空本存档的排队信息？如需清空，请连续点击确定2下（5s内）。"
CONFIRM_WINDOW_SECONDS = 5.0


class DoubleConfirmGate:
    """Two-click confirmation gate with a five-second validity window."""

    def __init__(self, window_seconds: float = CONFIRM_WINDOW_SECONDS) -> None:
        self.window_seconds = max(0.1, float(window_seconds))
        self.first_click_at = 0.0

    def reset(self) -> None:
        self.first_click_at = 0.0

    def click(self, now: float | None = None) -> bool:
        current = time.monotonic() if now is None else float(now)
        if self.first_click_at > 0.0 and 0.0 <= current - self.first_click_at <= self.window_seconds:
            self.reset()
            return True
        self.first_click_at = current
        return False


def _execute_queue_clear(panel: Any) -> None:
    if panel._backend_is_running():
        threading.Thread(
            target=panel._queue_backend_op,
            args=("/api/queue/clear", {}, "已清空"),
            daemon=True,
        ).start()
    else:
        threading.Thread(
            target=panel._queue_local_op,
            args=(lambda _entries: [], "已清空"),
            daemon=True,
        ).start()


def _open_queue_clear_dialog(panel: Any) -> None:
    existing = getattr(panel, "_issue268_queue_clear_dialog", None)
    try:
        if existing is not None and existing.winfo_exists():
            existing.deiconify()
            existing.lift()
            return
    except Exception:
        pass

    window = tk.Toplevel(panel.root)
    panel._issue268_queue_clear_dialog = window
    gate = DoubleConfirmGate()
    panel._issue268_queue_clear_gate = gate
    window.title(WARNING_TITLE)
    window.resizable(False, False)
    try:
        window.transient(panel.root)
        window.grab_set()
    except Exception:
        pass

    body = ttk.Frame(window, padding=(22, 20, 22, 16))
    body.pack(fill="both", expand=True)
    ttk.Label(body, text=WARNING_TEXT, wraplength=390, justify="left").grid(
        row=0, column=0, columnspan=2, sticky="w", pady=(0, 18)
    )

    def close() -> None:
        gate.reset()
        try:
            window.grab_release()
        except Exception:
            pass
        try:
            window.destroy()
        except Exception:
            pass

    def confirm() -> None:
        if not gate.click():
            return
        close()
        _execute_queue_clear(panel)

    ok_button = ttk.Button(body, text="确定", command=confirm, width=12)
    cancel_button = ttk.Button(body, text="取消", command=close, width=12)
    ok_button.grid(row=1, column=0, padx=(0, 10))
    cancel_button.grid(row=1, column=1)
    window.protocol("WM_DELETE_WINDOW", close)

    def on_destroy(_event: Any = None) -> None:
        try:
            if getattr(panel, "_issue268_queue_clear_dialog", None) is window:
                panel._issue268_queue_clear_dialog = None
        except Exception:
            pass

    window.bind("<Destroy>", on_destroy, add="+")
    try:
        window.update_idletasks()
        x = panel.root.winfo_rootx() + max(0, (panel.root.winfo_width() - window.winfo_width()) // 2)
        y = panel.root.winfo_rooty() + max(0, (panel.root.winfo_height() - window.winfo_height()) // 2)
        window.geometry(f"+{x}+{y}")
        ok_button.focus_set()
    except Exception:
        pass


def install_queue_clear_dialog(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    original = getattr(panel_class, "_queue_clear", None)
    if not callable(original):
        return False
    if bool(getattr(original, "_issue268_queue_clear_dialog", False)):
        return True

    @functools.wraps(original)
    def queue_clear_with_dialog(self: Any) -> None:
        _open_queue_clear_dialog(self)

    setattr(queue_clear_with_dialog, "_issue268_queue_clear_dialog", True)
    setattr(panel_class, "_queue_clear", queue_clear_with_dialog)
    return True


__all__ = [
    "CONFIRM_WINDOW_SECONDS",
    "DoubleConfirmGate",
    "WARNING_TEXT",
    "WARNING_TITLE",
    "install_queue_clear_dialog",
]
