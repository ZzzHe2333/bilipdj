from __future__ import annotations

import json
import threading
import urllib.error
import urllib.request
from typing import Any


def install_style_save_transport(features_module: Any | None = None) -> bool:
    if features_module is None:
        from . import control_panel_features as features_module
    current = getattr(features_module, "_start_async_style_save", None)
    if not callable(current):
        return False
    if bool(getattr(current, "_bilipdj_single_write_transport", False)):
        return True

    def start_async_style_save(panel: Any, module: Any, payload: dict[str, Any]) -> None:
        if bool(getattr(panel, "_style_save_busy", False)):
            panel._style_save_pending = dict(payload)
            if hasattr(panel, "_style_save_status_var"):
                panel._style_save_status_var.set("等待保存……")
            return
        panel._style_save_busy = True
        panel._style_save_pending = None
        if hasattr(panel, "_style_save_status_var"):
            panel._style_save_status_var.set("正在后台保存……")
        port = str(panel.port_var.get()).strip() or "9816"

        def worker() -> None:
            error = ""
            saved_through_backend = False
            request = urllib.request.Request(
                f"http://127.0.0.1:{port}/api/style",
                data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                with urllib.request.urlopen(request, timeout=1.2) as response:
                    status = int(getattr(response, "status", 200) or 200)
                    if 200 <= status < 300:
                        saved_through_backend = True
            except (urllib.error.URLError, TimeoutError, OSError, ValueError):
                saved_through_backend = False

            if not saved_through_backend:
                try:
                    backend = module.load_backend_server_module()
                    backend.save_style(payload)
                except Exception as exc:
                    error = str(exc)

            def finish() -> None:
                panel._style_save_busy = False
                if error:
                    panel._style_save_status_var.set("保存失败")
                    panel._append_log(f"[GUI] 样式写入失败：{error}", warn=True)
                else:
                    import time

                    panel._style_save_status_var.set(f"保存成功 {time.strftime('%H:%M:%S')}")
                    panel._append_log(
                        "[GUI] 字体与排版样式已后台保存"
                        + ("并广播" if saved_through_backend else "到本地")
                    )
                    if panel._overlay_window_alive():
                        panel._overlay_style = dict(panel._load_style_data())
                        panel._redraw_overlay()
                pending = getattr(panel, "_style_save_pending", None)
                panel._style_save_pending = None
                if isinstance(pending, dict) and pending != payload:
                    start_async_style_save(panel, module, pending)

            try:
                panel.root.after(0, finish)
            except Exception:
                pass

        threading.Thread(
            target=worker,
            name="bilipdj-style-save",
            daemon=True,
        ).start()

    setattr(start_async_style_save, "_bilipdj_single_write_transport", True)
    setattr(features_module, "_start_async_style_save", start_async_style_save)
    return True


__all__ = ["install_style_save_transport"]
