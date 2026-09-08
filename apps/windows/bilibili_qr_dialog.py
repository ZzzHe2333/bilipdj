from __future__ import annotations

import base64
import io
import json
import threading
import tkinter as tk
import urllib.error
import urllib.request
from tkinter import ttk
from typing import Any, Callable

try:
    from PIL import Image, ImageTk
except Exception:  # noqa: BLE001
    Image = ImageTk = None

POLL_INTERVAL_MS = 2000
REQUEST_TIMEOUT_SECONDS = 5.0


class BilibiliQrLoginDialog:
    """Native Tk dialog backed by the existing local Bilibili QR HTTP API."""

    def __init__(
        self,
        parent: tk.Misc,
        *,
        port: str | int = "9816",
        on_success: Callable[[str, int, str], None] | None = None,
    ) -> None:
        self.parent = parent
        self.port = str(port).strip() or "9816"
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.on_success = on_success
        self.window = tk.Toplevel(parent)
        self.window.title("Bilibili 扫码登录")
        self.window.geometry("430x560")
        self.window.minsize(400, 520)
        self.window.transient(parent)
        self.window.protocol("WM_DELETE_WINDOW", self.close)
        self.window.columnconfigure(0, weight=1)
        self.window.rowconfigure(0, weight=1)

        self.status_var = tk.StringVar(value="正在请求二维码…")
        self._qrcode_key = ""
        self._generation = 0
        self._closed = False
        self._poll_job: str | None = None
        self._photo: Any | None = None
        self._busy = False

        self._build_ui()
        try:
            self.window.grab_set()
        except tk.TclError:
            pass
        self.refresh()

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.window, padding=18)
        frame.grid(row=0, column=0, sticky="nsew")
        frame.columnconfigure(0, weight=1)

        ttk.Label(frame, text="Bilibili 扫码登录", font=("Microsoft YaHei UI", 16, "bold")).grid(
            row=0, column=0, sticky="w", pady=(0, 6)
        )
        ttk.Label(
            frame,
            text="使用 Bilibili App 扫码并在手机上确认。登录成功后会自动回填 Cookie 与 UID。",
            wraplength=380,
            justify="left",
        ).grid(row=1, column=0, sticky="ew", pady=(0, 14))

        self.qr_frame = ttk.Frame(frame, width=300, height=300)
        self.qr_frame.grid(row=2, column=0, pady=(0, 14))
        self.qr_frame.grid_propagate(False)
        self.qr_label = ttk.Label(self.qr_frame, text="二维码加载中…", anchor="center", justify="center")
        self.qr_label.place(relx=0.5, rely=0.5, anchor="center")

        ttk.Label(frame, textvariable=self.status_var, wraplength=380, justify="left").grid(
            row=3, column=0, sticky="ew", pady=(0, 14)
        )

        buttons = ttk.Frame(frame)
        buttons.grid(row=4, column=0, sticky="ew")
        buttons.columnconfigure(0, weight=1)
        buttons.columnconfigure(1, weight=1)
        self.refresh_button = ttk.Button(buttons, text="刷新二维码", command=self.refresh)
        self.refresh_button.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        ttk.Button(buttons, text="关闭", command=self.close).grid(row=0, column=1, sticky="ew", padx=(6, 0))

    def _set_status(self, text: str) -> None:
        if not self._closed:
            self.status_var.set(text)

    def _run_worker(self, target: Callable[[], None], *, name: str) -> None:
        threading.Thread(target=target, name=name, daemon=True).start()

    def _get_json(self, path: str) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            headers={"Accept": "application/json"},
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    def _post_json(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.base_url}{path}",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8", errors="replace"))

    def refresh(self) -> None:
        if self._closed or self._busy:
            return
        self._generation += 1
        generation = self._generation
        self._cancel_poll()
        self._qrcode_key = ""
        self._busy = True
        self.refresh_button.configure(state="disabled")
        self.qr_label.configure(image="", text="二维码加载中…")
        self._photo = None
        self._set_status("正在请求二维码…")

        def worker() -> None:
            try:
                payload = self._get_json("/api/bili/qr/start")
                data = payload.get("data", {}) if isinstance(payload, dict) else {}
                if not isinstance(data, dict):
                    data = {}
                qrcode_key = str(data.get("qrcode_key", "") or "").strip()
                qr_image = str(data.get("qr_image_base64", "") or "").strip()
                qr_url = str(data.get("url", "") or "").strip()
                qr_error = str(data.get("qr_image_error", "") or "").strip()
                if not qrcode_key:
                    message = str(payload.get("message", "二维码数据为空")) if isinstance(payload, dict) else "二维码数据为空"
                    raise ValueError(message)
                self.parent.after(
                    0,
                    lambda: self._apply_qr_result(generation, qrcode_key, qr_image, qr_url, qr_error),
                )
            except Exception as exc:  # noqa: BLE001
                self.parent.after(0, lambda exc=exc: self._apply_request_error(generation, "二维码请求失败", exc))

        self._run_worker(worker, name="bilipdj-bili-qr-start")

    def _apply_qr_result(
        self,
        generation: int,
        qrcode_key: str,
        qr_image_base64: str,
        qr_url: str,
        qr_error: str,
    ) -> None:
        if self._closed or generation != self._generation:
            return
        self._busy = False
        self.refresh_button.configure(state="normal")
        self._qrcode_key = qrcode_key
        try:
            self._render_qr(qr_image_base64)
        except Exception as exc:  # noqa: BLE001
            fallback = qr_url or "请点击刷新二维码重试"
            self.qr_label.configure(image="", text=f"二维码图片加载失败\n{fallback}")
            self._photo = None
            qr_error = qr_error or str(exc)
        self._set_status(f"二维码已生成，但图片显示有异常：{qr_error}" if qr_error else "请使用 Bilibili App 扫码。")
        self._schedule_poll(generation)

    def _render_qr(self, qr_image_base64: str) -> None:
        if not qr_image_base64:
            raise ValueError("后端未返回二维码图片")
        if Image is None or ImageTk is None:
            raise RuntimeError("Pillow/ImageTk 不可用")
        raw = base64.b64decode(qr_image_base64, validate=True)
        image = Image.open(io.BytesIO(raw)).convert("RGB")
        image.thumbnail((280, 280))
        self._photo = ImageTk.PhotoImage(image)
        self.qr_label.configure(image=self._photo, text="")

    def _schedule_poll(self, generation: int) -> None:
        if self._closed or generation != self._generation or not self._qrcode_key:
            return
        self._poll_job = self.parent.after(POLL_INTERVAL_MS, lambda: self._start_poll(generation))

    def _start_poll(self, generation: int) -> None:
        self._poll_job = None
        if self._closed or generation != self._generation or not self._qrcode_key:
            return
        qrcode_key = self._qrcode_key

        def worker() -> None:
            try:
                payload = self._post_json("/api/bili/qr/poll", {"qrcode_key": qrcode_key})
                self.parent.after(0, lambda: self._apply_poll_result(generation, payload))
            except Exception as exc:  # noqa: BLE001
                self.parent.after(0, lambda exc=exc: self._apply_poll_error(generation, exc))

        self._run_worker(worker, name="bilipdj-bili-qr-poll")

    def _apply_poll_result(self, generation: int, payload: dict[str, Any]) -> None:
        if self._closed or generation != self._generation:
            return
        data = payload.get("data", {}) if isinstance(payload, dict) else {}
        if not isinstance(data, dict):
            data = {}
        try:
            code = int(data.get("code", -1))
        except (TypeError, ValueError):
            code = -1
        message = str(data.get("message", "") or "").strip()

        if code == 0:
            cookie = str(data.get("cookie", "") or "").strip()
            try:
                uid = int(data.get("uid", 0) or 0)
            except (TypeError, ValueError):
                uid = 0
            uname = str(data.get("uname", "") or "").strip()
            if not cookie:
                self._set_status("扫码成功，但未拿到 Cookie，请刷新二维码重试。")
                return
            account = f"{uname} ({uid})" if uname and uid > 0 else (f"UID {uid}" if uid > 0 else "Bilibili 账号")
            self._set_status(f"登录成功：{account}。登录态已保存。")
            self.refresh_button.configure(state="disabled")
            if callable(self.on_success):
                self.on_success(cookie, uid, uname)
            self.parent.after(700, self.close)
            return
        if code == 86101:
            self._set_status("二维码已生成，请扫码。")
            self._schedule_poll(generation)
            return
        if code == 86090:
            self._set_status("已扫码，请在手机上确认登录。")
            self._schedule_poll(generation)
            return
        if code == 86038:
            self._set_status("二维码已失效，请点击“刷新二维码”。")
            return
        self._set_status(f"等待登录：{message or '状态未知'} (code={code})")
        self._schedule_poll(generation)

    def _apply_poll_error(self, generation: int, exc: Exception) -> None:
        if self._closed or generation != self._generation:
            return
        self._set_status(f"扫码状态查询失败：{self._friendly_error(exc)}；将继续重试。")
        self._schedule_poll(generation)

    def _apply_request_error(self, generation: int, prefix: str, exc: Exception) -> None:
        if self._closed or generation != self._generation:
            return
        self._busy = False
        self.refresh_button.configure(state="normal")
        self.qr_label.configure(image="", text="暂时无法获取二维码")
        self._photo = None
        self._set_status(f"{prefix}：{self._friendly_error(exc)}。请确认后端服务已启动，然后点击“刷新二维码”。")

    @staticmethod
    def _friendly_error(exc: Exception) -> str:
        if isinstance(exc, urllib.error.HTTPError):
            return f"HTTP {exc.code}"
        if isinstance(exc, urllib.error.URLError):
            return str(getattr(exc, "reason", exc))
        return str(exc)

    def _cancel_poll(self) -> None:
        if self._poll_job is None:
            return
        try:
            self.parent.after_cancel(self._poll_job)
        except tk.TclError:
            pass
        self._poll_job = None

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._generation += 1
        self._cancel_poll()
        try:
            self.window.grab_release()
        except tk.TclError:
            pass
        try:
            self.window.destroy()
        except tk.TclError:
            pass


def patch_control_panel_qr_login(panel_class: type[Any]) -> bool:
    """Replace the browser-based login action with the native QR dialog."""

    if not isinstance(panel_class, type):
        return False
    if bool(getattr(panel_class, "_bilipdj_native_qr_login_installed", False)):
        return True

    def open_native_config(self: Any) -> None:
        port = str(self.port_var.get()).strip() or "9816"
        notice = str(getattr(self, "_FREE_NOTICE", "") or "")
        if notice:
            self._append_log(notice, warn=True)

        existing = getattr(self, "_bilibili_qr_dialog", None)
        window = getattr(existing, "window", None)
        if window is not None:
            try:
                if window.winfo_exists():
                    window.lift()
                    window.focus_force()
                    return
            except tk.TclError:
                pass

        def on_success(cookie: str, uid: int, uname: str) -> None:
            self.cookie_var.set(cookie)
            if uid > 0:
                self.uid_var.set(str(uid))
            account = f"{uname}/{uid}" if uname and uid > 0 else (f"UID {uid}" if uid > 0 else "Bilibili 账号")
            self._bilibili_fetch_status_var.set(f"扫码登录成功 · {account} · Cookie 已自动保存")
            self._append_log(f"[GUI] Bilibili 扫码登录成功：{account}，Cookie 已由后端持久化")

        self._bilibili_qr_dialog = BilibiliQrLoginDialog(self.root, port=port, on_success=on_success)

    setattr(panel_class, "open_config", open_native_config)
    setattr(panel_class, "_bilipdj_native_qr_login_installed", True)
    return True


__all__ = ["BilibiliQrLoginDialog", "patch_control_panel_qr_login"]
