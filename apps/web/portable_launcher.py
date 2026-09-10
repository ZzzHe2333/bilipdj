from __future__ import annotations

import multiprocessing as mp
import os
import subprocess
import sys
import threading
import time
import tkinter as tk
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

if __name__ == "__main__":
    # Required by PyInstaller when JavaScript plugins spawn their isolated worker.
    mp.freeze_support()

try:
    import pystray
except Exception:  # pragma: no cover - optional in source/developer mode
    pystray = None

try:
    from PIL import Image, ImageDraw
except Exception:  # pragma: no cover - Pillow is bundled in release builds
    Image = ImageDraw = None

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from apps.server import server as backend  # noqa: E402
from apps.server import configure_runtime_paths  # noqa: E402
from apps.server import main as server_main  # noqa: E402

FROZEN = bool(getattr(sys, "frozen", False))
APP_DIR = Path(sys.executable).resolve().parent if FROZEN else REPO_ROOT
BUNDLE_ROOT = Path(getattr(sys, "_MEIPASS", REPO_ROOT)).resolve()


def _load_version() -> str:
    for path in (BUNDLE_ROOT / "VERSION", APP_DIR / "VERSION", REPO_ROOT / "VERSION"):
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value:
            return value
    return "unknown"


APP_VERSION = _load_version()
APP_NAME = "弹幕排队姬"
WINDOW_TITLE = f"{APP_NAME} 后端服务器系统 v{APP_VERSION}"


def _runtime_port() -> int:
    configure_runtime_paths(backend)
    config = backend.load_config()
    server_cfg = backend.normalize_server_config(config.get("server", {}))
    return int(server_cfg["port"])


def _health_url(port: int) -> str:
    return f"http://127.0.0.1:{port}/health"


def _is_backend_ready(port: int) -> bool:
    try:
        with urllib.request.urlopen(_health_url(port), timeout=0.5) as response:
            return 200 <= int(getattr(response, "status", 200)) < 500
    except (urllib.error.URLError, TimeoutError, OSError, ValueError):
        return False


def _backend_command() -> list[str]:
    if FROZEN:
        return [sys.executable, "--backend"]
    return [sys.executable, str(Path(__file__).resolve()), "--backend"]


def _hidden_backend_process_options() -> dict[str, Any]:
    """Return Windows child-process options that never allocate/show a console."""
    if sys.platform != "win32":
        return {"creationflags": 0}

    options: dict[str, Any] = {
        "creationflags": int(getattr(subprocess, "CREATE_NO_WINDOW", 0)),
    }
    try:
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= int(getattr(subprocess, "STARTF_USESHOWWINDOW", 0))
        startupinfo.wShowWindow = int(getattr(subprocess, "SW_HIDE", 0))
        options["startupinfo"] = startupinfo
    except (AttributeError, OSError):
        pass
    return options


def _run_backend_mode() -> None:
    server_main.main([])


def _tray_image() -> Any | None:
    if Image is None or ImageDraw is None:
        return None
    image = Image.new("RGBA", (64, 64), (32, 24, 74, 255))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((7, 7, 57, 57), radius=13, fill=(108, 92, 231, 255))
    draw.ellipse((16, 16, 26, 26), fill=(255, 255, 255, 255))
    draw.ellipse((38, 16, 48, 26), fill=(255, 255, 255, 255))
    draw.rounded_rectangle((16, 34, 48, 44), radius=5, fill=(255, 255, 255, 255))
    return image


class WebPortableLauncher:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(WINDOW_TITLE)
        self.root.geometry("560x310")
        self.root.minsize(520, 285)
        self.backend_proc: subprocess.Popen[bytes] | None = None
        self.owns_backend = False
        self.port = _runtime_port()
        self.ready = False
        self._closing = False
        self._tray_icon: Any | None = None
        self._tray_thread: threading.Thread | None = None
        self.status_var = tk.StringVar(value="准备启动后端服务器……")
        self.detail_var = tk.StringVar(value=f"本地服务：http://127.0.0.1:{self.port}")
        self.hint_var = tk.StringVar(
            value="后端服务器必须保持运行。关闭后，Web 控制台、Windows 客户端和第三方客户端将无法使用排队、弹幕和管理功能。"
        )
        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.request_close)

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=(22, 18, 22, 18))
        frame.pack(fill="both", expand=True)

        title_row = ttk.Frame(frame)
        title_row.pack(fill="x")
        ttk.Label(
            title_row,
            text=f"弹幕排队姬 后端服务器系统 v{APP_VERSION}",
            font=("Microsoft YaHei UI", 15, "bold"),
        ).pack(side="left")
        ttk.Label(title_row, text="Server").pack(side="right")

        self.progress = ttk.Progressbar(frame, mode="indeterminate")
        self.progress.pack(fill="x", pady=(18, 12))

        ttk.Label(frame, textvariable=self.status_var, font=("Microsoft YaHei UI", 10, "bold")).pack(anchor="w")
        ttk.Label(frame, textvariable=self.detail_var).pack(anchor="w", pady=(5, 0))
        ttk.Label(
            frame,
            textvariable=self.hint_var,
            wraplength=505,
            justify="left",
            foreground="#a24646",
        ).pack(anchor="w", pady=(8, 0))

        buttons = ttk.Frame(frame)
        buttons.pack(fill="x", pady=(22, 0))
        self.config_btn = ttk.Button(buttons, text="打开 Web 控制台", command=self.open_config, state="disabled")
        self.config_btn.pack(side="left", padx=(0, 8))
        self.index_btn = ttk.Button(buttons, text="打开队列看板", command=self.open_index, state="disabled")
        self.index_btn.pack(side="left", padx=(0, 8))
        self.minimize_btn = ttk.Button(buttons, text="隐藏到右下角", command=self.hide_to_tray)
        self.minimize_btn.pack(side="left")
        ttk.Button(buttons, text="停止后端并退出", command=self.confirm_stop_and_exit).pack(side="right")

    def start(self) -> None:
        self.progress.start(12)
        if _is_backend_ready(self.port):
            self.status_var.set("检测到后端服务器已在运行，正在使用现有服务。")
            self.detail_var.set(f"服务地址：http://127.0.0.1:{self.port} · 外部后端")
            self._mark_ready(open_browser=True)
            return

        self.status_var.set("正在后台启动后端服务器……")
        self.detail_var.set(f"服务地址：http://127.0.0.1:{self.port} · 正在等待健康检查")
        try:
            self.backend_proc = subprocess.Popen(
                _backend_command(),
                cwd=str(APP_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                env=os.environ.copy(),
                **_hidden_backend_process_options(),
            )
            self.owns_backend = True
        except OSError as exc:
            self.status_var.set("后端服务器启动失败。")
            self.progress.stop()
            self.detail_var.set(str(exc))
            messagebox.showerror("启动失败", str(exc), parent=self.root)
            return

        threading.Thread(target=self._wait_until_ready, name="bilipdj-server-ready", daemon=True).start()

    def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + 20.0
        error = ""
        while time.monotonic() < deadline:
            process = self.backend_proc
            if process is not None and process.poll() is not None:
                error = f"后端服务器提前退出，退出码 {process.returncode}"
                break
            if _is_backend_ready(self.port):
                self.root.after(0, lambda: self._mark_ready(open_browser=True))
                return
            time.sleep(0.2)
        if not error:
            error = "等待后端服务器启动超时"
        self.root.after(0, lambda text=error: self._mark_failed(text))

    def _mark_ready(self, *, open_browser: bool) -> None:
        self.ready = True
        self.progress.stop()
        self.progress.configure(mode="determinate", maximum=100, value=100)
        self.status_var.set("后端服务器运行正常，客户端可以使用。")
        owner = "本程序管理" if self.owns_backend else "外部服务（退出本窗口不会停止它）"
        self.detail_var.set(f"服务地址：http://127.0.0.1:{self.port} · {owner}")
        self.hint_var.set(
            "请保持后端服务器运行。可点击“隐藏到右下角”让它在系统托盘继续工作；若停止后端，所有前端将失去核心服务。"
        )
        self.config_btn.configure(state="normal")
        self.index_btn.configure(state="normal")
        if open_browser:
            self.open_config()
            # Keep the established source contract while changing the action
            # from a plain minimize to the new system-tray behavior.
            self.root.after(1200, self._auto_minimize)

    def _auto_minimize(self) -> None:
        self.hide_to_tray()

    def _mark_failed(self, error: str) -> None:
        self.ready = False
        self.progress.stop()
        self.status_var.set("后端服务器启动失败。")
        self.detail_var.set(error)
        self.hint_var.set("请检查端口占用或运行目录权限；也请确认安全软件没有拦截，然后重新启动后端服务器系统。")
        try:
            self.root.deiconify()
            self.root.lift()
        except tk.TclError:
            pass
        messagebox.showerror("启动失败", error, parent=self.root)

    def open_config(self) -> None:
        webbrowser.open(f"http://127.0.0.1:{self.port}/control")

    def open_index(self) -> None:
        webbrowser.open(f"http://127.0.0.1:{self.port}/index")

    def _ensure_tray_icon(self) -> bool:
        if self._tray_icon is not None:
            return True
        if pystray is None:
            return False
        image = _tray_image()
        if image is None:
            return False

        def on_show(_icon: Any = None, _item: Any = None) -> None:
            self.root.after(0, self.show_window)

        def on_control(_icon: Any = None, _item: Any = None) -> None:
            self.root.after(0, self.open_config)

        def on_queue(_icon: Any = None, _item: Any = None) -> None:
            self.root.after(0, self.open_index)

        def on_exit(_icon: Any = None, _item: Any = None) -> None:
            self.root.after(0, self.confirm_stop_and_exit)

        menu = pystray.Menu(
            pystray.MenuItem("显示后端服务器系统", on_show, default=True),
            pystray.MenuItem("打开 Web 控制台", on_control),
            pystray.MenuItem("打开队列看板", on_queue),
            pystray.Menu.SEPARATOR,
            pystray.MenuItem("停止后端并退出", on_exit),
        )
        self._tray_icon = pystray.Icon("bilipdj-server", image, WINDOW_TITLE, menu)

        def run_tray() -> None:
            try:
                self._tray_icon.run()
            except Exception:
                self.root.after(0, self.show_window)

        self._tray_thread = threading.Thread(target=run_tray, name="bilipdj-tray", daemon=True)
        self._tray_thread.start()
        return True

    def hide_to_tray(self) -> None:
        if self._closing:
            return
        if self._ensure_tray_icon():
            try:
                self.root.withdraw()
            except tk.TclError:
                pass
            return
        # Source mode or environments without a tray implementation fall back
        # to normal minimization instead of making the window unreachable.
        try:
            self.root.iconify()
        except tk.TclError:
            pass

    def show_window(self) -> None:
        if self._closing:
            return
        try:
            self.root.deiconify()
            self.root.state("normal")
            self.root.lift()
            self.root.focus_force()
        except tk.TclError:
            pass

    def request_close(self) -> None:
        if self._closing:
            return
        if not self.owns_backend:
            # The launcher did not create the backend, so closing this manager
            # is safe and must not terminate somebody else's Server process.
            self._stop_tray()
            self.root.destroy()
            return
        choice = messagebox.askyesnocancel(
            "关闭后端服务器？",
            "关闭后端服务器后，Web 控制台、Windows 客户端和第三方客户端将无法使用排队、弹幕和管理功能。\n\n"
            "选择“是”：停止后端并退出。\n"
            "选择“否”：隐藏到右下角并继续运行。\n"
            "选择“取消”：返回窗口。",
            parent=self.root,
        )
        if choice is True:
            self.stop_and_exit()
        elif choice is False:
            self.hide_to_tray()

    def confirm_stop_and_exit(self) -> None:
        if self._closing:
            return
        if self.owns_backend and self.ready:
            if not messagebox.askyesno(
                "停止后端并退出",
                "确认停止后端服务器？\n\n停止后，所有依赖此后端的前端和第三方客户端都将无法继续使用核心功能。",
                parent=self.root,
            ):
                return
        self.stop_and_exit()

    def _stop_tray(self) -> None:
        icon = self._tray_icon
        self._tray_icon = None
        if icon is not None:
            try:
                icon.stop()
            except Exception:
                pass

    def stop_and_exit(self) -> None:
        if self._closing:
            return
        self._closing = True
        self._stop_tray()
        process = self.backend_proc
        if self.owns_backend and process is not None and process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=4)
            except subprocess.TimeoutExpired:
                try:
                    process.kill()
                except OSError:
                    pass
            except OSError:
                pass
        self.backend_proc = None
        try:
            self.root.destroy()
        except tk.TclError:
            pass


def main() -> None:
    if "--backend" in sys.argv[1:]:
        _run_backend_mode()
        return

    root = tk.Tk()
    app = WebPortableLauncher(root)
    root.after(120, app.start)
    root.mainloop()


if __name__ == "__main__":
    main()
