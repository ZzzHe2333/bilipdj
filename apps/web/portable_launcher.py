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
    configure_runtime_paths(backend)
    server_main.configure_web_assets()
    config = backend.load_config()
    server_cfg = backend.normalize_server_config(config.get("server", {}))
    host = os.getenv("DANMUJI_BACKEND_HOST") or str(server_cfg["host"])
    port = int(os.getenv("DANMUJI_BACKEND_PORT", int(server_cfg["port"])))
    backend.run_server(host=host, port=port)


def _tray_image() -> Any:
    if Image is None or ImageDraw is None:
        return None
    image = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
    draw = ImageDraw.Draw(image)
    draw.rounded_rectangle((7, 7, 57, 57), radius=14, fill=(27, 129, 230, 255))
    draw.ellipse((19, 21, 29, 31), fill=(255, 255, 255, 255))
    draw.ellipse((35, 21, 45, 31), fill=(255, 255, 255, 255))
    draw.rounded_rectangle((18, 37, 46, 43), radius=3, fill=(255, 255, 255, 255))
    return image


class WebPortableLauncher:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.port = _runtime_port()
        self.backend_proc: subprocess.Popen[Any] | None = None
        self.owns_backend = False
        self.ready = False
        self._closing = False
        self._tray_icon: Any = None
        self._tray_thread: threading.Thread | None = None

        root.title(WINDOW_TITLE)
        root.geometry("620x370")
        root.minsize(560, 330)
        root.protocol("WM_DELETE_WINDOW", self.request_close)

        try:
            icon_path = BUNDLE_ROOT / "apps" / "windows" / "assets" / "256x.ico"
            if icon_path.is_file():
                root.iconbitmap(default=str(icon_path))
        except Exception:
            pass

        container = ttk.Frame(root, padding=24)
        container.pack(fill="both", expand=True)
        ttk.Label(container, text=APP_NAME, font=("Microsoft YaHei UI", 20, "bold")).pack(anchor="w")
        ttk.Label(
            container,
            text=f"Web 便携版后端服务器 · v{APP_VERSION}",
            font=("Microsoft YaHei UI", 10),
        ).pack(anchor="w", pady=(4, 18))

        status_frame = ttk.LabelFrame(container, text="Server 状态", padding=16)
        status_frame.pack(fill="x")
        self.status_var = tk.StringVar(value="正在检查后端状态……")
        self.detail_var = tk.StringVar(value=f"监听地址：http://127.0.0.1:{self.port}")
        ttk.Label(status_frame, textvariable=self.status_var, font=("Microsoft YaHei UI", 11, "bold")).pack(anchor="w")
        ttk.Label(status_frame, textvariable=self.detail_var).pack(anchor="w", pady=(7, 0))

        button_row = ttk.Frame(container)
        button_row.pack(fill="x", pady=(20, 0))
        self.control_button = ttk.Button(button_row, text="打开 Web 控制台", command=self.open_config, state="disabled")
        self.control_button.pack(side="left")
        self.queue_button = ttk.Button(button_row, text="打开队列看板", command=self.open_index, state="disabled")
        self.queue_button.pack(side="left", padx=(10, 0))
        ttk.Button(button_row, text="隐藏到托盘", command=self.hide_to_tray).pack(side="right")

        ttk.Label(
            container,
            text="关闭窗口时可选择停止后端，或隐藏到右下角继续运行。",
            foreground="#666666",
        ).pack(anchor="w", pady=(20, 0))

    def start(self) -> None:
        if _is_backend_ready(self.port):
            self.ready = True
            self.owns_backend = False
            self._set_ready("检测到已运行的 BiliPDJ 后端；本窗口不会在退出时结束它。")
            return
        try:
            options = _hidden_backend_process_options()
            self.backend_proc = subprocess.Popen(
                _backend_command(),
                cwd=str(APP_DIR),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL if FROZEN else None,
                stderr=subprocess.DEVNULL if FROZEN else None,
                **options,
            )
            self.owns_backend = True
        except OSError as exc:
            self.status_var.set("后端启动失败")
            self.detail_var.set(str(exc))
            return

        self.status_var.set("正在启动后端服务器……")
        self.detail_var.set(f"等待 http://127.0.0.1:{self.port}/health")
        self.root.after(150, self._poll_backend_ready)

    def _poll_backend_ready(self) -> None:
        if self._closing:
            return
        process = self.backend_proc
        if process is not None and process.poll() is not None:
            self.status_var.set("后端进程已退出")
            self.detail_var.set(f"退出代码：{process.returncode}")
            return
        if _is_backend_ready(self.port):
            self.ready = True
            self._set_ready(f"后端已启动，监听 127.0.0.1:{self.port}。")
            return
        self.root.after(250, self._poll_backend_ready)

    def _set_ready(self, detail: str) -> None:
        self.status_var.set("后端服务器运行中")
        self.detail_var.set(detail)
        self.control_button.configure(state="normal")
        self.queue_button.configure(state="normal")

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
