from __future__ import annotations

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
APP_NAME = "弹幕排队姬 Web"


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


def _run_backend_mode() -> None:
    server_main.main([])


class WebPortableLauncher:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(f"{APP_NAME} v{APP_VERSION}")
        self.root.geometry("520x220")
        self.root.minsize(520, 220)
        self.backend_proc: subprocess.Popen[bytes] | None = None
        self.owns_backend = False
        self.port = _runtime_port()
        self.status_var = tk.StringVar(value="准备启动内置后端…")
        self.detail_var = tk.StringVar(value=f"本地地址：http://127.0.0.1:{self.port}")
        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.close)

    def _build_ui(self) -> None:
        frame = ttk.Frame(self.root, padding=22)
        frame.pack(fill="both", expand=True)
        ttk.Label(frame, text=f"弹幕排队姬 Web v{APP_VERSION}", font=("Microsoft YaHei UI", 16, "bold")).pack(anchor="w")
        ttk.Label(frame, textvariable=self.status_var, font=("Microsoft YaHei UI", 10)).pack(anchor="w", pady=(18, 4))
        ttk.Label(frame, textvariable=self.detail_var).pack(anchor="w")

        buttons = ttk.Frame(frame)
        buttons.pack(fill="x", pady=(22, 0))
        self.config_btn = ttk.Button(buttons, text="打开 Web 控制台", command=self.open_config, state="disabled")
        self.config_btn.pack(side="left", padx=(0, 8))
        self.index_btn = ttk.Button(buttons, text="打开队列看板", command=self.open_index, state="disabled")
        self.index_btn.pack(side="left", padx=(0, 8))
        ttk.Button(buttons, text="停止并退出", command=self.close).pack(side="right")

    def start(self) -> None:
        if _is_backend_ready(self.port):
            self.status_var.set("检测到后端已在运行，直接使用现有服务。")
            self._mark_ready(open_browser=True)
            return

        self.status_var.set("正在启动内置后端…")
        creationflags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        try:
            self.backend_proc = subprocess.Popen(
                _backend_command(),
                cwd=str(APP_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                creationflags=creationflags,
                env=os.environ.copy(),
            )
            self.owns_backend = True
        except OSError as exc:
            self.status_var.set("后端启动失败。")
            messagebox.showerror("启动失败", str(exc), parent=self.root)
            return

        threading.Thread(target=self._wait_until_ready, name="bilipdj-web-ready", daemon=True).start()

    def _wait_until_ready(self) -> None:
        deadline = time.monotonic() + 20.0
        error = ""
        while time.monotonic() < deadline:
            process = self.backend_proc
            if process is not None and process.poll() is not None:
                error = f"内置后端提前退出，退出码 {process.returncode}"
                break
            if _is_backend_ready(self.port):
                self.root.after(0, lambda: self._mark_ready(open_browser=True))
                return
            time.sleep(0.2)
        if not error:
            error = "等待内置后端启动超时"
        self.root.after(0, lambda text=error: self._mark_failed(text))

    def _mark_ready(self, *, open_browser: bool) -> None:
        self.status_var.set("后端已启动，Web 控制台可以使用。")
        self.config_btn.configure(state="normal")
        self.index_btn.configure(state="normal")
        if open_browser:
            self.open_config()

    def _mark_failed(self, error: str) -> None:
        self.status_var.set("后端启动失败。")
        self.detail_var.set(error)
        messagebox.showerror("启动失败", error, parent=self.root)

    def open_config(self) -> None:
        webbrowser.open(f"http://127.0.0.1:{self.port}/control")

    def open_index(self) -> None:
        webbrowser.open(f"http://127.0.0.1:{self.port}/index")

    def close(self) -> None:
        process = self.backend_proc
        if self.owns_backend and process is not None and process.poll() is None:
            try:
                process.terminate()
                process.wait(timeout=4)
            except subprocess.TimeoutExpired:
                process.kill()
            except OSError:
                pass
        self.backend_proc = None
        self.root.destroy()


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
