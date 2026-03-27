from __future__ import annotations

import json
import queue
import subprocess
import sys
import threading
import tkinter as tk
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from tkinter import messagebox, ttk

REPO_DIR = Path(__file__).resolve().parent.parent
BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", REPO_DIR))
APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else REPO_DIR
CONFIG_PATH = APP_DIR / "config.yaml"
SERVER_PATH = BUNDLE_DIR / "backend" / "server.py"
APP_VERSION = "0.4.0"
LOG_LEVEL_OPTIONS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
MAX_QUEUE_ARCHIVE_SLOTS = 5


def parse_scalar(value: str):
    value = value.strip()
    if value == "":
        return ""
    lowered = value.lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    if lowered in {"null", "none"}:
        return None
    if (value.startswith('"') and value.endswith('"')) or (
        value.startswith("'") and value.endswith("'")
    ):
        return value[1:-1]
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        return value


def load_simple_yaml(path: Path) -> dict:
    if not path.exists():
        return {}

    root: dict = {}
    stack: list[tuple[int, dict]] = [(-1, root)]

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        if ":" not in stripped:
            continue

        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()

        while stack and indent <= stack[-1][0]:
            stack.pop()

        current = stack[-1][1] if stack else root
        if value == "":
            child: dict = {}
            current[key] = child
            stack.append((indent, child))
        else:
            current[key] = parse_scalar(value)

    return root


def save_config(path: Path, config: dict) -> None:
    server = config.get("server", {})
    api = config.get("api", {})
    logging_cfg = config.get("logging", {})
    queue_archive = config.get("queue_archive", {})
    slots = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(queue_archive.get("slots", 3))))
    escaped_cookie = str(api.get("cookie", "")).replace('"', '\\"')

    content = f"""# Danmuji 全局配置
server:
  host: {server.get('host', '0.0.0.0')}
  port: {int(server.get('port', 9816))}

api:
  roomid: {int(api.get('roomid', 0))}
  uid: {int(api.get('uid', 0))}
  cookie: \"{escaped_cookie}\"

# 前端 myjs.js 可覆盖配置（如需扩展可继续加键值）
myjs:

logging:
  # 支持 DEBUG / INFO / WARNING / ERROR / CRITICAL
  level: {str(logging_cfg.get('level', 'INFO')).upper()}
  # 每次启动默认清理多少天前日志
  retention_days: {int(logging_cfg.get('retention_days', 15))}

queue_archive:
  enabled: {'true' if bool(queue_archive.get('enabled', True)) else 'false'}
  # 存档位（1~5）
  slots: {slots}
"""
    path.write_text(content, encoding="utf-8")


class ControlPanelApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(f"Danmuji 控制台 v{APP_VERSION}")
        self.server_proc: subprocess.Popen[str] | None = None
        self.log_queue: queue.Queue[str] = queue.Queue()
        self.log_pump_running = False
        self.stdout_thread: threading.Thread | None = None
        self.stderr_thread: threading.Thread | None = None

        self.status_var = tk.StringVar(value="服务未启动")
        self.host_var = tk.StringVar(value="0.0.0.0")
        self.port_var = tk.StringVar(value="9816")
        self.roomid_var = tk.StringVar(value="0")
        self.uid_var = tk.StringVar(value="0")
        self.cookie_var = tk.StringVar(value="")
        self.log_level_var = tk.StringVar(value="INFO")
        self.retention_days_var = tk.StringVar(value="15")
        self.queue_enabled_var = tk.BooleanVar(value=True)
        self.queue_slots_var = tk.StringVar(value="3")
        self.queue_slot_choice_var = tk.IntVar(value=3)
        self.ws_light_var = tk.StringVar(value="●")
        self.ws_text_var = tk.StringVar(value="直播间链接状态：未连接")

        self._build_ui()
        self.load_from_file()
        self.root.after(200, self.start_server)
        self.root.after(1000, self.refresh_runtime_status)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.grid(sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        row = 0
        for label, var in [
            ("监听地址", self.host_var),
            ("监听端口", self.port_var),
            ("直播间号", self.roomid_var),
            ("UID", self.uid_var),
            ("Cookie", self.cookie_var),
            ("日志保留天数", self.retention_days_var),
        ]:
            ttk.Label(main, text=label).grid(row=row, column=0, sticky="w", pady=4)
            if label == "Cookie":
                entry = ttk.Entry(main, textvariable=var, width=60)
            else:
                entry = ttk.Entry(main, textvariable=var, width=30)
            entry.grid(row=row, column=1, sticky="ew", pady=4)
            row += 1

        ttk.Label(main, text="日志等级").grid(row=row, column=0, sticky="w", pady=4)
        self.log_level_combo = ttk.Combobox(
            main,
            textvariable=self.log_level_var,
            values=LOG_LEVEL_OPTIONS,
            width=27,
            state="readonly",
        )
        self.log_level_combo.grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        ttk.Label(main, text="日志存档槽位").grid(row=row, column=0, sticky="w", pady=4)
        slot_frame = ttk.Frame(main)
        slot_frame.grid(row=row, column=1, sticky="w", pady=4)
        for slot in range(1, MAX_QUEUE_ARCHIVE_SLOTS + 1):
            ttk.Radiobutton(
                slot_frame,
                text=f"槽位{slot}",
                variable=self.queue_slot_choice_var,
                value=slot,
            ).grid(row=0, column=slot - 1, padx=(0, 8), sticky="w")
        row += 1

        ttk.Checkbutton(main, text="启用排队存档", variable=self.queue_enabled_var).grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        button_bar = ttk.Frame(main)
        button_bar.grid(row=row, column=0, columnspan=2, sticky="w", pady=(8, 4))
        ttk.Button(button_bar, text="保存配置", command=self.save_to_file).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_bar, text="刷新配置", command=self.load_from_file).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_bar, text="启动后端", command=self.start_server).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(button_bar, text="停止后端", command=self.stop_server).grid(row=0, column=3, padx=(0, 8))
        ttk.Button(button_bar, text="打开Web界面", command=self.open_web).grid(row=0, column=4)

        status_bar = ttk.Frame(main)
        status_bar.grid(row=row + 1, column=0, columnspan=2, sticky="w", pady=(8, 0))
        ttk.Label(status_bar, textvariable=self.ws_light_var, foreground="#0b5", font=("Arial", 14, "bold")).grid(
            row=0, column=0, sticky="w"
        )
        ttk.Label(status_bar, textvariable=self.ws_text_var).grid(row=0, column=1, sticky="w", padx=(8, 0))

        ttk.Label(main, textvariable=self.status_var, foreground="#0b5").grid(
            row=row + 2, column=0, columnspan=2, sticky="w", pady=(8, 0)
        )

        ttk.Label(main, text="实时日志").grid(row=row + 3, column=0, sticky="nw", pady=(10, 4))
        log_frame = ttk.Frame(main)
        log_frame.grid(row=row + 3, column=1, sticky="nsew", pady=(10, 4))
        self.log_text = tk.Text(log_frame, height=10, wrap="word", state="disabled")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        main.columnconfigure(1, weight=1)
        main.rowconfigure(row + 3, weight=1)

    def load_from_file(self) -> None:
        config = load_simple_yaml(CONFIG_PATH)
        server = config.get("server", {})
        api = config.get("api", {})
        logging_cfg = config.get("logging", {})
        queue_archive = config.get("queue_archive", {})

        self.host_var.set(str(server.get("host", "0.0.0.0")))
        self.port_var.set(str(server.get("port", 9816)))
        self.roomid_var.set(str(api.get("roomid", 0)))
        self.uid_var.set(str(api.get("uid", 0)))
        self.cookie_var.set(str(api.get("cookie", "")))
        self.log_level_var.set(str(logging_cfg.get("level", "INFO")))
        self.retention_days_var.set(str(logging_cfg.get("retention_days", 15)))
        self.queue_enabled_var.set(bool(queue_archive.get("enabled", True)))
        loaded_slots = int(queue_archive.get("slots", 3))
        loaded_slots = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, loaded_slots))
        self.queue_slot_choice_var.set(loaded_slots)
        self.queue_slots_var.set(str(loaded_slots))
        self.status_var.set("已加载配置")
        self._append_log("[GUI] 已加载配置")

    def refresh_runtime_status(self) -> None:
        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}/api/runtime-status"
        try:
            with urllib.request.urlopen(url, timeout=1.5) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
            active = bool(payload.get("danmu_stream_active"))
            ws_clients = int(payload.get("ws_clients", 0))
            if active:
                self.ws_light_var.set("🟢")
                self.ws_text_var.set(f"直播间链接状态：已连接（WS 客户端 {ws_clients}）")
            else:
                self.ws_light_var.set("🔴")
                self.ws_text_var.set(f"直播间链接状态：等待弹幕流（WS 客户端 {ws_clients}）")
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
            self.ws_light_var.set("🔴")
            self.ws_text_var.set("直播间链接状态：后端未响应")
        finally:
            self.root.after(2000, self.refresh_runtime_status)

    def gather_config(self) -> dict:
        return {
            "server": {
                "host": self.host_var.get().strip() or "0.0.0.0",
                "port": int(self.port_var.get().strip() or 9816),
            },
            "api": {
                "roomid": int(self.roomid_var.get().strip() or 0),
                "uid": int(self.uid_var.get().strip() or 0),
                "cookie": self.cookie_var.get().strip(),
            },
            "myjs": {},
            "logging": {
                "level": self.log_level_var.get().strip().upper() or "INFO",
                "retention_days": int(self.retention_days_var.get().strip() or 15),
            },
            "queue_archive": {
                "enabled": bool(self.queue_enabled_var.get()),
                "slots": min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(self.queue_slot_choice_var.get()))),
            },
        }

    def _append_log(self, message: str) -> None:
        self.log_text.configure(state="normal")
        self.log_text.insert("end", f"{message}\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _enqueue_log(self, message: str) -> None:
        self.log_queue.put(message)

    def _schedule_log_pump(self) -> None:
        if self.log_pump_running:
            return
        self.log_pump_running = True
        self.root.after(120, self._flush_log_queue)

    def _flush_log_queue(self) -> None:
        while True:
            try:
                message = self.log_queue.get_nowait()
            except queue.Empty:
                break
            self._append_log(message)
        self.root.after(120, self._flush_log_queue)

    def _read_stream_lines(self, stream, tag: str) -> None:
        try:
            for line in iter(stream.readline, ""):
                text = line.rstrip()
                if text:
                    self._enqueue_log(f"[{tag}] {text}")
        finally:
            try:
                stream.close()
            except OSError:
                pass

    def _bind_process_logs(self) -> None:
        if not self.server_proc:
            return
        if self.server_proc.stdout:
            self.stdout_thread = threading.Thread(
                target=self._read_stream_lines,
                args=(self.server_proc.stdout, "STDOUT"),
                daemon=True,
            )
            self.stdout_thread.start()
        if self.server_proc.stderr:
            self.stderr_thread = threading.Thread(
                target=self._read_stream_lines,
                args=(self.server_proc.stderr, "STDERR"),
                daemon=True,
            )
            self.stderr_thread.start()

    def save_to_file(self) -> None:
        try:
            config = self.gather_config()
            save_config(CONFIG_PATH, config)
            self.status_var.set("配置保存成功")
            self._append_log("[GUI] 配置保存成功")
        except ValueError:
            messagebox.showerror("输入错误", "请检查数字字段（端口/直播间号/UID/保留天数/槽位）")
        except OSError as exc:
            messagebox.showerror("保存失败", str(exc))

    def start_server(self) -> None:
        if self.server_proc and self.server_proc.poll() is None:
            self.status_var.set("后端已经在运行")
            self._append_log("[GUI] 后端已经在运行")
            return

        try:
            self.save_to_file()
            if getattr(sys, "frozen", False):
                command = [sys.executable, "--backend"]
            else:
                command = [sys.executable, str(SERVER_PATH)]
            self.server_proc = subprocess.Popen(
                command,
                cwd=str(APP_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
            )
            self.status_var.set("后端已启动")
            self._append_log(f"[GUI] 后端已启动：{' '.join(command)}")
            self._bind_process_logs()
            self._schedule_log_pump()
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("启动失败", str(exc))

    def stop_server(self) -> None:
        if not self.server_proc or self.server_proc.poll() is not None:
            self.status_var.set("后端未运行")
            return

        self.server_proc.terminate()
        try:
            self.server_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.server_proc.kill()
        self.status_var.set("后端已停止")
        self._append_log("[GUI] 后端已停止")

    def open_web(self) -> None:
        port = self.port_var.get().strip() or "9816"
        confirmed = messagebox.askokcancel(
            "免费软件提示",
            "该软件是免费软件，如果收费购买（亲手帮安装除外），请立刻退款！\n\n点击“确定”后打开后台管理页面。",
        )
        if not confirmed:
            self.status_var.set("已取消打开网页")
            return
        webbrowser.open(f"http://127.0.0.1:{port}/config")

    def on_close(self) -> None:
        if self.server_proc and self.server_proc.poll() is None:
            self.stop_server()
        self.root.destroy()


def main() -> None:
    if "--backend" in sys.argv[1:]:
        from backend import server as backend_server

        config = backend_server.load_config()
        host = str(config.get("server", {}).get("host", "0.0.0.0"))
        port = int(config.get("server", {}).get("port", 9816))
        backend_server.run_server(host=host, port=port)
        return

    root = tk.Tk()
    app = ControlPanelApp(root)
    root.minsize(760, 560)
    root.mainloop()


if __name__ == "__main__":
    main()
