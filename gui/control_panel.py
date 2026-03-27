from __future__ import annotations

import subprocess
import sys
import tkinter as tk
import webbrowser
from pathlib import Path
from tkinter import messagebox, ttk

ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config.yaml"
SERVER_PATH = ROOT_DIR / "backend" / "server.py"


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
  # 三个存档位（像游戏存档）
  slots: {int(queue_archive.get('slots', 3))}
"""
    path.write_text(content, encoding="utf-8")


class ControlPanelApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title("Danmuji 控制台")
        self.server_proc: subprocess.Popen[str] | None = None

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

        self._build_ui()
        self.load_from_file()
        self.root.after(200, self.start_server)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self) -> None:
        main = ttk.Frame(self.root, padding=12)
        main.grid(sticky="nsew")
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)

        row = 0
        for label, var in [
            ("Host", self.host_var),
            ("Port", self.port_var),
            ("Room ID", self.roomid_var),
            ("UID", self.uid_var),
            ("Cookie", self.cookie_var),
            ("Log Level", self.log_level_var),
            ("Retention Days", self.retention_days_var),
            ("Queue Slots", self.queue_slots_var),
        ]:
            ttk.Label(main, text=label).grid(row=row, column=0, sticky="w", pady=4)
            if label == "Cookie":
                entry = ttk.Entry(main, textvariable=var, width=60)
            else:
                entry = ttk.Entry(main, textvariable=var, width=30)
            entry.grid(row=row, column=1, sticky="ew", pady=4)
            row += 1

        ttk.Checkbutton(main, text="启用排队存档", variable=self.queue_enabled_var).grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        button_bar = ttk.Frame(main)
        button_bar.grid(row=row, column=0, columnspan=2, sticky="w", pady=(8, 4))
        ttk.Button(button_bar, text="保存配置", command=self.save_to_file).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(button_bar, text="启动后端", command=self.start_server).grid(row=0, column=1, padx=(0, 8))
        ttk.Button(button_bar, text="停止后端", command=self.stop_server).grid(row=0, column=2, padx=(0, 8))
        ttk.Button(button_bar, text="打开Web界面", command=self.open_web).grid(row=0, column=3)

        ttk.Label(main, textvariable=self.status_var, foreground="#0b5").grid(
            row=row + 1, column=0, columnspan=2, sticky="w", pady=(8, 0)
        )
        main.columnconfigure(1, weight=1)

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
        self.queue_slots_var.set(str(queue_archive.get("slots", 3)))
        self.status_var.set("已加载配置")

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
                "slots": int(self.queue_slots_var.get().strip() or 3),
            },
        }

    def save_to_file(self) -> None:
        try:
            config = self.gather_config()
            save_config(CONFIG_PATH, config)
            self.status_var.set("配置保存成功")
        except ValueError:
            messagebox.showerror("输入错误", "请检查数字字段（Port/Room ID/UID/Retention/Slots）")
        except OSError as exc:
            messagebox.showerror("保存失败", str(exc))

    def start_server(self) -> None:
        if self.server_proc and self.server_proc.poll() is None:
            self.status_var.set("后端已经在运行")
            return

        try:
            self.save_to_file()
            command = [sys.executable, str(SERVER_PATH)]
            self.server_proc = subprocess.Popen(
                command,
                cwd=str(ROOT_DIR),
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            self.status_var.set("后端已启动")
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

    def open_web(self) -> None:
        port = self.port_var.get().strip() or "9816"
        webbrowser.open(f"http://127.0.0.1:{port}/")

    def on_close(self) -> None:
        if self.server_proc and self.server_proc.poll() is None:
            self.stop_server()
        self.root.destroy()


def main() -> None:
    root = tk.Tk()
    app = ControlPanelApp(root)
    root.minsize(640, 420)
    root.mainloop()


if __name__ == "__main__":
    main()
