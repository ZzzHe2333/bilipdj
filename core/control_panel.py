from __future__ import annotations

import importlib
import importlib.util
import json
import queue
import re
import subprocess
import sys
import threading
import tkinter as tk
import urllib.error
import urllib.request
import webbrowser
from pathlib import Path
from tkinter import messagebox, ttk
from typing import Any

REPO_DIR = Path(__file__).resolve().parents[2]
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

# 打包提示：显式导入 core.backend.server，确保 PyInstaller 能追踪到其依赖（如 http.server）。
import core.backend.server as _backend_server_hint  # noqa: F401

BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", REPO_DIR))
APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else REPO_DIR
CONFIG_PATH = APP_DIR / "config.yaml"
QUANXIAN_PATH = APP_DIR / "quanxian.yaml"
KAIGUAN_PATH = APP_DIR / "kaiguan.yaml"
SERVER_PATH = BUNDLE_DIR / "core" / "backend" / "server.py"
APP_VERSION = "0.4.0"
LOG_LEVEL_OPTIONS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
MAX_QUEUE_ARCHIVE_SLOTS = 5
KAIGUAN_LABELS = [
    ("paidui",           "普通排队"),
    ("guanfu_paidui",    "官服排队"),
    ("bfu_paidui",       "B服排队"),
    ("chaoji_paidui",    "超级排队"),
    ("mifu_paidui",      "米服排队"),
    ("quxiao_paidui",    "取消排队"),
    ("xiugai_paidui",    "修改排队内容"),
    ("jianzhang_chadui", "舰长插队"),
    ("fangguan_op",      "允许房管执行管理命令"),
]
DEFAULT_KAIGUAN_GUI = {k: True if i < 7 else False for i, (k, _) in enumerate(KAIGUAN_LABELS)}
SENSITIVE_LOG_PATTERNS = [
    re.compile(r"(?i)\b(cookie|auth_token|SESSDATA|bili_jct|buvid3|DedeUserID(?:__ckMd5)?)\s*[:=]\s*([^\s,;]+)"),
]
_BACKEND_SERVER_MODULE: Any | None = None


def sanitize_log_message(message: str) -> str:
    sanitized = str(message)
    for pattern in SENSITIVE_LOG_PATTERNS:
        sanitized = pattern.sub(lambda match: f"{match.group(1)}=<hidden>", sanitized)
    return sanitized


def load_backend_server_module() -> Any:
    global _BACKEND_SERVER_MODULE
    if _BACKEND_SERVER_MODULE is not None:
        return _BACKEND_SERVER_MODULE

    module_names = (
        "bilipdj.core.backend.server",
        "core.backend.server",
        "backend.server",
    )
    for module_name in module_names:
        try:
            _BACKEND_SERVER_MODULE = importlib.import_module(module_name)
            return _BACKEND_SERVER_MODULE
        except ModuleNotFoundError:
            continue

    spec = importlib.util.spec_from_file_location("pdj_backend_server", SERVER_PATH)
    if spec is None or spec.loader is None:
        raise ModuleNotFoundError(f"Unable to load backend server module from {SERVER_PATH}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _BACKEND_SERVER_MODULE = module
    return module


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


def next_meaningful_line(lines: list[str], start_index: int):
    for idx in range(start_index, len(lines)):
        stripped = lines[idx].strip()
        if stripped and not stripped.startswith("#"):
            return idx, lines[idx]
    return None


def load_simple_yaml(path: Path) -> dict:
    if not path.exists():
        return {}

    root: dict = {}
    stack: list[tuple[int, dict | list]] = [(-1, root)]
    lines = path.read_text(encoding="utf-8").splitlines()

    for index, raw_line in enumerate(lines):
        line = raw_line.rstrip()
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        indent = len(line) - len(line.lstrip(" "))
        while stack and indent <= stack[-1][0]:
            stack.pop()

        current = stack[-1][1] if stack else root
        if stripped.startswith("- "):
            if not isinstance(current, list):
                continue
            item_value = stripped[2:].strip()
            if item_value == "":
                child = {}
                current.append(child)
                stack.append((indent, child))
            else:
                current.append(parse_scalar(item_value))
            continue

        if ":" not in stripped or not isinstance(current, dict):
            continue

        key, value = stripped.split(":", 1)
        key = key.strip()
        value = value.strip()

        if value == "":
            next_line = next_meaningful_line(lines, index + 1)
            if next_line is not None:
                next_raw = next_line[1]
                next_indent = len(next_raw) - len(next_raw.lstrip(" "))
                next_stripped = next_raw.strip()
                child = [] if next_indent > indent and next_stripped.startswith("- ") else {}
            else:
                child = {}
            current[key] = child
            stack.append((indent, child))
        else:
            current[key] = parse_scalar(value)

    return root


def merge_config(defaults: dict, custom: dict) -> dict:
    merged = dict(defaults)
    for key, value in custom.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = merge_config(merged[key], value)
        else:
            merged[key] = value
    return merged


def yaml_quote_string(value) -> str:
    text = str(value).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{text}"'


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
        main.columnconfigure(0, weight=1)
        main.rowconfigure(1, weight=1)

        # --- 顶部：服务器控制按钮和状态 ---
        top = ttk.Frame(main)
        top.grid(row=0, column=0, sticky="ew", pady=(0, 8))

        btn_bar = ttk.Frame(top)
        btn_bar.pack(side="left")
        ttk.Button(btn_bar, text="启动后端", command=self.start_server).grid(row=0, column=0, padx=(0, 6))
        ttk.Button(btn_bar, text="停止后端", command=self.stop_server).grid(row=0, column=1, padx=(0, 6))
        ttk.Button(btn_bar, text="打开Web界面", command=self.open_web).grid(row=0, column=2)

        ttk.Label(top, textvariable=self.status_var, foreground="#0b5").pack(side="left", padx=(16, 0))

        # --- 标签页 ---
        notebook = ttk.Notebook(main)
        notebook.grid(row=1, column=0, sticky="nsew")

        # Tab 0: 日志
        log_tab = ttk.Frame(notebook, padding=8)
        notebook.add(log_tab, text="日志")
        self._build_log_tab(log_tab)

        # Tab 1: 设置
        settings_tab = ttk.Frame(notebook, padding=8)
        notebook.add(settings_tab, text="设置")
        self._build_settings_tab(settings_tab)

        # Tab 2: 权限
        quanxian_tab = ttk.Frame(notebook, padding=8)
        notebook.add(quanxian_tab, text="权限")
        self._build_quanxian_tab(quanxian_tab)

        # Tab 3: 开关
        kaiguan_tab = ttk.Frame(notebook, padding=8)
        notebook.add(kaiguan_tab, text="开关")
        self._build_kaiguan_tab(kaiguan_tab)

        # Tab 4: 关于
        about_tab = ttk.Frame(notebook, padding=8)
        notebook.add(about_tab, text="关于")
        self._build_about_tab(about_tab)

    def _build_log_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        # 连接状态指示
        status_bar = ttk.Frame(frame)
        status_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(
            status_bar,
            textvariable=self.ws_light_var,
            foreground="#0b5",
            font=("Arial", 14, "bold"),
        ).pack(side="left")
        ttk.Label(status_bar, textvariable=self.ws_text_var).pack(side="left", padx=(8, 0))

        # 日志文本
        log_frame = ttk.Frame(frame)
        log_frame.grid(row=1, column=0, sticky="nsew")
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, height=18, wrap="word", state="disabled")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

    def _build_settings_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(1, weight=1)

        row = 0
        for label, var, wide in [
            ("监听地址", self.host_var, False),
            ("监听端口", self.port_var, False),
            ("直播间号", self.roomid_var, False),
            ("UID", self.uid_var, False),
            ("Cookie", self.cookie_var, True),
            ("日志保留天数", self.retention_days_var, False),
        ]:
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=4)
            ttk.Entry(frame, textvariable=var, width=60 if wide else 30).grid(
                row=row, column=1, sticky="ew", pady=4
            )
            row += 1

        ttk.Label(frame, text="日志等级").grid(row=row, column=0, sticky="w", pady=4)
        self.log_level_combo = ttk.Combobox(
            frame,
            textvariable=self.log_level_var,
            values=LOG_LEVEL_OPTIONS,
            width=27,
            state="readonly",
        )
        self.log_level_combo.grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        ttk.Label(frame, text="日志存档槽位").grid(row=row, column=0, sticky="w", pady=4)
        slot_frame = ttk.Frame(frame)
        slot_frame.grid(row=row, column=1, sticky="w", pady=4)
        for slot in range(1, MAX_QUEUE_ARCHIVE_SLOTS + 1):
            ttk.Radiobutton(
                slot_frame,
                text=f"槽位{slot}",
                variable=self.queue_slot_choice_var,
                value=slot,
            ).grid(row=0, column=slot - 1, padx=(0, 8), sticky="w")
        row += 1

        ttk.Checkbutton(frame, text="启用排队存档", variable=self.queue_enabled_var).grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        btn_bar = ttk.Frame(frame)
        btn_bar.grid(row=row, column=0, columnspan=2, sticky="w", pady=(10, 4))
        ttk.Button(btn_bar, text="保存配置", command=self.save_to_file).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(btn_bar, text="刷新配置", command=self.load_from_file).grid(row=0, column=1)

    def _build_about_tab(self, frame: ttk.Frame) -> None:
        ttk.Label(frame, text=f"Danmuji 弹幕排队控制台", font=("Arial", 15, "bold")).pack(pady=(20, 6))
        ttk.Label(frame, text=f"版本：v{APP_VERSION}").pack()
        ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=12)
        ttk.Label(frame, text="Bilibili 直播弹幕排队管理工具").pack()
        ttk.Label(frame, text="排队逻辑由 Python 后端统一处理，前端仅负责显示。").pack(pady=(4, 0))
        ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=12)
        ttk.Label(
            frame,
            text="该软件是免费软件，如果收费购买（亲手帮安装除外），请立刻退款！",
            foreground="#c00",
        ).pack()

    def _build_quanxian_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(0, weight=1)
        self._quanxian_text: dict[str, tk.Text] = {}
        levels = [
            ("super_admin", "最高管理员（可新增/删除管理员，拥有全部权限）"),
            ("admin",       "管理员（拥有除新增/删除管理员以外的所有权限）"),
            ("jianzhang",   "舰长（仅拥有插队命令权限）"),
            ("member",      "成员（普通观众，仅自助排队/取消/修改）"),
        ]
        for row_idx, (key, label) in enumerate(levels):
            ttk.Label(frame, text=label).grid(row=row_idx * 2, column=0, sticky="w", pady=(8, 2))
            container = ttk.Frame(frame)
            container.grid(row=row_idx * 2 + 1, column=0, sticky="ew", pady=(0, 2))
            container.columnconfigure(0, weight=1)
            t = tk.Text(container, height=3, wrap="word")
            t.grid(row=0, column=0, sticky="ew")
            sb = ttk.Scrollbar(container, orient="vertical", command=t.yview)
            sb.grid(row=0, column=1, sticky="ns")
            t.configure(yscrollcommand=sb.set)
            self._quanxian_text[key] = t

        btn_row = len(levels) * 2
        btn_bar = ttk.Frame(frame)
        btn_bar.grid(row=btn_row, column=0, sticky="w", pady=(10, 0))
        ttk.Button(btn_bar, text="保存权限", command=self._save_quanxian).pack(side="left", padx=(0, 8))
        ttk.Button(btn_bar, text="刷新权限", command=self._load_quanxian).pack(side="left")
        self._load_quanxian()

    def _build_kaiguan_tab(self, frame: ttk.Frame) -> None:
        self._kaiguan_vars: dict[str, tk.BooleanVar] = {}
        for row_idx, (key, label) in enumerate(KAIGUAN_LABELS):
            default = DEFAULT_KAIGUAN_GUI.get(key, True)
            var = tk.BooleanVar(value=default)
            self._kaiguan_vars[key] = var
            ttk.Checkbutton(frame, text=label, variable=var).grid(row=row_idx, column=0, sticky="w", pady=2)

        btn_bar = ttk.Frame(frame)
        btn_bar.grid(row=len(KAIGUAN_LABELS), column=0, sticky="w", pady=(12, 0))
        ttk.Button(btn_bar, text="保存开关", command=self._save_kaiguan).pack(side="left", padx=(0, 8))
        ttk.Button(btn_bar, text="刷新开关", command=self._load_kaiguan).pack(side="left")
        self._load_kaiguan()

    def _load_quanxian(self) -> None:
        port = self.port_var.get().strip() or "9816"
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/quanxian", timeout=2) as r:
                data = json.loads(r.read().decode("utf-8", errors="replace"))
            for key, widget in self._quanxian_text.items():
                widget.delete("1.0", "end")
                items = [x for x in data.get(key, []) if x]
                widget.insert("end", "\n".join(items))
        except Exception:
            # 后端未运行时从本地文件读
            raw = load_simple_yaml(QUANXIAN_PATH)
            for key, widget in self._quanxian_text.items():
                widget.delete("1.0", "end")
                items = [x for x in raw.get(key, []) if x]
                widget.insert("end", "\n".join(items))

    def _save_quanxian(self) -> None:
        payload: dict[str, list[str]] = {}
        for key, widget in self._quanxian_text.items():
            names = [line.strip() for line in widget.get("1.0", "end").splitlines() if line.strip()]
            payload[key] = names
        port = self.port_var.get().strip() or "9816"
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/quanxian",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=2):
                pass
            self._append_log("[GUI] 权限配置已保存并生效")
        except Exception:
            # 后端未运行时直接写文件
            self._write_quanxian_local(payload)
            self._append_log("[GUI] 权限配置已保存到本地（后端未运行，下次启动生效）")

    def _write_quanxian_local(self, payload: dict[str, list[str]]) -> None:
        labels = {
            "super_admin": "最高管理员：拥有所有权限，包括新增/删除管理员",
            "admin": "管理员：拥有除新增/删除管理员以外的所有操作权限",
            "jianzhang": "舰长：仅拥有「插队」命令权限",
            "member": "成员：普通观众",
        }
        lines: list[str] = ["# 权限配置\n"]
        for key in ("super_admin", "admin", "jianzhang", "member"):
            lines.append(f"# {labels.get(key, key)}\n{key}:\n")
            for item in payload.get(key, []):
                escaped = str(item).replace('"', '\\"')
                lines.append(f'  - "{escaped}"\n')
            lines.append("\n")
        QUANXIAN_PATH.write_text("".join(lines), encoding="utf-8")

    def _load_kaiguan(self) -> None:
        port = self.port_var.get().strip() or "9816"
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/kaiguan", timeout=2) as r:
                data = json.loads(r.read().decode("utf-8", errors="replace"))
            for key, var in self._kaiguan_vars.items():
                var.set(bool(data.get(key, DEFAULT_KAIGUAN_GUI.get(key, True))))
        except Exception:
            raw = load_simple_yaml(KAIGUAN_PATH)
            for key, var in self._kaiguan_vars.items():
                default = DEFAULT_KAIGUAN_GUI.get(key, True)
                val = raw.get(key, default)
                var.set(bool(val) if isinstance(val, bool) else default)

    def _save_kaiguan(self) -> None:
        payload = {key: var.get() for key, var in self._kaiguan_vars.items()}
        port = self.port_var.get().strip() or "9816"
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"http://127.0.0.1:{port}/api/kaiguan",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=2):
                pass
            self._append_log("[GUI] 功能开关已保存并生效")
        except Exception:
            self._write_kaiguan_local(payload)
            self._append_log("[GUI] 功能开关已保存到本地（后端未运行，下次启动生效）")

    def _write_kaiguan_local(self, payload: dict[str, bool]) -> None:
        comments = {
            "paidui": "普通排队（排队 / 排队 xxx）",
            "guanfu_paidui": "官服排队",
            "bfu_paidui": "B服排队",
            "chaoji_paidui": "超级排队",
            "mifu_paidui": "米服排队",
            "quxiao_paidui": "取消排队",
            "xiugai_paidui": "修改/替换排队内容",
            "jianzhang_chadui": "舰长插队",
            "fangguan_op": "允许B站房管执行管理员命令",
        }
        lines: list[str] = ["# 功能开关（true=启用，false=禁用）\n"]
        for key, _ in KAIGUAN_LABELS:
            value = payload.get(key, DEFAULT_KAIGUAN_GUI.get(key, True))
            value_str = "true" if value else "false"
            lines.append(f"{key}: {value_str}              # {comments.get(key, key)}\n")
        KAIGUAN_PATH.write_text("".join(lines), encoding="utf-8")

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
        self.log_queue.put(sanitize_log_message(message))

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
            while True:
                try:
                    line = stream.readline()
                except UnicodeDecodeError as exc:
                    self._enqueue_log(f"[{tag}] <decode error: {exc}>")
                    continue
                if line == "":
                    break
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
            backend_server = load_backend_server_module()
            config = backend_server._merge_config(  # type: ignore[attr-defined]
                backend_server.load_config(),
                self.gather_config(),
            )
            backend_server.save_config(config)
            self.status_var.set("配置保存成功")
            self._append_log("[GUI] 配置保存成功")
        except ValueError:
            messagebox.showerror("输入错误", "请检查数字字段（端口/直播间号/UID/保留天数/槽位）")
        except OSError as exc:
            messagebox.showerror("保存失败", str(exc))
            return
        self._switch_queue_slot()

    def _switch_queue_slot(self) -> None:
        slot = self.queue_slot_choice_var.get()
        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}/api/queue/switch"
        body = json.dumps({"slot": slot}).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=2) as resp:
                result = json.loads(resp.read().decode("utf-8", errors="replace"))
            size = result.get("size", 0)
            self._append_log(f"[GUI] 已切换到存档槽位 {slot}，队列 {size} 人")
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            self._append_log(f"[GUI] 存档槽位已选择 {slot}（后端未运行，下次启动生效）")

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
                encoding="utf-8",
                errors="replace",
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
        webbrowser.open(f"http://127.0.0.1:{port}/index")

    def on_close(self) -> None:
        if self.server_proc and self.server_proc.poll() is None:
            self.stop_server()
        self.root.destroy()


def main() -> None:
    if "--backend" in sys.argv[1:]:
        backend_server = load_backend_server_module()
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
