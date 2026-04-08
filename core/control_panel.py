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

REPO_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = Path(__file__).resolve().parent  # bilipdj/core/
if str(REPO_DIR) not in sys.path:
    sys.path.insert(0, str(REPO_DIR))

# 打包提示：显式导入 core.server，确保 PyInstaller 能追踪到其依赖（如 http.server）。
import core.server as _backend_server_hint  # noqa: F401

BUNDLE_DIR = Path(getattr(sys, "_MEIPASS", REPO_DIR))
APP_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else REPO_DIR
_YAML_DIR = APP_DIR if getattr(sys, "frozen", False) else CORE_DIR
CONFIG_PATH = _YAML_DIR / "config.yaml"
QUANXIAN_PATH = _YAML_DIR / "quanxian.yaml"
KAIGUAN_PATH = _YAML_DIR / "kaiguan.yaml"
SERVER_PATH = BUNDLE_DIR / "core" / "server.py"
APP_VERSION = "0.4.0"
LOG_LEVEL_OPTIONS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
_LOG_LEVEL_RE = re.compile(r"\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]")
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


def _query_gpu() -> str:
    """尝试通过 nvidia-smi 或 GPUtil 读取 GPU 占用率，均失败则返回 N/A。"""
    try:
        result = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=utilization.gpu,memory.used,memory.total",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=3,
        )
        if result.returncode == 0 and result.stdout.strip():
            parts = [p.strip() for p in result.stdout.strip().split(",")]
            if len(parts) >= 3:
                used_gb = int(parts[1]) / 1024
                total_gb = int(parts[2]) / 1024
                return f"{parts[0]}%  显存 {used_gb:.1f} GB / {total_gb:.1f} GB"
    except Exception:  # noqa: BLE001
        pass
    try:
        import GPUtil  # type: ignore[import-untyped]
        gpus = GPUtil.getGPUs()
        if gpus:
            g = gpus[0]
            return (
                f"{g.load * 100:.1f}%  显存"
                f" {g.memoryUsed / 1024:.1f} GB / {g.memoryTotal / 1024:.1f} GB"
            )
    except Exception:  # noqa: BLE001
        pass
    return "N/A（未检测到 GPU 或驱动不支持）"


def load_backend_server_module() -> Any:
    global _BACKEND_SERVER_MODULE
    if _BACKEND_SERVER_MODULE is not None:
        return _BACKEND_SERVER_MODULE

    module_names = (
        "bilipdj.core.server",
        "core.server",
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
        self.roomid_var = tk.StringVar(value="3049445")
        self.uid_var = tk.StringVar(value="0")
        self.cookie_var = tk.StringVar(value="")
        self.log_level_var = tk.StringVar(value="INFO")
        self.retention_days_var = tk.StringVar(value="15")
        self.queue_enabled_var = tk.BooleanVar(value=True)
        self.queue_slots_var = tk.StringVar(value="3")
        self.queue_slot_choice_var = tk.IntVar(value=3)
        self.auto_start_var = tk.BooleanVar(value=False)
        self.ws_light_var = tk.StringVar(value="●")
        self.ws_text_var = tk.StringVar(value="直播间链接状态：未连接")

        self._build_ui()
        self.load_from_file()
        if self.auto_start_var.get():
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
        ttk.Button(btn_bar, text="配置页", command=self.open_config).grid(row=0, column=2, padx=(0, 6))
        ttk.Button(btn_bar, text="排队展示页", command=self.open_web).grid(row=0, column=3)

        ttk.Label(top, textvariable=self.status_var, foreground="#0b5").pack(side="left", padx=(16, 0))

        # --- 标签页 ---
        notebook = ttk.Notebook(main)
        notebook.grid(row=1, column=0, sticky="nsew")

        # Tab 0: 日志
        log_tab = ttk.Frame(notebook, padding=8)
        notebook.add(log_tab, text="日志")
        self._build_log_tab(log_tab)

        # Tab 1: 当前排队
        queue_tab = ttk.Frame(notebook, padding=8)
        notebook.add(queue_tab, text="当前排队")
        self._build_queue_tab(queue_tab)

        # Tab 2: 设置
        settings_tab = ttk.Frame(notebook, padding=8)
        notebook.add(settings_tab, text="设置")
        self._build_settings_tab(settings_tab)

        # Tab 3: 权限
        quanxian_tab = ttk.Frame(notebook, padding=8)
        notebook.add(quanxian_tab, text="权限")
        self._build_quanxian_tab(quanxian_tab)

        # Tab 4: 开关
        kaiguan_tab = ttk.Frame(notebook, padding=8)
        notebook.add(kaiguan_tab, text="开关")
        self._build_kaiguan_tab(kaiguan_tab)

        # Tab 5: 性能
        perf_tab = ttk.Frame(notebook, padding=8)
        notebook.add(perf_tab, text="性能")
        self._build_perf_tab(perf_tab)

        # Tab 6: 关于
        about_tab = ttk.Frame(notebook, padding=8)
        notebook.add(about_tab, text="关于")
        self._build_about_tab(about_tab)

    def _build_queue_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        # 顶部：排队人数 + 手动刷新按钮
        top_bar = ttk.Frame(frame)
        top_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.queue_count_var = tk.StringVar(value="当前排队：0 人")
        ttk.Label(top_bar, textvariable=self.queue_count_var, font=("Arial", 11, "bold")).pack(side="left")
        ttk.Button(top_bar, text="刷新", command=lambda: threading.Thread(target=self._refresh_queue_list, daemon=True).start()).pack(side="right")

        # 排队列表（Treeview + 滚动条）
        tree_frame = ttk.Frame(frame)
        tree_frame.grid(row=1, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = ("seq", "name", "content")
        self.queue_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self.queue_tree.heading("seq", text="#")
        self.queue_tree.heading("name", text="用户名")
        self.queue_tree.heading("content", text="排队内容")
        self.queue_tree.column("seq", width=40, minwidth=30, anchor="center", stretch=False)
        self.queue_tree.column("name", width=160, minwidth=80, anchor="w")
        self.queue_tree.column("content", width=300, minwidth=100, anchor="w")

        y_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.queue_tree.yview)
        self.queue_tree.configure(yscrollcommand=y_scroll.set)

        self.queue_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")

        # 启动定时刷新
        self.root.after(2000, self._auto_refresh_queue)

    @staticmethod
    def _parse_queue_item(item: str) -> tuple[str, str]:
        """将排队条目解析为 (用户名, 排队内容)。

        格式举例：
          - "用户名"              -> ("用户名", "")
          - "用户名 角色名"       -> ("用户名", "角色名")
          - "G|用户名 角色名"     -> ("用户名", "[官服] 角色名")
          - "B|用户名 角色名"     -> ("用户名", "[B服] 角色名")
          - "S|用户名 角色名"     -> ("用户名", "[超级] 角色名")
          - "M|用户名 角色名"     -> ("用户名", "[米服] 角色名")
        """
        PREFIX_MAP = {"G": "官服", "B": "B服", "S": "超级", "M": "米服"}
        prefix_label = ""
        text = item.strip()
        if len(text) >= 3 and text[1] == "|" and text[0] in PREFIX_MAP:
            prefix_label = f"[{PREFIX_MAP[text[0]]}] "
            text = text[2:]

        parts = text.split(" ", 1)
        name = parts[0].strip()
        content = prefix_label + (parts[1].strip() if len(parts) > 1 else "")
        return name, content

    def _backend_is_running(self) -> bool:
        return bool(self.server_proc and self.server_proc.poll() is None)

    def _refresh_queue_list(self) -> None:
        """在后台线程中拉取队列数据，UI 更新回到主线程。"""
        port = self.port_var.get().strip() or "9816"
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/queue/state", timeout=2) as r:
                data = json.loads(r.read().decode("utf-8", errors="replace"))
            items: list[str] = data.get("queue", [])
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
            return  # 后端未运行，保持上一次显示

        self.root.after(0, lambda: self._update_queue_ui(items))

    def _update_queue_ui(self, items: list[str]) -> None:
        for child in self.queue_tree.get_children():
            self.queue_tree.delete(child)
        for idx, raw_item in enumerate(items, start=1):
            name, content = self._parse_queue_item(raw_item)
            self.queue_tree.insert("", "end", values=(idx, name, content))
        self.queue_count_var.set(f"当前排队：{len(items)} 人")

    def _auto_refresh_queue(self) -> None:
        if self._backend_is_running():
            threading.Thread(target=self._refresh_queue_list, daemon=True).start()
        self.root.after(3000, self._auto_refresh_queue)

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
        _sys_font = ("Microsoft YaHei UI", 9) if sys.platform == "win32" else ("PingFang SC", 11) if sys.platform == "darwin" else ("Sans", 10)
        self.log_text = tk.Text(log_frame, height=18, wrap="word", state="disabled", font=_sys_font)
        self.log_text.tag_configure("warn", foreground="#c00")
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

        ttk.Checkbutton(frame, text="启动时自动运行后端", variable=self.auto_start_var).grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        btn_bar = ttk.Frame(frame)
        btn_bar.grid(row=row, column=0, columnspan=2, sticky="w", pady=(10, 4))
        ttk.Button(btn_bar, text="保存配置", command=self.save_to_file).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(btn_bar, text="刷新配置", command=self.load_from_file).grid(row=0, column=1)

    def _build_perf_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(1, weight=1)
        self._perf_vars: dict[str, tk.StringVar] = {}
        rows = [
            ("cpu",  "CPU 使用率"),
            ("mem",  "本进程内存"),
            ("sysmem", "系统内存"),
            ("disk", "磁盘"),
            ("gpu",  "GPU"),
        ]
        for row_idx, (key, label) in enumerate(rows):
            ttk.Label(frame, text=label, width=10, anchor="e").grid(
                row=row_idx, column=0, sticky="e", padx=(0, 12), pady=8
            )
            var = tk.StringVar(value="读取中…")
            self._perf_vars[key] = var
            ttk.Label(frame, textvariable=var, anchor="w").grid(
                row=row_idx, column=1, sticky="w"
            )
        self.root.after(500, self._refresh_perf)

    def _refresh_perf(self) -> None:
        threading.Thread(target=self._fetch_perf, daemon=True).start()
        self.root.after(2000, self._refresh_perf)

    def _fetch_perf(self) -> None:
        try:
            import psutil  # type: ignore[import-untyped]
            cpu_text = f"{psutil.cpu_percent(interval=0.3):.1f}%"
            proc = psutil.Process()
            proc_mem = proc.memory_info().rss
            mem_text = f"{proc_mem / 1024**2:.1f} MB"
            sys_mem = psutil.virtual_memory()
            sysmem_text = (
                f"{sys_mem.used / 1024**3:.1f} GB / {sys_mem.total / 1024**3:.1f} GB"
                f"  ({sys_mem.percent:.1f}%)"
            )
            disk = psutil.disk_usage(str(APP_DIR))
            disk_text = (
                f"{disk.used / 1024**3:.1f} GB / {disk.total / 1024**3:.1f} GB"
                f"  ({disk.percent:.1f}%)"
            )
        except ImportError:
            cpu_text = mem_text = sysmem_text = disk_text = "需安装 psutil"
        except Exception as exc:  # noqa: BLE001
            cpu_text = mem_text = sysmem_text = disk_text = f"读取失败: {exc}"

        gpu_text = _query_gpu()

        self.root.after(
            0,
            lambda: (
                self._perf_vars["cpu"].set(cpu_text),
                self._perf_vars["mem"].set(mem_text),
                self._perf_vars["sysmem"].set(sysmem_text),
                self._perf_vars["disk"].set(disk_text),
                self._perf_vars["gpu"].set(gpu_text),
            ),
        )

    def _build_about_tab(self, frame: ttk.Frame) -> None:
        ttk.Label(frame, text="Danmuji 弹幕排队控制台", font=("Microsoft YaHei UI", 15, "bold") if sys.platform == "win32" else ("", 15, "bold")).pack(pady=(20, 6))
        ttk.Label(frame, text=f"版本：v{APP_VERSION}").pack()
        ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=12)
        ttk.Label(frame, text="Bilibili 直播弹幕排队管理工具").pack()
        ttk.Label(frame, text="排队逻辑由 Python 后端统一处理，前端仅负责显示。").pack(pady=(4, 0))
        ttk.Separator(frame, orient="horizontal").pack(fill="x", pady=12)
        for line in [
            "本软件完全免费，源码公开。",
            "若有人向你收费获取此软件（亲手上门帮安装调试除外），请立刻退款并举报！",
            "",
            "【侵权/倒卖责任】",
            "• 民事责任：侵权方须停止侵权、赔偿损失（含维权合理费用）。",
            "• 刑事责任：以营利为目的的侵权行为，违法所得数额较大或",
            "  情节严重的，依《著作权法》第53条及相关司法解释，",
            "  可被追究刑事责任，最高判处3年有期徒刑并处罚金。",
        ]:
            ttk.Label(frame, text=line, foreground="#c00" if line.startswith("若") or line.startswith("【") or line.startswith("•") or line.startswith(" ") else "").pack(anchor="w", padx=20)

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
        active_slot = int(queue_archive.get("active_slot", 1))
        active_slot = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, active_slot))
        self.queue_slot_choice_var.set(active_slot)
        self.queue_slots_var.set(str(MAX_QUEUE_ARCHIVE_SLOTS))
        ui_cfg = config.get("ui", {})
        self.auto_start_var.set(bool(ui_cfg.get("auto_start_backend", False)))
        self.status_var.set("已加载配置")
        self._append_log("[GUI] 已加载配置")

    def refresh_runtime_status(self) -> None:
        if not self._backend_is_running():
            self.ws_light_var.set("🔴")
            self.ws_text_var.set("直播间链接状态：后端未启动")
            self.root.after(2000, self.refresh_runtime_status)
            return

        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}/api/runtime-status"

        def _fetch() -> None:
            try:
                with urllib.request.urlopen(url, timeout=1.5) as resp:
                    payload = json.loads(resp.read().decode("utf-8", errors="replace"))
                active = bool(payload.get("danmu_stream_active"))
                ws_clients = int(payload.get("ws_clients", 0))
                if active:
                    light, text = "🟢", f"直播间链接状态：已连接（WS 客户端 {ws_clients}）"
                else:
                    light, text = "🔴", f"直播间链接状态：等待弹幕流（WS 客户端 {ws_clients}）"
            except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
                light, text = "🔴", "直播间链接状态：后端未响应"
            self.root.after(0, lambda: (self.ws_light_var.set(light), self.ws_text_var.set(text)))
            self.root.after(2000, self.refresh_runtime_status)

        threading.Thread(target=_fetch, daemon=True).start()

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
                "slots": MAX_QUEUE_ARCHIVE_SLOTS,
                "active_slot": min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(self.queue_slot_choice_var.get()))),
            },
            "ui": {
                "auto_start_backend": bool(self.auto_start_var.get()),
            },
        }

    def _append_log(self, message: str, warn: bool = False) -> None:
        self.log_text.configure(state="normal")
        start = self.log_text.index("end-1c")
        self.log_text.insert("end", f"{message}\n")
        if warn:
            self.log_text.tag_add("warn", start, self.log_text.index("end-1c"))
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _enqueue_log(self, message: str) -> None:
        match = _LOG_LEVEL_RE.search(message)
        if match:
            msg_level = _LEVEL_ORDER.get(match.group(1), 0)
            min_level = _LEVEL_ORDER.get(self.log_level_var.get().upper(), 0)
            if msg_level < min_level:
                return
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
            _cflags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            self.server_proc = subprocess.Popen(
                command,
                cwd=str(APP_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=_cflags,
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

    _FREE_NOTICE = (
        "【免费提示】本软件完全免费，源码公开。"
        "若有人向你收费获取此软件（亲手上门帮安装调试除外），请立刻退款并举报！"
        "侵权/倒卖者将承担民事赔偿责任，情节严重者依据《著作权法》可追究刑事责任（最高判处 3 年有期徒刑并处罚金）。"
    )

    def open_config(self) -> None:
        port = self.port_var.get().strip() or "9816"
        self._append_log(self._FREE_NOTICE, warn=True)
        webbrowser.open(f"http://127.0.0.1:{port}/config")

    def open_web(self) -> None:
        port = self.port_var.get().strip() or "9816"
        self._append_log(self._FREE_NOTICE, warn=True)
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
    root.withdraw()
    try:
        root.wm_attributes("-alpha", 0)
    except tk.TclError:
        pass
    app = ControlPanelApp(root)
    root.minsize(760, 560)
    root.update_idletasks()
    root.deiconify()
    try:
        root.wm_attributes("-alpha", 1)
    except tk.TclError:
        pass
    root.mainloop()


if __name__ == "__main__":
    main()
