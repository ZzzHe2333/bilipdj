from __future__ import annotations

import importlib
import importlib.util
import json
import os
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

if sys.platform == "win32":
    import ctypes
else:
    ctypes = None

try:
    from PIL import Image, ImageColor, ImageDraw, ImageFont, ImageTk

    PIL_AVAILABLE = True
except Exception:  # noqa: BLE001
    Image = ImageColor = ImageDraw = ImageFont = ImageTk = None
    PIL_AVAILABLE = False

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
OVERLAY_HOST_SCRIPT = BUNDLE_DIR / "core" / "overlay_host.py"
OVERLAY_HOST_EXE_NAME = "paiduijitm.exe"
APP_NAME = "弹幕排队姬"
APP_VERSION = "0.4.0"
LOG_LEVEL_OPTIONS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_LEVEL_ORDER = {"DEBUG": 0, "INFO": 1, "WARNING": 2, "ERROR": 3, "CRITICAL": 4}
_LOG_LEVEL_RE = re.compile(r"\[(DEBUG|INFO|WARNING|ERROR|CRITICAL)\]")
# 匹配后端日志完整时间戳前缀：2026-04-09 12:34:56,789 [INFO] name:
_LOG_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2} (\d{2}:\d{2}:\d{2}),\d+ \[(?:DEBUG|INFO|WARNING|ERROR|CRITICAL)\] [^:]+: (.*)")
# 匹配面板显示格式 "HH:MM:SS 内容"
_PANEL_TS_RE = re.compile(r"^(\d{2}:\d{2}:\d{2}) (.*)", re.DOTALL)
MAX_QUEUE_ARCHIVE_SLOTS = 10
OVERLAY_REFRESH_MS = 1200
OVERLAY_TRANSPARENT_COLOR = "#010101"
OVERLAY_RESIZE_MARGIN = 8
OVERLAY_MIN_WIDTH = 320
OVERLAY_MIN_HEIGHT = 180
DEFAULT_OVERLAY_SETTINGS = {
    "width": 860,
    "height": 420,
    "scale": 100,
}
KAIGUAN_LABELS = [
    ("paidui",           "排队总开关"),
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
    ui_cfg = config.get("ui", {})
    overlay_cfg = ui_cfg.get("overlay_window", {}) if isinstance(ui_cfg, dict) else {}
    logging_cfg = config.get("logging", {})
    queue_archive = config.get("queue_archive", {})
    slots = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(queue_archive.get("slots", 3))))
    active_slot = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(queue_archive.get("active_slot", 1))))
    escaped_cookie = str(api.get("cookie", "")).replace('"', '\\"')
    try:
        overlay_width = max(OVERLAY_MIN_WIDTH, int(overlay_cfg.get("width", DEFAULT_OVERLAY_SETTINGS["width"])))
    except (TypeError, ValueError):
        overlay_width = DEFAULT_OVERLAY_SETTINGS["width"]
    try:
        overlay_height = max(OVERLAY_MIN_HEIGHT, int(overlay_cfg.get("height", DEFAULT_OVERLAY_SETTINGS["height"])))
    except (TypeError, ValueError):
        overlay_height = DEFAULT_OVERLAY_SETTINGS["height"]
    try:
        overlay_scale = max(40, min(250, int(overlay_cfg.get("scale", DEFAULT_OVERLAY_SETTINGS["scale"]))))
    except (TypeError, ValueError):
        overlay_scale = DEFAULT_OVERLAY_SETTINGS["scale"]

    content = f"""# 弹幕排队姬 全局配置
server:
  host: {server.get('host', '0.0.0.0')}
  port: {int(server.get('port', 9816))}

api:
  roomid: {int(api.get('roomid', 0))}
  uid: {int(api.get('uid', 0))}
  cookie: \"{escaped_cookie}\"

# 前端 myjs.js 可覆盖配置（如需扩展可继续加键值）
myjs:

ui:
  auto_start_backend: {'true' if bool(ui_cfg.get('auto_start_backend', False)) else 'false'}
  language: {yaml_quote_string(ui_cfg.get('language', '中文'))}
  overlay_window:
    width: {overlay_width}
    height: {overlay_height}
    scale: {overlay_scale}

logging:
  # 支持 DEBUG / INFO / WARNING / ERROR / CRITICAL
  level: {str(logging_cfg.get('level', 'INFO')).upper()}
  # 每次启动默认清理多少天前日志
  retention_days: {int(logging_cfg.get('retention_days', 15))}

queue_archive:
  enabled: {'true' if bool(queue_archive.get('enabled', True)) else 'false'}
  # 存档位（1~10）
  slots: {slots}
  active_slot: {active_slot}
"""
    path.write_text(content, encoding="utf-8")


class ControlPanelApp:
    def __init__(self, root: tk.Tk) -> None:
        self.root = root
        self.root.title(f"{APP_NAME} 控制台 v{APP_VERSION}")
        self._apply_root_icon()
        self.server_proc: subprocess.Popen[str] | None = None
        self.overlay_proc: subprocess.Popen[str] | None = None
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
        self.retention_days_var = tk.StringVar(value="7")
        self.queue_enabled_var = tk.BooleanVar(value=True)
        self.queue_slot_var = tk.StringVar(value="1")
        self.queue_slot_choice_var = tk.IntVar(value=1)
        self.auto_start_var = tk.BooleanVar(value=False)
        self.language_var = tk.StringVar(value="中文")
        self.overlay_width_var = tk.StringVar(value=str(DEFAULT_OVERLAY_SETTINGS["width"]))
        self.overlay_height_var = tk.StringVar(value=str(DEFAULT_OVERLAY_SETTINGS["height"]))
        self.overlay_scale_var = tk.StringVar(value=str(DEFAULT_OVERLAY_SETTINGS["scale"]))
        self.ws_light_var = tk.StringVar(value="●")
        self.ws_text_var = tk.StringVar(value="直播间链接状态：未连接")

        self._clear_click_time: float = 0.0
        self._blacklist_clear_click_time: float = 0.0
        self._prev_slot: int = self.queue_slot_choice_var.get()
        self._overlay_window: tk.Toplevel | None = None
        self._overlay_canvas: tk.Canvas | None = None
        self._overlay_photo: Any | None = None
        self._overlay_items: list[str] = []
        self._overlay_style: dict[str, Any] = {}
        self._overlay_refresh_running = False
        self._overlay_topmost = True
        self._overlay_drag_origin: tuple[int, int] | None = None
        self._overlay_window_origin: tuple[int, int] | None = None
        self._overlay_resize_mode = ""
        self._overlay_resize_origin: tuple[int, int] | None = None
        self._overlay_resize_geometry: tuple[int, int, int, int] | None = None
        self._overlay_last_size: tuple[int, int] = (0, 0)
        self._overlay_font_cache: dict[tuple[str, int], Any] = {}
        self._overlay_font_path = self._detect_overlay_font_path()

        self._build_ui()
        self.load_from_file()
        self._append_log("[GUI] 初始化完成 — 后端尚未启动，请点击「启动后端」")
        if self.auto_start_var.get():
            self.root.after(200, self.start_server)
        self.root.after(1000, self.refresh_runtime_status)
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _apply_root_icon(self) -> None:
        icon_candidates = (
            CORE_DIR / "256x.ico",
            APP_DIR / "core" / "256x.ico",
            BUNDLE_DIR / "core" / "256x.ico",
            APP_DIR / "256x.ico",
        )
        for icon_path in icon_candidates:
            if not icon_path.exists():
                continue
            try:
                self.root.iconbitmap(str(icon_path))
                return
            except tk.TclError:
                continue

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
        ttk.Button(btn_bar, text="排队展示页", command=self.open_web).grid(row=0, column=3, padx=(0, 6))
        ttk.Button(btn_bar, text="透明弹窗", command=self.open_overlay_window).grid(row=0, column=4)

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

        # Tab 2: 黑名单
        blacklist_tab = ttk.Frame(notebook, padding=8)
        notebook.add(blacklist_tab, text="黑名单")
        self._build_blacklist_tab(blacklist_tab)

        # Tab 3: 设置
        settings_tab = ttk.Frame(notebook, padding=8)
        notebook.add(settings_tab, text="设置")
        self._build_settings_tab(settings_tab)

        # Tab 4: 权限
        quanxian_tab = ttk.Frame(notebook, padding=8)
        notebook.add(quanxian_tab, text="权限")
        self._build_quanxian_tab(quanxian_tab)

        # Tab 5: 开关
        kaiguan_tab = ttk.Frame(notebook, padding=8)
        notebook.add(kaiguan_tab, text="开关")
        self._build_kaiguan_tab(kaiguan_tab)

        # Tab 6: 性能
        perf_tab = ttk.Frame(notebook, padding=8)
        notebook.add(perf_tab, text="性能")
        self._build_perf_tab(perf_tab)

        # Tab 7: 样式设置
        style_tab = ttk.Frame(notebook, padding=8)
        notebook.add(style_tab, text="样式设置")
        self._build_style_tab(style_tab)

        # Tab 8: 关于
        about_tab = ttk.Frame(notebook, padding=8)
        notebook.add(about_tab, text="关于")
        self._build_about_tab(about_tab)

    def _build_queue_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        # 顶部：排队人数 + 状态 + 手动刷新按钮
        top_bar = ttk.Frame(frame)
        top_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.queue_count_var = tk.StringVar(value="当前排队：0 人")
        ttk.Label(top_bar, textvariable=self.queue_count_var, font=("Arial", 11, "bold")).pack(side="left")
        self.queue_status_var = tk.StringVar(value="")
        ttk.Label(top_bar, textvariable=self.queue_status_var, foreground="#0a0", width=28).pack(side="left", padx=(10, 0))
        ttk.Button(top_bar, text="刷新", command=lambda: threading.Thread(target=self._refresh_queue_list, daemon=True).start()).pack(side="right")
        self.queue_slot_combo_top = ttk.Combobox(
            top_bar,
            textvariable=self.queue_slot_var,
            values=[str(slot) for slot in range(1, MAX_QUEUE_ARCHIVE_SLOTS + 1)],
            width=4,
            state="readonly",
        )
        self.queue_slot_combo_top.pack(side="right", padx=(0, 6))
        self.queue_slot_combo_top.bind("<<ComboboxSelected>>", self._on_queue_slot_selected)
        ttk.Label(top_bar, text="存档").pack(side="right", padx=(0, 4))

        # 排队列表（Treeview + 滚动条）
        tree_frame = ttk.Frame(frame)
        tree_frame.grid(row=1, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = ("seq", "item_id", "content")
        self.queue_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self.queue_tree.heading("seq", text="#")
        self.queue_tree.heading("item_id", text="ID")
        self.queue_tree.heading("content", text="排队内容")
        self.queue_tree.column("seq", width=40, minwidth=30, anchor="center", stretch=False)
        self.queue_tree.column("item_id", width=160, minwidth=80, anchor="w")
        self.queue_tree.column("content", width=300, minwidth=100, anchor="w")

        y_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.queue_tree.yview)
        self.queue_tree.configure(yscrollcommand=y_scroll.set)

        self.queue_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")
        self.queue_tree.bind("<Double-1>", self._on_queue_double_click)

        # 操作按钮栏
        op_bar = ttk.Frame(frame)
        op_bar.grid(row=2, column=0, sticky="ew", pady=(6, 0))
        ttk.Button(op_bar, text="删除", command=self._queue_delete).pack(side="left", padx=(0, 4))
        ttk.Button(op_bar, text="上移", command=self._queue_move_up).pack(side="left", padx=(0, 4))
        ttk.Button(op_bar, text="下移", command=self._queue_move_down).pack(side="left", padx=(0, 4))
        ttk.Button(op_bar, text="在下方新增", command=self._queue_insert).pack(side="left", padx=(0, 4))
        ttk.Button(op_bar, text="一键清空", command=self._queue_clear).pack(side="right")

        # 启动定时刷新
        self.root.after(2000, self._auto_refresh_queue)

    @staticmethod
    def _parse_queue_item(item: str) -> tuple[str, str]:
        """将原始排队条目解析为 (id, 内容)。"""
        try:
            backend_server = load_backend_server_module()
            return backend_server.queue_item_to_parts(item)
        except Exception:
            text = str(item or "").strip()
            if not text:
                return "", ""
            parts = text.split(" ", 1)
            item_id = parts[0].strip()
            content = parts[1].strip() if len(parts) > 1 else ""
            return item_id, content

    @staticmethod
    def _queue_entry_to_item(item_id: str, content: str) -> str:
        try:
            backend_server = load_backend_server_module()
            return str(backend_server.queue_parts_to_item(item_id, content) or "").strip()
        except Exception:
            item_id_text = str(item_id or "").strip()
            content_text = str(content or "").strip()
            return f"{item_id_text} {content_text}".rstrip()

    def _queue_entries_to_items(self, entries: list[dict[str, str]]) -> list[str]:
        items: list[str] = []
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            item = self._queue_entry_to_item(entry.get("id", ""), entry.get("content", ""))
            if item:
                items.append(item)
        return items

    def _load_style_data(self) -> dict[str, Any]:
        style = dict(self._DEFAULT_STYLE)
        try:
            backend_server = load_backend_server_module()
            data = backend_server.load_style()
            if isinstance(data, dict):
                style.update(data)
        except Exception:
            pass
        return style

    @staticmethod
    def _detect_overlay_font_path() -> str:
        if sys.platform != "win32":
            return ""
        fonts_dir = Path(os.environ.get("WINDIR", r"C:\Windows")) / "Fonts"
        candidates = (
            "msyhbd.ttc",
            "msyh.ttc",
            "simhei.ttf",
            "simsun.ttc",
            "arial.ttf",
        )
        for candidate in candidates:
            path = fonts_dir / candidate
            if path.exists():
                return str(path)
        return ""

    @staticmethod
    def _sanitize_overlay_dimension(value: Any, default: int, minimum: int) -> int:
        try:
            parsed = int(str(value).strip() or default)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, parsed)

    @staticmethod
    def _sanitize_overlay_scale(value: Any) -> int:
        try:
            parsed = int(str(value).strip() or DEFAULT_OVERLAY_SETTINGS["scale"])
        except (TypeError, ValueError):
            parsed = DEFAULT_OVERLAY_SETTINGS["scale"]
        return max(40, min(250, parsed))

    def _get_overlay_settings(self) -> dict[str, int]:
        return {
            "width": self._sanitize_overlay_dimension(self.overlay_width_var.get(), DEFAULT_OVERLAY_SETTINGS["width"], OVERLAY_MIN_WIDTH),
            "height": self._sanitize_overlay_dimension(self.overlay_height_var.get(), DEFAULT_OVERLAY_SETTINGS["height"], OVERLAY_MIN_HEIGHT),
            "scale": self._sanitize_overlay_scale(self.overlay_scale_var.get()),
        }

    def _set_overlay_settings(self, settings: dict[str, Any] | None) -> dict[str, int]:
        raw = settings if isinstance(settings, dict) else {}
        normalized = {
            "width": self._sanitize_overlay_dimension(raw.get("width", DEFAULT_OVERLAY_SETTINGS["width"]), DEFAULT_OVERLAY_SETTINGS["width"], OVERLAY_MIN_WIDTH),
            "height": self._sanitize_overlay_dimension(raw.get("height", DEFAULT_OVERLAY_SETTINGS["height"]), DEFAULT_OVERLAY_SETTINGS["height"], OVERLAY_MIN_HEIGHT),
            "scale": self._sanitize_overlay_scale(raw.get("scale", DEFAULT_OVERLAY_SETTINGS["scale"])),
        }
        self.overlay_width_var.set(str(normalized["width"]))
        self.overlay_height_var.set(str(normalized["height"]))
        self.overlay_scale_var.set(str(normalized["scale"]))
        return normalized

    def _backend_is_running(self) -> bool:
        return bool(self.server_proc and self.server_proc.poll() is None)

    def _set_queue_slot_selection(self, slot: int) -> int:
        slot = min(MAX_QUEUE_ARCHIVE_SLOTS, max(1, int(slot)))
        self.queue_slot_choice_var.set(slot)
        self.queue_slot_var.set(str(slot))
        return slot

    def _get_selected_slot(self) -> int:
        try:
            slot = int(str(self.queue_slot_var.get()).strip() or self.queue_slot_choice_var.get())
        except (TypeError, ValueError, tk.TclError):
            try:
                slot = int(self.queue_slot_choice_var.get())
            except (TypeError, ValueError, tk.TclError):
                slot = 1
        return self._set_queue_slot_selection(slot)

    def _persist_active_slot_to_config(self, slot: int) -> bool:
        try:
            backend_server = load_backend_server_module()
            config = backend_server.load_config()
            updated = backend_server._merge_config(  # type: ignore[attr-defined]
                config,
                {
                    "queue_archive": {
                        "enabled": bool(config.get("queue_archive", {}).get("enabled", True)),
                        "slots": MAX_QUEUE_ARCHIVE_SLOTS,
                        "active_slot": slot,
                    }
                },
            )
            backend_server.save_config(updated)
            return True
        except Exception as exc:  # noqa: BLE001
            self.root.after(0, lambda: self._append_log(f"[GUI] 写入存档槽位配置失败: {exc}"))
            return False

    def _on_queue_slot_selected(self, _event=None) -> None:
        slot = self._get_selected_slot()
        if slot == self._prev_slot:
            return
        threading.Thread(target=self._apply_queue_slot_selection, args=(slot,), daemon=True).start()

    def _apply_queue_slot_selection(self, slot: int) -> None:
        self._persist_active_slot_to_config(slot)
        self._switch_queue_slot(slot)

    @staticmethod
    def _build_queue_entry(item_id: Any, content: Any, last_operation_at: Any = "") -> dict[str, str]:
        return {
            "id": str(item_id or "").strip(),
            "content": str(content or "").strip(),
            "last_operation_at": str(last_operation_at or "").strip(),
        }

    @staticmethod
    def _queue_entry_timestamp_now() -> str:
        import time as _time

        return _time.strftime("%Y-%m-%d %H:%M:%S", _time.localtime())

    def _normalize_queue_entries(self, items: list[str]) -> list[dict[str, str]]:
        entries: list[dict[str, str]] = []
        for item in items:
            item_id, content = self._parse_queue_item(str(item))
            entries.append(self._build_queue_entry(item_id, content))
        return entries

    def _extract_entries_from_payload(self, payload: Any) -> list[dict[str, str]]:
        if isinstance(payload, dict):
            raw_entries = payload.get("entries")
            if isinstance(raw_entries, list):
                entries: list[dict[str, str]] = []
                for entry in raw_entries:
                    if not isinstance(entry, dict):
                        continue
                    entries.append(
                        self._build_queue_entry(
                            entry.get("id", ""),
                            entry.get("content", ""),
                            entry.get("last_operation_at", ""),
                        )
                    )
                return entries
            raw_queue = payload.get("queue")
            if isinstance(raw_queue, list):
                return self._normalize_queue_entries([str(item) for item in raw_queue])
        return []

    def _active_slot_csv(self) -> "Path | None":
        """返回当前活跃存档槽位的 CSV 路径（可在后端未启动时使用）。"""
        try:
            bs = load_backend_server_module()
            slot = self._get_selected_slot()
            return bs.PD_DIR / f"queue_archive_slot_{slot}.csv"
        except Exception:
            return None

    def _read_queue_entries_from_csv(self) -> list[dict[str, str]]:
        """直接从当前槽位 CSV 读取结构化队列条目。"""
        path = self._active_slot_csv()
        if path is None or not path.exists():
            return []
        try:
            bs = load_backend_server_module()
            entries = bs.read_queue_archive_entries(path)
            return [
                self._build_queue_entry(
                    entry.get("id", ""),
                    entry.get("content", ""),
                    entry.get("last_operation_at", ""),
                )
                for entry in entries
                if isinstance(entry, dict)
            ]
        except Exception:
            return []

    def _write_queue_entries_to_csv(self, entries: list[dict[str, str]]) -> bool:
        """将结构化条目列表写回当前活跃存档槽位的 CSV。"""
        path = self._active_slot_csv()
        if path is None:
            return False
        try:
            bs = load_backend_server_module()
            bs.write_queue_archive_entries(path, entries)
            return True
        except Exception as exc:
            self.root.after(0, lambda: self._append_log(f"[GUI] CSV 写入失败: {exc}"))
            return False

    def _fetch_queue_entries_from_backend(self) -> list[dict[str, str]] | None:
        if not self._backend_is_running():
            return None
        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}/api/queue/state"
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
            return self._extract_entries_from_payload(payload)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
            return None

    def _refresh_queue_list(self) -> None:
        """后端运行时优先读取内存队列，否则读取当前槽位 CSV。"""
        entries = self._fetch_queue_entries_from_backend()
        if entries is None:
            entries = self._read_queue_entries_from_csv()
        self.root.after(0, lambda: self._update_queue_ui(entries))

    def _update_queue_ui(self, entries: list[dict[str, str]]) -> None:
        # 记住当前选中序号，刷新后恢复
        sel = self.queue_tree.selection()
        prev_idx: int | None = None
        if sel:
            try:
                prev_idx = int(self.queue_tree.item(sel[0], "values")[0])
            except (IndexError, ValueError):
                prev_idx = None

        for child in self.queue_tree.get_children():
            self.queue_tree.delete(child)
        iid_map: dict[int, str] = {}
        for idx, entry in enumerate(entries, start=1):
            iid = self.queue_tree.insert(
                "",
                "end",
                values=(idx, str(entry.get("id", "")), str(entry.get("content", ""))),
            )
            iid_map[idx] = iid
        self.queue_count_var.set(f"当前排队：{len(entries)} 人")

        # 恢复选中
        if prev_idx is not None and prev_idx in iid_map:
            self.queue_tree.selection_set(iid_map[prev_idx])
            self.queue_tree.see(iid_map[prev_idx])

    def _auto_refresh_queue(self) -> None:
        threading.Thread(target=self._refresh_queue_list, daemon=True).start()
        self.root.after(3000, self._auto_refresh_queue)

    def _overlay_window_alive(self) -> bool:
        return bool(self._overlay_window and self._overlay_window.winfo_exists())

    def _overlay_default_geometry(self) -> str:
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        settings = self._get_overlay_settings()
        width = min(screen_w - 48, settings["width"])
        height = min(screen_h - 48, settings["height"])
        x = max(24, screen_w - width - 80)
        y = max(24, min(120, screen_h - height - 80))
        return f"{width}x{height}+{x}+{y}"

    def _overlay_process_running(self) -> bool:
        return bool(self.overlay_proc and self.overlay_proc.poll() is None)

    def _build_overlay_command(self) -> list[str]:
        settings = self._get_overlay_settings()
        port = self.port_var.get().strip() or "9816"
        common_args = [
            "--port",
            str(port),
            "--width",
            str(settings["width"]),
            "--height",
            str(settings["height"]),
            "--scale",
            str(settings["scale"]),
        ]
        if not self._overlay_topmost:
            common_args.append("--no-topmost")
        if getattr(sys, "frozen", False):
            overlay_exe = APP_DIR / OVERLAY_HOST_EXE_NAME
            if overlay_exe.exists():
                return [str(overlay_exe), *common_args]
            # fallback: run overlay host in current executable
            return [sys.executable, "--overlay-host", *common_args]
        return [sys.executable, str(OVERLAY_HOST_SCRIPT), *common_args]

    def _stop_overlay_process(self) -> None:
        if not self._overlay_process_running():
            self.overlay_proc = None
            return
        try:
            self.overlay_proc.terminate()
            self.overlay_proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.overlay_proc.kill()
        finally:
            self.overlay_proc = None

    def _restart_overlay_process(self) -> None:
        self._stop_overlay_process()
        self.open_overlay_window()

    def _set_overlay_topmost(self, topmost: bool) -> None:
        self._overlay_topmost = topmost
        if self._overlay_process_running():
            self._restart_overlay_process()
        self._append_log(f"[GUI] 透明窗口已{'置顶' if topmost else '取消置顶'}")

    def open_overlay_window(self) -> None:
        if self._overlay_process_running():
            self._append_log("[GUI] 透明弹窗进程已在运行")
            return

        command = self._build_overlay_command()
        if not command:
            messagebox.showerror("启动失败", "无法构建透明弹窗启动命令")
            return

        try:
            overlay_env = os.environ.copy()
            overlay_env["DANMUJI_OVERLAY_FROM_GUI"] = "1"
            _cflags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
            self.overlay_proc = subprocess.Popen(
                command,
                cwd=str(APP_DIR),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1,
                creationflags=_cflags,
                env=overlay_env,
            )
            if self.overlay_proc.stdout:
                threading.Thread(
                    target=self._read_stream_lines,
                    args=(self.overlay_proc.stdout, "OVERLAY"),
                    daemon=True,
                ).start()
            if self.overlay_proc.stderr:
                threading.Thread(
                    target=self._read_stream_lines,
                    args=(self.overlay_proc.stderr, "OVERLAY-ERR"),
                    daemon=True,
                ).start()
            self._append_log(f"[GUI] 透明弹窗进程已启动：{' '.join(command)}")
            if getattr(sys, "frozen", False) and not (APP_DIR / OVERLAY_HOST_EXE_NAME).exists():
                self._append_log(
                    f"[GUI] 未找到独立进程 {OVERLAY_HOST_EXE_NAME}，当前使用主程序进程承载透明窗"
                )
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("启动失败", str(exc))

    def _close_overlay_window(self) -> None:
        if not self._overlay_window_alive():
            self._overlay_window = None
            self._overlay_canvas = None
            self._overlay_photo = None
            return
        try:
            self._overlay_window.destroy()
        except tk.TclError:
            pass
        self._overlay_window = None
        self._overlay_canvas = None
        self._overlay_photo = None
        self._overlay_refresh_running = False
        self._overlay_resize_mode = ""
        self._overlay_drag_origin = None
        self._overlay_resize_origin = None

    def _apply_overlay_native_window_style(self) -> None:
        if sys.platform != "win32" or ctypes is None or not self._overlay_window_alive():
            return
        try:
            hwnd = self._overlay_window.winfo_id()
            GWL_EXSTYLE = -20
            WS_EX_APPWINDOW = 0x00040000
            WS_EX_TOOLWINDOW = 0x00000080
            SWP_NOMOVE = 0x0002
            SWP_NOSIZE = 0x0001
            SWP_NOZORDER = 0x0004
            SWP_FRAMECHANGED = 0x0020
            exstyle = ctypes.windll.user32.GetWindowLongW(hwnd, GWL_EXSTYLE)
            exstyle = (exstyle | WS_EX_APPWINDOW) & ~WS_EX_TOOLWINDOW
            ctypes.windll.user32.SetWindowLongW(hwnd, GWL_EXSTYLE, exstyle)
            ctypes.windll.user32.SetWindowPos(
                hwnd,
                0,
                0,
                0,
                0,
                0,
                SWP_NOMOVE | SWP_NOSIZE | SWP_NOZORDER | SWP_FRAMECHANGED,
            )
            self._overlay_window.withdraw()
            self._overlay_window.after(10, self._restore_overlay_window)
        except Exception as exc:  # noqa: BLE001
            self._append_log(f"[GUI] 透明弹窗任务栏样式设置失败: {exc}")

    def _restore_overlay_window(self) -> None:
        if not self._overlay_window_alive():
            return
        self._overlay_window.deiconify()
        self._overlay_window.lift()
        try:
            self._overlay_window.wm_attributes("-topmost", self._overlay_topmost)
        except tk.TclError:
            pass

    @staticmethod
    def _overlay_hit_test(x: int, y: int, width: int, height: int) -> str:
        margin = OVERLAY_RESIZE_MARGIN
        left = x <= margin
        right = x >= width - margin
        top = y <= margin
        bottom = y >= height - margin
        if top and left:
            return "nw"
        if top and right:
            return "ne"
        if bottom and left:
            return "sw"
        if bottom and right:
            return "se"
        if left:
            return "w"
        if right:
            return "e"
        if top:
            return "n"
        if bottom:
            return "s"
        return ""

    @staticmethod
    def _overlay_cursor_for_mode(mode: str) -> str:
        return {
            "n": "sb_v_double_arrow",
            "s": "sb_v_double_arrow",
            "e": "sb_h_double_arrow",
            "w": "sb_h_double_arrow",
            "ne": "size_ne_sw",
            "sw": "size_ne_sw",
            "nw": "size_nw_se",
            "se": "size_nw_se",
        }.get(mode, "")

    def _set_overlay_cursor(self, cursor: str) -> None:
        if self._overlay_canvas is None:
            return
        try:
            self._overlay_canvas.configure(cursor=cursor)
        except tk.TclError:
            self._overlay_canvas.configure(cursor="")

    def _on_overlay_canvas_motion(self, event) -> None:
        if not self._overlay_window_alive() or self._overlay_canvas is None:
            return
        if self._overlay_drag_origin or self._overlay_resize_mode:
            return
        mode = self._overlay_hit_test(
            int(event.x),
            int(event.y),
            self._overlay_canvas.winfo_width(),
            self._overlay_canvas.winfo_height(),
        )
        self._set_overlay_cursor(self._overlay_cursor_for_mode(mode))

    def _begin_overlay_interaction(self, event) -> None:
        if not self._overlay_window_alive() or self._overlay_canvas is None:
            return
        mode = self._overlay_hit_test(
            int(event.x),
            int(event.y),
            self._overlay_canvas.winfo_width(),
            self._overlay_canvas.winfo_height(),
        )
        if mode:
            self._overlay_resize_mode = mode
            self._overlay_resize_origin = (event.x_root, event.y_root)
            self._overlay_resize_geometry = (
                self._overlay_window.winfo_x(),
                self._overlay_window.winfo_y(),
                self._overlay_window.winfo_width(),
                self._overlay_window.winfo_height(),
            )
            self._set_overlay_cursor(self._overlay_cursor_for_mode(mode))
            return
        self._overlay_drag_origin = (event.x_root, event.y_root)
        self._overlay_window_origin = (
            self._overlay_window.winfo_x(),
            self._overlay_window.winfo_y(),
        )
        self._set_overlay_cursor("fleur")

    def _perform_overlay_interaction(self, event) -> None:
        if not self._overlay_window_alive():
            return
        if self._overlay_resize_mode and self._overlay_resize_origin and self._overlay_resize_geometry:
            dx = event.x_root - self._overlay_resize_origin[0]
            dy = event.y_root - self._overlay_resize_origin[1]
            x, y, width, height = self._overlay_resize_geometry
            new_x, new_y, new_w, new_h = x, y, width, height
            if "e" in self._overlay_resize_mode:
                new_w = max(OVERLAY_MIN_WIDTH, width + dx)
            if "s" in self._overlay_resize_mode:
                new_h = max(OVERLAY_MIN_HEIGHT, height + dy)
            if "w" in self._overlay_resize_mode:
                new_w = max(OVERLAY_MIN_WIDTH, width - dx)
                new_x = x + (width - new_w)
            if "n" in self._overlay_resize_mode:
                new_h = max(OVERLAY_MIN_HEIGHT, height - dy)
                new_y = y + (height - new_h)
            self._overlay_window.geometry(f"{new_w}x{new_h}+{new_x}+{new_y}")
            return
        if not self._overlay_drag_origin or not self._overlay_window_origin:
            return
        dx = event.x_root - self._overlay_drag_origin[0]
        dy = event.y_root - self._overlay_drag_origin[1]
        x = self._overlay_window_origin[0] + dx
        y = self._overlay_window_origin[1] + dy
        self._overlay_window.geometry(f"+{x}+{y}")

    def _end_overlay_interaction(self, _event) -> None:
        self._overlay_drag_origin = None
        self._overlay_window_origin = None
        self._overlay_resize_mode = ""
        self._overlay_resize_origin = None
        self._overlay_resize_geometry = None
        self._set_overlay_cursor("")

    def _on_overlay_window_configure(self, event) -> None:
        if not self._overlay_window_alive():
            return
        if event.widget is not self._overlay_window:
            return
        current_size = (self._overlay_window.winfo_width(), self._overlay_window.winfo_height())
        if current_size != self._overlay_last_size:
            self._overlay_last_size = current_size
            self.overlay_width_var.set(str(max(OVERLAY_MIN_WIDTH, current_size[0])))
            self.overlay_height_var.set(str(max(OVERLAY_MIN_HEIGHT, current_size[1])))
            self._redraw_overlay()

    def _toggle_overlay_topmost(self) -> None:
        if not self._overlay_window_alive():
            return
        self._overlay_topmost = not self._overlay_topmost
        try:
            self._overlay_window.wm_attributes("-topmost", self._overlay_topmost)
        except tk.TclError:
            pass
        status = "开启" if self._overlay_topmost else "关闭"
        self._append_log(f"[GUI] 透明弹窗置顶已{status}")

    def _refresh_overlay_async(self) -> None:
        if not self._overlay_window_alive():
            self._overlay_refresh_running = False
            return
        if self._overlay_refresh_running:
            return
        self._overlay_refresh_running = True
        threading.Thread(target=self._refresh_overlay_worker, daemon=True).start()

    def _refresh_overlay_worker(self) -> None:
        try:
            entries = self._fetch_queue_entries_from_backend()
            if entries is None:
                entries = self._read_queue_entries_from_csv()
            items = [
                f"{str(entry.get('id', '')).strip()} {str(entry.get('content', '')).strip()}".rstrip()
                for entry in entries
                if isinstance(entry, dict) and str(entry.get("id", "") or entry.get("content", "")).strip()
            ]
            style = self._load_style_data()
        except Exception:
            items = []
            style = dict(self._DEFAULT_STYLE)

        def _apply() -> None:
            if not self._overlay_window_alive():
                self._overlay_refresh_running = False
                return
            changed = items != self._overlay_items or style != self._overlay_style
            self._overlay_items = list(items)
            self._overlay_style = dict(style)
            self._overlay_refresh_running = False
            if changed:
                self._redraw_overlay()
            self.root.after(OVERLAY_REFRESH_MS, self._refresh_overlay_async)

        self.root.after(0, _apply)

    def _overlay_get_font(self, size: int) -> Any:
        size = max(12, int(size))
        cache_key = (self._overlay_font_path, size)
        cached = self._overlay_font_cache.get(cache_key)
        if cached is not None:
            return cached
        try:
            if self._overlay_font_path:
                font = ImageFont.truetype(self._overlay_font_path, size=size)
            else:
                font = ImageFont.load_default()
        except Exception:  # noqa: BLE001
            font = ImageFont.load_default()
        self._overlay_font_cache[cache_key] = font
        return font

    @staticmethod
    def _parse_overlay_color(value: Any, fallback: str) -> tuple[int, int, int, int]:
        text = str(value or "").strip() or fallback
        lowered = text.lower()
        if lowered.startswith("rgba(") and lowered.endswith(")"):
            parts = [part.strip() for part in lowered[5:-1].split(",")]
            if len(parts) == 4:
                try:
                    red = max(0, min(255, int(float(parts[0]))))
                    green = max(0, min(255, int(float(parts[1]))))
                    blue = max(0, min(255, int(float(parts[2]))))
                    alpha_raw = float(parts[3])
                    alpha = int(max(0.0, min(1.0, alpha_raw)) * 255) if alpha_raw <= 1 else int(max(0, min(255, alpha_raw)))
                    return red, green, blue, alpha
                except ValueError:
                    pass
        if lowered.startswith("rgb(") and lowered.endswith(")"):
            parts = [part.strip() for part in lowered[4:-1].split(",")]
            if len(parts) == 3:
                try:
                    return (
                        max(0, min(255, int(float(parts[0])))),
                        max(0, min(255, int(float(parts[1])))),
                        max(0, min(255, int(float(parts[2])))),
                        255,
                    )
                except ValueError:
                    pass
        try:
            red, green, blue = ImageColor.getrgb(text)
        except Exception:  # noqa: BLE001
            red, green, blue = ImageColor.getrgb(fallback)
        return red, green, blue, 255

    @staticmethod
    def _make_overlay_gradient(size: tuple[int, int], start: tuple[int, int, int, int], end: tuple[int, int, int, int], horizontal: bool) -> Any:
        width, height = size
        gradient = Image.new("RGBA", (max(1, width), max(1, height)), start)
        draw = ImageDraw.Draw(gradient)
        span = (width - 1) if horizontal else (height - 1)
        span = max(1, span)
        for offset in range(span + 1):
            ratio = offset / span
            color = tuple(
                int(start[idx] + (end[idx] - start[idx]) * ratio)
                for idx in range(4)
            )
            if horizontal:
                draw.line((offset, 0, offset, height), fill=color)
            else:
                draw.line((0, offset, width, offset), fill=color)
        return gradient

    def _wrap_overlay_text(self, text: str, font: Any, max_width: int) -> str:
        content = str(text or "").strip()
        if not content or max_width <= 24:
            return content
        probe = Image.new("RGBA", (4, 4), (0, 0, 0, 0))
        draw = ImageDraw.Draw(probe)
        lines: list[str] = []
        current = ""
        for char in content:
            candidate = f"{current}{char}"
            try:
                width = int(draw.textlength(candidate, font=font))
            except Exception:  # noqa: BLE001
                width = len(candidate) * 12
            if current and width > max_width:
                lines.append(current)
                current = char
            else:
                current = candidate
        if current:
            lines.append(current)
        return "\n".join(lines)

    def _draw_overlay_gradient_text(
        self,
        image: Any,
        text: str,
        x: int,
        y: int,
        font: Any,
        start_color: tuple[int, int, int, int],
        end_color: tuple[int, int, int, int],
        stroke_color: tuple[int, int, int, int],
        *,
        stroke_width: int,
        horizontal: bool,
        spacing: int = 4,
    ) -> tuple[int, int]:
        if not text:
            return 0, 0
        probe = Image.new("RGBA", (4, 4), (0, 0, 0, 0))
        probe_draw = ImageDraw.Draw(probe)
        bbox = probe_draw.multiline_textbbox(
            (0, 0),
            text,
            font=font,
            spacing=spacing,
            stroke_width=stroke_width,
        )
        width = max(1, bbox[2] - bbox[0])
        height = max(1, bbox[3] - bbox[1])
        pad = stroke_width + 4
        layer_size = (width + pad * 2, height + pad * 2)
        text_pos = (pad - bbox[0], pad - bbox[1])

        outline = Image.new("RGBA", layer_size, (0, 0, 0, 0))
        outline_draw = ImageDraw.Draw(outline)
        outline_draw.multiline_text(
            text_pos,
            text,
            font=font,
            fill=stroke_color,
            spacing=spacing,
            stroke_width=stroke_width,
            stroke_fill=stroke_color,
        )

        fill_mask = Image.new("L", layer_size, 0)
        fill_draw = ImageDraw.Draw(fill_mask)
        fill_draw.multiline_text(
            text_pos,
            text,
            font=font,
            fill=255,
            spacing=spacing,
        )
        gradient = self._make_overlay_gradient(layer_size, start_color, end_color, horizontal)
        fill_layer = Image.new("RGBA", layer_size, (0, 0, 0, 0))
        fill_layer.paste(gradient, (0, 0), fill_mask)
        outline.alpha_composite(fill_layer)
        image.alpha_composite(outline, (int(x), int(y)))
        return width + pad * 2, height + pad * 2

    def _redraw_overlay(self) -> None:
        if not self._overlay_window_alive() or self._overlay_canvas is None or not PIL_AVAILABLE:
            return
        canvas = self._overlay_canvas
        width = max(1, canvas.winfo_width())
        height = max(1, canvas.winfo_height())
        if width <= 1 or height <= 1:
            return

        style = dict(self._DEFAULT_STYLE)
        style.update(self._overlay_style)
        overlay_settings = self._get_overlay_settings()
        try:
            queue_font_size = int(str(style.get("queue_font_size", 50)).strip() or 50)
        except ValueError:
            queue_font_size = 50
        queue_font_size = max(14, int(queue_font_size * overlay_settings["scale"] / 100))

        grad_start = self._parse_overlay_color(style.get("text_grad_start", "#f7f7f7"), "#f7f7f7")
        grad_end = self._parse_overlay_color(style.get("text_grad_end", "rgba(255,255,255,0.6)"), "rgba(255,255,255,0.6)")
        stroke = self._parse_overlay_color(style.get("text_stroke_color", "#000000"), "#000000")
        border_base = self._parse_overlay_color(style.get("text_color", "#eaf6ff"), "#eaf6ff")
        border_color = (border_base[0], border_base[1], border_base[2], 110)

        image = Image.new("RGBA", (width, height), (0, 0, 0, 0))
        border_px = 1
        border_draw = ImageDraw.Draw(image)
        for offset in range(border_px):
            border_draw.rectangle(
                (offset, offset, width - 1 - offset, height - 1 - offset),
                outline=border_color,
            )

        queue_font = self._overlay_get_font(queue_font_size)
        queue_x = 14
        queue_y = 12
        max_text_width = max(80, width - queue_x - 16)
        line_gap = max(2, int(queue_font_size * 0.16))
        stroke_width = max(2, int(queue_font_size * 0.05))
        for item in self._overlay_items:
            text = str(item or "").strip()
            if not text:
                continue
            wrapped = self._wrap_overlay_text(text, queue_font, max_text_width)
            _text_width, text_height = self._draw_overlay_gradient_text(
                image,
                wrapped,
                queue_x,
                queue_y,
                queue_font,
                grad_start,
                grad_end,
                stroke,
                stroke_width=stroke_width,
                horizontal=True,
            )
            queue_y += text_height + line_gap
            if queue_y >= height - queue_font_size:
                break

        self._overlay_photo = ImageTk.PhotoImage(image)
        canvas.delete("all")
        canvas.create_image(0, 0, anchor="nw", image=self._overlay_photo)

    def _build_blacklist_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

        top_bar = ttk.Frame(frame)
        top_bar.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        self.blacklist_count_var = tk.StringVar(value="黑名单：0 人")
        ttk.Label(top_bar, textvariable=self.blacklist_count_var, font=("Arial", 11, "bold")).pack(side="left")
        self.blacklist_status_var = tk.StringVar(value="")
        ttk.Label(top_bar, textvariable=self.blacklist_status_var, foreground="#0a0", width=28).pack(side="left", padx=(10, 0))
        ttk.Button(top_bar, text="刷新", command=lambda: threading.Thread(target=self._refresh_blacklist_list, daemon=True).start()).pack(side="right")

        tree_frame = ttk.Frame(frame)
        tree_frame.grid(row=1, column=0, sticky="nsew")
        tree_frame.columnconfigure(0, weight=1)
        tree_frame.rowconfigure(0, weight=1)

        columns = ("seq", "name")
        self.blacklist_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", selectmode="browse")
        self.blacklist_tree.heading("seq", text="#")
        self.blacklist_tree.heading("name", text="用户名")
        self.blacklist_tree.column("seq", width=40, minwidth=30, anchor="center", stretch=False)
        self.blacklist_tree.column("name", width=420, minwidth=120, anchor="w")

        y_scroll = ttk.Scrollbar(tree_frame, orient="vertical", command=self.blacklist_tree.yview)
        self.blacklist_tree.configure(yscrollcommand=y_scroll.set)
        self.blacklist_tree.grid(row=0, column=0, sticky="nsew")
        y_scroll.grid(row=0, column=1, sticky="ns")

        op_bar = ttk.Frame(frame)
        op_bar.grid(row=2, column=0, sticky="ew", pady=(6, 0))
        ttk.Button(op_bar, text="新增", command=self._blacklist_add).pack(side="left", padx=(0, 4))
        ttk.Button(op_bar, text="删除", command=self._blacklist_delete).pack(side="left", padx=(0, 4))
        ttk.Button(op_bar, text="一键清空", command=self._blacklist_clear).pack(side="right")

        self.root.after(2200, self._auto_refresh_blacklist)

    @staticmethod
    def _build_blacklist_entry(name: Any) -> dict[str, str]:
        return {"id": str(name or "").strip(), "content": ""}

    def _normalize_blacklist_entries(self, values: list[Any]) -> list[dict[str, str]]:
        entries: list[dict[str, str]] = []
        seen: set[str] = set()
        for value in values:
            if isinstance(value, dict):
                name = str(value.get("id", "") or value.get("content", "")).strip()
            else:
                name = str(value or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            entries.append(self._build_blacklist_entry(name))
        return entries

    def _extract_blacklist_entries_from_payload(self, payload: Any) -> list[dict[str, str]]:
        if isinstance(payload, dict):
            raw_entries = payload.get("entries")
            if isinstance(raw_entries, list):
                return self._normalize_blacklist_entries(raw_entries)
        if isinstance(payload, list):
            return self._normalize_blacklist_entries(payload)
        return []

    def _blacklist_csv_path(self) -> "Path | None":
        try:
            bs = load_backend_server_module()
            return getattr(bs, "BLACKLIST_PATH", APP_DIR / "core" / "cd" / "blacklist.csv")
        except Exception:
            return APP_DIR / "core" / "cd" / "blacklist.csv"

    def _read_blacklist_entries_from_csv(self) -> list[dict[str, str]]:
        path = self._blacklist_csv_path()
        if path is None or not path.exists():
            try:
                bs = load_backend_server_module()
                return self._normalize_blacklist_entries(bs.load_quanxian().get("blacklist", []))
            except Exception:
                return []
        try:
            bs = load_backend_server_module()
            entries = bs.read_blacklist_entries(path)
            return self._normalize_blacklist_entries(entries)
        except Exception:
            return []

    def _write_blacklist_entries_to_config(self, entries: list[dict[str, str]]) -> bool:
        try:
            bs = load_backend_server_module()
            quanxian = bs.load_quanxian()
            quanxian["blacklist"] = [str(entry.get("id", "")).strip() for entry in entries if str(entry.get("id", "")).strip()]
            bs.save_quanxian(quanxian)
            return True
        except Exception as exc:
            self.root.after(0, lambda: self._append_log(f"[GUI] 黑名单保存失败: {exc}"))
            return False

    def _fetch_blacklist_entries_from_backend(self) -> list[dict[str, str]] | None:
        if not self._backend_is_running():
            return None
        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}/api/blacklist/state"
        try:
            with urllib.request.urlopen(url, timeout=2) as resp:
                payload = json.loads(resp.read().decode("utf-8", errors="replace"))
            return self._extract_blacklist_entries_from_payload(payload)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError):
            return None

    def _refresh_blacklist_list(self) -> None:
        entries = self._fetch_blacklist_entries_from_backend()
        if entries is None:
            entries = self._read_blacklist_entries_from_csv()
        self.root.after(0, lambda: self._update_blacklist_ui(entries))

    def _update_blacklist_ui(self, entries: list[dict[str, str]]) -> None:
        sel = self.blacklist_tree.selection()
        prev_idx: int | None = None
        if sel:
            try:
                prev_idx = int(self.blacklist_tree.item(sel[0], "values")[0])
            except (IndexError, ValueError):
                prev_idx = None

        for child in self.blacklist_tree.get_children():
            self.blacklist_tree.delete(child)
        iid_map: dict[int, str] = {}
        for idx, entry in enumerate(entries, start=1):
            iid = self.blacklist_tree.insert("", "end", values=(idx, str(entry.get("id", ""))))
            iid_map[idx] = iid
        self.blacklist_count_var.set(f"黑名单：{len(entries)} 人")

        if prev_idx is not None and prev_idx in iid_map:
            self.blacklist_tree.selection_set(iid_map[prev_idx])
            self.blacklist_tree.see(iid_map[prev_idx])

    def _auto_refresh_blacklist(self) -> None:
        threading.Thread(target=self._refresh_blacklist_list, daemon=True).start()
        self.root.after(4000, self._auto_refresh_blacklist)

    def _get_selected_blacklist_index(self) -> int | None:
        sel = self.blacklist_tree.selection()
        if not sel:
            return None
        values = self.blacklist_tree.item(sel[0], "values")
        try:
            return int(values[0])
        except (IndexError, ValueError):
            return None

    def _blacklist_backend_op(self, path: str, payload: dict[str, Any], status_msg: str = "") -> bool:
        if not self._backend_is_running():
            return False
        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}{path}"
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=2) as resp:
                result = json.loads(resp.read().decode("utf-8", errors="replace"))
            entries = self._extract_blacklist_entries_from_payload(result)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            self.root.after(0, lambda: self._append_log(f"[GUI] 黑名单操作失败: {exc}"))
            return False

        import time as _time
        ts = _time.strftime("%H:%M:%S")
        msg = f"{ts} {status_msg}" if status_msg else ts
        self.root.after(0, lambda: (self._update_blacklist_ui(entries), self.blacklist_status_var.set(msg)))
        return True

    def _blacklist_local_op(self, op, status_msg: str = "") -> None:
        entries = self._read_blacklist_entries_from_csv()
        new_entries = op([dict(entry) for entry in entries])
        if new_entries is None:
            new_entries = entries
        new_entries = self._normalize_blacklist_entries(new_entries)
        if not self._write_blacklist_entries_to_config(new_entries):
            return
        final_entries = self._read_blacklist_entries_from_csv()
        import time as _time
        ts = _time.strftime("%H:%M:%S")
        msg = f"{ts} {status_msg}" if status_msg else ts
        self.root.after(0, lambda: (self._update_blacklist_ui(final_entries), self.blacklist_status_var.set(msg)))

    def _blacklist_add(self) -> None:
        from tkinter import simpledialog
        name = simpledialog.askstring("新增黑名单", "请输入要加入黑名单的用户名：", parent=self.root)
        if not name or not name.strip():
            return
        target = name.strip()
        if self._backend_is_running():
            threading.Thread(
                target=self._blacklist_backend_op,
                args=("/api/blacklist/add", {"name": target}, f"已加入黑名单：{target}"),
                daemon=True,
            ).start()
            return

        def op(entries):
            entries.append(self._build_blacklist_entry(target))
            return entries

        threading.Thread(target=self._blacklist_local_op, args=(op, f"已加入黑名单：{target}"), daemon=True).start()

    def _blacklist_delete(self) -> None:
        idx = self._get_selected_blacklist_index()
        if idx is None:
            return
        if self._backend_is_running():
            threading.Thread(
                target=self._blacklist_backend_op,
                args=("/api/blacklist/delete", {"index": idx}, f"已删除第{idx}个黑名单用户"),
                daemon=True,
            ).start()
            return

        def op(entries):
            if 1 <= idx <= len(entries):
                entries.pop(idx - 1)
            return entries

        threading.Thread(target=self._blacklist_local_op, args=(op, f"已删除第{idx}个黑名单用户"), daemon=True).start()

    def _blacklist_clear(self) -> None:
        import time
        now = time.time()
        if self._blacklist_clear_click_time > 0 and now - self._blacklist_clear_click_time <= 5.0:
            self._blacklist_clear_click_time = 0.0
            if self._backend_is_running():
                threading.Thread(
                    target=self._blacklist_backend_op,
                    args=("/api/blacklist/clear", {}, "黑名单已清空"),
                    daemon=True,
                ).start()
            else:
                threading.Thread(target=self._blacklist_local_op, args=(lambda _entries: [], "黑名单已清空"), daemon=True).start()
        else:
            self._blacklist_clear_click_time = now
            self._append_log("[GUI] 确认清空黑名单？请在 5 秒内再次点击「一键清空」")

    # ── 队列操作辅助 ──────────────────────────────────────────────────────

    def _get_selected_index(self) -> int | None:
        """返回当前选中项的序号（1-based），未选中则返回 None。"""
        sel = self.queue_tree.selection()
        if not sel:
            return None
        values = self.queue_tree.item(sel[0], "values")
        try:
            return int(values[0])
        except (IndexError, ValueError):
            return None

    def _queue_backend_op(self, path: str, payload: dict[str, Any], status_msg: str = "") -> bool:
        if not self._backend_is_running():
            return False
        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}{path}"
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=2) as resp:
                result = json.loads(resp.read().decode("utf-8", errors="replace"))
            entries = self._extract_entries_from_payload(result)
        except (urllib.error.URLError, TimeoutError, ValueError, json.JSONDecodeError) as exc:
            self.root.after(0, lambda: self._append_log(f"[GUI] 队列操作失败: {exc}"))
            return False

        import time as _time
        ts = _time.strftime("%H:%M:%S")
        msg = f"{ts} {status_msg}" if status_msg else ts
        self.root.after(0, lambda: (self._update_queue_ui(entries), self.queue_status_var.set(msg)))
        return True

    def _queue_local_op(self, op, status_msg: str = "") -> None:
        entries = self._read_queue_entries_from_csv()
        new_entries = op([dict(entry) for entry in entries])
        if new_entries is None:
            new_entries = entries
        new_entries = [
            self._build_queue_entry(
                entry.get("id", ""),
                entry.get("content", ""),
                entry.get("last_operation_at", ""),
            )
            for entry in new_entries
            if isinstance(entry, dict)
        ]
        if not self._write_queue_entries_to_csv(new_entries):
            return
        import time as _time
        ts = _time.strftime("%H:%M:%S")
        msg = f"{ts} {status_msg}" if status_msg else ts
        self.root.after(0, lambda: (self._update_queue_ui(new_entries), self.queue_status_var.set(msg)))

    def _get_selected_row_values(self) -> tuple[int, str, str] | None:
        sel = self.queue_tree.selection()
        if not sel:
            return None
        values = self.queue_tree.item(sel[0], "values")
        try:
            idx = int(values[0])
        except (IndexError, ValueError):
            return None
        item_id = str(values[1]) if len(values) > 1 else ""
        content = str(values[2]) if len(values) > 2 else ""
        return idx, item_id, content

    def _edit_selected_queue_content(self) -> None:
        selected = self._get_selected_row_values()
        if selected is None:
            return
        idx, item_id, current_content = selected
        from tkinter import simpledialog

        new_content = simpledialog.askstring(
            "修改排队内容",
            f"请修改 {item_id or '该条目'} 的排队内容：",
            initialvalue=current_content,
            parent=self.root,
        )
        if new_content is None:
            return

        normalized_content = new_content.strip()
        if self._backend_is_running():
            threading.Thread(
                target=self._queue_backend_op,
                args=("/api/queue/update", {"index": idx, "content": normalized_content}, f"已修改第{idx}位内容"),
                daemon=True,
            ).start()
            return

        def op(entries):
            if 1 <= idx <= len(entries):
                entries[idx - 1]["content"] = normalized_content
                entries[idx - 1]["last_operation_at"] = self._queue_entry_timestamp_now()
            return entries

        threading.Thread(target=self._queue_local_op, args=(op, f"已修改第{idx}位内容"), daemon=True).start()

    def _on_queue_double_click(self, event) -> None:
        row_id = self.queue_tree.identify_row(event.y)
        if not row_id:
            return
        self.queue_tree.selection_set(row_id)
        self._edit_selected_queue_content()

    def _queue_delete(self) -> None:
        idx = self._get_selected_index()
        if idx is None:
            return
        if self._backend_is_running():
            threading.Thread(
                target=self._queue_backend_op,
                args=("/api/queue/delete", {"index": idx}, f"已删除第{idx}位"),
                daemon=True,
            ).start()
            return

        def op(entries):
            if 1 <= idx <= len(entries):
                entries.pop(idx - 1)
            return entries

        threading.Thread(target=self._queue_local_op, args=(op, f"已删除第{idx}位"), daemon=True).start()

    def _queue_move_up(self) -> None:
        idx = self._get_selected_index()
        if idx is None:
            return
        if self._backend_is_running():
            threading.Thread(
                target=self._queue_backend_op,
                args=("/api/queue/move", {"index": idx, "direction": "up"}, f"第{idx}位已上移"),
                daemon=True,
            ).start()
            return

        def op(entries):
            if 2 <= idx <= len(entries):
                entries[idx - 2], entries[idx - 1] = entries[idx - 1], entries[idx - 2]
                ts = self._queue_entry_timestamp_now()
                entries[idx - 2]["last_operation_at"] = ts
                entries[idx - 1]["last_operation_at"] = ts
            return entries

        threading.Thread(target=self._queue_local_op, args=(op, f"第{idx}位已上移"), daemon=True).start()

    def _queue_move_down(self) -> None:
        idx = self._get_selected_index()
        if idx is None:
            return
        if self._backend_is_running():
            threading.Thread(
                target=self._queue_backend_op,
                args=("/api/queue/move", {"index": idx, "direction": "down"}, f"第{idx}位已下移"),
                daemon=True,
            ).start()
            return

        def op(entries):
            if 1 <= idx <= len(entries) - 1:
                entries[idx - 1], entries[idx] = entries[idx], entries[idx - 1]
                ts = self._queue_entry_timestamp_now()
                entries[idx - 1]["last_operation_at"] = ts
                entries[idx]["last_operation_at"] = ts
            return entries

        threading.Thread(target=self._queue_local_op, args=(op, f"第{idx}位已下移"), daemon=True).start()

    def _queue_insert(self) -> None:
        idx = self._get_selected_index() or 0
        from tkinter import simpledialog
        entry = simpledialog.askstring("在下方新增", "请输入排队内容（如：用户名 角色名）：", parent=self.root)
        if not entry or not entry.strip():
            return
        val = entry.strip()
        if self._backend_is_running():
            threading.Thread(
                target=self._queue_backend_op,
                args=("/api/queue/insert", {"after": idx, "entry": val}, f"已在第{idx}位后新增"),
                daemon=True,
            ).start()
            return

        item_id, content = self._parse_queue_item(val)

        def op(entries):
            pos = max(0, min(idx, len(entries)))
            entries.insert(pos, self._build_queue_entry(item_id, content, self._queue_entry_timestamp_now()))
            return entries

        threading.Thread(target=self._queue_local_op, args=(op, f"已在第{idx}位后新增"), daemon=True).start()

    def _queue_clear(self) -> None:
        import time
        now = time.time()
        if self._clear_click_time > 0 and now - self._clear_click_time <= 5.0:
            self._clear_click_time = 0.0
            if self._backend_is_running():
                threading.Thread(
                    target=self._queue_backend_op,
                    args=("/api/queue/clear", {}, "已清空"),
                    daemon=True,
                ).start()
            else:
                threading.Thread(target=self._queue_local_op, args=(lambda _entries: [], "已清空"), daemon=True).start()
        else:
            self._clear_click_time = now
            self._append_log("[GUI] 确认清空？请在 5 秒内再次点击「一键清空」")

    # ── 样式设置辅助 ──────────────────────────────────────────────────────

    _DEFAULT_STYLE = {
        "bg1": "#0e2036", "bg2": "#060b14", "bg3": "#020409",
        "text_color": "#eaf6ff", "queue_font_size": "50",
        "text_grad_start": "#f7f7f7", "text_grad_end": "rgba(255,255,255,0.6)",
        "text_stroke_color": "#000000",
    }

    def _build_style_tab(self, frame: ttk.Frame) -> None:
        from tkinter import colorchooser
        frame.columnconfigure(0, weight=0)
        frame.columnconfigure(1, weight=1)
        frame.rowconfigure(0, weight=1)

        left = ttk.Frame(frame)
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 16))

        right = ttk.LabelFrame(frame, text="预览效果（近似）")
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)

        fields = [
            ("bg1",              "背景渐变色 1",   True),
            ("bg2",              "背景渐变色 2",   True),
            ("bg3",              "背景渐变色 3",   True),
            ("text_color",       "页面文字颜色",   True),
            ("queue_font_size",  "队列字体大小(px)", False),
            ("text_grad_start",  "文字渐变起始色", True),
            ("text_grad_end",    "文字渐变结束色", False),
            ("text_stroke_color","文字描边颜色",   True),
        ]
        self._style_vars: dict[str, tk.StringVar] = {}
        for row_idx, (key, label, has_picker) in enumerate(fields):
            ttk.Label(left, text=label, anchor="e", width=14).grid(row=row_idx, column=0, sticky="e", padx=(0, 6), pady=3)
            var = tk.StringVar(value=self._DEFAULT_STYLE.get(key, ""))
            self._style_vars[key] = var
            entry = ttk.Entry(left, textvariable=var, width=20)
            entry.grid(row=row_idx, column=1, sticky="w")
            if has_picker:
                def _pick(v=var):
                    color = colorchooser.askcolor(color=v.get() if v.get().startswith("#") else "#ffffff", parent=frame)
                    if color and color[1]:
                        v.set(color[1])
                ttk.Button(left, text="取色", command=_pick, width=4).grid(row=row_idx, column=2, padx=(4, 0))

        btn_bar = ttk.Frame(left)
        btn_bar.grid(row=len(fields), column=0, columnspan=3, sticky="w", pady=(12, 0))
        ttk.Button(btn_bar, text="保存样式", command=self._save_style).grid(row=0, column=0, padx=(0, 8))
        ttk.Button(btn_bar, text="恢复默认", command=self._reset_style).grid(row=0, column=1)
        self._style_save_status_var = tk.StringVar(value="")
        ttk.Label(btn_bar, textvariable=self._style_save_status_var, foreground="#0a0").grid(row=0, column=2, padx=(12, 0))

        # 预览画布
        self._style_preview_canvas = tk.Canvas(right, highlightthickness=0)
        self._style_preview_canvas.grid(row=0, column=0, sticky="nsew", padx=4, pady=4)
        for var in self._style_vars.values():
            var.trace_add("write", lambda *_: self.root.after(0, self._redraw_style_preview))
        self._style_preview_canvas.bind("<Configure>", lambda *_: self._redraw_style_preview())

        self._load_style_into_ui()

    def _redraw_style_preview(self) -> None:
        cv = self._style_preview_canvas
        w = cv.winfo_width()
        h = cv.winfo_height()
        if w <= 1 or h <= 1:
            return

        def _safe(key: str, fallback: str) -> str:
            v = self._style_vars.get(key, tk.StringVar()).get().strip()
            return v if v.startswith("#") else fallback

        bg = _safe("bg1", "#0e2036")
        text_c = _safe("text_grad_start", "#f7f7f7")
        side_c = _safe("text_color", "#eaf6ff")
        try:
            fsize_raw = int(self._style_vars["queue_font_size"].get().strip() or 50)
        except (ValueError, KeyError):
            fsize_raw = 50
        # 按预览宽度等比缩放：假设原始宽1920
        fsize = max(8, int(fsize_raw * w / 1920 * 2.5))

        cv.configure(bg=bg)
        cv.delete("all")

        # 侧边竖排文字区域
        side_w = max(24, w // 8)
        cv.create_rectangle(0, 0, side_w, h, fill=_safe("bg2", "#060b14"), outline="")
        font_side = ("Microsoft YaHei UI", max(6, fsize - 4), "bold") if sys.platform == "win32" else ("", max(6, fsize - 4), "bold")
        cv.create_text(side_w // 2, 16, text="排\n队\n姬", fill=side_c, font=font_side, anchor="n")

        # 主队列文字
        font_main = ("Microsoft YaHei UI", fsize, "bold italic") if sys.platform == "win32" else ("", fsize, "bold italic")
        sample = ["示例用户名 角色名", "第二位 职业名称", "第三位用户"]
        y = 12
        for item in sample:
            cv.create_text(side_w + 10, y, text=item, fill=text_c, font=font_main, anchor="nw")
            y += fsize + 6
            if y > h - fsize:
                break

    def _load_style_into_ui(self) -> None:
        try:
            backend_server = load_backend_server_module()
            data = backend_server.load_style()
        except Exception:
            data = {}
        for key, var in self._style_vars.items():
            val = data.get(key)
            if val is not None:
                var.set(str(val))

    def _save_style(self) -> None:
        data: dict = {}
        for key, var in self._style_vars.items():
            v = var.get().strip()
            if key == "queue_font_size":
                try:
                    data[key] = int(v)
                except ValueError:
                    data[key] = 50
            else:
                data[key] = v
        # 始终先写本地文件（index.html 启动时从文件 fetch）
        try:
            backend_server = load_backend_server_module()
            backend_server.save_style(data)
        except Exception as exc:
            self._append_log(f"[GUI] 样式写入文件失败: {exc}")
            self._style_save_status_var.set("保存失败")
            return
        # 如果后端在跑，通知它也刷新（可选）
        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}/api/style"
        body = json.dumps(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=2):
                pass
        except (urllib.error.URLError, TimeoutError):
            pass
        import time as _t
        self._style_save_status_var.set(f"✓ 修改成功 {_t.strftime('%H:%M:%S')}")
        self._append_log("[GUI] 样式已保存，刷新排队展示页即可生效")
        if self._overlay_window_alive():
            self._overlay_style = dict(self._load_style_data())
            self._redraw_overlay()

    def _reset_style(self) -> None:
        for key, var in self._style_vars.items():
            var.set(self._DEFAULT_STYLE.get(key, ""))

    def _apply_overlay_settings_from_ui(self) -> None:
        settings = self._set_overlay_settings(self._get_overlay_settings())
        self._apply_overlay_settings_to_window(settings)
        if self._overlay_process_running():
            self._restart_overlay_process()
        self._append_log(
            f"[GUI] 透明窗口设置已应用：{settings['width']}x{settings['height']}，缩放 {settings['scale']}%"
        )

    def _apply_overlay_settings_to_window(self, settings: dict[str, int] | None = None) -> None:
        if not self._overlay_window_alive():
            return
        normalized = self._set_overlay_settings(settings or self._get_overlay_settings())
        current_x = self._overlay_window.winfo_x()
        current_y = self._overlay_window.winfo_y()
        self._overlay_window.geometry(
            f"{normalized['width']}x{normalized['height']}+{current_x}+{current_y}"
        )
        self._redraw_overlay()

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
        self.log_text.tag_configure("ts", foreground="#080")   # 时间戳：绿色
        self.log_text.tag_configure("ev", foreground="#111")   # 事件内容：深黑
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_frame, orient="vertical", command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

    def _build_settings_tab(self, frame: ttk.Frame) -> None:
        frame.columnconfigure(1, weight=1)
        frame.columnconfigure(2, weight=1)

        row = 0
        for label, var in [
            ("监听地址", self.host_var),
            ("监听端口", self.port_var),
            ("直播间号", self.roomid_var),
            ("UID", self.uid_var),
            ("日志保留天数", self.retention_days_var),
        ]:
            ttk.Label(frame, text=label).grid(row=row, column=0, sticky="w", pady=4)
            ttk.Entry(frame, textvariable=var, width=30).grid(
                row=row, column=1, sticky="ew", pady=4
            )
            row += 1

        # Cookie 行单独处理，右侧加"获取"按钮
        ttk.Label(frame, text="Cookie").grid(row=row, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.cookie_var, width=60).grid(
            row=row, column=1, sticky="ew", pady=4
        )
        ttk.Button(frame, text="获取", command=self.open_config, width=6).grid(
            row=row, column=2, padx=(4, 0), pady=4, sticky="w"
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

        ttk.Label(frame, text="当前存档槽位").grid(row=row, column=0, sticky="w", pady=4)
        self.queue_slot_combo_settings = ttk.Combobox(
            frame,
            textvariable=self.queue_slot_var,
            values=[str(slot) for slot in range(1, MAX_QUEUE_ARCHIVE_SLOTS + 1)],
            width=8,
            state="readonly",
        )
        self.queue_slot_combo_settings.grid(row=row, column=1, sticky="w", pady=4)
        self.queue_slot_combo_settings.bind("<<ComboboxSelected>>", self._on_queue_slot_selected)
        row += 1

        ttk.Checkbutton(frame, text="启用排队存档", variable=self.queue_enabled_var).grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        ttk.Checkbutton(frame, text="启动时自动运行后端", variable=self.auto_start_var).grid(
            row=row, column=1, sticky="w", pady=4
        )
        row += 1

        ttk.Label(frame, text="语言").grid(row=row, column=0, sticky="w", pady=4)
        lang_cb = ttk.Combobox(frame, textvariable=self.language_var, values=["中文"], state="readonly", width=10)
        lang_cb.grid(row=row, column=1, sticky="w", pady=4)
        row += 1

        overlay_frame = ttk.LabelFrame(frame, text="透明窗口设置", padding=8)
        overlay_frame.grid(row=row, column=0, columnspan=3, sticky="ew", pady=(10, 6))
        overlay_frame.columnconfigure(1, weight=1)
        overlay_frame.columnconfigure(3, weight=1)
        ttk.Label(overlay_frame, text="宽度(px)").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(overlay_frame, textvariable=self.overlay_width_var, width=12).grid(row=0, column=1, sticky="w", pady=4)
        ttk.Label(overlay_frame, text="高度(px)").grid(row=0, column=2, sticky="w", padx=(16, 0), pady=4)
        ttk.Entry(overlay_frame, textvariable=self.overlay_height_var, width=12).grid(row=0, column=3, sticky="w", pady=4)
        ttk.Label(overlay_frame, text="文字缩放(%)").grid(row=1, column=0, sticky="w", pady=4)
        ttk.Entry(overlay_frame, textvariable=self.overlay_scale_var, width=12).grid(row=1, column=1, sticky="w", pady=4)
        ttk.Label(
            overlay_frame,
            text='在 OBS 中使用「窗口捕获」，按标题”排队透明弹窗”选择；拖动边框可缩放。',
        ).grid(row=2, column=0, columnspan=4, sticky="w", pady=(6, 0))
        ttk.Button(overlay_frame, text="应用到透明窗", command=self._apply_overlay_settings_from_ui).grid(row=1, column=3, sticky="e", pady=4)

        ctrl_frame = ttk.Frame(overlay_frame)
        ctrl_frame.grid(row=3, column=0, columnspan=4, sticky="w", pady=(6, 0))
        ttk.Button(ctrl_frame, text="关闭弹窗", command=self._stop_overlay_process).pack(side="left", padx=(0, 6))
        ttk.Button(ctrl_frame, text="置顶", command=lambda: self._set_overlay_topmost(True)).pack(side="left", padx=(0, 6))
        ttk.Button(ctrl_frame, text="取消置顶", command=lambda: self._set_overlay_topmost(False)).pack(side="left")
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
            ("disk", "程序目录"),
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
            dir_bytes = sum(
                f.stat().st_size
                for f in APP_DIR.rglob("*")
                if f.is_file()
            )
            disk = psutil.disk_usage(str(APP_DIR))
            dir_mb = dir_bytes / 1024 ** 2
            dir_pct = dir_bytes / disk.total * 100
            disk_text = f"{dir_mb:.1f} MB，占硬盘的 {dir_pct:.2f}%"
        except ImportError:
            cpu_text = mem_text = sysmem_text = disk_text = "需安装 psutil"
        except Exception as exc:  # noqa: BLE001
            cpu_text = mem_text = sysmem_text = disk_text = f"读取失败: {exc}"

        self.root.after(
            0,
            lambda: (
                self._perf_vars["cpu"].set(cpu_text),
                self._perf_vars["mem"].set(mem_text),
                self._perf_vars["sysmem"].set(sysmem_text),
                self._perf_vars["disk"].set(disk_text),
            ),
        )

    def _build_about_tab(self, frame: ttk.Frame) -> None:
        ttk.Label(frame, text=f"{APP_NAME} 控制台", font=("Microsoft YaHei UI", 15, "bold") if sys.platform == "win32" else ("", 15, "bold")).pack(pady=(20, 6))
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
            ("blacklist",   "黑名单（禁止触发任何弹幕指令，也不能同时是管理员/最高管理员）"),
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
            # 后端未运行时从本地配置读（优先 config.yaml，兼容 quanxian.yaml）
            try:
                backend_server = load_backend_server_module()
                raw = backend_server.load_quanxian()
            except Exception:
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
            # 后端未运行时写本地配置（同步写入 config.yaml）
            self._write_quanxian_local(payload)
            self._append_log("[GUI] 权限配置已保存到本地（后端未运行，下次启动生效）")

    def _write_quanxian_local(self, payload: dict[str, list[str]]) -> None:
        try:
            backend_server = load_backend_server_module()
            backend_server.save_quanxian(payload)
        except Exception:
            labels = {
                "super_admin": "最高管理员：拥有所有权限，包括新增/删除管理员",
                "admin": "管理员：拥有除新增/删除管理员以外的所有操作权限",
                "jianzhang": "舰长：仅拥有「插队」命令权限",
                "member": "成员：普通观众",
                "blacklist": "黑名单：禁止触发任何弹幕指令，且不能同时是最高管理员/管理员",
            }
            lines: list[str] = ["# 权限配置\n"]
            for key in ("super_admin", "admin", "jianzhang", "member", "blacklist"):
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
            try:
                backend_server = load_backend_server_module()
                raw = backend_server.load_kaiguan()
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
        try:
            backend_server = load_backend_server_module()
            backend_server.save_kaiguan(payload)
        except Exception:
            comments = {
                "paidui": "排队总开关：关闭后普通/官服/B服/超级/米服排队全部关闭",
                "guanfu_paidui": "官服排队（需总开关开启）",
                "bfu_paidui": "B服排队（需总开关开启）",
                "chaoji_paidui": "超级排队（需总开关开启）",
                "mifu_paidui": "米服排队（需总开关开启）",
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
        self.retention_days_var.set(str(logging_cfg.get("retention_days", 7)))
        self.queue_enabled_var.set(bool(queue_archive.get("enabled", True)))
        active_slot = int(queue_archive.get("active_slot", 1))
        active_slot = self._set_queue_slot_selection(active_slot)
        self._prev_slot = active_slot
        ui_cfg = config.get("ui", {})
        self._set_overlay_settings(ui_cfg.get("overlay_window", DEFAULT_OVERLAY_SETTINGS))
        self.auto_start_var.set(bool(ui_cfg.get("auto_start_backend", False)))
        self.language_var.set(str(ui_cfg.get("language", "中文")))
        if self._overlay_window_alive():
            self._apply_overlay_settings_to_window()
            self._overlay_style = dict(self._load_style_data())
            self._redraw_overlay()
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
                "retention_days": int(self.retention_days_var.get().strip() or 7),
            },
            "queue_archive": {
                "enabled": bool(self.queue_enabled_var.get()),
                "slots": MAX_QUEUE_ARCHIVE_SLOTS,
                "active_slot": self._get_selected_slot(),
            },
            "ui": {
                "auto_start_backend": bool(self.auto_start_var.get()),
                "language": self.language_var.get(),
                "overlay_window": self._get_overlay_settings(),
            },
        }

    def _append_log(self, message: str, warn: bool = False) -> None:
        import time as _t
        self.log_text.configure(state="normal")
        # 若消息尚无时间戳前缀，补一个
        if not _PANEL_TS_RE.match(message):
            message = f"{_t.strftime('%H:%M:%S')} {message}"
        m = _PANEL_TS_RE.match(message)
        if m and not warn:
            ts_part = m.group(1)
            ev_part = m.group(2)
            self.log_text.insert("end", ts_part, "ts")
            self.log_text.insert("end", f" {ev_part}\n", "ev")
        else:
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

        # 剥离 [STDERR]/[STDOUT] 包装，取出真正的日志行
        inner = message
        for pfx in ("[STDERR] ", "[STDOUT] "):
            if inner.startswith(pfx):
                inner = inner[len(pfx):]
                break

        # 将后端完整时间戳 "2026-04-09 12:34:56,ms [LEVEL] name: msg" → "12:34:56 msg"
        ts_match = _LOG_TS_RE.match(inner)
        if ts_match:
            panel_line = f"{ts_match.group(1)} {ts_match.group(2)}"
        else:
            panel_line = inner

        self.log_queue.put(sanitize_log_message(panel_line))

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
        if self._overlay_window_alive():
            self._apply_overlay_settings_to_window()
        if self._overlay_process_running():
            self._restart_overlay_process()
        self._switch_queue_slot()

    def _read_slot_csv(self, slot: int) -> list[dict[str, str]]:
        """读取指定槽位 CSV 的结构化队列条目。"""
        try:
            bs = load_backend_server_module()
            path = bs.PD_DIR / f"queue_archive_slot_{slot}.csv"
        except Exception:
            return []
        if not path.exists():
            return []
        try:
            entries = bs.read_queue_archive_entries(path)
            return [
                self._build_queue_entry(
                    entry.get("id", ""),
                    entry.get("content", ""),
                    entry.get("last_operation_at", ""),
                )
                for entry in entries
                if isinstance(entry, dict)
            ]
        except Exception:
            return []

    def _switch_queue_slot(self, slot: int | None = None) -> None:
        new_slot = self._set_queue_slot_selection(slot) if slot is not None else self._get_selected_slot()
        old_slot = self._prev_slot
        if new_slot == old_slot:
            return
        # 切换前读取旧槽位人数（以 CSV 为准）
        old_count = len(self._read_slot_csv(old_slot))
        # 读取新槽位 CSV 人数
        new_count_csv = len(self._read_slot_csv(new_slot))

        port = self.port_var.get().strip() or "9816"
        url = f"http://127.0.0.1:{port}/api/queue/switch"
        body = json.dumps({"slot": new_slot}).encode("utf-8")
        req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=2) as resp:
                result = json.loads(resp.read().decode("utf-8", errors="replace"))
            new_count = result.get("size", new_count_csv)
            self._append_log(f"[GUI] 切换到存档槽位 {new_slot}，旧存档 {old_count} 人，新存档 {new_count} 人")
            self._prev_slot = new_slot
            self.root.after(300, lambda: threading.Thread(target=self._refresh_queue_list, daemon=True).start())
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
            self._append_log(f"[GUI] 存档槽位已选择 {new_slot}，旧存档 {old_count} 人，新存档 {new_count_csv} 人（下次启动生效）")
            self._prev_slot = new_slot

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
            backend_env = os.environ.copy()
            backend_env["DANMUJI_LAUNCHED_BY_GUI"] = "1"
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
                env=backend_env,
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
        self._stop_overlay_process()
        self._close_overlay_window()
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
    if "--overlay-host" in sys.argv[1:]:
        from core import overlay_host

        args = [arg for arg in sys.argv[1:] if arg != "--overlay-host"]
        overlay_host.main(args)
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
