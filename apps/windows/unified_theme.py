from __future__ import annotations

import copy
import functools
import json
import sys
import threading
import urllib.error
import urllib.request
from pathlib import Path
from tkinter import colorchooser, filedialog, messagebox
from typing import Any

_PATCH_LOCK = threading.RLock()
PROFILE_NAME = "BiliPDJ-appearance-profile.json"

DEFAULT_APPEARANCE: dict[str, Any] = {
    "schema": 1,
    "design": "aurora",
    "mode": "dark",
    "font_family": "Microsoft YaHei UI",
    "font_size": 10,
    "radius": 10,
    "dark": {
        "background": "#090E1A", "sidebar": "#0D1424", "surface": "#111A2C",
        "surface_alt": "#18233A", "input": "#0D1424", "border": "#26334D",
        "text": "#E6EDF7", "muted": "#8A9AB3", "accent": "#7C6CF2",
        "accent_hover": "#9184FF", "selection": "#7C6CF2", "success": "#32D583",
        "warning": "#F5B942", "danger": "#F97066",
    },
    "light": {
        "background": "#F4F6FB", "sidebar": "#EAEDF5", "surface": "#FFFFFF",
        "surface_alt": "#F0EFFF", "input": "#FBFBFE", "border": "#D5D9E7",
        "text": "#20263A", "muted": "#687089", "accent": "#6757D9",
        "accent_hover": "#5142BC", "selection": "#6757D9", "success": "#007A40",
        "warning": "#B57600", "danger": "#D92D20",
    },
}

COLOR_FIELDS = (
    ("background", "页面背景"), ("sidebar", "侧栏 / 顶栏"), ("surface", "卡片背景"),
    ("surface_alt", "次级卡片 / Hover"), ("input", "输入框背景"), ("border", "边框"),
    ("text", "正文"), ("muted", "次要文字"), ("accent", "品牌主色"),
    ("accent_hover", "主色 Hover"), ("selection", "选中颜色"), ("success", "成功"),
    ("warning", "警告"), ("danger", "错误 / 危险"),
)


def _clone(value: Any) -> Any:
    return copy.deepcopy(value)


def _normalize(raw: Any) -> dict[str, Any]:
    incoming = raw if isinstance(raw, dict) else {}
    result = _clone(DEFAULT_APPEARANCE)
    mode = str(incoming.get("mode", "dark") or "dark").strip().lower()
    result["mode"] = mode if mode in {"system", "light", "dark"} else "dark"
    result["font_family"] = str(incoming.get("font_family", result["font_family"]) or result["font_family"]).strip()[:80]
    try:
        result["font_size"] = max(8, min(20, int(incoming.get("font_size", result["font_size"]))))
    except (TypeError, ValueError):
        pass
    try:
        result["radius"] = max(0, min(24, int(incoming.get("radius", result["radius"]))))
    except (TypeError, ValueError):
        pass
    for scheme in ("dark", "light"):
        palette = incoming.get(scheme, {}) if isinstance(incoming.get(scheme), dict) else {}
        for key, _label in COLOR_FIELDS:
            value = str(palette.get(key, "") or "").strip()
            if len(value) == 7 and value.startswith("#"):
                try:
                    int(value[1:], 16)
                except ValueError:
                    continue
                result[scheme][key] = value.upper()
    return result


def _system_dark() -> bool:
    if sys.platform == "win32":
        try:
            import winreg  # noqa: PLC0415

            with winreg.OpenKey(
                winreg.HKEY_CURRENT_USER,
                r"Software\Microsoft\Windows\CurrentVersion\Themes\Personalize",
            ) as key:
                value, _ = winreg.QueryValueEx(key, "AppsUseLightTheme")
                return int(value) == 0
        except Exception:
            return True
    return True


def _resolved_dark(appearance: dict[str, Any]) -> bool:
    mode = str(appearance.get("mode", "dark") or "dark").lower()
    if mode == "system":
        return _system_dark()
    return mode != "light"


def _port(panel: Any) -> str:
    var = getattr(panel, "port_var", None)
    try:
        return str(var.get()).strip() or "9816"
    except Exception:
        return "9816"


def _api(panel: Any, path: str, *, payload: dict[str, Any] | None = None, timeout: float = 2.5) -> dict[str, Any]:
    url = f"http://127.0.0.1:{_port(panel)}{path}"
    data = None
    method = "GET"
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        method = "POST"
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = json.loads(response.read().decode("utf-8", errors="replace"))
    if not isinstance(raw, dict):
        raise ValueError("后端返回的主题数据无效")
    if raw.get("status") == "error":
        raise RuntimeError(str(raw.get("message", "主题接口错误")))
    return raw


def _bootstrap_appearance(module: Any) -> dict[str, Any]:
    candidates = [
        Path(getattr(module, "APP_DIR", Path.cwd())) / "appearance.json",
        Path(__file__).resolve().parents[2] / "core" / "appearance.json",
    ]
    for path in candidates:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict):
            return _normalize(payload)
    return _clone(DEFAULT_APPEARANCE)


def _palette_to_t(p: dict[str, str], *, dark: bool) -> dict[str, str]:
    return {
        "bg": p["background"], "bg2": p["sidebar"], "surface": p["surface"],
        "accent": p["accent"], "accent_dim": p["accent_hover"],
        "fg": p["text"], "fg2": p["muted"], "border": p["border"],
        "disabled": p["muted"], "btn_bg": p["surface_alt"], "btn_active": p["surface_alt"],
        "input_bg": p["input"], "select_bg": p["selection"], "select_fg": "#FFFFFF",
        "status_ok": p["success"], "warn": p["warning"], "error": p["danger"],
        "ts": p["muted"], "info": p["accent_hover"], "danmu": p["accent"],
        "event": p["success"], "ev": p["text"],
    }


def _set_class_palettes(panel_class: type[Any], appearance: dict[str, Any]) -> None:
    panel_class._THEME_DARK = _palette_to_t(appearance["dark"], dark=True)
    panel_class._THEME_LIGHT = _palette_to_t(appearance["light"], dark=False)


def _apply_fonts(panel: Any, module: Any) -> None:
    appearance = _normalize(getattr(panel, "_bilipdj_appearance", DEFAULT_APPEARANCE))
    family = str(appearance.get("font_family", "Microsoft YaHei UI"))
    size = int(appearance.get("font_size", 10))
    style = module.ttk.Style(panel.root)
    normal = (family, size)
    bold = (family, size, "bold")
    for name in ("TLabel", "TButton", "TEntry", "TCombobox", "TCheckbutton", "TRadiobutton", "TNotebook.Tab"):
        style.configure(name, font=normal)
    style.configure("Nav.TButton", font=(family, size + 1))
    style.configure("NavSelected.TButton", font=(family, size + 1, "bold"))
    style.configure("Settings.TNotebook.Tab", font=(family, size + 1, "bold"))
    style.configure("TLabelframe.Label", font=bold)
    style.configure("Treeview", font=normal, rowheight=size + 12)
    style.configure("Treeview.Heading", font=bold)
    brand = getattr(panel, "_brand_label", None)
    if brand is not None:
        try:
            brand.configure(font=(family, size + 5, "bold"))
        except Exception:
            pass
    for row, button, _indicator in list(getattr(panel, "_nav_items", []) or []):
        try:
            selected = list(panel._nav_items).index((row, button, _indicator)) == getattr(panel, "_active_page", 0)
            button.configure(font=(family, size + 1, "bold" if selected else "normal"))
        except Exception:
            pass


def _set_status(panel: Any, text: str, *, error: bool = False) -> None:
    var = getattr(panel, "_unified_theme_status_var", None)
    if var is not None:
        try:
            var.set(text)
        except Exception:
            pass
    logger = getattr(panel, "_append_log", None)
    if callable(logger) and error:
        logger(f"[GUI] {text}", warn=True)


def _load_from_server(panel: Any, module: Any, *, quiet: bool = False) -> bool:
    try:
        payload = _api(panel, "/api/appearance")
        appearance = _normalize(payload.get("appearance"))
    except Exception as exc:
        if not quiet:
            _set_status(panel, f"读取后端主题失败：{exc}", error=True)
        return False
    panel._bilipdj_appearance = appearance
    panel._unified_theme_dirty = False
    _set_class_palettes(type(panel), appearance)
    panel._apply_theme(_resolved_dark(appearance))
    _fill_editor(panel)
    if not quiet:
        _set_status(panel, "已从 Server 读取通用主题。")
    return True


def _save_to_server(panel: Any, module: Any) -> bool:
    appearance = _collect_editor(panel)
    try:
        payload = _api(panel, "/api/appearance", payload={"appearance": appearance})
        saved = _normalize(payload.get("appearance"))
    except Exception as exc:
        _set_status(panel, f"保存失败：{exc}", error=True)
        messagebox.showerror("保存主题失败", f"需要后端服务器正在运行。\n\n{exc}", parent=panel.root)
        return False
    panel._bilipdj_appearance = saved
    panel._unified_theme_dirty = False
    _set_class_palettes(type(panel), saved)
    panel._apply_theme(_resolved_dark(saved))
    _fill_editor(panel)
    _set_status(panel, "已保存到 Server；Windows/Web 共用 appearance.json。")
    return True


def _editor_scheme(panel: Any) -> str:
    var = getattr(panel, "_unified_theme_scheme_var", None)
    try:
        return "light" if str(var.get()) == "light" else "dark"
    except Exception:
        return "dark"


def _fill_editor(panel: Any) -> None:
    appearance = _normalize(getattr(panel, "_bilipdj_appearance", DEFAULT_APPEARANCE))
    for attr, key in (
        ("_unified_theme_mode_var", "mode"), ("_unified_theme_font_var", "font_family"),
        ("_unified_theme_font_size_var", "font_size"), ("_unified_theme_radius_var", "radius"),
    ):
        var = getattr(panel, attr, None)
        if var is not None:
            try:
                var.set(str(appearance[key]))
            except Exception:
                pass
    palette = appearance[_editor_scheme(panel)]
    for key, _label in COLOR_FIELDS:
        var = getattr(panel, "_unified_theme_color_vars", {}).get(key)
        if var is not None:
            try:
                var.set(palette[key])
            except Exception:
                pass
        button = getattr(panel, "_unified_theme_color_buttons", {}).get(key)
        if button is not None:
            try:
                button.configure(bg=palette[key], activebackground=palette[key])
            except Exception:
                pass


def _collect_editor(panel: Any) -> dict[str, Any]:
    appearance = _normalize(getattr(panel, "_bilipdj_appearance", DEFAULT_APPEARANCE))
    try:
        appearance["mode"] = str(panel._unified_theme_mode_var.get())
        appearance["font_family"] = str(panel._unified_theme_font_var.get()).strip() or DEFAULT_APPEARANCE["font_family"]
        appearance["font_size"] = int(panel._unified_theme_font_size_var.get())
        appearance["radius"] = int(panel._unified_theme_radius_var.get())
    except Exception:
        pass
    palette = appearance[_editor_scheme(panel)]
    for key, _label in COLOR_FIELDS:
        var = getattr(panel, "_unified_theme_color_vars", {}).get(key)
        try:
            value = str(var.get()).strip().upper()
            if len(value) == 7 and value.startswith("#"):
                int(value[1:], 16)
                palette[key] = value
        except Exception:
            continue
    panel._bilipdj_appearance = _normalize(appearance)
    return panel._bilipdj_appearance


def _preview_editor(panel: Any) -> None:
    appearance = _collect_editor(panel)
    panel._unified_theme_dirty = True
    _set_class_palettes(type(panel), appearance)
    panel._apply_theme(_resolved_dark(appearance))
    _set_status(panel, "正在本地实时预览，尚未保存到后端。")


def _choose_color(panel: Any, key: str) -> None:
    var = panel._unified_theme_color_vars[key]
    try:
        initial = str(var.get())
    except Exception:
        initial = "#7C6CF2"
    _rgb, value = colorchooser.askcolor(color=initial, parent=panel.root, title=f"选择 {dict(COLOR_FIELDS)[key]}")
    if value:
        var.set(value.upper())
        _preview_editor(panel)
        _fill_editor(panel)


def _export_profile(panel: Any) -> None:
    try:
        profile = _api(panel, "/api/appearance/profile", timeout=4.0)
    except Exception as exc:
        messagebox.showerror("导出失败", f"需要后端服务器正在运行。\n\n{exc}", parent=panel.root)
        return
    target = filedialog.asksaveasfilename(
        parent=panel.root,
        title="导出 Windows / Web / OBS 通用配置",
        initialfile=PROFILE_NAME,
        defaultextension=".json",
        filetypes=(("BiliPDJ 通用配置", "*.json"), ("所有文件", "*.*")),
    )
    if not target:
        return
    Path(target).write_text(json.dumps(profile, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _set_status(panel, f"已导出通用配置：{Path(target).name}")


def _import_profile(panel: Any, module: Any) -> None:
    source = filedialog.askopenfilename(
        parent=panel.root,
        title="导入 Windows / Web / OBS 通用配置",
        filetypes=(("BiliPDJ 通用配置", "*.json"), ("所有文件", "*.*")),
    )
    if not source:
        return
    try:
        raw = json.loads(Path(source).read_text(encoding="utf-8-sig"))
        payload = _api(panel, "/api/appearance/profile", payload=raw, timeout=5.0)
        appearance = _normalize(payload.get("appearance"))
    except Exception as exc:
        messagebox.showerror("导入失败", str(exc), parent=panel.root)
        return
    panel._bilipdj_appearance = appearance
    panel._unified_theme_dirty = False
    _set_class_palettes(type(panel), appearance)
    panel._apply_theme(_resolved_dark(appearance))
    _fill_editor(panel)
    _set_status(panel, f"已导入 {Path(source).name}；界面主题与 OBS 样式已同步。")


def _install_theme_tab(panel: Any, module: Any) -> None:
    notebook = getattr(panel, "settings_notebook", None)
    if notebook is None or getattr(panel, "_unified_theme_page", None) is not None:
        return

    page = module.ttk.Frame(notebook, padding=14)
    page.columnconfigure(0, weight=1)
    notebook.add(page, text="界面主题")
    panel._unified_theme_page = page

    panel._unified_theme_mode_var = module.tk.StringVar(master=panel.root, value="dark")
    panel._unified_theme_scheme_var = module.tk.StringVar(master=panel.root, value="dark")
    panel._unified_theme_font_var = module.tk.StringVar(master=panel.root, value="Microsoft YaHei UI")
    panel._unified_theme_font_size_var = module.tk.StringVar(master=panel.root, value="10")
    panel._unified_theme_radius_var = module.tk.StringVar(master=panel.root, value="10")
    panel._unified_theme_status_var = module.tk.StringVar(master=panel.root, value="")
    panel._unified_theme_color_vars = {key: module.tk.StringVar(master=panel.root, value="#000000") for key, _ in COLOR_FIELDS}
    panel._unified_theme_color_buttons = {}

    intro = module.ttk.LabelFrame(page, text="BiliPDJ Aurora · Windows / Web 通用主题", padding=12)
    intro.grid(row=0, column=0, sticky="ew")
    intro.columnconfigure(1, weight=1)
    module.ttk.Label(intro, text="当前模式", width=14).grid(row=0, column=0, sticky="w", pady=4)
    mode = module.ttk.Combobox(intro, textvariable=panel._unified_theme_mode_var, state="readonly", values=("system", "light", "dark"), width=16)
    mode.grid(row=0, column=1, sticky="w", pady=4)
    module.ttk.Label(intro, text="编辑配色", width=14).grid(row=0, column=2, sticky="w", padx=(20, 0))
    scheme = module.ttk.Combobox(intro, textvariable=panel._unified_theme_scheme_var, state="readonly", values=("dark", "light"), width=16)
    scheme.grid(row=0, column=3, sticky="w", pady=4)
    module.ttk.Label(intro, text="字体", width=14).grid(row=1, column=0, sticky="w", pady=4)
    module.ttk.Entry(intro, textvariable=panel._unified_theme_font_var, width=32).grid(row=1, column=1, sticky="ew", pady=4)
    module.ttk.Label(intro, text="字号", width=14).grid(row=1, column=2, sticky="w", padx=(20, 0), pady=4)
    module.ttk.Spinbox(intro, textvariable=panel._unified_theme_font_size_var, from_=8, to=20, width=8).grid(row=1, column=3, sticky="w", pady=4)
    module.ttk.Label(intro, text="圆角（Web）", width=14).grid(row=2, column=0, sticky="w", pady=4)
    module.ttk.Spinbox(intro, textvariable=panel._unified_theme_radius_var, from_=0, to=24, width=8).grid(row=2, column=1, sticky="w", pady=4)
    module.ttk.Label(intro, text="Windows 原生 Tk 使用同一颜色/字体；圆角按系统控件能力近似。", style="Muted.Card.TLabel").grid(row=2, column=2, columnspan=2, sticky="w", padx=(20, 0))

    colors = module.ttk.LabelFrame(page, text="通用 Design Tokens", padding=12)
    colors.grid(row=1, column=0, sticky="ew", pady=(10, 0))
    for col in (1, 4):
        colors.columnconfigure(col, weight=1)
    for index, (key, label) in enumerate(COLOR_FIELDS):
        group = 0 if index < 7 else 1
        row = index if group == 0 else index - 7
        base_col = 0 if group == 0 else 3
        module.ttk.Label(colors, text=label, width=15).grid(row=row, column=base_col, sticky="w", pady=3, padx=(0, 6))
        entry = module.ttk.Entry(colors, textvariable=panel._unified_theme_color_vars[key], width=12)
        entry.grid(row=row, column=base_col + 1, sticky="ew", pady=3, padx=(0, 6))
        swatch = module.tk.Button(colors, width=3, relief="flat", cursor="hand2", command=lambda k=key: _choose_color(panel, k))
        swatch.grid(row=row, column=base_col + 2, sticky="w", pady=3, padx=(0, 14))
        panel._unified_theme_color_buttons[key] = swatch
        entry.bind("<FocusOut>", lambda _e: _preview_editor(panel))

    actions = module.ttk.Frame(page)
    actions.grid(row=2, column=0, sticky="ew", pady=(12, 0))
    module.ttk.Button(actions, text="保存到后端", style="Primary.TButton", command=lambda: _save_to_server(panel, module)).pack(side="left", padx=(0, 7))
    module.ttk.Button(actions, text="从后端刷新", command=lambda: _load_from_server(panel, module)).pack(side="left", padx=(0, 7))
    module.ttk.Button(actions, text="导出通用配置", command=lambda: _export_profile(panel)).pack(side="left", padx=(0, 7))
    module.ttk.Button(actions, text="导入通用配置", command=lambda: _import_profile(panel, module)).pack(side="left", padx=(0, 7))
    module.ttk.Button(actions, text="恢复 Aurora 默认", command=lambda: _reset_preview(panel)).pack(side="left")
    module.ttk.Label(page, text="导出的 JSON 同时包含 appearance（Windows/Web）和 display_style（OBS/队列），两端可直接互导。", style="Muted.Card.TLabel").grid(row=3, column=0, sticky="w", pady=(10, 0))
    module.ttk.Label(page, textvariable=panel._unified_theme_status_var).grid(row=4, column=0, sticky="w", pady=(5, 0))

    mode.bind("<<ComboboxSelected>>", lambda _e: _preview_editor(panel))
    scheme.bind("<<ComboboxSelected>>", lambda _e: _fill_editor(panel))
    for var in (panel._unified_theme_font_var, panel._unified_theme_font_size_var, panel._unified_theme_radius_var):
        var.trace_add("write", lambda *_args: None)
    _fill_editor(panel)


def _reset_preview(panel: Any) -> None:
    panel._bilipdj_appearance = _clone(DEFAULT_APPEARANCE)
    panel._unified_theme_dirty = True
    _set_class_palettes(type(panel), panel._bilipdj_appearance)
    panel._apply_theme(_resolved_dark(panel._bilipdj_appearance))
    _fill_editor(panel)
    _set_status(panel, "已恢复 Aurora 默认预览；点击“保存到后端”后跨端同步。")


def _schedule_poll(panel: Any, module: Any) -> None:
    def poll() -> None:
        try:
            if not bool(getattr(panel, "_unified_theme_dirty", False)):
                _load_from_server(panel, module, quiet=True)
        finally:
            try:
                panel._unified_theme_poll_job = panel.root.after(7000, poll)
            except Exception:
                pass
    try:
        panel._unified_theme_poll_job = panel.root.after(3500, poll)
    except Exception:
        pass


def patch_control_panel_unified_theme(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = sys.modules.get(str(getattr(panel_class, "__module__", "") or ""))
    if module is None or Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_bilipdj_unified_theme_installed", False)):
            return True
        original_build_ui = getattr(panel_class, "_build_ui", None)
        original_apply_theme = getattr(panel_class, "_apply_theme", None)
        original_apply_font = getattr(panel_class, "_apply_ui_font_size", None)
        original_toggle = getattr(panel_class, "_toggle_theme", None)
        if not all(callable(item) for item in (original_build_ui, original_apply_theme, original_apply_font, original_toggle)):
            return False

        bootstrap = _bootstrap_appearance(module)
        _set_class_palettes(panel_class, bootstrap)

        @functools.wraps(original_apply_theme)
        def apply_unified_theme(self: Any, dark: bool = True, *args: Any, **kwargs: Any) -> Any:
            appearance = _normalize(getattr(self, "_bilipdj_appearance", bootstrap))
            self._bilipdj_appearance = appearance
            _set_class_palettes(type(self), appearance)
            result = original_apply_theme(self, dark, *args, **kwargs)
            _apply_fonts(self, module)
            return result

        @functools.wraps(original_apply_font)
        def apply_unified_font(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_apply_font(self, *args, **kwargs)
            _apply_fonts(self, module)
            return result

        @functools.wraps(original_build_ui)
        def build_ui_with_unified_theme(self: Any, *args: Any, **kwargs: Any) -> Any:
            self._bilipdj_appearance = _clone(bootstrap)
            self._unified_theme_dirty = False
            result = original_build_ui(self, *args, **kwargs)
            _install_theme_tab(self, module)
            self._apply_theme(_resolved_dark(self._bilipdj_appearance))
            _schedule_poll(self, module)
            return result

        @functools.wraps(original_toggle)
        def toggle_and_sync(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_toggle(self, *args, **kwargs)
            appearance = _normalize(getattr(self, "_bilipdj_appearance", bootstrap))
            appearance["mode"] = "dark" if bool(getattr(self, "_dark_mode", True)) else "light"
            self._bilipdj_appearance = appearance
            self._unified_theme_dirty = False
            try:
                payload = _api(self, "/api/appearance", payload={"appearance": appearance})
                self._bilipdj_appearance = _normalize(payload.get("appearance"))
            except Exception:
                pass
            _fill_editor(self)
            return result

        panel_class._apply_theme = apply_unified_theme
        panel_class._apply_ui_font_size = apply_unified_font
        panel_class._build_ui = build_ui_with_unified_theme
        panel_class._toggle_theme = toggle_and_sync
        panel_class._bilipdj_unified_theme_installed = True
        return True


__all__ = ["DEFAULT_APPEARANCE", "patch_control_panel_unified_theme"]
