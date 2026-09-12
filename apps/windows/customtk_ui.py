from __future__ import annotations

import functools
import sys
import threading
from pathlib import Path
from typing import Any

import customtkinter as ctk

_PATCH_LOCK = threading.RLock()
NAV_WIDTH = 178
CONTENT_GAP = 14
WINDOW_WIDTH = 1180
WINDOW_HEIGHT = 720


def _translate_frame_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    mapped = dict(kwargs)
    if "bg" in mapped and "fg_color" not in mapped:
        mapped["fg_color"] = mapped.pop("bg")
    if "background" in mapped and "fg_color" not in mapped:
        mapped["fg_color"] = mapped.pop("background")
    return mapped


def _translate_label_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    mapped = _translate_frame_kwargs(kwargs)
    if "fg" in mapped and "text_color" not in mapped:
        mapped["text_color"] = mapped.pop("fg")
    if "foreground" in mapped and "text_color" not in mapped:
        mapped["text_color"] = mapped.pop("foreground")
    return mapped


def _translate_button_kwargs(kwargs: dict[str, Any]) -> dict[str, Any]:
    mapped = _translate_label_kwargs(kwargs)
    if "activebackground" in mapped and "hover_color" not in mapped:
        mapped["hover_color"] = mapped.pop("activebackground")
    if "activeforeground" in mapped:
        mapped.setdefault("text_color", mapped.pop("activeforeground"))
    return mapped


class BiliPDJCTk(ctk.CTk):
    """CTk root that accepts the small set of legacy Tk color aliases still used by business code."""

    def configure(self, *args: Any, **kwargs: Any) -> Any:
        mapped = dict(kwargs)
        if "bg" in mapped and "fg_color" not in mapped:
            mapped["fg_color"] = mapped.pop("bg")
        if "background" in mapped and "fg_color" not in mapped:
            mapped["fg_color"] = mapped.pop("background")
        return super().configure(*args, **mapped)

    config = configure


class CompatFrame(ctk.CTkFrame):
    def configure(self, *args: Any, **kwargs: Any) -> Any:
        return super().configure(*args, **_translate_frame_kwargs(kwargs))

    config = configure


class CompatLabel(ctk.CTkLabel):
    def configure(self, *args: Any, **kwargs: Any) -> Any:
        return super().configure(*args, **_translate_label_kwargs(kwargs))

    config = configure


class CompatButton(ctk.CTkButton):
    def configure(self, *args: Any, **kwargs: Any) -> Any:
        return super().configure(*args, **_translate_button_kwargs(kwargs))

    config = configure


class CompatProgressBar(ctk.CTkProgressBar):
    def start(self, interval: int | None = None) -> Any:  # noqa: ARG002 - ttk compatibility
        return super().start()


def _palette(panel: Any, dark: bool | None = None) -> dict[str, str]:
    resolved = bool(getattr(panel, "_dark_mode", True)) if dark is None else bool(dark)
    return panel._THEME_DARK if resolved else panel._THEME_LIGHT


def _font_family() -> str:
    if sys.platform == "win32":
        return "Microsoft YaHei UI"
    if sys.platform == "darwin":
        return "PingFang SC"
    return "Sans"


def _safe_configure(widget: Any, **kwargs: Any) -> None:
    if widget is None:
        return
    try:
        widget.configure(**kwargs)
    except Exception:
        pass


def _recolor_custom_shell(panel: Any, dark: bool) -> None:
    t = _palette(panel, dark)
    try:
        ctk.set_appearance_mode("dark" if dark else "light")
    except Exception:
        pass

    _safe_configure(getattr(panel, "_ctk_main_frame", None), fg_color=t["bg"])
    _safe_configure(getattr(panel, "_ctk_top_frame", None), fg_color="transparent")
    _safe_configure(getattr(panel, "_ctk_shell_frame", None), fg_color="transparent")
    _safe_configure(
        getattr(panel, "_nav_frame", None),
        fg_color=t["bg2"],
        border_color=t["border"],
    )
    _safe_configure(
        getattr(panel, "_ctk_content_frame", None),
        fg_color=t["surface"],
        border_color=t["border"],
    )
    _safe_configure(getattr(panel, "_brand_label", None), text_color=t["accent"])
    _safe_configure(getattr(panel, "_ctk_status_label", None), text_color=t["fg2"])

    for button in list(getattr(panel, "_ctk_header_buttons", []) or []):
        _safe_configure(
            button,
            fg_color=t["btn_bg"],
            hover_color=t["btn_active"],
            text_color=t["fg"],
            border_color=t["border"],
        )
    _safe_configure(
        getattr(panel, "_start_button", None),
        fg_color=t["accent"],
        hover_color=t["accent_dim"],
        text_color="#FFFFFF",
        border_width=0,
    )
    _safe_configure(
        getattr(panel, "_stop_button", None),
        fg_color="transparent",
        hover_color=t["btn_active"],
        text_color=t["error"],
        border_color=t["error"],
        border_width=1,
    )
    _safe_configure(
        getattr(panel, "_theme_btn", None),
        fg_color=t["btn_bg"],
        hover_color=t["btn_active"],
        text_color=t["fg"],
        border_color=t["border"],
    )
    _safe_configure(
        getattr(panel, "_startup_indicator", None),
        fg_color=t["surface"],
        border_color=t["border"],
    )
    _safe_configure(
        getattr(panel, "_startup_progress", None),
        fg_color=t["bg2"],
        progress_color=t["accent"],
        border_color=t["border"],
    )


def _refont_custom_shell(panel: Any) -> None:
    try:
        size = max(8, min(20, int(panel.ui_font_size_var.get().strip() or 10)))
    except Exception:
        size = 10
    family = _font_family()
    _safe_configure(getattr(panel, "_brand_label", None), font=(family, size + 6, "bold"))
    _safe_configure(getattr(panel, "_ctk_status_label", None), font=(family, size))
    for button in list(getattr(panel, "_ctk_header_buttons", []) or []):
        _safe_configure(button, font=(family, size))
    _safe_configure(getattr(panel, "_start_button", None), font=(family, size, "bold"))
    _safe_configure(getattr(panel, "_stop_button", None), font=(family, size))
    _safe_configure(getattr(panel, "_theme_btn", None), font=(family, size + 2, "bold"))


def _build_custom_shell(panel: Any) -> None:
    t = _palette(panel)
    family = _font_family()

    panel._nav_items = []
    panel._content_pages = []
    panel._active_page = 0

    root = panel.root
    root.grid_columnconfigure(0, weight=1)
    root.grid_rowconfigure(0, weight=1)

    main = CompatFrame(root, fg_color=t["bg"], corner_radius=0)
    main.grid(row=0, column=0, sticky="nsew", padx=18, pady=(12, 18))
    main.grid_columnconfigure(0, weight=1)
    main.grid_rowconfigure(1, weight=1)
    panel._ctk_main_frame = main

    top = CompatFrame(main, fg_color="transparent", corner_radius=0, height=78)
    top.grid(row=0, column=0, sticky="ew", pady=(0, 12))
    top.grid_columnconfigure(1, weight=1)
    top.grid_propagate(False)
    panel._ctk_top_frame = top

    panel._brand_label = CompatLabel(
        top,
        text="弹幕排队姬",
        text_color=t["accent"],
        font=(family, 18, "bold"),
        anchor="w",
    )
    panel._brand_label.grid(row=0, column=0, rowspan=2, sticky="w", padx=(4, 24))

    status_box = CompatFrame(top, fg_color="transparent", corner_radius=0)
    status_box.grid(row=0, column=1, sticky="w")
    panel._header_dot_label = CompatLabel(
        status_box,
        text="●",
        width=16,
        text_color=t["error"],
        font=("Segoe UI Symbol", 11, "bold"),
    )
    panel._header_dot_label.pack(side="left")
    panel._ctk_status_label = CompatLabel(
        status_box,
        textvariable=panel.header_status_var,
        text_color=t["fg2"],
        font=(family, 10),
        anchor="w",
    )
    panel._ctk_status_label.pack(side="left", padx=(6, 0))

    btn_bar = CompatFrame(top, fg_color="transparent", corner_radius=0)
    btn_bar.grid(row=0, column=2, sticky="e")
    panel._ctk_header_buttons = []

    panel._start_button = CompatButton(
        btn_bar,
        text="启动服务",
        command=panel.start_server,
        width=92,
        height=34,
        corner_radius=8,
        fg_color=t["accent"],
        hover_color=t["accent_dim"],
        text_color="#FFFFFF",
        font=(family, 10, "bold"),
    )
    panel._start_button.grid(row=0, column=0, padx=(0, 7))

    panel._stop_button = CompatButton(
        btn_bar,
        text="停止",
        command=panel.stop_server,
        width=68,
        height=34,
        corner_radius=8,
        fg_color="transparent",
        hover_color=t["btn_active"],
        text_color=t["error"],
        border_width=1,
        border_color=t["error"],
        font=(family, 10),
    )
    panel._stop_button.grid(row=0, column=1, padx=(0, 7))

    header_defs = (
        ("登录配置", panel.open_config, 84),
        ("队列看板", panel.open_web, 84),
        ("OBS 弹窗", panel.open_overlay_window, 84),
    )
    for column, (text, command, width) in enumerate(header_defs, start=2):
        button = CompatButton(
            btn_bar,
            text=text,
            command=command,
            width=width,
            height=34,
            corner_radius=8,
            fg_color=t["btn_bg"],
            hover_color=t["btn_active"],
            text_color=t["fg"],
            border_width=1,
            border_color=t["border"],
            font=(family, 10),
        )
        button.grid(row=0, column=column, padx=(0, 7))
        panel._ctk_header_buttons.append(button)

    panel._theme_btn = CompatButton(
        btn_bar,
        text="☀" if panel._dark_mode else "☾",
        command=panel._toggle_theme,
        width=38,
        height=34,
        corner_radius=8,
        fg_color=t["btn_bg"],
        hover_color=t["btn_active"],
        text_color=t["fg"],
        border_width=1,
        border_color=t["border"],
        font=(family, 12, "bold"),
    )
    panel._theme_btn.grid(row=0, column=5)

    panel._startup_indicator = CompatFrame(
        top,
        fg_color=t["surface"],
        corner_radius=8,
        border_width=1,
        border_color=t["border"],
        height=30,
    )
    panel._startup_indicator.grid(row=1, column=1, columnspan=2, sticky="ew", pady=(7, 0))
    panel._startup_indicator.grid_columnconfigure(1, weight=1)
    CompatLabel(
        panel._startup_indicator,
        textvariable=panel.startup_status_var,
        text_color=t["fg2"],
        font=(family, 9),
        anchor="w",
        width=150,
    ).grid(row=0, column=0, sticky="w", padx=(10, 8))
    panel._startup_progress = CompatProgressBar(
        panel._startup_indicator,
        mode="indeterminate",
        height=7,
        corner_radius=4,
        fg_color=t["bg2"],
        progress_color=t["accent"],
        border_width=0,
    )
    panel._startup_progress.grid(row=0, column=1, sticky="ew", padx=(0, 10))
    panel._startup_indicator.grid_remove()

    shell = CompatFrame(main, fg_color="transparent", corner_radius=0)
    shell.grid(row=1, column=0, sticky="nsew")
    shell.grid_columnconfigure(0, weight=0, minsize=NAV_WIDTH)
    shell.grid_columnconfigure(1, weight=1)
    shell.grid_rowconfigure(0, weight=1)
    panel._ctk_shell_frame = shell

    nav = CompatFrame(
        shell,
        width=NAV_WIDTH,
        fg_color=t["bg2"],
        corner_radius=14,
        border_width=1,
        border_color=t["border"],
    )
    nav.grid(row=0, column=0, sticky="ns", padx=(0, CONTENT_GAP))
    # The width is structural, not measured from label requested widths.  Both
    # geometry propagators are disabled so bold/normal font changes can never
    # push the content column to the right.
    nav.grid_propagate(False)
    nav.pack_propagate(False)
    panel._nav_frame = nav
    panel._issue217_nav_width = NAV_WIDTH

    CompatLabel(
        nav,
        text="控制台",
        text_color=t["fg2"],
        font=(family, 9, "bold"),
        anchor="w",
    ).pack(fill="x", padx=14, pady=(14, 8))

    content = CompatFrame(
        shell,
        fg_color=t["surface"],
        corner_radius=14,
        border_width=1,
        border_color=t["border"],
    )
    content.grid(row=0, column=1, sticky="nsew")
    content.grid_columnconfigure(0, weight=1)
    content.grid_rowconfigure(0, weight=1)
    panel._ctk_content_frame = content

    page_defs = (
        ("日志", panel._build_log_tab),
        ("当前排队", panel._build_queue_tab),
        ("设置", panel._build_settings_tab),
        ("透明窗口", panel._build_overlay_tab),
        ("权限", panel._build_quanxian_tab),
        ("性能", panel._build_perf_tab),
        ("关于", panel._build_about_tab),
    )

    for index, (label, builder) in enumerate(page_defs):
        row = CompatFrame(nav, height=43, fg_color=t["bg2"], corner_radius=8)
        row.pack(fill="x", padx=8, pady=2)
        row.pack_propagate(False)
        indicator = CompatFrame(row, width=3, fg_color=t["bg2"], corner_radius=2)
        indicator.pack(side="left", fill="y", pady=7)
        button = CompatButton(
            row,
            text=label,
            command=lambda i=index: panel._show_page(i),
            anchor="w",
            height=39,
            corner_radius=8,
            fg_color=t["bg2"],
            hover_color=t["btn_active"],
            text_color=t["fg2"],
            font=(family, 11),
        )
        button.pack(side="left", fill="both", expand=True, padx=(2, 0))
        panel._nav_items.append((row, button, indicator))

        page = CompatFrame(content, fg_color="transparent", corner_radius=0)
        page.grid(row=0, column=0, sticky="nsew", padx=14, pady=14)
        page.grid_columnconfigure(0, weight=1)
        page.grid_rowconfigure(0, weight=1)
        builder(page)
        panel._content_pages.append(page)

    panel._bilipdj_customtkinter_shell = True
    panel._apply_theme(panel._dark_mode)
    panel._show_page(0)
    panel._refresh_header_status()


def patch_control_panel_customtkinter(panel_class: type[Any]) -> bool:
    """Install the CustomTkinter shell before legacy feature wrappers are layered on top."""

    if not isinstance(panel_class, type):
        return False
    module = sys.modules.get(str(getattr(panel_class, "__module__", "") or ""))
    if module is None or Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_bilipdj_customtkinter_ui_installed", False)):
            return True

        original_apply_theme = getattr(panel_class, "_apply_theme", None)
        original_apply_font = getattr(panel_class, "_apply_ui_font_size", None)
        if not callable(original_apply_theme) or not callable(original_apply_font):
            return False

        def build_customtk_ui(self: Any, *args: Any, **kwargs: Any) -> None:  # noqa: ARG001
            _build_custom_shell(self)

        @functools.wraps(original_apply_theme)
        def apply_customtk_theme(self: Any, dark: bool = True, *args: Any, **kwargs: Any) -> Any:
            result = original_apply_theme(self, dark, *args, **kwargs)
            _recolor_custom_shell(self, bool(dark))
            return result

        @functools.wraps(original_apply_font)
        def apply_customtk_font(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_apply_font(self, *args, **kwargs)
            _refont_custom_shell(self)
            return result

        panel_class._build_ui = build_customtk_ui
        panel_class._apply_theme = apply_customtk_theme
        panel_class._apply_ui_font_size = apply_customtk_font
        panel_class._bilipdj_customtkinter_ui_installed = True
        return True


def run_control_panel(module: Any) -> None:
    """Run the GUI with a CTk root while delegating backend/overlay modes unchanged."""

    if "--backend" in sys.argv[1:] or "--overlay-host" in sys.argv[1:]:
        module.main()
        return

    try:
        ctk.set_default_color_theme("blue")
        ctk.set_appearance_mode("dark")
    except Exception:
        pass

    root = BiliPDJCTk()
    root.withdraw()
    try:
        root.wm_attributes("-alpha", 0)
    except Exception:
        pass

    module.ControlPanelApp(root)
    root.geometry(f"{WINDOW_WIDTH}x{WINDOW_HEIGHT}")
    root.minsize(WINDOW_WIDTH, WINDOW_HEIGHT)
    root.maxsize(WINDOW_WIDTH, WINDOW_HEIGHT)
    root.resizable(False, False)
    root.update_idletasks()
    root.deiconify()
    try:
        root.wm_attributes("-alpha", 1)
    except Exception:
        pass
    root.mainloop()


__all__ = [
    "BiliPDJCTk",
    "CompatButton",
    "CompatFrame",
    "CompatLabel",
    "CompatProgressBar",
    "NAV_WIDTH",
    "patch_control_panel_customtkinter",
    "run_control_panel",
]
