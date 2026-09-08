from __future__ import annotations

import functools
import random
import sys
import threading
import time
import webbrowser
from pathlib import Path
from typing import Any

import qrcode
from PIL import Image, ImageTk

SUPPORT_URL = "https://m.sdyuntuo.cn/ProductEn/Index/01929ef4362a8858"
SUPPORT_COPY = "项目免费开源使用，申请流量卡可以为项目带来持续支持。"
DONATION_COPY = "如果这个项目帮到了你，也可以自愿扫码赞赏，支持后续维护与更新。"
DONATION_ASSET = "WxZSM.png"
GUANGGAO_LEVEL = "GUANGGAO"
GUANGGAO_TEXT = "【打扰一下】如果有需要正规大流量电话卡的，可以点击 支持我们-申请流量卡，自助申请哦。你的每张正常申请使用，都能给本项目带来持续的支持！"
PROMO_INITIAL_DELAY_RANGE_MS = (45_000, 150_000)
PROMO_REPEAT_DELAY_RANGE_MS = (480_000, 1_080_000)

_PATCH_LOCK = threading.RLock()


def _make_qr_photo(master: Any, size: int = 220) -> ImageTk.PhotoImage:
    qr = qrcode.QRCode(
        error_correction=qrcode.constants.ERROR_CORRECT_M,
        box_size=6,
        border=4,
    )
    qr.add_data(SUPPORT_URL)
    qr.make(fit=True)
    image = qr.make_image(fill_color="black", back_color="white").get_image().convert("RGB")
    image = image.resize((size, size), Image.Resampling.NEAREST)
    return ImageTk.PhotoImage(image, master=master)


def _donation_asset_candidates() -> tuple[Path, ...]:
    project_root = Path(__file__).resolve().parents[2]
    bundle_root = Path(getattr(sys, "_MEIPASS", project_root)).resolve()
    app_dir = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else project_root
    relative = Path("apps") / "web" / "static" / DONATION_ASSET
    return (
        bundle_root / relative,
        project_root / relative,
        app_dir / relative,
        app_dir / "_internal" / relative,
    )


def _make_donation_photo(master: Any, size: int = 220) -> ImageTk.PhotoImage:
    source = next((path for path in _donation_asset_candidates() if path.is_file()), None)
    if source is None:
        raise FileNotFoundError(DONATION_ASSET)
    with Image.open(source) as opened:
        image = opened.convert("RGB")
        image.thumbnail((size, size), Image.Resampling.LANCZOS)
        canvas = Image.new("RGB", (size, size), "white")
        x = (size - image.width) // 2
        y = (size - image.height) // 2
        canvas.paste(image, (x, y))
    return ImageTk.PhotoImage(canvas, master=master)


def _clear_children(widget: Any) -> None:
    try:
        children = list(widget.winfo_children())
    except Exception:
        children = []
    for child in children:
        try:
            child.destroy()
        except Exception:
            pass


def _render_support_content(panel: Any, frame: Any, module: Any) -> None:
    """Render the canonical Windows support page used by all Tk patch layers."""
    _clear_children(frame)
    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(0, weight=1)

    card = module.ttk.Frame(frame, padding=(28, 24))
    card.grid(row=0, column=0, sticky="nsew")
    card.columnconfigure(0, weight=1, uniform="support")
    card.columnconfigure(1, weight=1, uniform="support")

    module.ttk.Label(card, text="支持我们", font=("Microsoft YaHei UI", 18, "bold")).grid(
        row=0, column=0, columnspan=2, pady=(0, 6)
    )
    module.ttk.Label(
        card,
        text="项目本体永久免费开源。你可以申请流量卡支持项目，也可以选择扫码赞赏。",
        justify="center",
        wraplength=760,
    ).grid(row=1, column=0, columnspan=2, pady=(0, 20))

    left = module.ttk.LabelFrame(card, text="申请流量卡", padding=(20, 18))
    left.grid(row=2, column=0, sticky="nsew", padx=(0, 10))
    left.columnconfigure(0, weight=1)
    module.ttk.Label(left, text=SUPPORT_COPY, justify="center", wraplength=320).grid(row=0, column=0, pady=(0, 14))
    try:
        qr_photo = _make_qr_photo(panel.root, size=220)
        panel._support_us_qr_photo = qr_photo
        module.ttk.Label(left, image=qr_photo).grid(row=1, column=0, pady=(0, 14))
    except Exception as exc:  # noqa: BLE001
        module.ttk.Label(left, text=f"流量卡二维码生成失败：{exc}", justify="center", wraplength=300).grid(
            row=1, column=0, pady=(0, 14)
        )
    module.ttk.Button(
        left,
        text="申请流量卡",
        style="Primary.TButton",
        command=lambda: webbrowser.open(SUPPORT_URL),
    ).grid(row=2, column=0, ipadx=18, ipady=5, pady=(0, 10))
    module.ttk.Label(left, text="点击按钮跳转，或使用手机扫描二维码。", justify="center", wraplength=320).grid(
        row=3, column=0, pady=(0, 6)
    )
    module.ttk.Label(left, text=SUPPORT_URL, justify="center", wraplength=330).grid(row=4, column=0)

    right = module.ttk.LabelFrame(card, text="赞赏项目", padding=(20, 18))
    right.grid(row=2, column=1, sticky="nsew", padx=(10, 0))
    right.columnconfigure(0, weight=1)
    module.ttk.Label(right, text=DONATION_COPY, justify="center", wraplength=320).grid(row=0, column=0, pady=(0, 14))
    try:
        donation_photo = _make_donation_photo(panel.root, size=220)
        panel._support_us_donation_photo = donation_photo
        module.ttk.Label(right, image=donation_photo).grid(row=1, column=0, pady=(0, 14))
        module.ttk.Label(right, text="微信赞赏码", justify="center").grid(row=2, column=0, pady=(0, 8))
    except Exception as exc:  # noqa: BLE001
        module.ttk.Label(right, text=f"赞赏码加载失败：{exc}", justify="center", wraplength=300).grid(
            row=1, column=0, pady=(0, 14)
        )
    module.ttk.Label(right, text="赞赏完全自愿，不影响任何功能使用。", justify="center", wraplength=320).grid(
        row=3, column=0
    )


def _build_support_page(panel: Any, module: Any) -> None:
    if getattr(panel, "_support_us_page", None) is not None:
        return
    nav_items = getattr(panel, "_nav_items", None)
    content_pages = getattr(panel, "_content_pages", None)
    if not nav_items or not content_pages:
        return

    tk = module.tk
    ttk = module.ttk
    nav = nav_items[0][0].master
    content = content_pages[0].master
    index = len(content_pages)

    row = tk.Frame(nav, bd=0, highlightthickness=0)
    row.pack(fill="x", pady=1)
    indicator = tk.Frame(row, width=3, bd=0)
    indicator.pack(side="left", fill="y")
    button = tk.Button(
        row,
        text="支持我们",
        command=lambda i=index: panel._show_page(i),
        anchor="w",
        padx=13,
        pady=10,
        bd=0,
        relief="flat",
        highlightthickness=0,
        cursor="hand2",
    )
    button.pack(side="left", fill="x", expand=True)

    page = ttk.Frame(content, padding=2)
    page.grid(row=0, column=0, sticky="nsew")
    _render_support_content(panel, page, module)

    nav_items.append((row, button, indicator))
    content_pages.append(page)
    panel._support_us_page = page

    try:
        panel._apply_theme(bool(getattr(panel, "_dark_mode", False)))
    except Exception:
        pass


def _configure_guanggao_tag(panel: Any) -> None:
    log_text = getattr(panel, "log_text", None)
    if log_text is None:
        return
    try:
        font = log_text.cget("font")
        log_text.tag_configure("level_guanggao", foreground="#d9a441", font=font)
    except Exception:
        pass


def _emit_guanggao(panel: Any) -> None:
    """Insert a promotion into the Tk text widget only; never call the file-backed GUI logger."""
    root = getattr(panel, "root", None)
    log_text = getattr(panel, "log_text", None)
    renderer = getattr(panel, "_render_log_record", None)
    if root is None or log_text is None or not callable(renderer):
        return
    try:
        if not bool(root.winfo_exists()):
            return
        log_text.configure(state="normal")
        renderer(time.strftime("%H:%M:%S"), GUANGGAO_LEVEL, GUANGGAO_TEXT)
        auto_var = getattr(panel, "log_auto_scroll_var", None)
        if auto_var is None or bool(auto_var.get()):
            log_text.see("end")
        log_text.configure(state="disabled")
    except Exception:
        try:
            log_text.configure(state="disabled")
        except Exception:
            pass


def _schedule_guanggao(panel: Any, *, initial: bool) -> None:
    root = getattr(panel, "root", None)
    if root is None:
        return
    low, high = PROMO_INITIAL_DELAY_RANGE_MS if initial else PROMO_REPEAT_DELAY_RANGE_MS
    delay = random.randint(low, high)

    def fire() -> None:
        _emit_guanggao(panel)
        _schedule_guanggao(panel, initial=False)

    try:
        panel._support_us_guanggao_after = root.after(delay, fire)
    except Exception:
        pass


def patch_control_panel_support_us(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    with _PATCH_LOCK:
        current = getattr(panel_class, "_build_ui", None)
        if not callable(current):
            return False
        if bool(getattr(current, "_bilipdj_support_us", False)):
            return True

        @functools.wraps(current)
        def build_ui_with_support(self: Any) -> None:
            current(self)
            _build_support_page(self, module)
            _configure_guanggao_tag(self)
            if not getattr(self, "_support_us_guanggao_scheduled", False):
                self._support_us_guanggao_scheduled = True
                _schedule_guanggao(self, initial=True)

        setattr(build_ui_with_support, "_bilipdj_support_us", True)
        setattr(panel_class, "_build_ui", build_ui_with_support)
        setattr(panel_class, "_emit_guanggao", _emit_guanggao)
        return True


__all__ = [
    "SUPPORT_URL",
    "SUPPORT_COPY",
    "DONATION_COPY",
    "DONATION_ASSET",
    "GUANGGAO_LEVEL",
    "GUANGGAO_TEXT",
    "PROMO_INITIAL_DELAY_RANGE_MS",
    "PROMO_REPEAT_DELAY_RANGE_MS",
    "patch_control_panel_support_us",
]
