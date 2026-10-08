from __future__ import annotations

import functools
import sys
import threading
from pathlib import Path
from typing import Any

from PIL import Image, ImageTk

DONATION_COPY = "如果这个项目帮到了你，欢迎自愿扫码赞赏，支持后续维护与更新。"
DONATION_ASSET = "WxZSM.png"

_PATCH_LOCK = threading.RLock()


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
    """Render the donation-only Windows support page used by all Tk patch layers."""
    _clear_children(frame)
    frame.columnconfigure(0, weight=1)
    frame.rowconfigure(0, weight=1)

    card = module.ttk.Frame(frame, padding=(28, 24))
    card.grid(row=0, column=0, sticky="nsew")
    card.columnconfigure(0, weight=1)

    module.ttk.Label(card, text="支持我们", font=("Microsoft YaHei UI", 18, "bold")).grid(
        row=0, column=0, pady=(0, 6)
    )
    module.ttk.Label(
        card,
        text="项目本体永久免费开源，赞赏完全自愿。",
        justify="center",
        wraplength=760,
    ).grid(row=1, column=0, pady=(0, 20))

    donation = module.ttk.LabelFrame(card, text="赞赏项目", padding=(24, 20))
    donation.grid(row=2, column=0, padx=24)
    donation.columnconfigure(0, weight=1)
    module.ttk.Label(donation, text=DONATION_COPY, justify="center", wraplength=400).grid(
        row=0, column=0, pady=(0, 14)
    )
    try:
        donation_photo = _make_donation_photo(panel.root, size=220)
        panel._support_us_donation_photo = donation_photo
        module.ttk.Label(donation, image=donation_photo).grid(row=1, column=0, pady=(0, 14))
        module.ttk.Label(donation, text="微信赞赏码", justify="center").grid(row=2, column=0, pady=(0, 8))
    except Exception as exc:  # noqa: BLE001
        module.ttk.Label(donation, text=f"赞赏码加载失败：{exc}", justify="center", wraplength=400).grid(
            row=1, column=0, pady=(0, 14)
        )
    module.ttk.Label(donation, text="赞赏完全自愿，不影响任何功能使用。", justify="center", wraplength=400).grid(
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

        setattr(build_ui_with_support, "_bilipdj_support_us", True)
        setattr(panel_class, "_build_ui", build_ui_with_support)
        return True


__all__ = [
    "DONATION_COPY",
    "DONATION_ASSET",
    "patch_control_panel_support_us",
]
