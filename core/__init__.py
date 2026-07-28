"""Shared UI bootstrap for bilipdj.

The desktop control panel historically used native ttk checkboxes for boolean
settings. Install one compatible ttk.Checkbutton subclass here so every
existing boolean control keeps its variable/command behaviour while using the
same pill-shaped switch appearance.
"""

from __future__ import annotations

import os
import tkinter as tk
from tkinter import ttk
from typing import Any

try:
    from PIL import Image, ImageDraw, ImageFont, ImageTk
except Exception:  # pragma: no cover - Pillow is bundled in release builds
    Image = ImageDraw = ImageFont = ImageTk = None


_SWITCH_STYLE = "BilipdjSwitch.TCheckbutton"
_ORIGINAL_CHECKBUTTON = getattr(ttk, "_bilipdj_original_checkbutton", ttk.Checkbutton)
setattr(ttk, "_bilipdj_original_checkbutton", _ORIGINAL_CHECKBUTTON)
_REGISTERED_THEMES: set[tuple[str, str]] = set()


def _draw_fallback_switch(master: tk.Misc, *, selected: bool, disabled: bool) -> tk.PhotoImage:
    """Draw a small transparent switch without Pillow."""

    width, height = 64, 30
    # A newly created PhotoImage is transparent. Only the pill and knob pixels
    # are filled, so the control works on both dark and light application themes.
    image = tk.PhotoImage(master=master, width=width, height=height)

    track = "#aeb6c0" if disabled else ("#22c55e" if selected else "#c9cdd3")
    knob = "#eef1f4" if disabled else "#ffffff"
    radius = 14
    center_y = height // 2
    for y in range(1, height - 1):
        for x in range(width):
            left_center = radius
            right_center = width - radius - 1
            inside = radius <= x <= right_center
            if x < radius:
                inside = (x - left_center) ** 2 + (y - center_y) ** 2 <= radius ** 2
            elif x > right_center:
                inside = (x - right_center) ** 2 + (y - center_y) ** 2 <= radius ** 2
            if inside:
                image.put(track, (x, y))

    knob_radius = 11
    knob_center_x = width - 15 if selected else 15
    for y in range(height):
        for x in range(width):
            if (x - knob_center_x) ** 2 + (y - center_y) ** 2 <= knob_radius ** 2:
                image.put(knob, (x, y))
    return image


def _draw_switch(master: tk.Misc, *, selected: bool, disabled: bool = False) -> Any:
    """Return an antialiased transparent ON/OFF switch image."""

    if Image is None or ImageDraw is None or ImageTk is None:
        return _draw_fallback_switch(master, selected=selected, disabled=disabled)

    width, height, scale = 64, 30, 3
    canvas = Image.new("RGBA", (width * scale, height * scale), (0, 0, 0, 0))
    draw = ImageDraw.Draw(canvas)

    if disabled:
        track_fill = "#b8c0c8" if selected else "#d9dde2"
        track_outline = "#a8b0b8"
        knob_fill = "#eef1f4"
        text_fill = "#7d8791"
    elif selected:
        track_fill = "#22c55e"
        track_outline = "#16a34a"
        knob_fill = "#ffffff"
        text_fill = "#ffffff"
    else:
        track_fill = "#c9cdd3"
        track_outline = "#aeb8c5"
        knob_fill = "#ffffff"
        text_fill = "#5f6b7a"

    box = (1 * scale, 1 * scale, (width - 1) * scale, (height - 1) * scale)
    draw.rounded_rectangle(
        box,
        radius=14 * scale,
        fill=track_fill,
        outline=track_outline,
        width=max(1, scale),
    )

    knob_left = 37 if selected else 4
    knob_box = (
        knob_left * scale,
        4 * scale,
        (knob_left + 22) * scale,
        26 * scale,
    )
    draw.ellipse(knob_box, fill=knob_fill)

    resampling = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
    canvas = canvas.resize((width, height), resampling)

    # Draw the compact state word after downsampling so the default bitmap font
    # stays crisp on every platform and does not depend on an external font.
    text_draw = ImageDraw.Draw(canvas)
    state_text = "ON" if selected else "OFF"
    font = ImageFont.load_default() if ImageFont is not None else None
    text_box = text_draw.textbbox((0, 0), state_text, font=font)
    text_width = text_box[2] - text_box[0]
    text_height = text_box[3] - text_box[1]
    text_x = 10 if selected else 38
    text_y = max(0, (height - text_height) // 2 - 1)
    if not selected:
        text_x = max(30, min(width - text_width - 7, text_x))
    text_draw.text((text_x, text_y), state_text, font=font, fill=text_fill)
    return ImageTk.PhotoImage(canvas, master=master)


def _ensure_switch_style(widget: tk.Misc) -> None:
    """Register the no-indicator ttk layout for the active theme."""

    style = ttk.Style(widget)
    try:
        theme = style.theme_use()
    except tk.TclError:
        theme = ""
    key = (str(widget.tk), theme)
    if key in _REGISTERED_THEMES:
        return

    try:
        style.layout(
            _SWITCH_STYLE,
            [
                (
                    "Checkbutton.padding",
                    {
                        "sticky": "nswe",
                        "children": [
                            (
                                "Checkbutton.focus",
                                {
                                    "side": "left",
                                    "sticky": "w",
                                    "children": [
                                        (
                                            "Checkbutton.label",
                                            {"side": "left", "sticky": "nswe"},
                                        )
                                    ],
                                },
                            )
                        ],
                    },
                )
            ],
        )
    except tk.TclError:
        # Some minimal Tk builds do not expose the focus element. The label-only
        # layout still removes the square native indicator.
        style.layout(
            _SWITCH_STYLE,
            [
                (
                    "Checkbutton.padding",
                    {
                        "sticky": "nswe",
                        "children": [
                            ("Checkbutton.label", {"side": "left", "sticky": "nswe"})
                        ],
                    },
                )
            ],
        )
    style.configure(_SWITCH_STYLE, padding=(0, 2), anchor="w")
    _REGISTERED_THEMES.add(key)


class SliderCheckbutton(_ORIGINAL_CHECKBUTTON):
    """Drop-in ttk.Checkbutton rendered as a pill-shaped ON/OFF switch."""

    def __init__(self, master: tk.Misc | None = None, **kwargs: Any) -> None:
        # All boolean controls share one appearance. Existing text, variable,
        # command, onvalue/offvalue, state and geometry behaviour remain native.
        kwargs.pop("image", None)
        kwargs.pop("compound", None)
        kwargs["style"] = _SWITCH_STYLE
        kwargs.setdefault("cursor", "hand2")
        super().__init__(master, **kwargs)

        self._switch_images: tuple[Any, Any, Any, Any] | None = None
        self._install_switch_visuals()
        self.bind("<<ThemeChanged>>", self._on_theme_changed, add="+")
        # ControlPanelApp switches to the clam theme after constructing pages.
        # Re-register after the event loop starts so the custom layout survives.
        self.after_idle(self._install_switch_visuals)

    def _on_theme_changed(self, _event: tk.Event[Any] | None = None) -> None:
        self.after_idle(self._install_switch_visuals)

    def _install_switch_visuals(self) -> None:
        if not self.winfo_exists():
            return
        _ensure_switch_style(self)
        off = _draw_switch(self, selected=False)
        on = _draw_switch(self, selected=True)
        off_disabled = _draw_switch(self, selected=False, disabled=True)
        on_disabled = _draw_switch(self, selected=True, disabled=True)
        self._switch_images = (off, on, off_disabled, on_disabled)
        self.configure(
            compound="right",
            image=(
                off,
                "selected disabled",
                on_disabled,
                "disabled",
                off_disabled,
                "selected",
                on,
            ),
        )


def install_slider_switches() -> None:
    """Replace ttk.Checkbutton once for every module sharing tkinter.ttk."""

    if os.environ.get("BILIPDJ_NATIVE_CHECKBUTTONS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return
    ttk.Checkbutton = SliderCheckbutton


install_slider_switches()

__all__ = ["SliderCheckbutton", "install_slider_switches"]
