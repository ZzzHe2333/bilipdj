"""Indeterminate progress + lightweight light sweep during version discovery.

The animation lives entirely on Tk's event loop. It never runs in the release
HTTP worker and stops as soon as either check flow clears the busy flag.
"""
from __future__ import annotations

import functools
import tkinter as tk
from typing import Any

from . import update_page, update_ui, update_version_selector

GLOW_TICK_MS = 70
GLOW_STEPS = 42
GLOW_COLORS = (
    "#443b72", "#5b4a91", "#7760bd", "#a187e4",
    "#d5c8ff", "#f1ebff", "#d5c8ff", "#a187e4",
    "#7760bd", "#5b4a91", "#443b72",
)


def _animate_check_glow(app: Any) -> None:
    if not getattr(app, "_update_check_glow_active", False):
        return
    canvas = getattr(app, "_update_check_glow_canvas", None)
    if canvas is None:
        return
    try:
        if not canvas.winfo_exists():
            _stop_check_glow(app)
            return
        width = max(1, int(canvas.winfo_width()))
        phase = int(getattr(app, "_update_check_glow_phase", 0))
        center = int(-105 + (width + 210) * ((phase % GLOW_STEPS) / max(1, GLOW_STEPS - 1)))
        canvas.delete("glow")
        canvas.create_rectangle(0, 0, width, 5, fill="#302a4b", outline="", tags="glow")
        for index, color in enumerate(GLOW_COLORS):
            x = center + (index - 5) * 13
            if x >= width or x + 13 <= 0:
                continue
            canvas.create_rectangle(max(0, x), 0, min(width, x + 13), 5,
                                    fill=color, outline="", tags="glow")
        app._update_check_glow_phase = phase + 1
        app._update_check_glow_after = app.root.after(GLOW_TICK_MS, lambda: _animate_check_glow(app))
    except (tk.TclError, RuntimeError):
        _stop_check_glow(app)


def _start_check_glow(app: Any) -> None:
    if getattr(app, "_update_check_glow_active", False):
        return
    canvas = getattr(app, "_update_check_glow_canvas", None)
    progress = getattr(app, "_update_progress", None)
    if canvas is None or progress is None:
        return
    try:
        if not canvas.winfo_exists():
            return
        app._update_check_glow_active = True
        app._update_check_glow_phase = 0
        canvas.place(relx=0, rely=1, anchor="sw", relwidth=1, height=5)
        progress.configure(mode="indeterminate")
        progress.start(14)
        _animate_check_glow(app)
    except (tk.TclError, RuntimeError):
        _stop_check_glow(app)


def _stop_check_glow(app: Any) -> None:
    app._update_check_glow_active = False
    task = getattr(app, "_update_check_glow_after", None)
    app._update_check_glow_after = None
    if task is not None:
        try:
            app.root.after_cancel(task)
        except (tk.TclError, RuntimeError, AttributeError):
            pass
    progress = getattr(app, "_update_progress", None)
    if progress is not None:
        try:
            progress.stop()
            progress.configure(mode="determinate")
        except (tk.TclError, RuntimeError):
            pass
    canvas = getattr(app, "_update_check_glow_canvas", None)
    if canvas is not None:
        try:
            canvas.place_forget()
            canvas.delete("glow")
        except (tk.TclError, RuntimeError):
            pass


def _install_check_widget(app: Any) -> None:
    progress = getattr(app, "_update_progress", None)
    parent = getattr(progress, "master", None)
    if parent is None or getattr(app, "_update_check_glow_canvas", None) is not None:
        return
    try:
        slots = parent.grid_slaves(row=0, column=0)
        if not slots:
            return
        slot = slots[0]
        canvas = tk.Canvas(slot, height=5, bd=0, highlightthickness=0, background="#302a4b")
        app._update_check_glow_canvas = canvas
        app._update_check_glow_active = False
        app._update_check_glow_after = None
        app._update_check_glow_phase = 0
        app.root.bind(
            "<Destroy>",
            lambda event: _stop_check_glow(app) if event.widget is app.root else None,
            add="+",
        )
    except (tk.TclError, RuntimeError, AttributeError):
        return


def install_update_check_glow() -> bool:
    """Install before the desktop shell captures the update-page builder."""
    current_builder = update_page.build_update_tab
    if not getattr(current_builder, "_bilipdj_update_check_glow", False):
        @functools.wraps(current_builder)
        def build_with_glow(app: Any, *args: Any, **kwargs: Any) -> Any:
            result = current_builder(app, *args, **kwargs)
            _install_check_widget(app)
            return result

        build_with_glow._bilipdj_update_check_glow = True  # type: ignore[attr-defined]
        update_page.build_update_tab = build_with_glow

    current_busy = update_ui._set_busy  # noqa: SLF001
    if not getattr(current_busy, "_bilipdj_update_check_glow", False):
        @functools.wraps(current_busy)
        def busy_with_glow(app: Any, busy: bool) -> Any:
            try:
                return current_busy(app, busy)
            finally:
                if not busy and getattr(app, "_update_check_glow_active", False):
                    _stop_check_glow(app)

        busy_with_glow._bilipdj_update_check_glow = True  # type: ignore[attr-defined]
        update_ui._set_busy = busy_with_glow  # type: ignore[attr-defined]  # noqa: SLF001

    for module, attr in ((update_version_selector, "check_for_versions"), (update_ui, "check_for_updates")):
        current = getattr(module, attr)
        if getattr(current, "_bilipdj_update_check_glow", False):
            continue

        def wrap(check: Any) -> Any:
            @functools.wraps(check)
            def check_with_glow(app: Any, *args: Any, **kwargs: Any) -> Any:
                if getattr(app, "_update_busy", False):
                    return check(app, *args, **kwargs)
                _start_check_glow(app)
                try:
                    return check(app, *args, **kwargs)
                except Exception:
                    _stop_check_glow(app)
                    raise

            check_with_glow._bilipdj_update_check_glow = True  # type: ignore[attr-defined]
            return check_with_glow

        setattr(module, attr, wrap(current))
    return True


__all__ = ["install_update_check_glow"]
