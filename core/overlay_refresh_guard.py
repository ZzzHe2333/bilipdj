"""Thread-safe refresh loop and advanced typography for the standalone Tk overlay.

Tk font support is imported only while a real overlay is being drawn. Backend,
model and test-only imports therefore continue working on Python builds where
``tkinter`` is unavailable.
"""
from __future__ import annotations

import functools
import queue
import threading
from typing import Any

from .style_option_guard import STYLE_OPTION_DEFAULTS

_PATCH_LOCK = threading.RLock()
_RESULT_POLL_MS = 50
_STYLE_KEYS = (
    "text_color",
    "text_stroke_color",
    "text_stroke_enabled",
    "queue_font_size",
    "queue_font_weight",
    "queue_font_style",
    "auto_scroll",
    "show_sequence",
    *STYLE_OPTION_DEFAULTS.keys(),
)


def _int_value(value: Any, default: int, low: int, high: int) -> int:
    try:
        parsed = int(float(str(value).strip().removesuffix("px")))
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(high, parsed))


def _float_value(value: Any, default: float, low: float, high: float) -> float:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        parsed = default
    return max(low, min(high, parsed))


def _style_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().casefold()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


def _safe_color(value: Any, default: str) -> str:
    text = str(value or "").strip()
    if len(text) == 4 and text.startswith("#"):
        return "#" + "".join(character * 2 for character in text[1:])
    if len(text) == 7 and text.startswith("#"):
        return text
    return default


def _blend_color(foreground: str, background: str, opacity_percent: int) -> str:
    opacity = max(0.0, min(1.0, opacity_percent / 100.0))
    try:
        fg = tuple(int(foreground[index:index + 2], 16) for index in (1, 3, 5))
        bg = tuple(int(background[index:index + 2], 16) for index in (1, 3, 5))
    except (ValueError, IndexError):
        return foreground
    mixed = tuple(
        round(bg_value + (fg_value - bg_value) * opacity)
        for fg_value, bg_value in zip(fg, bg, strict=False)
    )
    return "#" + "".join(f"{value:02x}" for value in mixed)


def _font_family(value: Any) -> str:
    text = str(value or STYLE_OPTION_DEFAULTS["queue_font_family"]).strip()
    first = text.split(",", 1)[0].strip().strip('"\'')
    return first or "Microsoft YaHei UI"


def _font_style_spec(weight_value: Any, style_value: Any) -> tuple[str, str]:
    try:
        numeric_weight = int(str(weight_value).strip() or 700)
    except (TypeError, ValueError):
        numeric_weight = 700
    weight = "bold" if numeric_weight >= 600 else "normal"
    normalized_style = str(style_value or "").strip().lower()
    slant = "italic" if normalized_style in {"italic", "oblique"} else "roman"
    return weight, slant


def _create_font(*, root: Any, family: str, size: int, weight: str, slant: str) -> Any:
    from tkinter import font as tkfont  # imported only by the real overlay process

    return tkfont.Font(root=root, family=family, size=size, weight=weight, slant=slant)


def _character_advance(font: Any, character: str, letter_spacing: int, word_spacing: int) -> int:
    extra = word_spacing if character.isspace() else letter_spacing
    return max(0, int(font.measure(character)) + extra)


def _measure_spaced(font: Any, text: str, letter_spacing: int, word_spacing: int) -> int:
    if not text:
        return 0
    if letter_spacing == 0 and word_spacing == 0:
        return int(font.measure(text))
    return sum(_character_advance(font, character, letter_spacing, word_spacing) for character in text)


def _wrap_text(font: Any, text: str, max_width: int, letter_spacing: int, word_spacing: int) -> list[str]:
    if not text:
        return [""]
    lines: list[str] = []
    current = ""
    current_width = 0
    for character in text:
        advance = _character_advance(font, character, letter_spacing, word_spacing)
        if current and current_width + advance > max_width:
            lines.append(current.rstrip())
            current = character.lstrip() if character.isspace() else character
            current_width = _measure_spaced(font, current, letter_spacing, word_spacing)
        else:
            current += character
            current_width += advance
    if current or not lines:
        lines.append(current.rstrip())
    return lines


def _build_refresh_result(app: Any, style_snapshot: dict[str, Any]):
    queue_payload = app._request_json("/api/queue/state")
    style_payload = app._request_json("/api/style")
    entries = []
    if isinstance(queue_payload, dict):
        raw_entries = queue_payload.get("entries", [])
        if isinstance(raw_entries, list):
            entries = raw_entries
    items = [
        f"{str(entry.get('id', '')).strip()} {str(entry.get('content', '')).strip()}".rstrip()
        for entry in entries
        if isinstance(entry, dict)
        and str(entry.get("id", "") or entry.get("content", "")).strip()
    ]
    next_style = dict(STYLE_OPTION_DEFAULTS)
    next_style.update(style_snapshot)
    if isinstance(style_payload, dict):
        for key in dict.fromkeys(_STYLE_KEYS):
            if key in style_payload:
                next_style[key] = style_payload.get(key)
    return items, next_style


def _draw_spaced_line(
    canvas: Any,
    text: str,
    *,
    x: float,
    y: float,
    available_width: int,
    align: str,
    font: Any,
    fill: str,
    stroke_fill: str,
    stroke_enabled: bool,
    stroke_radius: int,
    letter_spacing: int,
    word_spacing: int,
) -> None:
    text_width = _measure_spaced(font, text, letter_spacing, word_spacing)
    if align == "center":
        cursor_x = x + max(0, (available_width - text_width) / 2)
    elif align == "right":
        cursor_x = x + max(0, available_width - text_width)
    else:
        cursor_x = x

    offsets = [(0, 0)]
    if stroke_enabled:
        offsets = [
            (dx, dy)
            for dx in range(-stroke_radius, stroke_radius + 1)
            for dy in range(-stroke_radius, stroke_radius + 1)
            if dx != 0 or dy != 0
        ] + [(0, 0)]

    if letter_spacing == 0 and word_spacing == 0:
        for dx, dy in offsets:
            canvas.create_text(
                cursor_x + dx,
                y + dy,
                anchor="nw",
                text=text,
                fill=fill if (dx, dy) == (0, 0) else stroke_fill,
                font=font,
            )
        return

    for character in text:
        for dx, dy in offsets:
            canvas.create_text(
                cursor_x + dx,
                y + dy,
                anchor="nw",
                text=character,
                fill=fill if (dx, dy) == (0, 0) else stroke_fill,
                font=font,
            )
        cursor_x += _character_advance(font, character, letter_spacing, word_spacing)


def _redraw_advanced(app: Any, module: Any) -> None:
    canvas = app.canvas
    width = max(1, canvas.winfo_width())
    height = max(1, canvas.winfo_height())
    if width <= 1 or height <= 1:
        return

    style = dict(STYLE_OPTION_DEFAULTS)
    style.update(getattr(app, "style", {}))
    auto_scroll = _style_bool(style.get("auto_scroll"), False)
    show_sequence = _style_bool(style.get("show_sequence"), False)
    if not auto_scroll:
        app._scroll_offset = 0.0
        app._cancel_scroll_job()

    canvas.delete("all")
    canvas.create_rectangle(0, 0, width - 1, height - 1, outline="#7fa3b8", width=1)

    scale = max(40, min(250, int(getattr(app, "scale", 100))))
    font_size = _int_value(style.get("queue_font_size"), 50, 8, 300)
    font_size = max(8, int(font_size * scale / 100))
    weight, slant = _font_style_spec(style.get("queue_font_weight"), style.get("queue_font_style"))
    font = _create_font(
        root=app.root,
        family=_font_family(style.get("queue_font_family")),
        size=font_size,
        weight=weight,
        slant=slant,
    )
    letter_spacing = _int_value(style.get("queue_letter_spacing"), 0, -20, 100)
    word_spacing = _int_value(style.get("queue_word_spacing"), 0, -20, 200)
    line_height = _float_value(style.get("queue_line_height"), 1.2, 0.6, 5.0)
    item_gap = _int_value(style.get("queue_item_gap"), 10, 0, 200)
    padding_x = _int_value(style.get("queue_item_padding_x"), 14, 0, 200)
    padding_y = _int_value(style.get("queue_item_padding_y"), 8, 0, 200)
    opacity = _int_value(style.get("queue_text_opacity"), 100, 0, 100)
    align = str(style.get("queue_text_align", "left") or "left").strip().lower()
    if align not in {"left", "center", "right"}:
        align = "left"

    transparent_color = _safe_color(getattr(module, "OVERLAY_TRANSPARENT_COLOR", "#010101"), "#010101")
    text_color = _blend_color(_safe_color(style.get("text_color"), "#eaf6ff"), transparent_color, opacity)
    stroke_color = _blend_color(_safe_color(style.get("text_stroke_color"), "#000000"), transparent_color, opacity)
    stroke_enabled = _style_bool(style.get("text_stroke_enabled"), True)
    stroke_radius = max(1, min(3, int(font_size * 0.045)))
    line_step = max(1, int(font.metrics("linespace") * line_height))

    logical_y = float(padding_y + 4)
    available_width = max(20, width - 2 * padding_x)
    for index, item in enumerate(app.items, start=1):
        text = module._display_queue_text(index, item, show_sequence)
        lines = _wrap_text(font, text, available_width, letter_spacing, word_spacing)
        draw_y = logical_y - app._scroll_offset
        for line_index, line in enumerate(lines):
            _draw_spaced_line(
                canvas,
                line,
                x=float(padding_x),
                y=draw_y + line_index * line_step,
                available_width=available_width,
                align=align,
                font=font,
                fill=text_color,
                stroke_fill=stroke_color,
                stroke_enabled=stroke_enabled,
                stroke_radius=stroke_radius,
                letter_spacing=letter_spacing,
                word_spacing=word_spacing,
            )
        logical_y += max(1, len(lines)) * line_step + 2 * padding_y + item_gap

    app._scroll_content_height = max(0.0, logical_y - item_gap)
    visible_height = max(1.0, float(height - 2 * padding_y))
    max_offset = max(0.0, app._scroll_content_height - visible_height)
    if app._scroll_offset > max_offset:
        app._scroll_offset = max_offset
    if auto_scroll and max_offset > 1:
        app._schedule_scroll()


def patch_overlay_module(module: Any) -> bool:
    """Patch one loaded overlay module, idempotently."""

    app_class = getattr(module, "OverlayHostApp", None)
    if not isinstance(app_class, type):
        return False
    with _PATCH_LOCK:
        if bool(getattr(app_class, "_bilipdj_refresh_guard_installed", False)):
            return True
        original_close = getattr(app_class, "_close", None)
        original_redraw = getattr(app_class, "_redraw", None)
        if not callable(original_close) or not callable(original_redraw):
            return False
        advanced_supported = callable(getattr(module, "_display_queue_text", None)) and isinstance(
            getattr(module, "DEFAULT_STYLE", None), dict
        )
        defaults = getattr(module, "DEFAULT_STYLE", None)
        if isinstance(defaults, dict):
            for key, value in STYLE_OPTION_DEFAULTS.items():
                defaults.setdefault(key, value)
        refresh_delay = max(100, int(getattr(module, "OVERLAY_REFRESH_MS", 1200)))

        def ensure_state(self: Any) -> None:
            if not hasattr(self, "_refresh_result_queue"):
                self._refresh_result_queue = queue.Queue()
            if not hasattr(self, "_refresh_poll_job"):
                self._refresh_poll_job = None
            if not hasattr(self, "_refresh_timer_job"):
                self._refresh_timer_job = None
            if not hasattr(self, "_refresh_closed"):
                self._refresh_closed = False

        def runtime_redraw(self: Any) -> None:
            if advanced_supported:
                return _redraw_advanced(self, module)
            return original_redraw(self)

        def schedule_poll(self: Any) -> None:
            ensure_state(self)
            if self._refresh_closed or self._refresh_poll_job is not None:
                return
            try:
                self._refresh_poll_job = self.root.after(
                    _RESULT_POLL_MS,
                    lambda: poll_refresh_results(self),
                )
            except Exception:
                self._refresh_poll_job = None
                self._refresh_running = False

        def refresh_worker(self: Any, style_snapshot: dict[str, Any]) -> None:
            result = None
            try:
                result = _build_refresh_result(self, style_snapshot)
            except Exception:
                result = None
            finally:
                self._refresh_result_queue.put(result)

        def refresh_async(self: Any) -> None:
            ensure_state(self)
            if self._refresh_closed or self._refresh_running:
                return
            self._refresh_running = True
            style_snapshot = dict(getattr(self, "style", {}))
            threading.Thread(
                target=refresh_worker,
                args=(self, style_snapshot),
                name="bilipdj-overlay-refresh",
                daemon=True,
            ).start()
            schedule_poll(self)

        def poll_refresh_results(self: Any) -> None:
            ensure_state(self)
            self._refresh_poll_job = None
            if self._refresh_closed:
                self._refresh_running = False
                return
            latest = ...
            while True:
                try:
                    latest = self._refresh_result_queue.get_nowait()
                except queue.Empty:
                    break
            if latest is ...:
                schedule_poll(self)
                return
            self._refresh_running = False
            if latest is not None:
                items, next_style = latest
                changed = items != self.items or next_style != self.style
                self.items = list(items)
                self.style = dict(next_style)
                if changed:
                    self._reset_scroll()
                    self._redraw()
            if not self._refresh_closed:
                try:
                    self._refresh_timer_job = self.root.after(
                        refresh_delay,
                        lambda: refresh_timer_fired(self),
                    )
                except Exception:
                    self._refresh_timer_job = None

        def refresh_timer_fired(self: Any) -> None:
            self._refresh_timer_job = None
            refresh_async(self)

        @functools.wraps(original_close)
        def close_with_refresh_shutdown(self: Any) -> None:
            ensure_state(self)
            self._refresh_closed = True
            self._refresh_running = False
            for attribute in ("_refresh_poll_job", "_refresh_timer_job"):
                job = getattr(self, attribute, None)
                if job is not None:
                    try:
                        self.root.after_cancel(job)
                    except Exception:
                        pass
                    setattr(self, attribute, None)
            return original_close(self)

        setattr(app_class, "_refresh_async", refresh_async)
        setattr(app_class, "_refresh_worker", refresh_worker)
        setattr(app_class, "_poll_refresh_results", poll_refresh_results)
        setattr(app_class, "_redraw", runtime_redraw)
        setattr(app_class, "_close", close_with_refresh_shutdown)
        setattr(app_class, "_bilipdj_refresh_guard_installed", True)
        return True


__all__ = ["patch_overlay_module"]
