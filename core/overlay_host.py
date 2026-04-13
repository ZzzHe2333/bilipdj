from __future__ import annotations

import argparse
import json
import sys
import threading
import tkinter as tk
import urllib.error
import urllib.request
from typing import Any

if sys.platform == "win32":
    import ctypes
else:
    ctypes = None

OVERLAY_TRANSPARENT_COLOR = "#010101"
OVERLAY_REFRESH_MS = 1200
OVERLAY_RESIZE_MARGIN = 8
OVERLAY_MIN_WIDTH = 320
OVERLAY_MIN_HEIGHT = 180
DEFAULT_STYLE = {
    "text_color": "#eaf6ff",
    "text_stroke_color": "#000000",
    "text_stroke_enabled": True,
    "queue_font_size": 50,
    "queue_font_weight": "700",
    "queue_font_style": "italic",
}


def _to_int(value: Any, default: int) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _clamp(value: int, low: int, high: int) -> int:
    return max(low, min(high, value))


def _safe_color(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    return text if text.startswith("#") else fallback


def _font_style_spec(weight_value: Any, style_value: Any) -> str:
    try:
        numeric_weight = int(str(weight_value).strip() or 700)
    except (TypeError, ValueError):
        numeric_weight = 700
    normalized_style = str(style_value or "").strip().lower()
    parts: list[str] = []
    if numeric_weight >= 600:
        parts.append("bold")
    if normalized_style in {"italic", "oblique"}:
        parts.append("italic")
    return " ".join(parts) if parts else "normal"


def _style_bool(value: Any, default: bool = True) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return default


class OverlayHostApp:
    def __init__(self, *, port: int, width: int, height: int, scale: int, topmost: bool = True) -> None:
        self.port = int(port)
        self.width = max(OVERLAY_MIN_WIDTH, int(width))
        self.height = max(OVERLAY_MIN_HEIGHT, int(height))
        self.scale = _clamp(int(scale), 40, 250)

        self.items: list[str] = []
        self.style: dict[str, Any] = dict(DEFAULT_STYLE)
        self._refresh_running = False
        self._topmost = topmost
        self._drag_origin: tuple[int, int] | None = None
        self._window_origin: tuple[int, int] | None = None
        self._resize_mode = ""
        self._resize_origin: tuple[int, int] | None = None
        self._resize_geometry: tuple[int, int, int, int] | None = None

        self.root = tk.Tk()
        self.root.withdraw()  # hide immediately to prevent decoration flash
        self.root.title("排队透明弹窗")
        if sys.platform != "win32":
            # non-Windows: overrideredirect for frameless; Win32 handles this via ctypes instead
            self.root.overrideredirect(True)
        self.root.resizable(True, True)
        self.root.configure(bg=OVERLAY_TRANSPARENT_COLOR)
        self.root.geometry(self._default_geometry())
        try:
            self.root.wm_attributes("-transparentcolor", OVERLAY_TRANSPARENT_COLOR)
        except tk.TclError:
            try:
                self.root.wm_attributes("-alpha", 0.96)
            except tk.TclError:
                pass
        try:
            self.root.wm_attributes("-topmost", self._topmost)
        except tk.TclError:
            pass

        self.canvas = tk.Canvas(
            self.root,
            bg=OVERLAY_TRANSPARENT_COLOR,
            bd=0,
            highlightthickness=0,
            relief="flat",
        )
        self.canvas.pack(fill="both", expand=True)

        # Escape still works as emergency close; right-click and double-click removed
        # (close / topmost are now controlled from the main GUI)
        self.root.bind("<Escape>", lambda _event: self._close())
        self.root.bind("<Configure>", self._on_window_configure)
        self.canvas.bind("<Configure>", lambda _event: self._redraw())
        self.canvas.bind("<Motion>", self._on_canvas_motion)
        self.canvas.bind("<Leave>", lambda _event: self.canvas.configure(cursor=""))
        self.canvas.bind("<ButtonPress-1>", self._begin_interaction)
        self.canvas.bind("<B1-Motion>", self._perform_interaction)
        self.canvas.bind("<ButtonRelease-1>", self._end_interaction)

        self.root.update_idletasks()
        self._apply_native_window_style()
        self._refresh_async()

    def _default_geometry(self) -> str:
        screen_w = self.root.winfo_screenwidth()
        screen_h = self.root.winfo_screenheight()
        width = min(screen_w - 48, self.width)
        height = min(screen_h - 48, self.height)
        x = max(24, screen_w - width - 80)
        y = max(24, min(120, screen_h - height - 80))
        return f"{width}x{height}+{x}+{y}"

    def _apply_native_window_style(self) -> None:
        if sys.platform != "win32" or ctypes is None:
            self.root.deiconify()
            return
        try:
            hwnd = self.root.winfo_id()
            GWL_STYLE = -16
            GWL_EXSTYLE = -20
            WS_CAPTION = 0x00C00000
            WS_SYSMENU = 0x00080000
            WS_MINIMIZEBOX = 0x00020000
            WS_MAXIMIZEBOX = 0x00010000
            WS_THICKFRAME = 0x00040000
            WS_EX_APPWINDOW = 0x00040000
            WS_EX_TOOLWINDOW = 0x00000080
            SWP_NOMOVE = 0x0002
            SWP_NOSIZE = 0x0001
            SWP_NOZORDER = 0x0004
            SWP_FRAMECHANGED = 0x0020

            # Strip all window decorations — leaves a plain frameless window that OBS can still
            # find by title ("排队透明弹窗") via standard EnumWindows, unlike WS_POPUP windows
            # created by overrideredirect which some OBS versions skip in their window list.
            style = ctypes.windll.user32.GetWindowLongW(hwnd, GWL_STYLE)
            style &= ~(WS_CAPTION | WS_SYSMENU | WS_MINIMIZEBOX | WS_MAXIMIZEBOX | WS_THICKFRAME)
            ctypes.windll.user32.SetWindowLongW(hwnd, GWL_STYLE, style)

            # Ensure taskbar visibility and OBS window enumeration
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
            self.root.after(10, self._restore_window)
        except Exception as exc:  # noqa: BLE001
            print(f"[OVERLAY] apply native window style failed: {exc}")
            self.root.deiconify()

    def _restore_window(self) -> None:
        try:
            self.root.deiconify()
            self.root.lift()
            self.root.wm_attributes("-topmost", self._topmost)
        except tk.TclError:
            pass

    @staticmethod
    def _hit_test(x: int, y: int, width: int, height: int) -> str:
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
    def _cursor_for_mode(mode: str) -> str:
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

    def _set_cursor(self, cursor: str) -> None:
        try:
            self.canvas.configure(cursor=cursor)
        except tk.TclError:
            self.canvas.configure(cursor="")

    def _on_canvas_motion(self, event) -> None:
        if self._drag_origin or self._resize_mode:
            return
        mode = self._hit_test(
            int(event.x),
            int(event.y),
            self.canvas.winfo_width(),
            self.canvas.winfo_height(),
        )
        self._set_cursor(self._cursor_for_mode(mode))

    def _begin_interaction(self, event) -> None:
        mode = self._hit_test(
            int(event.x),
            int(event.y),
            self.canvas.winfo_width(),
            self.canvas.winfo_height(),
        )
        if mode:
            self._resize_mode = mode
            self._resize_origin = (event.x_root, event.y_root)
            self._resize_geometry = (
                self.root.winfo_x(),
                self.root.winfo_y(),
                self.root.winfo_width(),
                self.root.winfo_height(),
            )
            self._set_cursor(self._cursor_for_mode(mode))
            return
        self._drag_origin = (event.x_root, event.y_root)
        self._window_origin = (self.root.winfo_x(), self.root.winfo_y())
        self._set_cursor("fleur")

    def _perform_interaction(self, event) -> None:
        if self._resize_mode and self._resize_origin and self._resize_geometry:
            dx = event.x_root - self._resize_origin[0]
            dy = event.y_root - self._resize_origin[1]
            x, y, width, height = self._resize_geometry
            new_x, new_y, new_w, new_h = x, y, width, height
            if "e" in self._resize_mode:
                new_w = max(OVERLAY_MIN_WIDTH, width + dx)
            if "s" in self._resize_mode:
                new_h = max(OVERLAY_MIN_HEIGHT, height + dy)
            if "w" in self._resize_mode:
                new_w = max(OVERLAY_MIN_WIDTH, width - dx)
                new_x = x + (width - new_w)
            if "n" in self._resize_mode:
                new_h = max(OVERLAY_MIN_HEIGHT, height - dy)
                new_y = y + (height - new_h)
            self.root.geometry(f"{new_w}x{new_h}+{new_x}+{new_y}")
            return

        if not self._drag_origin or not self._window_origin:
            return
        dx = event.x_root - self._drag_origin[0]
        dy = event.y_root - self._drag_origin[1]
        x = self._window_origin[0] + dx
        y = self._window_origin[1] + dy
        self.root.geometry(f"+{x}+{y}")

    def _end_interaction(self, _event) -> None:
        self._drag_origin = None
        self._window_origin = None
        self._resize_mode = ""
        self._resize_origin = None
        self._resize_geometry = None
        self._set_cursor("")

    def _on_window_configure(self, event) -> None:
        if event.widget is not self.root:
            return
        self.width = max(OVERLAY_MIN_WIDTH, self.root.winfo_width())
        self.height = max(OVERLAY_MIN_HEIGHT, self.root.winfo_height())
        self._redraw()

    def _close(self) -> None:
        try:
            self.root.destroy()
        except tk.TclError:
            pass

    def _request_json(self, path: str, timeout: float = 1.5) -> dict[str, Any] | None:
        url = f"http://127.0.0.1:{self.port}{path}"
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8", errors="replace"))
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError):
            return None

    def _refresh_async(self) -> None:
        if self._refresh_running:
            return
        self._refresh_running = True
        threading.Thread(target=self._refresh_worker, daemon=True).start()

    def _refresh_worker(self) -> None:
        queue_payload = self._request_json("/api/queue/state")
        style_payload = self._request_json("/api/style")

        entries = []
        if isinstance(queue_payload, dict):
            payload_entries = queue_payload.get("entries", [])
            if isinstance(payload_entries, list):
                entries = payload_entries

        items = [
            f"{str(entry.get('id', '')).strip()} {str(entry.get('content', '')).strip()}".rstrip()
            for entry in entries
            if isinstance(entry, dict) and str(entry.get("id", "") or entry.get("content", "")).strip()
        ]

        next_style = dict(self.style)
        if isinstance(style_payload, dict):
            for key in ("text_color", "text_stroke_color", "text_stroke_enabled", "queue_font_size", "queue_font_weight", "queue_font_style"):
                if key in style_payload:
                    next_style[key] = style_payload.get(key)

        def _apply() -> None:
            self._refresh_running = False
            changed = items != self.items or next_style != self.style
            self.items = list(items)
            self.style = dict(next_style)
            if changed:
                self._redraw()
            self.root.after(OVERLAY_REFRESH_MS, self._refresh_async)

        self.root.after(0, _apply)

    def _redraw(self) -> None:
        canvas = self.canvas
        width = max(1, canvas.winfo_width())
        height = max(1, canvas.winfo_height())
        if width <= 1 or height <= 1:
            return

        canvas.delete("all")
        canvas.create_rectangle(0, 0, width - 1, height - 1, outline="#7fa3b8", width=1)

        queue_font_size = _to_int(self.style.get("queue_font_size", 50), 50)
        queue_font_size = max(12, int(queue_font_size * self.scale / 100))
        queue_text_color = _safe_color(self.style.get("text_color", "#eaf6ff"), "#eaf6ff")
        queue_stroke_color = _safe_color(self.style.get("text_stroke_color", "#000000"), "#000000")
        stroke_enabled = _style_bool(self.style.get("text_stroke_enabled", True), True)
        text_font = (
            "Microsoft YaHei UI",
            queue_font_size,
            _font_style_spec(
                self.style.get("queue_font_weight", "700"),
                self.style.get("queue_font_style", "italic"),
            ),
        )
        stroke_radius = max(1, int(queue_font_size * 0.06))
        stroke_offsets = [
            (dx, dy)
            for dx in range(-stroke_radius, stroke_radius + 1)
            for dy in range(-stroke_radius, stroke_radius + 1)
            if dx != 0 or dy != 0
        ]

        y = 12
        max_text_width = max(80, width - 28)
        line_gap = max(2, int(queue_font_size * 0.16))
        for text in self.items:
            if stroke_enabled:
                for dx, dy in stroke_offsets:
                    canvas.create_text(
                        14 + dx,
                        y + dy,
                        anchor="nw",
                        text=text,
                        fill=queue_stroke_color,
                        font=text_font,
                        width=max_text_width,
                        justify="left",
                    )
            draw_id = canvas.create_text(
                14,
                y,
                anchor="nw",
                text=text,
                fill=queue_text_color,
                font=text_font,
                width=max_text_width,
                justify="left",
            )
            bbox = canvas.bbox(draw_id)
            if bbox is None:
                y += queue_font_size + line_gap + stroke_radius
            else:
                y = bbox[3] + line_gap + stroke_radius
            if y >= height - queue_font_size:
                break

    def run(self) -> None:
        self.root.mainloop()


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="弹幕排队姬透明窗口宿主")
    parser.add_argument("--port", type=int, default=9816)
    parser.add_argument("--width", type=int, default=400)
    parser.add_argument("--height", type=int, default=400)
    parser.add_argument("--scale", type=int, default=50)
    parser.add_argument("--no-topmost", action="store_true", default=False,
                        help="启动时不置顶（可从主控制台切换）")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv if argv is not None else sys.argv[1:])
    app = OverlayHostApp(
        port=max(1, int(args.port)),
        width=max(OVERLAY_MIN_WIDTH, int(args.width)),
        height=max(OVERLAY_MIN_HEIGHT, int(args.height)),
        scale=_clamp(int(args.scale), 40, 250),
        topmost=not args.no_topmost,
    )
    app.run()


if __name__ == "__main__":
    main()
