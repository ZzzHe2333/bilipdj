"""Minimal native Win32 splash: just "BiliPDJ 启动中" inside a rainbow orbit.

The splash has no Tk imports or child controls. Its own Win32 thread draws
a borderless, centered ring before heavy server and GUI modules are imported.
Each *completed* initialization milestone unlocks one more color; the orbit
keeps rotating while the main thread works.
"""
from __future__ import annotations

import math
import os
import threading
from typing import Sequence

_SUPPRESSED_OPTIONS = frozenset({
    "--backend", "--overlay-host", "--gui-startup-self-test",
    "--gui-close-self-test", "--plugin-runtime-self-test",
})

# Seven fixed orbit segments, unlocked left-to-right by real milestones.
RAINBOW_RGB = (
    (255, 73, 82),   # red
    (255, 158, 51),  # orange
    (255, 212, 75),  # yellow
    (79, 222, 143),  # green
    (72, 218, 244),  # cyan
    (91, 132, 255),  # blue
    (193, 105, 255), # violet
)
_TITLE = "BiliPDJ 启动中"
_SIZE = 290
_CENTER = _SIZE // 2
_RADIUS = 100
_BACKGROUND = (17, 19, 31)
_TRACK = (44, 47, 65)
_SW_SHOWNOACTIVATE = 4
_PM_REMOVE = 0x0001
_WS_POPUP = 0x80000000
_WS_EX_TOPMOST = 0x00000008
_WS_EX_TOOLWINDOW = 0x00000080
_DT_CENTER_VCENTER_SINGLELINE = 0x25
_SRCCOPY = 0x00CC0020


def should_show_startup_splash(argv: Sequence[str], *, frozen: bool, platform: str | None = None) -> bool:
    actual_platform = os.name if platform is None else platform
    return bool(frozen and actual_platform == "nt" and not _SUPPRESSED_OPTIONS.intersection(argv))


def _stage_colors(completed: int) -> tuple[tuple[int, int, int], ...]:
    """The first red arc is visible immediately; never show future colors."""
    return RAINBOW_RGB[:max(1, min(len(RAINBOW_RGB), int(completed)))]


def _colorref(rgb: tuple[int, int, int]) -> int:
    r, g, b = rgb
    return r | (g << 8) | (b << 16)


def _mix_color(rgb: tuple[int, int, int], amount: float) -> tuple[int, int, int]:
    return tuple(int(bg + (channel - bg) * amount) for bg, channel in zip(_BACKGROUND, rgb))


class NativeStartupSplash:
    def __init__(self, *, enabled: bool) -> None:
        self.enabled = bool(enabled)
        self._status = _TITLE  # compatibility for the existing bootstrap
        self._completed = 1
        self._painted_stage = 0
        self._stage_painted = threading.Event()
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._hwnd = 0

    def start(self) -> None:
        if not self.enabled or self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="bilipdj-rainbow-startup", daemon=True)
        self._thread.start()
        self._ready.wait(timeout=0.35)

    def update(self, text: str) -> None:
        """Keep the bootstrap API; phase descriptions are intentionally hidden."""
        if self.enabled and not self._stop.is_set():
            self._status = str(text or _TITLE)[:90]

    def advance(self, completed: int) -> None:
        """Advance only after a real boot phase succeeds (monotonic, 1..7)."""
        if self.enabled and not self._stop.is_set():
            self._completed = max(self._completed, min(7, max(1, int(completed))))
            self._stage_painted.clear()

    def wait_for_stage(self, completed: int, timeout: float = 0.16) -> bool:
        """Allow the last violet arc one rendered frame before hiding splash."""
        if not self.enabled:
            return True
        if self._painted_stage >= completed:
            return True
        self._stage_painted.wait(timeout=max(0.0, timeout))
        return self._painted_stage >= completed

    def close(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=0.7)

    def _run(self) -> None:
        """All HWND, HDC, GDI object ownership remains on this native thread."""
        hwnd = 0
        user32 = None
        try:
            import ctypes
            from ctypes import wintypes

            user32 = ctypes.WinDLL("user32", use_last_error=True)
            gdi32 = ctypes.WinDLL("gdi32", use_last_error=True)

            create = user32.CreateWindowExW
            create.argtypes = (
                wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPCWSTR,
                wintypes.DWORD, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                ctypes.c_int, wintypes.HWND, wintypes.HMENU,
                wintypes.HINSTANCE, ctypes.c_void_p,
            )
            create.restype = wintypes.HWND
            user32.IsWindow.argtypes = (wintypes.HWND,)
            user32.IsWindow.restype = wintypes.BOOL
            user32.PeekMessageW.argtypes = (
                ctypes.POINTER(wintypes.MSG), wintypes.HWND,
                wintypes.UINT, wintypes.UINT, wintypes.UINT,
            )
            user32.PeekMessageW.restype = wintypes.BOOL
            user32.GetDC.argtypes = (wintypes.HWND,)
            user32.GetDC.restype = wintypes.HDC
            user32.ReleaseDC.argtypes = (wintypes.HWND, wintypes.HDC)
            gdi32.CreateCompatibleDC.argtypes = (wintypes.HDC,)
            gdi32.CreateCompatibleDC.restype = wintypes.HDC
            gdi32.CreateCompatibleBitmap.argtypes = (wintypes.HDC, ctypes.c_int, ctypes.c_int)
            gdi32.CreateCompatibleBitmap.restype = wintypes.HBITMAP
            gdi32.SelectObject.argtypes = (wintypes.HDC, wintypes.HGDIOBJ)
            gdi32.SelectObject.restype = wintypes.HGDIOBJ
            gdi32.CreateSolidBrush.argtypes = (wintypes.DWORD,)
            gdi32.CreateSolidBrush.restype = wintypes.HBRUSH
            gdi32.CreatePen.argtypes = (ctypes.c_int, ctypes.c_int, wintypes.DWORD)
            gdi32.CreatePen.restype = wintypes.HPEN
            gdi32.MoveToEx.argtypes = (wintypes.HDC, ctypes.c_int, ctypes.c_int, ctypes.c_void_p)
            gdi32.LineTo.argtypes = (wintypes.HDC, ctypes.c_int, ctypes.c_int)
            gdi32.DeleteObject.argtypes = (wintypes.HGDIOBJ,)
            gdi32.DeleteDC.argtypes = (wintypes.HDC,)
            gdi32.BitBlt.argtypes = (
                wintypes.HDC, ctypes.c_int, ctypes.c_int,
                ctypes.c_int, ctypes.c_int, wintypes.HDC,
                ctypes.c_int, ctypes.c_int, wintypes.DWORD,
            )
            gdi32.SetTextColor.argtypes = (wintypes.HDC, wintypes.DWORD)
            gdi32.SetBkMode.argtypes = (wintypes.HDC, ctypes.c_int)
            user32.DrawTextW.argtypes = (
                wintypes.HDC, wintypes.LPCWSTR, ctypes.c_int,
                ctypes.POINTER(wintypes.RECT), wintypes.UINT,
            )
            gdi32.CreateFontW.restype = wintypes.HFONT
            gdi32.CreateFontW.argtypes = (
                ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD,
                wintypes.DWORD, wintypes.DWORD, wintypes.DWORD, wintypes.DWORD,
                wintypes.LPCWSTR,
            )
            user32.FillRect.argtypes = (wintypes.HDC, ctypes.POINTER(wintypes.RECT), wintypes.HBRUSH)

            x = max(0, (user32.GetSystemMetrics(0) - _SIZE) // 2)
            y = max(0, (user32.GetSystemMetrics(1) - _SIZE) // 2)
            hwnd = create(
                _WS_EX_TOPMOST | _WS_EX_TOOLWINDOW,
                "STATIC", "", _WS_POPUP,
                x, y, _SIZE, _SIZE, None, None, None, None,
            )
            if not hwnd:
                return
            self._hwnd = int(hwnd)
            # Expose only the dark circular orbit surface: no title bar,
            # window caption, buttons or secondary progress controls.
            gdi32.CreateEllipticRgn.argtypes = (ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int)
            gdi32.CreateEllipticRgn.restype = wintypes.HRGN
            user32.SetWindowRgn.argtypes = (wintypes.HWND, wintypes.HRGN, wintypes.BOOL)
            region = gdi32.CreateEllipticRgn(0, 0, _SIZE, _SIZE)
            if region and not user32.SetWindowRgn(hwnd, region, True):
                gdi32.DeleteObject(region)

            font = gdi32.CreateFontW(-23, 0, 0, 0, 650, 0, 0, 0, 1, 0, 0, 5, 0, "Segoe UI")
            text_rect = wintypes.RECT(38, _CENTER - 22, _SIZE - 38, _CENTER + 23)

            def draw_frame(rotation: float) -> bool:
                screen = user32.GetDC(hwnd)
                if not screen:
                    return False
                memory = gdi32.CreateCompatibleDC(screen)
                bitmap = gdi32.CreateCompatibleBitmap(screen, _SIZE, _SIZE) if memory else None
                if not memory or not bitmap:
                    if bitmap:
                        gdi32.DeleteObject(bitmap)
                    if memory:
                        gdi32.DeleteDC(memory)
                    user32.ReleaseDC(hwnd, screen)
                    return False
                old_bitmap = gdi32.SelectObject(memory, bitmap)
                bg = gdi32.CreateSolidBrush(_colorref(_BACKGROUND))
                try:
                    user32.FillRect(memory, ctypes.byref(wintypes.RECT(0, 0, _SIZE, _SIZE)), bg)

                    def stroke(color: tuple[int, int, int], width: int, angle: float) -> None:
                        pen = gdi32.CreatePen(0, width, _colorref(color))
                        if not pen:
                            return
                        previous = gdi32.SelectObject(memory, pen)
                        try:
                            for k in range(13):
                                theta = math.radians(angle + k * 3.1)
                                px = round(_CENTER + _RADIUS * math.cos(theta))
                                py = round(_CENTER + _RADIUS * math.sin(theta))
                                if k == 0:
                                    gdi32.MoveToEx(memory, px, py, None)
                                else:
                                    gdi32.LineTo(memory, px, py)
                        finally:
                            gdi32.SelectObject(memory, previous)
                            gdi32.DeleteObject(pen)

                    for index in range(7):
                        start = rotation + index * (360 / 7) - 90
                        stroke(_TRACK, 6, start)
                    for index, rgb in enumerate(_stage_colors(self._completed)):
                        start = rotation + index * (360 / 7) - 90
                        stroke(_mix_color(rgb, .24), 18, start)
                        stroke(_mix_color(rgb, .52), 12, start)
                        stroke(rgb, 7, start)

                    previous_font = gdi32.SelectObject(memory, font) if font else None
                    try:
                        gdi32.SetBkMode(memory, 1)  # transparent text background
                        gdi32.SetTextColor(memory, _colorref((246, 246, 252)))
                        user32.DrawTextW(memory, _TITLE, -1, ctypes.byref(text_rect),
                                         _DT_CENTER_VCENTER_SINGLELINE)
                    finally:
                        if previous_font:
                            gdi32.SelectObject(memory, previous_font)
                    gdi32.BitBlt(screen, 0, 0, _SIZE, _SIZE, memory, 0, 0, _SRCCOPY)
                    self._painted_stage = self._completed
                    self._stage_painted.set()
                    return True
                finally:
                    gdi32.DeleteObject(bg)
                    gdi32.SelectObject(memory, old_bitmap)
                    gdi32.DeleteObject(bitmap)
                    gdi32.DeleteDC(memory)
                    user32.ReleaseDC(hwnd, screen)

            user32.ShowWindow(hwnd, _SW_SHOWNOACTIVATE)
            if not draw_frame(0.0):
                return
            self._ready.set()
            rotation = 0.0
            msg = wintypes.MSG()
            while not self._stop.wait(0.040) and user32.IsWindow(hwnd):
                while user32.PeekMessageW(ctypes.byref(msg), None, 0, 0, _PM_REMOVE):
                    user32.TranslateMessage(ctypes.byref(msg))
                    user32.DispatchMessageW(ctypes.byref(msg))
                rotation = (rotation + 2.8) % 360
                draw_frame(rotation)
        except Exception:
            # A transient Win32 drawing failure must never prevent boot.
            return
        finally:
            self._ready.set()
            self._stage_painted.set()
            if hwnd and user32:
                try:
                    user32.DestroyWindow(hwnd)
                except Exception:
                    pass
            self._hwnd = 0


def open_startup_splash(argv: Sequence[str], *, frozen: bool) -> NativeStartupSplash:
    splash = NativeStartupSplash(enabled=should_show_startup_splash(argv, frozen=frozen))
    splash.start()
    return splash


__all__ = ["NativeStartupSplash", "open_startup_splash", "should_show_startup_splash", "RAINBOW_RGB"]
