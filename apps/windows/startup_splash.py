"""A minimal native Windows startup indicator, independent of tkinter.

It is created before importing the expensive server and control-panel modules.
A worker thread owns all HWNDs and its message loop; the main thread only
publishes text stages, avoiding cross-thread Tk use and UI freezes.
"""
from __future__ import annotations

import os
import threading
import time
from typing import Any, Sequence

_SUPPRESSED_OPTIONS = frozenset({
    "--backend",
    "--overlay-host",
    "--gui-startup-self-test",
    "--gui-close-self-test",
    "--plugin-runtime-self-test",
})

# Win32 styles/messages.  Use system window classes instead of registering
# custom Python callbacks that might be collected during interpreter shutdown.
_WS_POPUP = 0x80000000
_WS_CAPTION = 0x00C00000
_WS_BORDER = 0x00800000
_WS_CHILD = 0x40000000
_WS_VISIBLE = 0x10000000
_WS_EX_TOPMOST = 0x00000008
_WS_EX_TOOLWINDOW = 0x00000080
_PBS_MARQUEE = 0x00000008
_PBM_SETMARQUEE = 0x040A
_SW_SHOWNOACTIVATE = 4
_PM_REMOVE = 1

_WIDTH = 390
_HEIGHT = 148


def should_show_startup_splash(argv: Sequence[str], *, frozen: bool, platform: str | None = None) -> bool:
    """Show only for the actual packaged Windows desktop, never its children."""
    actual_platform = os.name if platform is None else platform
    return bool(frozen and actual_platform == "nt" and not _SUPPRESSED_OPTIONS.intersection(argv))


class NativeStartupSplash:
    def __init__(self, *, enabled: bool) -> None:
        self.enabled = bool(enabled)
        self._status = "正在启动弹幕排队姬…"
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._thread: threading.Thread | None = None
        self._hwnd = 0

    def start(self) -> None:
        if not self.enabled or self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run, name="bilipdj-native-startup-splash", daemon=True
        )
        self._thread.start()
        # Do not delay app initialization even if Windows is slow to create
        # the native window. The splash proceeds asynchronously.
        self._ready.wait(timeout=0.35)

    def update(self, text: str) -> None:
        if self.enabled and not self._stop.is_set():
            self._status = str(text or "正在启动…")[:90]

    def close(self) -> None:
        if self._stop.is_set():
            return
        self._stop.set()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=0.7)

    def _run(self) -> None:
        """All native windows are created, updated, and destroyed here."""
        hwnd = 0
        font = 0
        try:
            import ctypes
            from ctypes import wintypes

            user32 = ctypes.WinDLL("user32", use_last_error=True)
            gdi32 = ctypes.WinDLL("gdi32", use_last_error=True)
            comctl32 = ctypes.WinDLL("comctl32", use_last_error=True)

            create = user32.CreateWindowExW
            create.argtypes = (
                wintypes.DWORD, wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD,
                ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_int,
                wintypes.HWND, wintypes.HMENU, wintypes.HINSTANCE, ctypes.c_void_p,
            )
            create.restype = wintypes.HWND
            user32.SetWindowTextW.argtypes = (wintypes.HWND, wintypes.LPCWSTR)
            user32.SetWindowTextW.restype = wintypes.BOOL
            user32.SendMessageW.argtypes = (wintypes.HWND, wintypes.UINT, wintypes.WPARAM, wintypes.LPARAM)
            user32.SendMessageW.restype = wintypes.LPARAM
            user32.ShowWindow.argtypes = (wintypes.HWND, ctypes.c_int)
            user32.IsWindow.argtypes = (wintypes.HWND,)
            user32.IsWindow.restype = wintypes.BOOL
            user32.PeekMessageW.argtypes = (
                ctypes.POINTER(wintypes.MSG), wintypes.HWND, wintypes.UINT,
                wintypes.UINT, wintypes.UINT,
            )
            user32.PeekMessageW.restype = wintypes.BOOL

            comctl32.InitCommonControls()
            center_x = max(0, (int(user32.GetSystemMetrics(0)) - _WIDTH) // 2)
            center_y = max(0, (int(user32.GetSystemMetrics(1)) - _HEIGHT) // 2)
            hwnd = create(
                _WS_EX_TOPMOST | _WS_EX_TOOLWINDOW, "STATIC", "弹幕排队姬",
                _WS_POPUP | _WS_CAPTION | _WS_BORDER,
                center_x, center_y, _WIDTH, _HEIGHT, None, None, None, None,
            )
            if not hwnd:
                return
            self._hwnd = int(hwnd)
            title = create(0, "STATIC", "弹幕排队姬  ·  正在启动",
                           _WS_CHILD | _WS_VISIBLE, 20, 15, 350, 30,
                           hwnd, None, None, None)
            status = create(0, "STATIC", self._status,
                            _WS_CHILD | _WS_VISIBLE, 20, 58, 350, 26,
                            hwnd, None, None, None)
            progress = create(0, "msctls_progress32", "",
                              _WS_CHILD | _WS_VISIBLE | _PBS_MARQUEE,
                              20, 95, 348, 15, hwnd, None, None, None)
            if not all((title, status, progress)):
                return
            # Larger Segoe UI heading without extra Python/UI frameworks.
            create_font = gdi32.CreateFontW
            create_font.restype = wintypes.HFONT
            font = create_font(-21, 0, 0, 0, 600, 0, 0, 0, 1, 0, 0, 5, 0, "Segoe UI")
            if font:
                user32.SendMessageW(title, 0x0030, font, 1)  # WM_SETFONT
            user32.SendMessageW(progress, _PBM_SETMARQUEE, 1, 36)
            user32.ShowWindow(hwnd, _SW_SHOWNOACTIVATE)
            user32.UpdateWindow(hwnd)
            self._ready.set()
            displayed = self._status
            message = wintypes.MSG()
            while not self._stop.wait(0.055) and user32.IsWindow(hwnd):
                while user32.PeekMessageW(ctypes.byref(message), None, 0, 0, _PM_REMOVE):
                    user32.TranslateMessage(ctypes.byref(message))
                    user32.DispatchMessageW(ctypes.byref(message))
                if displayed != self._status:
                    displayed = self._status
                    user32.SetWindowTextW(status, displayed)
        except Exception:
            # An unavailable Win32 API must never prevent the app from starting.
            return
        finally:
            self._ready.set()
            if hwnd:
                try:
                    user32.DestroyWindow(hwnd)
                except Exception:
                    pass
            if font:
                try:
                    gdi32.DeleteObject(font)
                except Exception:
                    pass
            self._hwnd = 0


def open_startup_splash(argv: Sequence[str], *, frozen: bool) -> NativeStartupSplash:
    splash = NativeStartupSplash(enabled=should_show_startup_splash(argv, frozen=frozen))
    splash.start()
    return splash


__all__ = ["NativeStartupSplash", "open_startup_splash", "should_show_startup_splash"]
