from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from .version import APP_VERSION

ABOUT_TOOL_NAME = "Bilibili 直播弹幕排队管理工具"
ABOUT_ARCHITECTURE = "排队逻辑由 Python 后端统一处理，前端仅负责显示。"
ABOUT_FREE_NOTICE = "本软件完全免费，源码公开，Github Action自动打包，无后台无病毒，不损害电脑。若有人向你收费获取此软件（亲手帮安装调试除外），请立刻退款并举报！"
ABOUT_CIVIL = "• 民事责任：侵权方须停止侵权、赔偿损失（含维权合理费用）。"
ABOUT_CRIMINAL = "• 刑事责任：以营利为目的的侵权行为，情节严重时可能被追究刑事责任。"


def _build_about_tab(app: Any, frame: Any, module: Any) -> None:
    frame.columnconfigure(0, weight=1)

    module.ttk.Label(
        frame,
        text=f"{module.APP_NAME} 控制台",
        font=("Microsoft YaHei UI", 15, "bold") if sys.platform == "win32" else ("", 15, "bold"),
    ).grid(row=0, column=0, pady=(16, 6))

    module.ttk.Label(frame, text=f"当前版本号：{APP_VERSION}").grid(row=1, column=0, sticky="w", padx=20)
    module.ttk.Label(frame, text="您的版本是：Windows版").grid(row=2, column=0, sticky="w", padx=20, pady=(4, 0))
    module.ttk.Separator(frame, orient="horizontal").grid(row=3, column=0, sticky="ew", pady=12)

    module.ttk.Label(frame, text=ABOUT_TOOL_NAME).grid(row=4, column=0, sticky="w", padx=20)
    module.ttk.Label(frame, text=ABOUT_ARCHITECTURE).grid(row=5, column=0, sticky="w", padx=20, pady=(4, 0))

    module.ttk.Label(
        frame,
        text=ABOUT_FREE_NOTICE,
        wraplength=820,
        justify="left",
    ).grid(row=6, column=0, sticky="w", padx=20, pady=(16, 10))

    module.ttk.Label(
        frame,
        text="【侵权/倒卖责任】",
        foreground="#c00",
    ).grid(row=7, column=0, sticky="w", padx=20, pady=(4, 2))
    module.ttk.Label(
        frame,
        text=ABOUT_CIVIL,
        foreground="#c00",
        wraplength=820,
        justify="left",
    ).grid(row=8, column=0, sticky="w", padx=20, pady=2)
    module.ttk.Label(
        frame,
        text=ABOUT_CRIMINAL,
        foreground="#c00",
        wraplength=820,
        justify="left",
    ).grid(row=9, column=0, sticky="w", padx=20, pady=2)


def patch_control_panel_about(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])
    if Path(str(getattr(module, "__file__", ""))).name != "control_panel.py":
        return False

    def build_about_tab(panel: Any, frame: Any) -> None:
        _build_about_tab(panel, frame, module)

    setattr(panel_class, "_build_about_tab", build_about_tab)
    setattr(panel_class, "_bilipdj_about_page_unified", True)
    return True


__all__ = [
    "ABOUT_ARCHITECTURE",
    "ABOUT_CIVIL",
    "ABOUT_CRIMINAL",
    "ABOUT_FREE_NOTICE",
    "ABOUT_TOOL_NAME",
    "patch_control_panel_about",
]
