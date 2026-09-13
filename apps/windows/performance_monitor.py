from __future__ import annotations

import threading
from pathlib import Path
from typing import Any

MIN_REFRESH_SECONDS = 0.5
MAX_REFRESH_SECONDS = 60.0
DEFAULT_REFRESH_SECONDS = 2.0


def _directory_size(root: Path) -> int:
    total = 0
    try:
        paths = root.rglob("*")
    except OSError:
        return 0
    for path in paths:
        try:
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
        except OSError:
            continue
    return total


def _format_bytes(value: int) -> str:
    amount = float(max(0, value))
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if amount < 1024.0 or unit == "TB":
            return f"{amount:.1f} {unit}" if unit != "B" else f"{int(amount)} B"
        amount /= 1024.0
    return f"{amount:.1f} TB"


def _refresh_seconds(panel: Any) -> float:
    raw = str(getattr(panel, "perf_refresh_seconds_var", None).get() if hasattr(panel, "perf_refresh_seconds_var") else DEFAULT_REFRESH_SECONDS).strip()
    try:
        value = float(raw)
    except ValueError:
        value = DEFAULT_REFRESH_SECONDS
    value = min(MAX_REFRESH_SECONDS, max(MIN_REFRESH_SECONDS, value))
    normalized = f"{value:g}"
    if hasattr(panel, "perf_refresh_seconds_var") and panel.perf_refresh_seconds_var.get() != normalized:
        panel.perf_refresh_seconds_var.set(normalized)
    return value


def _cancel_scheduled(panel: Any) -> None:
    job = getattr(panel, "_perf_after_job", None)
    if job is None:
        return
    try:
        panel.root.after_cancel(job)
    except Exception:
        pass
    panel._perf_after_job = None


def _schedule_next(panel: Any) -> None:
    _cancel_scheduled(panel)
    if not bool(getattr(panel, "perf_monitor_enabled_var", None).get() if hasattr(panel, "perf_monitor_enabled_var") else False):
        return
    delay_ms = int(_refresh_seconds(panel) * 1000)
    panel._perf_after_job = panel.root.after(delay_ms, panel._refresh_perf)


def _refresh_perf(panel: Any) -> None:
    if bool(getattr(panel, "_perf_refresh_running", False)):
        return
    panel._perf_refresh_running = True
    enabled = bool(panel.perf_monitor_enabled_var.get()) if hasattr(panel, "perf_monitor_enabled_var") else False
    app_dir = Path(getattr(panel, "_perf_app_dir", Path.cwd())).resolve()
    need_directory = not bool(getattr(panel, "_perf_directory_loaded", False))

    def worker() -> None:
        values: dict[str, str] = {}
        errors: list[str] = []
        try:
            import psutil  # type: ignore[import-untyped]

            process = psutil.Process()
            values["mem"] = _format_bytes(int(process.memory_info().rss))
            if enabled:
                values["cpu"] = f"{psutil.cpu_percent(interval=0.2):.1f}%"
                vm = psutil.virtual_memory()
                values["sysmem"] = f"{vm.percent:.1f}% · {_format_bytes(int(vm.used))} / {_format_bytes(int(vm.total))}"
        except Exception as exc:  # noqa: BLE001
            errors.append(f"性能读取失败：{exc}")
            values.setdefault("mem", "读取失败")
            if enabled:
                values.setdefault("cpu", "读取失败")
                values.setdefault("sysmem", "读取失败")

        if need_directory:
            try:
                values["disk"] = _format_bytes(_directory_size(app_dir))
            except Exception as exc:  # noqa: BLE001
                errors.append(f"目录读取失败：{exc}")
                values["disk"] = "读取失败"

        def apply() -> None:
            panel._perf_refresh_running = False
            if need_directory:
                panel._perf_directory_loaded = True
            for key, value in values.items():
                var = getattr(panel, "_perf_vars", {}).get(key)
                if var is not None:
                    var.set(value)
            if not enabled:
                # Explicitly do not read CPU/system memory while monitoring is
                # disabled. Keep the UI clear about why no live value exists.
                if "cpu" in getattr(panel, "_perf_vars", {}):
                    panel._perf_vars["cpu"].set("监控已关闭")
                if "sysmem" in getattr(panel, "_perf_vars", {}):
                    panel._perf_vars["sysmem"].set("监控已关闭")
            status = getattr(panel, "perf_monitor_status_var", None)
            if status is not None:
                if errors:
                    status.set("；".join(errors))
                elif enabled:
                    status.set(f"性能监控已开启，每 {_refresh_seconds(panel):g} 秒刷新。")
                else:
                    status.set("性能监控已关闭；本次启动已读取进程内存和程序目录大小。")
            _schedule_next(panel)

        try:
            panel.root.after(0, apply)
        except Exception:
            panel._perf_refresh_running = False

    threading.Thread(target=worker, name="bilipdj-performance-monitor", daemon=True).start()


def _toggle_monitor(panel: Any) -> None:
    _cancel_scheduled(panel)
    if bool(panel.perf_monitor_enabled_var.get()):
        panel._refresh_perf()
    else:
        if "cpu" in getattr(panel, "_perf_vars", {}):
            panel._perf_vars["cpu"].set("监控已关闭")
        if "sysmem" in getattr(panel, "_perf_vars", {}):
            panel._perf_vars["sysmem"].set("监控已关闭")
        panel.perf_monitor_status_var.set("性能监控已关闭；不会持续读取 CPU 使用率和系统内存。")


def _build_perf_tab(panel: Any, frame: Any, module: Any) -> None:
    for child in frame.winfo_children():
        child.destroy()
    frame.columnconfigure(0, weight=1)
    frame.columnconfigure(1, weight=1)
    panel._perf_vars = {}
    panel._perf_refresh_running = False
    panel._perf_after_job = None
    panel._perf_directory_loaded = False
    panel._perf_app_dir = Path(getattr(module, "APP_DIR", Path.cwd())).resolve()
    panel.perf_monitor_enabled_var = module.tk.BooleanVar(value=False)
    panel.perf_refresh_seconds_var = module.tk.StringVar(value="2")
    panel.perf_monitor_status_var = module.tk.StringVar(value="性能监控默认关闭。")

    module.ttk.Label(frame, text="运行性能", font=("Microsoft YaHei UI", 15, "bold")).grid(
        row=0, column=0, columnspan=2, sticky="w", pady=(0, 3)
    )
    controls = module.ttk.Frame(frame)
    controls.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(2, 10))
    module.ttk.Checkbutton(
        controls,
        text="性能监控",
        variable=panel.perf_monitor_enabled_var,
        command=lambda: _toggle_monitor(panel),
    ).pack(side="left")
    module.ttk.Label(controls, text="自动刷新时间").pack(side="left", padx=(18, 6))
    refresh_entry = module.ttk.Entry(controls, textvariable=panel.perf_refresh_seconds_var, width=7)
    refresh_entry.pack(side="left")
    module.ttk.Label(controls, text="秒（0.5～60）").pack(side="left", padx=(4, 0))
    refresh_entry.bind("<Return>", lambda _event: (_refresh_seconds(panel), _schedule_next(panel)))
    refresh_entry.bind("<FocusOut>", lambda _event: _refresh_seconds(panel))

    module.ttk.Label(
        frame,
        text="关闭时仅在程序启动后读取一次本进程内存和程序目录大小，不读取 CPU 使用率与系统内存；开启后按设定周期刷新实时指标。",
        wraplength=800,
        justify="left",
    ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(0, 12))

    metrics = [
        ("cpu", "CPU 使用率", "当前系统 CPU 负载（仅开启监控时读取）"),
        ("mem", "本进程内存", "当前 Windows 控制台进程 RSS"),
        ("sysmem", "系统内存", "整机内存使用情况（仅开启监控时读取）"),
        ("disk", "程序目录", "BiliPDJ 程序目录当前实际文件大小"),
    ]
    for index, (key, title, hint) in enumerate(metrics):
        row = 3 + index // 2
        column = index % 2
        card = module.ttk.LabelFrame(frame, text=title, padding=(14, 10))
        card.grid(row=row, column=column, sticky="nsew", padx=(0 if column == 0 else 6, 6 if column == 0 else 0), pady=6)
        card.columnconfigure(0, weight=1)
        initial = "监控已关闭" if key in {"cpu", "sysmem"} else "读取中…"
        value_var = module.tk.StringVar(value=initial)
        panel._perf_vars[key] = value_var
        module.ttk.Label(card, textvariable=value_var, font=("Microsoft YaHei UI", 16, "bold")).grid(row=0, column=0, sticky="w")
        module.ttk.Label(card, text=hint, wraplength=350, justify="left").grid(row=1, column=0, sticky="w", pady=(5, 0))

    module.ttk.Label(frame, textvariable=panel.perf_monitor_status_var, wraplength=800).grid(
        row=5, column=0, columnspan=2, sticky="w", pady=(10, 0)
    )
    # Required startup snapshot even when monitoring remains off.
    panel.root.after(100, panel._refresh_perf)


def install_performance_monitor(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    if bool(getattr(panel_class, "_bilipdj_performance_monitor_installed", False)):
        return True
    module = __import__(str(panel_class.__module__), fromlist=["*"])

    def build_perf_tab(self: Any, frame: Any) -> None:
        _build_perf_tab(self, frame, module)

    def refresh_perf(self: Any) -> None:
        _refresh_perf(self)

    setattr(panel_class, "_build_perf_tab", build_perf_tab)
    setattr(panel_class, "_refresh_perf", refresh_perf)
    setattr(panel_class, "_bilipdj_performance_monitor_installed", True)
    return True


__all__ = [
    "DEFAULT_REFRESH_SECONDS",
    "MAX_REFRESH_SECONDS",
    "MIN_REFRESH_SECONDS",
    "install_performance_monitor",
]
