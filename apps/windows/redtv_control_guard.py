"""Windows Tk integration for the gated red-TV platform."""
from __future__ import annotations

import functools
import re
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()


def _promote_redtv_labels(module: Any) -> None:
    mapping = getattr(module, "PLATFORM_LABEL_TO_VALUE", None)
    if isinstance(mapping, dict):
        items = [(str(label), str(value)) for label, value in mapping.items() if str(value) != "youtube"]
        items.append(("红色小电视", "youtube"))
        module.PLATFORM_LABEL_TO_VALUE = dict(items)
        module.PLATFORM_VALUE_TO_LABEL = {value: key for key, value in module.PLATFORM_LABEL_TO_VALUE.items()}
        module.PLATFORM_LABELS = tuple(module.PLATFORM_LABEL_TO_VALUE.keys())

    reserved = getattr(module, "RESERVED_PLATFORM_KEYS", ())
    if "youtube" not in reserved:
        module.RESERVED_PLATFORM_KEYS = tuple(reserved) + ("youtube",)
    meta = getattr(module, "RESERVED_PLATFORM_UI_META", None)
    if isinstance(meta, dict):
        meta["youtube"] = {"title": "红色小电视", "hint": ""}


def _walk(widget: Any):
    yield widget
    try:
        children = list(widget.winfo_children())
    except Exception:
        children = []
    for child in children:
        yield from _walk(child)


def _configured_from_vars(panel: Any) -> bool:
    var_map = getattr(panel, "reserved_platform_vars", {}).get("youtube", {})
    try:
        target = str(var_map.get("room_url").get() or "").strip()
        if not target:
            target = str(var_map.get("room_id").get() or "").strip()
    except Exception:
        return False
    if not target:
        return False
    try:
        from core.youtube_protocol import extract_video_id
        extract_video_id(target)
        return True
    except Exception:
        return False


def _redtv_vars(panel: Any) -> dict[str, Any]:
    return getattr(panel, "reserved_platform_vars", {}).get("youtube", {})


def _build_redtv_frame(panel: Any, module: Any) -> None:
    frame_map = getattr(panel, "_platform_frame_map", None)
    if not isinstance(frame_map, dict) or "youtube" not in frame_map:
        return
    old = frame_map.get("youtube")
    parent = getattr(old, "master", None)
    if parent is None:
        return
    try:
        old.destroy()
    except Exception:
        pass

    frame = module.ttk.LabelFrame(parent, text="红色小电视", padding=10)
    frame.grid(row=0, column=0, sticky="ew")
    frame.columnconfigure(1, weight=1)
    panel._redtv_frame = frame

    gate = module.ttk.Frame(frame)
    gate.grid(row=0, column=0, columnspan=2, sticky="w")
    panel._redtv_gate_frame = gate
    button = module.ttk.Button(gate, text="尝试解锁", width=10)
    button.grid(row=0, column=0, sticky="w")
    panel._redtv_unlock_button = button
    denied = module.ttk.Label(gate, text="您无权访问")
    panel._redtv_denied_label = denied

    config = module.ttk.Frame(frame)
    config.columnconfigure(1, weight=1)
    panel._redtv_config_frame = config
    vars_map = _redtv_vars(panel)
    module.ttk.Label(config, text="直播链接").grid(row=0, column=0, sticky="w", pady=4)
    module.ttk.Entry(config, textvariable=vars_map.get("room_url"), width=64).grid(row=0, column=1, sticky="ew", pady=4)
    module.ttk.Label(config, text="Cookie（可选）").grid(row=1, column=0, sticky="w", pady=4)
    module.ttk.Entry(config, textvariable=vars_map.get("cookie"), width=64, show="●").grid(row=1, column=1, sticky="ew", pady=4)
    module.ttk.Label(
        config,
        text="支持直播聊天和聊天回放；保存有效直播链接后，下次启动会直接显示此配置。",
        wraplength=720,
    ).grid(row=2, column=0, columnspan=2, sticky="w", pady=(6, 0))

    def unlock() -> None:
        if bool(getattr(panel, "_redtv_unlock_busy", False)):
            return
        panel._redtv_unlock_busy = True
        try:
            button.configure(state="disabled", text="检测中…")
        except Exception:
            pass

        def worker() -> None:
            try:
                from core.youtube_protocol import probe_google_access
                allowed = bool(probe_google_access(timeout=4.0))
            except Exception:
                allowed = False

            def apply() -> None:
                panel._redtv_unlock_busy = False
                if allowed:
                    panel._redtv_session_unlocked = True
                    panel._redtv_access_denied = False
                else:
                    panel._redtv_session_unlocked = False
                    panel._redtv_access_denied = True
                _render_redtv_gate(panel)

            panel.root.after(0, apply)

        threading.Thread(target=worker, name="bilipdj-redtv-unlock", daemon=True).start()

    button.configure(command=unlock)
    frame_map["youtube"] = frame
    _render_redtv_gate(panel)
    try:
        panel._refresh_platform_settings_visibility()
    except Exception:
        pass


def _render_redtv_gate(panel: Any) -> None:
    gate = getattr(panel, "_redtv_gate_frame", None)
    button = getattr(panel, "_redtv_unlock_button", None)
    denied = getattr(panel, "_redtv_denied_label", None)
    config = getattr(panel, "_redtv_config_frame", None)
    if gate is None or button is None or denied is None or config is None:
        return

    configured = _configured_from_vars(panel)
    panel._redtv_configured = configured
    unlocked = configured or bool(getattr(panel, "_redtv_session_unlocked", False))
    denied_state = bool(getattr(panel, "_redtv_access_denied", False)) and not unlocked

    try:
        button.grid_remove()
        denied.grid_remove()
        config.grid_remove()
    except Exception:
        return

    if unlocked:
        config.grid(row=1, column=0, columnspan=2, sticky="ew")
    elif denied_state:
        denied.grid(row=0, column=0, sticky="w")
    else:
        try:
            button.configure(state="normal", text="尝试解锁")
        except Exception:
            pass
        button.grid(row=0, column=0, sticky="w")


def _normalize_redtv_for_save(panel: Any) -> None:
    try:
        current = panel._platform_label_to_value(panel.platform_var.get())
    except Exception:
        current = ""
    if current != "youtube":
        return

    configured = _configured_from_vars(panel)
    unlocked = bool(getattr(panel, "_redtv_session_unlocked", False))
    if not configured and not unlocked:
        raise ValueError("请先点击“尝试解锁”")

    vars_map = _redtv_vars(panel)
    room_url_var = vars_map.get("room_url")
    room_id_var = vars_map.get("room_id")
    enabled_var = vars_map.get("enabled")
    target = str(room_url_var.get() or "").strip() if room_url_var is not None else ""
    if not target and room_id_var is not None:
        target = str(room_id_var.get() or "").strip()
    if not target:
        raise ValueError("请填写红色小电视直播链接")

    try:
        from core.youtube_protocol import canonical_room_url, extract_video_id
        video_id = extract_video_id(target)
    except Exception as exc:
        raise ValueError(f"红色小电视直播链接无效：{exc}") from exc

    if room_id_var is not None:
        room_id_var.set(video_id)
    if room_url_var is not None:
        room_url_var.set(canonical_room_url(video_id))
    if enabled_var is not None:
        enabled_var.set(True)


def _find_stream_frame(panel: Any) -> Any | None:
    root = getattr(panel, "settings_notebook", None) or getattr(panel, "root", None)
    if root is None:
        return None
    for widget in _walk(root):
        try:
            if str(widget.cget("text") or "").strip() == "弹幕流":
                return widget
        except Exception:
            continue
    return None


def _install_active_checkbox(panel: Any, module: Any) -> None:
    if not _configured_from_vars(panel):
        return
    vars_map = getattr(panel, "_platform_vars", None)
    if not isinstance(vars_map, dict):
        return
    if "youtube" not in vars_map:
        vars_map["youtube"] = module.tk.BooleanVar(value=False)
    if bool(getattr(panel, "_redtv_active_platform_ui_installed", False)):
        return

    frame = _find_stream_frame(panel)
    if frame is not None:
        try:
            module.ttk.Checkbutton(frame, text="红色小电视（一个直播间）", variable=vars_map["youtube"]).grid(row=3, column=0, sticky="w", pady=5)
        except Exception:
            pass
        for widget in list(_walk(frame)):
            try:
                text = str(widget.cget("text") or "")
            except Exception:
                continue
            if "快手" in text and ("斗鱼" in text or "视频号" in text):
                try:
                    widget.grid_configure(row=4, column=0, sticky="w", pady=(8, 0))
                except Exception:
                    pass

    status_var = getattr(panel, "_platform_status_var", None)
    if status_var is not None:
        guard = {"busy": False}

        def sync_and_alias(*_args: Any) -> None:
            if guard["busy"]:
                return
            try:
                text = str(status_var.get() or "")
            except Exception:
                return
            match = re.search(r"已激活：([^；;]+)", text)
            if match:
                raw = match.group(1).strip()
                active = set() if raw in {"", "无"} else {item.strip().lower() for item in re.split(r"[,，+]+", raw) if item.strip()}
                try:
                    vars_map["youtube"].set("youtube" in active)
                except Exception:
                    pass
            aliased = re.sub(r"\byoutube\b", "红色小电视", text, flags=re.IGNORECASE)
            if aliased != text:
                guard["busy"] = True
                try:
                    status_var.set(aliased)
                finally:
                    guard["busy"] = False

        try:
            status_var.trace_add("write", sync_and_alias)
        except Exception:
            pass

    panel._redtv_active_platform_ui_installed = True


def patch_control_panel_redtv(panel_class: type[Any]) -> bool:
    if not isinstance(panel_class, type):
        return False
    module = __import__(str(panel_class.__module__), fromlist=["*"])

    with _PATCH_LOCK:
        if bool(getattr(panel_class, "_redtv_control_guard_installed", False)):
            return True

        _promote_redtv_labels(module)
        original_init = getattr(panel_class, "__init__", None)
        original_build_ui = getattr(panel_class, "_build_ui", None)
        original_set_payload = getattr(panel_class, "_set_platform_config_payload", None)
        original_refresh = getattr(panel_class, "_refresh_platform_settings_visibility", None)
        original_save = getattr(panel_class, "save_to_file", None)

        if not all(callable(item) for item in (original_init, original_build_ui, original_set_payload, original_refresh, original_save)):
            return False

        @functools.wraps(original_init)
        def init_with_redtv(self: Any, *args: Any, **kwargs: Any) -> Any:
            self._redtv_session_unlocked = False
            self._redtv_access_denied = False
            self._redtv_configured = False
            self._redtv_unlock_busy = False
            return original_init(self, *args, **kwargs)

        @functools.wraps(original_build_ui)
        def build_ui_with_redtv(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_build_ui(self, *args, **kwargs)
            _build_redtv_frame(self, module)
            return result

        @functools.wraps(original_set_payload)
        def set_payload_with_redtv(self: Any, payload: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_set_payload(self, payload, *args, **kwargs)
            self._redtv_configured = _configured_from_vars(self)
            _render_redtv_gate(self)
            if self._redtv_configured:
                _install_active_checkbox(self, module)
            return result

        @functools.wraps(original_refresh)
        def refresh_with_redtv(self: Any, *args: Any, **kwargs: Any) -> Any:
            result = original_refresh(self, *args, **kwargs)
            try:
                if self._platform_label_to_value(self.platform_var.get()) == "youtube":
                    _render_redtv_gate(self)
            except Exception:
                pass
            return result

        @functools.wraps(original_save)
        def save_with_redtv(self: Any, *args: Any, **kwargs: Any) -> Any:
            _normalize_redtv_for_save(self)
            result = original_save(self, *args, **kwargs)
            if result and _configured_from_vars(self):
                self._redtv_configured = True
                self._redtv_session_unlocked = True
                self._redtv_access_denied = False
                _render_redtv_gate(self)
                _install_active_checkbox(self, module)
            return result

        panel_class.__init__ = init_with_redtv
        panel_class._build_ui = build_ui_with_redtv
        panel_class._set_platform_config_payload = set_payload_with_redtv
        panel_class._refresh_platform_settings_visibility = refresh_with_redtv
        panel_class.save_to_file = save_with_redtv
        panel_class._redtv_control_guard_installed = True
        return True


__all__ = ["patch_control_panel_redtv"]
