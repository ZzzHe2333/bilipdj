from __future__ import annotations

import functools
import threading
from typing import Any

_PATCH_LOCK = threading.RLock()


def _iter_widgets(widget: Any):
    yield widget
    try:
        children = list(widget.winfo_children())
    except Exception:
        children = []
    for child in children:
        yield from _iter_widgets(child)


def _translate_text(widget: Any, translations: dict[str, str]) -> None:
    try:
        current = str(widget.cget("text") or "")
    except Exception:
        return
    if not current:
        return
    source = getattr(widget, "_bilipdj_i18n_source_text", None)
    last = getattr(widget, "_bilipdj_i18n_last_text", None)
    if source is None or (last is not None and current != last):
        source = current
        try:
            setattr(widget, "_bilipdj_i18n_source_text", source)
        except Exception:
            pass
    translated = str(translations.get(str(source), source))
    if current != translated:
        try:
            widget.configure(text=translated)
        except Exception:
            return
    try:
        setattr(widget, "_bilipdj_i18n_last_text", translated)
    except Exception:
        pass


def _translate_notebook(widget: Any, translations: dict[str, str]) -> None:
    try:
        tabs = list(widget.tabs())
    except Exception:
        return
    sources = getattr(widget, "_bilipdj_i18n_tab_sources", {})
    lasts = getattr(widget, "_bilipdj_i18n_tab_lasts", {})
    if not isinstance(sources, dict):
        sources = {}
    if not isinstance(lasts, dict):
        lasts = {}
    for tab_id in tabs:
        try:
            current = str(widget.tab(tab_id, "text") or "")
        except Exception:
            continue
        key = str(tab_id)
        if key not in sources or (key in lasts and current != lasts[key]):
            sources[key] = current
        translated = str(translations.get(sources[key], sources[key]))
        if current != translated:
            try:
                widget.tab(tab_id, text=translated)
            except Exception:
                pass
        lasts[key] = translated
    widget._bilipdj_i18n_tab_sources = sources
    widget._bilipdj_i18n_tab_lasts = lasts


def _translate_tree_headings(widget: Any, translations: dict[str, str]) -> None:
    try:
        columns = ["#0", *list(widget.cget("columns"))]
    except Exception:
        return
    sources = getattr(widget, "_bilipdj_i18n_heading_sources", {})
    lasts = getattr(widget, "_bilipdj_i18n_heading_lasts", {})
    if not isinstance(sources, dict):
        sources = {}
    if not isinstance(lasts, dict):
        lasts = {}
    for column in columns:
        try:
            current = str(widget.heading(column).get("text", "") or "")
        except Exception:
            continue
        key = str(column)
        if key not in sources or (key in lasts and current != lasts[key]):
            sources[key] = current
        translated = str(translations.get(sources[key], sources[key]))
        if current != translated:
            try:
                widget.heading(column, text=translated)
            except Exception:
                pass
        lasts[key] = translated
    widget._bilipdj_i18n_heading_sources = sources
    widget._bilipdj_i18n_heading_lasts = lasts


def apply_translations(root: Any, translations: dict[str, str]) -> None:
    mapping = translations if isinstance(translations, dict) else {}
    for widget in _iter_widgets(root):
        _translate_text(widget, mapping)
        _translate_notebook(widget, mapping)
        _translate_tree_headings(widget, mapping)


def _plugin_frame(panel: Any) -> Any | None:
    notebook = getattr(panel, "settings_notebook", None)
    if notebook is None:
        return None
    try:
        for tab_id in notebook.tabs():
            text = str(notebook.tab(tab_id, "text") or "")
            if text == "插件管理" or getattr(notebook.nametowidget(tab_id), "_bilipdj_language_panel", False):
                return notebook.nametowidget(tab_id)
    except Exception:
        return None
    return None


def install_language_ui() -> bool:
    """Add language selection to plugin manager and translate current Tk text."""
    from . import windows_ui

    with _PATCH_LOCK:
        current = getattr(windows_ui, "_build_plugin_manager_tab", None)
        if not callable(current):
            return False
        if bool(getattr(current, "_issue250_language_ui", False)):
            return True

        @functools.wraps(current)
        def build_plugin_manager_tab(panel: Any, module: Any) -> None:
            current(panel, module)
            frame = _plugin_frame(panel)
            if frame is None or bool(getattr(panel, "_language_ui_installed", False)):
                return
            frame._bilipdj_language_panel = True
            section = module.ttk.LabelFrame(frame, text="界面语言", padding=10)
            section.grid(row=6, column=0, sticky="ew", pady=(12, 0))
            section.columnconfigure(1, weight=1)
            module.ttk.Label(section, text="语言").grid(row=0, column=0, sticky="w", padx=(0, 8))
            language_var = module.tk.StringVar(value="简体中文 (zh-CN)")
            language_box = module.ttk.Combobox(section, textvariable=language_var, state="readonly", width=34)
            language_box.grid(row=0, column=1, sticky="ew")
            refresh_button = module.ttk.Button(section, text="刷新语言", width=10)
            refresh_button.grid(row=0, column=2, sticky="e", padx=(8, 0))
            status_var = module.tk.StringVar(value="语言插件使用当前中文文字作为翻译 key；缺失内容自动回退中文。")
            module.ttk.Label(section, textvariable=status_var, wraplength=760, justify="left").grid(
                row=1, column=0, columnspan=3, sticky="w", pady=(7, 0)
            )

            panel._language_ui_installed = True
            panel._language_translations = {}
            panel._language_choices = {"简体中文 (zh-CN)": "zh-CN"}
            panel._language_active = "zh-CN"
            panel._language_var = language_var
            panel._language_box = language_box
            panel._language_status_var = status_var

            def apply_current() -> None:
                try:
                    apply_translations(panel.root, dict(getattr(panel, "_language_translations", {}) or {}))
                except Exception:
                    pass

            def install_periodic_refresh() -> None:
                if not bool(getattr(panel, "_language_ui_installed", False)):
                    return
                apply_current()
                try:
                    panel._language_periodic_job = panel.root.after(1500, install_periodic_refresh)
                except Exception:
                    pass

            def render(payload: dict[str, Any]) -> None:
                languages = payload.get("languages", [])
                choices: dict[str, str] = {}
                code_to_label: dict[str, str] = {}
                if isinstance(languages, list):
                    for item in languages:
                        if not isinstance(item, dict):
                            continue
                        code = str(item.get("code", "") or "")
                        name = str(item.get("name", code) or code)
                        if not code:
                            continue
                        label = f"{name} ({code})"
                        choices[label] = code
                        code_to_label[code] = label
                if "zh-CN" not in code_to_label:
                    choices = {"简体中文 (zh-CN)": "zh-CN", **choices}
                    code_to_label["zh-CN"] = "简体中文 (zh-CN)"
                panel._language_choices = choices
                panel._language_active = str(payload.get("active", "zh-CN") or "zh-CN")
                translations = payload.get("translations", {})
                panel._language_translations = dict(translations) if isinstance(translations, dict) else {}
                language_box.configure(values=tuple(choices.keys()))
                language_var.set(code_to_label.get(panel._language_active, code_to_label["zh-CN"]))
                status_var.set(
                    f"当前语言：{panel._language_active} · 可用语言：{len(choices)} · "
                    f"翻译条目：{len(panel._language_translations)}"
                )
                apply_current()

            def load() -> None:
                status_var.set("正在读取语言插件……")

                def worker() -> None:
                    try:
                        payload = windows_ui._plugin_request(panel, "/api/language", timeout=8.0)
                    except Exception as exc:  # noqa: BLE001
                        panel.root.after(0, lambda error=str(exc): status_var.set(f"读取语言失败：{error}"))
                        return
                    panel.root.after(0, lambda: render(payload))

                threading.Thread(target=worker, name="bilipdj-language-load", daemon=True).start()

            def change(_event: Any | None = None) -> None:
                code = panel._language_choices.get(str(language_var.get()), "zh-CN")
                status_var.set(f"正在切换到 {code}……")

                def worker() -> None:
                    try:
                        payload = windows_ui._plugin_request(
                            panel,
                            "/api/language",
                            method="POST",
                            payload={"language": code},
                            timeout=8.0,
                        )
                    except Exception as exc:  # noqa: BLE001
                        panel.root.after(0, lambda error=str(exc): status_var.set(f"切换语言失败：{error}"))
                        return
                    panel.root.after(0, lambda: render(payload))

                threading.Thread(target=worker, name="bilipdj-language-switch", daemon=True).start()

            language_box.bind("<<ComboboxSelected>>", change)
            refresh_button.configure(command=load)
            panel._language_refresh = load
            panel.root.after(650, load)
            panel.root.after(1800, install_periodic_refresh)

        setattr(build_plugin_manager_tab, "_issue250_language_ui", True)
        windows_ui._build_plugin_manager_tab = build_plugin_manager_tab
        return True


__all__ = ["apply_translations", "install_language_ui"]
