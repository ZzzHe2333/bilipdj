"""Red TV runtime integration for the existing multi-platform backend."""
from __future__ import annotations

import copy
import threading
from http import HTTPStatus
from pathlib import Path
from typing import Any

from . import youtube_protocol

_PATCH_LOCK = threading.RLock()


def _append_unique(values: Any, item: str) -> tuple[str, ...]:
    result: list[str] = []
    for value in values if isinstance(values, (list, tuple)) else ():
        text = str(value or "").strip().lower()
        if text and text not in result:
            result.append(text)
    if item not in result:
        result.append(item)
    return tuple(result)


def _install_relay_factory(server_module: Any) -> None:
    original = getattr(server_module, "_create_danmu_relay", None)
    if not callable(original) or bool(getattr(original, "_bilipdj_redtv_wrapped", False)):
        return

    def create_danmu_relay_with_redtv(server: Any) -> Any:
        try:
            platform = str(
                server_module._get_runtime_platform(getattr(server, "runtime_config", {})) or ""
            ).strip().lower()
        except Exception:
            platform = str(getattr(server, "runtime_config", {}).get("platform", "") or "").strip().lower()
        if platform == "youtube":
            return youtube_protocol.YoutubeDanmuRelay(server)
        return original(server)

    create_danmu_relay_with_redtv._bilipdj_redtv_wrapped = True  # type: ignore[attr-defined]
    server_module._create_danmu_relay = create_danmu_relay_with_redtv


def _patch_issue79(issue79_module: Any) -> None:
    if issue79_module is None:
        return

    issue79_module.SUPPORTED_ACTIVE_PLATFORMS = _append_unique(
        getattr(issue79_module, "SUPPORTED_ACTIVE_PLATFORMS", ("bilibili", "douyin")),
        "youtube",
    )

    original_runtime = getattr(issue79_module, "_runtime_for_platform", None)
    if callable(original_runtime) and not bool(getattr(original_runtime, "_bilipdj_redtv_wrapped", False)):
        def runtime_for_platform_with_redtv(server: Any, platform: str) -> dict[str, Any]:
            runtime = original_runtime(server, platform)
            if str(platform).strip().lower() == "youtube":
                section = dict(runtime.get("youtube", {}) or {})
                section["enabled"] = True
                runtime["youtube"] = section
            return runtime

        runtime_for_platform_with_redtv._bilipdj_redtv_wrapped = True  # type: ignore[attr-defined]
        issue79_module._runtime_for_platform = runtime_for_platform_with_redtv

    original_writer = getattr(issue79_module, "_write_control_html", None)
    if callable(original_writer) and not bool(getattr(original_writer, "_bilipdj_redtv_wrapped", False)):
        def write_control_html_with_redtv(self: Any, server_module: Any) -> None:
            if not self._require_loopback():
                return
            file_path = Path(server_module.UI_DIR) / "control.html"
            if not file_path.is_file():
                self._write_json({"status": "error", "message": "Web control panel is missing"}, status=404)
                return
            text = file_path.read_text(encoding="utf-8")
            css_tag = '<link rel="stylesheet" href="/control_issue79.css">'
            issue_js_tag = '<script src="/control_issue79.js"></script>'
            huya_js_tag = '<script src="/control_huya.js"></script>'
            redtv_js_tag = '<script src="/control_redtv.js"></script>'
            if css_tag not in text:
                text = text.replace("</head>", f"  {css_tag}\n</head>")
            if issue_js_tag not in text:
                text = text.replace("</body>", f"  {issue_js_tag}\n</body>")
            if (Path(server_module.UI_DIR) / "control_huya.js").is_file() and huya_js_tag not in text:
                text = text.replace("</body>", f"  {huya_js_tag}\n</body>")
            if redtv_js_tag not in text:
                text = text.replace("</body>", f"  {redtv_js_tag}\n</body>")
            body = text.encode("utf-8")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        write_control_html_with_redtv._bilipdj_redtv_wrapped = True  # type: ignore[attr-defined]
        issue79_module._write_control_html = write_control_html_with_redtv


def install_youtube_runtime_guard(server_module: Any, issue79_module: Any | None = None) -> bool:
    if server_module is None:
        return False

    with _PATCH_LOCK:
        if bool(getattr(server_module, "_redtv_runtime_guard_installed", False)):
            return True

        server_module.SUPPORTED_RUNTIME_PLATFORMS = _append_unique(
            getattr(server_module, "SUPPORTED_RUNTIME_PLATFORMS", ("bilibili", "douyin")),
            "youtube",
        )
        server_module.RESERVED_RUNTIME_PLATFORMS = _append_unique(
            getattr(server_module, "RESERVED_RUNTIME_PLATFORMS", ()),
            "youtube",
        )
        all_platforms = list(getattr(server_module, "SUPPORTED_RUNTIME_PLATFORMS", ()))
        all_platforms.extend(getattr(server_module, "RESERVED_RUNTIME_PLATFORMS", ()))
        server_module.ALL_RUNTIME_PLATFORMS = tuple(dict.fromkeys(str(value) for value in all_platforms))

        display_names = getattr(server_module, "PLATFORM_DISPLAY_NAMES", None)
        if isinstance(display_names, dict):
            display_names["youtube"] = "红色小电视"

        defaults = getattr(server_module, "DEFAULT_CONFIG", None)
        section_default = getattr(server_module, "RESERVED_PLATFORM_SECTION_DEFAULT", {})
        if isinstance(defaults, dict) and "youtube" not in defaults:
            defaults["youtube"] = copy.deepcopy(section_default if isinstance(section_default, dict) else {})

        _install_relay_factory(server_module)
        _patch_issue79(issue79_module)

        server_module.youtube_protocol = youtube_protocol
        server_module.YoutubeDanmuRelay = youtube_protocol.YoutubeDanmuRelay
        server_module._redtv_runtime_guard_installed = True
        return True


__all__ = ["install_youtube_runtime_guard"]
