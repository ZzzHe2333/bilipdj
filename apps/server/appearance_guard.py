from __future__ import annotations

import copy
import json
import re
import threading
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

APPEARANCE_SCHEMA = 1
PROFILE_KIND = "bilipdj-appearance-profile"
_HEX_RE = re.compile(r"^#[0-9a-fA-F]{6}$")
_PATCH_LOCK = threading.RLock()

DEFAULT_APPEARANCE: dict[str, Any] = {
    "schema": APPEARANCE_SCHEMA,
    "design": "aurora",
    "mode": "dark",
    "font_family": "Microsoft YaHei UI",
    "font_size": 10,
    "radius": 10,
    "dark": {
        "background": "#090E1A",
        "sidebar": "#0D1424",
        "surface": "#111A2C",
        "surface_alt": "#18233A",
        "input": "#0D1424",
        "border": "#26334D",
        "text": "#E6EDF7",
        "muted": "#8A9AB3",
        "accent": "#7C6CF2",
        "accent_hover": "#9184FF",
        "selection": "#7C6CF2",
        "success": "#32D583",
        "warning": "#F5B942",
        "danger": "#F97066",
    },
    "light": {
        "background": "#F4F6FB",
        "sidebar": "#EAEDF5",
        "surface": "#FFFFFF",
        "surface_alt": "#F0EFFF",
        "input": "#FBFBFE",
        "border": "#D5D9E7",
        "text": "#20263A",
        "muted": "#687089",
        "accent": "#6757D9",
        "accent_hover": "#5142BC",
        "selection": "#6757D9",
        "success": "#007A40",
        "warning": "#B57600",
        "danger": "#D92D20",
    },
}

PALETTE_KEYS = tuple(DEFAULT_APPEARANCE["dark"].keys())


def _appearance_path(server_module: Any) -> Path:
    return Path(getattr(server_module, "_YAML_DIR")) / "appearance.json"


def _atomic_write(server_module: Any, path: Path, text: str) -> None:
    writer = getattr(server_module, "_atomic_write_text", None)
    if callable(writer):
        writer(path, text, encoding="utf-8")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    temp.write_text(text, encoding="utf-8")
    temp.replace(path)


def _color(value: Any, fallback: str) -> str:
    text = str(value or "").strip()
    return text.upper() if _HEX_RE.fullmatch(text) else fallback.upper()


def _normalize_palette(raw: Any, defaults: dict[str, str]) -> dict[str, str]:
    incoming = raw if isinstance(raw, dict) else {}
    return {key: _color(incoming.get(key), defaults[key]) for key in PALETTE_KEYS}


def normalize_appearance(raw: Any) -> dict[str, Any]:
    incoming = raw if isinstance(raw, dict) else {}
    mode = str(incoming.get("mode", DEFAULT_APPEARANCE["mode"]) or "dark").strip().lower()
    if mode not in {"system", "light", "dark"}:
        mode = "dark"
    try:
        font_size = int(incoming.get("font_size", DEFAULT_APPEARANCE["font_size"]))
    except (TypeError, ValueError):
        font_size = int(DEFAULT_APPEARANCE["font_size"])
    try:
        radius = int(incoming.get("radius", DEFAULT_APPEARANCE["radius"]))
    except (TypeError, ValueError):
        radius = int(DEFAULT_APPEARANCE["radius"])
    font_family = str(incoming.get("font_family", DEFAULT_APPEARANCE["font_family"]) or "").strip()
    if not font_family:
        font_family = str(DEFAULT_APPEARANCE["font_family"])
    font_family = font_family[:80]
    return {
        "schema": APPEARANCE_SCHEMA,
        "design": "aurora",
        "mode": mode,
        "font_family": font_family,
        "font_size": max(8, min(20, font_size)),
        "radius": max(0, min(24, radius)),
        "dark": _normalize_palette(incoming.get("dark"), DEFAULT_APPEARANCE["dark"]),
        "light": _normalize_palette(incoming.get("light"), DEFAULT_APPEARANCE["light"]),
    }


def migrate_legacy_web_theme(raw: Any) -> dict[str, Any]:
    incoming = raw if isinstance(raw, dict) else {}
    result = copy.deepcopy(DEFAULT_APPEARANCE)
    mode = str(incoming.get("mode", "dark") or "dark").strip().lower()
    result["mode"] = mode if mode in {"system", "light", "dark"} else "dark"
    target = "light" if result["mode"] == "light" else "dark"
    palette = dict(result[target])
    mapping = {
        "accent": "accent",
        "bg": "background",
        "panel": "surface",
        "text": "text",
    }
    for old_key, new_key in mapping.items():
        value = str(incoming.get(old_key, "") or "").strip()
        if _HEX_RE.fullmatch(value):
            palette[new_key] = value.upper()
    result[target] = palette
    return normalize_appearance(result)


def load_appearance(server_module: Any) -> dict[str, Any]:
    path = _appearance_path(server_module)
    if path.is_file():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                return normalize_appearance(payload)
        except (OSError, json.JSONDecodeError):
            pass

    # Migrate the old Windows ui.theme choice when appearance.json does not yet exist.
    fallback = copy.deepcopy(DEFAULT_APPEARANCE)
    try:
        config = server_module.load_config()
        ui = config.get("ui", {}) if isinstance(config, dict) else {}
        old_mode = str(ui.get("theme", "") if isinstance(ui, dict) else "").strip().lower()
        if old_mode in {"light", "dark"}:
            fallback["mode"] = old_mode
    except Exception:
        pass
    return normalize_appearance(fallback)


def appearance_exists(server_module: Any) -> bool:
    return _appearance_path(server_module).is_file()


def save_appearance(server_module: Any, raw: Any) -> dict[str, Any]:
    normalized = normalize_appearance(raw)
    path = _appearance_path(server_module)
    _atomic_write(server_module, path, json.dumps(normalized, ensure_ascii=False, indent=2) + "\n")

    # Keep the old ui.theme field synchronized for older Windows builds.
    try:
        config = server_module.load_config()
        if isinstance(config, dict):
            ui = config.get("ui", {})
            ui = dict(ui) if isinstance(ui, dict) else {}
            legacy_mode = normalized["mode"]
            if legacy_mode == "system":
                legacy_mode = str(ui.get("theme", "dark") or "dark").lower()
                if legacy_mode not in {"light", "dark"}:
                    legacy_mode = "dark"
            ui["theme"] = legacy_mode
            config["ui"] = ui
            server_module.save_config(config)
    except Exception:
        pass
    return normalized


def build_profile(server_module: Any) -> dict[str, Any]:
    return {
        "schema": APPEARANCE_SCHEMA,
        "kind": PROFILE_KIND,
        "appearance": load_appearance(server_module),
        "display_style": server_module.load_style(),
    }


def import_profile(server_module: Any, raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise ValueError("配置必须是 JSON 对象")

    # Full cross-client profile.
    if raw.get("kind") == PROFILE_KIND or "appearance" in raw or "display_style" in raw:
        appearance = raw.get("appearance", load_appearance(server_module))
        saved = save_appearance(server_module, appearance)
        style = raw.get("display_style")
        if isinstance(style, dict):
            server_module.save_style(style)
        return {
            "schema": APPEARANCE_SCHEMA,
            "kind": PROFILE_KIND,
            "appearance": saved,
            "display_style": server_module.load_style(),
        }

    # Old Web localStorage theme payload.
    if any(key in raw for key in ("accent", "bg", "panel", "text")):
        saved = save_appearance(server_module, migrate_legacy_web_theme(raw))
        return {
            "schema": APPEARANCE_SCHEMA,
            "kind": PROFILE_KIND,
            "appearance": saved,
            "display_style": server_module.load_style(),
        }

    # Old style.json can still be imported through the same cross-client file dialog.
    if any(key in raw for key in ("bg1", "bg2", "bg3", "text_color", "queue_font_size")):
        server_module.save_style(raw)
        return build_profile(server_module)

    # Direct appearance.json is also accepted.
    if "dark" in raw or "light" in raw or "mode" in raw:
        saved = save_appearance(server_module, raw)
        return {
            "schema": APPEARANCE_SCHEMA,
            "kind": PROFILE_KIND,
            "appearance": saved,
            "display_style": server_module.load_style(),
        }
    raise ValueError("无法识别的主题/样式配置格式")


def _read_json_body(handler: Any) -> dict[str, Any]:
    try:
        length = int(handler.headers.get("Content-Length", "0") or 0)
    except (TypeError, ValueError):
        length = 0
    if length < 0 or length > 2 * 1024 * 1024:
        return {}
    raw = handler.rfile.read(length).decode("utf-8", errors="replace") if length else "{}"
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def install_appearance_guard(server_module: Any) -> bool:
    if server_module is None or not hasattr(server_module, "ApiHandler"):
        return False
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_appearance_guard_installed", False)):
            return True

        server_module.APPEARANCE_SCHEMA = APPEARANCE_SCHEMA
        server_module.DEFAULT_APPEARANCE = copy.deepcopy(DEFAULT_APPEARANCE)
        server_module.APPEARANCE_PATH = _appearance_path(server_module)
        server_module.load_appearance = lambda: load_appearance(server_module)
        server_module.save_appearance = lambda payload: save_appearance(server_module, payload)
        server_module.build_appearance_profile = lambda: build_profile(server_module)
        server_module.import_appearance_profile = lambda payload: import_profile(server_module, payload)

        handler_class = server_module.ApiHandler
        original_get = handler_class.do_GET
        original_post = handler_class.do_POST

        def do_GET(self: Any) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/api/appearance":
                if not self._require_loopback():
                    return
                self._write_json({
                    "status": "ok",
                    "exists": appearance_exists(server_module),
                    "appearance": load_appearance(server_module),
                })
                return
            if path == "/api/appearance/profile":
                if not self._require_loopback():
                    return
                self._write_json(build_profile(server_module))
                return
            return original_get(self)

        def do_POST(self: Any) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path not in {"/api/appearance", "/api/appearance/profile"}:
                return original_post(self)
            if not self._require_loopback():
                return
            payload = _read_json_body(self)
            try:
                if path == "/api/appearance":
                    raw = payload.get("appearance", payload)
                    appearance = save_appearance(server_module, raw)
                    self._write_json({"status": "ok", "appearance": appearance})
                else:
                    profile = import_profile(server_module, payload)
                    self._write_json({"status": "ok", **profile})
            except ValueError as exc:
                self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)
            except Exception as exc:  # noqa: BLE001
                self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.INTERNAL_SERVER_ERROR)

        handler_class.do_GET = do_GET
        handler_class.do_POST = do_POST
        server_module._appearance_guard_installed = True
        return True


__all__ = [
    "APPEARANCE_SCHEMA",
    "DEFAULT_APPEARANCE",
    "PROFILE_KIND",
    "build_profile",
    "import_profile",
    "install_appearance_guard",
    "load_appearance",
    "normalize_appearance",
    "save_appearance",
]
