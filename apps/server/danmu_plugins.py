"""Unified danmu-source plugin registry for BiliPDJ.

Issue #128 introduces Plugin API v1. Existing platform protocol modules are
registered as built-in danmu acquisition plugins and Relay creation is routed
through one registry instead of a chain of platform-specific wrappers.
"""
from __future__ import annotations

import copy
import functools
import threading
from dataclasses import dataclass, field
from http import HTTPStatus
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

PLUGIN_API_VERSION = 1
PLUGIN_TYPE = "danmu_source"
_PATCH_LOCK = threading.RLock()

RelayFactory = Callable[[Any], Any]


@dataclass(frozen=True)
class DanmuPlugin:
    plugin_id: str
    platform: str
    name: str
    relay_factory: RelayFactory = field(repr=False, compare=False)
    version: str = "builtin"
    plugin_api: int = PLUGIN_API_VERSION
    plugin_type: str = PLUGIN_TYPE
    source: str = "builtin"
    config_section: str = ""
    capabilities: tuple[str, ...] = ("danmu",)
    available: bool = True

    def public_info(self) -> dict[str, Any]:
        return {
            "id": self.plugin_id,
            "platform": self.platform,
            "name": self.name,
            "version": self.version,
            "plugin_api": self.plugin_api,
            "type": self.plugin_type,
            "source": self.source,
            "config_section": self.config_section or self.platform,
            "capabilities": list(self.capabilities),
            "available": bool(self.available),
        }


class DanmuPluginRegistry:
    """Thread-safe registry for danmu acquisition plugins."""

    def __init__(self) -> None:
        self._by_platform: dict[str, DanmuPlugin] = {}
        self._by_id: dict[str, DanmuPlugin] = {}
        self._lock = threading.RLock()

    def register(self, plugin: DanmuPlugin, *, replace: bool = False) -> DanmuPlugin:
        platform = str(plugin.platform or "").strip().lower()
        plugin_id = str(plugin.plugin_id or "").strip().lower()
        if not platform or not plugin_id:
            raise ValueError("plugin_id and platform are required")
        if int(plugin.plugin_api) != PLUGIN_API_VERSION:
            raise ValueError(
                f"unsupported plugin API {plugin.plugin_api}; expected {PLUGIN_API_VERSION}"
            )
        with self._lock:
            existing_platform = self._by_platform.get(platform)
            existing_id = self._by_id.get(plugin_id)
            if not replace and (existing_platform is not None or existing_id is not None):
                raise ValueError(f"danmu plugin already registered: {plugin_id}/{platform}")
            if existing_platform is not None:
                self._by_id.pop(existing_platform.plugin_id.lower(), None)
            if existing_id is not None:
                self._by_platform.pop(existing_id.platform.lower(), None)
            self._by_platform[platform] = plugin
            self._by_id[plugin_id] = plugin
        return plugin

    def get(self, platform_or_id: str) -> DanmuPlugin | None:
        key = str(platform_or_id or "").strip().lower()
        with self._lock:
            return self._by_platform.get(key) or self._by_id.get(key)

    def platform_ids(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(self._by_platform.keys())

    def list_plugins(self) -> list[DanmuPlugin]:
        with self._lock:
            return list(self._by_platform.values())

    def list_public(self) -> list[dict[str, Any]]:
        return [plugin.public_info() for plugin in self.list_plugins()]

    def create_relay(self, platform: str, server: Any) -> Any:
        plugin = self.get(platform)
        if plugin is None or not plugin.available:
            raise ValueError(f"unsupported danmu plugin platform: {platform}")
        return plugin.relay_factory(server)


def _class_factory(relay_class: type[Any]) -> RelayFactory:
    def create(server: Any) -> Any:
        return relay_class(server)

    create.__name__ = f"create_{relay_class.__name__}"
    return create


def _relay_class(server_module: Any, *candidates: str) -> type[Any] | None:
    for candidate in candidates:
        value = getattr(server_module, candidate, None)
        if isinstance(value, type):
            return value
    return None


def _protocol_relay_class(server_module: Any, module_name: str, class_name: str) -> type[Any] | None:
    module = getattr(server_module, module_name, None)
    value = getattr(module, class_name, None) if module is not None else None
    return value if isinstance(value, type) else None


def _register_current_builtin_plugins(server_module: Any, registry: DanmuPluginRegistry) -> None:
    specs: list[tuple[str, str, str, type[Any] | None, tuple[str, ...]]] = [
        (
            "bilibili",
            "builtin.bilibili.danmu",
            "Bilibili 获取弹幕插件",
            _protocol_relay_class(server_module, "bilibili_protocol", "BilibiliDanmuRelay"),
            ("danmu", "gift"),
        ),
        (
            "douyin",
            "builtin.douyin.danmu",
            "抖音获取弹幕插件",
            _protocol_relay_class(server_module, "douyin_protocol", "DouyinDanmuRelay"),
            ("danmu", "gift"),
        ),
        (
            "huya",
            "builtin.huya.danmu",
            "虎牙获取弹幕插件",
            _relay_class(server_module, "HuyaDanmuRelay"),
            ("danmu",),
        ),
        (
            "youtube",
            "builtin.youtube.danmu",
            "YouTube 获取弹幕插件",
            _relay_class(server_module, "YoutubeDanmuRelay"),
            ("danmu",),
        ),
        (
            "twitch",
            "builtin.twitch.danmu",
            "Twitch 获取弹幕插件",
            _relay_class(server_module, "TwitchDanmuRelay"),
            ("danmu",),
        ),
    ]
    for platform, plugin_id, name, relay_class, capabilities in specs:
        if relay_class is None:
            continue
        registry.register(
            DanmuPlugin(
                plugin_id=plugin_id,
                platform=platform,
                name=name,
                relay_factory=_class_factory(relay_class),
                config_section=platform,
                capabilities=capabilities,
            ),
            replace=True,
        )


def _sync_legacy_platform_metadata(server_module: Any, issue79_module: Any, registry: DanmuPluginRegistry) -> None:
    platforms = registry.platform_ids()
    server_module.SUPPORTED_RUNTIME_PLATFORMS = platforms
    reserved = tuple(
        str(value).strip().lower()
        for value in getattr(server_module, "RESERVED_RUNTIME_PLATFORMS", ())
        if str(value).strip().lower() not in platforms
    )
    server_module.RESERVED_RUNTIME_PLATFORMS = reserved
    server_module.ALL_RUNTIME_PLATFORMS = tuple(dict.fromkeys((*platforms, *reserved)))

    display_names = getattr(server_module, "PLATFORM_DISPLAY_NAMES", None)
    if isinstance(display_names, dict):
        for plugin in registry.list_plugins():
            display_names[plugin.platform] = plugin.name

    if issue79_module is not None:
        # Compatibility alias for older helpers. The authoritative source is the registry.
        issue79_module.SUPPORTED_ACTIVE_PLATFORMS = platforms


def _install_registry_factory(server_module: Any, registry: DanmuPluginRegistry) -> None:
    def create_danmu_relay_from_plugin(server: Any) -> Any:
        try:
            platform = str(
                server_module._get_runtime_platform(getattr(server, "runtime_config", {})) or ""
            ).strip().lower()
        except Exception:
            platform = str(getattr(server, "runtime_config", {}).get("platform", "") or "").strip().lower()
        if not platform:
            platform = str(getattr(server_module, "DEFAULT_PLATFORM", "bilibili") or "bilibili")
        return registry.create_relay(platform, server)

    create_danmu_relay_from_plugin._bilipdj_plugin_registry_factory = True  # type: ignore[attr-defined]
    server_module._create_danmu_relay = create_danmu_relay_from_plugin


def _install_issue79_plugin_bridge(server_module: Any, issue79_module: Any, registry: DanmuPluginRegistry) -> None:
    if issue79_module is None:
        return

    def normalize_active_platforms(_module: Any, config: Any) -> tuple[str, ...]:
        cfg = config if isinstance(config, dict) else {}
        supported = set(registry.platform_ids())
        raw = cfg.get("active_platforms")
        if isinstance(raw, (list, tuple)):
            result: list[str] = []
            for value in raw:
                name = str(value or "").strip().lower()
                if name in supported and name not in result:
                    result.append(name)
            return tuple(result)
        try:
            legacy = str(server_module._get_runtime_platform(cfg) or "").strip().lower()
        except Exception:
            legacy = str(cfg.get("platform", "") or "").strip().lower()
        if legacy in supported:
            return (legacy,)
        default = str(getattr(server_module, "DEFAULT_PLATFORM", "bilibili") or "bilibili").strip().lower()
        return (default,) if default in supported else tuple(registry.platform_ids()[:1])

    def runtime_for_platform(server: Any, platform: str) -> dict[str, Any]:
        runtime = copy.deepcopy(getattr(server, "runtime_config", {}) or {})
        platform_key = str(platform or "").strip().lower()
        runtime["platform"] = platform_key
        plugin = registry.get(platform_key)
        section_key = plugin.config_section if plugin is not None else platform_key
        if section_key:
            section = runtime.get(section_key)
            if isinstance(section, dict):
                section = dict(section)
                section["enabled"] = True
                runtime[section_key] = section
            elif platform_key != "bilibili":
                runtime[section_key] = {"enabled": True}
        return runtime

    def platform_payload(_server_module: Any, server: Any) -> dict[str, Any]:
        active = normalize_active_platforms(server_module, getattr(server, "runtime_config", {}))
        relay = getattr(server, "danmu_relay", None)
        runtime = relay.get_runtime_status() if relay is not None and hasattr(relay, "get_runtime_status") else {}
        return {
            "status": "ok",
            "supported": list(registry.platform_ids()),
            "active": list(active),
            "one_room_per_platform": True,
            "plugins": registry.list_public(),
            "plugin_api": PLUGIN_API_VERSION,
            "runtime": runtime if isinstance(runtime, dict) else {},
        }

    def write_control_html(self: Any, module: Any) -> None:
        if not self._require_loopback():
            return
        file_path = Path(module.UI_DIR) / "control.html"
        if not file_path.is_file():
            self._write_json({"status": "error", "message": "Web control panel is missing"}, status=404)
            return
        text = file_path.read_text(encoding="utf-8")
        css_tag = '<link rel="stylesheet" href="/control_issue79.css">'
        if css_tag not in text:
            text = text.replace("</head>", f"  {css_tag}\n</head>")
        script_names = [
            "control_issue79.js",
            "control_huya.js",
            "control_redtv.js",
            "control_purple_mouse.js",
            "control_plugins.js",
        ]
        for name in script_names:
            if name != "control_issue79.js" and not (Path(module.UI_DIR) / name).is_file():
                continue
            tag = f'<script src="/{name}"></script>'
            if tag not in text:
                text = text.replace("</body>", f"  {tag}\n</body>")
        body = text.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    issue79_module._normalize_active_platforms = normalize_active_platforms
    issue79_module._runtime_for_platform = runtime_for_platform
    issue79_module._platform_payload = platform_payload
    issue79_module._write_control_html = write_control_html
    issue79_module.SUPPORTED_ACTIVE_PLATFORMS = registry.platform_ids()


def install_danmu_plugin_system(server_module: Any, issue79_module: Any = None) -> DanmuPluginRegistry:
    """Install Plugin API v1 and migrate all currently available relays."""
    with _PATCH_LOCK:
        registry = getattr(server_module, "danmu_plugin_registry", None)
        if not isinstance(registry, DanmuPluginRegistry):
            registry = DanmuPluginRegistry()
            server_module.danmu_plugin_registry = registry
            server_module.DANMU_PLUGIN_REGISTRY = registry
        _register_current_builtin_plugins(server_module, registry)
        _sync_legacy_platform_metadata(server_module, issue79_module, registry)
        _install_registry_factory(server_module, registry)
        _install_issue79_plugin_bridge(server_module, issue79_module, registry)
        server_module.PLUGIN_API_VERSION = PLUGIN_API_VERSION
        server_module._danmu_plugin_system_installed = True
        return registry


def install_danmu_plugin_api(server_module: Any, issue79_module: Any = None) -> bool:
    """Expose local-only plugin metadata after all handler guards are installed."""
    handler_class = getattr(server_module, "ApiHandler", None)
    registry = getattr(server_module, "danmu_plugin_registry", None)
    if not isinstance(handler_class, type) or not isinstance(registry, DanmuPluginRegistry):
        return False
    original_get = getattr(handler_class, "do_GET", None)
    if not callable(original_get) or bool(getattr(original_get, "_bilipdj_plugin_api", False)):
        return True

    @functools.wraps(original_get)
    def do_GET_with_plugins(self: Any) -> None:  # noqa: N802
        path = urlparse(self.path).path
        if path not in {"/api/plugins/danmu", "/api/danmu/plugins"}:
            return original_get(self)
        if not self._require_loopback():
            return
        active: list[str] = []
        if issue79_module is not None:
            try:
                active = list(
                    issue79_module._normalize_active_platforms(
                        server_module,
                        getattr(self.server, "runtime_config", {}),
                    )
                )
            except Exception:
                active = []
        self._write_json(
            {
                "status": "ok",
                "plugin_api": PLUGIN_API_VERSION,
                "type": PLUGIN_TYPE,
                "plugins": registry.list_public(),
                "active": active,
            }
        )

    do_GET_with_plugins._bilipdj_plugin_api = True  # type: ignore[attr-defined]
    handler_class.do_GET = do_GET_with_plugins
    return True


__all__ = [
    "PLUGIN_API_VERSION",
    "PLUGIN_TYPE",
    "DanmuPlugin",
    "DanmuPluginRegistry",
    "install_danmu_plugin_system",
    "install_danmu_plugin_api",
]
