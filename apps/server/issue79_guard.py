from __future__ import annotations

import copy
import json
import threading
import urllib.request
from http import HTTPStatus
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

REPOSITORY = "ZzzHe2333/bilipdj"
MANIFEST_URL = f"https://github.com/{REPOSITORY}/releases/latest/download/update-manifest.json"
RAW_MANIFEST_URL = f"https://raw.githubusercontent.com/{REPOSITORY}/now/update-manifest.json"
LATEST_RELEASE_API = f"https://api.github.com/repos/{REPOSITORY}/releases/latest"
SUPPORTED_ACTIVE_PLATFORMS = ("bilibili", "douyin")
_PATCH_LOCK = threading.RLock()


def _normalize_active_platforms(server_module: Any, config: Any) -> tuple[str, ...]:
    cfg = config if isinstance(config, dict) else {}
    raw = cfg.get("active_platforms")
    if isinstance(raw, (list, tuple)):
        result: list[str] = []
        for value in raw:
            name = str(value or "").strip().lower()
            if name in SUPPORTED_ACTIVE_PLATFORMS and name not in result:
                result.append(name)
        # An explicit empty list means “disable all relays”. Old configurations
        # without active_platforms continue to fall back to the single platform.
        return tuple(result)

    try:
        legacy = str(server_module._get_runtime_platform(cfg) or "bilibili").strip().lower()
    except Exception:
        legacy = str(cfg.get("platform", "bilibili") or "bilibili").strip().lower()
    if legacy in SUPPORTED_ACTIVE_PLATFORMS:
        return (legacy,)
    return ("bilibili",)


def _runtime_for_platform(server: Any, platform: str) -> dict[str, Any]:
    runtime = copy.deepcopy(getattr(server, "runtime_config", {}) or {})
    runtime["platform"] = platform
    if platform == "douyin":
        douyin = dict(runtime.get("douyin", {}) or {})
        douyin["enabled"] = True
        runtime["douyin"] = douyin
    return runtime


class _RelayServerProxy:
    """Give each relay its own platform view while sharing one backend state."""

    def __init__(self, server: Any, platform: str) -> None:
        self._server = server
        self.platform = platform
        self.runtime_config = _runtime_for_platform(server, platform)

    def refresh_runtime_config(self) -> None:
        self.runtime_config = _runtime_for_platform(self._server, self.platform)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._server, name)


class MultiPlatformRelayManager:
    """Run one relay per active platform and expose the legacy relay interface."""

    platform = "multi"

    def __init__(self, server_module: Any, server: Any, active_platforms: tuple[str, ...]) -> None:
        self.server_module = server_module
        self.server = server
        self.active_platforms = tuple(active_platforms)
        self._relays: dict[str, Any] = {}
        self._proxies: dict[str, _RelayServerProxy] = {}
        self._lock = threading.RLock()
        self._started = False

    def start(self) -> None:
        with self._lock:
            if self._started:
                return
            self._started = True
            for platform in self.active_platforms:
                proxy = _RelayServerProxy(self.server, platform)
                relay = self.server_module._create_danmu_relay(proxy)
                self._proxies[platform] = proxy
                self._relays[platform] = relay
                relay.start()
                self.server.logger.info("Danmu relay started platform=%s (multi-platform)", platform)
            if not self.active_platforms:
                self.server.logger.info("All danmu relays are disabled by active_platforms")

    def stop(self) -> None:
        with self._lock:
            relays = list(self._relays.values())
        for relay in relays:
            try:
                relay.stop()
            except Exception:  # noqa: BLE001
                pass

    def join(self, timeout: float | None = None) -> None:
        with self._lock:
            relays = list(self._relays.values())
        if not relays:
            return
        per_relay = None if timeout is None else max(0.05, float(timeout) / len(relays))
        for relay in relays:
            try:
                relay.join(timeout=per_relay)
            except Exception:  # noqa: BLE001
                pass

    def request_reconnect(self) -> None:
        with self._lock:
            items = [(name, relay, self._proxies.get(name)) for name, relay in self._relays.items()]
        for _name, relay, proxy in items:
            try:
                if proxy is not None:
                    proxy.refresh_runtime_config()
                callback = getattr(relay, "request_reconnect", None)
                if callable(callback):
                    callback()
            except Exception:  # noqa: BLE001
                pass

    def get_runtime_status(self) -> dict[str, Any]:
        with self._lock:
            items = list(self._relays.items())
        statuses: dict[str, dict[str, Any]] = {}
        for platform, relay in items:
            try:
                value = relay.get_runtime_status() if hasattr(relay, "get_runtime_status") else {}
            except Exception as exc:  # noqa: BLE001
                value = {"connected": False, "error": str(exc)}
            status = dict(value) if isinstance(value, dict) else {}
            status.setdefault("platform", platform)
            statuses[platform] = status

        primary_name = self.active_platforms[0] if self.active_platforms else ""
        primary = dict(statuses.get(primary_name, {}))
        connected = any(bool(item.get("connected", False)) for item in statuses.values())
        primary.update(
            {
                "connected": connected,
                "platform": primary_name if len(self.active_platforms) <= 1 else "multi",
                "active_platforms": list(self.active_platforms),
                "platforms": statuses,
            }
        )
        return primary


def _read_json_url(url: str, *, timeout: float = 12.0) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json, application/vnd.github+json",
            "User-Agent": "bilipdj-update-manifest",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8-sig", errors="strict"))
    if not isinstance(payload, dict):
        raise ValueError("update manifest is not a JSON object")
    return payload


def _package_from_asset(asset: Any) -> dict[str, Any] | None:
    if not isinstance(asset, dict):
        return None
    filename = str(asset.get("name", "") or "").strip()
    url = str(asset.get("browser_download_url", "") or "").strip()
    if not filename or not url:
        return None
    digest = str(asset.get("digest", "") or "").strip().lower()
    if digest.startswith("sha256:"):
        digest = digest.split(":", 1)[1]
    try:
        size = int(asset.get("size", 0) or 0)
    except (TypeError, ValueError):
        size = 0
    return {"filename": filename, "url": url, "sha256": digest, "size": max(0, size)}


def _manifest_from_release(payload: dict[str, Any]) -> dict[str, Any]:
    tag_name = str(payload.get("tag_name", "") or "").strip()
    version = tag_name[1:] if tag_name.lower().startswith("v") else tag_name
    packages: dict[str, dict[str, Any]] = {}
    for raw in payload.get("assets", []) if isinstance(payload.get("assets"), list) else []:
        package = _package_from_asset(raw)
        if package is None or not package.get("filename", "").lower().endswith(".zip"):
            continue
        lower = package["filename"].lower()
        if "windows-tk-portable-x64" in lower or "bilibili-danmuji-windows-x64" in lower:
            packages["windows-tk-x64"] = package
        elif "web-portable-x64" in lower:
            packages["web-portable-x64"] = package
    return {
        "schema": 1,
        "version": version,
        "tag_name": tag_name,
        "name": str(payload.get("name", "") or tag_name),
        "published_at": str(payload.get("published_at", "") or ""),
        "release_url": str(payload.get("html_url", "") or ""),
        "notes": str(payload.get("body", "") or ""),
        "packages": packages,
    }


def _load_update_manifest() -> dict[str, Any]:
    errors: list[str] = []
    for url in (MANIFEST_URL, RAW_MANIFEST_URL):
        try:
            payload = _read_json_url(url)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{url}: {exc}")
            continue
        if isinstance(payload.get("packages"), dict) and str(payload.get("version", "") or "").strip():
            return payload
    try:
        return _manifest_from_release(_read_json_url(LATEST_RELEASE_API))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"{LATEST_RELEASE_API}: {exc}")
    raise RuntimeError("无法获取更新清单：" + " | ".join(errors[-3:]))


def _read_version(server_module: Any) -> str:
    candidates = [
        Path(getattr(server_module, "BUNDLE_DIR", ".")) / "VERSION",
        Path(getattr(server_module, "APP_DIR", ".")) / "VERSION",
        Path(getattr(server_module, "REPO_DIR", ".")) / "VERSION",
    ]
    for path in candidates:
        try:
            value = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if value:
            return value
    return "unknown"


def _update_payload(server_module: Any) -> dict[str, Any]:
    current = _read_version(server_module)
    try:
        manifest = _load_update_manifest()
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "message": str(exc), "current_version": current}
    packages = manifest.get("packages", {}) if isinstance(manifest.get("packages"), dict) else {}
    package = packages.get("web-portable-x64", {})
    return {
        "status": "ok",
        "current_version": current,
        "latest_version": str(manifest.get("version", "") or ""),
        "tag_name": str(manifest.get("tag_name", "") or ""),
        "name": str(manifest.get("name", "") or ""),
        "published_at": str(manifest.get("published_at", "") or ""),
        "html_url": str(manifest.get("release_url", "") or ""),
        "body": str(manifest.get("notes", "") or "")[:12000],
        "manifest_schema": manifest.get("schema", 1),
        "package": package if isinstance(package, dict) else {},
    }


def _write_control_html(self: Any, server_module: Any) -> None:
    if not self._require_loopback():
        return
    file_path = Path(server_module.UI_DIR) / "control.html"
    if not file_path.is_file():
        self._write_json({"status": "error", "message": "Web control panel is missing"}, status=404)
        return
    text = file_path.read_text(encoding="utf-8")
    css_tag = '<link rel="stylesheet" href="/control_issue79.css">'
    js_tag = '<script src="/control_issue79.js"></script>'
    if css_tag not in text:
        text = text.replace("</head>", f"  {css_tag}\n</head>")
    if js_tag not in text:
        text = text.replace("</body>", f"  {js_tag}\n</body>")
    body = text.encode("utf-8")
    self.send_response(HTTPStatus.OK)
    self.send_header("Content-Type", "text/html; charset=utf-8")
    self.send_header("Cache-Control", "no-store")
    self.send_header("Content-Length", str(len(body)))
    self.end_headers()
    self.wfile.write(body)


def _read_json_body(self: Any) -> dict[str, Any]:
    try:
        length = int(self.headers.get("Content-Length", "0") or 0)
    except (TypeError, ValueError):
        length = 0
    raw = self.rfile.read(max(0, length)).decode("utf-8", errors="replace") if length else "{}"
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = {}
    return payload if isinstance(payload, dict) else {}


def _platform_payload(server_module: Any, server: Any) -> dict[str, Any]:
    active = _normalize_active_platforms(server_module, getattr(server, "runtime_config", {}))
    relay = getattr(server, "danmu_relay", None)
    runtime = relay.get_runtime_status() if relay is not None and hasattr(relay, "get_runtime_status") else {}
    return {
        "status": "ok",
        "supported": list(SUPPORTED_ACTIVE_PLATFORMS),
        "active": list(active),
        "one_room_per_platform": True,
        "runtime": runtime if isinstance(runtime, dict) else {},
    }


def install_issue79_guard(server_module: Any) -> bool:
    if server_module is None or not hasattr(server_module, "ApiHandler"):
        return False
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_issue79_guard_installed", False)):
            return True

        def ensure_multi_platform_relay(server: Any, *, reconnect: bool = False) -> None:
            desired = _normalize_active_platforms(server_module, getattr(server, "runtime_config", {}))
            current = getattr(server, "danmu_relay", None)
            if not isinstance(current, MultiPlatformRelayManager) or current.active_platforms != desired:
                if current is not None:
                    try:
                        current.stop()
                    except Exception:  # noqa: BLE001
                        pass
                    try:
                        current.join(timeout=2.0)
                    except Exception:  # noqa: BLE001
                        pass
                manager = MultiPlatformRelayManager(server_module, server, desired)
                server.danmu_relay = manager
                manager.start()
                return
            if reconnect:
                current.request_reconnect()

        server_module._ensure_danmu_relay = ensure_multi_platform_relay

        handler_class = server_module.ApiHandler
        original_get = handler_class.do_GET
        original_post = handler_class.do_POST

        def do_GET(self: Any) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path in {"/control", "/control/", "/control.html"}:
                _write_control_html(self, server_module)
                return
            if path == "/api/platforms/active":
                if not self._require_loopback():
                    return
                self._write_json(_platform_payload(server_module, self.server))
                return
            if path == "/api/control/update":
                if not self._require_loopback():
                    return
                self._write_json(_update_payload(server_module))
                return
            return original_get(self)

        def do_POST(self: Any) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/api/platforms/active":
                if not self._require_loopback():
                    return
                payload = _read_json_body(self)
                raw = payload.get("active", [])
                active: list[str] = []
                if isinstance(raw, (list, tuple)):
                    for value in raw:
                        name = str(value or "").strip().lower()
                        if name in SUPPORTED_ACTIVE_PLATFORMS and name not in active:
                            active.append(name)
                config = server_module.load_config()
                config["active_platforms"] = active
                if active:
                    config["platform"] = active[0]
                douyin = dict(config.get("douyin", {}) or {})
                douyin["enabled"] = "douyin" in active
                config["douyin"] = douyin
                server_module.save_config(config)
                self.server.runtime_config = server_module.load_config()
                server_module._ensure_danmu_relay(self.server, reconnect=True)
                self._write_json(_platform_payload(server_module, self.server))
                return
            if path == "/api/queue/insert":
                if not self._require_loopback():
                    return
                payload = _read_json_body(self)
                try:
                    after = int(payload.get("after", 0) or 0)
                except (TypeError, ValueError):
                    after = 0
                username = str(payload.get("username", "") or "").strip()
                content = str(payload.get("content", "") or "").strip()
                entry = str(payload.get("entry", "") or "").strip()
                if username:
                    entry = username if not content else f"{username} {content}"
                if not entry:
                    self._write_json(
                        {"status": "error", "message": "username/entry is required"},
                        status=HTTPStatus.BAD_REQUEST,
                    )
                    return
                qm = getattr(self.server, "queue_manager", None)
                if qm is None:
                    self._write_json({"status": "error", "message": "queue_manager not ready"}, status=503)
                    return
                current = qm.insert_item(max(0, after), entry)
                entries = qm.get_queue_entries()
                self._write_json({"status": "ok", "queue": current, "entries": entries, "size": len(current)})
                return
            return original_post(self)

        handler_class.do_GET = do_GET
        handler_class.do_POST = do_POST
        server_module.MultiPlatformRelayManager = MultiPlatformRelayManager
        server_module._issue79_guard_installed = True
        return True


__all__ = [
    "MANIFEST_URL",
    "MultiPlatformRelayManager",
    "SUPPORTED_ACTIVE_PLATFORMS",
    "install_issue79_guard",
]
