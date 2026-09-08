from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

DEFAULT_UPDATE_NETWORK: dict[str, Any] = {
    "bypass_system_proxy": False,
    "use_third_party_proxy": False,
    "proxy_host": "",
    "proxy_port": "",
    "use_mirrorchyan": False,
}
TEST_URL = "https://api.github.com/repos/ZzzHe2333/bilipdj/releases/latest"


def _app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def settings_path(app_dir: Path | None = None) -> Path:
    root = Path(app_dir) if app_dir is not None else _app_dir()
    return root / "core" / "cd" / "update_settings.json"


def normalize_update_network(payload: Any) -> dict[str, Any]:
    raw = payload if isinstance(payload, dict) else {}
    host = str(raw.get("proxy_host", "") or "").strip()
    port_text = str(raw.get("proxy_port", "") or "").strip()
    if port_text:
        try:
            port = int(port_text)
        except (TypeError, ValueError):
            port_text = ""
        else:
            port_text = str(port) if 1 <= port <= 65535 else ""
    bypass = bool(raw.get("bypass_system_proxy", False))
    third_party = bool(raw.get("use_third_party_proxy", False))
    if bypass and third_party:
        # An explicit third-party proxy is more specific than the direct mode.
        bypass = False
    return {
        "bypass_system_proxy": bypass,
        "use_third_party_proxy": third_party,
        "proxy_host": host,
        "proxy_port": port_text,
        "use_mirrorchyan": False,
    }


def load_update_network(app_dir: Path | None = None) -> dict[str, Any]:
    path = settings_path(app_dir)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        payload = {}
    merged = dict(DEFAULT_UPDATE_NETWORK)
    if isinstance(payload, dict):
        merged.update(payload)
    return normalize_update_network(merged)


def save_update_network(payload: Any, app_dir: Path | None = None) -> dict[str, Any]:
    normalized = normalize_update_network(payload)
    path = settings_path(app_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temp.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)
    return normalized


def _proxy_url(config: dict[str, Any]) -> str:
    host = str(config.get("proxy_host", "") or "").strip()
    port = str(config.get("proxy_port", "") or "").strip()
    if not host or not port:
        raise ValueError("启用第三方代理后必须填写代理地址和端口")
    if "://" in host:
        base = host.rstrip("/")
        parsed_host = base
    else:
        parsed_host = f"http://{host}"
    return f"{parsed_host}:{port}"


def build_opener(config: dict[str, Any] | None = None) -> urllib.request.OpenerDirector:
    cfg = normalize_update_network(config if config is not None else load_update_network())
    if cfg["use_third_party_proxy"]:
        proxy = _proxy_url(cfg)
        handler = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
    elif cfg["bypass_system_proxy"]:
        # Empty mapping disables environment/Windows system proxy discovery.
        # Only a TUN/packet-level proxy can still intercept this traffic.
        handler = urllib.request.ProxyHandler({})
    else:
        handler = urllib.request.ProxyHandler()
    return urllib.request.build_opener(handler)


def open_url(request: Any, *, timeout: float = 15.0, config: dict[str, Any] | None = None):
    return build_opener(config).open(request, timeout=timeout)


def test_update_connection(config: dict[str, Any] | None = None, *, timeout: float = 10.0) -> dict[str, Any]:
    cfg = normalize_update_network(config if config is not None else load_update_network())
    request = urllib.request.Request(
        TEST_URL,
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "bilipdj-update-network-test",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    started = time.perf_counter()
    try:
        with open_url(request, timeout=timeout, config=cfg) as response:
            status = int(getattr(response, "status", 200) or 200)
            response.read(256)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"代理检测失败：HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"代理检测失败：{exc.reason}") from exc
    except TimeoutError as exc:
        raise RuntimeError("代理检测超时") from exc
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    mode = "第三方代理" if cfg["use_third_party_proxy"] else (
        "绕过系统代理" if cfg["bypass_system_proxy"] else "系统代理/直连"
    )
    return {"ok": 200 <= status < 400, "status": status, "latency_ms": elapsed_ms, "mode": mode}


def install_update_client_network_guard(module: Any | None = None) -> bool:
    if module is None:
        try:
            from . import update_client as module
        except ImportError:
            import update_client as module  # type: ignore[no-redef]
    current = getattr(module, "_request", None)
    if not callable(current):
        return False
    if bool(getattr(current, "_bilipdj_update_network_guard", False)):
        return True

    def request_with_selected_proxy(url: str, *, timeout: float = 15.0):
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "User-Agent": getattr(module, "USER_AGENT", "bilipdj-auto-updater"),
                "X-GitHub-Api-Version": "2022-11-28",
            },
        )
        try:
            return open_url(request, timeout=timeout)
        except urllib.error.HTTPError as exc:
            raise module.UpdateError(f"GitHub 请求失败：HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise module.UpdateError(f"无法连接 GitHub：{exc.reason}") from exc
        except TimeoutError as exc:
            raise module.UpdateError("连接 GitHub 超时") from exc

    setattr(request_with_selected_proxy, "_bilipdj_update_network_guard", True)
    setattr(module, "_request", request_with_selected_proxy)
    return True


__all__ = [
    "DEFAULT_UPDATE_NETWORK",
    "build_opener",
    "install_update_client_network_guard",
    "load_update_network",
    "normalize_update_network",
    "save_update_network",
    "settings_path",
    "test_update_connection",
]
