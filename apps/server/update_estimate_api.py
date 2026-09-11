from __future__ import annotations

import hashlib
import json
import threading
import urllib.error
import urllib.parse
import urllib.request
from http import HTTPStatus
from pathlib import Path
from typing import Any

_PATCH_LOCK = threading.RLock()
ESTIMATE_PATH = "/api/control/update-estimate"


def _fetch_latest_release() -> dict[str, Any]:
    request = urllib.request.Request(
        "https://api.github.com/repos/ZzzHe2333/bilipdj/releases/latest",
        headers={
            "Accept": "application/vnd.github+json",
            "User-Agent": "bilipdj-update-estimate",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=8) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    if not isinstance(payload, dict):
        raise RuntimeError("GitHub Release 响应无效")
    return payload


def _asset(payload: dict[str, Any], suffix: str) -> dict[str, Any] | None:
    assets = payload.get("assets", [])
    if not isinstance(assets, list):
        return None
    for item in assets:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "") or "")
        if name.endswith(suffix):
            return item
    return None


def _persist_check(server_module: Any, data: dict[str, Any]) -> None:
    key_dir = Path(getattr(server_module, "KEY_DIR", Path(getattr(server_module, "APP_DIR", ".")) / "key"))
    key_dir.mkdir(parents=True, exist_ok=True)
    path = key_dir / "update-check.json"
    raw = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    path.write_bytes(raw)
    digest = hashlib.sha256(raw).hexdigest()
    (key_dir / "update-check.json.sha256").write_text(
        f"{digest}  update-check.json\n",
        encoding="ascii",
    )


def _estimate_payload(server_module: Any) -> dict[str, Any]:
    release = _fetch_latest_release()
    tag = str(release.get("tag_name", "") or "")
    web = _asset(release, "-Web-Portable-x64.zip")
    web_files = _asset(release, "-Web-files.json")
    web_pack = _asset(release, "-Web-Incremental-x64.pack")
    windows = _asset(release, "-Windows-Tk-Portable-x64.zip")
    files = _asset(release, "-Windows-Tk-files.json")
    pack = _asset(release, "-Windows-Tk-Incremental-x64.pack")
    web_incremental = bool(web_files and web_pack)
    result = {
        "status": "ok",
        "tag_name": tag,
        "latest_version": tag.lstrip("v"),
        "web": {
            "full_download_bytes": int(web.get("size", 0) or 0) if web else 0,
            "incremental_available": web_incremental,
            "incremental_download_bytes": int(web_pack.get("size", 0) or 0) if web_pack else None,
            "file_manifest_bytes": int(web_files.get("size", 0) or 0) if web_files else 0,
            "resource_pack_bytes": int(web_pack.get("size", 0) or 0) if web_pack else 0,
            "note": (
                "Web Portable 支持逐文件 SHA-256 扫描与 HTTP Range 增量下载；显示值为资源包上限，实际只下载变化文件。"
                if web_incremental else
                "当前 Release 未提供 Web Portable 增量资源，可使用全量更新。"
            ),
        },
        "windows": {
            "full_download_bytes": int(windows.get("size", 0) or 0) if windows else 0,
            "file_manifest_bytes": int(files.get("size", 0) or 0) if files else 0,
            "resource_pack_bytes": int(pack.get("size", 0) or 0) if pack else 0,
        },
    }
    _persist_check(server_module, result)
    return result


def install_update_estimate_api(server_module: Any) -> bool:
    if server_module is None or not hasattr(server_module, "ApiHandler"):
        return False
    with _PATCH_LOCK:
        if bool(getattr(server_module, "_issue167_update_estimate_api", False)):
            return True
        handler_class = server_module.ApiHandler
        original_get = handler_class.do_GET

        def do_GET(self: Any) -> None:  # noqa: N802
            path = urllib.parse.urlparse(self.path).path
            if path != ESTIMATE_PATH:
                return original_get(self)
            if not self._require_loopback():
                return
            try:
                payload = _estimate_payload(server_module)
            except urllib.error.HTTPError as exc:
                self._write_json(
                    {"status": "error", "message": f"GitHub HTTP {exc.code}"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            except Exception as exc:  # noqa: BLE001
                self._write_json(
                    {"status": "error", "message": f"更新大小检测失败：{exc}"},
                    status=HTTPStatus.BAD_GATEWAY,
                )
                return
            self._write_json(payload)

        handler_class.do_GET = do_GET
        server_module._issue167_update_estimate_api = True
        return True


__all__ = ["ESTIMATE_PATH", "install_update_estimate_api"]
