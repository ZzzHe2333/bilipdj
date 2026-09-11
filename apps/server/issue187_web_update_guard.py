from __future__ import annotations

import json
import os
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.parse
from pathlib import Path
from typing import Any


def _stop_process(process: subprocess.Popen[Any] | None) -> None:
    if process is None:
        return
    try:
        if process.poll() is not None:
            return
    except Exception:
        return
    try:
        process.terminate()
        process.wait(timeout=2.0)
        return
    except subprocess.TimeoutExpired:
        pass
    except Exception:
        pass
    try:
        process.kill()
    except Exception:
        pass
    try:
        process.wait(timeout=2.0)
    except Exception:
        pass


def install_issue187_web_update_guard(server_module: Any) -> bool:
    from apps.server import web_update_api

    if bool(getattr(web_update_api, "_issue187_safe_launch_installed", False)):
        return True

    def launch_update_safe(active_server_module: Any, active_server: Any, payload: dict[str, Any]) -> dict[str, Any]:
        if not bool(getattr(web_update_api.sys, "frozen", False)):
            raise RuntimeError("自动更新仅支持 Web Portable 打包版；源码模式不会修改仓库文件")

        app_dir = Path(getattr(active_server_module, "APP_DIR", ".")).resolve()
        updater_source = app_dir / web_update_api.WEB_UPDATER_EXE
        if not updater_source.is_file():
            raise RuntimeError(f"Web Portable 缺少独立更新器：{web_update_api.WEB_UPDATER_EXE}")

        mode = str(payload.get("mode", "") or "").strip().lower()
        if mode not in {"full", "incremental", "restore"}:
            raise ValueError("mode 必须是 full / incremental / restore")

        session_dir = Path(tempfile.mkdtemp(prefix="bilipdj-web-update-launch-"))
        process: subprocess.Popen[Any] | None = None
        try:
            updater_copy = session_dir / web_update_api.WEB_UPDATER_EXE
            shutil.copy2(updater_source, updater_copy)
            token = secrets.token_urlsafe(32)
            port = web_update_api._free_port()  # noqa: SLF001
            server_port = int(getattr(active_server, "server_port", 9816) or 9816)
            request: dict[str, Any] = {
                "schema": 1,
                "session": secrets.token_hex(12),
                "token": token,
                "port": port,
                "mode": mode,
                "app_dir": str(app_dir),
                "main_exe": web_update_api.WEB_MAIN_EXE,
                "updater_exe": web_update_api.WEB_UPDATER_EXE,
                "backend_pid": os.getpid(),
                "launcher_pid": os.getppid(),
                "current_version": web_update_api._version(active_server_module),  # noqa: SLF001
                "return_url": f"http://127.0.0.1:{server_port}/control",
                "launch_dir": str(session_dir),
            }

            if mode in {"full", "incremental"}:
                manifest = web_update_api._load_manifest()  # noqa: SLF001
                package = manifest["packages"][web_update_api.WEB_PACKAGE_KEY]
                if mode == "incremental":
                    if not isinstance(package.get("file_manifest"), dict) or not isinstance(package.get("incremental"), dict):
                        raise RuntimeError("当前 Release 没有 Web Portable 增量资源；请使用全量更新")
                request["target_version"] = str(manifest.get("version", "") or "")
                request["tag_name"] = str(manifest.get("tag_name", "") or "")
                request["package"] = package
            else:
                backup_path, backup_version = web_update_api._safe_backup(  # noqa: SLF001
                    app_dir,
                    str(payload.get("backup_id", "") or ""),
                )
                request["target_version"] = backup_version
                request["backup_path"] = str(backup_path)
                request["backup_id"] = backup_path.name

            request_path = session_dir / "request.json"
            request_path.write_text(json.dumps(request, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
            ready_path = session_dir / "ready.json"

            process = subprocess.Popen(
                [str(updater_copy), "--request", str(request_path)],
                cwd=str(session_dir),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                creationflags=web_update_api._launch_flags(),  # noqa: SLF001
            )

            deadline = time.monotonic() + 8.0
            while time.monotonic() < deadline:
                if ready_path.is_file():
                    try:
                        ready = json.loads(ready_path.read_text(encoding="utf-8"))
                    except (OSError, json.JSONDecodeError):
                        ready = {}
                    if ready.get("status") == "ok":
                        return {
                            "status": "ok",
                            "mode": mode,
                            "target_version": request.get("target_version", ""),
                            "update_url": f"http://127.0.0.1:{port}/update.html?token={urllib.parse.quote(token)}",
                            "message": "独立 Web 更新器已启动；主服务将在更新页打开后退出。",
                        }
                    error = str(ready.get("error", "") or "Web 更新器启动失败")
                    raise RuntimeError(error)
                if process.poll() is not None:
                    raise RuntimeError(f"Web 更新器提前退出，退出码 {process.returncode}")
                time.sleep(0.08)
            raise RuntimeError("等待独立 Web 更新器启动超时")
        except Exception:
            _stop_process(process)
            shutil.rmtree(session_dir, ignore_errors=True)
            raise

    web_update_api._launch_update = launch_update_safe  # type: ignore[attr-defined]  # noqa: SLF001
    web_update_api._issue187_safe_launch_installed = True
    return True


__all__ = ["install_issue187_web_update_guard"]
