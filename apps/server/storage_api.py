"""Loopback-only management API for non-destructive storage conflict choice."""
from __future__ import annotations

import json
from http import HTTPStatus
from typing import Any
from urllib.parse import urlparse

from . import user_data


def install_storage_api(server_module: Any) -> None:
    if bool(getattr(server_module, "_storage_api_installed", False)):
        return
    handler = server_module.ApiHandler
    old_get = handler.do_GET
    old_post = handler.do_POST

    def do_GET(self: Any) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/storage/status":
            return old_get(self)
        if not self._require_loopback():
            return
        plan = user_data.storage_plan(server_module.APP_DIR)
        self._write_json({
            "status": "ok",
            "mode": plan["mode"],
            "conflict": bool(plan["conflict"]),
            "choice": plan["choice"],
            "active": str(server_module.DATA_DIR),
            "legacy": str(plan["legacy"]),
            "user": str(plan["user"]),
        })

    def do_POST(self: Any) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/storage/choice":
            return old_post(self)
        if not self._require_loopback():
            return
        try:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if not 0 < length < 4096:
                raise ValueError("无效数据长度")
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            if not isinstance(payload, dict) or payload.get("choice") not in {"legacy", "user"}:
                raise ValueError("请明确选择项目旧数据或系统用户目录数据")
            result = user_data.choose_storage(server_module.APP_DIR, payload["choice"])
            self._write_json({"status": "ok", **result})
        except (ValueError, OSError, json.JSONDecodeError) as exc:
            self._write_json({"status": "error", "message": str(exc)}, status=HTTPStatus.BAD_REQUEST)

    handler.do_GET = do_GET
    handler.do_POST = do_POST
    server_module._storage_api_installed = True


__all__ = ["install_storage_api"]
