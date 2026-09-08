"""Security policy for callbacks that carry login credentials."""
from __future__ import annotations

import datetime as dt
import functools
import ipaddress
import json
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlparse


def _is_loopback_host(hostname: str) -> bool:
    host = str(hostname or "").strip().rstrip(".").casefold()
    if host == "localhost" or host.endswith(".localhost"):
        return True
    try:
        return bool(host) and ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def validate_callback_url(value: Any) -> str:
    """Return a credential-safe callback URL or raise ``ValueError``."""

    url = str(value or "").strip()
    parsed = urlparse(url)
    if not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("callback URL must contain a host and no user information")
    scheme = parsed.scheme.casefold()
    if scheme == "https":
        return url
    if scheme == "http" and _is_loopback_host(parsed.hostname):
        return url
    raise ValueError("credential callback requires HTTPS except for loopback HTTP")


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: D401
        raise urllib.error.HTTPError(
            req.full_url,
            code,
            "credential callback redirects are disabled",
            headers,
            fp,
        )


def patch_login_callback(module: Any) -> bool:
    """Replace one server callback dispatcher with a redirect-safe version."""

    if module is None:
        return False
    original = getattr(module, "_dispatch_login_callback", None)
    if not callable(original):
        return False
    if bool(getattr(original, "_bilipdj_secure_callback", False)):
        return True

    @functools.wraps(original)
    def secure_dispatch(
        callback_cfg: dict[str, Any],
        *,
        cookie: str,
        bilibili_data: dict[str, Any],
        logger: Any,
    ) -> tuple[bool, str]:
        if not bool(callback_cfg.get("enabled", False)):
            return False, "callback disabled"
        try:
            callback_url = validate_callback_url(callback_cfg.get("url", ""))
        except ValueError as exc:
            logger.warning("扫码回调已拒绝: %s", exc)
            return False, f"callback rejected ({exc})"

        payload = {
            "event": "bilibili_qr_login_success",
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
            "cookie": str(cookie or ""),
            "bilibili": dict(bilibili_data) if isinstance(bilibili_data, dict) else {},
        }
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        try:
            timeout_seconds = int(callback_cfg.get("timeout_seconds", 5) or 5)
        except (TypeError, ValueError):
            timeout_seconds = 5
        timeout_seconds = max(1, min(30, timeout_seconds))
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": "DanmujiBackend/0.3",
        }
        auth_token = str(callback_cfg.get("auth_token", "") or "").strip()
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"

        request = urllib.request.Request(
            callback_url,
            data=body,
            method="POST",
            headers=headers,
        )
        opener = urllib.request.build_opener(_NoRedirectHandler())
        try:
            with opener.open(request, timeout=timeout_seconds) as response:
                status = int(getattr(response, "status", 200))
                if 200 <= status < 300:
                    return True, f"callback ok (status={status})"
                return False, f"callback failed (status={status})"
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            logger.warning("扫码回调失败: %s", exc)
            return False, f"callback failed ({exc})"

    setattr(secure_dispatch, "_bilipdj_secure_callback", True)
    setattr(module, "_dispatch_login_callback", secure_dispatch)
    return True


__all__ = ["patch_login_callback", "validate_callback_url"]
