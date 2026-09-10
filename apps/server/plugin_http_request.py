"""Bounded HTTP transport used by sandboxed plugin Host APIs (Issue #140)."""
from __future__ import annotations

import base64
import binascii
import math
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Mapping
from urllib.parse import urlparse

# The QuickJS Host JSON envelope is capped at 1 MiB. Keep request bodies small
# enough that Base64 and JSON escaping still fit inside that existing boundary.
MAX_HTTP_REQUEST_BODY_BYTES = 128 * 1024
MAX_HTTP_RESPONSE_BYTES = 4 * 1024 * 1024
MAX_HTTP_REQUEST_HEADERS = 64
MAX_HTTP_HEADER_NAME_BYTES = 128
MAX_HTTP_HEADER_VALUE_BYTES = 8192
ALLOWED_HTTP_METHODS = frozenset({"GET", "POST"})
EXPOSED_RESPONSE_HEADERS = frozenset(
    {
        "cache-control",
        "content-encoding",
        "content-language",
        "content-length",
        "content-type",
        "date",
        "etag",
        "expires",
        "last-modified",
        "location",
        "retry-after",
    }
)


@dataclass(frozen=True)
class PreparedPluginHttpRequest:
    request: urllib.request.Request
    timeout: float


def _validated_url(raw_url: Any) -> str:
    url = str(raw_url or "")
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.hostname:
        raise ValueError("only http/https URLs are allowed")
    return url


def _validated_timeout(value: Any) -> float:
    try:
        timeout = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("HTTP timeout must be a number") from exc
    if not math.isfinite(timeout):
        raise ValueError("HTTP timeout must be finite")
    return max(1.0, min(30.0, timeout))


def _validated_headers(value: Any) -> dict[str, str]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("HTTP headers must be an object")
    if len(value) > MAX_HTTP_REQUEST_HEADERS:
        raise ValueError(f"HTTP request cannot contain more than {MAX_HTTP_REQUEST_HEADERS} headers")
    result: dict[str, str] = {}
    for raw_name, raw_value in value.items():
        name = str(raw_name)
        item = str(raw_value)
        if not name or "\r" in name or "\n" in name or ":" in name:
            raise ValueError("HTTP header name is invalid")
        if "\r" in item or "\n" in item:
            raise ValueError(f"HTTP header value contains a line break: {name}")
        if len(name.encode("utf-8")) > MAX_HTTP_HEADER_NAME_BYTES:
            raise ValueError(f"HTTP header name is too long: {name[:32]}")
        if len(item.encode("utf-8")) > MAX_HTTP_HEADER_VALUE_BYTES:
            raise ValueError(f"HTTP header value is too long: {name}")
        result[name] = item
    return result


def _request_body(options: Mapping[str, Any], method: str) -> bytes | None:
    has_text = "body" in options and options.get("body") is not None
    has_base64 = "body_base64" in options and options.get("body_base64") is not None
    if has_text and has_base64:
        raise ValueError("HTTP body and body_base64 are mutually exclusive")
    if method == "GET" and (has_text or has_base64):
        raise ValueError("GET request body is not allowed")

    body: bytes | None = None
    if has_text:
        value = options.get("body")
        if not isinstance(value, str):
            raise ValueError("HTTP body must be a UTF-8 string")
        body = value.encode("utf-8")
    elif has_base64:
        value = options.get("body_base64")
        if not isinstance(value, str):
            raise ValueError("HTTP body_base64 must be a Base64 string")
        try:
            body = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("HTTP body_base64 is not valid Base64") from exc

    if body is not None and len(body) > MAX_HTTP_REQUEST_BODY_BYTES:
        raise ValueError(f"HTTP request body exceeds {MAX_HTTP_REQUEST_BODY_BYTES} bytes")
    return body


def prepare_plugin_http_request(url: Any, options: Any = None) -> PreparedPluginHttpRequest:
    """Validate Host options and build an urllib request without performing I/O."""
    if options is None:
        options = {}
    if not isinstance(options, dict):
        raise ValueError("HTTP options must be an object")

    method = str(options.get("method", "GET") or "GET").strip().upper()
    if method not in ALLOWED_HTTP_METHODS:
        raise ValueError("HTTP method must be GET or POST")
    headers = _validated_headers(options.get("headers", {}))
    timeout = _validated_timeout(options.get("timeout", 10))
    body = _request_body(options, method)

    request = urllib.request.Request(
        _validated_url(url),
        data=body,
        headers=headers,
        method=method,
    )
    return PreparedPluginHttpRequest(request=request, timeout=timeout)


def _safe_response_headers(headers: Any) -> dict[str, str]:
    if headers is None:
        return {}
    result: dict[str, str] = {}
    try:
        items = headers.items()
    except AttributeError:
        return result
    for raw_name, raw_value in items:
        name = str(raw_name or "").strip().lower()
        if name in EXPOSED_RESPONSE_HEADERS:
            result[name] = str(raw_value or "")[:MAX_HTTP_HEADER_VALUE_BYTES]
    return result


def _read_response(response: Any) -> dict[str, Any]:
    data = response.read(MAX_HTTP_RESPONSE_BYTES + 1)
    if len(data) > MAX_HTTP_RESPONSE_BYTES:
        raise ValueError("HTTP response exceeds plugin safety limit")
    status = getattr(response, "status", None)
    if status is None:
        status = getattr(response, "code", None)
    if status is None and callable(getattr(response, "getcode", None)):
        status = response.getcode()
    return {
        "status": int(status or 0),
        "headers": _safe_response_headers(getattr(response, "headers", None)),
        "data_base64": base64.b64encode(data).decode("ascii"),
    }


def perform_plugin_http_request(prepared: PreparedPluginHttpRequest) -> dict[str, Any]:
    """Execute a prepared request; HTTP error status codes are returned as responses."""
    try:
        with urllib.request.urlopen(prepared.request, timeout=prepared.timeout) as response:
            return _read_response(response)
    except urllib.error.HTTPError as exc:
        try:
            return _read_response(exc)
        finally:
            exc.close()


__all__ = [
    "ALLOWED_HTTP_METHODS",
    "EXPOSED_RESPONSE_HEADERS",
    "MAX_HTTP_REQUEST_BODY_BYTES",
    "MAX_HTTP_RESPONSE_BYTES",
    "PreparedPluginHttpRequest",
    "perform_plugin_http_request",
    "prepare_plugin_http_request",
]
