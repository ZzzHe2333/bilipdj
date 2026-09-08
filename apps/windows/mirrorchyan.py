"""Prepared, opt-in MirrorChyan update client. Nothing calls this module by default."""
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

API_ROOT = "https://mirrorchyan.com/api/resources"


@dataclass(frozen=True)
class MirrorChyanSettings:
    enabled: bool = False
    resource_id: str = ""
    cdk: str = ""
    user_agent: str = "bilipdj"


def check_latest(settings: MirrorChyanSettings, current_version: str, *, timeout: float = 10.0) -> dict[str, Any]:
    if not settings.enabled:
        return {"enabled": False, "checked": False, "message": "MirrorChyan integration is disabled"}
    if not settings.resource_id.strip():
        raise ValueError("MirrorChyan resource_id is not configured")
    query = urllib.parse.urlencode({"current_version": current_version, "cdk": settings.cdk, "user_agent": settings.user_agent})
    resource = urllib.parse.quote(settings.resource_id.strip(), safe="")
    req = urllib.request.Request(f"{API_ROOT}/{resource}/latest?{query}", headers={"User-Agent": settings.user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        payload = json.loads(response.read().decode("utf-8", errors="replace"))
    if not isinstance(payload, dict):
        raise RuntimeError("MirrorChyan returned an invalid response")
    return {"enabled": True, "checked": True, **payload}
