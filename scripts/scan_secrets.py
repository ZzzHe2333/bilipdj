#!/usr/bin/env python3
"""Fail packaging when tracked source appears to contain private credentials."""
from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PATTERNS = (
    re.compile(r"(?i)(SESSDATA|bili_jct|DedeUserID|access_token|refresh_token|auth_token)\s*[:=]\s*['\"]([^\s'\"<]{8,})['\"]"),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
)
ALLOW = {"scripts/scan_secrets.py"}


def main() -> int:
    files = subprocess.check_output(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
        cwd=ROOT,
        text=True,
        encoding="utf-8",
    ).splitlines()
    findings: list[str] = []
    for name in files:
        normalized = name.replace("\\", "/")
        if normalized in ALLOW:
            continue
        path = ROOT / name
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        if any(pattern.search(text) for pattern in PATTERNS):
            findings.append(normalized)
    if findings:
        print("Potential secrets detected in tracked files:")
        print("\n".join(f"- {item}" for item in findings))
        return 1
    print(f"Secret scan passed ({len(files)} tracked files).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
