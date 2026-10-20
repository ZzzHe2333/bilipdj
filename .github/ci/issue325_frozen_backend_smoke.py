"""Issue #325: launch the actual frozen Windows Tk backend and probe HTTP.

Requires --exe dist/bilipdj/main.exe on Windows GitHub Actions.
Never writes cookies or user configuration. Does not test a real Douyin account.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path


def unused_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def fetch_json(url: str) -> dict:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with opener.open(request, timeout=1.5) as response:
        if response.status != 200:
            raise AssertionError(f"HTTP {response.status} for {url}")
        payload = json.loads(response.read(1024 * 1024).decode("utf-8"))
    if not isinstance(payload, dict):
        raise AssertionError("Expected JSON response")
    return payload


def run_frozen_backend(exe: Path) -> None:
    if not exe.is_file():
        raise FileNotFoundError(exe)
    with tempfile.TemporaryDirectory(prefix="bilipdj-issue325-") as tmp:
        port = unused_port()
        root = Path(tmp)
        env = os.environ.copy()
        env.update({
            "BILIPDJ_DATA_DIR": str(root / "private_data"),
            "BILIPDJ_UNATTENDED_BACKEND": "1",
            "DANMUJI_BACKEND_HOST": "127.0.0.1",
            "DANMUJI_BACKEND_PORT": str(port),
            "BILIPDJ_PORTABLE_AUTO_BACKEND": "0",
        })
        # Keep Windows CI's inherited proxy settings from changing loopback checks.
        env["NO_PROXY"] = env["no_proxy"] = "localhost,127.0.0.1"
        output = root / "backend-output.log"
        with output.open("wb") as stream:
            proc = subprocess.Popen(
                [str(exe.resolve()), "--backend"],
                cwd=str(exe.resolve().parent),
                env=env, stdout=stream, stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 45
                last_error = ""
                while time.monotonic() < deadline:
                    if proc.poll() is not None:
                        raise AssertionError(
                            f"Frozen backend exited early with code {proc.returncode} "
                            "(inspect backend startup logs privately)"
                        )
                    try:
                        healthy = fetch_json(f"http://127.0.0.1:{port}/api/config/basic")
                        if not isinstance(healthy, dict):
                            raise AssertionError("Missing backend config")
                        login = fetch_json(f"http://127.0.0.1:{port}/api/douyin/login/status")
                        assert login.get("status") == "ok", "Login status endpoint missing"
                        assert login.get("state") in {"idle", "success", "waiting_scan", "failed"}
                        assert login.get("logged_in") is False, "Fresh profile cannot be logged in"
                        assert "cookie" not in login and "token" not in login
                        assert "sessionid" not in json.dumps(login).lower()
                        print("Issue #325 frozen Windows backend + Douyin login HTTP smoke: PASS")
                        return
                    except (urllib.error.URLError, OSError, ValueError, AssertionError) as exc:
                        last_error = f"{type(exc).__name__}"
                    time.sleep(0.3)
                raise AssertionError(
                    f"Frozen backend not ready within 45s; last error type: {last_error}"
                )
            finally:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--exe", type=Path, required=True)
    args = parser.parse_args()
    if sys.platform != "win32":
        raise RuntimeError("Frozen backend smoke is a Windows-only CI check")
    run_frozen_backend(args.exe)


if __name__ == "__main__":
    main()
