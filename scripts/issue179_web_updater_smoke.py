from __future__ import annotations

import json
import tempfile
import threading
import urllib.error
import urllib.request
from pathlib import Path

from apps.web.web_updater import UpdateHost


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="bilipdj-web-updater-smoke-") as temp:
        root = Path(temp)
        html = root / "update.html"
        html.write_text("<!doctype html><title>smoke</title>", encoding="utf-8")
        request = {
            "token": "0123456789abcdefghijklmnopqrstuvwxyz",
            "mode": "full",
            "target_version": "9.9.9",
            "return_url": "http://127.0.0.1:9816/control",
        }
        server = UpdateHost(("127.0.0.1", 0), request, html)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            port = server.server_port
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/update.html", timeout=2) as response:
                assert response.status == 200
                assert b"smoke" in response.read()
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/api/status?token={request['token']}", timeout=2
            ) as response:
                payload = json.loads(response.read().decode("utf-8"))
                assert payload["status"] == "running"
                assert payload["target_version"] == "9.9.9"
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{port}/api/status?token=wrong", timeout=2)
            except urllib.error.HTTPError as exc:
                assert exc.code == 403
            else:
                raise AssertionError("wrong updater token should be rejected")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    print("issue #179 Web updater HTTP smoke: OK")


if __name__ == "__main__":
    main()
