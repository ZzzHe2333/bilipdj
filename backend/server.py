from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 9816

ROOT_DIR = Path(__file__).resolve().parent.parent
MODEL_JSON_PATH = ROOT_DIR / "models" / "danmuji_initial_model.json"


def load_model() -> dict[str, Any]:
    """Load the initial model JSON from disk."""
    with MODEL_JSON_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


class ApiHandler(BaseHTTPRequestHandler):
    server_version = "DanmujiBackend/0.1"

    def _write_json(self, payload: dict[str, Any], status: int = HTTPStatus.OK) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._write_json(
                {
                    "status": "ok",
                    "service": "danmuji-python-backend",
                    "port": self.server.server_port,
                }
            )
            return

        if self.path == "/model":
            try:
                model = load_model()
                self._write_json({"status": "ok", "model": model})
            except FileNotFoundError:
                self._write_json(
                    {
                        "status": "error",
                        "message": f"Model file not found: {MODEL_JSON_PATH}",
                    },
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            except json.JSONDecodeError as exc:
                self._write_json(
                    {
                        "status": "error",
                        "message": f"Model JSON is invalid: {exc}",
                    },
                    status=HTTPStatus.INTERNAL_SERVER_ERROR,
                )
            return

        if self.path == "/":
            self._write_json(
                {
                    "status": "ok",
                    "message": "Danmuji Python backend is running.",
                    "endpoints": ["/health", "/model"],
                }
            )
            return

        self._write_json(
            {"status": "error", "message": f"Path not found: {self.path}"},
            status=HTTPStatus.NOT_FOUND,
        )


def run_server(host: str = DEFAULT_HOST, port: int = DEFAULT_PORT) -> None:
    httpd = ThreadingHTTPServer((host, port), ApiHandler)
    print(f"Danmuji backend started on http://{host}:{port}")
    httpd.serve_forever()


if __name__ == "__main__":
    host = os.getenv("DANMUJI_BACKEND_HOST", DEFAULT_HOST)
    port = int(os.getenv("DANMUJI_BACKEND_PORT", DEFAULT_PORT))
    run_server(host=host, port=port)
