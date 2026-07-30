from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from core.openai_api import API_STYLE_RESPONSES, OpenAIAnswerClient
from core.runtime_guards import cleanup_update_residue, install_openai_protocol_guard


class _Endpoint:
    def __init__(self, response) -> None:
        self.response = response
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(dict(kwargs))
        return self.response


class _FakeClient:
    def __init__(self) -> None:
        response = SimpleNamespace(
            output_text="ok",
            output=[],
            id="response-1",
            model="custom-model-b",
            usage=None,
        )
        self.responses = _Endpoint(response)
        self.chat = SimpleNamespace(completions=_Endpoint(None))


class RuntimeGuardTests(unittest.TestCase):
    def test_custom_model_override_keeps_client_protocol(self) -> None:
        install_openai_protocol_guard()
        fake = _FakeClient()
        client = OpenAIAnswerClient(
            provider="custom",
            base_url="http://127.0.0.1:8000/v1",
            model="custom-model-a",
            api_style=API_STYLE_RESPONSES,
            client=fake,
        )
        answer = client.ask("question", model="custom-model-b")
        self.assertEqual(answer.api_style, API_STYLE_RESPONSES)
        self.assertEqual(len(fake.responses.calls), 1)
        self.assertEqual(fake.chat.completions.calls, [])

    def test_recorded_update_directory_is_removed(self) -> None:
        work_dir = Path(tempfile.mkdtemp(prefix="bilipdj-update-"))
        (work_dir / "package.zip").write_bytes(b"archive")
        with tempfile.TemporaryDirectory() as app_temp:
            app_dir = Path(app_temp)
            (app_dir / "update-result.json").write_text(
                json.dumps({"cleanup_dir": str(work_dir)}),
                encoding="utf-8",
            )
            self.assertTrue(cleanup_update_residue(app_dir, attempts=1))
            self.assertFalse(work_dir.exists())

    def test_cleanup_marker_cannot_delete_arbitrary_directory(self) -> None:
        with tempfile.TemporaryDirectory(prefix="not-an-update-") as target_temp:
            target = Path(target_temp)
            marker_root = Path(tempfile.mkdtemp(prefix="bilipdj-marker-"))
            try:
                (marker_root / "update-result.json").write_text(
                    json.dumps({"cleanup_dir": str(target)}),
                    encoding="utf-8",
                )
                self.assertFalse(cleanup_update_residue(marker_root, attempts=1))
                self.assertTrue(target.exists())
            finally:
                import shutil

                shutil.rmtree(marker_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
