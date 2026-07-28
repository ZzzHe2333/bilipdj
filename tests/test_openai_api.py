from __future__ import annotations

import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.openai_api import (
    OpenAIAnswerClient,
    OpenAIConfigurationError,
    OpenAIEmptyResponseError,
    OpenAIRequestError,
    get_model_answer,
)


class _FakeResponses:
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self.response = response
        self.error = error
        self.calls: list[dict] = []

    def create(self, **kwargs):
        self.calls.append(dict(kwargs))
        if self.error is not None:
            raise self.error
        return self.response


class _FakeClient:
    def __init__(self, response=None, error: Exception | None = None) -> None:
        self.responses = _FakeResponses(response=response, error=error)


class OpenAIAnswerInterfaceTests(unittest.TestCase):
    def test_model_is_required(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(OpenAIConfigurationError):
                OpenAIAnswerClient(client=_FakeClient())

    def test_ask_returns_structured_answer_and_request_metadata(self) -> None:
        response = SimpleNamespace(
            output_text="队列答案",
            id="resp_123",
            model="test-model",
            _request_id="req_123",
            usage=SimpleNamespace(input_tokens=4, output_tokens=2, total_tokens=6),
        )
        fake = _FakeClient(response=response)
        client = OpenAIAnswerClient(model="test-model", client=fake)

        answer = client.ask(
            "现在第几名？",
            instructions="只回答结果",
            max_output_tokens=100,
            metadata={"feature": "queue-rank"},
        )

        self.assertEqual(answer.text, "队列答案")
        self.assertEqual(answer.response_id, "resp_123")
        self.assertEqual(answer.request_id, "req_123")
        self.assertEqual(answer.total_tokens, 6)
        self.assertEqual(
            fake.responses.calls,
            [
                {
                    "model": "test-model",
                    "input": "现在第几名？",
                    "store": False,
                    "instructions": "只回答结果",
                    "max_output_tokens": 100,
                    "metadata": {"feature": "queue-rank"},
                }
            ],
        )

    def test_output_items_are_used_when_output_text_is_empty(self) -> None:
        response = SimpleNamespace(
            output_text="",
            output=[SimpleNamespace(content=[SimpleNamespace(type="output_text", text="备用文本")])],
            id="resp_fallback",
            model="test-model",
            usage=None,
        )
        answer = OpenAIAnswerClient(model="test-model", client=_FakeClient(response=response)).ask("问题")
        self.assertEqual(answer.text, "备用文本")

    def test_empty_response_raises_clear_error(self) -> None:
        response = SimpleNamespace(output_text="", output=[], id="resp_empty", model="test-model")
        client = OpenAIAnswerClient(model="test-model", client=_FakeClient(response=response))
        with self.assertRaises(OpenAIEmptyResponseError) as ctx:
            client.ask("问题")
        self.assertIn("resp_empty", str(ctx.exception))

    def test_sdk_error_is_wrapped(self) -> None:
        source_error = RuntimeError("network unavailable")
        source_error.request_id = "req_failed"
        client = OpenAIAnswerClient(model="test-model", client=_FakeClient(error=source_error))
        with self.assertRaises(OpenAIRequestError) as ctx:
            client.ask("问题")
        self.assertIn("network unavailable", str(ctx.exception))
        self.assertIn("req_failed", str(ctx.exception))

    def test_convenience_function_uses_environment_configuration(self) -> None:
        response = SimpleNamespace(
            output_text="环境变量调用成功",
            id="resp_env",
            model="env-model",
            _request_id="req_env",
            usage=None,
        )
        created_clients: list[object] = []

        class FakeOpenAI:
            def __init__(self, **kwargs) -> None:
                self.options = kwargs
                self.responses = _FakeResponses(response=response)
                created_clients.append(self)

        fake_module = types.ModuleType("openai")
        fake_module.OpenAI = FakeOpenAI
        with patch.dict(
            sys.modules,
            {"openai": fake_module},
        ), patch.dict(
            os.environ,
            {
                "OPENAI_API_KEY": "test-key",
                "OPENAI_MODEL": "env-model",
                "OPENAI_BASE_URL": "https://example.invalid/v1",
            },
            clear=True,
        ):
            result = get_model_answer("测试问题")

        self.assertEqual(result, "环境变量调用成功")
        self.assertEqual(len(created_clients), 1)
        self.assertEqual(created_clients[0].options["api_key"], "test-key")
        self.assertEqual(created_clients[0].options["base_url"], "https://example.invalid/v1")
        self.assertEqual(created_clients[0].responses.calls[0]["store"], False)


if __name__ == "__main__":
    unittest.main()
