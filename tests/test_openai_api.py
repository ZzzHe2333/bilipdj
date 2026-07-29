from __future__ import annotations

import os
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from core.openai_api import (
    API_STYLE_CHAT_COMPLETIONS,
    API_STYLE_RESPONSES,
    OpenAIAnswerClient,
    OpenAIConfigurationError,
    OpenAIEmptyResponseError,
    OpenAIRequestError,
    get_model_answer,
    get_provider_catalog,
    list_provider_models,
    normalize_provider_name,
    resolve_provider_config,
)


class _FakeEndpoint:
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
    def __init__(
        self,
        *,
        responses_response=None,
        chat_response=None,
        error: Exception | None = None,
    ) -> None:
        self.responses = _FakeEndpoint(responses_response, error=error)
        self.chat = SimpleNamespace(
            completions=_FakeEndpoint(chat_response, error=error)
        )


class ProviderCatalogTests(unittest.TestCase):
    def test_required_provider_presets_are_available(self) -> None:
        catalog = get_provider_catalog()
        self.assertTrue(
            {
                "openai",
                "deepseek",
                "minimax",
                "qwen",
                "xiaomi_mimo",
                "claude",
                "opencode",
                "custom",
            }.issubset(catalog)
        )
        self.assertIn("gpt-5.6-terra", [m["id"] for m in catalog["openai"]["models"]])
        self.assertIn("deepseek-v4-flash", [m["id"] for m in catalog["deepseek"]["models"]])
        self.assertIn("MiniMax-M2.7", [m["id"] for m in catalog["minimax"]["models"]])
        self.assertIn("qwen3.7-plus", [m["id"] for m in catalog["qwen"]["models"]])
        self.assertIn("mimo-v2.5-pro", [m["id"] for m in catalog["xiaomi_mimo"]["models"]])
        self.assertIn("claude-sonnet-4-6", [m["id"] for m in catalog["claude"]["models"]])

    def test_provider_aliases_are_supported(self) -> None:
        self.assertEqual(normalize_provider_name("anthropic"), "claude")
        self.assertEqual(normalize_provider_name("dashscope"), "qwen")
        self.assertEqual(normalize_provider_name("xiaomimimo"), "xiaomi_mimo")
        self.assertEqual(normalize_provider_name("opencode-zen"), "opencode")

    def test_unknown_provider_is_rejected(self) -> None:
        with self.assertRaises(OpenAIConfigurationError):
            normalize_provider_name("unknown-provider")

    def test_curated_models_do_not_block_custom_model_ids(self) -> None:
        self.assertGreater(len(list_provider_models("deepseek")), 0)
        config = resolve_provider_config(
            provider="custom",
            api_key="key",
            base_url="https://gateway.example/v1",
            model="vendor/private-model-2026",
            api_style=API_STYLE_RESPONSES,
        )
        self.assertEqual(config.model, "vendor/private-model-2026")
        self.assertEqual(config.base_url, "https://gateway.example/v1")
        self.assertEqual(config.api_style, API_STYLE_RESPONSES)

    def test_provider_environment_and_defaults_are_resolved(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DEEPSEEK_API_KEY": "deepseek-key",
                "DEEPSEEK_MODEL": "deepseek-v4-pro",
            },
            clear=True,
        ):
            config = resolve_provider_config(provider="deepseek")
        self.assertEqual(config.api_key, "deepseek-key")
        self.assertEqual(config.model, "deepseek-v4-pro")
        self.assertEqual(config.base_url, "https://api.deepseek.com")
        self.assertEqual(config.api_style, API_STYLE_CHAT_COMPLETIONS)

    def test_opencode_protocol_is_selected_per_model(self) -> None:
        responses_config = resolve_provider_config(
            provider="opencode",
            api_key="zen-key",
            model="gpt-5.6-terra",
        )
        chat_config = resolve_provider_config(
            provider="opencode",
            api_key="zen-key",
            model="deepseek-v4-flash",
        )
        self.assertEqual(responses_config.api_style, API_STYLE_RESPONSES)
        self.assertEqual(chat_config.api_style, API_STYLE_CHAT_COMPLETIONS)


class OpenAIAnswerInterfaceTests(unittest.TestCase):
    def test_responses_api_returns_structured_answer(self) -> None:
        response = SimpleNamespace(
            output_text="Responses 答案",
            id="resp_123",
            model="gpt-5.6-terra",
            _request_id="req_123",
            usage=SimpleNamespace(input_tokens=4, output_tokens=2, total_tokens=6),
        )
        fake = _FakeClient(responses_response=response)
        client = OpenAIAnswerClient(
            provider="openai",
            model="gpt-5.6-terra",
            client=fake,
        )

        answer = client.ask(
            "现在第几名？",
            instructions="只回答结果",
            max_output_tokens=100,
            metadata={"feature": "queue-rank"},
        )

        self.assertEqual(answer.text, "Responses 答案")
        self.assertEqual(answer.provider, "openai")
        self.assertEqual(answer.api_style, API_STYLE_RESPONSES)
        self.assertEqual(answer.total_tokens, 6)
        self.assertEqual(
            fake.responses.calls,
            [
                {
                    "model": "gpt-5.6-terra",
                    "input": "现在第几名？",
                    "store": False,
                    "instructions": "只回答结果",
                    "max_output_tokens": 100,
                    "metadata": {"feature": "queue-rank"},
                }
            ],
        )
        self.assertEqual(fake.chat.completions.calls, [])

    def test_chat_completions_provider_uses_messages(self) -> None:
        response = SimpleNamespace(
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content="DeepSeek 答案")
                )
            ],
            id="chat_123",
            model="deepseek-v4-flash",
            _request_id="req_chat",
            usage=SimpleNamespace(
                prompt_tokens=5,
                completion_tokens=3,
                total_tokens=8,
            ),
        )
        fake = _FakeClient(chat_response=response)
        client = OpenAIAnswerClient(
            provider="deepseek",
            model="deepseek-v4-flash",
            client=fake,
        )

        answer = client.ask(
            "问题",
            instructions="系统提示",
            max_output_tokens=80,
            temperature=0.5,
        )

        self.assertEqual(answer.text, "DeepSeek 答案")
        self.assertEqual(answer.input_tokens, 5)
        self.assertEqual(answer.output_tokens, 3)
        self.assertEqual(answer.api_style, API_STYLE_CHAT_COMPLETIONS)
        self.assertEqual(
            fake.chat.completions.calls[0],
            {
                "model": "deepseek-v4-flash",
                "messages": [
                    {"role": "system", "content": "系统提示"},
                    {"role": "user", "content": "问题"},
                ],
                "temperature": 0.5,
                "max_tokens": 80,
            },
        )
        self.assertEqual(fake.responses.calls, [])

    def test_minimax_uses_its_compatible_token_parameter(self) -> None:
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="MiniMax 答案"))],
            id="mini_1",
            model="MiniMax-M2.7",
            usage=None,
        )
        fake = _FakeClient(chat_response=response)
        client = OpenAIAnswerClient(
            provider="minimax",
            model="MiniMax-M2.7",
            client=fake,
        )
        client.ask("问题", max_output_tokens=256)
        call = fake.chat.completions.calls[0]
        self.assertEqual(call["max_completion_tokens"], 256)
        self.assertNotIn("max_tokens", call)

    def test_opencode_model_override_switches_endpoint(self) -> None:
        chat_response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="Zen Chat"))],
            id="zen_chat",
            model="deepseek-v4-pro",
            usage=None,
        )
        fake = _FakeClient(chat_response=chat_response)
        client = OpenAIAnswerClient(
            provider="opencode",
            model="gpt-5.6-terra",
            client=fake,
        )
        answer = client.ask("问题", model="deepseek-v4-pro")
        self.assertEqual(answer.api_style, API_STYLE_CHAT_COMPLETIONS)
        self.assertEqual(len(fake.chat.completions.calls), 1)
        self.assertEqual(fake.responses.calls, [])

    def test_output_items_are_used_when_output_text_is_empty(self) -> None:
        response = SimpleNamespace(
            output_text="",
            output=[
                SimpleNamespace(
                    content=[SimpleNamespace(type="output_text", text="备用文本")]
                )
            ],
            id="resp_fallback",
            model="gpt-5.6-terra",
            usage=None,
        )
        answer = OpenAIAnswerClient(
            provider="openai",
            client=_FakeClient(responses_response=response),
        ).ask("问题")
        self.assertEqual(answer.text, "备用文本")

    def test_empty_response_raises_clear_error(self) -> None:
        response = SimpleNamespace(
            output_text="",
            output=[],
            id="resp_empty",
            model="gpt-5.6-terra",
        )
        client = OpenAIAnswerClient(
            provider="openai",
            client=_FakeClient(responses_response=response),
        )
        with self.assertRaises(OpenAIEmptyResponseError) as ctx:
            client.ask("问题")
        self.assertIn("resp_empty", str(ctx.exception))

    def test_sdk_error_is_wrapped_with_provider_name(self) -> None:
        source_error = RuntimeError("network unavailable")
        source_error.request_id = "req_failed"
        client = OpenAIAnswerClient(
            provider="deepseek",
            client=_FakeClient(error=source_error),
        )
        with self.assertRaises(OpenAIRequestError) as ctx:
            client.ask("问题")
        self.assertIn("DeepSeek", str(ctx.exception))
        self.assertIn("network unavailable", str(ctx.exception))
        self.assertIn("req_failed", str(ctx.exception))

    def test_convenience_function_builds_provider_specific_sdk_client(self) -> None:
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="千问答案"))],
            id="qwen_1",
            model="qwen3.7-plus",
            _request_id="req_qwen",
            usage=None,
        )
        created_clients: list[object] = []

        class FakeOpenAI:
            def __init__(self, **kwargs) -> None:
                self.options = kwargs
                self.responses = _FakeEndpoint()
                self.chat = SimpleNamespace(
                    completions=_FakeEndpoint(response)
                )
                created_clients.append(self)

        fake_module = types.ModuleType("openai")
        fake_module.OpenAI = FakeOpenAI
        with patch.dict(sys.modules, {"openai": fake_module}), patch.dict(
            os.environ,
            {
                "DASHSCOPE_API_KEY": "dashscope-key",
                "QWEN_MODEL": "qwen3.7-plus",
            },
            clear=True,
        ):
            result = get_model_answer("测试问题", provider="qwen")

        self.assertEqual(result, "千问答案")
        self.assertEqual(len(created_clients), 1)
        self.assertEqual(created_clients[0].options["api_key"], "dashscope-key")
        self.assertEqual(
            created_clients[0].options["base_url"],
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        self.assertEqual(
            created_clients[0].chat.completions.calls[0]["model"],
            "qwen3.7-plus",
        )


if __name__ == "__main__":
    unittest.main()
