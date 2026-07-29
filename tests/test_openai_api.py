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
    def __init__(self, *, responses_response=None, chat_response=None, error=None) -> None:
        self.responses = _FakeEndpoint(responses_response, error)
        self.chat = SimpleNamespace(completions=_FakeEndpoint(chat_response, error))


class ProviderCatalogTests(unittest.TestCase):
    def test_required_presets_and_provider_catalog(self) -> None:
        catalog = get_provider_catalog()
        required = {
            "openai",
            "deepseek",
            "minimax",
            "qwen",
            "xiaomi_mimo",
            "claude",
            "opencode",
            "custom",
        }
        self.assertTrue(required.issubset(catalog))
        self.assertEqual(
            catalog["minimax"]["base_url"],
            "https://api.minimaxi.com/v1",
        )
        minimax_models = [item["id"] for item in catalog["minimax"]["models"]]
        self.assertIn("MiniMax-M2.7", minimax_models)
        self.assertNotIn("MiniMax-M3", minimax_models)
        self.assertIn(
            "minimax-m3",
            [item["id"] for item in catalog["opencode"]["models"]],
        )
        self.assertIn(
            "qwen3.7-flash",
            [item["id"] for item in catalog["qwen"]["models"]],
        )
        self.assertEqual(catalog["claude"]["default_model"], "claude-sonnet-5")
        self.assertFalse(catalog["custom"]["api_key_required"])

    def test_aliases(self) -> None:
        self.assertEqual(normalize_provider_name("anthropic"), "claude")
        self.assertEqual(normalize_provider_name("dashscope"), "qwen")
        self.assertEqual(normalize_provider_name("xiaomimimo"), "xiaomi_mimo")
        self.assertEqual(normalize_provider_name("opencode-zen"), "opencode")

    def test_openai_key_is_not_forwarded_to_third_party(self) -> None:
        with patch.dict(os.environ, {"OPENAI_API_KEY": "openai-secret"}, clear=True):
            with self.assertRaises(OpenAIConfigurationError):
                resolve_provider_config(provider="deepseek")

    def test_generic_llm_key_can_be_explicitly_shared(self) -> None:
        with patch.dict(os.environ, {"LLM_API_KEY": "runtime-key"}, clear=True):
            config = resolve_provider_config(provider="deepseek")
        self.assertEqual(config.api_key, "runtime-key")

    def test_keyless_custom_endpoint(self) -> None:
        config = resolve_provider_config(
            provider="custom",
            base_url="http://127.0.0.1:11434/v1/",
            model="local-model",
        )
        self.assertEqual(config.api_key, "not-needed")
        self.assertEqual(config.base_url, "http://127.0.0.1:11434/v1")

    def test_opencode_protocol_is_selected_per_model(self) -> None:
        responses = resolve_provider_config(
            provider="opencode",
            api_key="k",
            model="gpt-5.6-terra",
        )
        chat = resolve_provider_config(
            provider="opencode",
            api_key="k",
            model="deepseek-v4-flash",
        )
        self.assertEqual(responses.api_style, API_STYLE_RESPONSES)
        self.assertEqual(chat.api_style, API_STYLE_CHAT_COMPLETIONS)


class OpenAIAnswerInterfaceTests(unittest.TestCase):
    def test_responses_api_privacy_default(self) -> None:
        response = SimpleNamespace(
            output_text="answer",
            id="resp_1",
            model="gpt-5.6",
            _request_id="req_1",
            usage=SimpleNamespace(input_tokens=1, output_tokens=2, total_tokens=3),
        )
        fake = _FakeClient(responses_response=response)
        answer = OpenAIAnswerClient(provider="openai", client=fake).ask("question")
        self.assertEqual(answer.text, "answer")
        self.assertFalse(fake.responses.calls[0]["store"])

    def test_custom_responses_does_not_send_store_by_default(self) -> None:
        response = SimpleNamespace(output_text="answer", id="r", model="m", usage=None)
        fake = _FakeClient(responses_response=response)
        client = OpenAIAnswerClient(
            provider="custom",
            base_url="http://localhost:8000/v1",
            model="m",
            api_style="responses",
            client=fake,
        )
        client.ask("question")
        self.assertNotIn("store", fake.responses.calls[0])

    def test_minimax_uses_max_completion_tokens(self) -> None:
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
            id="c",
            model="MiniMax-M2.7",
            usage=None,
        )
        fake = _FakeClient(chat_response=response)
        client = OpenAIAnswerClient(provider="minimax", client=fake)
        client.ask("question", max_output_tokens=123)
        self.assertEqual(
            fake.chat.completions.calls[0]["max_completion_tokens"],
            123,
        )
        self.assertNotIn("max_tokens", fake.chat.completions.calls[0])

    def test_claude_sonnet_5_rejects_sampling_overrides_locally(self) -> None:
        fake = _FakeClient()
        client = OpenAIAnswerClient(provider="claude", client=fake)
        with self.assertRaises(OpenAIConfigurationError):
            client.ask("question", temperature=0.5)
        self.assertEqual(fake.chat.completions.calls, [])

    def test_opencode_model_override_switches_protocol(self) -> None:
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="ok"))],
            id="c",
            model="deepseek-v4-pro",
            usage=None,
        )
        fake = _FakeClient(chat_response=response)
        client = OpenAIAnswerClient(
            provider="opencode",
            model="gpt-5.6-terra",
            client=fake,
        )
        result = client.ask("question", model="deepseek-v4-pro")
        self.assertEqual(result.api_style, API_STYLE_CHAT_COMPLETIONS)
        self.assertEqual(len(fake.chat.completions.calls), 1)
        self.assertEqual(fake.responses.calls, [])

    def test_empty_response_and_request_error(self) -> None:
        empty = SimpleNamespace(
            output_text="",
            output=[],
            id="empty",
            model="gpt-5.6",
        )
        with self.assertRaises(OpenAIEmptyResponseError):
            OpenAIAnswerClient(
                provider="openai",
                client=_FakeClient(responses_response=empty),
            ).ask("q")

        error = RuntimeError("network")
        error.request_id = "req-failed"
        with self.assertRaises(OpenAIRequestError) as ctx:
            OpenAIAnswerClient(
                provider="deepseek",
                client=_FakeClient(error=error),
            ).ask("q")
        self.assertIn("req-failed", str(ctx.exception))

    def test_convenience_function_builds_provider_client(self) -> None:
        response = SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="qwen answer"))],
            id="q",
            model="qwen3.7-plus",
            usage=None,
        )
        created = []

        class FakeOpenAI:
            def __init__(self, **kwargs):
                self.options = kwargs
                self.responses = _FakeEndpoint()
                self.chat = SimpleNamespace(completions=_FakeEndpoint(response))
                created.append(self)

        fake_module = types.ModuleType("openai")
        fake_module.OpenAI = FakeOpenAI
        with patch.dict(sys.modules, {"openai": fake_module}), patch.dict(
            os.environ,
            {"DASHSCOPE_API_KEY": "key", "QWEN_MODEL": "qwen3.7-plus"},
            clear=True,
        ):
            result = get_model_answer("q", provider="qwen")
        self.assertEqual(result, "qwen answer")
        self.assertEqual(
            created[0].options["base_url"],
            "https://dashscope.aliyuncs.com/compatible-mode/v1",
        )


if __name__ == "__main__":
    unittest.main()
