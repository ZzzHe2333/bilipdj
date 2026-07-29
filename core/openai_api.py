"""Reusable multi-provider interface built on the official OpenAI Python SDK.

Provider presets are conveniences only. Explicit ``api_key``, ``base_url``,
``model`` and ``api_style`` values always override them. Importing this module
performs no network request.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

API_STYLE_RESPONSES = "responses"
API_STYLE_CHAT_COMPLETIONS = "chat_completions"
SUPPORTED_API_STYLES = frozenset({API_STYLE_RESPONSES, API_STYLE_CHAT_COMPLETIONS})


class OpenAIInterfaceError(RuntimeError):
    """Base exception raised by this interface."""


class OpenAIConfigurationError(OpenAIInterfaceError):
    """Raised when provider configuration is missing or incompatible."""


class OpenAISDKNotInstalledError(OpenAIInterfaceError):
    """Raised when the OpenAI Python SDK is unavailable."""


class OpenAIRequestError(OpenAIInterfaceError):
    """Raised when a model request fails."""


class OpenAIEmptyResponseError(OpenAIInterfaceError):
    """Raised when the response contains no readable text."""


@dataclass(frozen=True, slots=True)
class ProviderModel:
    id: str
    name: str
    api_style: str | None = None


@dataclass(frozen=True, slots=True)
class ProviderPreset:
    id: str
    name: str
    base_url: str
    api_key_envs: tuple[str, ...]
    model_envs: tuple[str, ...]
    base_url_envs: tuple[str, ...]
    default_model: str
    default_api_style: str
    models: tuple[ProviderModel, ...]
    api_key_required: bool = True
    chat_token_parameter: str = "max_tokens"
    responses_store_default: bool | None = None


@dataclass(frozen=True, slots=True)
class ResolvedProviderConfig:
    provider: str
    provider_name: str
    api_key: str
    base_url: str
    model: str
    api_style: str
    chat_token_parameter: str
    responses_store_default: bool | None


@dataclass(frozen=True, slots=True)
class OpenAIAnswer:
    text: str
    model: str
    provider: str = ""
    api_style: str = ""
    response_id: str = ""
    request_id: str = ""
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


def _model(
    model_id: str,
    name: str | None = None,
    api_style: str | None = None,
) -> ProviderModel:
    return ProviderModel(model_id, name or model_id, api_style)


PROVIDER_PRESETS: dict[str, ProviderPreset] = {
    "openai": ProviderPreset(
        id="openai",
        name="OpenAI",
        base_url="https://api.openai.com/v1",
        api_key_envs=("OPENAI_API_KEY",),
        model_envs=("OPENAI_MODEL",),
        base_url_envs=("OPENAI_BASE_URL",),
        default_model="gpt-5.6",
        default_api_style=API_STYLE_RESPONSES,
        models=(
            _model("gpt-5.6", "GPT-5.6"),
            _model("gpt-5.6-sol", "GPT-5.6 Sol"),
            _model("gpt-5.6-terra", "GPT-5.6 Terra"),
            _model("gpt-5.6-luna", "GPT-5.6 Luna"),
        ),
        responses_store_default=False,
    ),
    "deepseek": ProviderPreset(
        id="deepseek",
        name="DeepSeek",
        base_url="https://api.deepseek.com",
        api_key_envs=("DEEPSEEK_API_KEY",),
        model_envs=("DEEPSEEK_MODEL",),
        base_url_envs=("DEEPSEEK_BASE_URL",),
        default_model="deepseek-v4-flash",
        default_api_style=API_STYLE_CHAT_COMPLETIONS,
        models=(
            _model("deepseek-v4-flash", "DeepSeek V4 Flash"),
            _model("deepseek-v4-pro", "DeepSeek V4 Pro"),
        ),
    ),
    "minimax": ProviderPreset(
        id="minimax",
        name="MiniMax",
        base_url="https://api.minimaxi.com/v1",
        api_key_envs=("MINIMAX_API_KEY",),
        model_envs=("MINIMAX_MODEL",),
        base_url_envs=("MINIMAX_BASE_URL",),
        default_model="MiniMax-M2.7",
        default_api_style=API_STYLE_CHAT_COMPLETIONS,
        models=tuple(
            _model(model_id)
            for model_id in (
                "MiniMax-M2.7",
                "MiniMax-M2.7-highspeed",
                "MiniMax-M2.5",
                "MiniMax-M2.5-highspeed",
                "MiniMax-M2.1",
                "MiniMax-M2.1-highspeed",
                "MiniMax-M2",
            )
        ),
        chat_token_parameter="max_completion_tokens",
    ),
    "qwen": ProviderPreset(
        id="qwen",
        name="Qwen / 阿里云百炼",
        base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        api_key_envs=("DASHSCOPE_API_KEY", "QWEN_API_KEY"),
        model_envs=("QWEN_MODEL", "DASHSCOPE_MODEL"),
        base_url_envs=("QWEN_BASE_URL", "DASHSCOPE_BASE_URL"),
        default_model="qwen3.7-plus",
        default_api_style=API_STYLE_CHAT_COMPLETIONS,
        models=(
            _model("qwen3.7-max", "Qwen3.7 Max"),
            _model("qwen3.7-plus", "Qwen3.7 Plus"),
            _model("qwen3.7-flash", "Qwen3.7 Flash"),
            _model("qwen3.6-flash", "Qwen3.6 Flash"),
            _model("qwen3-coder-plus", "Qwen3 Coder Plus"),
        ),
    ),
    "xiaomi_mimo": ProviderPreset(
        id="xiaomi_mimo",
        name="Xiaomi MiMo",
        base_url="https://api.xiaomimimo.com/v1",
        api_key_envs=("MIMO_API_KEY", "XIAOMIMIMO_API_KEY"),
        model_envs=("MIMO_MODEL", "XIAOMIMIMO_MODEL"),
        base_url_envs=("MIMO_BASE_URL", "XIAOMIMIMO_BASE_URL"),
        default_model="mimo-v2.5-pro",
        default_api_style=API_STYLE_CHAT_COMPLETIONS,
        models=(
            _model("mimo-v2.5-pro", "MiMo V2.5 Pro"),
            _model("mimo-v2.5", "MiMo V2.5"),
        ),
        chat_token_parameter="max_completion_tokens",
    ),
    "claude": ProviderPreset(
        id="claude",
        name="Claude / Anthropic",
        base_url="https://api.anthropic.com/v1",
        api_key_envs=("ANTHROPIC_API_KEY",),
        model_envs=("ANTHROPIC_MODEL", "CLAUDE_MODEL"),
        base_url_envs=("ANTHROPIC_BASE_URL", "CLAUDE_BASE_URL"),
        default_model="claude-sonnet-5",
        default_api_style=API_STYLE_CHAT_COMPLETIONS,
        models=(
            _model("claude-opus-5", "Claude Opus 5"),
            _model("claude-sonnet-5", "Claude Sonnet 5"),
            _model("claude-sonnet-4-6", "Claude Sonnet 4.6"),
            _model("claude-haiku-4-5", "Claude Haiku 4.5"),
        ),
    ),
    "opencode": ProviderPreset(
        id="opencode",
        name="OpenCode Zen",
        base_url="https://opencode.ai/zen/v1",
        api_key_envs=("OPENCODE_API_KEY", "OPENCODE_ZEN_API_KEY"),
        model_envs=("OPENCODE_MODEL",),
        base_url_envs=("OPENCODE_BASE_URL",),
        default_model="gpt-5.6-terra",
        default_api_style=API_STYLE_RESPONSES,
        models=(
            _model("gpt-5.6-sol", "GPT-5.6 Sol", API_STYLE_RESPONSES),
            _model("gpt-5.6-terra", "GPT-5.6 Terra", API_STYLE_RESPONSES),
            _model("gpt-5.6-luna", "GPT-5.6 Luna", API_STYLE_RESPONSES),
            _model("deepseek-v4-pro", "DeepSeek V4 Pro", API_STYLE_CHAT_COMPLETIONS),
            _model("deepseek-v4-flash", "DeepSeek V4 Flash", API_STYLE_CHAT_COMPLETIONS),
            _model("minimax-m3", "MiniMax M3", API_STYLE_CHAT_COMPLETIONS),
            _model("minimax-m2.7", "MiniMax M2.7", API_STYLE_CHAT_COMPLETIONS),
            _model("minimax-m2.5", "MiniMax M2.5", API_STYLE_CHAT_COMPLETIONS),
            _model("glm-5.2", "GLM 5.2", API_STYLE_CHAT_COMPLETIONS),
            _model("kimi-k2.7-code", "Kimi K2.7 Code", API_STYLE_CHAT_COMPLETIONS),
            _model("mimo-v2.5-free", "MiMo V2.5 Free", API_STYLE_CHAT_COMPLETIONS),
        ),
        responses_store_default=False,
    ),
    "custom": ProviderPreset(
        id="custom",
        name="自定义 OpenAI 兼容服务",
        base_url="",
        api_key_envs=("OPENAI_API_KEY",),
        model_envs=("OPENAI_MODEL",),
        base_url_envs=("OPENAI_BASE_URL",),
        default_model="",
        default_api_style=API_STYLE_CHAT_COMPLETIONS,
        models=(),
        api_key_required=False,
    ),
}

_PROVIDER_ALIASES = {
    "anthropic": "claude",
    "dashscope": "qwen",
    "aliyun": "qwen",
    "mimo": "xiaomi_mimo",
    "xiaomimimo": "xiaomi_mimo",
    "xiaomi-mimo": "xiaomi_mimo",
    "opencode-zen": "opencode",
    "opencode_zen": "opencode",
    "zen": "opencode",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first_environment_value(names: Sequence[str]) -> str:
    for name in names:
        value = _text(os.getenv(name))
        if value:
            return value
    return ""


def normalize_provider_name(provider: str | None) -> str:
    raw = _text(
        provider
        or os.getenv("LLM_PROVIDER")
        or os.getenv("OPENAI_PROVIDER")
        or "openai"
    ).lower()
    canonical = _PROVIDER_ALIASES.get(raw, raw)
    if canonical not in PROVIDER_PRESETS:
        choices = ", ".join(sorted(PROVIDER_PRESETS))
        raise OpenAIConfigurationError(
            f"Unknown provider {raw!r}; available providers: {choices}"
        )
    return canonical


def get_provider_preset(provider: str | None) -> ProviderPreset:
    return PROVIDER_PRESETS[normalize_provider_name(provider)]


def list_provider_presets() -> tuple[ProviderPreset, ...]:
    return tuple(PROVIDER_PRESETS.values())


def list_provider_models(provider: str | None) -> tuple[ProviderModel, ...]:
    return get_provider_preset(provider).models


def get_provider_catalog() -> dict[str, dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for provider_id, preset in PROVIDER_PRESETS.items():
        result[provider_id] = {
            "id": preset.id,
            "name": preset.name,
            "base_url": preset.base_url,
            "default_model": preset.default_model,
            "default_api_style": preset.default_api_style,
            "api_key_required": preset.api_key_required,
            "api_key_envs": list(preset.api_key_envs),
            "model_envs": list(preset.model_envs),
            "base_url_envs": list(preset.base_url_envs),
            "models": [
                {
                    "id": model.id,
                    "name": model.name,
                    "api_style": model.api_style or preset.default_api_style,
                }
                for model in preset.models
            ],
        }
    return result


def _resolve_api_style(
    preset: ProviderPreset,
    model: str,
    explicit: str | None,
) -> str:
    style = _text(explicit).lower() if explicit else preset.default_api_style
    if not explicit:
        for item in preset.models:
            if item.id == model and item.api_style:
                style = item.api_style
                break
    if style not in SUPPORTED_API_STYLES:
        raise OpenAIConfigurationError(
            "api_style must be 'responses' or 'chat_completions'"
        )
    return style


def resolve_provider_config(
    *,
    provider: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_style: str | None = None,
    require_api_key: bool = True,
) -> ResolvedProviderConfig:
    """Resolve settings without forwarding an OpenAI key to a third-party host."""

    preset = get_provider_preset(provider)
    resolved_model = _text(
        model
        or _first_environment_value(preset.model_envs)
        or os.getenv("LLM_MODEL")
        or preset.default_model
    )
    if not resolved_model:
        raise OpenAIConfigurationError("Model is required")

    resolved_url = _text(
        base_url
        or _first_environment_value(preset.base_url_envs)
        or os.getenv("LLM_BASE_URL")
        or preset.base_url
    )
    if not resolved_url:
        raise OpenAIConfigurationError("Base URL is required for a custom provider")

    # OPENAI_API_KEY is deliberately not a fallback for third-party presets.
    resolved_key = _text(
        api_key
        or _first_environment_value(preset.api_key_envs)
        or os.getenv("LLM_API_KEY")
    )
    if preset.api_key_required and require_api_key and not resolved_key:
        env_hint = " / ".join(preset.api_key_envs)
        raise OpenAIConfigurationError(
            f"{preset.name} API key is required; set {env_hint}"
        )

    return ResolvedProviderConfig(
        provider=preset.id,
        provider_name=preset.name,
        api_key=resolved_key or "not-needed",
        base_url=resolved_url.rstrip("/"),
        model=resolved_model,
        api_style=_resolve_api_style(preset, resolved_model, api_style),
        chat_token_parameter=preset.chat_token_parameter,
        responses_store_default=preset.responses_store_default,
    )


def _usage_value(usage: Any, *names: str) -> int | None:
    if usage is None:
        return None
    for name in names:
        value = usage.get(name) if isinstance(usage, Mapping) else getattr(usage, name, None)
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _responses_text(response: Any) -> str:
    direct = _text(getattr(response, "output_text", ""))
    if direct:
        return direct

    chunks: list[str] = []
    for item in getattr(response, "output", None) or []:
        content = item.get("content") if isinstance(item, Mapping) else getattr(item, "content", None)
        for part in content or []:
            kind = _text(
                part.get("type")
                if isinstance(part, Mapping)
                else getattr(part, "type", "")
            )
            value = (
                part.get("text")
                if isinstance(part, Mapping)
                else getattr(part, "text", None)
            )
            if kind in {"", "text", "output_text"} and _text(value):
                chunks.append(_text(value))
    return "\n".join(chunks).strip()


def _chat_text(response: Any) -> str:
    choices = getattr(response, "choices", None) or []
    if not choices:
        return ""

    first = choices[0]
    message = first.get("message") if isinstance(first, Mapping) else getattr(first, "message", None)
    if message is None:
        return ""

    content = message.get("content") if isinstance(message, Mapping) else getattr(message, "content", None)
    if isinstance(content, str):
        return content.strip()

    chunks: list[str] = []
    for part in content or []:
        value = (
            part.get("text", part.get("content", ""))
            if isinstance(part, Mapping)
            else getattr(part, "text", getattr(part, "content", ""))
        )
        if _text(value):
            chunks.append(_text(value))
    return "\n".join(chunks).strip()


def _validate_provider_parameters(
    provider: str,
    model: str,
    temperature: float | None,
    top_p: float | None,
) -> None:
    # Anthropic's OpenAI compatibility layer rejects non-default sampling
    # parameters for Claude Sonnet 5. Fail locally with a clear message.
    if provider == "claude" and model == "claude-sonnet-5":
        if temperature is not None or top_p is not None:
            raise OpenAIConfigurationError(
                "Claude Sonnet 5 OpenAI compatibility does not accept "
                "temperature or top_p overrides"
            )


class OpenAIAnswerClient:
    def __init__(
        self,
        *,
        provider: str | None = None,
        api_key: str | None = None,
        model: str | None = None,
        base_url: str | None = None,
        api_style: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
        client: Any | None = None,
    ) -> None:
        self.config = resolve_provider_config(
            provider=provider,
            api_key=api_key,
            model=model,
            base_url=base_url,
            api_style=api_style,
            require_api_key=client is None,
        )
        self.provider = self.config.provider
        self.model = self.config.model
        self.base_url = self.config.base_url
        self.api_style = self.config.api_style

        if client is not None:
            self._client = client
            return

        try:
            from openai import OpenAI
        except ImportError as exc:
            raise OpenAISDKNotInstalledError(
                "Install the project's openai dependency"
            ) from exc

        self._client = OpenAI(
            api_key=self.config.api_key,
            base_url=self.base_url,
            timeout=max(0.1, float(timeout)),
            max_retries=max(0, int(max_retries)),
        )

    def ask(
        self,
        prompt: str,
        *,
        instructions: str | None = None,
        model: str | None = None,
        api_style: str | None = None,
        max_output_tokens: int | None = 512,
        store: bool | None = None,
        metadata: Mapping[str, str] | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        extra_body: Mapping[str, Any] | None = None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> OpenAIAnswer:
        user_input = _text(prompt)
        if not user_input:
            raise ValueError("prompt must not be empty")

        selected_model = _text(model or self.model)
        style_hint = self.api_style if model is None and api_style is None else api_style
        style = _resolve_api_style(
            get_provider_preset(self.provider),
            selected_model,
            style_hint,
        )
        _validate_provider_parameters(
            self.provider,
            selected_model,
            temperature,
            top_p,
        )

        common: dict[str, Any] = {}
        if temperature is not None:
            common["temperature"] = float(temperature)
        if top_p is not None:
            common["top_p"] = float(top_p)
        if extra_body:
            common["extra_body"] = dict(extra_body)
        if extra_headers:
            common["extra_headers"] = {
                str(key): str(value) for key, value in extra_headers.items()
            }

        try:
            if style == API_STYLE_RESPONSES:
                request: dict[str, Any] = {
                    "model": selected_model,
                    "input": user_input,
                    **common,
                }
                effective_store = (
                    self.config.responses_store_default
                    if store is None
                    else bool(store)
                )
                if effective_store is not None:
                    request["store"] = effective_store
                if _text(instructions):
                    request["instructions"] = _text(instructions)
                if max_output_tokens is not None:
                    request["max_output_tokens"] = max(
                        1,
                        int(max_output_tokens),
                    )
                if metadata:
                    request["metadata"] = {
                        str(key): str(value)
                        for key, value in metadata.items()
                        if str(key).strip()
                    }
                response = self._client.responses.create(**request)
                answer = _responses_text(response)
            else:
                messages: list[dict[str, str]] = []
                if _text(instructions):
                    messages.append(
                        {"role": "system", "content": _text(instructions)}
                    )
                messages.append({"role": "user", "content": user_input})
                request = {
                    "model": selected_model,
                    "messages": messages,
                    **common,
                }
                if max_output_tokens is not None:
                    request[self.config.chat_token_parameter] = max(
                        1,
                        int(max_output_tokens),
                    )
                response = self._client.chat.completions.create(**request)
                answer = _chat_text(response)
        except Exception as exc:
            request_id = _text(
                getattr(exc, "request_id", "")
                or getattr(exc, "_request_id", "")
            )
            suffix = f" (request_id={request_id})" if request_id else ""
            raise OpenAIRequestError(
                f"{self.config.provider_name} request failed: {exc}{suffix}"
            ) from exc

        if not answer:
            response_id = _text(getattr(response, "id", ""))
            suffix = f" (response_id={response_id})" if response_id else ""
            raise OpenAIEmptyResponseError(
                f"{self.config.provider_name} response contained no text{suffix}"
            )

        usage = getattr(response, "usage", None)
        return OpenAIAnswer(
            text=answer,
            model=_text(getattr(response, "model", "")) or selected_model,
            provider=self.provider,
            api_style=style,
            response_id=_text(getattr(response, "id", "")),
            request_id=_text(
                getattr(response, "_request_id", "")
                or getattr(response, "request_id", "")
            ),
            input_tokens=_usage_value(usage, "input_tokens", "prompt_tokens"),
            output_tokens=_usage_value(
                usage,
                "output_tokens",
                "completion_tokens",
            ),
            total_tokens=_usage_value(usage, "total_tokens"),
        )


def get_model_answer(
    prompt: str,
    *,
    instructions: str | None = None,
    provider: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_style: str | None = None,
    timeout: float = 30.0,
    max_retries: int = 2,
    max_output_tokens: int | None = 512,
    store: bool | None = None,
    temperature: float | None = None,
    top_p: float | None = None,
    extra_body: Mapping[str, Any] | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> str:
    client = OpenAIAnswerClient(
        provider=provider,
        api_key=api_key,
        model=model,
        base_url=base_url,
        api_style=api_style,
        timeout=timeout,
        max_retries=max_retries,
    )
    return client.ask(
        prompt,
        instructions=instructions,
        max_output_tokens=max_output_tokens,
        store=store,
        temperature=temperature,
        top_p=top_p,
        extra_body=extra_body,
        extra_headers=extra_headers,
    ).text


ask_openai = get_model_answer

__all__ = [
    "API_STYLE_CHAT_COMPLETIONS",
    "API_STYLE_RESPONSES",
    "OpenAIAnswer",
    "OpenAIAnswerClient",
    "OpenAIConfigurationError",
    "OpenAIEmptyResponseError",
    "OpenAIInterfaceError",
    "OpenAIRequestError",
    "OpenAISDKNotInstalledError",
    "PROVIDER_PRESETS",
    "ProviderModel",
    "ProviderPreset",
    "ResolvedProviderConfig",
    "ask_openai",
    "get_model_answer",
    "get_provider_catalog",
    "get_provider_preset",
    "list_provider_models",
    "list_provider_presets",
    "normalize_provider_name",
    "resolve_provider_config",
]
