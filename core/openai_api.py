"""Multi-provider OpenAI-compatible model interface.

The module keeps one public calling surface while supporting provider presets for
OpenAI, DeepSeek, MiniMax, Qwen, Xiaomi MiMo, Claude, OpenCode Zen, and fully
custom OpenAI-compatible endpoints.

Provider presets are conveniences, not restrictions: callers may always
override ``api_key``, ``base_url``, ``model`` and ``api_style``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

API_STYLE_RESPONSES = "responses"
API_STYLE_CHAT_COMPLETIONS = "chat_completions"
SUPPORTED_API_STYLES = frozenset(
    {API_STYLE_RESPONSES, API_STYLE_CHAT_COMPLETIONS}
)


class OpenAIInterfaceError(RuntimeError):
    """Base exception raised by this interface."""


class OpenAIConfigurationError(OpenAIInterfaceError):
    """Raised when required provider configuration is missing or invalid."""


class OpenAISDKNotInstalledError(OpenAIInterfaceError):
    """Raised when the ``openai`` package is unavailable."""


class OpenAIRequestError(OpenAIInterfaceError):
    """Raised when a model request fails."""


class OpenAIEmptyResponseError(OpenAIInterfaceError):
    """Raised when a model response contains no readable text."""


@dataclass(frozen=True, slots=True)
class ProviderModel:
    """One curated model entry belonging to a provider preset."""

    id: str
    name: str
    api_style: str | None = None


@dataclass(frozen=True, slots=True)
class ProviderPreset:
    """Static provider configuration used by :class:`OpenAIAnswerClient`."""

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


@dataclass(frozen=True, slots=True)
class ResolvedProviderConfig:
    """Final provider configuration after arguments and environment overrides."""

    provider: str
    provider_name: str
    api_key: str
    base_url: str
    model: str
    api_style: str
    chat_token_parameter: str


@dataclass(frozen=True, slots=True)
class OpenAIAnswer:
    """Structured model answer returned by :meth:`OpenAIAnswerClient.ask`."""

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
    display_name: str | None = None,
    api_style: str | None = None,
) -> ProviderModel:
    return ProviderModel(
        id=model_id,
        name=display_name or model_id,
        api_style=api_style,
    )


PROVIDER_PRESETS: dict[str, ProviderPreset] = {
    "openai": ProviderPreset(
        id="openai",
        name="OpenAI",
        base_url="https://api.openai.com/v1",
        api_key_envs=("OPENAI_API_KEY",),
        model_envs=("OPENAI_MODEL",),
        base_url_envs=("OPENAI_BASE_URL",),
        default_model="gpt-5.6-terra",
        default_api_style=API_STYLE_RESPONSES,
        models=(
            _model("gpt-5.6-sol", "GPT-5.6 Sol"),
            _model("gpt-5.6-terra", "GPT-5.6 Terra"),
            _model("gpt-5.6-luna", "GPT-5.6 Luna"),
        ),
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
        models=(
            _model("MiniMax-M2.7"),
            _model("MiniMax-M2.7-highspeed"),
            _model("MiniMax-M2.5"),
            _model("MiniMax-M2.5-highspeed"),
            _model("MiniMax-M2.1"),
            _model("MiniMax-M2.1-highspeed"),
            _model("MiniMax-M2"),
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
        default_api_style=API_STYLE_RESPONSES,
        models=(
            _model("mimo-v2.5-pro", "MiMo V2.5 Pro"),
            _model("mimo-v2.5", "MiMo V2.5"),
            _model("mimo-v2-pro", "MiMo V2 Pro"),
            _model("mimo-v2-omni", "MiMo V2 Omni"),
        ),
    ),
    "claude": ProviderPreset(
        id="claude",
        name="Claude / Anthropic",
        base_url="https://api.anthropic.com/v1/",
        api_key_envs=("ANTHROPIC_API_KEY",),
        model_envs=("ANTHROPIC_MODEL", "CLAUDE_MODEL"),
        base_url_envs=("ANTHROPIC_BASE_URL", "CLAUDE_BASE_URL"),
        default_model="claude-sonnet-4-6",
        default_api_style=API_STYLE_CHAT_COMPLETIONS,
        models=(
            _model("claude-opus-5", "Claude Opus 5"),
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
            _model(
                "deepseek-v4-pro",
                "DeepSeek V4 Pro",
                API_STYLE_CHAT_COMPLETIONS,
            ),
            _model(
                "deepseek-v4-flash",
                "DeepSeek V4 Flash",
                API_STYLE_CHAT_COMPLETIONS,
            ),
            _model("minimax-m3", "MiniMax M3", API_STYLE_CHAT_COMPLETIONS),
            _model("minimax-m2.7", "MiniMax M2.7", API_STYLE_CHAT_COMPLETIONS),
            _model("minimax-m2.5", "MiniMax M2.5", API_STYLE_CHAT_COMPLETIONS),
            _model("glm-5.2", "GLM 5.2", API_STYLE_CHAT_COMPLETIONS),
            _model(
                "kimi-k2.7-code",
                "Kimi K2.7 Code",
                API_STYLE_CHAT_COMPLETIONS,
            ),
            _model(
                "mimo-v2.5-free",
                "MiMo V2.5 Free",
                API_STYLE_CHAT_COMPLETIONS,
            ),
        ),
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
    ),
}

_PROVIDER_ALIASES: dict[str, str] = {
    "anthropic": "claude",
    "claude": "claude",
    "dashscope": "qwen",
    "aliyun": "qwen",
    "qwen": "qwen",
    "mimo": "xiaomi_mimo",
    "xiaomimimo": "xiaomi_mimo",
    "xiaomi-mimo": "xiaomi_mimo",
    "xiaomi_mimo": "xiaomi_mimo",
    "opencode-zen": "opencode",
    "opencode_zen": "opencode",
    "zen": "opencode",
}


def _clean_optional(value: Any) -> str:
    return str(value or "").strip()


def _first_environment_value(names: Sequence[str]) -> str:
    for name in names:
        value = _clean_optional(os.getenv(name))
        if value:
            return value
    return ""


def normalize_provider_name(provider: str | None) -> str:
    """Return the canonical provider ID and validate it."""

    raw = _clean_optional(
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
    """Return one provider preset by canonical name or alias."""

    return PROVIDER_PRESETS[normalize_provider_name(provider)]


def list_provider_presets() -> tuple[ProviderPreset, ...]:
    """Return all provider presets in display order."""

    return tuple(PROVIDER_PRESETS.values())


def list_provider_models(provider: str | None) -> tuple[ProviderModel, ...]:
    """Return curated models for one provider."""

    return get_provider_preset(provider).models


def get_provider_catalog() -> dict[str, dict[str, Any]]:
    """Return a JSON-serializable provider/model catalog for future UI use."""

    result: dict[str, dict[str, Any]] = {}
    for provider_id, preset in PROVIDER_PRESETS.items():
        result[provider_id] = {
            "id": preset.id,
            "name": preset.name,
            "base_url": preset.base_url,
            "default_model": preset.default_model,
            "default_api_style": preset.default_api_style,
            "api_key_envs": list(preset.api_key_envs),
            "model_envs": list(preset.model_envs),
            "base_url_envs": list(preset.base_url_envs),
            "models": [
                {
                    "id": item.id,
                    "name": item.name,
                    "api_style": item.api_style or preset.default_api_style,
                }
                for item in preset.models
            ],
        }
    return result


def _model_api_style(
    preset: ProviderPreset,
    model: str,
    explicit_api_style: str | None,
) -> str:
    if explicit_api_style:
        resolved = _clean_optional(explicit_api_style).lower()
    else:
        resolved = preset.default_api_style
        for item in preset.models:
            if item.id == model and item.api_style:
                resolved = item.api_style
                break
    if resolved not in SUPPORTED_API_STYLES:
        raise OpenAIConfigurationError(
            "api_style must be 'responses' or 'chat_completions'"
        )
    return resolved


def resolve_provider_config(
    *,
    provider: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    api_style: str | None = None,
    require_api_key: bool = True,
) -> ResolvedProviderConfig:
    """Resolve provider settings from arguments, environment and presets."""

    preset = get_provider_preset(provider)
    resolved_model = _clean_optional(
        model
        or _first_environment_value(preset.model_envs)
        or os.getenv("OPENAI_MODEL")
        or preset.default_model
    )
    if not resolved_model:
        raise OpenAIConfigurationError(
            "Model is required; pass model=... or configure a provider model variable"
        )

    resolved_base_url = _clean_optional(
        base_url
        or _first_environment_value(preset.base_url_envs)
        or (
            os.getenv("OPENAI_BASE_URL")
            if preset.id in {"openai", "custom"}
            else ""
        )
        or preset.base_url
    )
    if not resolved_base_url:
        raise OpenAIConfigurationError(
            "Base URL is required for a custom OpenAI-compatible provider"
        )

    resolved_api_key = _clean_optional(
        api_key
        or _first_environment_value(preset.api_key_envs)
        or (
            os.getenv("OPENAI_API_KEY")
            if preset.id not in {"openai", "custom"}
            else ""
        )
    )
    if preset.api_key_required and require_api_key and not resolved_api_key:
        env_hint = " / ".join(preset.api_key_envs)
        raise OpenAIConfigurationError(
            f"{preset.name} API key is required; pass api_key=... or set {env_hint}"
        )

    return ResolvedProviderConfig(
        provider=preset.id,
        provider_name=preset.name,
        api_key=resolved_api_key or "not-needed",
        base_url=resolved_base_url,
        model=resolved_model,
        api_style=_model_api_style(preset, resolved_model, api_style),
        chat_token_parameter=preset.chat_token_parameter,
    )


def _read_usage_value(usage: Any, *names: str) -> int | None:
    if usage is None:
        return None
    for name in names:
        value = (
            usage.get(name)
            if isinstance(usage, Mapping)
            else getattr(usage, name, None)
        )
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _extract_responses_text(response: Any) -> str:
    direct = _clean_optional(getattr(response, "output_text", ""))
    if direct:
        return direct

    chunks: list[str] = []
    output = getattr(response, "output", None)
    if not isinstance(output, (list, tuple)):
        return ""

    for item in output:
        content = (
            item.get("content")
            if isinstance(item, Mapping)
            else getattr(item, "content", None)
        )
        if not isinstance(content, (list, tuple)):
            continue
        for part in content:
            if isinstance(part, Mapping):
                part_type = _clean_optional(part.get("type"))
                text = part.get("text")
            else:
                part_type = _clean_optional(getattr(part, "type", ""))
                text = getattr(part, "text", None)
            if part_type in {"output_text", "text", ""} and text is not None:
                cleaned = _clean_optional(text)
                if cleaned:
                    chunks.append(cleaned)
    return "\n".join(chunks).strip()


def _extract_chat_text(response: Any) -> str:
    choices = getattr(response, "choices", None)
    if not isinstance(choices, (list, tuple)) or not choices:
        return ""
    first = choices[0]
    message = (
        first.get("message")
        if isinstance(first, Mapping)
        else getattr(first, "message", None)
    )
    if message is None:
        return ""
    content = (
        message.get("content")
        if isinstance(message, Mapping)
        else getattr(message, "content", None)
    )
    if isinstance(content, str):
        return content.strip()
    if not isinstance(content, (list, tuple)):
        return ""

    chunks: list[str] = []
    for part in content:
        if isinstance(part, Mapping):
            value = part.get("text", part.get("content", ""))
        else:
            value = getattr(part, "text", getattr(part, "content", ""))
        cleaned = _clean_optional(value)
        if cleaned:
            chunks.append(cleaned)
    return "\n".join(chunks).strip()


class OpenAIAnswerClient:
    """Reusable wrapper around the official ``openai.OpenAI`` client."""

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
        self.timeout = max(0.1, float(timeout))
        self.max_retries = max(0, int(max_retries))

        if client is not None:
            self._client = client
            return

        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - depends on installation
            raise OpenAISDKNotInstalledError(
                "The openai package is not installed; install project requirements"
            ) from exc

        self._client = OpenAI(
            api_key=self.config.api_key,
            base_url=self.config.base_url,
            timeout=self.timeout,
            max_retries=self.max_retries,
        )

    def ask(
        self,
        prompt: str,
        *,
        instructions: str | None = None,
        model: str | None = None,
        api_style: str | None = None,
        max_output_tokens: int | None = 512,
        store: bool = False,
        metadata: Mapping[str, str] | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        extra_body: Mapping[str, Any] | None = None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> OpenAIAnswer:
        """Call the selected provider and return a structured answer."""

        user_input = _clean_optional(prompt)
        if not user_input:
            raise ValueError("prompt must not be empty")

        resolved_model = _clean_optional(model or self.model)
        preset = get_provider_preset(self.provider)
        style_override = api_style
        if model is None and style_override is None:
            style_override = self.api_style
        resolved_api_style = _model_api_style(
            preset,
            resolved_model,
            style_override,
        )
        common_options: dict[str, Any] = {}
        if temperature is not None:
            common_options["temperature"] = float(temperature)
        if top_p is not None:
            common_options["top_p"] = float(top_p)
        if extra_body:
            common_options["extra_body"] = dict(extra_body)
        if extra_headers:
            common_options["extra_headers"] = {
                str(key): str(value) for key, value in extra_headers.items()
            }

        try:
            if resolved_api_style == API_STYLE_RESPONSES:
                request: dict[str, Any] = {
                    "model": resolved_model,
                    "input": user_input,
                    "store": bool(store),
                    **common_options,
                }
                cleaned_instructions = _clean_optional(instructions)
                if cleaned_instructions:
                    request["instructions"] = cleaned_instructions
                if max_output_tokens is not None:
                    request["max_output_tokens"] = max(
                        1, int(max_output_tokens)
                    )
                if metadata:
                    request["metadata"] = {
                        str(key): str(value)
                        for key, value in metadata.items()
                        if str(key).strip()
                    }
                response = self._client.responses.create(**request)
                text = _extract_responses_text(response)
            else:
                messages: list[dict[str, str]] = []
                cleaned_instructions = _clean_optional(instructions)
                if cleaned_instructions:
                    messages.append(
                        {"role": "system", "content": cleaned_instructions}
                    )
                messages.append({"role": "user", "content": user_input})
                request = {
                    "model": resolved_model,
                    "messages": messages,
                    **common_options,
                }
                if max_output_tokens is not None:
                    request[self.config.chat_token_parameter] = max(
                        1, int(max_output_tokens)
                    )
                response = self._client.chat.completions.create(**request)
                text = _extract_chat_text(response)
        except Exception as exc:
            request_id = _clean_optional(getattr(exc, "request_id", ""))
            suffix = f" (request_id={request_id})" if request_id else ""
            raise OpenAIRequestError(
                f"{self.config.provider_name} request failed: {exc}{suffix}"
            ) from exc

        if not text:
            response_id = _clean_optional(getattr(response, "id", ""))
            suffix = f" (response_id={response_id})" if response_id else ""
            raise OpenAIEmptyResponseError(
                f"{self.config.provider_name} response contained no text{suffix}"
            )

        usage = getattr(response, "usage", None)
        return OpenAIAnswer(
            text=text,
            model=_clean_optional(getattr(response, "model", ""))
            or resolved_model,
            provider=self.provider,
            api_style=resolved_api_style,
            response_id=_clean_optional(getattr(response, "id", "")),
            request_id=_clean_optional(getattr(response, "_request_id", "")),
            input_tokens=_read_usage_value(
                usage, "input_tokens", "prompt_tokens"
            ),
            output_tokens=_read_usage_value(
                usage, "output_tokens", "completion_tokens"
            ),
            total_tokens=_read_usage_value(usage, "total_tokens"),
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
    store: bool = False,
    temperature: float | None = None,
    top_p: float | None = None,
    extra_body: Mapping[str, Any] | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> str:
    """Convenience interface returning only generated answer text."""

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
