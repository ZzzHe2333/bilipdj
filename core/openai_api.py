"""Reusable multi-provider interface built on the official OpenAI Python SDK.

Presets are conveniences only. Explicit ``api_key``, ``base_url``, ``model`` and
``api_style`` values always override them. Importing this module performs no
network request.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

API_STYLE_RESPONSES = "responses"
API_STYLE_CHAT_COMPLETIONS = "chat_completions"
SUPPORTED_API_STYLES = {API_STYLE_RESPONSES, API_STYLE_CHAT_COMPLETIONS}


class OpenAIInterfaceError(RuntimeError):
    pass


class OpenAIConfigurationError(OpenAIInterfaceError):
    pass


class OpenAISDKNotInstalledError(OpenAIInterfaceError):
    pass


class OpenAIRequestError(OpenAIInterfaceError):
    pass


class OpenAIEmptyResponseError(OpenAIInterfaceError):
    pass


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


def _m(model_id: str, name: str | None = None, style: str | None = None) -> ProviderModel:
    return ProviderModel(model_id, name or model_id, style)


PROVIDER_PRESETS: dict[str, ProviderPreset] = {
    "openai": ProviderPreset(
        "openai", "OpenAI", "https://api.openai.com/v1",
        ("OPENAI_API_KEY",), ("OPENAI_MODEL",), ("OPENAI_BASE_URL",),
        "gpt-5.6", API_STYLE_RESPONSES,
        (_m("gpt-5.6", "GPT-5.6"), _m("gpt-5.6-sol", "GPT-5.6 Sol"),
         _m("gpt-5.6-terra", "GPT-5.6 Terra"), _m("gpt-5.6-luna", "GPT-5.6 Luna")),
        responses_store_default=False,
    ),
    "deepseek": ProviderPreset(
        "deepseek", "DeepSeek", "https://api.deepseek.com",
        ("DEEPSEEK_API_KEY",), ("DEEPSEEK_MODEL",), ("DEEPSEEK_BASE_URL",),
        "deepseek-v4-flash", API_STYLE_CHAT_COMPLETIONS,
        (_m("deepseek-v4-flash", "DeepSeek V4 Flash"),
         _m("deepseek-v4-pro", "DeepSeek V4 Pro")),
    ),
    "minimax": ProviderPreset(
        "minimax", "MiniMax", "https://api.minimax.io/v1",
        ("MINIMAX_API_KEY",), ("MINIMAX_MODEL",), ("MINIMAX_BASE_URL",),
        "MiniMax-M2.7", API_STYLE_CHAT_COMPLETIONS,
        tuple(_m(x) for x in (
            "MiniMax-M3", "MiniMax-M2.7", "MiniMax-M2.7-highspeed",
            "MiniMax-M2.5", "MiniMax-M2.5-highspeed", "MiniMax-M2.1",
            "MiniMax-M2.1-highspeed", "MiniMax-M2",
        )),
        chat_token_parameter="max_completion_tokens",
    ),
    "qwen": ProviderPreset(
        "qwen", "Qwen / 阿里云百炼",
        "https://dashscope.aliyuncs.com/compatible-mode/v1",
        ("DASHSCOPE_API_KEY", "QWEN_API_KEY"),
        ("QWEN_MODEL", "DASHSCOPE_MODEL"),
        ("QWEN_BASE_URL", "DASHSCOPE_BASE_URL"),
        "qwen3.7-plus", API_STYLE_CHAT_COMPLETIONS,
        (_m("qwen3.7-max", "Qwen3.7 Max"), _m("qwen3.7-plus", "Qwen3.7 Plus"),
         _m("qwen3.6-flash", "Qwen3.6 Flash"),
         _m("qwen3-coder-plus", "Qwen3 Coder Plus")),
    ),
    "xiaomi_mimo": ProviderPreset(
        "xiaomi_mimo", "Xiaomi MiMo", "https://api.xiaomimimo.com/v1",
        ("MIMO_API_KEY", "XIAOMIMIMO_API_KEY"),
        ("MIMO_MODEL", "XIAOMIMIMO_MODEL"),
        ("MIMO_BASE_URL", "XIAOMIMIMO_BASE_URL"),
        "mimo-v2.5-pro", API_STYLE_CHAT_COMPLETIONS,
        (_m("mimo-v2.5-pro", "MiMo V2.5 Pro"), _m("mimo-v2.5", "MiMo V2.5")),
        chat_token_parameter="max_completion_tokens",
    ),
    "claude": ProviderPreset(
        "claude", "Claude / Anthropic", "https://api.anthropic.com/v1",
        ("ANTHROPIC_API_KEY",), ("ANTHROPIC_MODEL", "CLAUDE_MODEL"),
        ("ANTHROPIC_BASE_URL", "CLAUDE_BASE_URL"),
        "claude-sonnet-4-6", API_STYLE_CHAT_COMPLETIONS,
        (_m("claude-opus-5", "Claude Opus 5"),
         _m("claude-sonnet-4-6", "Claude Sonnet 4.6"),
         _m("claude-haiku-4-5", "Claude Haiku 4.5")),
    ),
    "opencode": ProviderPreset(
        "opencode", "OpenCode Zen", "https://opencode.ai/zen/v1",
        ("OPENCODE_API_KEY", "OPENCODE_ZEN_API_KEY"),
        ("OPENCODE_MODEL",), ("OPENCODE_BASE_URL",),
        "gpt-5.6-terra", API_STYLE_RESPONSES,
        (
            _m("gpt-5.6-sol", "GPT-5.6 Sol", API_STYLE_RESPONSES),
            _m("gpt-5.6-terra", "GPT-5.6 Terra", API_STYLE_RESPONSES),
            _m("gpt-5.6-luna", "GPT-5.6 Luna", API_STYLE_RESPONSES),
            _m("deepseek-v4-pro", "DeepSeek V4 Pro", API_STYLE_CHAT_COMPLETIONS),
            _m("deepseek-v4-flash", "DeepSeek V4 Flash", API_STYLE_CHAT_COMPLETIONS),
            _m("minimax-m3", "MiniMax M3", API_STYLE_CHAT_COMPLETIONS),
            _m("minimax-m2.7", "MiniMax M2.7", API_STYLE_CHAT_COMPLETIONS),
            _m("minimax-m2.5", "MiniMax M2.5", API_STYLE_CHAT_COMPLETIONS),
            _m("glm-5.2", "GLM 5.2", API_STYLE_CHAT_COMPLETIONS),
            _m("kimi-k2.7-code", "Kimi K2.7 Code", API_STYLE_CHAT_COMPLETIONS),
            _m("mimo-v2.5-free", "MiMo V2.5 Free", API_STYLE_CHAT_COMPLETIONS),
        ),
        responses_store_default=False,
    ),
    "custom": ProviderPreset(
        "custom", "自定义 OpenAI 兼容服务", "",
        ("OPENAI_API_KEY",), ("OPENAI_MODEL",), ("OPENAI_BASE_URL",),
        "", API_STYLE_CHAT_COMPLETIONS, (), api_key_required=False,
    ),
}

_PROVIDER_ALIASES = {
    "anthropic": "claude", "dashscope": "qwen", "aliyun": "qwen",
    "mimo": "xiaomi_mimo", "xiaomimimo": "xiaomi_mimo",
    "xiaomi-mimo": "xiaomi_mimo", "opencode-zen": "opencode",
    "opencode_zen": "opencode", "zen": "opencode",
}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _env(names: Sequence[str]) -> str:
    return next((_text(os.getenv(name)) for name in names if _text(os.getenv(name))), "")


def normalize_provider_name(provider: str | None) -> str:
    raw = _text(provider or os.getenv("LLM_PROVIDER") or os.getenv("OPENAI_PROVIDER") or "openai").lower()
    canonical = _PROVIDER_ALIASES.get(raw, raw)
    if canonical not in PROVIDER_PRESETS:
        raise OpenAIConfigurationError(
            f"Unknown provider {raw!r}; available providers: {', '.join(sorted(PROVIDER_PRESETS))}"
        )
    return canonical


def get_provider_preset(provider: str | None) -> ProviderPreset:
    return PROVIDER_PRESETS[normalize_provider_name(provider)]


def list_provider_presets() -> tuple[ProviderPreset, ...]:
    return tuple(PROVIDER_PRESETS.values())


def list_provider_models(provider: str | None) -> tuple[ProviderModel, ...]:
    return get_provider_preset(provider).models


def get_provider_catalog() -> dict[str, dict[str, Any]]:
    return {
        key: {
            "id": p.id, "name": p.name, "base_url": p.base_url,
            "default_model": p.default_model,
            "default_api_style": p.default_api_style,
            "api_key_required": p.api_key_required,
            "api_key_envs": list(p.api_key_envs),
            "model_envs": list(p.model_envs),
            "base_url_envs": list(p.base_url_envs),
            "models": [{"id": m.id, "name": m.name,
                        "api_style": m.api_style or p.default_api_style} for m in p.models],
        }
        for key, p in PROVIDER_PRESETS.items()
    }


def _api_style(preset: ProviderPreset, model: str, explicit: str | None) -> str:
    style = _text(explicit).lower() if explicit else preset.default_api_style
    if not explicit:
        style = next((m.api_style for m in preset.models if m.id == model and m.api_style), style)
    if style not in SUPPORTED_API_STYLES:
        raise OpenAIConfigurationError("api_style must be 'responses' or 'chat_completions'")
    return style


def resolve_provider_config(
    *, provider: str | None = None, api_key: str | None = None,
    model: str | None = None, base_url: str | None = None,
    api_style: str | None = None, require_api_key: bool = True,
) -> ResolvedProviderConfig:
    """Resolve settings without forwarding an OpenAI key to a third-party host."""
    preset = get_provider_preset(provider)
    resolved_model = _text(model or _env(preset.model_envs) or os.getenv("LLM_MODEL") or preset.default_model)
    if not resolved_model:
        raise OpenAIConfigurationError("Model is required")
    resolved_url = _text(base_url or _env(preset.base_url_envs) or os.getenv("LLM_BASE_URL") or preset.base_url)
    if not resolved_url:
        raise OpenAIConfigurationError("Base URL is required for a custom provider")
    # OPENAI_API_KEY is intentionally not a fallback for non-OpenAI presets.
    resolved_key = _text(api_key or _env(preset.api_key_envs) or os.getenv("LLM_API_KEY"))
    if preset.api_key_required and require_api_key and not resolved_key:
        raise OpenAIConfigurationError(
            f"{preset.name} API key is required; set {' / '.join(preset.api_key_envs)}"
        )
    return ResolvedProviderConfig(
        preset.id, preset.name, resolved_key or "not-needed", resolved_url.rstrip("/"),
        resolved_model, _api_style(preset, resolved_model, api_style),
        preset.chat_token_parameter, preset.responses_store_default,
    )


def _usage(usage: Any, *names: str) -> int | None:
    for name in names:
        value = usage.get(name) if isinstance(usage, Mapping) else getattr(usage, name, None)
        try:
            if value is not None:
                return int(value)
        except (TypeError, ValueError):
            pass
    return None


def _responses_text(response: Any) -> str:
    direct = _text(getattr(response, "output_text", ""))
    if direct:
        return direct
    chunks: list[str] = []
    for item in getattr(response, "output", None) or []:
        content = item.get("content") if isinstance(item, Mapping) else getattr(item, "content", None)
        for part in content or []:
            kind = _text(part.get("type") if isinstance(part, Mapping) else getattr(part, "type", ""))
            value = part.get("text") if isinstance(part, Mapping) else getattr(part, "text", None)
            if kind in {"", "text", "output_text"} and _text(value):
                chunks.append(_text(value))
    return "\n".join(chunks)


def _chat_text(response: Any) -> str:
    choices = getattr(response, "choices", None) or []
    if not choices:
        return ""
    first = choices[0]
    message = first.get("message") if isinstance(first, Mapping) else getattr(first, "message", None)
    content = message.get("content") if isinstance(message, Mapping) else getattr(message, "content", None)
    if isinstance(content, str):
        return content.strip()
    chunks: list[str] = []
    for part in content or []:
        value = part.get("text", part.get("content", "")) if isinstance(part, Mapping) else getattr(part, "text", getattr(part, "content", ""))
        if _text(value):
            chunks.append(_text(value))
    return "\n".join(chunks)


class OpenAIAnswerClient:
    def __init__(
        self, *, provider: str | None = None, api_key: str | None = None,
        model: str | None = None, base_url: str | None = None,
        api_style: str | None = None, timeout: float = 30.0,
        max_retries: int = 2, client: Any | None = None,
    ) -> None:
        self.config = resolve_provider_config(
            provider=provider, api_key=api_key, model=model, base_url=base_url,
            api_style=api_style, require_api_key=client is None,
        )
        self.provider, self.model = self.config.provider, self.config.model
        self.base_url, self.api_style = self.config.base_url, self.config.api_style
        if client is not None:
            self._client = client
            return
        try:
            from openai import OpenAI
        except ImportError as exc:
            raise OpenAISDKNotInstalledError("Install the project's openai dependency") from exc
        self._client = OpenAI(
            api_key=self.config.api_key, base_url=self.base_url,
            timeout=max(0.1, float(timeout)), max_retries=max(0, int(max_retries)),
        )

    def ask(
        self, prompt: str, *, instructions: str | None = None,
        model: str | None = None, api_style: str | None = None,
        max_output_tokens: int | None = 512, store: bool | None = None,
        metadata: Mapping[str, str] | None = None,
        temperature: float | None = None, top_p: float | None = None,
        extra_body: Mapping[str, Any] | None = None,
        extra_headers: Mapping[str, str] | None = None,
    ) -> OpenAIAnswer:
        user_input = _text(prompt)
        if not user_input:
            raise ValueError("prompt must not be empty")
        selected_model = _text(model or self.model)
        style_hint = self.api_style if model is None and api_style is None else api_style
        style = _api_style(get_provider_preset(self.provider), selected_model, style_hint)
        common: dict[str, Any] = {}
        if temperature is not None:
            common["temperature"] = float(temperature)
        if top_p is not None:
            common["top_p"] = float(top_p)
        if extra_body:
            common["extra_body"] = dict(extra_body)
        if extra_headers:
            common["extra_headers"] = {str(k): str(v) for k, v in extra_headers.items()}
        try:
            if style == API_STYLE_RESPONSES:
                request: dict[str, Any] = {"model": selected_model, "input": user_input, **common}
                effective_store = self.config.responses_store_default if store is None else bool(store)
                if effective_store is not None:
                    request["store"] = effective_store
                if _text(instructions):
                    request["instructions"] = _text(instructions)
                if max_output_tokens is not None:
                    request["max_output_tokens"] = max(1, int(max_output_tokens))
                if metadata:
                    request["metadata"] = {str(k): str(v) for k, v in metadata.items() if str(k).strip()}
                response = self._client.responses.create(**request)
                answer = _responses_text(response)
            else:
                messages = []
                if _text(instructions):
                    messages.append({"role": "system", "content": _text(instructions)})
                messages.append({"role": "user", "content": user_input})
                request = {"model": selected_model, "messages": messages, **common}
                if max_output_tokens is not None:
                    request[self.config.chat_token_parameter] = max(1, int(max_output_tokens))
                response = self._client.chat.completions.create(**request)
                answer = _chat_text(response)
        except Exception as exc:
            request_id = _text(getattr(exc, "request_id", "") or getattr(exc, "_request_id", ""))
            suffix = f" (request_id={request_id})" if request_id else ""
            raise OpenAIRequestError(f"{self.config.provider_name} request failed: {exc}{suffix}") from exc
        if not answer:
            rid = _text(getattr(response, "id", ""))
            raise OpenAIEmptyResponseError(
                f"{self.config.provider_name} response contained no text" + (f" (response_id={rid})" if rid else "")
            )
        usage = getattr(response, "usage", None)
        return OpenAIAnswer(
            answer, _text(getattr(response, "model", "")) or selected_model,
            self.provider, style, _text(getattr(response, "id", "")),
            _text(getattr(response, "_request_id", "") or getattr(response, "request_id", "")),
            _usage(usage, "input_tokens", "prompt_tokens"),
            _usage(usage, "output_tokens", "completion_tokens"),
            _usage(usage, "total_tokens"),
        )


def get_model_answer(
    prompt: str, *, instructions: str | None = None, provider: str | None = None,
    api_key: str | None = None, model: str | None = None,
    base_url: str | None = None, api_style: str | None = None,
    timeout: float = 30.0, max_retries: int = 2,
    max_output_tokens: int | None = 512, store: bool | None = None,
    temperature: float | None = None, top_p: float | None = None,
    extra_body: Mapping[str, Any] | None = None,
    extra_headers: Mapping[str, str] | None = None,
) -> str:
    client = OpenAIAnswerClient(
        provider=provider, api_key=api_key, model=model, base_url=base_url,
        api_style=api_style, timeout=timeout, max_retries=max_retries,
    )
    return client.ask(
        prompt, instructions=instructions, max_output_tokens=max_output_tokens,
        store=store, temperature=temperature, top_p=top_p,
        extra_body=extra_body, extra_headers=extra_headers,
    ).text


ask_openai = get_model_answer

__all__ = [
    "API_STYLE_CHAT_COMPLETIONS", "API_STYLE_RESPONSES", "OpenAIAnswer",
    "OpenAIAnswerClient", "OpenAIConfigurationError", "OpenAIEmptyResponseError",
    "OpenAIInterfaceError", "OpenAIRequestError", "OpenAISDKNotInstalledError",
    "PROVIDER_PRESETS", "ProviderModel", "ProviderPreset", "ResolvedProviderConfig",
    "ask_openai", "get_model_answer", "get_provider_catalog", "get_provider_preset",
    "list_provider_models", "list_provider_presets", "normalize_provider_name",
    "resolve_provider_config",
]
