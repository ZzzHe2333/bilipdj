"""Reusable OpenAI Responses API interface.

This module is intentionally independent from the queue and danmu handlers. It
provides a small synchronous interface that future features can call when they
need a model-generated answer.

Configuration is read from explicit arguments first, then environment variables:

- ``OPENAI_API_KEY``: required when a client is not injected.
- ``OPENAI_MODEL``: required unless ``model=...`` is supplied.
- ``OPENAI_BASE_URL``: optional OpenAI-compatible endpoint.

No API request is made while importing this module.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Mapping


class OpenAIInterfaceError(RuntimeError):
    """Base exception raised by this interface."""


class OpenAIConfigurationError(OpenAIInterfaceError):
    """Raised when required client configuration is missing."""


class OpenAISDKNotInstalledError(OpenAIInterfaceError):
    """Raised when the ``openai`` package is unavailable."""


class OpenAIRequestError(OpenAIInterfaceError):
    """Raised when an OpenAI request fails."""


class OpenAIEmptyResponseError(OpenAIInterfaceError):
    """Raised when the model response contains no readable text."""


@dataclass(frozen=True, slots=True)
class OpenAIAnswer:
    """Structured result returned by :meth:`OpenAIAnswerClient.ask`."""

    text: str
    model: str
    response_id: str = ""
    request_id: str = ""
    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


def _clean_optional(value: Any) -> str:
    return str(value or "").strip()


def _read_usage_value(usage: Any, name: str) -> int | None:
    if usage is None:
        return None
    value = usage.get(name) if isinstance(usage, Mapping) else getattr(usage, name, None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _extract_output_text(response: Any) -> str:
    """Extract text from a Responses API object, including a compatibility fallback."""

    direct = _clean_optional(getattr(response, "output_text", ""))
    if direct:
        return direct

    chunks: list[str] = []
    output = getattr(response, "output", None)
    if not isinstance(output, (list, tuple)):
        return ""

    for item in output:
        content = item.get("content") if isinstance(item, Mapping) else getattr(item, "content", None)
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


class OpenAIAnswerClient:
    """Small reusable wrapper around ``openai.OpenAI``.

    ``client`` is injectable so callers and tests can supply any object exposing
    ``client.responses.create(**kwargs)``. When omitted, the official OpenAI
    Python SDK is imported lazily.
    """

    def __init__(
        self,
        *,
        api_key: str | None = None,
        model: str | None = None,
        base_url: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 2,
        client: Any | None = None,
    ) -> None:
        self.model = _clean_optional(model or os.getenv("OPENAI_MODEL"))
        if not self.model:
            raise OpenAIConfigurationError(
                "OpenAI model is required; pass model=... or set OPENAI_MODEL"
            )

        self.base_url = _clean_optional(base_url or os.getenv("OPENAI_BASE_URL"))
        self.timeout = max(0.1, float(timeout))
        self.max_retries = max(0, int(max_retries))

        if client is not None:
            self._client = client
            return

        resolved_api_key = _clean_optional(api_key or os.getenv("OPENAI_API_KEY"))
        if not resolved_api_key:
            raise OpenAIConfigurationError(
                "OpenAI API key is required; pass api_key=... or set OPENAI_API_KEY"
            )

        try:
            from openai import OpenAI
        except ImportError as exc:  # pragma: no cover - depends on installation
            raise OpenAISDKNotInstalledError(
                "The openai package is not installed; install project requirements"
            ) from exc

        client_options: dict[str, Any] = {
            "api_key": resolved_api_key,
            "timeout": self.timeout,
            "max_retries": self.max_retries,
        }
        if self.base_url:
            client_options["base_url"] = self.base_url
        self._client = OpenAI(**client_options)

    def ask(
        self,
        prompt: str,
        *,
        instructions: str | None = None,
        model: str | None = None,
        max_output_tokens: int | None = 512,
        store: bool = False,
        metadata: Mapping[str, str] | None = None,
    ) -> OpenAIAnswer:
        """Call the model and return a structured answer.

        This method has no connection to the current danmu or queue logic. A
        later feature can call it and decide where the returned text should go.
        """

        user_input = _clean_optional(prompt)
        if not user_input:
            raise ValueError("prompt must not be empty")

        resolved_model = _clean_optional(model or self.model)
        if not resolved_model:
            raise OpenAIConfigurationError("model must not be empty")

        request: dict[str, Any] = {
            "model": resolved_model,
            "input": user_input,
            "store": bool(store),
        }
        cleaned_instructions = _clean_optional(instructions)
        if cleaned_instructions:
            request["instructions"] = cleaned_instructions
        if max_output_tokens is not None:
            request["max_output_tokens"] = max(1, int(max_output_tokens))
        if metadata:
            request["metadata"] = {
                str(key): str(value)
                for key, value in metadata.items()
                if str(key).strip()
            }

        try:
            response = self._client.responses.create(**request)
        except Exception as exc:  # SDK exceptions share the common Exception base
            request_id = _clean_optional(getattr(exc, "request_id", ""))
            suffix = f" (request_id={request_id})" if request_id else ""
            raise OpenAIRequestError(f"OpenAI request failed: {exc}{suffix}") from exc

        text = _extract_output_text(response)
        if not text:
            response_id = _clean_optional(getattr(response, "id", ""))
            suffix = f" (response_id={response_id})" if response_id else ""
            raise OpenAIEmptyResponseError(f"OpenAI response contained no text{suffix}")

        usage = getattr(response, "usage", None)
        return OpenAIAnswer(
            text=text,
            model=_clean_optional(getattr(response, "model", "")) or resolved_model,
            response_id=_clean_optional(getattr(response, "id", "")),
            request_id=_clean_optional(getattr(response, "_request_id", "")),
            input_tokens=_read_usage_value(usage, "input_tokens"),
            output_tokens=_read_usage_value(usage, "output_tokens"),
            total_tokens=_read_usage_value(usage, "total_tokens"),
        )


def get_model_answer(
    prompt: str,
    *,
    instructions: str | None = None,
    api_key: str | None = None,
    model: str | None = None,
    base_url: str | None = None,
    timeout: float = 30.0,
    max_retries: int = 2,
    max_output_tokens: int | None = 512,
    store: bool = False,
) -> str:
    """Convenience interface that returns only the generated answer text."""

    client = OpenAIAnswerClient(
        api_key=api_key,
        model=model,
        base_url=base_url,
        timeout=timeout,
        max_retries=max_retries,
    )
    return client.ask(
        prompt,
        instructions=instructions,
        max_output_tokens=max_output_tokens,
        store=store,
    ).text


# Short alias for future integrations.
ask_openai = get_model_answer


__all__ = [
    "OpenAIAnswer",
    "OpenAIAnswerClient",
    "OpenAIConfigurationError",
    "OpenAIEmptyResponseError",
    "OpenAIInterfaceError",
    "OpenAIRequestError",
    "OpenAISDKNotInstalledError",
    "ask_openai",
    "get_model_answer",
]
