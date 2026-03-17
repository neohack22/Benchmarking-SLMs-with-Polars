from enum import Enum
from typing import Any
import time

import requests
from pydantic import BaseModel


class ProviderName(str, Enum):
    OPENAI = "openai"
    XAI = "xai"
    CEREBRAS = "cerebras"


class ProviderConfig(BaseModel):
    provider: ProviderName
    model_name: str
    api_key: str
    temperature: float = 0.1
    max_tokens: int = 1024
    system_prompt: str = ""


class RunProviderRequest(BaseModel):
    config: ProviderConfig


class CodeMetrics(BaseModel):
    response_text: str
    duration_seconds: float
    peak_ram_mb: float
    peak_gpu_mb: float


_PROVIDER_BASE_URLS: dict[ProviderName, str] = {
    ProviderName.OPENAI: "https://api.openai.com/v1",
    ProviderName.XAI: "https://api.x.ai/v1",
    ProviderName.CEREBRAS: "https://api.cerebras.ai/v1",
}


def _build_system_prompt(extra_system_prompt: str, schema: dict[str, Any]) -> str:
    base = (
        "You are a Polars expert. "
        "Return only valid Python code. "
        "Do not use markdown fences. "
        "Assume polars is imported as pl. "
        "Store the final answer in a variable named result. "
        f"Dataset schema: {schema}"
    )
    if not extra_system_prompt.strip():
        return base
    return f"{base}\n\nAdditional system instructions:\n{extra_system_prompt.strip()}"


def _strip_code_fences(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines:
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    return text.replace("```python", "").replace("```", "").strip()


def _build_payload(
    provider: ProviderName,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    max_tokens: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }

    if provider in {ProviderName.OPENAI, ProviderName.CEREBRAS}:
        payload["temperature"] = temperature
        payload["max_completion_tokens"] = max_tokens
    elif provider is ProviderName.XAI:
        payload["temperature"] = temperature
        payload["max_tokens"] = max_tokens

    return payload


def call_provider_api(
    provider: ProviderName,
    model: str,
    api_key: str,
    prompt: str,
    schema: dict[str, Any],
    temp: float,
    max_tokens: int,
    extra_system_prompt: str = "",
    timeout: float = 300.0,
) -> CodeMetrics:
    system_prompt = _build_system_prompt(extra_system_prompt, schema)
    payload = _build_payload(
        provider=provider,
        model=model,
        system_prompt=system_prompt,
        user_prompt=prompt,
        temperature=temp,
        max_tokens=max_tokens,
    )

    start = time.perf_counter()

    response = requests.post(
        f"{_PROVIDER_BASE_URLS[provider]}/chat/completions",
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        json=payload,
        timeout=timeout,
    )

    if response.status_code >= 400:
        print("CEREBRAS STATUS:", response.status_code)
        print("CEREBRAS BODY:", response.text)

    duration_seconds = time.perf_counter() - start
    response.raise_for_status()

    data = response.json()
    text = data["choices"][0]["message"]["content"]

    return CodeMetrics(
        response_text=_strip_code_fences(text),
        duration_seconds=duration_seconds,
        peak_ram_mb=0.0,
        peak_gpu_mb=0.0,
    )