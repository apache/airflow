# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
LLM provider abstraction layer.

Defines a provider interface (``LLMProvider``) and two implementations:

* ``OpenAIProvider`` — calls the OpenAI chat completions API.
* ``LocalRuleProvider`` — generates summaries from heuristic rules with
  no external calls (zero-cost, zero-latency, zero-data-leakage fallback).

Provider selection is driven by Airflow config (``[triage] llm_provider``)
and resolved at runtime via ``get_llm_provider()``.  Callers (triage service,
summarizer) depend only on the ``LLMProvider`` protocol — no provider-specific
code leaks into those modules.
"""

from __future__ import annotations

import abc
import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Response value object
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LLMResponse:
    """Immutable result returned by every provider."""

    text: str
    provider_name: str
    is_fallback: bool = False


# ---------------------------------------------------------------------------
# Abstract provider
# ---------------------------------------------------------------------------


class LLMProvider(abc.ABC):
    """
    Contract that every LLM provider must satisfy.

    Implementations must be stateless (safe to share across threads) and
    must never raise on transient failures — return a best-effort
    ``LLMResponse`` instead so the caller can still present *something*.
    """

    @abc.abstractmethod
    def summarize(self, log_text: str, category: str, error_line: str | None = None) -> LLMResponse:
        """
        Produce a human-readable root-cause summary.

        Parameters
        ----------
        log_text:
            Raw or trimmed task-instance log.
        category:
            Top-ranked failure category from the classifier
            (e.g. ``"TRANSIENT"``, ``"CODE"``).
        error_line:
            Optional extracted last error line for extra context.
        """

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Short identifier for logging / response metadata."""


# ---------------------------------------------------------------------------
# Local rule-based provider (always available, no external calls)
# ---------------------------------------------------------------------------

_CATEGORY_HINTS: dict[str, str] = {
    "TRANSIENT": (
        "This looks like a transient infrastructure issue — the task hit a "
        "timeout or received a retryable HTTP error. Retrying with back-off "
        "is the safest first step."
    ),
    "DATA_QUALITY": (
        "The failure is data-related — a schema mismatch, null-constraint "
        "violation, or validation error. Inspect the source data and the "
        "expected schema before re-running."
    ),
    "RESOURCE": (
        "The task ran out of resources (memory or disk). Scale up the "
        "worker, reduce batch size, or switch to chunked/streaming "
        "processing."
    ),
    "CODE": (
        "A Python exception indicates a bug in the task code — an import "
        "error, type error, or missing key. Fix the code and redeploy."
    ),
    "EXTERNAL_DEPENDENCY": (
        "An external service is unreachable — DNS failure, TLS certificate "
        "issue, or connection refused. Check network connectivity and "
        "service health."
    ),
}


class LocalRuleProvider(LLMProvider):
    """Generate summaries using deterministic rules — no external calls."""

    @property
    def name(self) -> str:
        return "local-rule"

    def summarize(self, log_text: str, category: str, error_line: str | None = None) -> LLMResponse:
        """Build a summary from category hints and the last error line."""
        hint = _CATEGORY_HINTS.get(category, "Review the task log for details.")
        parts = [hint]
        if error_line:
            parts.append(f"Last error: {error_line}")
        return LLMResponse(
            text=" ".join(parts),
            provider_name=self.name,
            is_fallback=False,
        )


# ---------------------------------------------------------------------------
# OpenAI provider
# ---------------------------------------------------------------------------


class OpenAIProvider(LLMProvider):
    """Call the OpenAI chat completions API for richer summaries."""

    def __init__(self, *, api_key: str | None = None, model: str = "gpt-4o-mini") -> None:
        self._api_key = api_key
        self._model = model

    @property
    def name(self) -> str:
        return "openai"

    def summarize(self, log_text: str, category: str, error_line: str | None = None) -> LLMResponse:
        """Call OpenAI and fall back to local rules on any failure."""
        try:
            return self._call_openai(log_text, category, error_line)
        except Exception:
            log.warning("OpenAI provider failed — falling back to local rules", exc_info=True)
            fallback = LocalRuleProvider().summarize(log_text, category, error_line)
            return LLMResponse(
                text=fallback.text,
                provider_name=self.name,
                is_fallback=True,
            )

    def _call_openai(self, log_text: str, category: str, error_line: str | None) -> LLMResponse:
        """Perform the actual API call."""
        try:
            import openai
        except ImportError as exc:
            raise RuntimeError(
                "The 'openai' package is required for the OpenAI provider. "
                "Install it with: pip install openai"
            ) from exc

        api_key = self._api_key or _read_api_key_from_config()
        client = openai.OpenAI(api_key=api_key)

        system_msg = (
            "You are an Airflow operations assistant. Given a task failure log, "
            "produce a concise (2-3 sentence) root-cause summary and one actionable "
            "next step. Do not repeat the raw log."
        )
        user_msg = f"Category: {category}\n"
        if error_line:
            user_msg += f"Last error: {error_line}\n"
        user_msg += f"\nLog excerpt (last 2000 chars):\n{log_text[-2000:]}"

        response = client.chat.completions.create(
            model=self._model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": user_msg},
            ],
            max_tokens=256,
            temperature=0.3,
        )
        text = response.choices[0].message.content or ""
        return LLMResponse(text=text.strip(), provider_name=self.name)


# ---------------------------------------------------------------------------
# Config-driven factory
# ---------------------------------------------------------------------------

_PROVIDERS: dict[str, type[LLMProvider]] = {
    "local": LocalRuleProvider,
    "openai": OpenAIProvider,
}


def _read_api_key_from_config() -> str:
    """Read ``[triage] openai_api_key`` from Airflow config."""
    from airflow.configuration import conf

    key = conf.get("triage", "openai_api_key", fallback="")
    if not key:
        raise ValueError(
            "OpenAI API key not configured. Set [triage] openai_api_key in airflow.cfg "
            "or pass it via the AIRFLOW__TRIAGE__OPENAI_API_KEY environment variable."
        )
    return key


def get_llm_provider() -> LLMProvider:
    """
    Return the configured LLM provider instance.

    Reads ``[triage] llm_provider`` from ``airflow.cfg`` (default: ``local``).
    Falls back to ``LocalRuleProvider`` if the requested provider cannot be
    instantiated.
    """
    try:
        from airflow.configuration import conf

        provider_name = conf.get("triage", "llm_provider", fallback="local")
    except ImportError:
        # Outside Airflow (tests, demo) — default to local.
        provider_name = "local"

    provider_cls = _PROVIDERS.get(provider_name)
    if provider_cls is None:
        log.warning("Unknown LLM provider '%s' — falling back to 'local'", provider_name)
        provider_cls = LocalRuleProvider

    try:
        return provider_cls()
    except Exception:
        log.warning(
            "Failed to instantiate '%s' provider — falling back to 'local'", provider_name, exc_info=True
        )
        return LocalRuleProvider()
