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
LLM-powered retry policy using pydantic-ai for error classification.

Requires Airflow 3.3+ (RetryPolicy was added in AIP-105).
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import TYPE_CHECKING, cast

from pydantic import BaseModel

from airflow.providers.common.compat.sdk import redact

try:
    from airflow.sdk.definitions.retry_policy import (
        ExceptionRetryPolicy,
        RetryDecision,
        RetryPolicy,
    )
except ImportError:
    raise ImportError(
        "LLMRetryPolicy requires Airflow 3.3+ which includes RetryPolicy support. "
        "Please upgrade apache-airflow-core."
    ) from None

if TYPE_CHECKING:
    from collections.abc import Callable

    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.retry_policy import RetryRule

log = logging.getLogger(__name__)

__all__ = ["ErrorClassification", "LLMRetryPolicy", "redact_registered_secrets"]

DEFAULT_INSTRUCTIONS = (
    "You are an error classifier for a data pipeline system. "
    "Given an error message from a failed task, classify it into one of these categories:\n\n"
    "- rate_limit: API throttling or quota exceeded. Should retry after a delay.\n"
    "- auth: Credentials invalid, expired, or missing permissions. Should NOT retry.\n"
    "- network: Transient connectivity issue. Should retry quickly.\n"
    "- data: Schema validation, type mismatch, or bad input data. Should NOT retry.\n"
    "- resource: Resource not found or unavailable (e.g., missing table, bucket). Should NOT retry.\n"
    "- transient: Temporary issue likely to resolve on its own. Should retry.\n"
    "- permanent: Problem that won't resolve without code or config changes. Should NOT retry.\n\n"
    "Set suggested_delay_seconds based on the error type: "
    "60 for rate limits, 10 for network, 30 for transient. "
    "Set 0 for errors that should not retry."
)


class ErrorClassification(BaseModel):
    """Structured LLM output for error classification."""

    category: str
    """One of: rate_limit, auth, network, data, resource, transient, permanent."""
    should_retry: bool
    """Whether the operation should be retried."""
    suggested_delay_seconds: int = 0
    """How long to wait before retrying (0 if should_retry is False)."""
    reasoning: str
    """Brief explanation of the classification decision."""


def redact_registered_secrets(message: str) -> str:
    """Mask values registered via ``mask_secret()``; the default ``redactor`` for :class:`LLMRetryPolicy`."""
    # redact() is typed for arbitrary containers; a str in always yields a str out.
    return cast("str", redact(message))


class LLMRetryPolicy(RetryPolicy):
    """
    Retry policy that uses an LLM to classify errors and decide retry behaviour.

    Uses :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
    to call any configured LLM provider (OpenAI, Anthropic, Bedrock, Vertex,
    Ollama, etc.) for error classification with structured output.

    When the LLM call itself fails, the policy falls back to ``fallback_rules``
    (if provided) or returns DEFAULT to use the task's standard retry logic.

    :param llm_conn_id: Airflow connection ID for the LLM provider.
    :param model_id: Model identifier override (e.g. ``"openai:gpt-4o-mini"``
        for cost efficiency). If not set, uses the model from the connection.
    :param instructions: Custom system prompt for classification.
        Defaults to a general-purpose error classifier.
    :param fallback_rules: Optional list of
        :class:`~airflow.sdk.definitions.retry_policy.RetryRule` applied when the
        LLM call fails. Provides a deterministic safety net.
    :param timeout: Maximum seconds to wait for the LLM response before
        falling back.  Defaults to 30s.  The LLM provider's own timeout
        (e.g. 600s for Anthropic) is much longer; this keeps the retry
        decision path fast even when the provider is degraded.
    :param redactor: Callable applied to the exception's string representation
        before it is added to the classification prompt. Defaults to
        :func:`~airflow.providers.common.ai.policies.retry.redact_registered_secrets`,
        which only masks values already registered via ``mask_secret()``.
        Pass a custom callable to replace the default masking entirely --
        for example to redact free-text PII the secrets masker cannot see.
        To disable masking altogether, use ``redact_exception=False`` --
        not ``redactor=None``.
    :param redact_exception: Whether to redact the exception's string
        representation before it is added to the classification prompt.
        Defaults to ``True``. Set to ``False`` to send the raw exception
        text as-is. Passing ``redact_exception=False`` together with
        an explicit ``redactor`` raises ``ValueError`` at construction time,
        since the two settings would otherwise conflict silently.
    :param max_exception_length: Maximum number of characters of the
        (already redacted) exception message included in the prompt. Longer
        messages are truncated with a trailing ``"... (truncated)"`` marker.
        Must be a positive integer. Defaults to 4096.

    .. warning::
        The exception's string representation is sent to the configured
        external LLM provider (OpenAI, Anthropic, Bedrock, Vertex, Ollama,
        etc.) as part of the classification prompt, so it may leak whatever
        the failing task put in the exception message — connection strings,
        credential fragments, PII, or other secrets. By default
        ``_classify()`` runs the message through
        :func:`~airflow.providers.common.ai.policies.retry.redact_registered_secrets`
        via ``redactor``, which masks values already registered via
        ``mask_secret()`` (for example, connection passwords Airflow
        captured while resolving the failing task's connections). This does
        **not** perform general-purpose PII detection and will not catch
        arbitrary sensitive strings that were never registered as secrets --
        for free-text PII (emails, customer names, etc.) supply your own
        ``redactor``, or pass ``redact_exception=False`` to disable
        redaction altogether. You are still responsible for confirming that
        your task's exception messages are safe to send to a third-party
        LLM provider.
    """

    def __init__(
        self,
        llm_conn_id: str,
        model_id: str | None = None,
        instructions: str | None = None,
        fallback_rules: list[RetryRule] | None = None,
        timeout: float = 30.0,
        *,
        redactor: Callable[[str], str] | None = None,
        redact_exception: bool = True,
        max_exception_length: int = 4096,
    ) -> None:
        if max_exception_length <= 0:
            raise ValueError(f"max_exception_length must be a positive integer, got {max_exception_length}")
        if not redact_exception and redactor is not None:
            raise ValueError(
                "redactor must not be set when redact_exception=False -- passing an explicit "
                "redactor while also disabling redaction is contradictory. Either drop "
                "redact_exception=False to keep using redactor, or drop redactor to disable "
                "redaction entirely."
            )
        self.llm_conn_id = llm_conn_id
        self.model_id = model_id
        self.instructions = instructions or DEFAULT_INSTRUCTIONS
        self.fallback_rules = fallback_rules
        self.timeout = timeout
        self.redactor: Callable[[str], str] | None = (
            None if not redact_exception else redactor if redactor is not None else redact_registered_secrets
        )
        self.redact_exception = redact_exception
        self.max_exception_length = max_exception_length

    def evaluate(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
        context: Context | None = None,
    ) -> RetryDecision:
        try:
            return self._classify(exception, try_number, max_tries)
        except Exception:
            log.exception("LLM retry classification failed, using fallback")
            if self.fallback_rules:
                return ExceptionRetryPolicy(rules=self.fallback_rules).evaluate(
                    exception, try_number, max_tries, context
                )
            return RetryDecision.default()

    def _classify(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
    ) -> RetryDecision:
        from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook

        hook = PydanticAIHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)
        agent = hook.create_agent(
            output_type=ErrorClassification,
            instructions=self.instructions,
        )

        # Redact before truncating -- truncating first could cut a registered secret in half.
        message = self.redactor(str(exception)) if self.redactor is not None else str(exception)
        if len(message) > self.max_exception_length:
            message = f"{message[: self.max_exception_length]}... (truncated)"
        prompt = (
            f"Classify this error from a data pipeline task "
            f"(attempt {try_number} of {max_tries}):\n\n"
            f"{type(exception).__name__}: {message}"
        )

        from pydantic_ai.settings import ModelSettings

        result = agent.run_sync(
            prompt,
            model_settings=ModelSettings(timeout=self.timeout),
        )
        classification = result.output

        log.info(
            "LLM error classification: category=%s, should_retry=%s, delay=%ds, reasoning=%s",
            classification.category,
            classification.should_retry,
            classification.suggested_delay_seconds,
            classification.reasoning,
        )

        if not classification.should_retry:
            return RetryDecision.fail(reason=f"{classification.category}: {classification.reasoning}")

        delay = (
            timedelta(seconds=classification.suggested_delay_seconds)
            if classification.suggested_delay_seconds > 0
            else None
        )
        return RetryDecision.retry(
            delay=delay,
            reason=f"{classification.category}: {classification.reasoning}",
        )
