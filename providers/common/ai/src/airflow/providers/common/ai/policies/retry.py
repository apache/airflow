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

The model answers one question, which category the failure belongs to, and everything
else is derived in the worker from the author's :class:`ErrorCategory` table: whether the
category is retried, after how long, and how sure the model has to be. That shape is what
a classifier model such as TypeSafe's Jev answers, and a text model answers it too.

Requires Airflow 3.3+ (RetryPolicy was added in AIP-105).
"""

from __future__ import annotations

import logging
import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, cast

from airflow.providers.common.ai.policies.decision import check_bar
from airflow.providers.common.ai.utils.decision import (
    BARE_OUTPUT_FIELD,
    ModelConfidence,
    ReviewReason,
    described_choices,
    picked_key,
    review_reason,
    threshold_for,
)
from airflow.providers.common.compat.sdk import redact

try:
    from airflow.sdk.definitions.retry_policy import (
        ExceptionRetryPolicy,
        RetryAction,
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

__all__ = [
    "DEFAULT_CATEGORIES",
    "ErrorCategory",
    "LLMRetryPolicy",
    "redact_registered_secrets",
]


@dataclass(frozen=True)
class ErrorCategory:
    """
    One kind of failure the model may name, and what the policy does when it does.

    The value of :class:`LLMRetryPolicy`'s ``categories`` mapping, keyed by the category
    name the model answers with.

    :param description: What failures belong here. Sent to the model in the output schema
        next to the category name, so the model reads the option together with its meaning
        rather than guessing from the name.
    :param retry: Whether a failure in this category is retried (``True``, default) or fails
        the task at once.
    :param delay: How long to wait before the retry. ``None`` (default) keeps the task's own
        ``retry_delay`` and backoff. Only meaningful with ``retry=True``.
    :param min_confidence: A bar for this category that differs from the policy's
        ``min_confidence``, so a category whose wrong pick costs more (``permanent`` ends
        the task) can demand more certainty. Needs a policy bar to differ from; ``None``
        inherits it.
    """

    description: str
    retry: bool = True
    delay: timedelta | None = None
    min_confidence: float | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.description, str) or not self.description.strip():
            raise ValueError("ErrorCategory.description must be a non-empty string.")
        if not isinstance(self.retry, bool):
            raise TypeError(f"ErrorCategory.retry must be True or False, got {self.retry!r}.")
        if self.delay is not None:
            if not isinstance(self.delay, timedelta):
                raise TypeError(
                    f"ErrorCategory.delay must be a timedelta or None, got {self.delay!r}. "
                    "Use timedelta(seconds=60) rather than 60."
                )
            if self.delay < timedelta(0):
                raise ValueError(f"ErrorCategory.delay must not be negative, got {self.delay!r}.")
            if not self.retry:
                raise ValueError(
                    "ErrorCategory.delay is set but retry=False; a failed task has no retry to delay."
                )
        if self.min_confidence is not None:
            object.__setattr__(
                self, "min_confidence", check_bar(self.min_confidence, "ErrorCategory.min_confidence")
            )


DEFAULT_CATEGORIES: Mapping[str, ErrorCategory] = MappingProxyType(
    {
        "rate_limit": ErrorCategory("API throttling or a quota exceeded.", delay=timedelta(seconds=60)),
        "auth": ErrorCategory("Credentials invalid, expired, or missing permissions.", retry=False),
        "network": ErrorCategory(
            "Transient connectivity issue: connection reset, DNS, TLS handshake.", delay=timedelta(seconds=10)
        ),
        "data": ErrorCategory("Schema validation, type mismatch, or bad input data.", retry=False),
        "resource": ErrorCategory(
            "Resource not found or unavailable, such as a missing table or bucket.", retry=False
        ),
        "transient": ErrorCategory(
            "Temporary issue likely to resolve on its own.", delay=timedelta(seconds=30)
        ),
        "permanent": ErrorCategory(
            "Problem that will not resolve without a code or configuration change.", retry=False
        ),
    }
)
"""
The example taxonomy :class:`LLMRetryPolicy` classifies into when ``categories`` is not set.

Retried: ``rate_limit`` after 60s, ``network`` after 10s, ``transient`` after 30s. Failed at
once: ``auth``, ``data``, ``resource``, ``permanent``. Read-only; spread it into your own
mapping to change one entry.
"""

DEFAULT_INSTRUCTIONS = (
    "You are an error classifier for a data pipeline system. Given an error message from a failed "
    "task, pick the single category that best describes it. Each category's description says what "
    "it covers."
)
"""
The default system prompt. It no longer recites the categories: those travel in the output
schema from ``categories``, with their descriptions, so a prompt built as
``DEFAULT_INSTRUCTIONS + hints`` should teach error strings and not name categories or delays.
"""


def redact_registered_secrets(message: str) -> str:
    """Mask values registered via ``mask_secret()``; the default ``redactor`` for :class:`LLMRetryPolicy`."""
    # redact() is typed for arbitrary containers; a str in always yields a str out.
    return cast("str", redact(message))


class LLMRetryPolicy(RetryPolicy):
    """
    Retry policy that uses an LLM to name the kind of failure, then acts on the author's table.

    Uses :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
    to call any configured LLM provider (OpenAI, Anthropic, Bedrock, Vertex,
    Ollama, a classifier model such as TypeSafe's Jev, etc.).

    The model's only job is to pick one of ``categories``; it reads each one's description
    from the output schema. Whether that category is retried, after how long, and how sure
    the model has to be all come from the :class:`ErrorCategory` in the worker process.

    When the LLM call itself fails, or the model is not sure enough to act on, the policy
    falls back to ``fallback_rules`` (if provided) or returns DEFAULT to use the task's
    standard retry logic.

    :param llm_conn_id: Airflow connection ID for the LLM provider.
    :param model_id: Model identifier override (e.g. ``"openai:gpt-4o-mini"``
        for cost efficiency, or ``"typesafe:jev-1.13.0"`` for a classifier model).
        If not set, uses the model from the connection.
    :param instructions: Custom system prompt for classification. Defaults to a general
        error classifier. Instructions can teach the model your stack's error strings; the
        categories themselves, and what each one means, are ``categories``.
    :param fallback_rules: Optional list of
        :class:`~airflow.sdk.definitions.retry_policy.RetryRule` applied when the LLM call
        fails or the answer is under its confidence bar. Provides a deterministic safety net.
    :param timeout: Maximum seconds to wait for the LLM response before
        falling back.  Defaults to 30s.  The LLM provider's own timeout
        (e.g. 600s for Anthropic) is much longer; this keeps the retry
        decision path fast even when the provider is degraded.
    :param categories: The failure kinds the model chooses between, each with its
        description, action, delay and bar. Defaults to :data:`DEFAULT_CATEGORIES`. Passing
        this **replaces** the default mapping rather than merging into it; at least two
        categories are required. The model is constrained to these names, so an answer
        outside them is rejected before the policy acts on it.
    :param min_confidence: The confidence, from 0 to 1, the model's answer needs for the
        policy to act on it. ``None`` (default) is no bar: the answer is acted on as it
        always was. Confidence comes from models that report one, such as a classifier
        model, in ``provider_details``. Under the bar, or when a bar is set and the model
        reported no confidence, the answer is discarded and ``fallback_rules`` then the
        task's own retry behaviour apply, so swapping the connection to a text model does
        not silently switch off a control the author set. A category's own
        ``min_confidence`` overrides this one for that category.
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
        categories: Mapping[str, ErrorCategory] | None = None,
        min_confidence: float | None = None,
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
        if instructions and categories is None:
            # The one silent upgrade case: a 0.9.x prompt that named its own categories or delays.
            warnings.warn(
                "LLMRetryPolicy: instructions are custom but categories is not set, so the model chooses "
                "between DEFAULT_CATEGORIES and their delays. Instructions no longer define categories, "
                "retry or delay; if yours name a category outside the defaults or a delay, move it into "
                "categories={name: ErrorCategory(...)}.",
                UserWarning,
                stacklevel=2,
            )
        self.instructions = instructions or DEFAULT_INSTRUCTIONS
        self.fallback_rules = fallback_rules
        self.timeout = timeout
        self.min_confidence = None if min_confidence is None else check_bar(min_confidence, "min_confidence")
        self.categories: Mapping[str, ErrorCategory] = self._validate_categories(
            DEFAULT_CATEGORIES if categories is None else categories
        )
        self.redactor: Callable[[str], str] | None = (
            None if not redact_exception else redactor if redactor is not None else redact_registered_secrets
        )
        self.redact_exception = redact_exception
        self.max_exception_length = max_exception_length

    def _validate_categories(self, categories: Mapping[str, ErrorCategory]) -> dict[str, ErrorCategory]:
        if not isinstance(categories, Mapping):
            raise TypeError(
                f"categories must be a mapping of name to ErrorCategory, got {type(categories).__name__}."
            )
        if len(categories) < 2:
            raise ValueError(
                f"categories needs at least two entries for the model to choose between, got {len(categories)}."
            )
        for name, category in categories.items():
            if not isinstance(name, str) or not name.strip():
                raise ValueError(f"categories keys must be non-empty strings, got {name!r}.")
            if not isinstance(category, ErrorCategory):
                raise TypeError(
                    f"categories[{name!r}] must be an ErrorCategory, got {type(category).__name__}."
                )
        with_bar = sorted(
            name for name, category in categories.items() if category.min_confidence is not None
        )
        if with_bar and self.min_confidence is None:
            raise ValueError(
                f"categories {with_bar} set min_confidence but the policy has none to inherit. "
                "Set min_confidence on LLMRetryPolicy as the bar for every other category."
            )
        # A fresh dict, not the caller's mapping: DEFAULT_CATEGORIES is a mappingproxy, which
        # deepcopy rejects, and TaskGroup(default_args=...) and operator copies deep-copy the policy.
        return dict(categories)

    @property
    def _category_bars(self) -> dict[str, float]:
        return {
            name: category.min_confidence
            for name, category in self.categories.items()
            if category.min_confidence is not None
        }

    def evaluate(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
        context: Context | None = None,
    ) -> RetryDecision:
        outcome: RetryDecision | str
        try:
            outcome = self._classify(exception, try_number, max_tries)
        except Exception:
            log.exception("LLM retry classification failed, using fallback")
            outcome = "model_error"
        if isinstance(outcome, RetryDecision):
            return outcome
        return self._fall_back(exception, try_number, max_tries, context, why=outcome)

    def _fall_back(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
        context: Context | None,
        *,
        why: str,
    ) -> RetryDecision:
        """
        Take the deterministic path: ``fallback_rules`` when one matches, else the task's own retry behaviour.

        The decision's reason says the model's answer was not applied and why, so a ``retry_reason``
        read later is not mistaken for a plain rule match or for a classifier decision.
        """
        prefix = f"LLM classification not applied ({why})"
        if self.fallback_rules:
            ruled = ExceptionRetryPolicy(rules=self.fallback_rules).evaluate(
                exception, try_number, max_tries, context
            )
            if ruled.action is not RetryAction.DEFAULT:
                return RetryDecision(
                    action=ruled.action, retry_delay=ruled.retry_delay, reason=f"{prefix}; {ruled.reason}"
                )
        return RetryDecision(action=RetryAction.DEFAULT, reason=f"{prefix}; task retry settings apply")

    def _classify(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
    ) -> RetryDecision | ReviewReason:
        """
        Ask the model which category the failure is and act on the answer.

        Returns the reason (``"below_threshold"`` or ``"missing_confidence"``) instead of a
        decision when the answer is under its confidence bar, so :meth:`evaluate` takes the
        same fallback path it takes when the model call fails and can say why.
        """
        from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook

        hook = PydanticAIHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)
        output_type = described_choices(
            "ErrorCategory", {name: category.description for name, category in self.categories.items()}
        )
        agent = hook.create_agent(output_type=output_type, instructions=self.instructions)

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
        # The output type validated the answer, so it is one of the configured names.
        name = picked_key(result.output)
        category = self.categories[name]

        # A bare output type is one field, ``response``; its confidence is what the bar is compared against.
        model_confidence = ModelConfidence.from_result(result)
        confidence = model_confidence.confidence.get(BARE_OUTPUT_FIELD)
        threshold = threshold_for(self.min_confidence, self._category_bars, [name])
        uncertain = review_reason(require_approval=False, threshold=threshold, confidence=confidence)

        summary = (
            f"category={name}"
            f" confidence={'n/a' if confidence is None else f'{confidence:.2f}'}"
            f" threshold={'n/a' if threshold is None else f'{threshold:.2f}'}"
        )
        if uncertain is not None:
            log.info(
                "LLM error classification not acted on (%s): %s, using fallback. model=%s probabilities=%s",
                uncertain,
                summary,
                model_confidence.model or "n/a",
                model_confidence.probabilities.get(BARE_OUTPUT_FIELD) or "n/a",
            )
            return uncertain

        if not category.retry:
            reason = f"{summary} action=fail"
            log.info("LLM error classification: %s model=%s", reason, model_confidence.model or "n/a")
            return RetryDecision.fail(reason=reason)

        delay_text = "task default" if category.delay is None else f"{category.delay.total_seconds():g}s"
        reason = f"{summary} action=retry delay={delay_text}"
        log.info("LLM error classification: %s model=%s", reason, model_confidence.model or "n/a")
        return RetryDecision.retry(delay=category.delay, reason=reason)
