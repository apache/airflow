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
Model-backed retry policies, one per layer of a ladder from hardcoded to reasoning.

* **Fallback rules.** :class:`~airflow.sdk.definitions.retry_policy.ExceptionRetryPolicy` and
  ``fallback_rules``: ``RetryRule`` matches on the exception type, then the task's own
  ``retries`` and ``retry_delay``. No model. Always the floor.
* **Classifier.** :class:`ClassifierRetryPolicy`: the model names one of the author's
  ``categories`` and the :class:`ErrorCategory` table decides whether that category is
  retried, after how long, and how sure the model has to be. Tuned through descriptions and
  a confidence bar, not through reasoning. A classifier model such as TypeSafe's Jev runs
  here; a text model can too.
* **LLM.** :class:`LLMRetryPolicy`: a text model classifies the failure, decides whether to
  retry and how long to wait from ``instructions``, and explains itself.

They chain: ``ClassifierRetryPolicy(..., fallback_policy=LLMRetryPolicy(...))`` consults the
LLM when the classifier is unsure or unreachable, and whatever no layer decides falls to the
rules.

Requires Airflow 3.3+ (RetryPolicy was added in AIP-105).
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, ClassVar, Literal, cast

from pydantic import BaseModel

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

    from pydantic_ai import Agent
    from pydantic_ai.agent import AgentRunResult

    from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
    from airflow.sdk.definitions.context import Context
    from airflow.sdk.definitions.retry_policy import RetryRule

log = logging.getLogger(__name__)

__all__ = [
    "CLASSIFIER_INSTRUCTIONS",
    "DEFAULT_CATEGORIES",
    "DEFAULT_INSTRUCTIONS",
    "ClassifierRetryPolicy",
    "ErrorCategory",
    "ErrorClassification",
    "LLMRetryPolicy",
    "redact_registered_secrets",
]

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
"""The default system prompt of :class:`LLMRetryPolicy`: the taxonomy, retry rules and delays live here."""


class ErrorClassification(BaseModel):
    """Structured LLM output for error classification."""

    category: str
    """One of the categories the instructions describe, by default: rate_limit, auth, network, data, resource, transient, permanent."""
    should_retry: bool
    """Whether the operation should be retried."""
    suggested_delay_seconds: int = 0
    """How long to wait before retrying (0 if should_retry is False)."""
    reasoning: str
    """Brief explanation of the classification decision."""


@dataclass(frozen=True)
class ErrorCategory:
    """
    One kind of failure a :class:`ClassifierRetryPolicy` may name, and what it does when it does.

    The value of the policy's ``categories`` mapping, keyed by the category name the model
    answers with.

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
The default ``categories`` of :class:`ClassifierRetryPolicy`: the same seven the
:data:`DEFAULT_INSTRUCTIONS` describe, with the same retry/fail split and delays.

Retried: ``rate_limit`` after 60s, ``network`` after 10s, ``transient`` after 30s. Failed at
once: ``auth``, ``data``, ``resource``, ``permanent``. Read-only; spread it into your own
mapping to change one entry.
"""

CLASSIFIER_INSTRUCTIONS = (
    "You are an error classifier for a data pipeline system. Given an error message from a failed "
    "task, pick the single category that best describes it. Each category's description says what "
    "it covers."
)
"""
The default system prompt of :class:`ClassifierRetryPolicy`. It does not recite the
categories: those travel in the output schema with their descriptions, so a prompt built as
``CLASSIFIER_INSTRUCTIONS + hints`` should teach error strings and not name categories or delays.
"""


def redact_registered_secrets(message: str) -> str:
    """Mask values registered via ``mask_secret()``; the default ``redactor`` for the policies here."""
    # redact() is typed for arbitrary containers; a str in always yields a str out.
    return cast("str", redact(message))


_REDACTION_PARAMS_DOC = """
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
        credential fragments, PII, or other secrets. By default the message
        is run through
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


class _ModelRetryPolicy(RetryPolicy):
    """What the two model-backed policies share: the connection, the prompt, redaction, and the rules floor."""

    _default_instructions: ClassVar[str]

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
        self.instructions = instructions or self._default_instructions
        self.fallback_rules = fallback_rules
        self.timeout = timeout
        self.redactor: Callable[[str], str] | None = (
            None if not redact_exception else redactor if redactor is not None else redact_registered_secrets
        )
        self.redact_exception = redact_exception
        self.max_exception_length = max_exception_length

    def _hook(self) -> PydanticAIHook:
        from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook

        return PydanticAIHook(llm_conn_id=self.llm_conn_id, model_id=self.model_id)

    def _prompt(self, exception: BaseException, try_number: int, max_tries: int) -> str:
        # Redact before truncating -- truncating first could cut a registered secret in half.
        message = self.redactor(str(exception)) if self.redactor is not None else str(exception)
        if len(message) > self.max_exception_length:
            message = f"{message[: self.max_exception_length]}... (truncated)"
        return (
            f"Classify this error from a data pipeline task "
            f"(attempt {try_number} of {max_tries}):\n\n"
            f"{type(exception).__name__}: {message}"
        )

    def _run(
        self, agent: Agent[Any, Any], exception: BaseException, try_number: int, max_tries: int
    ) -> AgentRunResult[Any]:
        from pydantic_ai.settings import ModelSettings

        return agent.run_sync(
            self._prompt(exception, try_number, max_tries),
            model_settings=ModelSettings(timeout=self.timeout),
        )

    def _rules_decision(
        self, exception: BaseException, try_number: int, max_tries: int, context: Context | None
    ) -> RetryDecision:
        """Return the rules floor's decision: a ``fallback_rules`` match, else the task's own retry behaviour."""
        if self.fallback_rules:
            return ExceptionRetryPolicy(rules=self.fallback_rules).evaluate(
                exception, try_number, max_tries, context
            )
        return RetryDecision.default()


class LLMRetryPolicy(_ModelRetryPolicy):
    """
    Retry policy that uses an LLM to classify errors and decide retry behaviour.

    Uses :class:`~airflow.providers.common.ai.hooks.pydantic_ai.PydanticAIHook`
    to call any configured LLM provider (OpenAI, Anthropic, Bedrock, Vertex,
    Ollama, etc.) for error classification with structured output. The model
    returns an :class:`ErrorClassification`: which category the error is, whether
    to retry, how long to wait, and why, all steered by ``instructions``. This is
    the reasoning layer; for a cheap typed decision from a classifier model such as
    TypeSafe's Jev, use :class:`ClassifierRetryPolicy`, which can name this policy as
    its ``fallback_policy``.

    When the LLM call itself fails, the policy falls back to ``fallback_rules``
    (if provided) or returns DEFAULT to use the task's standard retry logic.

    :param llm_conn_id: Airflow connection ID for the LLM provider.
    :param model_id: Model identifier override (e.g. ``"openai:gpt-4o-mini"``
        for cost efficiency). If not set, uses the model from the connection.
    :param instructions: Custom system prompt for classification.
        Defaults to a general-purpose error classifier, :data:`DEFAULT_INSTRUCTIONS`.
        The instructions are the whole taxonomy: the category names, which to
        retry, and the delays.
    :param fallback_rules: Optional list of
        :class:`~airflow.sdk.definitions.retry_policy.RetryRule` applied when the
        LLM call fails. Provides a deterministic safety net.
    :param timeout: Maximum seconds to wait for the LLM response before
        falling back.  Defaults to 30s.  The LLM provider's own timeout
        (e.g. 600s for Anthropic) is much longer; this keeps the retry
        decision path fast even when the provider is degraded.
    """

    __doc__ = (__doc__ or "") + _REDACTION_PARAMS_DOC  # ``or ""`` keeps the import working under python -OO

    _default_instructions = DEFAULT_INSTRUCTIONS

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
            return self._rules_decision(exception, try_number, max_tries, context)

    def _classify(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
    ) -> RetryDecision:
        agent = self._hook().create_agent(output_type=ErrorClassification, instructions=self.instructions)
        try:
            result = self._run(agent, exception, try_number, max_tries)
        except Exception as exc:
            if "not supported by this model" in str(exc):
                # A classifier model refuses ErrorClassification's free-text fields client-side. A text
                # model whose profile lacks structured output raises the same words, so this is a hint.
                log.error(
                    "This model cannot answer ErrorClassification. If it is a classifier model such as "
                    "TypeSafe's Jev, use ClassifierRetryPolicy, which asks it a typed question."
                )
            raise
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


class ClassifierRetryPolicy(_ModelRetryPolicy):
    """
    Retry policy where the model names the kind of failure and the author's table decides.

    The model's only job is to pick one of ``categories``; it reads each one's description
    from the output schema. Whether that category is retried, after how long, and how sure
    the model has to be all come from the :class:`ErrorCategory` in the worker process.
    That is the shape a classifier model such as TypeSafe's Jev answers, in a few hundred
    milliseconds and with a confidence; a text model answers it too.

    When the model call fails, or the answer is under its confidence bar, the policy
    consults ``fallback_policy`` if set, then ``fallback_rules``, then returns DEFAULT to use
    the task's standard retry logic.

    :param llm_conn_id: Airflow connection ID for the model.
    :param model_id: Model identifier override (e.g. ``"typesafe:jev-1.13.0"``).
        If not set, uses the model from the connection.
    :param instructions: Custom system prompt. Defaults to :data:`CLASSIFIER_INSTRUCTIONS`.
        Instructions can teach the model your stack's error strings; the categories
        themselves, and what each one means, are ``categories``.
    :param fallback_rules: Optional list of
        :class:`~airflow.sdk.definitions.retry_policy.RetryRule` applied when the model
        call fails or the answer is under its confidence bar and ``fallback_policy`` decided
        nothing. Provides a deterministic safety net.
    :param timeout: Maximum seconds to wait for the model response before
        falling back.  Defaults to 30s.
    :param categories: The failure kinds the model chooses between, each with its
        description, action, delay and bar. Defaults to :data:`DEFAULT_CATEGORIES`. Passing
        this **replaces** the default mapping rather than merging into it; at least two
        categories are required. The model is constrained to these names, so an answer
        outside them is rejected before the policy acts on it.
    :param min_confidence: The confidence, from 0 to 1, the model's answer needs for the
        policy to act on it. ``None`` (default) is no bar: the answer is acted on whatever
        the confidence. Confidence comes from models that report one, such as a classifier
        model, in ``provider_details``. Under the bar, or when a bar is set and the model
        reported no confidence, the answer is discarded and ``fallback_policy``, then
        ``fallback_rules``, then the task's own retry behaviour apply, so swapping the
        connection to a text model does not silently switch off a control the author set.
        A category's own ``min_confidence`` overrides this one for that category.
    :param fallback_policy: A policy to consult when the answer is under its bar, reports no
        confidence, or the model call fails; typically an :class:`LLMRetryPolicy` on a text
        model, so the classifier handles the clear cases and a reasoning model the rest.
        Its RETRY or FAIL is used, with the reason prefixed by why the classifier's answer
        was not. A DEFAULT from it counts as no decision, whatever reason it carries, and this
        policy's ``fallback_rules`` and then the task's own retry behaviour apply. Without
        ``min_confidence`` the classifier's answer is always acted on, so this policy is
        consulted only when the classifier call itself fails.
    """

    __doc__ = (__doc__ or "") + _REDACTION_PARAMS_DOC  # ``or ""`` keeps the import working under python -OO

    _default_instructions = CLASSIFIER_INSTRUCTIONS

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
        fallback_policy: RetryPolicy | None = None,
        redactor: Callable[[str], str] | None = None,
        redact_exception: bool = True,
        max_exception_length: int = 4096,
    ) -> None:
        super().__init__(
            llm_conn_id,
            model_id,
            instructions,
            fallback_rules,
            timeout,
            redactor=redactor,
            redact_exception=redact_exception,
            max_exception_length=max_exception_length,
        )
        self.min_confidence = None if min_confidence is None else check_bar(min_confidence, "min_confidence")
        self.categories: dict[str, ErrorCategory] = self._validate_categories(
            DEFAULT_CATEGORIES if categories is None else categories
        )
        if fallback_policy is not None and not isinstance(fallback_policy, RetryPolicy):
            raise TypeError(f"fallback_policy must be a RetryPolicy, got {type(fallback_policy).__name__}.")
        self.fallback_policy = fallback_policy

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
                "Set min_confidence on ClassifierRetryPolicy as the bar for every other category."
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
            log.exception("Classifier retry classification failed, using fallback")
            outcome = "model_error"
        if isinstance(outcome, RetryDecision):
            return outcome
        if self.fallback_policy is not None:
            escalated = self._escalate(
                self.fallback_policy, exception, try_number, max_tries, context, why=outcome
            )
            if escalated is not None:
                return escalated
        return self._fall_back(exception, try_number, max_tries, context, why=outcome)

    def _escalate(
        self,
        policy: RetryPolicy,
        exception: BaseException,
        try_number: int,
        max_tries: int,
        context: Context | None,
        *,
        why: str,
    ) -> RetryDecision | None:
        """
        Consult ``fallback_policy`` and return its RETRY or FAIL, or None when it decided nothing.

        Only RETRY and FAIL are decisions. DEFAULT means the policy had nothing to add to the
        task's own settings, whatever reason it attached (its own fallback message, or a matched
        rule with ``action=DEFAULT``), so this policy's ``fallback_rules`` still get their say.
        A decision comes back with the reason prefixed by why the classifier's answer was not
        used, so a ``retry_reason`` shows the whole chain.
        """
        log.info("Classifier answer not applied (%s), consulting %s", why, type(policy).__name__)
        try:
            decision = policy.evaluate(exception, try_number, max_tries, context)
        except Exception:
            log.exception("fallback_policy failed, using fallback rules")
            return None
        if decision.action is RetryAction.DEFAULT:
            log.info(
                "%s decided nothing (%s), using fallback rules",
                type(policy).__name__,
                decision.reason or "no reason given",
            )
            return None
        prefix = f"escalated ({why})"
        return RetryDecision(
            action=decision.action,
            retry_delay=decision.retry_delay,
            reason=prefix if decision.reason is None else f"{prefix}; {decision.reason}",
        )

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
        Take the rules floor, with a reason that says the classifier's answer was not applied and why.

        A ``retry_reason`` read later is then not mistaken for a plain rule match or a classifier
        decision. A matched rule always carries a reason; an unmatched evaluation is DEFAULT with
        none. A matched rule keeps its action and reason whatever the action; on DEFAULT the worker
        applies the task's own settings and the reason reaches only the log.
        """
        prefix = f"classifier answer not applied ({why})"
        ruled = self._rules_decision(exception, try_number, max_tries, context)
        if ruled.action is not RetryAction.DEFAULT or ruled.reason is not None:
            return RetryDecision(
                action=ruled.action, retry_delay=ruled.retry_delay, reason=f"{prefix}; {ruled.reason}"
            )
        return RetryDecision(action=RetryAction.DEFAULT, reason=f"{prefix}; task retry settings apply")

    def _classify(
        self,
        exception: BaseException,
        try_number: int,
        max_tries: int,
    ) -> RetryDecision | ReviewReason | Literal["model_error"]:
        """
        Ask the model which category the failure is and act on the answer.

        Returns the reason (``"below_threshold"``, ``"missing_confidence"``, or ``"model_error"``
        for an answer the table does not know) instead of a decision when the answer is not
        acted on, so :meth:`evaluate` takes the escalation and fallback path and can say why.
        """
        output_type = described_choices(
            "ErrorCategory", {name: category.description for name, category in self.categories.items()}
        )
        agent = self._hook().create_agent(output_type=output_type, instructions=self.instructions)
        result = self._run(agent, exception, try_number, max_tries)
        name = picked_key(result.output)
        category = self.categories.get(name)
        if category is None:
            # The output type constrains the answer to the configured names, so reaching this needs
            # the schema and the table to disagree.
            log.error("Classifier answered %r, which is not a configured category", name)
            return "model_error"

        # A bare output type is one field, ``response``; its confidence is what the bar is compared against.
        model_confidence = ModelConfidence.from_result(result)
        model_name = model_confidence.model or "n/a"
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
                "Classifier answer not applied (%s): %s. model=%s probabilities=%s",
                uncertain,
                summary,
                model_name,
                model_confidence.probabilities.get(BARE_OUTPUT_FIELD) or "n/a",
            )
            return uncertain

        if category.retry:
            delay_text = "task default" if category.delay is None else f"{category.delay.total_seconds():g}s"
            reason = f"{summary} action=retry delay={delay_text}"
            decision = RetryDecision.retry(delay=category.delay, reason=reason)
        else:
            reason = f"{summary} action=fail"
            decision = RetryDecision.fail(reason=reason)
        log.info("Classifier decision: %s model=%s", reason, model_name)
        return decision
