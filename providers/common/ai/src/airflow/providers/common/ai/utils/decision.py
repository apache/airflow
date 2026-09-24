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
Confidence gating and the decision record shared by the LLM operators.

A classifier model such as TypeSafe's reports a confidence per output field in
``ModelResponse.provider_details`` (``{"confidence": {field: float}, "probabilities":
{field: {option: float}}}``). A text model reports nothing there. These helpers read
that, decide whether an answer should go to a person before the operator acts on it,
and shape the ``decision`` XCom record that says what was proposed, what was done, and
why.

Confidence is a statistic on the shape of the probability distribution the model
returned: concentrated on one option is high, spread out is low. It is not the
probability that the answer is correct, and the gate does not describe it as such.

The public configuration lives in :mod:`airflow.providers.common.ai.policies.decision`
(:class:`DecisionPolicy`, :class:`BranchOption`); this module is the machinery the
operators and the retry policy call, including :func:`described_choices`, which builds
the option type a model picks from with each option's description in the schema.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Literal

from airflow.providers.common.ai.exceptions import LowConfidenceError
from airflow.providers.common.ai.policies.decision import DecisionPolicy, UncertainAction

__all__ = [
    "BARE_OUTPUT_FIELD",
    "DECISION_XCOM_KEY",
    "DecidedBy",
    "DecisionPolicy",
    "LowConfidenceError",
    "ModelConfidence",
    "ReviewReason",
    "check_uncertain_action",
    "decision_record",
    "describe_confidence",
    "described_choices",
    "finalize_record",
    "initial_decided_by",
    "picked_key",
    "policy_fails",
    "policy_record",
    "review_reason",
    "threshold_for",
    "timed_out_record",
    "validate_decision_policy",
]

DECISION_XCOM_KEY = "decision"

BARE_OUTPUT_FIELD = "response"
"""The field name a classifier model reports a bare (non-object) output type's confidence under."""

ReviewReason = Literal["require_approval", "below_threshold", "missing_confidence"]
DecidedBy = Literal["model", "human", "timeout_default", "policy", "timeout"]

_UNCERTAIN: tuple[ReviewReason, ...] = ("below_threshold", "missing_confidence")

_NOT_LOADED = object()
Choice: Any = _NOT_LOADED
Choices: Any = _NOT_LOADED


def _choice_types() -> tuple[Any, Any]:
    """
    Return pydantic-ai's ``(Choice, Choices)``, or ``(None, None)`` before 2.46.0.

    Loaded on first use rather than at import: importing ``pydantic_ai`` costs most of a second,
    and this module is imported by the retry policy at Dag-parse time, where nothing needs it yet.
    Tests patch the module attributes directly, which this honours.
    """
    global Choice, Choices
    if Choice is _NOT_LOADED or Choices is _NOT_LOADED:
        try:
            from pydantic_ai import (  # type: ignore[attr-defined]
                Choice as loaded_choice,
                Choices as loaded_choices,
            )
        except ImportError:  # pydantic-ai < 2.46.0: build the same schema from an Enum instead
            Choice = Choices = None
        else:
            Choice, Choices = loaded_choice, loaded_choices
    return Choice, Choices


def validate_decision_policy(policy: Any) -> DecisionPolicy:
    """Accept a :class:`DecisionPolicy` or None (no gate); reject anything else at construction."""
    if policy is None:
        return DecisionPolicy()
    if not isinstance(policy, DecisionPolicy):
        raise TypeError(f"decision_policy must be a DecisionPolicy, got {type(policy).__name__}.")
    return policy


@dataclass
class ModelConfidence:
    """What the model reported about its own answer, or nothing for a model that reports nothing."""

    model: str | None = None
    confidence: dict[str, float] = field(default_factory=dict)
    probabilities: dict[str, dict[str, float]] = field(default_factory=dict)

    @classmethod
    def from_result(cls, result: Any) -> ModelConfidence:
        """
        Read ``provider_details`` off a pydantic-ai run result; empty maps when there are none.

        A confidence that is not a finite number is treated as not reported, so a NaN can never
        pass the gate as confident.
        """
        response = getattr(result, "response", None)
        model = getattr(response, "model_name", None)
        details = getattr(response, "provider_details", None)
        confidence: dict[str, float] = {}
        probabilities: dict[str, dict[str, float]] = {}
        if isinstance(details, Mapping):
            raw_confidence = details.get("confidence")
            if isinstance(raw_confidence, Mapping):
                confidence = {
                    str(k): float(v)
                    for k, v in raw_confidence.items()
                    if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v)
                }
            raw_probabilities = details.get("probabilities")
            if isinstance(raw_probabilities, Mapping):
                probabilities = {
                    str(k): {str(option): float(p) for option, p in v.items()}
                    for k, v in raw_probabilities.items()
                    if isinstance(v, Mapping)
                }
        return cls(
            model=model if isinstance(model, str) else None,
            confidence=confidence,
            probabilities=probabilities,
        )

    def lowest(self, keys: Iterable[str] | None = None) -> float | None:
        """Return the least confident of the given fields (all when ``keys`` is None), or None if none reported."""
        values = [value for name, value in self.confidence.items() if keys is None or name in keys]
        return min(values) if values else None


def threshold_for(
    min_confidence: float | None, overrides: Mapping[str, float] | None, keys: Iterable[str]
) -> float | None:
    """
    Return the bar that applies to what the model picked.

    No policy bar means no bar, whatever the overrides say (the branch operator rejects that
    combination at construction). Otherwise every picked option has a bar, its own or the
    policy's, and when several were picked at once the strictest applies.
    """
    if min_confidence is None:
        return None
    bars = [(overrides or {}).get(key, min_confidence) for key in keys]
    return max(bars) if bars else min_confidence


def review_reason(
    *, require_approval: bool, threshold: float | None, confidence: float | None
) -> ReviewReason | None:
    """
    Why the answer does not proceed on its own, or None to act on it.

    ``require_approval`` always wins. Below that, no bar means proceed. A bar with no reported
    confidence is ``"missing_confidence"``: a text model reports none, and so does a bounded
    float field, and a model swap must not silently switch off a control the author set. A
    reported confidence under the bar is ``"below_threshold"``. What happens next is
    ``on_uncertain``'s call; see :func:`check_uncertain_action`.
    """
    if require_approval:
        return "require_approval"
    if threshold is None:
        return None
    if confidence is None:
        return "missing_confidence"
    if confidence < threshold:
        return "below_threshold"
    return None


def policy_fails(review: ReviewReason | None, on_uncertain: UncertainAction) -> bool:
    """Whether ``on_uncertain="fail"`` applies to this answer: it is uncertain and the policy says fail."""
    return on_uncertain == "fail" and review in _UNCERTAIN


def initial_decided_by(review: ReviewReason | None, on_uncertain: UncertainAction) -> DecidedBy | None:
    """
    Who decides when the record is first written.

    ``"model"`` when the answer proceeds on its own; ``"policy"`` when ``on_uncertain="fail"``
    is about to fail the task, so the record explains the failure without a join on task state;
    None while a review is pending, until :func:`finalize_record` fills it in.
    """
    if review is None:
        return "model"
    if policy_fails(review, on_uncertain):
        return "policy"
    return None


def check_uncertain_action(
    review: ReviewReason | None,
    on_uncertain: UncertainAction,
    *,
    what: str,
    confidence: float | None,
    threshold: float | None,
) -> None:
    """Raise :class:`LowConfidenceError` when the answer is under its bar and the action is ``"fail"``."""
    if not policy_fails(review, on_uncertain):
        return
    if review == "missing_confidence":
        raise LowConfidenceError(
            f"min_confidence={threshold:.2f} is set for {what} but the model reported no confidence for "
            "its answer, and on_uncertain='fail'. A text model reports none; a bounded float field "
            "reports none because the probability is the answer. Use a classifier model, remove the "
            "bar, or set on_uncertain='review'."
        )
    raise LowConfidenceError(
        f"Confidence {confidence:.2f} for {what} is below min_confidence={threshold:.2f}, "
        "and on_uncertain='fail'."
    )


def policy_record(policy: DecisionPolicy, branch_bars: Mapping[str, float] | None = None) -> dict[str, Any]:
    """Record the gate settings in force when a decision was made, as plain JSON."""
    return {
        "min_confidence": policy.min_confidence,
        "on_uncertain": policy.on_uncertain,
        "branches": dict(branch_bars) if branch_bars else None,
    }


def decision_record(
    *,
    model_confidence: ModelConfidence,
    proposed: Any,
    action: Any,
    threshold: float | None,
    review: ReviewReason | None,
    decided_by: DecidedBy | None,
    policy: dict[str, Any],
) -> dict[str, Any]:
    """Build the ``decision`` XCom value: JSON-serialisable, ``action`` None until a review resolves."""
    return {
        "model": model_confidence.model,
        "proposed": proposed,
        "action": action,
        "confidence": model_confidence.confidence,
        "probabilities": model_confidence.probabilities,
        "min_confidence": threshold,
        "review": review,
        "decided_by": decided_by,
        "policy": policy,
    }


def finalize_record(record: dict[str, Any], event: dict[str, Any], *, action: Any) -> dict[str, Any]:
    """Copy a pending record with what ran and who decided, read from the review response."""
    timed_out = bool(event.get("timedout")) or event.get("responded_by_user") is None
    return {**record, "action": action, "decided_by": "timeout_default" if timed_out else "human"}


def timed_out_record(record: dict[str, Any]) -> dict[str, Any]:
    """Copy a pending record for a review that timed out with no default answer: nothing ran, nobody decided."""
    return {**record, "action": None, "decided_by": "timeout"}


def describe_confidence(model_confidence: ModelConfidence, key: str, threshold: float | None) -> str:
    """One Markdown paragraph for a review body: the confidence, the bar, and the distribution."""
    confidence = model_confidence.confidence.get(key)
    if confidence is None:
        line = "Confidence: not reported by the model"
    else:
        line = f"Confidence: {confidence:.2f}"
    if threshold is not None:
        line += f" (minimum {threshold:.2f})"
    probabilities = model_confidence.probabilities.get(key)
    if probabilities:
        ranked = sorted(probabilities.items(), key=lambda item: item[1], reverse=True)
        line += "\n\nProbabilities: " + ", ".join(f"{option} {p:.2f}" for option, p in ranked)
    return line


def described_choices(name: str, options: Mapping[str, str | None]) -> type[Any]:
    """
    Build the type a model picks one of ``options`` from, each option carrying its description.

    With a description on any option the schema renders as ``anyOf`` of ``{const, description}``
    instead of a bare ``enum`` list. That is the one JSON Schema shape that carries a description
    per value, and it is what both a text model's tool schema and pydantic-ai's TypeSafe adapter
    read an option's meaning from. On pydantic-ai 2.46+ the type is its ``Choices``; before that,
    an ``Enum`` whose schema hook emits the same shape. Either way the model has to answer with one
    of the keys, in the order given, and :func:`picked_key` returns that key whichever type answered.
    """
    keys = list(options)
    descriptions = {key: text for key, text in options.items() if text}
    choice, choices = _choice_types()
    if choices is not None:
        if not descriptions:
            return choices(keys, name=name)
        return choices({key: choice(descriptions.get(key)) for key in keys}, name=name)

    # Generated member names: Enum reserves ``_sunder_`` names and ``mro``, and a key need not be an
    # identifier. The key is the member's value, which is what the schema, the model and ``picked`` use.
    enum_cls: type[Enum] = Enum(name, {f"option_{i}": key for i, key in enumerate(keys)})  # type: ignore[misc]
    if not descriptions:
        return enum_cls

    def json_schema(cls: type[Enum], core_schema: Any, handler: Any) -> dict[str, Any]:
        rendered: list[dict[str, Any]] = []
        for member in cls:
            option: dict[str, Any] = {"const": member.value, "type": "string"}
            if text := descriptions.get(member.value):
                option["description"] = text
            rendered.append(option)
        return {"anyOf": rendered, "title": cls.__name__}

    # pydantic looks this hook up on the type when it builds the schema, so attaching it to the
    # functional-API enum is the same as defining it in a class body.
    setattr(enum_cls, "__get_pydantic_json_schema__", classmethod(json_schema))
    return enum_cls


def picked_key(value: Any) -> str:
    """Return the key a picked option stands for: ``Choices`` answers with the key, the Enum fallback with a member."""
    return value.value if isinstance(value, Enum) else str(value)
