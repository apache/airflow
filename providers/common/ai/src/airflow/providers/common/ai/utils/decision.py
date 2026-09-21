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
operators call.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

from airflow.providers.common.ai.exceptions import LowConfidenceError
from airflow.providers.common.ai.policies.decision import DecisionPolicy, UncertainAction

__all__ = [
    "DECISION_XCOM_KEY",
    "DecidedBy",
    "DecisionPolicy",
    "LowConfidenceError",
    "ModelConfidence",
    "ReviewReason",
    "check_uncertain_action",
    "decision_record",
    "describe_confidence",
    "finalize_record",
    "initial_decided_by",
    "policy_fails",
    "policy_record",
    "review_reason",
    "threshold_for",
    "timed_out_record",
    "validate_decision_policy",
]

DECISION_XCOM_KEY = "decision"

ReviewReason = Literal["require_approval", "below_threshold", "missing_confidence"]
DecidedBy = Literal["model", "human", "timeout_default", "policy", "timeout"]

_UNCERTAIN: tuple[ReviewReason, ...] = ("below_threshold", "missing_confidence")


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
