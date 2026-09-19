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
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any, Literal

DECISION_XCOM_KEY = "decision"

MissingConfidencePolicy = Literal["review", "fail", "proceed"]
MISSING_CONFIDENCE_POLICIES: tuple[MissingConfidencePolicy, ...] = ("review", "fail", "proceed")

ReviewReason = Literal["require_approval", "below_threshold", "missing_confidence"]


class MissingConfidenceError(ValueError):
    """A threshold is set, the model reported no confidence, and ``on_missing_confidence="fail"``."""


def _check_threshold(value: Any, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number between 0 and 1, got {value!r}.")
    if not 0 <= value <= 1:
        raise ValueError(f"{label} must be between 0 and 1, got {value!r}.")
    return float(value)


def validate_review_below(review_below: Any) -> float | dict[str, float] | None:
    """
    Validate ``review_below`` at construction and return it normalised.

    A number applies to every answer. A mapping applies a different bar per option
    (branch task ID, or output field), and an option missing from the mapping has no bar.
    """
    if review_below is None:
        return None
    if isinstance(review_below, Mapping):
        if not review_below:
            raise ValueError("review_below mapping must name at least one option; pass None for no gate.")
        return {
            str(key): _check_threshold(value, f"review_below[{key!r}]") for key, value in review_below.items()
        }
    return _check_threshold(review_below, "review_below")


def validate_missing_confidence_policy(policy: Any) -> MissingConfidencePolicy:
    if policy not in MISSING_CONFIDENCE_POLICIES:
        raise ValueError(
            f"on_missing_confidence must be one of {MISSING_CONFIDENCE_POLICIES}, got {policy!r}."
        )
    return policy


@dataclass
class ModelConfidence:
    """What the model reported about its own answer, or nothing for a model that reports nothing."""

    model: str | None = None
    confidence: dict[str, float] = field(default_factory=dict)
    probabilities: dict[str, dict[str, float]] = field(default_factory=dict)

    @classmethod
    def from_result(cls, result: Any) -> ModelConfidence:
        """Read ``provider_details`` off a pydantic-ai run result; empty maps when there are none."""
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
                    if isinstance(v, (int, float)) and not isinstance(v, bool)
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


def threshold_for(review_below: float | Mapping[str, float] | None, keys: Iterable[str]) -> float | None:
    """
    Return the bar that applies to what the model picked.

    A number is the bar for everything. A mapping is a bar per option; when several options
    were picked at once the strictest listed one applies, and when none of the picked options
    is listed there is no bar.
    """
    if review_below is None:
        return None
    if isinstance(review_below, Mapping):
        bars = [review_below[key] for key in keys if key in review_below]
        return max(bars) if bars else None
    return float(review_below)


def review_reason(
    *,
    require_approval: bool,
    threshold: float | None,
    confidence: float | None,
    on_missing_confidence: MissingConfidencePolicy,
    what: str,
) -> ReviewReason | None:
    """
    Why the answer goes to a person, or None to act on it.

    ``require_approval`` always wins. Below that, a configured bar with no reported confidence
    follows ``on_missing_confidence``: review by default, because a model swap must not
    silently switch off a control the author set; ``"fail"`` raises; ``"proceed"`` acts.
    """
    if require_approval:
        return "require_approval"
    if threshold is None:
        return None
    if confidence is None:
        if on_missing_confidence == "proceed":
            return None
        if on_missing_confidence == "fail":
            raise MissingConfidenceError(
                f"review_below is set for {what} but the model reported no confidence for its answer, "
                "and on_missing_confidence='fail'. A text model reports none; a bounded float field "
                "reports none because the probability is the answer. Use a classifier model, or set "
                "on_missing_confidence to 'review' or 'proceed'."
            )
        return "missing_confidence"
    if confidence < threshold:
        return "below_threshold"
    return None


def decision_record(
    *,
    model_confidence: ModelConfidence,
    proposed: Any,
    action: Any,
    threshold: float | None,
    review: ReviewReason | None,
    decided_by: Literal["model", "human", "timeout_default"] | None,
) -> dict[str, Any]:
    """Build the ``decision`` XCom value: JSON-serialisable, ``action`` None until a review resolves."""
    return {
        "model": model_confidence.model,
        "proposed": proposed,
        "action": action,
        "confidence": model_confidence.confidence,
        "probabilities": model_confidence.probabilities,
        "threshold": threshold,
        "review": review,
        "decided_by": decided_by,
    }


def describe_confidence(model_confidence: ModelConfidence, key: str, threshold: float | None) -> str:
    """One Markdown paragraph for a review body: the confidence, the bar, and the distribution."""
    confidence = model_confidence.confidence.get(key)
    if confidence is None:
        line = "Confidence: not reported by the model"
    else:
        line = f"Confidence: {confidence:.2f}"
    if threshold is not None:
        line += f" (review below {threshold:.2f})"
    probabilities = model_confidence.probabilities.get(key)
    if probabilities:
        ranked = sorted(probabilities.items(), key=lambda item: item[1], reverse=True)
        line += "\n\nProbabilities: " + ", ".join(f"{option} {p:.2f}" for option, p in ranked)
    return line
