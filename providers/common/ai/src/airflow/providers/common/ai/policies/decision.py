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
The decision policy an LLM operator applies to the model's answer, and the branch options it chooses from.

``DecisionPolicy`` says when the operator may act on the answer by itself and what happens when it
may not. ``BranchOption`` describes one branch of :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`
and lets it demand a different confidence than the policy's. Both are plain configuration: the operator
keeps them as attributes and builds the model-facing types at run time.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, ClassVar, Literal

__all__ = ["BranchOption", "DecisionPolicy", "UNCERTAIN_ACTIONS", "UncertainAction", "check_bar"]

UncertainAction = Literal["review", "fail"]
UNCERTAIN_ACTIONS: tuple[UncertainAction, ...] = ("review", "fail")


def check_bar(value: Any, label: str) -> float:
    """Validate one confidence bar: a number from 0 to 1, returned as a float."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{label} must be a number between 0 and 1, got {value!r}.")
    if not 0 <= value <= 1:
        raise ValueError(f"{label} must be between 0 and 1, got {value!r}.")
    return float(value)


@dataclass(frozen=True)
class DecisionPolicy:
    """
    When an LLM operator may act on the model's answer by itself, and what happens when it may not.

    :param min_confidence: The confidence, from 0 to 1, the answer needs for the operator to act
        without a person. ``None`` (default) is no gate: the operator behaves as it always has.
        Confidence comes from models that report one, such as a classifier model (TypeSafe's),
        in ``provider_details``. A model that reports none counts as under the bar, so swapping
        the connection to a text model does not silently switch off a control the author set.
    :param on_uncertain: What happens under the bar. ``"review"`` (default) sends the answer to
        human review through the same approval flow as ``require_approval``, which needs Airflow
        3.1+; ``"fail"`` fails the task with
        :class:`~airflow.providers.common.ai.exceptions.LowConfidenceError`. Inert without
        ``min_confidence``. ``require_approval=True`` on the operator always wins and asks a
        person whatever the confidence.
    """

    min_confidence: float | None = None
    on_uncertain: UncertainAction = "review"

    def __post_init__(self) -> None:
        if self.min_confidence is not None:
            object.__setattr__(self, "min_confidence", check_bar(self.min_confidence, "min_confidence"))
        if self.on_uncertain not in UNCERTAIN_ACTIONS:
            raise ValueError(f"on_uncertain must be one of {UNCERTAIN_ACTIONS}, got {self.on_uncertain!r}.")

    @property
    def gates(self) -> bool:
        """Whether a bar is set at all."""
        return self.min_confidence is not None

    @property
    def reviews(self) -> bool:
        """Whether an uncertain answer opens a human review (as opposed to failing the task)."""
        return self.gates and self.on_uncertain == "review"


@dataclass
class BranchOption:
    """
    One branch the model may pick: what choosing it means, and the confidence it needs.

    The value of :class:`~airflow.providers.common.ai.operators.llm_branch.LLMBranchOperator`'s
    ``branches`` mapping, keyed by downstream task ID. A bare string in that mapping is shorthand
    for ``BranchOption(description=...)``.

    :param description: What selecting this branch means: its scope and its boundary cases. Sent
        to the model in the output schema next to the option, so the model reads the option
        together with its meaning rather than guessing from the task ID. Supports Jinja
        templating. ``None`` presents the branch by its task ID alone.
    :param min_confidence: A bar for this branch that differs from the :class:`DecisionPolicy`'s,
        so a branch whose wrong pick costs more can demand more certainty. Needs a policy bar to
        differ from; ``None`` inherits it.
    """

    description: str | None = None
    min_confidence: float | None = None

    template_fields: ClassVar[Sequence[str]] = ("description",)

    def __post_init__(self) -> None:
        if self.min_confidence is not None:
            self.min_confidence = check_bar(self.min_confidence, "BranchOption.min_confidence")
