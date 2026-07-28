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
from __future__ import annotations

from typing import TYPE_CHECKING, Any

import attrs

if TYPE_CHECKING:
    from collections.abc import Set


@attrs.define(frozen=True)
class PartitionSatisfaction:
    """
    Structured result returned by :meth:`WaitPolicy.is_satisfied_by_keys`.

    :param satisfied: ``True`` if the matched key set meets the policy's firing threshold.
    :param unreachable: ``True`` if the policy threshold can never be met given the window's
        cardinality, regardless of how many upstream events arrive.
    :param unreachable_reason: Human-readable explanation of why the policy is unreachable,
        constructed atomically by the policy from its own repr and the window's cardinality.
        Non-``None`` if and only if ``unreachable`` is ``True``; the scheduler may forward
        this string directly to :meth:`~logging.Logger.warning` without further formatting.
    """

    satisfied: bool
    unreachable: bool
    unreachable_reason: str | None

    def __attrs_post_init__(self) -> None:
        if self.unreachable and self.unreachable_reason is None:
            raise ValueError("unreachable_reason must be set when unreachable is True")
        if not self.unreachable and self.unreachable_reason is not None:
            raise ValueError("unreachable_reason must be None when unreachable is False")


class WaitPolicy:
    """
    An object the scheduler asks whether a partitioned Dag run should fire.

    Concrete policies are ``WaitForAll`` and ``MinimumCount``. The scheduler
    calls only :meth:`is_satisfied_by_keys`, which returns a :class:`PartitionSatisfaction`
    carrying both the satisfaction result and the unreachability flag.
    :meth:`is_satisfied` and :meth:`is_unreachable` are internal collaboration
    points used by policy implementations; they are not called directly by the
    scheduler.

    :meta private:
    """

    def is_satisfied(self, matched: int, expected: int) -> bool:
        raise NotImplementedError

    def is_satisfied_by_keys(self, *, matched: Set[str], expected: Set[str]) -> PartitionSatisfaction:
        """
        Return a :class:`PartitionSatisfaction` for the given key sets.

        The base default converts sets to counts, then calls :meth:`is_satisfied`
        and :meth:`is_unreachable` — both using ``len(expected)`` as the cardinality.
        Override to avoid materialising the full intersection (see ``WaitForAll``).
        """
        cardinality = len(expected)
        unreachable = self.is_unreachable(cardinality)
        return PartitionSatisfaction(
            satisfied=self.is_satisfied(matched=len(matched & expected), expected=cardinality),
            unreachable=unreachable,
            unreachable_reason=(
                f"wait policy {self!r} can never be satisfied given the window's cardinality {cardinality}"
                if unreachable
                else None
            ),
        )

    def is_unreachable(self, expected: int) -> bool:
        raise NotImplementedError

    def serialize(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> WaitPolicy:
        raise NotImplementedError


@attrs.define(frozen=True)
class WaitForAll(WaitPolicy):
    """
    Fires only when every expected upstream key has arrived (``matched == expected``).

    An empty window (both zero) is vacuously satisfied and never unreachable.
    """

    def is_satisfied(self, matched: int, expected: int) -> bool:
        return matched == expected

    def is_satisfied_by_keys(self, *, matched: Set[str], expected: Set[str]) -> PartitionSatisfaction:
        # Short-circuits on the first missing key; avoids materializing the full intersection.
        return PartitionSatisfaction(
            satisfied=(expected <= matched), unreachable=False, unreachable_reason=None
        )

    def is_unreachable(self, expected: int) -> bool:
        return False

    def serialize(self) -> dict[str, Any]:
        return {}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> WaitForAll:
        return cls()


@attrs.define(frozen=True)
class MinimumCount(WaitPolicy):
    """
    Fires once a minimum number of upstream keys have arrived.

    ``n > 0``: fires when ``matched >= n`` (absolute lower bound).

    ``n < 0``: fires when ``matched >= max(0, expected + n)`` — i.e. at most
    ``-n`` keys are still missing. The clamp ensures negative offsets never
    produce a negative effective threshold, keeping the empty-window case
    vacuously satisfied.

    ``n == 0`` is rejected at construction because it is degenerate: zero
    would always fire, even on empty ticks.

    ``is_unreachable(expected)`` returns ``True`` when ``n > 0`` and
    ``n > expected``, meaning the threshold can never be met regardless of
    how many upstream events arrive. Negative ``n`` is bounded by ``expected``
    after the clamp, so it is never unreachable.
    """

    n: int = attrs.field()

    @n.validator
    def _validate_n(self, attribute: attrs.Attribute, value: int) -> None:
        if value == 0:
            raise ValueError(
                "MinimumCount(0) is degenerate: n=0 would always fire, even on empty windows. "
                "Use WaitForAll() to require every key, or MinimumCount(n) with n != 0."
            )

    def is_satisfied(self, matched: int, expected: int) -> bool:
        if self.n > 0:
            return matched >= self.n
        return matched >= max(0, expected + self.n)

    def is_unreachable(self, expected: int) -> bool:
        if self.n > 0:
            return self.n > expected
        return False

    def serialize(self) -> dict[str, Any]:
        return {"n": self.n}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> MinimumCount:
        return cls(data["n"])
