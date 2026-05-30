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

from typing import Any


class WaitPolicy:
    """
    An object the scheduler asks whether a partitioned Dag run should fire.

    Concrete policies are ``WaitForAll`` and ``MinimumCount``. Each implements
    ``is_satisfied(matched, expected)`` and ``is_unreachable(expected)``; the
    scheduler calls these methods directly in the hot path on every tick.
    """

    def is_satisfied(self, matched: int, expected: int) -> bool:
        raise NotImplementedError

    def is_unreachable(self, expected: int) -> bool:
        raise NotImplementedError

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> WaitPolicy:
        raise NotImplementedError


class WaitForAll(WaitPolicy):
    """
    Fires only when every expected upstream key has arrived.

    ``matched == expected`` is the satisfaction condition, including the
    vacuously-true case where both are zero (empty window).
    ``is_unreachable`` always returns ``False`` — even an empty window
    satisfies vacuously.
    """

    def is_satisfied(self, matched: int, expected: int) -> bool:
        return matched == expected

    def is_unreachable(self, expected: int) -> bool:
        return False

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> WaitForAll:
        return cls()

    def __repr__(self) -> str:
        return "WaitForAll()"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, WaitForAll)

    def __hash__(self) -> int:
        return hash(type(self))


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

    def __init__(self, n: int) -> None:
        if n == 0:
            raise ValueError(
                "MinimumCount(0) is degenerate: n=0 would always fire, even on empty windows. "
                "Use WaitForAll() to require every key, or MinimumCount(n) with n != 0."
            )
        self.n = n

    def is_satisfied(self, matched: int, expected: int) -> bool:
        if self.n > 0:
            return matched >= self.n
        return matched >= max(0, expected + self.n)

    def is_unreachable(self, expected: int) -> bool:
        if self.n > 0:
            return self.n > expected
        return False

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> MinimumCount:
        return cls(data["n"])

    def __repr__(self) -> str:
        return f"MinimumCount(n={self.n})"

    def __eq__(self, other: object) -> bool:
        return isinstance(other, MinimumCount) and self.n == other.n

    def __hash__(self) -> int:
        return hash((type(self), self.n))
