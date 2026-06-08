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

import attrs


class WaitPolicy:
    """
    An object the scheduler asks whether a partitioned Dag run should fire.

    Concrete policies are ``WaitForAll`` and ``MinimumCount``. The scheduler
    calls ``is_satisfied(matched, expected)`` and ``is_unreachable(expected)``
    on the core-side counterparts; this SDK class is the author-facing type
    for Dag file declarations.

    :meta private:
    """


@attrs.define(frozen=True)
class WaitForAll(WaitPolicy):
    """
    Fires only when every expected upstream key has arrived.

    ``matched == expected`` is the satisfaction condition, including the
    vacuously-true case where both are zero (empty window).
    """


@attrs.define(frozen=True)
class MinimumCount(WaitPolicy):
    """
    Fires once a minimum number of upstream keys have arrived.

    ``n > 0``: fires when ``matched >= n`` (absolute lower bound).

    ``n < 0``: fires when ``matched >= max(0, expected + n)`` — i.e. at most
    ``-n`` keys are still missing. Use this to tolerate occasional producer
    dropouts without blocking the downstream Dag run indefinitely.

    ``n == 0`` is rejected at construction because it is degenerate: zero
    would always fire (even on empty ticks) which forces the caller to choose
    ``WaitForAll`` or a positive ``n`` that expresses intent.

    Sign convention example::

        MinimumCount(5)  # fire once >=5 keys arrived
        MinimumCount(-3)  # fire once at most 3 keys are still missing
    """

    n: int = attrs.field()

    @n.validator
    def _validate_n(self, attribute: attrs.Attribute, value: int) -> None:
        if value == 0:
            raise ValueError(
                "MinimumCount(0) is degenerate: n=0 would always fire, even on empty windows. "
                "Use WaitForAll() to require every key, or MinimumCount(n) with n != 0."
            )
