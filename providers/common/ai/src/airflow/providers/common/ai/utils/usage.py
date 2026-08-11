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
"""Helpers for merging the ``max_cost`` convenience parameter into ``UsageLimits``."""

from __future__ import annotations

import dataclasses
from decimal import Decimal, InvalidOperation

from pydantic_ai.usage import UsageLimits


def resolve_usage_limits(
    usage_limits: UsageLimits | None, max_cost: Decimal | float | str | None
) -> UsageLimits | None:
    """
    Merge ``max_cost`` into ``usage_limits.cost_limit``, leaving other fields untouched.

    ``max_cost`` exists only because ``usage_limits`` (a ``UsageLimits`` object) cannot be
    templated -- it isn't a scalar and isn't in any operator's ``template_fields``. ``max_cost``
    is the templatable escape hatch for the single most common knob, which also means its value
    is not always in the Dag author's control (an unset Airflow Variable renders to ``""``, a
    typo renders to a non-numeric string).

    - ``max_cost is None``: ``usage_limits`` is returned unchanged (same object, by identity) --
      this keeps every existing ``usage_limits=None`` assertion in the test suite a true no-op.
    - ``max_cost`` has a value: builds (or copies) a ``UsageLimits`` with ``cost_limit`` set to
      ``Decimal(str(max_cost))`` -- never ``Decimal(max_cost)``, which would bake in binary-float
      noise for a value like ``0.1``. ``max_cost`` overrides any ``cost_limit`` already present
      on ``usage_limits``; every other field is preserved as-is.

    :raises ValueError: if ``max_cost`` cannot be parsed as a number, or is negative.
    """
    if max_cost is None:
        return usage_limits

    try:
        cost_limit = Decimal(str(max_cost))
    except InvalidOperation:
        raise ValueError(
            f"max_cost must be a number or a numeric string (got {max_cost!r}); "
            "if it is templated, check the rendered value."
        ) from None
    if cost_limit < 0:
        raise ValueError(f"max_cost must not be negative (got {max_cost!r})")

    if usage_limits is None:
        return UsageLimits(cost_limit=cost_limit)
    return dataclasses.replace(usage_limits, cost_limit=cost_limit)
