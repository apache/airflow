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
Built-in validator factories for :class:`~airflow.providers.common.ai.operators.llm_data_quality.LLMDataQualityOperator`.

Each factory returns a ``Callable[[Any], bool]`` that can be placed directly in the
``validators`` dict alongside plain lambdas.  They are intentionally decoupled from
the operator so they can be tested and composed independently.

Usage::

    from airflow.providers.common.ai.utils.dq_validation import (
        null_pct_check,
        row_count_check,
        duplicate_pct_check,
        between_check,
        exact_check,
    )

    validators = {
        "email_nulls": null_pct_check(max_pct=0.05),
        "min_customers": row_count_check(min_count=1000),
        "dup_emails": duplicate_pct_check(max_pct=0.01),
        "amount_range": between_check(min_val=0.0, max_val=1_000_000.0),
        "active_flag": exact_check(expected=0),
        "negative_rev": lambda v: v == 0,
    }
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def null_pct_check(*, max_pct: float) -> Callable[[Any], bool]:
    """
    Return a validator that passes when ``value <= max_pct``.

    :param max_pct: Maximum allowed null percentage (0.0 – 1.0).
    :raises TypeError: When the metric value cannot be converted to ``float``.
    """

    def _check(value: Any) -> bool:
        try:
            return float(value) <= max_pct
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"null_pct_check(max_pct={max_pct!r}): expected a numeric value, got {value!r}"
            ) from exc

    _check.__qualname__ = f"null_pct_check(max_pct={max_pct!r})"
    _check.__repr__ = lambda: f"null_pct_check(max_pct={max_pct!r})"  # type: ignore[method-assign]
    return _check


def row_count_check(*, min_count: int) -> Callable[[Any], bool]:
    """
    Return a validator that passes when ``value >= min_count``.

    :param min_count: Minimum required row count.
    :raises TypeError: When the metric value cannot be converted to ``int``.
    """

    def _check(value: Any) -> bool:
        try:
            return int(value) >= min_count
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"row_count_check(min_count={min_count!r}): expected an integer value, got {value!r}"
            ) from exc

    _check.__qualname__ = f"row_count_check(min_count={min_count!r})"
    _check.__repr__ = lambda: f"row_count_check(min_count={min_count!r})"  # type: ignore[method-assign]
    return _check


def duplicate_pct_check(*, max_pct: float) -> Callable[[Any], bool]:
    """
    Return a validator that passes when ``value <= max_pct``.

    :param max_pct: Maximum allowed duplicate percentage (0.0 – 1.0).
    :raises TypeError: When the metric value cannot be converted to ``float``.
    """

    def _check(value: Any) -> bool:
        try:
            return float(value) <= max_pct
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"duplicate_pct_check(max_pct={max_pct!r}): expected a numeric value, got {value!r}"
            ) from exc

    _check.__qualname__ = f"duplicate_pct_check(max_pct={max_pct!r})"
    _check.__repr__ = lambda: f"duplicate_pct_check(max_pct={max_pct!r})"  # type: ignore[method-assign]
    return _check


def between_check(*, min_val: float, max_val: float) -> Callable[[Any], bool]:
    """
    Return a validator that passes when ``min_val <= value <= max_val``.

    :param min_val: Inclusive lower bound.
    :param max_val: Inclusive upper bound.
    :raises ValueError: When *min_val* > *max_val*.
    :raises TypeError: When the metric value cannot be converted to ``float``.
    """
    if min_val > max_val:
        raise ValueError(f"between_check: min_val ({min_val!r}) must be <= max_val ({max_val!r})")

    def _check(value: Any) -> bool:
        try:
            return min_val <= float(value) <= max_val
        except (TypeError, ValueError) as exc:
            raise TypeError(
                f"between_check(min_val={min_val!r}, max_val={max_val!r}): "
                f"expected a numeric value, got {value!r}"
            ) from exc

    _check.__qualname__ = f"between_check(min_val={min_val!r}, max_val={max_val!r})"
    _check.__repr__ = lambda: f"between_check(min_val={min_val!r}, max_val={max_val!r})"  # type: ignore[method-assign]
    return _check


def exact_check(*, expected: Any) -> Callable[[Any], bool]:
    """
    Return a validator that passes when ``value == expected``.

    :param expected: The exact value the metric must equal.
    """

    def _check(value: Any) -> bool:
        return value == expected

    _check.__qualname__ = f"exact_check(expected={expected!r})"
    _check.__repr__ = lambda: f"exact_check(expected={expected!r})"  # type: ignore[method-assign]
    return _check
