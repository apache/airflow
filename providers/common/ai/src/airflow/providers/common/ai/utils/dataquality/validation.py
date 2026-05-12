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
Built-in and custom validator factories for data-quality checks.

Each factory returns a ``Callable[[Any], bool]`` and can be passed as the
``validator`` argument of :class:`~airflow.providers.common.ai.utils.dataquality.models.DQCheckInput`
to force a specific validator for that check, bypassing LLM selection.

Custom validators registered with :func:`register_validator` are exposed to the LLM
via the :class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset` catalog
so the model can select them automatically.

Usage::

    from airflow.providers.common.ai.utils.dataquality import DQCheckInput
    from airflow.providers.common.ai.utils.dataquality import null_pct_check, row_count_check

    checks = [
        DQCheckInput(
            name="email_nulls",
            description="Check for null emails",
            validator=null_pct_check(max_pct=0.05),
        ),
        DQCheckInput(
            name="min_customers",
            description="Ensure at least 1000 rows",
            validator=row_count_check(min_count=1000),
        ),
    ]


    @register_validator(
        "freshness_check",
        llm_context=(
            "Compute hours since the most recent row. "
            "SQL pattern: EXTRACT(EPOCH FROM (NOW() - MAX(ts_col))) / 3600.0. "
            "Returns a DOUBLE representing hours elapsed."
        ),
        check_category="freshness",
    )
    def freshness_check(*, max_hours: float):
        def _check(value):
            return float(value) <= max_hours

        return _check
"""

from __future__ import annotations

import functools
import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidatorEntry:
    """
    Metadata for a registered validator factory.

    :param factory: Callable that returns a ``Callable[[Any], bool]`` validator.
    :param llm_context: Optional hint injected into the LLM system prompt so
        the model knows what SQL metric format this validator expects.
    :param check_category: Optional custom check category.  When set, the LLM
        is instructed to use this category for grouping.
    :param row_level: When ``True`` the LLM is instructed to generate a plain
        ``SELECT pk, col FROM table`` (no aggregation).  The planner fetches
        every row and applies the validator callable to each column value.
    """

    factory: Callable[..., Callable[[Any], bool]]
    llm_context: str = ""
    check_category: str = ""
    row_level: bool = False


class ValidatorRegistry:
    """
    Registry for reusable validator factories with optional LLM context.

    Validators registered here carry an ``llm_context`` string that
    :class:`~airflow.providers.common.ai.toolsets.dataquality.sql.SQLDQToolset` injects
    into the LLM prompt, guiding the model to produce SQL that returns the metric
    format the validator expects.

    A module-level :data:`default_registry` instance is available.  Use the
    convenience decorator :func:`register_validator` to register into it.
    """

    def __init__(self) -> None:
        self._entries: dict[str, ValidatorEntry] = {}

    def register(
        self,
        name: str,
        *,
        llm_context: str = "",
        check_category: str = "",
        row_level: bool = False,
    ) -> Callable[[Callable[..., Callable[[Any], bool]]], Callable[..., Callable[[Any], bool]]]:
        """
        Return a decorator that registers a validator factory under *name*.

        :param name: Unique name for this validator.
        :param llm_context: SQL generation hint injected into the LLM prompt.
        :param check_category: Custom check category for LLM grouping.
        :param row_level: When ``True``, the LLM generates a plain SELECT
            returning raw row values instead of an aggregate query.
        :raises ValueError: If *name* is already registered.
        """
        if name in self._entries:
            raise ValueError(
                f"Validator {name!r} is already registered. "
                "Use a different name or unregister the existing one first."
            )

        def _decorator(
            factory: Callable[..., Callable[[Any], bool]],
        ) -> Callable[..., Callable[[Any], bool]]:
            signature = inspect.signature(factory)

            @functools.wraps(factory)
            def _wrapped_factory(*args: Any, **kwargs: Any) -> Callable[[Any], bool]:
                closure = factory(*args, **kwargs)
                arg_parts = [repr(a) for a in args]
                kwarg_parts = [f"{k}={v!r}" for k, v in sorted(kwargs.items())]
                call_str = f"{name}({', '.join(arg_parts + kwarg_parts)})"
                if not hasattr(closure, "_validator_name"):
                    closure._validator_name = name  # type: ignore[attr-defined]
                if not hasattr(closure, "_row_level"):
                    closure._row_level = row_level  # type: ignore[attr-defined]
                if not hasattr(closure, "_validator_display"):
                    closure._validator_display = call_str  # type: ignore[attr-defined]

                # Persist effective factory arguments (including defaults) on the
                # returned closure so execution-time helpers can read validator
                # configuration consistently even when callers omit kwargs.
                try:
                    bound = signature.bind_partial(*args, **kwargs)
                    bound.apply_defaults()
                    effective_kwargs = {
                        name: value for name, value in bound.arguments.items() if name != "self"
                    }
                except TypeError:
                    effective_kwargs = dict(kwargs)

                for k, v in sorted(effective_kwargs.items()):
                    if not hasattr(closure, f"_{k}"):
                        setattr(closure, f"_{k}", v)
                return closure

            _wrapped_factory._validator_name = name  # type: ignore[attr-defined]
            _wrapped_factory._llm_context = llm_context  # type: ignore[attr-defined]
            _wrapped_factory._check_category = check_category  # type: ignore[attr-defined]
            _wrapped_factory._row_level = row_level  # type: ignore[attr-defined]
            _wrapped_factory._validator_display = name  # type: ignore[attr-defined]
            _wrapped_factory.__name__ = factory.__name__
            _wrapped_factory.__qualname__ = factory.__qualname__
            _wrapped_factory.__doc__ = factory.__doc__

            self._entries[name] = ValidatorEntry(
                factory=_wrapped_factory,
                llm_context=llm_context,
                check_category=check_category,
                row_level=row_level,
            )
            return _wrapped_factory

        return _decorator

    def get(self, name: str) -> ValidatorEntry:
        """
        Return the :class:`ValidatorEntry` for *name*.

        :raises KeyError: If *name* is not registered.
        """
        try:
            return self._entries[name]
        except KeyError:
            raise KeyError(
                f"Validator {name!r} is not registered. Available validators: {sorted(self._entries)}"
            ) from None

    def list_validators(self) -> list[str]:
        """Return sorted list of all registered validator names."""
        return sorted(self._entries)

    def unregister(self, name: str) -> None:
        """
        Remove a validator from the registry.

        :raises KeyError: If *name* is not registered.
        """
        try:
            del self._entries[name]
        except KeyError:
            raise KeyError(f"Validator {name!r} is not registered.") from None


default_registry = ValidatorRegistry()


def register_validator(
    name: str,
    *,
    llm_context: str = "",
    check_category: str = "",
    row_level: bool = False,
) -> Callable[[Callable[..., Callable[[Any], bool]]], Callable[..., Callable[[Any], bool]]]:
    """
    Register a validator factory in the :data:`default_registry`.

    Use as a decorator on a factory function::

        @register_validator(
            "freshness_check",
            llm_context="Compute hours since MAX(updated_at). Returns DOUBLE.",
            check_category="freshness",
        )
        def freshness_check(*, max_hours: float):
            def _check(value):
                return float(value) <= max_hours

            return _check

    :param name: Unique name for this validator.
    :param llm_context: SQL generation hint injected into the LLM prompt.
    :param check_category: Custom check category for LLM grouping.
    :param row_level: When ``True``, the LLM generates a plain SELECT returning
        raw column values.
    """
    return default_registry.register(
        name, llm_context=llm_context, check_category=check_category, row_level=row_level
    )


@register_validator(
    "null_pct_check",
    llm_context="Returns null percentage as a float between 0.0 and 1.0. SQL pattern: COUNT(CASE WHEN col IS NULL THEN 1 END) * 1.0 / COUNT(*).",
    check_category="null_check",
)
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

    return _check


@register_validator(
    "row_count_check",
    llm_context="Returns an integer row count. SQL pattern: COUNT(*).",
    check_category="row_count",
)
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

    return _check


@register_validator(
    "duplicate_pct_check",
    llm_context="Returns duplicate percentage as a float between 0.0 and 1.0. SQL pattern: (COUNT(*) - COUNT(DISTINCT col)) * 1.0 / COUNT(*).",
    check_category="uniqueness",
)
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

    return _check


@register_validator(
    "between_check",
    llm_context="Returns a numeric value that will be compared against inclusive bounds. SQL should return a single DOUBLE or INTEGER metric.",
    check_category="numeric_range",
)
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

    return _check


@register_validator(
    "exact_check",
    llm_context="Returns a value that must exactly equal an expected constant. SQL should return a single scalar metric.",
    check_category="validity",
)
def exact_check(*, expected: Any) -> Callable[[Any], bool]:
    """
    Return a validator that passes when ``value == expected``.

    :param expected: The exact value the metric must equal.
    """

    def _check(value: Any) -> bool:
        return value == expected

    return _check
