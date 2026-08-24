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
"""Coerce a templated ``usage_limits`` dict into a real ``UsageLimits`` instance."""

from __future__ import annotations

import dataclasses
import math
import typing
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Any

from pydantic_ai.usage import UsageLimits


def _resolve_field_type(field: str, hint: Any) -> type:
    # Only ``X`` or ``X | None`` are supported shapes -- anything else (a Union of
    # two real types, a parameterized generic, a Literal, ...) has no single
    # unambiguous coercion target, so it must raise here rather than silently
    # picking one member and hiding the ambiguity behind a tripwire that never fires.
    args = [arg for arg in typing.get_args(hint) if arg is not type(None)]
    if not args:
        resolved = hint
    elif len(args) == 1:
        resolved = args[0]
    else:
        raise TypeError(f"UsageLimits.{field} has an unsupported annotation {hint!r}")
    if not isinstance(resolved, type):
        raise TypeError(f"UsageLimits.{field} resolved to a non-type {resolved!r}")
    return resolved


def _build_field_types() -> dict[str, type]:
    # ``pydantic_ai.usage`` uses ``from __future__ import annotations``, so
    # ``field.type`` is a string; ``get_type_hints`` resolves the real objects.
    hints = typing.get_type_hints(UsageLimits)
    return {
        field.name: _resolve_field_type(field.name, hints[field.name])
        for field in dataclasses.fields(UsageLimits)
    }


def _coerce_decimal(field: str, value: str) -> Decimal:
    try:
        parsed = Decimal(value)
    except InvalidOperation:
        raise ValueError(
            f"usage_limits[{field!r}] must be a number (got {value!r}); "
            "if it is templated, check the rendered value."
        ) from None
    return parsed


def _coerce_int(field: str, value: str) -> int:
    try:
        return int(value)
    except ValueError:
        raise ValueError(
            f"usage_limits[{field!r}] must be an integer (got {value!r}); "
            "if it is templated, check the rendered value."
        ) from None


# Deliberately the same vocabulary as ``airflow.utils.strings.TRUE_LIKE_VALUES`` so a
# Dag author who knows Airflow's config parsing already knows this one. Unlike
# ``to_boolean``, an unrecognized string raises instead of silently becoming ``False`` --
# this flag gates a pre-flight token-limit check, and silently turning it off would
# defeat the safeguard this PR exists to add.
_TRUE_LIKE = {"on", "t", "true", "y", "yes", "1"}
_FALSE_LIKE = {"off", "f", "false", "n", "no", "0"}


def _coerce_bool(field: str, value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in _TRUE_LIKE:
        return True
    if normalized in _FALSE_LIKE:
        return False
    raise ValueError(
        f"usage_limits[{field!r}] must be one of {sorted(_TRUE_LIKE | _FALSE_LIKE)} "
        f"(got {value!r}); if it is templated, check the rendered value."
    )


_FIELD_TYPES: dict[str, type] = _build_field_types()

# Keyed by the field's declared type rather than the field name so a new
# ``UsageLimits`` field of an already-supported type (another ``int`` cap, say)
# needs no change here. A field of an unsupported type raises loudly (see
# ``_coerce_value``) instead of the templated string silently reaching the
# dataclass unconverted and failing deep inside pydantic-ai instead.
_COERCERS: dict[type, Callable[[str, str], Any]] = {
    Decimal: _coerce_decimal,
    int: _coerce_int,
    bool: _coerce_bool,
}


def _is_finite(value: Decimal | int | float) -> bool:
    # Dispatch by type instead of calling math.isfinite directly on everything:
    # math.isfinite converts its argument to float first, which overflows a large
    # int into OverflowError and raises outright on a Decimal signaling NaN --
    # neither looks like "not finite", they look like an unhandled crash. Decimal
    # has no float-sized exponent limit either, so a huge-but-finite Decimal must
    # not be misreported as non-finite just because float can't represent it.
    if isinstance(value, Decimal):
        return value.is_finite()
    if isinstance(value, int):
        return True
    return math.isfinite(value)


def _validate_range(field: str, value: Decimal | int | float) -> None:
    if not _is_finite(value):
        raise ValueError(
            f"usage_limits[{field!r}] must be a finite number (got {value!r}); "
            "a non-finite value would silently disable that limit."
        )
    if value < 0:
        raise ValueError(f"usage_limits[{field!r}] must not be negative (got {value!r})")


def _unknown_field_message(field: str) -> str:
    valid_fields = ", ".join(sorted(_FIELD_TYPES))
    return f"usage_limits has no field {field!r}; valid fields are: {valid_fields}"


def _truncated_repr(value: Any, limit: int = 100) -> str:
    # A container-shape error on a templated field usually means the render
    # produced something that merely looks right (e.g. a long string that reads
    # like a dict literal) -- the author needs to see what actually came out, not
    # just its type. Truncate so a large rendered blob doesn't bloat the exception.
    text = repr(value)
    return text if len(text) <= limit else f"{text[:limit]}..."


def _coerce_value(field: str, value: Any) -> Any:
    # ``None`` is always the author's explicit choice to disable that limit --
    # Jinja never produces ``None`` from a string template -- so it passes through.
    if value is None:
        return value

    field_type = _FIELD_TYPES[field]
    # Only ``str`` values are converted: Jinja only ever renders a scalar leaf to
    # ``str``, so a non-``str`` value is exactly what the author wrote (a literal
    # ``Decimal``, ``int``, or ``bool``) and is passed through unchanged.
    if isinstance(value, str):
        coercer = _COERCERS.get(field_type)
        if coercer is None:
            type_name = getattr(field_type, "__name__", field_type)
            raise ValueError(
                f"usage_limits[{field!r}] does not support templated (string) values "
                f"(got {value!r}); pass a {type_name} value instead."
            )
        value = coercer(field, value)
    elif field_type is Decimal and isinstance(value, (int, float)):
        # Decimal(str(x)), never Decimal(x): the latter bakes in binary-float noise
        # for a value like 0.1, and normalizing through str routes inf/nan through
        # the same finite check below as the templated-string path -- a bare
        # ``float`` must not bypass the one safety promise this module makes.
        value = _coerce_decimal(field, str(value))

    if field_type in (Decimal, int):
        _validate_range(field, value)
    return value


def coerce_usage_limits(usage_limits: UsageLimits | dict[str, Any] | None) -> UsageLimits | None:
    """
    Coerce a rendered ``usage_limits`` dict into a real ``UsageLimits`` instance.

    A ``UsageLimits`` instance has neither ``resolve`` nor ``template_fields``, so
    Airflow's template walk is a no-op on it even though ``usage_limits`` is in the
    operators' ``template_fields``. Passing a plain dict instead lets every field be
    templated, but the rendered value is then not in the Dag author's control: an
    unset Airflow Variable renders to ``""``, and a typo renders to an arbitrary
    non-numeric string. This function performs the defensive, per-field parsing
    that keeps those failures loud and specific instead of a ``TypeError`` raised
    deep inside pydantic-ai.

    - ``usage_limits is None``: returned unchanged.
    - ``usage_limits`` is already a ``UsageLimits`` instance: returned unchanged, by
      identity -- an author who built the object themselves owns its field values.
    - ``usage_limits`` is a dict: each value is coerced per its ``UsageLimits`` field
      type (see ``_coerce_value``) and the result is passed to ``UsageLimits(**...)``.

    The container shape can't be checked any earlier than this, at the operator's
    ``__init__``: ``usage_limits`` is a template field, so at ``__init__`` time it may
    still be a Jinja string that has not been rendered yet (the whole field written
    as a single expression), and this function is the first point that only ever sees
    the rendered value.

    :raises TypeError: if ``usage_limits`` is not a ``UsageLimits``, a ``dict``, or
        ``None`` -- a container-shape problem, checked first.
    :raises ValueError: if the dict has an unknown key, or a value cannot be coerced
        to its field's type, is not finite, or is negative -- a value problem, checked
        per field.
    """
    if usage_limits is None or isinstance(usage_limits, UsageLimits):
        return usage_limits
    if not isinstance(usage_limits, dict):
        raise TypeError(
            f"usage_limits must be a UsageLimits, a dict, or None (got "
            f"{type(usage_limits).__name__}: {_truncated_repr(usage_limits)}); "
            "if it is templated, check the rendered value."
        )

    coerced: dict[str, Any] = {}
    for field, value in usage_limits.items():
        if field not in _FIELD_TYPES:
            raise ValueError(_unknown_field_message(field))
        coerced[field] = _coerce_value(field, value)
    return UsageLimits(**coerced)
