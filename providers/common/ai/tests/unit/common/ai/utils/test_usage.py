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

from decimal import Decimal
from typing import Literal

import pytest
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.utils.usage import (
    _COERCERS,
    _FIELD_TYPES,
    _resolve_field_type,
    coerce_usage_limits,
)


class TestCoerceUsageLimitsIdentity:
    def test_none_returns_none(self):
        assert coerce_usage_limits(None) is None

    def test_usage_limits_instance_returned_unchanged(self):
        """An author-built UsageLimits is returned by identity -- its field
        values are never inspected or copied."""
        limits = UsageLimits(request_limit=3)
        assert coerce_usage_limits(limits) is limits


class TestCoerceUsageLimitsNativeDict:
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("cost_limit", Decimal("0.5")),
            ("request_limit", 5),
            ("count_tokens_before_request", True),
        ],
    )
    def test_native_value_matches_direct_construction(self, field, value):
        result = coerce_usage_limits({field: value})
        assert result == UsageLimits(**{field: value})


class TestCoerceUsageLimitsTemplatedDict:
    def test_cost_limit_string_coerced_to_decimal(self):
        result = coerce_usage_limits({"cost_limit": "0.5"})
        assert result.cost_limit == Decimal("0.5")

    @pytest.mark.parametrize("value", ["0.1", 0.1], ids=["templated-str", "native-float"])
    def test_cost_limit_uses_decimal_str_not_decimal_float(self, value):
        """Decimal(str(x)) semantics apply whether the value is templated (a str)
        or written literally (a native float): neither must pick up the
        binary-float noise that Decimal(0.1) would."""
        result = coerce_usage_limits({"cost_limit": value})
        assert result.cost_limit == Decimal("0.1")
        assert result.cost_limit != Decimal(0.1)

    def test_int_field_string_coerced_to_int(self):
        result = coerce_usage_limits({"request_limit": "5"})
        assert result.request_limit == 5
        assert isinstance(result.request_limit, int)

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("true", True),
            ("Yes", True),
            ("0", False),
            ("off", False),
        ],
    )
    def test_bool_field_string_coerced(self, value, expected):
        result = coerce_usage_limits({"count_tokens_before_request": value})
        assert result.count_tokens_before_request is expected

    def test_explicit_none_disables_the_limit(self):
        """None is the author's deliberate choice -- Jinja never produces None
        from a string template -- so it must pass through untouched."""
        result = coerce_usage_limits({"request_limit": None})
        assert result.request_limit is None


class TestCoerceUsageLimitsInvalidValues:
    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("cost_limit", ""),
            ("cost_limit", "n/a"),
            ("cost_limit", "$0.50"),
            ("request_limit", "abc"),
        ],
    )
    def test_unparsable_value_raises_naming_field_and_value(self, field, value):
        """The error must name the field and the offending value -- a bare
        ``decimal.InvalidOperation``/``ValueError`` traceback gives a Dag author
        no clue which key (often a mistyped or unset Airflow Variable) broke.
        Covers both the ``Decimal`` (``cost_limit``) and ``int`` (``request_limit``)
        coercion error paths."""
        with pytest.raises(ValueError, match=r"usage_limits\[") as exc_info:
            coerce_usage_limits({field: value})
        message = str(exc_info.value)
        assert f"usage_limits[{field!r}]" in message
        assert repr(value) in message

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("cost_limit", "inf"),
            ("cost_limit", "-inf"),
            ("cost_limit", "nan"),
            ("cost_limit", "Infinity"),
            ("cost_limit", float("inf")),
            ("cost_limit", float("-inf")),
            ("cost_limit", float("nan")),
            ("cost_limit", 1e400),
            ("request_limit", float("inf")),
            ("request_limit", float("nan")),
            ("total_tokens_limit", 1e400),
            ("tool_calls_limit", float("-inf")),
        ],
        ids=[
            "cost_limit-inf-str",
            "cost_limit--inf-str",
            "cost_limit-nan-str",
            "cost_limit-Infinity-str",
            "cost_limit-inf-float",
            "cost_limit--inf-float",
            "cost_limit-nan-float",
            "cost_limit-1e400-float",
            "request_limit-inf-float",
            "request_limit-nan-float",
            "total_tokens_limit-1e400-float",
            "tool_calls_limit--inf-float",
        ],
    )
    def test_non_finite_value_raises_for_any_numeric_field(self, field, value):
        """A non-finite value would compare as never-exceeded, silently disabling
        the cap the Dag author thinks they configured -- for every numeric field,
        not just ``cost_limit``. The finite check in ``_validate_range`` applies to
        every numeric field, independent of the ``Decimal`` string-normalization
        ``_coerce_value`` does for ``cost_limit`` specifically.
        ``tool_calls_limit=-inf`` is included because ``-inf < 0`` is also true, so
        the finite check must run (and raise) *before* the negative check, or this
        case would be caught with a misleading "must not be negative" message."""
        with pytest.raises(ValueError, match="finite"):
            coerce_usage_limits({field: value})

    def test_signaling_nan_raises_naming_the_field(self):
        """``Decimal('sNaN')`` traps on unguarded comparison (``InvalidOperation``,
        not ``ValueError``) and ``math.isfinite`` raises outright on it rather than
        returning ``False`` -- the finite check must use ``Decimal.is_finite()`` for
        ``Decimal`` values so this surfaces as a named ``ValueError`` like every
        other bad value, not an unhandled crash. Covers both the native ``Decimal``
        and the templated-string spelling."""
        for value in (Decimal("snan"), "snan"):
            with pytest.raises(ValueError, match="finite"):
                coerce_usage_limits({"cost_limit": value})

    def test_huge_but_finite_value_is_accepted(self):
        """A value too large for ``float`` is still finite -- ``Decimal`` has no
        float-sized exponent limit, and neither does Python's arbitrary-precision
        ``int``. ``math.isfinite`` would overflow converting either to a ``float``;
        the finite check must not go through ``float`` for these types."""
        huge = "1" + "0" * 400
        result = coerce_usage_limits({"total_tokens_limit": huge})
        assert result.total_tokens_limit == int(huge)

    @pytest.mark.parametrize(
        ("field", "value"),
        [
            ("cost_limit", "-1"),
            ("cost_limit", -1),
            ("cost_limit", -1.5),
            ("request_limit", "-1"),
            ("request_limit", -1),
        ],
    )
    def test_negative_value_raises_for_field(self, field, value):
        with pytest.raises(ValueError, match="must not be negative"):
            coerce_usage_limits({field: value})

    def test_zero_cost_limit_is_accepted(self):
        """0 is a valid (if unusual) cap and must not be rejected as falsy or negative."""
        result = coerce_usage_limits({"cost_limit": "0"})
        assert result.cost_limit == Decimal("0")

    def test_unknown_key_raises_naming_key_and_valid_fields(self):
        with pytest.raises(ValueError, match="cost_limitt") as exc_info:
            coerce_usage_limits({"cost_limitt": "1"})
        message = str(exc_info.value)
        assert "cost_limit" in message
        assert "request_limit" in message

    def test_unrecognized_bool_string_raises(self):
        with pytest.raises(ValueError, match="count_tokens_before_request"):
            coerce_usage_limits({"count_tokens_before_request": "maybe"})


class TestCoercersCompleteness:
    def test_every_field_type_has_a_coercer(self):
        """
        Guards against pydantic-ai adding a UsageLimits field whose type this
        module doesn't know how to coerce from a templated string.

        If this goes red after a pydantic-ai upgrade, it means a new field's
        type has no entry in ``_COERCERS`` -- add a ``coerce_*`` function for
        that type in ``airflow.providers.common.ai.utils.usage``.
        """
        missing = {
            field: field_type for field, field_type in _FIELD_TYPES.items() if field_type not in _COERCERS
        }
        assert not missing, (
            f"No coercer registered for {missing}; add a coerce_* function for that type "
            "in airflow.providers.common.ai.utils.usage._COERCERS"
        )


class TestResolveFieldType:
    """``_resolve_field_type`` must raise on any annotation shape it cannot
    unambiguously resolve, rather than silently picking a member -- see
    ``coerce_usage_limits``'s "loud, not silent" drift-detection design."""

    def test_ambiguous_union_raises(self):
        """A Union of two real types (not ``X | None``) has no single coercion
        target; silently picking the first member would hide the ambiguity."""
        with pytest.raises(TypeError, match="unsupported annotation"):
            _resolve_field_type("some_field", int | str | None)

    def test_parameterized_generic_raises(self):
        with pytest.raises(TypeError, match="unsupported annotation"):
            _resolve_field_type("some_field", dict[str, int])

    def test_non_type_resolution_raises(self):
        """A single-member ``Literal`` resolves to a value, not a type."""
        with pytest.raises(TypeError, match="resolved to a non-type"):
            _resolve_field_type("some_field", Literal["a"])
