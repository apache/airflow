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

import pytest
from pydantic_ai.usage import UsageLimits

from airflow.providers.common.ai.utils.usage import resolve_usage_limits


class TestResolveUsageLimitsNoMaxCost:
    def test_max_cost_none_returns_usage_limits_unchanged(self):
        """max_cost=None must be a true no-op: the exact same object comes back."""
        limits = UsageLimits(request_limit=3)
        assert resolve_usage_limits(limits, None) is limits

    def test_max_cost_none_with_usage_limits_none_returns_none(self):
        assert resolve_usage_limits(None, None) is None


class TestResolveUsageLimitsWithMaxCost:
    def test_max_cost_alone_builds_usage_limits(self):
        result = resolve_usage_limits(None, 0.5)
        assert isinstance(result, UsageLimits)
        assert result.cost_limit == Decimal("0.5")

    def test_max_cost_overrides_existing_cost_limit(self):
        limits = UsageLimits(cost_limit=Decimal("999"), request_limit=7)
        result = resolve_usage_limits(limits, 0.5)
        assert result is not limits
        assert result.cost_limit == Decimal("0.5")
        assert result.request_limit == 7

    def test_max_cost_preserves_other_fields(self):
        limits = UsageLimits(request_limit=3, input_tokens_limit=4_000, tool_calls_limit=2)
        result = resolve_usage_limits(limits, 1.25)
        assert result.request_limit == 3
        assert result.input_tokens_limit == 4_000
        assert result.tool_calls_limit == 2
        assert result.cost_limit == Decimal("1.25")

    @pytest.mark.parametrize(
        ("max_cost", "expected"),
        [
            (0.1, Decimal("0.1")),
            ("0.1", Decimal("0.1")),
            (1, Decimal("1")),
            ("2.50", Decimal("2.50")),
            (Decimal("2.50"), Decimal("2.50")),
        ],
    )
    def test_max_cost_accepts_float_str_and_decimal(self, max_cost, expected):
        result = resolve_usage_limits(None, max_cost)
        assert result.cost_limit == expected

    def test_max_cost_uses_decimal_str_not_decimal_float(self):
        """Decimal(str(x)) must be used, not Decimal(x) -- the latter bakes in
        binary-float noise for a value like 0.1."""
        result = resolve_usage_limits(None, 0.1)
        assert result.cost_limit == Decimal("0.1")
        assert result.cost_limit != Decimal(0.1)

    def test_max_cost_zero_is_accepted(self):
        """0 is a valid (if unusual) cap and must not be rejected as falsy or negative."""
        result = resolve_usage_limits(None, 0)
        assert result.cost_limit == Decimal("0")


class TestResolveUsageLimitsInvalidMaxCost:
    @pytest.mark.parametrize("max_cost", ["", "n/a", "$0.50"])
    def test_non_numeric_max_cost_raises_value_error_naming_the_value(self, max_cost):
        """The error must name ``max_cost`` and the offending value -- a bare
        ``decimal.InvalidOperation`` traceback gives a Dag author no clue which
        parameter (often a mistyped or unset Airflow Variable) broke."""
        with pytest.raises(ValueError, match="max_cost") as exc_info:
            resolve_usage_limits(None, max_cost)
        assert repr(max_cost) in str(exc_info.value)

    def test_negative_max_cost_raises_value_error(self):
        with pytest.raises(ValueError, match="max_cost must not be negative"):
            resolve_usage_limits(None, -1)

    @pytest.mark.parametrize(
        "max_cost",
        ["inf", "-inf", "nan", "Infinity", float("inf"), float("nan")],
    )
    def test_non_finite_max_cost_raises_value_error_naming_the_value(self, max_cost):
        """A non-finite cost_limit would compare as never-exceeded, silently
        disabling the cap the Dag author thinks they configured."""
        with pytest.raises(ValueError, match="max_cost") as exc_info:
            resolve_usage_limits(None, max_cost)
        assert repr(max_cost) in str(exc_info.value)
