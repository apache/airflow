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

import pytest

from airflow.providers.common.ai.utils.dq_validation import (
    between_check,
    duplicate_pct_check,
    exact_check,
    null_pct_check,
    row_count_check,
)


class TestNullPctCheck:
    def test_passes_when_value_at_threshold(self):
        assert null_pct_check(max_pct=0.05)(0.05) is True

    def test_passes_when_value_below_threshold(self):
        assert null_pct_check(max_pct=0.05)(0.01) is True

    def test_fails_when_value_above_threshold(self):
        assert null_pct_check(max_pct=0.05)(0.06) is False

    def test_accepts_integer_value(self):
        assert null_pct_check(max_pct=1)(0) is True

    def test_accepts_string_numeric_value(self):
        assert null_pct_check(max_pct=0.1)("0.05") is True

    def test_raises_type_error_for_non_numeric(self):
        with pytest.raises(TypeError, match="null_pct_check"):
            null_pct_check(max_pct=0.05)("not-a-number")

    def test_repr_contains_threshold(self):
        fn = null_pct_check(max_pct=0.05)
        assert "0.05" in repr(fn)
        assert "null_pct_check" in repr(fn)


class TestRowCountCheck:
    def test_passes_when_value_equals_min(self):
        assert row_count_check(min_count=1000)(1000) is True

    def test_passes_when_value_above_min(self):
        assert row_count_check(min_count=1000)(9999) is True

    def test_fails_when_value_below_min(self):
        assert row_count_check(min_count=1000)(999) is False

    def test_accepts_string_numeric(self):
        assert row_count_check(min_count=5)("10") is True

    def test_raises_type_error_for_non_numeric(self):
        with pytest.raises(TypeError, match="row_count_check"):
            row_count_check(min_count=100)(None)

    def test_repr_contains_threshold(self):
        fn = row_count_check(min_count=500)
        assert "500" in repr(fn)
        assert "row_count_check" in repr(fn)


class TestDuplicatePctCheck:
    def test_passes_when_value_at_threshold(self):
        assert duplicate_pct_check(max_pct=0.01)(0.01) is True

    def test_passes_when_value_below_threshold(self):
        assert duplicate_pct_check(max_pct=0.01)(0.005) is True

    def test_fails_when_value_above_threshold(self):
        assert duplicate_pct_check(max_pct=0.01)(0.02) is False

    def test_raises_type_error_for_non_numeric(self):
        with pytest.raises(TypeError, match="duplicate_pct_check"):
            duplicate_pct_check(max_pct=0.01)(object())

    def test_repr_contains_threshold(self):
        fn = duplicate_pct_check(max_pct=0.02)
        assert "0.02" in repr(fn)


class TestBetweenCheck:
    def test_passes_on_lower_boundary(self):
        assert between_check(min_val=0.0, max_val=100.0)(0.0) is True

    def test_passes_on_upper_boundary(self):
        assert between_check(min_val=0.0, max_val=100.0)(100.0) is True

    def test_passes_in_range(self):
        assert between_check(min_val=0.0, max_val=100.0)(50.0) is True

    def test_fails_below_range(self):
        assert between_check(min_val=10.0, max_val=100.0)(9.9) is False

    def test_fails_above_range(self):
        assert between_check(min_val=0.0, max_val=100.0)(100.1) is False

    def test_raises_value_error_when_min_greater_than_max(self):
        with pytest.raises(ValueError, match="min_val"):
            between_check(min_val=100.0, max_val=0.0)

    def test_raises_type_error_for_non_numeric(self):
        with pytest.raises(TypeError, match="between_check"):
            between_check(min_val=0.0, max_val=10.0)("abc")

    def test_repr_contains_bounds(self):
        fn = between_check(min_val=1.0, max_val=9.0)
        assert "1.0" in repr(fn)
        assert "9.0" in repr(fn)


class TestExactCheck:
    def test_passes_for_equal_value(self):
        assert exact_check(expected=0)(0) is True

    def test_fails_for_unequal_value(self):
        assert exact_check(expected=0)(1) is False

    def test_works_with_string(self):
        assert exact_check(expected="active")("active") is True

    def test_works_with_none(self):
        assert exact_check(expected=None)(None) is True

    def test_repr_contains_expected(self):
        fn = exact_check(expected=42)
        assert "42" in repr(fn)
        assert "exact_check" in repr(fn)
