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

from airflow.providers.common.ai.utils.dataquality.validation import (
    ValidatorRegistry,
    between_check,
    default_registry,
    duplicate_pct_check,
    exact_check,
    null_pct_check,
    register_validator,
    row_count_check,
)


class TestValidatorRegistry:
    def test_register_and_get(self):
        registry = ValidatorRegistry()

        @registry.register("my_check", llm_context="returns float")
        def my_check(*, threshold: float):
            def _check(value):
                return float(value) <= threshold

            return _check

        entry = registry.get("my_check")
        assert entry.llm_context == "returns float"

    def test_duplicate_registration_raises(self):
        registry = ValidatorRegistry()

        @registry.register("dup_check")
        def dup_check():
            return lambda v: True

        with pytest.raises(ValueError, match="already registered"):

            @registry.register("dup_check")
            def dup_check2():
                return lambda v: True

    def test_get_unknown_raises_key_error(self):
        registry = ValidatorRegistry()
        with pytest.raises(KeyError, match="not registered"):
            registry.get("nonexistent")

    def test_list_validators_returns_sorted(self):
        registry = ValidatorRegistry()

        @registry.register("b_check")
        def _b():
            return lambda v: True

        @registry.register("a_check")
        def _a():
            return lambda v: True

        assert registry.list_validators() == ["a_check", "b_check"]

    def test_unregister(self):
        registry = ValidatorRegistry()

        @registry.register("temp_check")
        def _temp():
            return lambda v: True

        registry.unregister("temp_check")
        assert "temp_check" not in registry.list_validators()

    def test_unregister_unknown_raises(self):
        registry = ValidatorRegistry()
        with pytest.raises(KeyError):
            registry.unregister("nonexistent")

    def test_closure_gets_validator_name_attribute(self):
        registry = ValidatorRegistry()

        @registry.register("named_check")
        def named_check(*, threshold: float):
            def _check(value):
                return float(value) <= threshold

            return _check

        fn = named_check(threshold=0.5)
        assert fn._validator_name == "named_check"

    def test_closure_gets_row_level_attribute(self):
        registry = ValidatorRegistry()

        @registry.register("row_check", row_level=True)
        def row_check():
            return lambda v: v is not None

        fn = row_check()
        assert fn._row_level is True


class TestDefaultRegistry:
    def test_default_registry_has_builtin_validators(self):
        validators = default_registry.list_validators()
        assert "null_pct_check" in validators
        assert "row_count_check" in validators
        assert "duplicate_pct_check" in validators
        assert "between_check" in validators
        assert "exact_check" in validators


class TestNullPctCheck:
    def test_passes_at_threshold(self):
        assert null_pct_check(max_pct=0.05)(0.05) is True

    def test_passes_below_threshold(self):
        assert null_pct_check(max_pct=0.05)(0.0) is True

    def test_fails_above_threshold(self):
        assert null_pct_check(max_pct=0.05)(0.06) is False

    def test_accepts_string_numeric(self):
        assert null_pct_check(max_pct=0.1)("0.05") is True

    def test_raises_for_non_numeric(self):
        with pytest.raises(TypeError, match="null_pct_check"):
            null_pct_check(max_pct=0.05)("not-a-number")

    def test_display_shows_threshold(self):
        fn = null_pct_check(max_pct=0.05)
        assert "0.05" in fn._validator_display


class TestRowCountCheck:
    def test_passes_at_min(self):
        assert row_count_check(min_count=100)(100) is True

    def test_passes_above_min(self):
        assert row_count_check(min_count=100)(200) is True

    def test_fails_below_min(self):
        assert row_count_check(min_count=100)(99) is False

    def test_raises_for_non_integer(self):
        with pytest.raises(TypeError, match="row_count_check"):
            row_count_check(min_count=100)("not-a-number")


class TestDuplicatePctCheck:
    def test_passes_at_threshold(self):
        assert duplicate_pct_check(max_pct=0.1)(0.1) is True

    def test_fails_above_threshold(self):
        assert duplicate_pct_check(max_pct=0.1)(0.11) is False

    def test_raises_for_non_numeric(self):
        with pytest.raises(TypeError, match="duplicate_pct_check"):
            duplicate_pct_check(max_pct=0.1)("bad")


class TestBetweenCheck:
    def test_passes_within_range(self):
        assert between_check(min_val=1.0, max_val=10.0)(5.0) is True

    def test_passes_at_lower_bound(self):
        assert between_check(min_val=1.0, max_val=10.0)(1.0) is True

    def test_passes_at_upper_bound(self):
        assert between_check(min_val=1.0, max_val=10.0)(10.0) is True

    def test_fails_below_range(self):
        assert between_check(min_val=1.0, max_val=10.0)(0.9) is False

    def test_fails_above_range(self):
        assert between_check(min_val=1.0, max_val=10.0)(10.1) is False

    def test_invalid_bounds_raises(self):
        with pytest.raises(ValueError, match="min_val"):
            between_check(min_val=10.0, max_val=1.0)

    def test_raises_for_non_numeric(self):
        with pytest.raises(TypeError, match="between_check"):
            between_check(min_val=1.0, max_val=10.0)("bad")


class TestExactCheck:
    def test_passes_when_equal(self):
        assert exact_check(expected=42)(42) is True

    def test_fails_when_not_equal(self):
        assert exact_check(expected=42)(43) is False

    def test_works_with_strings(self):
        assert exact_check(expected="ok")("ok") is True
        assert exact_check(expected="ok")("fail") is False


class TestRegisterValidatorDecorator:
    def test_registers_into_default_registry(self):
        # Use a unique name to avoid conflicts with other test runs
        name = "_test_register_validator_unique"
        if name in default_registry.list_validators():
            default_registry.unregister(name)

        @register_validator(name, llm_context="test context")
        def _test_check(*, threshold: float):
            return lambda v: float(v) <= threshold

        assert name in default_registry.list_validators()
        entry = default_registry.get(name)
        assert entry.llm_context == "test context"

        # Clean up
        default_registry.unregister(name)
