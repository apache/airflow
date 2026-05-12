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

from airflow.providers.common.ai.utils.dataquality.models import (
    DQCheckFailedError,
    DQCheckInput,
    DQCheckResult,
    DQReport,
    RowLevelResult,
)


class TestDQCheckInput:
    def test_valid_construction(self):
        c = DQCheckInput(name="null_emails", description="Check nulls")
        assert c.name == "null_emails"
        assert c.description == "Check nulls"
        assert c.validator is None

    def test_empty_name_raises(self):
        with pytest.raises(ValueError, match="name must not be empty"):
            DQCheckInput(name="", description="d")

    def test_whitespace_name_raises(self):
        with pytest.raises(ValueError, match="name must not be empty"):
            DQCheckInput(name="   ", description="d")

    def test_empty_description_raises(self):
        with pytest.raises(ValueError, match="description must not be empty"):
            DQCheckInput(name="n", description="")

    def test_coerce_from_dict(self):
        c = DQCheckInput.coerce({"name": "n", "description": "d"})
        assert isinstance(c, DQCheckInput)
        assert c.name == "n"

    def test_coerce_passthrough(self):
        orig = DQCheckInput(name="x", description="y")
        assert DQCheckInput.coerce(orig) is orig

    def test_coerce_invalid_type_raises(self):
        with pytest.raises(TypeError):
            DQCheckInput.coerce(42)

    def test_validator_stored(self):
        fn = lambda v: v == 0
        c = DQCheckInput(name="n", description="d", validator=fn)
        assert c.validator is fn

    def test_repr_no_validator(self):
        c = DQCheckInput(name="n", description="d")
        assert "validator=False" in repr(c)

    def test_repr_with_validator(self):
        c = DQCheckInput(name="n", description="d", validator=lambda v: True)
        assert "validator=True" in repr(c)


class TestDQCheckResult:
    def test_passed_result(self):
        r = DQCheckResult(check_name="null_emails", passed=True, value=0)
        assert r.passed is True
        assert r.failure_reason is None

    def test_failed_result_with_reason(self):
        r = DQCheckResult(check_name="dup_ids", passed=False, failure_reason="too many dupes", value=5)
        assert r.passed is False
        assert r.failure_reason == "too many dupes"

    def test_value_defaults_to_none(self):
        r = DQCheckResult(check_name="c", passed=True)
        assert r.value is None

    def test_serialises_to_dict(self):
        r = DQCheckResult(check_name="c", passed=True, value=42)
        d = r.model_dump()
        assert d["check_name"] == "c"
        assert d["passed"] is True
        assert d["value"] == 42


class TestRowLevelResult:
    def test_build_with_invalid_rows(self):
        result = RowLevelResult.build(invalid_values=["bad-a", "bad-b"], total=5, sample_limit=10)
        assert result.total == 5
        assert result.invalid == 2
        assert result.invalid_pct == pytest.approx(0.4)
        assert result.sample_violations == ["'bad-a'", "'bad-b'"]
        assert result.sample_size == 2

    def test_build_with_zero_total(self):
        result = RowLevelResult.build(invalid_values=[], total=0)
        assert result.invalid_pct == 0.0
        assert result.sample_size == 0

    def test_build_truncates_samples(self):
        result = RowLevelResult.build(invalid_values=[1, 2, 3], total=3, sample_limit=2)
        assert result.sample_violations == ["1", "2"]
        assert result.sample_size == 2


class TestDQReport:
    def test_build_all_pass(self):
        results = [
            DQCheckResult(check_name="null_emails", passed=True, value=0),
            DQCheckResult(check_name="dup_ids", passed=True, value=0),
        ]
        report = DQReport.build(results)
        assert report.passed is True
        assert report.failure_summary == ""
        assert report.results == results

    def test_build_one_failure(self):
        results = [
            DQCheckResult(check_name="null_emails", passed=False, failure_reason="nulls found", value=1),
            DQCheckResult(check_name="dup_ids", passed=True, value=0),
        ]
        report = DQReport.build(results)
        assert report.passed is False
        assert "Data quality checks failed (1/2):" in report.failure_summary
        assert "null_emails" in report.failure_summary
        assert "nulls found" in report.failure_summary

    def test_build_multiple_failures(self):
        results = [
            DQCheckResult(check_name="a", passed=False, value=10),
            DQCheckResult(check_name="b", passed=False, value=20),
        ]
        report = DQReport.build(results)
        assert report.passed is False
        assert "2/2" in report.failure_summary
        assert "a" in report.failure_summary
        assert "b" in report.failure_summary

    def test_build_no_failure_reason_uses_default(self):
        results = [DQCheckResult(check_name="c", passed=False, failure_reason=None)]
        report = DQReport.build(results)
        assert "validator returned False" in report.failure_summary

    def test_serialises_to_dict(self):
        results = [DQCheckResult(check_name="c", passed=True)]
        report = DQReport.build(results)
        d = report.model_dump()
        assert d["passed"] is True
        assert isinstance(d["results"], list)


class TestDQCheckFailedError:
    def test_is_airflow_exception(self):
        from airflow.providers.common.compat.sdk import AirflowException

        err = DQCheckFailedError("something failed")
        assert isinstance(err, AirflowException)

    def test_message_preserved(self):
        err = DQCheckFailedError("null_emails: 5 nulls")
        assert "null_emails" in str(err)
