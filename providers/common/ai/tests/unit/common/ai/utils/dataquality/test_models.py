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
    DQCheckPlan,
    DQCheckResult,
    DQPlan,
    DQReport,
    RowLevelResult,
)


class TestDQCheckInput:
    def test_valid_construction(self):
        c = DQCheckInput(name="null_ids", description="Check for null IDs")
        assert c.name == "null_ids"
        assert c.description == "Check for null IDs"
        assert c.validator is None

    def test_with_validator(self):
        fn = lambda v: True
        c = DQCheckInput(name="c", description="d", validator=fn)
        assert c.validator is fn

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

    def test_repr_shows_name_and_validator_presence(self):
        c = DQCheckInput(name="n", description="d", validator=lambda v: True)
        r = repr(c)
        assert "n" in r
        assert "True" in r


class TestRowLevelResult:
    def test_build_all_valid(self):
        result = RowLevelResult.build([], total=10)
        assert result.invalid == 0
        assert result.invalid_pct == 0.0
        assert result.passed is True if hasattr(result, "passed") else True

    def test_build_some_invalid(self):
        result = RowLevelResult.build(["bad1", "bad2"], total=10)
        assert result.invalid == 2
        assert result.invalid_pct == pytest.approx(0.2)
        assert result.sample_violations == ["'bad1'", "'bad2'"]

    def test_build_respects_sample_limit(self):
        invalids = list(range(30))
        result = RowLevelResult.build(invalids, total=30, sample_limit=5)
        assert result.sample_size == 5
        assert len(result.sample_violations) == 5

    def test_build_zero_total_gives_zero_pct(self):
        result = RowLevelResult.build([], total=0)
        assert result.invalid_pct == 0.0


class TestDQCheckResult:
    def test_default_fields(self):
        r = DQCheckResult(check_name="c", passed=True)
        assert r.value is None
        assert r.failure_reason is None
        assert r.metric_key is None
        assert r.sql_query is None

    def test_failed_result(self):
        r = DQCheckResult(check_name="c", passed=False, failure_reason="too many nulls")
        assert r.passed is False
        assert r.failure_reason == "too many nulls"


class TestDQReport:
    def test_build_all_pass(self):
        results = [
            DQCheckResult(check_name="c1", passed=True),
            DQCheckResult(check_name="c2", passed=True),
        ]
        report = DQReport.build(results)
        assert report.passed is True
        assert report.failure_summary == ""

    def test_build_with_failure(self):
        results = [
            DQCheckResult(check_name="c1", passed=True),
            DQCheckResult(check_name="c2", passed=False, failure_reason="exceeds threshold"),
        ]
        report = DQReport.build(results)
        assert report.passed is False
        assert "c2" in report.failure_summary
        assert "exceeds threshold" in report.failure_summary

    def test_build_multiple_failures(self):
        results = [
            DQCheckResult(check_name="c1", passed=False),
            DQCheckResult(check_name="c2", passed=False),
        ]
        report = DQReport.build(results)
        assert "2/2" in report.failure_summary

    def test_empty_results_passes(self):
        report = DQReport.build([])
        assert report.passed is True


class TestDQCheckPlan:
    def test_construction(self):
        plan = DQCheckPlan(
            check_name="c",
            sql_query="SELECT COUNT(*) FROM t",
            metric_key="count",
            validator_name="row_count_check",
            validator_args={"min_count": 100},
        )
        assert plan.check_name == "c"
        assert plan.row_level is False

    def test_row_level_flag(self):
        plan = DQCheckPlan(
            check_name="c",
            sql_query="SELECT email FROM t",
            metric_key="email",
            validator_name="email_format_check",
            validator_args={},
            row_level=True,
        )
        assert plan.row_level is True


class TestDQPlan:
    def test_empty_plan(self):
        plan = DQPlan()
        assert plan.checks == []

    def test_plan_with_checks(self):
        check_plan = DQCheckPlan(
            check_name="c",
            sql_query="SELECT 1",
            metric_key="k",
            validator_name="none",
            validator_args={},
        )
        plan = DQPlan(checks=[check_plan])
        assert len(plan.checks) == 1


class TestDQCheckFailedError:
    def test_is_exception(self):
        err = DQCheckFailedError("checks failed")
        assert isinstance(err, Exception)
        assert "checks failed" in str(err)
