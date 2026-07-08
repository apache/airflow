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

from airflow.providers.dataquality.results import DQRun, RuleResult, build_summary, compute_score


def make_result(status: str, name: str = "r") -> RuleResult:
    return RuleResult(rule_uid=f"uid-{name}", rule_name=name, status=status)


class TestComputeScore:
    @pytest.mark.parametrize(
        ("statuses", "expected"),
        [
            ([], None),
            (["pass", "pass"], 1.0),
            (["pass", "fail"], 0.5),
            (["pass", "error"], 0.5),
            (["pass", "pass", "pass", "warn"], 0.9375),
            (["fail", "fail"], 0.0),
        ],
    )
    def test_score(self, statuses, expected):
        results = [make_result(status, name=f"r{i}") for i, status in enumerate(statuses)]
        assert compute_score(results) == expected


class TestBuildSummary:
    def test_summary_counts_and_rule_names(self):
        run = DQRun(dag_id="d", task_id="t", run_id="r", ruleset_name="orders", table_ref="orders")
        results = [
            make_result("pass", "a"),
            make_result("warn", "b"),
            make_result("fail", "c"),
            make_result("error", "d"),
        ]

        summary = build_summary(run, results)

        assert summary["passed"] == 1
        assert summary["warned"] == 1
        assert summary["failed"] == 1
        assert summary["errored"] == 1
        assert summary["failed_rules"] == ["c", "d"]
        assert summary["warned_rules"] == ["b"]
        assert summary["run_uid"] == run.run_uid


class TestSerialization:
    def test_rule_result_round_trip(self):
        result = RuleResult(
            rule_uid="u",
            rule_name="r",
            status="fail",
            observed_value=3,
            condition={"equal_to": 0},
            description="r should equal 0",
            duration_ms=1.5,
            sql="SELECT 3",
        )
        assert RuleResult.from_dict(result.to_dict()) == result

    def test_run_round_trip(self):
        run = DQRun(dag_id="d", task_id="t", run_id="r", asset_names=("orders",))
        assert DQRun.from_dict(run.to_dict()) == run
