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

from airflow.providers.common.ai.utils.dq_models import DQCheck, DQCheckGroup, DQCheckResult, DQPlan, DQReport


def _build_plan() -> DQPlan:
    return DQPlan(
        groups=[
            DQCheckGroup(
                group_id="nulls",
                query="SELECT COUNT(*) AS null_email_count FROM customers WHERE email IS NULL",
                checks=[
                    DQCheck(
                        check_name="null_emails",
                        metric_key="null_email_count",
                        group_id="nulls",
                    )
                ],
            ),
            DQCheckGroup(
                group_id="dupes",
                query=(
                    "SELECT COUNT(*) AS dup_id_count FROM ("
                    "SELECT id FROM customers GROUP BY id HAVING COUNT(*) > 1) sub"
                ),
                checks=[
                    DQCheck(
                        check_name="dup_ids",
                        metric_key="dup_id_count",
                        group_id="dupes",
                    )
                ],
            ),
        ]
    )


class TestDQPlan:
    def test_check_names_flattens_all_group_checks(self):
        plan = _build_plan()

        assert plan.check_names == ["null_emails", "dup_ids"]

    def test_plan_hash_defaults_to_empty_string(self):
        plan = _build_plan()

        assert plan.plan_hash == ""


class TestDQReport:
    def test_build_returns_passed_report_when_all_checks_pass(self):
        results = [
            DQCheckResult(
                check_name="null_emails",
                metric_key="null_email_count",
                value=0,
                passed=True,
            ),
            DQCheckResult(
                check_name="dup_ids",
                metric_key="dup_id_count",
                value=0,
                passed=True,
            ),
        ]

        report = DQReport.build(results)

        assert report.passed is True
        assert report.results == results
        assert report.failure_summary == ""

    def test_build_includes_failure_details_for_failed_checks(self):
        results = [
            DQCheckResult(
                check_name="null_emails",
                metric_key="null_email_count",
                value=1,
                passed=False,
                failure_reason="null count must be zero",
            ),
            DQCheckResult(
                check_name="dup_ids",
                metric_key="dup_id_count",
                value=0,
                passed=True,
            ),
        ]

        report = DQReport.build(results)

        assert report.passed is False
        assert "Data quality checks failed (1/2):" in report.failure_summary
        assert "null_emails" in report.failure_summary
        assert "null_email_count" in report.failure_summary
        assert "null count must be zero" in report.failure_summary

    def test_build_uses_default_reason_when_failure_reason_is_missing(self):
        results = [
            DQCheckResult(
                check_name="dup_ids",
                metric_key="dup_id_count",
                value=10,
                passed=False,
                failure_reason=None,
            )
        ]

        report = DQReport.build(results)

        assert report.passed is False
        assert "validator returned False" in report.failure_summary
