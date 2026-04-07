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
"""Tests for the pr_enrichment module."""
from __future__ import annotations

from unittest.mock import patch

from airflow_breeze.utils.pr_enrichment import (
    apply_wildcard_label_filters,
    merge_pr_assessments,
    split_label_filters,
)
from airflow_breeze.utils.pr_models import PRData


def _make_pr(**overrides) -> PRData:
    defaults = dict(
        number=100,
        title="Test PR",
        body="",
        url="https://github.com/apache/airflow/pull/100",
        created_at="2025-01-01T00:00:00Z",
        updated_at="2025-01-01T01:00:00Z",
        node_id="PR_100",
        author_login="testuser",
        author_association="NONE",
        head_sha="abc123",
        base_ref="main",
        check_summary="",
        checks_state="SUCCESS",
        failed_checks=[],
        commits_behind=0,
        is_draft=False,
        mergeable="MERGEABLE",
        labels=[],
        unresolved_threads=[],
    )
    defaults.update(overrides)
    return PRData(**defaults)


class TestSplitLabelFilters:
    def test_no_wildcards(self):
        exact, wildcard, exact_ex, wildcard_ex = split_label_filters(
            ("area:core", "provider:amazon"), ("bug",)
        )
        assert exact == ("area:core", "provider:amazon")
        assert wildcard == []
        assert exact_ex == ("bug",)
        assert wildcard_ex == []

    def test_with_wildcards(self):
        exact, wildcard, exact_ex, wildcard_ex = split_label_filters(
            ("area:*", "provider:amazon"), ("bug*",)
        )
        assert exact == ("provider:amazon",)
        assert wildcard == ["area:*"]
        assert exact_ex == ()
        assert wildcard_ex == ["bug*"]

    def test_empty_input(self):
        exact, wildcard, exact_ex, wildcard_ex = split_label_filters((), ())
        assert exact == ()
        assert wildcard == []
        assert exact_ex == ()
        assert wildcard_ex == []


class TestApplyWildcardLabelFilters:
    def test_include_filter(self):
        pr1 = _make_pr(number=1, labels=["area:core", "bug"])
        pr2 = _make_pr(number=2, labels=["provider:amazon"])
        result = apply_wildcard_label_filters([pr1, pr2], ["area:*"], [])
        assert len(result) == 1
        assert result[0].number == 1

    def test_exclude_filter(self):
        pr1 = _make_pr(number=1, labels=["area:core", "bug"])
        pr2 = _make_pr(number=2, labels=["area:core"])
        result = apply_wildcard_label_filters([pr1, pr2], [], ["bug*"])
        assert len(result) == 1
        assert result[0].number == 2

    def test_no_filters(self):
        pr1 = _make_pr(number=1)
        result = apply_wildcard_label_filters([pr1], [], [])
        assert len(result) == 1


class TestMergePrAssessments:
    def test_all_none_returns_none(self):
        assert merge_pr_assessments(None, None, None) is None

    def test_merges_violations(self):
        from dataclasses import dataclass, field

        @dataclass
        class _MockAssessment:
            should_flag: bool = True
            violations: list = field(default_factory=list)
            summary: str = ""

        @dataclass
        class _MockViolation:
            category: str = "test"
            explanation: str = "test issue"
            severity: str = "warning"

        a1 = _MockAssessment(violations=[_MockViolation(category="ci")])
        a2 = _MockAssessment(violations=[_MockViolation(category="conflicts")])
        merged = merge_pr_assessments(a1, None, a2)
        assert merged is not None
        assert len(merged.violations) == 2
