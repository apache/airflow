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
"""Tests for the pr_actions module — action recommendation and execution logic."""
from __future__ import annotations

from dataclasses import dataclass, field
from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.utils.confirm import TriageAction
from airflow_breeze.utils.pr_actions import (
    AUTHOR_FLAGGED_CLOSE_THRESHOLD,
    MAX_CI_FAILURES_FOR_RERUN,
    MAX_COMMITS_BEHIND_FOR_RERUN,
    PRStateSnapshot,
    are_only_static_check_failures,
    compute_default_action,
    confirm_action,
    execute_triage_action,
    select_violations,
    snapshot_pr_state,
)
from airflow_breeze.utils.pr_models import PRData, UnresolvedThread


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------


def _make_pr(**overrides) -> PRData:
    """Create a PRData with sensible defaults, overridable by kwargs."""
    defaults = dict(
        number=100,
        title="Test PR",
        body="Test body",
        url="https://github.com/apache/airflow/pull/100",
        created_at="2025-01-01T00:00:00Z",
        updated_at="2025-01-01T01:00:00Z",
        node_id="PR_100",
        author_login="testuser",
        author_association="NONE",
        head_sha="abc123def456",
        base_ref="main",
        check_summary="",
        checks_state="SUCCESS",
        failed_checks=[],
        commits_behind=0,
        is_draft=False,
        mergeable="MERGEABLE",
        labels=[],
        unresolved_threads=[],
        review_decisions=[],
        has_collaborator_review=False,
    )
    defaults.update(overrides)
    return PRData(**defaults)


@dataclass
class _MockAssessment:
    """Lightweight mock for PRAssessment."""
    should_flag: bool = True
    should_report: bool = False
    violations: list = field(default_factory=list)
    summary: str = ""
    error: bool = False


@dataclass
class _MockViolation:
    """Lightweight mock for Violation."""
    category: str = "test"
    explanation: str = "test issue"
    severity: str = "warning"
    details: str = ""


@dataclass
class _MockRecentPRFailureInfo:
    """Lightweight mock for RecentPRFailureInfo."""
    failing_checks: dict = field(default_factory=dict)
    failing_check_names: set = field(default_factory=set)
    prs_examined: int = 5

    def find_matching_failures(self, pr_failed_checks: list[str]) -> list[str]:
        matched = []
        for check in pr_failed_checks:
            lower = check.lower()
            for recent_check in self.failing_check_names:
                if lower == recent_check.lower() or lower in recent_check.lower() or recent_check.lower() in lower:
                    matched.append(check)
                    break
        return matched


@dataclass
class _MockStats:
    """Lightweight mock for TriageStats."""
    total_converted: int = 0
    total_commented: int = 0
    total_closed: int = 0
    total_ready: int = 0
    total_rebased: int = 0
    total_rerun: int = 0
    total_skipped_action: int = 0
    total_workflows_approved: int = 0
    quit_early: bool = False


# ===========================================================================
# TestComputeDefaultAction
# ===========================================================================
class TestComputeDefaultAction:
    """Tests for the core action recommendation engine."""

    def test_should_report_returns_skip(self):
        """LLM flagged for reporting -> SKIP so user can review."""
        pr = _make_pr()
        assessment = _MockAssessment(should_report=True, summary="suspicious content")
        action, reason = compute_default_action(pr, assessment, {})
        assert action == TriageAction.SKIP
        assert "reporting" in reason.lower()

    def test_merge_conflicts_suggest_draft(self):
        """PR with merge conflicts and CI failures -> DRAFT."""
        pr = _make_pr(mergeable="CONFLICTING", failed_checks=["CI Test"])
        assessment = _MockAssessment(summary="issues found")
        action, reason = compute_default_action(pr, assessment, {})
        assert action == TriageAction.DRAFT
        assert "merge conflicts" in reason.lower()

    def test_author_flagged_count_above_threshold_suggests_close(self):
        """Author with many flagged PRs -> CLOSE."""
        pr = _make_pr(failed_checks=["CI Test"])
        assessment = _MockAssessment()
        count = AUTHOR_FLAGGED_CLOSE_THRESHOLD + 1
        action, reason = compute_default_action(pr, assessment, {"testuser": count})
        assert action == TriageAction.CLOSE
        assert "flagged" in reason.lower()

    def test_author_flagged_at_threshold_does_not_close(self):
        """Author at exactly the threshold -> NOT CLOSE."""
        pr = _make_pr(failed_checks=["CI Test"])
        assessment = _MockAssessment()
        action, _ = compute_default_action(
            pr, assessment, {"testuser": AUTHOR_FLAGGED_CLOSE_THRESHOLD}
        )
        assert action != TriageAction.CLOSE

    def test_no_checks_unknown_state_suggests_draft(self):
        """PR with UNKNOWN checks_state and no conflicts -> DRAFT (needs rebase)."""
        pr = _make_pr(checks_state="UNKNOWN")
        assessment = _MockAssessment()
        action, reason = compute_default_action(pr, assessment, {})
        assert action == TriageAction.DRAFT
        assert "rebase" in reason.lower()

    def test_all_failures_match_main_suggests_rerun(self):
        """All CI failures match main branch failures -> RERUN."""
        pr = _make_pr(failed_checks=["Tests postgres", "Tests mysql"])
        assessment = _MockAssessment()
        main_failures = _MockRecentPRFailureInfo(
            failing_check_names={"Tests postgres", "Tests mysql"}
        )
        action, reason = compute_default_action(pr, assessment, {}, main_failures)
        assert action == TriageAction.RERUN
        assert "systemic" in reason.lower()

    def test_some_failures_match_main_suggests_rerun(self):
        """Some CI failures match main branch -> RERUN."""
        pr = _make_pr(failed_checks=["Tests postgres", "Tests sqlite"])
        assessment = _MockAssessment()
        main_failures = _MockRecentPRFailureInfo(
            failing_check_names={"Tests postgres"}
        )
        action, reason = compute_default_action(pr, assessment, {}, main_failures)
        assert action == TriageAction.RERUN
        assert "1/2" in reason

    def test_only_static_check_failures_suggest_comment(self):
        """Only static check failures -> COMMENT (not rerun)."""
        pr = _make_pr(failed_checks=["Static checks", "MyPy checks"])
        assessment = _MockAssessment()
        action, reason = compute_default_action(pr, assessment, {})
        assert action == TriageAction.COMMENT
        assert "static" in reason.lower()

    def test_few_ci_failures_no_conflicts_suggest_rerun(self):
        """1-2 CI failures, no conflicts, not far behind -> RERUN."""
        pr = _make_pr(
            failed_checks=["Tests postgres"],
            commits_behind=10,
        )
        assessment = _MockAssessment()
        action, _ = compute_default_action(pr, assessment, {})
        assert action == TriageAction.RERUN

    def test_few_ci_failures_too_far_behind_suggest_draft(self):
        """1-2 CI failures but too far behind -> DRAFT."""
        pr = _make_pr(
            failed_checks=["Tests postgres"],
            commits_behind=MAX_COMMITS_BEHIND_FOR_RERUN + 1,
        )
        assessment = _MockAssessment()
        action, _ = compute_default_action(pr, assessment, {})
        assert action == TriageAction.DRAFT

    def test_too_many_ci_failures_suggest_draft(self):
        """More than MAX_CI_FAILURES_FOR_RERUN failures -> DRAFT."""
        failures = [f"Test {i}" for i in range(MAX_CI_FAILURES_FOR_RERUN + 1)]
        pr = _make_pr(failed_checks=failures)
        assessment = _MockAssessment()
        action, _ = compute_default_action(pr, assessment, {})
        assert action == TriageAction.DRAFT

    def test_no_ci_failures_only_unresolved_comments_suggest_comment(self):
        """CI passes, no conflicts, only unresolved comments -> COMMENT."""
        thread = UnresolvedThread(
            reviewer_login="reviewer1",
            reviewer_association="MEMBER",
            comment_body="Please fix this",
            comment_url="https://example.com",
            author_last_reply="",
        )
        pr = _make_pr(unresolved_threads=[thread])
        assessment = _MockAssessment()
        action, _ = compute_default_action(pr, assessment, {})
        assert action == TriageAction.COMMENT

    def test_ci_failures_with_unresolved_comments_suggest_draft(self):
        """CI failures + unresolved comments -> DRAFT."""
        thread = UnresolvedThread(
            reviewer_login="reviewer1",
            reviewer_association="MEMBER",
            comment_body="Please fix this",
            comment_url="https://example.com",
            author_last_reply="",
        )
        pr = _make_pr(
            failed_checks=["Tests postgres", "Tests mysql", "Tests sqlite"],
            unresolved_threads=[thread],
        )
        assessment = _MockAssessment()
        action, _ = compute_default_action(pr, assessment, {})
        assert action == TriageAction.DRAFT

    def test_default_fallback_is_draft(self):
        """Default action when no specific conditions match -> DRAFT."""
        pr = _make_pr(
            mergeable="CONFLICTING",
            failed_checks=["Tests"],
        )
        assessment = _MockAssessment()
        action, _ = compute_default_action(pr, assessment, {})
        assert action == TriageAction.DRAFT

    def test_reason_starts_with_uppercase_or_digit(self):
        """Reason string should start with uppercase letter or digit."""
        pr = _make_pr(failed_checks=["CI"])
        assessment = _MockAssessment()
        _, reason = compute_default_action(pr, assessment, {})
        assert reason[0].isupper() or reason[0].isdigit()

    def test_assessment_summary_included_in_reason(self):
        """Assessment summary appears in the reason string."""
        pr = _make_pr(failed_checks=["CI"])
        assessment = _MockAssessment(summary="Missing tests")
        _, reason = compute_default_action(pr, assessment, {})
        assert "missing tests" in reason.lower()


# ===========================================================================
# TestAreOnlyStaticCheckFailures
# ===========================================================================
class TestAreOnlyStaticCheckFailures:
    def test_empty(self):
        assert are_only_static_check_failures([]) is False

    def test_single_static_check(self):
        assert are_only_static_check_failures(["Static checks"]) is True

    def test_mypy(self):
        assert are_only_static_check_failures(["MyPy checks"]) is True

    def test_ruff(self):
        assert are_only_static_check_failures(["ruff linting"]) is True

    def test_mixed_static_and_test(self):
        assert are_only_static_check_failures(["Static checks", "Tests postgres"]) is False

    def test_only_test_failures(self):
        assert are_only_static_check_failures(["Tests postgres", "Tests sqlite"]) is False

    def test_lint_patterns(self):
        for name in ["pylint check", "bandit scan", "codespell", "yamllint", "shellcheck", "isort"]:
            assert are_only_static_check_failures([name]) is True, f"Expected True for {name}"

    def test_case_insensitive(self):
        assert are_only_static_check_failures(["STATIC CHECK"]) is True
        assert are_only_static_check_failures(["MyPy Checks"]) is True


# ===========================================================================
# TestSelectViolations
# ===========================================================================
class TestSelectViolations:
    def test_empty_returns_empty(self):
        assert select_violations([]) == []

    def test_single_returns_as_is(self):
        v = [_MockViolation()]
        assert select_violations(v) == v

    def test_two_violations_accept_all_default(self):
        """When user presses enter (empty), all violations returned."""
        v = [_MockViolation(category="a"), _MockViolation(category="b")]
        with patch("builtins.input", return_value=""):
            result = select_violations(v)
        assert result == v


# ===========================================================================
# TestPRStateSnapshot
# ===========================================================================
class TestPRStateSnapshot:
    def test_snapshot_captures_fields(self):
        pr = _make_pr(head_sha="abc123", updated_at="2025-01-01T00:00:00Z", is_draft=True)
        snap = snapshot_pr_state(pr)
        assert snap.head_sha == "abc123"
        assert snap.updated_at == "2025-01-01T00:00:00Z"
        assert snap.is_draft is True


# ===========================================================================
# TestConfirmAction
# ===========================================================================
class TestConfirmAction:
    def test_forced_yes(self):
        pr = _make_pr()
        result = confirm_action(pr, "Test action", forced_answer="y")
        assert result is True

    def test_forced_no(self):
        pr = _make_pr()
        result = confirm_action(pr, "Test action", forced_answer="n")
        assert result is False

    def test_forced_quit_sets_stats(self):
        pr = _make_pr()
        stats = _MockStats()
        result = confirm_action(pr, "Test action", forced_answer="q", stats=stats)
        assert result is False


# ===========================================================================
# TestExecuteTriageAction
# ===========================================================================
class TestExecuteTriageAction:
    def _make_ctx(self):
        ctx = MagicMock()
        ctx.stats = _MockStats()
        ctx.token = "fake-token"
        ctx.github_repository = "apache/airflow"
        ctx.answer_triage = None
        ctx.dry_run = False
        return ctx

    def test_skip_increments_counter(self):
        ctx = self._make_ctx()
        pr = _make_pr()
        result = execute_triage_action(ctx, pr, TriageAction.SKIP, draft_comment="", close_comment="")
        assert result is None
        assert ctx.stats.total_skipped_action == 1

    @patch("airflow_breeze.utils.pr_actions.confirm_action", return_value=True)
    @patch("airflow_breeze.utils.pr_github.add_label", return_value=True)
    def test_ready_adds_label(self, mock_add_label, mock_confirm):
        ctx = self._make_ctx()
        pr = _make_pr()
        execute_triage_action(ctx, pr, TriageAction.READY, draft_comment="", close_comment="")
        assert ctx.stats.total_ready == 1

    @patch("airflow_breeze.utils.pr_actions.confirm_action", return_value=False)
    def test_ready_cancelled_increments_skip(self, mock_confirm):
        ctx = self._make_ctx()
        pr = _make_pr()
        execute_triage_action(ctx, pr, TriageAction.READY, draft_comment="", close_comment="")
        assert ctx.stats.total_skipped_action == 1

    @patch("airflow_breeze.utils.pr_actions.confirm_action", return_value=True)
    @patch("airflow_breeze.utils.pr_github.convert_pr_to_draft", return_value=True)
    @patch("airflow_breeze.utils.pr_github.post_comment", return_value=True)
    def test_draft_converts_and_comments(self, mock_comment, mock_draft, mock_confirm):
        ctx = self._make_ctx()
        pr = _make_pr()
        execute_triage_action(
            ctx, pr, TriageAction.DRAFT,
            draft_comment="Please fix issues", close_comment=""
        )
        assert ctx.stats.total_converted == 1

    @patch("airflow_breeze.utils.pr_actions.confirm_action", return_value=True)
    @patch("airflow_breeze.utils.pr_github.close_pr", return_value=True)
    @patch("airflow_breeze.utils.pr_github.add_label", return_value=True)
    @patch("airflow_breeze.utils.pr_github.post_comment", return_value=True)
    def test_close_closes_and_labels(self, mock_comment, mock_label, mock_close, mock_confirm):
        ctx = self._make_ctx()
        pr = _make_pr()
        execute_triage_action(
            ctx, pr, TriageAction.CLOSE,
            draft_comment="", close_comment="Closing due to quality issues"
        )
        assert ctx.stats.total_closed == 1

    @patch("airflow_breeze.utils.pr_actions.confirm_action", return_value=True)
    @patch("airflow_breeze.utils.pr_github.post_comment", return_value=True)
    def test_comment_posts_comment(self, mock_comment, mock_confirm):
        ctx = self._make_ctx()
        pr = _make_pr()
        execute_triage_action(
            ctx, pr, TriageAction.COMMENT,
            draft_comment="Please fix", close_comment=""
        )
        assert ctx.stats.total_commented == 1

    @patch("airflow_breeze.utils.pr_workflows.rerun_failed_workflow_runs", return_value=1)
    def test_rerun_triggers_rerun(self, mock_rerun):
        ctx = self._make_ctx()
        pr = _make_pr(head_sha="abc123", failed_checks=["CI Test"])
        execute_triage_action(ctx, pr, TriageAction.RERUN, draft_comment="", close_comment="")
        assert ctx.stats.total_rerun == 1

    @patch("airflow_breeze.utils.pr_actions.confirm_action", return_value=True)
    @patch("airflow_breeze.utils.pr_github.update_pr_branch", return_value=True)
    def test_rebase_updates_branch(self, mock_update, mock_confirm):
        ctx = self._make_ctx()
        pr = _make_pr()
        execute_triage_action(ctx, pr, TriageAction.REBASE, draft_comment="", close_comment="")
        assert ctx.stats.total_rebased == 1

    @patch("airflow_breeze.utils.pr_actions.confirm_action", return_value=True)
    @patch("airflow_breeze.utils.pr_github.post_comment", return_value=True)
    def test_ping_posts_ping_comment(self, mock_comment, mock_confirm):
        ctx = self._make_ctx()
        thread = UnresolvedThread(
            reviewer_login="reviewer1",
            reviewer_association="MEMBER",
            comment_body="Fix this",
            comment_url="https://example.com",
            author_last_reply="",
        )
        pr = _make_pr(unresolved_threads=[thread])
        execute_triage_action(ctx, pr, TriageAction.PING, draft_comment="", close_comment="")
        assert ctx.stats.total_commented == 1

    def test_ping_without_threads_is_noop(self):
        ctx = self._make_ctx()
        pr = _make_pr(unresolved_threads=[])
        execute_triage_action(ctx, pr, TriageAction.PING, draft_comment="", close_comment="")
        assert ctx.stats.total_commented == 0


# ===========================================================================
# TestThresholdConstants
# ===========================================================================
class TestThresholdConstants:
    """Verify threshold constants are importable and have expected values."""

    def test_author_flagged_threshold(self):
        assert AUTHOR_FLAGGED_CLOSE_THRESHOLD == 3

    def test_max_ci_failures_for_rerun(self):
        assert MAX_CI_FAILURES_FOR_RERUN == 2

    def test_max_commits_behind_for_rerun(self):
        assert MAX_COMMITS_BEHIND_FOR_RERUN == 50
