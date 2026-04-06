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

from airflow_breeze.commands.pr_commands import _compute_default_action
from airflow_breeze.utils.confirm import TriageAction
from airflow_breeze.utils.github import PRAssessment, Violation, _has_demo_evidence, assess_pr_ui_demo
from airflow_breeze.utils.pr_models import PRData


class TestHasDemoEvidence:
    """Tests for _has_demo_evidence helper function."""

    def test_empty_body(self):
        assert _has_demo_evidence("") is False

    def test_none_body(self):
        assert _has_demo_evidence(None) is False

    def test_text_only_body(self):
        assert _has_demo_evidence("This PR fixes a bug in the UI sidebar component.") is False

    def test_html_img_tag_github_assets(self):
        body = (
            "## Changes\n"
            '<img width="1159" alt="image" '
            'src="https://github.com/user-attachments/assets/abc123-def456">'
        )
        assert _has_demo_evidence(body) is True

    def test_bare_github_assets_url(self):
        body = "## Demo\n\nhttps://github.com/user-attachments/assets/abc123-def456-video\n"
        assert _has_demo_evidence(body) is True

    def test_markdown_image_syntax(self):
        body = "![screenshot](https://example.com/image.png)"
        assert _has_demo_evidence(body) is True

    def test_direct_image_url(self):
        body = "See the result at https://example.com/demo.png"
        assert _has_demo_evidence(body) is True

    def test_direct_video_url(self):
        body = "Recording: https://example.com/screen.mp4"
        assert _has_demo_evidence(body) is True

    @pytest.mark.parametrize(
        "extension",
        ["png", "jpg", "jpeg", "gif", "webp", "mp4", "mov", "webm"],
    )
    def test_various_media_extensions(self, extension):
        body = f"https://example.com/file.{extension}"
        assert _has_demo_evidence(body) is True

    def test_no_false_positive_on_code_mention(self):
        body = "Updated the image component to handle png files better."
        assert _has_demo_evidence(body) is False

    def test_no_false_positive_on_url_without_media_extension(self):
        body = "See https://github.com/apache/airflow/pull/12345 for context."
        assert _has_demo_evidence(body) is False


class TestAssessPrUiDemo:
    """Tests for assess_pr_ui_demo deterministic check."""

    def test_no_ui_label_returns_none(self):
        result = assess_pr_ui_demo(
            pr_number=123,
            labels=["area:API", "kind:bug"],
            body="No screenshots here.",
            author_association="CONTRIBUTOR",
        )
        assert result is None

    @pytest.mark.parametrize("association", ["COLLABORATOR", "MEMBER", "OWNER"])
    def test_collaborator_association_returns_none(self, association):
        result = assess_pr_ui_demo(
            pr_number=123,
            labels=["area:UI"],
            body="No screenshots here.",
            author_association=association,
        )
        assert result is None

    def test_contributor_with_demo_returns_none(self):
        body = '<img width="500" src="https://github.com/user-attachments/assets/abc123">'
        result = assess_pr_ui_demo(
            pr_number=123,
            labels=["area:UI"],
            body=body,
            author_association="CONTRIBUTOR",
        )
        assert result is None

    def test_contributor_without_demo_returns_assessment(self):
        result = assess_pr_ui_demo(
            pr_number=123,
            labels=["area:UI"],
            body="This fixes a UI bug.",
            author_association="CONTRIBUTOR",
        )
        assert result is not None
        assert result.should_flag is True
        assert len(result.violations) == 1
        assert result.violations[0].category == "Missing UI demo"
        assert result.violations[0].severity == "warning"

    def test_none_association_without_demo_returns_assessment(self):
        result = assess_pr_ui_demo(
            pr_number=456,
            labels=["area:UI", "kind:feature"],
            body="Added new sidebar component.",
            author_association="NONE",
        )
        assert result is not None
        assert result.should_flag is True
        assert "screenshots" in result.violations[0].explanation.lower()

    def test_empty_body_returns_assessment(self):
        result = assess_pr_ui_demo(
            pr_number=789,
            labels=["area:UI"],
            body="",
            author_association="CONTRIBUTOR",
        )
        assert result is not None
        assert result.should_flag is True

    def test_summary_includes_pr_number(self):
        result = assess_pr_ui_demo(
            pr_number=42,
            labels=["area:UI"],
            body="Some text",
            author_association="CONTRIBUTOR",
        )
        assert result is not None
        assert "42" in result.summary


class TestComputeDefaultActionSoftViolations:
    """Test default action for PRs with UI demo violations."""

    @staticmethod
    def _make_pr(**overrides) -> PRData:
        defaults = {
            "number": 123,
            "title": "UI change",
            "body": "Some text",
            "url": "https://github.com/apache/airflow/pull/123",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
            "node_id": "PR_123",
            "author_login": "contributor",
            "author_association": "NONE",
            "head_sha": "abc123",
            "base_ref": "main",
            "check_summary": "",
            "checks_state": "SUCCESS",
            "failed_checks": [],
            "commits_behind": 10,
            "is_draft": False,
            "mergeable": "MERGEABLE",
            "labels": ["area:UI"],
            "unresolved_threads": [],
        }
        defaults.update(overrides)
        return PRData(**defaults)

    @staticmethod
    def _make_assessment(summary: str) -> PRAssessment:
        return PRAssessment(
            should_flag=True,
            violations=[
                Violation(
                    category="Missing UI demo",
                    explanation="No screenshots",
                    severity="warning",
                    details="Add screenshots",
                )
            ],
            summary=summary,
        )

    def test_soft_violation_only_suggests_draft(self):
        """A PR with only a soft violation (no CI failures, conflicts, or comments) gets DRAFT."""
        pr = self._make_pr()
        assessment = self._make_assessment("PR #123 changes UI code but has no screenshots or demo.")
        action, reason = _compute_default_action(pr, assessment, author_flagged_count={})
        assert action == TriageAction.DRAFT
        assert "draft" in reason

    def test_soft_violation_with_ci_failures_suggests_draft(self):
        """A PR with a soft violation AND multiple CI failures gets DRAFT (hard issue takes priority)."""
        pr = self._make_pr(
            failed_checks=["Tests / Core", "Tests / API", "Tests / CLI"],
            checks_state="FAILURE",
        )
        assessment = self._make_assessment("PR has CI failures and no demo.")
        action, _reason = _compute_default_action(pr, assessment, author_flagged_count={})
        assert action == TriageAction.DRAFT

    def test_soft_violation_with_conflicts_suggests_draft(self):
        """A PR with a soft violation AND merge conflicts gets DRAFT."""
        pr = self._make_pr(mergeable="CONFLICTING")
        assessment = self._make_assessment("PR has conflicts and no demo.")
        action, _reason = _compute_default_action(pr, assessment, author_flagged_count={})
        assert action == TriageAction.DRAFT
