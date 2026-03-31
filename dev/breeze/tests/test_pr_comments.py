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
"""Tests for PR comment building utilities."""

from __future__ import annotations

from dataclasses import dataclass

from airflow_breeze.utils.pr_comments import (
    build_close_comment,
    build_collaborator_comment,
    build_comment,
    build_review_nudge_comment,
)


@dataclass
class FakeViolation:
    category: str
    explanation: str
    severity: str = "warning"
    details: str = ""


@dataclass
class FakeStaleReview:
    reviewer_login: str
    author_pinged_reviewer: bool


class TestBuildCollaboratorComment:
    def test_draft_mode(self):
        v = [FakeViolation("ruff", "linting error")]
        result = build_collaborator_comment("alice", v, 0, "main", comment_only=False)
        assert "@alice" in result
        assert "Converted to draft" in result
        assert "ruff" in result

    def test_comment_only(self):
        v = [FakeViolation("mypy", "type error")]
        result = build_collaborator_comment("alice", v, 0, "main", comment_only=True)
        assert "@alice" in result
        assert "Issues found" in result
        assert "Converted to draft" not in result

    def test_rebase_note(self):
        v = [FakeViolation("ci", "failure")]
        result = build_collaborator_comment("bob", v, 50, "main")
        assert "50 commits behind" in result


class TestBuildComment:
    def test_collaborator_delegates(self):
        v = [FakeViolation("test", "missing")]
        result = build_comment("alice", v, 123, 0, "main", is_collaborator=True)
        # Collaborator comment is simpler
        assert "quality criteria" not in result.lower()

    def test_non_collaborator_has_criteria_link(self):
        v = [FakeViolation("test", "missing")]
        result = build_comment("alice", v, 123, 0, "main", is_collaborator=False)
        assert "quality criteria" in result.lower()
        assert "@alice" in result

    def test_comment_only_mode(self):
        v = [FakeViolation("test", "missing")]
        result = build_comment("alice", v, 123, 0, "main", comment_only=True)
        assert "converted to" not in result.lower()

    def test_rebase_note_for_far_behind(self):
        v = [FakeViolation("ci", "failure")]
        result = build_comment("bob", v, 123, 100, "main")
        assert "100 commits behind" in result

    def test_no_rebase_note_when_close(self):
        v = [FakeViolation("ci", "failure")]
        result = build_comment("bob", v, 123, 10, "main")
        assert "commits behind" not in result

    def test_error_severity_icon(self):
        v = [FakeViolation("critical", "bad code", severity="error")]
        result = build_comment("alice", v, 123, 0, "main")
        assert ":x:" in result

    def test_warning_severity_icon(self):
        v = [FakeViolation("style", "formatting", severity="warning")]
        result = build_comment("alice", v, 123, 0, "main")
        assert ":warning:" in result


class TestBuildCloseComment:
    def test_includes_violations(self):
        v = [FakeViolation("spam", "looks automated")]
        result = build_close_comment("spammer", v, 123, 1)
        assert "closed" in result.lower()
        assert "spam" in result

    def test_multiple_flagged_prs_warning(self):
        v = [FakeViolation("quality", "low")]
        result = build_close_comment("user", v, 123, 5)
        assert "5 PRs" in result

    def test_collaborator_simpler(self):
        v = [FakeViolation("issue", "problem")]
        result = build_close_comment("alice", v, 123, 1, is_collaborator=True)
        assert "quality criteria" not in result.lower()


class TestBuildReviewNudgeComment:
    def test_pinged_reviewer(self):
        reviews = [FakeStaleReview("reviewer1", author_pinged_reviewer=True)]
        result = build_review_nudge_comment("author", reviews)
        assert "@reviewer1" in result
        assert "take another look" in result

    def test_not_pinged(self):
        reviews = [FakeStaleReview("reviewer2", author_pinged_reviewer=False)]
        result = build_review_nudge_comment("author", reviews)
        assert "reviewer2" in result
        assert "ping the reviewer" in result

    def test_mixed(self):
        reviews = [
            FakeStaleReview("r1", author_pinged_reviewer=True),
            FakeStaleReview("r2", author_pinged_reviewer=False),
        ]
        result = build_review_nudge_comment("author", reviews)
        assert "@r1" in result
        assert "r2" in result
