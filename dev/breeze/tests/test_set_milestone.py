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

import json
from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.commands.ci_commands import (
    _determine_milestone_version,
    _find_latest_milestone,
    _find_matching_milestone,
    _get_backport_version_from_labels,
    _get_mention,
    _get_milestone_not_found_comment,
    _get_milestone_notification_comment,
    _get_milestone_prefix,
    _has_bug_fix_indicators,
    _parse_milestone_version,
    _parse_version_from_backport_label,
    _parse_version_from_branch,
    _should_skip_milestone_tagging,
)


class TestParseVersionFromBranch:
    """Test cases for _parse_version_from_branch."""

    @pytest.mark.parametrize(
        ("branch", "expected"),
        [
            ("v3-1-test", (3, 1)),
            ("v2-10-test", (2, 10)),
            ("v10-0-test", (10, 0)),
        ],
    )
    def test_valid_version_branch(self, branch, expected):
        assert _parse_version_from_branch(branch) == expected

    @pytest.mark.parametrize(
        "branch",
        ["main", "v3-test", "v3-1", "feature-branch"],
    )
    def test_invalid_version_branch(self, branch):
        assert _parse_version_from_branch(branch) is None


class TestParseVersionFromBackportLabel:
    """Test cases for _parse_version_from_backport_label."""

    @pytest.mark.parametrize(
        ("label", "expected"),
        [
            ("backport-to-v3-1-test", (3, 1)),
            ("backport-to-v2-10-test", (2, 10)),
        ],
    )
    def test_valid_backport_label(self, label, expected):
        assert _parse_version_from_backport_label(label) == expected

    @pytest.mark.parametrize(
        "label",
        ["backport-v3-1-test", "backport-to-main", "some-label"],
    )
    def test_invalid_backport_label(self, label):
        assert _parse_version_from_backport_label(label) is None


class TestGetMilestonePrefix:
    """Test cases for _get_milestone_prefix."""

    @pytest.mark.parametrize(
        ("major", "minor", "expected"),
        [
            (3, 1, "Airflow 3.1"),
            (2, 10, "Airflow 2.10"),
        ],
    )
    def test_milestone_prefix(self, major, minor, expected):
        assert _get_milestone_prefix(major, minor) == expected


class TestParseMilestoneVersion:
    """Test cases for _parse_milestone_version."""

    @pytest.mark.parametrize(
        ("title", "expected"),
        [
            ("Airflow 3.1.8", (3, 1, 8)),
            ("Airflow 3.2", (3, 2, 0)),
            ("Airflow 2.10.5", (2, 10, 5)),
        ],
    )
    def test_valid_milestone_version(self, title, expected):
        assert _parse_milestone_version(title) == expected

    @pytest.mark.parametrize(
        "title",
        ["Something else", "Airflow", "Airflow 3"],
    )
    def test_invalid_milestone_version(self, title):
        assert _parse_milestone_version(title) is None


class TestHasBugFixIndicators:
    """Test cases for _has_bug_fix_indicators."""

    @pytest.mark.parametrize(
        ("title", "labels"),
        [
            ("Fix: something broken", []),
            ("fix issue with scheduler", []),
            ("Bug in executor", []),
            ("BUG: critical issue", []),
            ("Normal title", ["kind:bug"]),
            ("Normal title", ["type:bug-fix"]),
        ],
    )
    def test_has_bug_indicators(self, title, labels):
        assert _has_bug_fix_indicators(title, labels)

    def test_no_bug_indicators(self):
        assert not _has_bug_fix_indicators("Add new feature", ["kind:feature"])


class TestShouldSkipMilestoneTagging:
    """Test cases for _should_skip_milestone_tagging."""

    @pytest.mark.parametrize(
        "labels",
        [
            ["area:dev-tools"],
            ["area:dev-env"],
            ["area:CI"],
        ],
    )
    def test_skip_with_skip_labels(self, labels):
        assert _should_skip_milestone_tagging(labels)

    def test_no_skip_without_skip_labels(self):
        assert not _should_skip_milestone_tagging(["kind:feature", "area:scheduler"])


class TestGetBackportVersionFromLabels:
    """Test cases for _get_backport_version_from_labels."""

    def test_backport_label_found(self):
        labels = ["kind:feature", "backport-to-v3-1-test", "other-label"]
        assert _get_backport_version_from_labels(labels) == (3, 1)

    def test_no_backport_label(self):
        labels = ["kind:feature", "other-label"]
        assert _get_backport_version_from_labels(labels) is None


class TestDetermineMilestoneVersion:
    """Test cases for _determine_milestone_version."""

    @pytest.mark.parametrize(
        (
            "labels",
            "title",
            "base_branch",
            "expected_version",
            "expected_reason_substring",
        ),
        [
            (["backport-to-v3-1-test"], "Some title", "main", (3, 1), "backport label"),
            ([], "Fix: something", "v3-1-test", (3, 1), "bug fix"),
            ([], "Add feature", "v3-1-test", (3, 1), "merged to version branch"),
            ([], "Add feature", "main", None, "not merged to a version branch"),
        ],
    )
    def test_determine_milestone_version(
        self, labels, title, base_branch, expected_version, expected_reason_substring
    ):
        version, reason = _determine_milestone_version(labels, title, base_branch)
        assert version == expected_version
        assert expected_reason_substring in reason


class TestGetMention:
    """Test cases for _get_mention."""

    @pytest.mark.parametrize(
        ("merged_by_login", "expected"),
        [
            ("testuser", "@testuser"),
            ("unknown", "maintainer"),
            ("", "maintainer"),
            (None, "maintainer"),
        ],
    )
    def test_mention(self, merged_by_login, expected):
        assert _get_mention(merged_by_login) == expected


class TestGetMilestoneNotificationComment:
    """Test cases for _get_milestone_notification_comment."""

    def test_notification_comment_content(self):
        comment = _get_milestone_notification_comment(
            "Airflow 3.1.8", 42, "testuser", "bug fix", "apache/airflow"
        )
        assert "@testuser" in comment
        assert "Airflow 3.1.8" in comment
        assert "bug fix" in comment
        assert "milestone/42" in comment


class TestGetMilestoneNotFoundComment:
    """Test cases for _get_milestone_not_found_comment."""

    def test_not_found_comment(self):
        comment = _get_milestone_not_found_comment(
            "testuser", "bug fix", "apache/airflow", "prefix 'Airflow 3.1'"
        )
        assert "@testuser" in comment
        assert "no open milestone was found" in comment
        assert "Action required" in comment


class TestFindMatchingMilestone:
    """Test cases for _find_matching_milestone."""

    def test_find_matching_milestone(self):
        mock_repo = MagicMock()
        mock_milestone1 = MagicMock()
        mock_milestone1.title = "Airflow 3.1.7"
        mock_milestone2 = MagicMock()
        mock_milestone2.title = "Airflow 3.1.8"
        mock_milestone3 = MagicMock()
        mock_milestone3.title = "Airflow 3.2"
        mock_repo.get_milestones.return_value = [mock_milestone1, mock_milestone2, mock_milestone3]

        result = _find_matching_milestone(mock_repo, "Airflow 3.1")
        assert result.title == "Airflow 3.1.8"  # Should get the latest patch version

    def test_no_matching_milestone(self):
        mock_repo = MagicMock()
        mock_milestone = MagicMock()
        mock_milestone.title = "Airflow 3.2"
        mock_repo.get_milestones.return_value = [mock_milestone]

        result = _find_matching_milestone(mock_repo, "Airflow 3.1")
        assert result is None


class TestFindLatestMilestone:
    """Test cases for _find_latest_milestone."""

    def test_find_latest_milestone(self):
        mock_repo = MagicMock()
        mock_milestone1 = MagicMock()
        mock_milestone1.title = "Airflow 3.1.8"
        mock_milestone2 = MagicMock()
        mock_milestone2.title = "Airflow 3.2"
        mock_milestone3 = MagicMock()
        mock_milestone3.title = "Airflow 2.10.5"
        mock_repo.get_milestones.return_value = [mock_milestone1, mock_milestone2, mock_milestone3]

        result = _find_latest_milestone(mock_repo)
        assert result.title == "Airflow 3.2"  # Should get the highest version

    def test_no_milestone_found(self):
        mock_repo = MagicMock()
        mock_repo.get_milestones.return_value = []

        result = _find_latest_milestone(mock_repo)
        assert result is None


class TestSetMilestoneCommand:
    """Test cases for set_milestone command."""

    @pytest.fixture
    def cli_runner(self):
        """Create a CliRunner for testing CLI commands."""
        from click.testing import CliRunner

        return CliRunner()

    @pytest.fixture
    def mock_github_setup(self):
        """Set up mock GitHub client, repo, and issue."""
        mock_gh = MagicMock()
        mock_repo = MagicMock()
        mock_issue = MagicMock()

        mock_gh.get_repo.return_value = mock_repo
        mock_repo.get_issue.return_value = mock_issue

        return mock_gh, mock_repo, mock_issue

    @pytest.mark.parametrize(
        ("base_branch", "skip_label"),
        [
            ("main", "area:CI"),
            ("main", "area:dev-tools"),
            ("main", "area:dev-env"),
            ("v3-1-test", "area:CI"),
            ("v3-1-test", "area:dev-tools"),
            ("v3-1-test", "area:dev-env"),
        ],
    )
    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_skip_label_should_skip(self, mock_get_client, base_branch, skip_label, cli_runner):
        """When PR has a skip label, milestone tagging should be skipped."""
        from airflow_breeze.commands.ci_commands import ci_group

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "12345",
                "--pr-title",
                "CI: update workflow",
                "--pr-labels",
                json.dumps([skip_label]),
                "--base-branch",
                base_branch,
                "--merged-by",
                "testuser",
                "--github-token",
                "fake-token",
            ],
        )

        mock_get_client.assert_not_called()
        assert "Skipping milestone tagging" in result.output

    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_main_branch_without_backport_label_should_skip(self, mock_get_client, cli_runner):
        """When PR is merged to main without backport label, milestone tagging should be skipped."""
        from airflow_breeze.commands.ci_commands import ci_group

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "12345",
                "--pr-title",
                "Add new feature",
                "--pr-labels",
                json.dumps(["kind:feature"]),
                "--base-branch",
                "main",
                "--merged-by",
                "testuser",
                "--github-token",
                "fake-token",
            ],
        )

        mock_get_client.assert_not_called()
        assert "No milestone to set" in result.output

    @pytest.mark.parametrize(
        ("base_branch", "pr_title", "pr_labels", "milestone_title", "expected_reason"),
        [
            # version branch - finds matching milestone (bug fix)
            (
                "v3-1-test",
                "Fix: scheduler issue",
                ["kind:bug"],
                "Airflow 3.1.8",
                "bug fix merged to version branch",
            ),
            # version branch - finds matching milestone (non-bug)
            (
                # Since we are on v3-1-test branch
                # so even the PR title and labels doesn't indicate a bug fix, we should still find the matching milestone for the version branch.
                "v3-1-test",
                "Add missing configuration",
                ["kind:documentation"],
                "Airflow 3.1.8",
                "merged to version branch",
            ),
            # backport label - finds version milestone
            (
                "main",
                "Add missing configuration",
                ["backport-to-v3-1-test", "kind:documentation"],
                "Airflow 3.1.8",
                "backport label targeting v3-1-test",
            ),
        ],
    )
    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_find_milestone_should_set_and_comment(
        self,
        mock_get_client,
        base_branch,
        pr_title,
        pr_labels,
        milestone_title,
        expected_reason,
        cli_runner,
        mock_github_setup,
    ):
        """When milestone is found, should set it and add comment."""
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        mock_issue.milestone = None
        mock_milestone = MagicMock()
        mock_milestone.title = milestone_title
        mock_milestone.number = 42

        mock_get_client.return_value = mock_gh
        mock_repo.get_milestones.return_value = [mock_milestone]

        captured_comments: list[str] = []
        mock_issue.create_comment.side_effect = lambda c: captured_comments.append(c)

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "12345",
                "--pr-title",
                pr_title,
                "--pr-labels",
                json.dumps(pr_labels),
                "--base-branch",
                base_branch,
                "--merged-by",
                "testuser",
                "--github-token",
                "fake-token",
            ],
        )

        mock_issue.edit.assert_called_once_with(milestone=mock_milestone)
        mock_issue.create_comment.assert_called_once()
        assert len(captured_comments) == 1

        expected_comment = f"""Hi @testuser, this PR was merged without a milestone set.
We've automatically set the milestone to **[{milestone_title}](https://github.com/apache/airflow/milestone/42)** based on: {expected_reason}
If this milestone is not correct, please update it to the appropriate milestone.

> This comment was generated by [Milestone Tag Assistant](https://github.com/apache/airflow/blob/main/.github/workflows/milestone-tag-assistant.yml).
"""
        assert captured_comments[0] == expected_comment
        assert "Successfully set milestone" in result.output
        assert milestone_title in result.output

    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_milestone_already_set_should_skip(self, mock_get_client, cli_runner, mock_github_setup):
        """When PR already has a milestone, should skip."""
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        existing_milestone = MagicMock()
        existing_milestone.title = "Existing Milestone"
        mock_issue.milestone = existing_milestone
        mock_get_client.return_value = mock_gh

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "12345",
                "--pr-title",
                "Some nice feature",
                "--base-branch",
                "v3-1-test",
                "--github-token",
                "fake-token",
            ],
        )

        mock_issue.edit.assert_not_called()
        mock_issue.create_comment.assert_not_called()
        # Rich console adds formatting/colors, so checking for parts of the string
        assert "already has milestone" in result.output
        assert "Existing Milestone" in result.output
        assert "Skipping" in result.output

    @pytest.mark.parametrize(
        ("base_branch", "pr_title", "pr_labels", "milestones", "expected_reason", "expected_search_criteria"),
        [
            # version branch - no matching milestone (only 3.2 exists, need 3.1)
            (
                "v3-1-test",
                "Fix: scheduler issue",
                ["kind:bug"],
                [MagicMock(title="Airflow 3.2")],
                "bug fix merged to version branch",
                "prefix 'Airflow 3.1'",
            ),
        ],
    )
    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_not_find_milestone_should_comment_warning(
        self,
        mock_get_client,
        base_branch,
        pr_title,
        pr_labels,
        milestones,
        expected_reason,
        expected_search_criteria,
        cli_runner,
        mock_github_setup,
    ):
        """When no milestone is found, should add warning comment."""
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        mock_issue.milestone = None
        captured_comments: list[str] = []
        mock_issue.create_comment.side_effect = lambda c: captured_comments.append(c)

        mock_get_client.return_value = mock_gh
        mock_repo.get_milestones.return_value = milestones

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "12345",
                "--pr-title",
                pr_title,
                "--pr-labels",
                json.dumps(pr_labels),
                "--base-branch",
                base_branch,
                "--merged-by",
                "testuser",
                "--github-token",
                "fake-token",
            ],
        )

        mock_issue.edit.assert_not_called()
        mock_issue.create_comment.assert_called_once()
        assert len(captured_comments) == 1

        expected_comment = f"""Hi @testuser, this PR was merged without a milestone set.
We tried to automatically set a milestone based on: {expected_reason}
However, **no open milestone was found** matching: {expected_search_criteria}

**Action required:** Please manually set the appropriate milestone for this PR.

> This comment was generated by [Milestone Tag Assistant](https://github.com/apache/airflow/blob/main/.github/workflows/milestone-tag-assistant.yml).
"""
        assert captured_comments[0] == expected_comment
        assert "No open milestone found" in result.output
