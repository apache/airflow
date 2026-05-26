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
import re
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from airflow_breeze.commands.ci_commands import (
    _actor_has_write_access,
    _all_backport_labels_removed,
    _determine_milestone_version,
    _find_latest_milestone,
    _find_matching_milestone,
    _get_backport_version_from_labels,
    _get_latest_backport_unlabel_actor,
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

_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain_output(output: str) -> str:
    """Strip ANSI color codes and collapse whitespace so wrap-tolerant substring
    asserts don't trip over Rich's color escapes or soft line wraps."""
    return " ".join(_ANSI_ESCAPE_RE.sub("", output).split())


def _label(name: str) -> MagicMock:
    """Build a mock that quacks like a PyGithub ``Label`` for ``issue.labels``."""
    m = MagicMock()
    m.name = name
    return m


def _unlabel_event(label_name: str, actor_login: str, when: datetime) -> MagicMock:
    """Build a mock that quacks like a PyGithub IssueEvent for an ``unlabeled`` event."""
    event = MagicMock()
    event.event = "unlabeled"
    event.label = _label(label_name)
    event.actor = MagicMock()
    event.actor.login = actor_login
    event.created_at = when
    return event


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

    def test_skip_when_all_backport_labels_removed_during_race_window(self):
        # Snapshot had a backport label, current has none — explicit maintainer signal.
        assert _should_skip_milestone_tagging(
            ["kind:bug"], snapshot_labels=["backport-to-v3-1-test", "kind:bug"]
        )

    def test_no_skip_when_backport_label_unchanged(self):
        assert not _should_skip_milestone_tagging(
            ["backport-to-v3-1-test", "kind:bug"],
            snapshot_labels=["backport-to-v3-1-test", "kind:bug"],
        )

    def test_no_skip_when_backport_label_added(self):
        # Adding a backport label is not a skip signal — the regular evaluation handles it.
        assert not _should_skip_milestone_tagging(
            ["backport-to-v3-1-test", "kind:bug"], snapshot_labels=["kind:bug"]
        )

    def test_no_skip_when_backport_label_replaced(self):
        # Swapping one backport label for another is a fix, not a cancel — current
        # state still has a backport, so the normal evaluation should run.
        assert not _should_skip_milestone_tagging(
            ["backport-to-v3-2-test", "kind:bug"],
            snapshot_labels=["backport-to-v3-1-test", "kind:bug"],
        )

    def test_no_skip_when_non_backport_label_removed(self):
        # Removing a non-backport label is irrelevant; tagging proceeds.
        assert not _should_skip_milestone_tagging(
            ["kind:bug"], snapshot_labels=["kind:bug", "kind:documentation"]
        )


class TestGetLatestBackportUnlabelActor:
    """Test cases for _get_latest_backport_unlabel_actor."""

    def test_returns_latest_actor_for_matching_unlabel(self):
        issue = MagicMock()
        issue.get_events.return_value = [
            _unlabel_event(
                "backport-to-v3-1-test", "alice", datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
            ),
            _unlabel_event("backport-to-v3-1-test", "bob", datetime(2026, 5, 23, 14, 0, tzinfo=timezone.utc)),
            _unlabel_event(
                "backport-to-v3-1-test", "carol", datetime(2026, 5, 23, 13, 0, tzinfo=timezone.utc)
            ),
        ]
        assert _get_latest_backport_unlabel_actor(issue, {"backport-to-v3-1-test"}) == "bob"

    def test_ignores_unrelated_label_events(self):
        issue = MagicMock()
        issue.get_events.return_value = [
            _unlabel_event("kind:documentation", "alice", datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)),
        ]
        assert _get_latest_backport_unlabel_actor(issue, {"backport-to-v3-1-test"}) is None

    def test_ignores_non_unlabeled_events(self):
        issue = MagicMock()
        labeled_event = MagicMock()
        labeled_event.event = "labeled"
        labeled_event.label = _label("backport-to-v3-1-test")
        labeled_event.actor = MagicMock()
        labeled_event.actor.login = "alice"
        labeled_event.created_at = datetime(2026, 5, 23, 12, 0, tzinfo=timezone.utc)
        issue.get_events.return_value = [labeled_event]
        assert _get_latest_backport_unlabel_actor(issue, {"backport-to-v3-1-test"}) is None

    def test_returns_none_when_events_fetch_raises(self):
        issue = MagicMock()
        issue.get_events.side_effect = RuntimeError("rate limited")
        assert _get_latest_backport_unlabel_actor(issue, {"backport-to-v3-1-test"}) is None


class TestActorHasWriteAccess:
    """Test cases for _actor_has_write_access."""

    @pytest.mark.parametrize("permission", ["admin", "write"])
    def test_returns_true_for_write_or_admin(self, permission):
        repo = MagicMock()
        repo.get_collaborator_permission.return_value = permission
        assert _actor_has_write_access(repo, "alice") is True

    @pytest.mark.parametrize("permission", ["read", "none", "triage"])
    def test_returns_false_for_other_permissions(self, permission):
        repo = MagicMock()
        repo.get_collaborator_permission.return_value = permission
        assert _actor_has_write_access(repo, "bot-user") is False

    def test_returns_none_when_lookup_raises(self):
        repo = MagicMock()
        repo.get_collaborator_permission.side_effect = RuntimeError("rate limited")
        assert _actor_has_write_access(repo, "alice") is None


class TestAllBackportLabelsRemoved:
    """Test cases for _all_backport_labels_removed."""

    def test_no_change(self):
        labels = ["backport-to-v3-1-test", "kind:bug"]
        assert _all_backport_labels_removed(labels, labels) == set()

    def test_single_backport_fully_removed(self):
        assert _all_backport_labels_removed(["backport-to-v3-1-test", "kind:bug"], ["kind:bug"]) == {
            "backport-to-v3-1-test"
        }

    def test_multiple_backports_fully_removed(self):
        assert _all_backport_labels_removed(["backport-to-v3-1-test", "backport-to-v3-2-test"], []) == {
            "backport-to-v3-1-test",
            "backport-to-v3-2-test",
        }

    def test_backport_replaced_returns_empty(self):
        # When a replacement backport remains, the helper reports nothing —
        # the regular re-evaluation should handle the new version.
        assert _all_backport_labels_removed(["backport-to-v3-1-test"], ["backport-to-v3-2-test"]) == set()

    def test_backport_added_not_reported(self):
        assert _all_backport_labels_removed([], ["backport-to-v3-1-test"]) == set()

    def test_non_backport_removal_not_reported(self):
        assert _all_backport_labels_removed(["kind:bug"], []) == set()


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
        # Fresh-issue labels match the workflow snapshot — no race, no re-evaluation.
        mock_issue.labels = [_label(name) for name in pr_labels]
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
                "--github-repository",
                "apache/airflow",
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
        # Fresh-issue labels match the workflow snapshot — no race, no re-evaluation.
        mock_issue.labels = [_label(name) for name in pr_labels]
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
                "--github-repository",
                "apache/airflow",
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

    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_backport_label_removed_after_snapshot_should_skip(
        self, mock_get_client, cli_runner, mock_github_setup
    ):
        """If a backport label is removed between the workflow snapshot and the action,
        the action must re-read labels from the issue and honour the current state —
        skip milestone-set when the only signal that triggered it (the backport label)
        is gone. Regression test for PR #67301 race.
        """
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        mock_issue.milestone = None
        mock_issue.labels = [_label("kind:documentation")]
        mock_get_client.return_value = mock_gh

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "67301",
                "--pr-title",
                "fix: typo",
                "--pr-labels",
                json.dumps(["backport-to-v3-2-test", "kind:documentation"]),
                "--base-branch",
                "main",
                "--merged-by",
                "shahar1",
                "--github-token",
                "fake-token",
                "--github-repository",
                "apache/airflow",
            ],
        )

        # Snapshot still has the backport label, but the fresh issue.labels does not.
        # The action must re-read, notice the change, and skip the milestone-set —
        # treating the maintainer's removal as an explicit "don't auto-tag" signal.
        mock_issue.edit.assert_not_called()
        mock_issue.create_comment.assert_not_called()
        assert "Labels changed since workflow snapshot" in result.output
        assert "backport labels removed during workflow window" in result.output
        assert "backport-to-v3-2-test" in result.output
        assert result.exit_code == 0

    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_backport_label_removed_on_version_branch_should_skip(
        self, mock_get_client, cli_runner, mock_github_setup
    ):
        """A backport label removed during the race window must take precedence
        over the merge-to-version-branch heuristic. Without this, a PR merged to
        a version branch would still get that branch's milestone even after a
        maintainer explicitly removed a different backport label, because the
        version-branch rule alone keeps producing a milestone.
        """
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        mock_issue.milestone = None
        # Snapshot had backport-to-v3-2-test on a PR merged to v3-1-test; the
        # maintainer removed the v3-2-test backport label inside the race window.
        mock_issue.labels = [_label("kind:bug")]
        mock_get_client.return_value = mock_gh

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "12345",
                "--pr-title",
                "Fix: scheduler issue",
                "--pr-labels",
                json.dumps(["backport-to-v3-2-test", "kind:bug"]),
                "--base-branch",
                "v3-1-test",
                "--merged-by",
                "testuser",
                "--github-token",
                "fake-token",
                "--github-repository",
                "apache/airflow",
            ],
        )

        mock_issue.edit.assert_not_called()
        mock_issue.create_comment.assert_not_called()
        assert "Labels changed since workflow snapshot" in result.output
        assert "backport labels removed during workflow window" in result.output
        assert "backport-to-v3-2-test" in result.output
        assert result.exit_code == 0

    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_skip_log_attributes_maintainer_who_unlabeled(
        self, mock_get_client, cli_runner, mock_github_setup
    ):
        """When the issue-events scan identifies a write-access user as the
        actor of the ``unlabeled`` event for the removed backport label, that
        login must appear in the skip log line so reviewers can audit who
        cancelled the auto-tag.
        """
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        mock_issue.milestone = None
        mock_issue.labels = [_label("kind:documentation")]
        mock_issue.get_events.return_value = [
            _unlabel_event(
                "backport-to-v3-2-test",
                "shahar1",
                datetime(2026, 5, 23, 20, 32, 17, tzinfo=timezone.utc),
            ),
        ]
        mock_repo.get_collaborator_permission.return_value = "write"
        mock_get_client.return_value = mock_gh

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "67301",
                "--pr-title",
                "fix: typo",
                "--pr-labels",
                json.dumps(["backport-to-v3-2-test", "kind:documentation"]),
                "--base-branch",
                "main",
                "--merged-by",
                "shahar1",
                "--github-token",
                "fake-token",
                "--github-repository",
                "apache/airflow",
            ],
        )

        mock_issue.edit.assert_not_called()
        mock_issue.create_comment.assert_not_called()
        mock_repo.get_collaborator_permission.assert_called_once_with("shahar1")
        plain = _plain_output(result.output)
        assert "by maintainer @shahar1" in plain
        assert "backport labels removed during workflow window" in plain
        assert result.exit_code == 0

    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_backport_removal_by_non_maintainer_should_fall_through(
        self, mock_get_client, cli_runner, mock_github_setup
    ):
        """If the issue-events scan identifies a user without write access
        (e.g. a bot or external contributor) as the ``unlabeled`` actor, the
        removal must NOT be treated as a maintainer signal. The action falls
        back to normal evaluation. On main with no current backport that
        means "no milestone to set" — still no edit, but for a different,
        non-attributed reason.
        """
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        mock_issue.milestone = None
        mock_issue.labels = [_label("kind:documentation")]
        mock_issue.get_events.return_value = [
            _unlabel_event(
                "backport-to-v3-2-test",
                "some-bot",
                datetime(2026, 5, 23, 20, 32, 17, tzinfo=timezone.utc),
            ),
        ]
        mock_repo.get_collaborator_permission.return_value = "read"
        mock_get_client.return_value = mock_gh

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "67301",
                "--pr-title",
                "fix: typo",
                "--pr-labels",
                json.dumps(["backport-to-v3-2-test", "kind:documentation"]),
                "--base-branch",
                "main",
                "--merged-by",
                "shahar1",
                "--github-token",
                "fake-token",
                "--github-repository",
                "apache/airflow",
            ],
        )

        mock_issue.edit.assert_not_called()
        plain = _plain_output(result.output)
        assert "Ignoring removal signal" in plain
        assert "@some-bot" in plain
        assert "No milestone to set after re-evaluation" in plain
        assert result.exit_code == 0

    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_backport_label_changed_after_snapshot_should_use_current(
        self, mock_get_client, cli_runner, mock_github_setup
    ):
        """If the backport label is replaced with a different version between
        snapshot and action (e.g. someone fixes the version target), the action
        must re-determine the version using the current label, not the stale one.
        """
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        mock_issue.milestone = None
        # Fresh state: now targets v3-2-test, not v3-1-test.
        mock_issue.labels = [_label("backport-to-v3-2-test"), _label("kind:bug")]
        mock_milestone = MagicMock()
        mock_milestone.title = "Airflow 3.2.3"
        mock_milestone.number = 140
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
                "Fix: scheduler issue",
                "--pr-labels",
                json.dumps(["backport-to-v3-1-test", "kind:bug"]),
                "--base-branch",
                "main",
                "--merged-by",
                "testuser",
                "--github-token",
                "fake-token",
                "--github-repository",
                "apache/airflow",
            ],
        )

        mock_issue.edit.assert_called_once_with(milestone=mock_milestone)
        assert "Labels changed since workflow snapshot" in result.output
        assert "Determination changed after re-read" in result.output
        assert "Airflow 3.2.3" in captured_comments[0]
        assert "backport label targeting v3-2-test" in captured_comments[0]
        assert result.exit_code == 0

    @patch("airflow_breeze.commands.ci_commands._get_github_client")
    def test_skip_label_added_after_snapshot_should_skip(
        self, mock_get_client, cli_runner, mock_github_setup
    ):
        """A skip label added after the snapshot must also halt the action."""
        from airflow_breeze.commands.ci_commands import ci_group

        mock_gh, mock_repo, mock_issue = mock_github_setup
        mock_issue.milestone = None
        # Snapshot had no skip label; fresh state added area:CI.
        mock_issue.labels = [_label("backport-to-v3-1-test"), _label("area:CI")]
        mock_get_client.return_value = mock_gh

        result = cli_runner.invoke(
            ci_group,
            [
                "set-milestone",
                "--pr-number",
                "12345",
                "--pr-title",
                "CI tweak",
                "--pr-labels",
                json.dumps(["backport-to-v3-1-test"]),
                "--base-branch",
                "main",
                "--merged-by",
                "testuser",
                "--github-token",
                "fake-token",
                "--github-repository",
                "apache/airflow",
            ],
        )

        mock_issue.edit.assert_not_called()
        mock_issue.create_comment.assert_not_called()
        assert "Skipping milestone tagging" in result.output
        assert "area:CI" in result.output
        assert result.exit_code == 0
