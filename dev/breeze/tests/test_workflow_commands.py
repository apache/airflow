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
import subprocess
from subprocess import CompletedProcess
from unittest.mock import MagicMock, call, patch

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.workflow_commands import (
    find_providers_missing_from_site_index,
    get_lines_added_to_site_index,
    workflow_run_publish,
    workflow_run_sync_staging_to_main,
)
from airflow_breeze.utils.confirm import Answer


def _make_gh_response(ref: str | None) -> CompletedProcess:
    """Return a fake CompletedProcess whose stdout mimics a gh api response."""
    body = {"ref": ref} if ref else {"message": "Not Found"}
    return MagicMock(spec=CompletedProcess, stdout=json.dumps(body).encode())


class TestPublishDocsTagValidation:
    """Tests for the ref validation logic in `breeze workflow-run publish-docs`."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def _invoke(self, runner: CliRunner, ref: str, extra_args: list[str] | None = None) -> object:
        args = ["--ref", ref, "--site-env", "staging", "apache-airflow"]
        if extra_args:
            args = extra_args + args
        return runner.invoke(workflow_run_publish, args, catch_exceptions=False)

    @patch("airflow_breeze.commands.workflow_commands.run_gh_command")
    def test_ref_is_valid_tag_passes_validation(self, mock_run_gh_command, runner):
        """When the ref exists as a tag, validation passes and the workflow is triggered."""
        tag_response = _make_gh_response("refs/tags/providers/2026-04-26")
        mock_run_gh_command.return_value = tag_response

        with patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor") as mock_trigger:
            result = self._invoke(runner, "providers/2026-04-26")

        assert result.exit_code == 0
        mock_trigger.assert_called()
        # Only one gh api call (tag check), no branch check needed
        mock_run_gh_command.assert_called_once()

    @patch("airflow_breeze.commands.workflow_commands.run_gh_command")
    def test_ref_is_branch_shows_actionable_message(self, mock_run_gh_command, runner):
        """When the ref exists as a branch but not a tag, the error names the branch
        and points to --skip-tag-validation."""
        tag_response = _make_gh_response(None)
        branch_response = _make_gh_response("refs/heads/main")
        mock_run_gh_command.side_effect = [tag_response, branch_response]

        result = self._invoke(runner, "main")

        assert result.exit_code == 1
        assert "exists as a branch but not as a tag" in result.output
        assert "--skip-tag-validation" in result.output
        # Both tag and branch checks must have been made
        assert mock_run_gh_command.call_count == 2
        calls = mock_run_gh_command.call_args_list
        assert "refs/tags/main" in calls[0][0][0][2]
        assert "refs/heads/main" in calls[1][0][0][2]

    @patch("airflow_breeze.commands.workflow_commands.run_gh_command")
    def test_ref_not_found_anywhere_shows_generic_message(self, mock_run_gh_command, runner):
        """When the ref exists neither as a tag nor a branch, a generic error is shown."""
        mock_run_gh_command.return_value = _make_gh_response(None)

        result = self._invoke(runner, "nonexistent-ref-xyz")

        assert result.exit_code == 1
        assert "does not exist as a tag or branch" in result.output
        assert "--skip-tag-validation" in result.output
        assert mock_run_gh_command.call_count == 2

    @patch("airflow_breeze.commands.workflow_commands.run_gh_command")
    def test_skip_tag_validation_bypasses_checks(self, mock_run_gh_command, runner):
        """With --skip-tag-validation, no gh api calls are made and the workflow proceeds."""
        with patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor") as mock_trigger:
            result = self._invoke(runner, "some-branch", extra_args=["--skip-tag-validation"])

        assert result.exit_code == 0
        mock_run_gh_command.assert_not_called()
        mock_trigger.assert_called()


def _site_index_with(*long_package_names: str) -> str:
    return "\n".join(
        f'  <li><a href="/docs/{name}/stable/index.html"><code>{name}</code></a></li>'
        for name in long_package_names
    )


def _make_stdout_response(stdout: str) -> CompletedProcess:
    return MagicMock(spec=CompletedProcess, returncode=0, stdout=stdout.encode())


SITE_INDEX_PATH = "landing-pages/site/content/en/docs/_index.md"
SITE_INDEX_PR_URL = "https://github.com/apache/airflow-site/pull/1690"


def _make_open_prs_response(*prs: tuple[int, str]) -> CompletedProcess:
    return _make_stdout_response(
        json.dumps(
            [
                {
                    "number": number,
                    "url": f"https://github.com/apache/airflow-site/pull/{number}",
                    "files": [{"path": path}],
                }
                for number, path in prs
            ]
        )
    )


def _make_diff(path: str, *added_long_package_names: str) -> str:
    added = _site_index_with(*added_long_package_names).splitlines()
    return "\n".join(
        [
            f"diff --git a/{path} b/{path}",
            f"--- a/{path}",
            f"+++ b/{path}",
            *(f"+{line}" for line in added),
            " unchanged",
        ]
    )


def _make_pr_diff_response(*added_long_package_names: str) -> CompletedProcess:
    return _make_stdout_response(_make_diff(SITE_INDEX_PATH, *added_long_package_names))


class TestPublishDocsSiteIndexCheck:
    """Tests for the airflow-site docs index check in `breeze workflow-run publish-docs`."""

    def _invoke(self, site_env: str, doc_packages: list[str], extra_args: list[str] | None = None):
        args = ["--ref", "providers/2026-10-13", "--site-env", site_env, *(extra_args or []), *doc_packages]
        return CliRunner().invoke(workflow_run_publish, args, catch_exceptions=False)

    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.run_gh_command", autospec=True)
    def test_live_publish_fails_when_provider_missing_from_index_and_prs(
        self, mock_run_gh_command, mock_trigger
    ):
        mock_run_gh_command.side_effect = [
            _make_gh_response("refs/tags/providers/2026-10-13"),
            _make_stdout_response(_site_index_with("apache-airflow-providers-amazon")),
            _make_open_prs_response((1690, SITE_INDEX_PATH), (1638, "README.md")),
            _make_pr_diff_response("apache-airflow-providers-modal"),
        ]

        result = self._invoke("live", ["amazon", "ibm.db2"])

        assert result.exit_code == 1
        assert "apache-airflow-providers-ibm-db2" in result.output
        assert "apache-airflow-providers-amazon" not in result.output
        assert "--skip-site-index-check" in result.output
        # Only the PR touching the index is diffed.
        assert mock_run_gh_command.call_count == 4
        assert mock_run_gh_command.call_args_list[3].args[0][3] == "1690"
        mock_trigger.assert_not_called()

    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.run_gh_command", autospec=True)
    def test_live_publish_proceeds_when_providers_are_in_index(self, mock_run_gh_command, mock_trigger):
        mock_run_gh_command.side_effect = [
            _make_gh_response("refs/tags/providers/2026-10-13"),
            _make_stdout_response(
                _site_index_with("apache-airflow-providers-amazon", "apache-airflow-providers-ibm-db2")
            ),
        ]

        result = self._invoke("live", ["amazon", "ibm.db2"])

        assert result.exit_code == 0
        assert (
            f"apache/airflow-site/contents/{SITE_INDEX_PATH}?ref=main"
            in (mock_run_gh_command.call_args_list[1].args[0][-1])
        )
        assert "Now merge the PRs" not in result.output
        mock_trigger.assert_called()

    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.run_gh_command", autospec=True)
    def test_live_publish_proceeds_and_asks_to_merge_open_index_pr(self, mock_run_gh_command, mock_trigger):
        mock_run_gh_command.side_effect = [
            _make_gh_response("refs/tags/providers/2026-10-13"),
            _make_stdout_response(_site_index_with("apache-airflow-providers-amazon")),
            _make_open_prs_response((1690, SITE_INDEX_PATH)),
            _make_pr_diff_response("apache-airflow-providers-ibm-db2"),
        ]

        result = self._invoke("live", ["amazon", "ibm.db2"])

        assert result.exit_code == 0
        mock_trigger.assert_called()
        assert "Now merge the PRs" in result.output
        assert SITE_INDEX_PR_URL in result.output

    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.run_gh_command", autospec=True)
    def test_live_publish_fails_when_index_cannot_be_read(self, mock_run_gh_command, mock_trigger):
        mock_run_gh_command.side_effect = [
            _make_gh_response("refs/tags/providers/2026-10-13"),
            subprocess.CalledProcessError(1, ["gh"]),
        ]

        result = self._invoke("live", ["amazon"])

        assert result.exit_code == 1
        assert "Could not check the docs index" in result.output
        mock_trigger.assert_not_called()

    @pytest.mark.parametrize(
        ("site_env", "doc_packages", "extra_args"),
        [
            pytest.param("staging", ["ibm.db2"], [], id="staging"),
            pytest.param("live", ["ibm.db2"], ["--skip-site-index-check"], id="skip-flag"),
            pytest.param("live", ["apache-airflow"], [], id="no-providers"),
        ],
    )
    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.run_gh_command", autospec=True)
    def test_index_is_not_checked(
        self, mock_run_gh_command, mock_trigger, site_env, doc_packages, extra_args
    ):
        mock_run_gh_command.return_value = _make_gh_response("refs/tags/providers/2026-10-13")

        result = self._invoke(site_env, doc_packages, extra_args)

        assert result.exit_code == 0
        mock_run_gh_command.assert_called_once()
        mock_trigger.assert_called()

    @patch("airflow_breeze.commands.workflow_commands.get_dry_run", autospec=True, return_value=True)
    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.run_gh_command", autospec=True)
    def test_index_is_not_checked_in_dry_run(self, mock_run_gh_command, mock_trigger, mock_get_dry_run):
        result = self._invoke("live", ["ibm.db2"], ["--skip-tag-validation"])

        assert result.exit_code == 0
        assert "Skipping the docs index check in dry-run mode" in result.output
        mock_run_gh_command.assert_not_called()
        mock_trigger.assert_called()


class TestFindProvidersMissingFromSiteIndex:
    @pytest.mark.parametrize(
        ("doc_packages", "exclude_docs", "expected"),
        [
            pytest.param(("amazon", "ibm.db2"), "no-docs-excluded", ["ibm.db2"], id="missing"),
            pytest.param(("ibm.db2", "modal"), "ibm.db2 modal", [], id="excluded"),
            # The S3 upload splits exclusions on spaces only, so it still publishes both.
            pytest.param(("ibm.db2", "modal"), "ibm.db2,modal", ["ibm.db2", "modal"], id="excluded-comma"),
            pytest.param(("common.dataquality",), "no-docs-excluded", [], id="not-ready"),
            pytest.param(("apache-airflow", "amazon"), "no-docs-excluded", [], id="non-provider"),
        ],
    )
    def test_find_missing(self, doc_packages, exclude_docs, expected):
        site_index = _site_index_with("apache-airflow-providers-amazon")

        assert find_providers_missing_from_site_index(doc_packages, exclude_docs, site_index) == expected

    @pytest.mark.parametrize("all_packages", ["all-providers", "all"])
    def test_all_providers_is_expanded(self, all_packages):
        missing = find_providers_missing_from_site_index(
            (all_packages,), "no-docs-excluded", _site_index_with("apache-airflow-providers-amazon")
        )

        assert "ibm.db2" in missing
        assert "amazon" not in missing
        assert "common.dataquality" not in missing


def test_only_lines_added_to_site_index_are_returned():
    diff = "\n".join(
        [
            _make_diff("README.md", "apache-airflow-providers-modal"),
            _make_diff(SITE_INDEX_PATH, "apache-airflow-providers-ibm-db2"),
        ]
    )

    added = "\n".join(get_lines_added_to_site_index(diff))

    assert "apache-airflow-providers-ibm-db2" in added
    assert "apache-airflow-providers-modal" not in added


class TestSyncStagingToMain:
    @pytest.mark.parametrize(
        ("answer", "expected_exit_code", "expected_triggered"),
        [
            pytest.param("y", 0, True, id="confirmed"),
            pytest.param("n", 0, False, id="skipped"),
            pytest.param("q", 1, False, id="quit"),
        ],
    )
    @patch("airflow_breeze.commands.workflow_commands.get_staging_only_commits", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor")
    def test_reset_staging_is_triggered_only_when_confirmed(
        self, mock_trigger, mock_get_commits, answer, expected_exit_code, expected_triggered
    ):
        mock_get_commits.return_value = []
        result = CliRunner().invoke(
            workflow_run_sync_staging_to_main, ["--answer", answer], catch_exceptions=False
        )

        assert result.exit_code == expected_exit_code
        if expected_triggered:
            assert mock_trigger.call_args_list == [
                call(workflow_name="reset-staging.yml", repo="apache/airflow-site", branch="main"),
                call(workflow_name="reset-staging.yml", repo="apache/airflow-site-archive", branch="main"),
            ]
        else:
            mock_trigger.assert_not_called()

    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.user_confirm", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.get_staging_only_commits", autospec=True)
    def test_staging_only_commits_are_listed_and_counted(self, mock_get_commits, mock_confirm, mock_trigger):
        mock_get_commits.side_effect = [
            ["abc1234 2026-09-30 Add staged docs", "def5678 2026-10-01 Fix index"],
            [],
        ]
        mock_confirm.return_value = Answer.YES

        result = CliRunner().invoke(workflow_run_sync_staging_to_main, [], catch_exceptions=False)

        assert result.exit_code == 0
        # rich highlights digits, so compare against the output with the ANSI codes removed
        plain_output = re.sub(r"\x1b\[[0-9;]*m", "", result.output)
        assert "abc1234 2026-09-30 Add staged docs" in plain_output
        assert "def5678 2026-10-01 Fix index" in plain_output
        assert "2 commit(s) only on staging will be dropped" in mock_confirm.call_args.args[0]
        assert mock_trigger.call_count == 2

    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.user_confirm", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.get_staging_only_commits", autospec=True)
    def test_no_deviation_output_when_staging_is_not_ahead(
        self, mock_get_commits, mock_confirm, mock_trigger
    ):
        mock_get_commits.return_value = []
        mock_confirm.return_value = Answer.YES

        result = CliRunner().invoke(workflow_run_sync_staging_to_main, [], catch_exceptions=False)

        assert result.exit_code == 0
        assert "will be dropped:" not in result.output
        assert "commit(s)" not in mock_confirm.call_args.args[0]
        assert mock_trigger.call_count == 2

    @patch("airflow_breeze.commands.workflow_commands.trigger_workflow_and_monitor", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.user_confirm", autospec=True)
    @patch("airflow_breeze.commands.workflow_commands.get_staging_only_commits", autospec=True)
    def test_compare_failure_falls_back_to_plain_prompt(self, mock_get_commits, mock_confirm, mock_trigger):
        mock_get_commits.side_effect = subprocess.CalledProcessError(1, ["gh"])
        mock_confirm.return_value = Answer.YES

        result = CliRunner().invoke(workflow_run_sync_staging_to_main, [], catch_exceptions=False)

        assert result.exit_code == 0
        assert "Could not compare staging to main in apache/airflow-site" in result.output
        assert "commit(s)" not in mock_confirm.call_args.args[0]
        assert mock_trigger.call_count == 2
