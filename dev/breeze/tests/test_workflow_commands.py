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
from subprocess import CompletedProcess
from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.workflow_commands import workflow_run_publish


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
