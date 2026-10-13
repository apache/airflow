#
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

import os
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import dag_processor_command

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

# The Dag processor resolves teams from the configured bundle partition, not the metadata DB, so
# that a fresh or reassigned deployment gets the right team before ``sync_bundles()`` has run.
BUNDLE_TEAMS = {
    "bundle_a": "team_a",
    "other_bundle_a": "team_a",
    "bundle_b": "team_b",
    "global_bundle": None,
}


@pytest.fixture
def dag_bundles_with_teams():
    with (
        conf_vars({("core", "multi_team"): "True"}),
        mock.patch.object(
            dag_processor_command, "_get_configured_bundle_team_names", return_value=dict(BUNDLE_TEAMS)
        ),
    ):
        yield


class TestDagProcessorCommand:
    """
    Tests the CLI interface and that it correctly calls the DagProcessor
    """

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @conf_vars({("core", "load_examples"): "False"})
    @mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner")
    def test_start_job(self, mock_runner):
        """Ensure that DagProcessorJobRunner is started"""
        mock_runner.return_value.job_type = "DagProcessorJob"
        args = self.parser.parse_args(["dag-processor"])
        dag_processor_command.dag_processor(args)
        mock_runner.return_value._execute.assert_called()

    @conf_vars({("core", "load_examples"): "False"})
    @mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner")
    def test_bundle_names_passed(self, mock_runner, configure_testing_dag_bundle):
        mock_runner.return_value.job_type = "DagProcessorJob"
        args = self.parser.parse_args(["dag-processor", "--bundle-name", "testing"])
        with configure_testing_dag_bundle(os.devnull):
            dag_processor_command.dag_processor(args)
        assert mock_runner.call_args.kwargs["processor"].bundle_names_to_parse == ["testing"]
        assert mock_runner.call_args.kwargs["job"].bundle_names == ["testing"]

    @pytest.mark.usefixtures("dag_bundles_with_teams")
    @pytest.mark.parametrize(
        ("bundle_names", "expected_team_names"),
        [
            pytest.param(None, ["team_a", "team_b"], id="every-configured-bundle"),
            pytest.param(["bundle_a"], ["team_a"], id="one-bundle-of-a-team"),
            pytest.param(["bundle_a", "other_bundle_a"], ["team_a"], id="several-bundles-of-one-team"),
            pytest.param(["bundle_a", "bundle_b"], ["team_a", "team_b"], id="bundles-of-several-teams"),
            pytest.param(["bundle_a", "global_bundle"], ["team_a"], id="bundles-of-a-team-and-of-no-team"),
            pytest.param(["global_bundle"], [], id="bundle-of-no-team"),
            pytest.param(["unknown_bundle"], [], id="unknown-bundle"),
        ],
    )
    def test_get_team_names(self, bundle_names, expected_team_names):
        assert dag_processor_command._get_team_names(bundle_names) == expected_team_names

    @conf_vars({("core", "multi_team"): "False"})
    @mock.patch.object(
        dag_processor_command, "_get_configured_bundle_team_names", return_value=dict(BUNDLE_TEAMS)
    )
    def test_get_team_names_returns_empty_outside_multi_team(self, mock_configured):
        assert dag_processor_command._get_team_names(["bundle_a"]) == []
        mock_configured.assert_not_called()

    @conf_vars({("core", "load_examples"): "False"})
    @mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner")
    @mock.patch("airflow.utils.cli.validate_dag_bundle_arg")
    @pytest.mark.usefixtures("dag_bundles_with_teams")
    def test_job_records_the_teams_owning_the_parsed_bundles(self, _, mock_runner):
        mock_runner.return_value.job_type = "DagProcessorJob"
        args = self.parser.parse_args(
            ["dag-processor", "--bundle-name", "bundle_a", "--bundle-name", "bundle_b"]
        )

        dag_processor_command.dag_processor(args)

        assert mock_runner.call_args.kwargs["job"].team_names == ["team_a", "team_b"]

    @conf_vars({("core", "load_examples"): "False"})
    @mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner")
    @mock.patch("airflow.utils.cli.DagBundlesManager", autospec=True)
    def test_bundle_validation_runs_with_server_context(self, mock_manager_cls, mock_runner, monkeypatch):
        mock_runner.return_value.job_type = "DagProcessorJob"
        captured_ctx = {}

        mock_bundle = mock.MagicMock()
        mock_bundle.name = "bundle1"

        def capture_env_and_return_bundles():
            captured_ctx["during_validation"] = os.environ.get("_AIRFLOW_PROCESS_CONTEXT")
            return [mock_bundle]

        mock_manager_cls.return_value.get_all_dag_bundles.side_effect = capture_env_and_return_bundles

        monkeypatch.delenv("_AIRFLOW_PROCESS_CONTEXT", raising=False)
        args = self.parser.parse_args(["dag-processor", "--bundle-name", "bundle1"])
        dag_processor_command.dag_processor(args)

        assert captured_ctx["during_validation"] == "server"
        assert "_AIRFLOW_PROCESS_CONTEXT" not in os.environ

    @mock.patch("airflow.cli.hot_reload.run_with_reloader")
    def test_dag_processor_with_dev_flag(self, mock_reloader):
        """Ensure that dag-processor with --dev flag uses hot-reload"""
        args = self.parser.parse_args(["dag-processor", "--dev"])
        dag_processor_command.dag_processor(args)

        # Verify that run_with_reloader was called
        mock_reloader.assert_called_once()
        # The callback function should be callable
        assert callable(mock_reloader.call_args[0][0])

    @conf_vars(
        {("core", "load_examples"): "False", ("profiling", "memray_trace_components"): "dag_processor"}
    )
    @mock.patch("airflow.cli.commands.dag_processor_command.run_command_with_daemon_option")
    @mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner")
    def test_memray_traces_the_job_and_not_the_parent_process(self, mock_runner, mock_daemon_option):
        """The callback runs on the far side of the daemon fork, so the tracer has to start there."""
        mock_runner.return_value.job_type = "DagProcessorJob"
        memray = mock.MagicMock()

        with mock.patch.dict("sys.modules", {"memray": memray}):
            dag_processor_command.dag_processor(self.parser.parse_args(["dag-processor"]))
            memray.Tracker.assert_not_called()

            mock_daemon_option.call_args.kwargs["callback"]()
            memray.Tracker.assert_called_once()
