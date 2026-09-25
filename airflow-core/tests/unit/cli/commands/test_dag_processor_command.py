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
import signal
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import dag_processor_command

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def restore_command_signal_handlers():
    """CLI actions install process-wide handlers; do not leak them into executor tests."""
    handlers = {sig: signal.getsignal(sig) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        yield
    finally:
        for sig, handler in handlers.items():
            signal.signal(sig, handler)


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

    @conf_vars({("core", "load_examples"): "False"})
    @mock.patch("airflow.dag_processing.executor_manager.ExecutorDagProcessor", autospec=True)
    @mock.patch("airflow.cli.commands.dag_processor_command.DagFileProcessorManager", autospec=True)
    @pytest.mark.parametrize("enabled", [True, False])
    def test_executor_parsing_is_explicit_opt_in(self, regular, experimental, enabled):
        args = self.parser.parse_args(
            ["dag-processor", "--num-runs", "1"] + (["--executor-parsing"] if enabled else [])
        )
        runner = dag_processor_command._create_dag_processor_job_runner(args)
        selected, unused = (experimental, regular) if enabled else (regular, experimental)
        selected.assert_called_once_with(max_runs=1, bundle_names_to_parse=None)
        unused.assert_not_called()
        assert runner.processor is selected.return_value

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
