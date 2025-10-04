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

import importlib
import os
from unittest import mock

from airflow.cli import cli_parser

from tests_common.test_utils.config import conf_vars


class TestDagProcessorCommand:
    """
    Tests the CLI interface and that it correctly calls the DagProcessor
    """

    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @conf_vars({("core", "load_examples"): "False"})
    def test_start_job(self):
        args = self.parser.parse_args(["dag-processor"])

        # Patch both decorators before importing the target function/module.
        # These decorators internally access the DB
        mock.patch("airflow.utils.cli.action_cli", lambda x: x).start()
        mock.patch(
            "airflow.cli.commands.dag_processor_command.providers_configuration_loaded", lambda x: x
        ).start()

        import airflow.cli.commands.dag_processor_command as dag_cmd

        importlib.reload(dag_cmd)

        with (
            mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner") as mock_runner,
            mock.patch(
                "airflow.cli.commands.dag_processor_command.run_command_with_daemon_option"
            ) as mock_run_cmd,
            mock.patch("airflow.cli.commands.dag_processor_command.run_job") as mock_run_job,
        ):
            # Arrange
            mock_execute = mock.Mock()
            mock_job = mock.Mock()
            mock_runner.return_value = mock.Mock(job=mock_job, _execute=mock_execute)

            def fake_run_command_with_daemon_option(*, args, process_name, callback, **kwargs):
                callback()

            mock_run_cmd.side_effect = fake_run_command_with_daemon_option
            mock_run_job.side_effect = lambda job, execute_callable: execute_callable()

            # Act
            dag_cmd.dag_processor(args)

            # Assert
            mock_execute.assert_called()

    @conf_vars({("core", "load_examples"): "False"})
    def test_bundle_names_passed(self, configure_testing_dag_bundle):
        mock.patch("airflow.utils.cli.action_cli", lambda x: x).start()
        mock.patch(
            "airflow.cli.commands.dag_processor_command.providers_configuration_loaded", lambda x: x
        ).start()

        import airflow.cli.commands.dag_processor_command as dag_cmd

        importlib.reload(dag_cmd)

        with (
            mock.patch("airflow.cli.commands.dag_processor_command.DagProcessorJobRunner") as mock_runner,
            mock.patch(
                "airflow.cli.commands.dag_processor_command.run_command_with_daemon_option"
            ) as mock_run_cmd,
            mock.patch("airflow.cli.commands.dag_processor_command.run_job") as mock_run_job,
        ):
            mock_runner.return_value.job_type = "DagProcessorJob"
            mock_runner.return_value._execute = mock.Mock()

            args = self.parser.parse_args(["dag-processor", "--bundle-name", "testing"])

            def fake_run_command_with_daemon_option(*, args, process_name, callback, **kwargs):
                callback()

            mock_run_cmd.side_effect = fake_run_command_with_daemon_option
            mock_run_job.side_effect = lambda job, execute_callable: execute_callable()

            # Act
            with configure_testing_dag_bundle(os.devnull):
                dag_cmd.dag_processor(args)

            # Assert
            assert mock_runner.call_args.kwargs["processor"].bundle_names_to_parse == ["testing"]
