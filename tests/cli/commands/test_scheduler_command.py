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
import contextlib
import io
import os
import unittest
from unittest import mock

from parameterized import parameterized

from airflow.cli import cli_parser
from airflow.cli.commands import scheduler_command
from airflow.utils.serve_logs import serve_logs
from tests.test_utils.config import conf_vars


class TestSchedulerCommand(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.parser = cli_parser.get_parser()

    @parameterized.expand(
        [
            ("CeleryExecutor", False),
            ("LocalExecutor", True),
            ("SequentialExecutor", True),
            ("KubernetesExecutor", False),
        ]
    )
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJob")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_serve_logs_on_scheduler(
        self,
        executor,
        expect_serve_logs,
        mock_process,
        mock_scheduler_job,
    ):
        args = self.parser.parse_args(['scheduler'])

        with conf_vars({("core", "executor"): executor}):
            scheduler_command.scheduler(args)
            if expect_serve_logs:
                mock_process.assert_called_once_with(target=serve_logs)
            else:
                mock_process.assert_not_called()

    @parameterized.expand(
        [
            ("LocalExecutor",),
            ("SequentialExecutor",),
        ]
    )
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJob")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_skip_serve_logs(self, executor, mock_process, mock_scheduler_job):
        args = self.parser.parse_args(['scheduler', '--skip-serve-logs'])
        with conf_vars({("core", "executor"): executor}):
            scheduler_command.scheduler(args)
            mock_process.assert_not_called()

    @parameterized.expand(
        [
            ("LocalExecutor",),
            ("SequentialExecutor",),
        ]
    )
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJob")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_graceful_shutdown(self, executor, mock_process, mock_scheduler_job):
        args = self.parser.parse_args(['scheduler'])
        with conf_vars({("core", "executor"): executor}):
            mock_scheduler_job.run.side_effect = Exception('Mock exception to trigger runtime error')
            try:
                scheduler_command.scheduler(args)
            finally:
                mock_process().terminate.assert_called()

    @mock.patch.dict(os.environ, {'AIRFLOW__CORE__PARALLELISM': '35'}, clear=True)
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJob")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_scheduler_print_config(self, mock_process, mock_scheduler_job):
        args = self.parser.parse_args(['scheduler'])
        with conf_vars({("core", "sql_alchemy_conn_cmd"): 'echo hello'}):
            with contextlib.redirect_stdout(io.StringIO()) as temp_stdout:
                scheduler_command.scheduler(args)
                output = temp_stdout.getvalue()

                assert "sql_alchemy_conn = < hidden > [cmd]" in output
                assert "max_active_tasks_per_dag = 16 [airflow.cfg]" in output
                assert "parallelism = < hidden > [env var]" in output
                assert "max_active_runs_per_dag = 16 [default]" not in output
