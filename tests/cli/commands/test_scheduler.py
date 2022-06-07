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
from unittest import mock

import pytest
from click.testing import CliRunner

from airflow.cli.commands import scheduler
from airflow.utils.serve_logs import serve_logs
from tests.test_utils.config import conf_vars


class TestSchedulerCommand:
    @classmethod
    def setup_class(cls):
        cls.runner = CliRunner()

    @pytest.mark.parametrize(
        'executor, expect_serve_logs',
        [
            ("CeleryExecutor", False),
            ("LocalExecutor", True),
            ("SequentialExecutor", True),
            ("KubernetesExecutor", False),
        ],
    )
    @mock.patch("airflow.jobs.scheduler_job.SchedulerJob")
    @mock.patch("airflow.cli.commands.scheduler.Process")
    def test_serve_logs_on_scheduler(
        self,
        mock_process,
        mock_scheduler_job,
        executor,
        expect_serve_logs,
    ):
        with conf_vars({("core", "executor"): executor}):
            response = self.runner.invoke(scheduler.scheduler)

            assert response.exit_code == 0

            if expect_serve_logs:
                mock_process.assert_called_once_with(target=serve_logs)
            else:
                mock_process.assert_not_called()

    @pytest.mark.parametrize(
        'executor',
        [
            "LocalExecutor",
            "SequentialExecutor",
        ],
    )
    @mock.patch("airflow.jobs.scheduler_job.SchedulerJob")
    @mock.patch("airflow.cli.commands.scheduler.Process")
    def test_skip_serve_logs(self, mock_process, mock_scheduler_job, executor):
        with conf_vars({("core", "executor"): executor}):
            response = self.runner.invoke(scheduler.scheduler, ['--skip-serve-logs'])

            assert response.exit_code == 0

            mock_process.assert_not_called()

    @pytest.mark.parametrize(
        'executor',
        [
            "LocalExecutor",
            "SequentialExecutor",
        ],
    )
    @mock.patch("airflow.jobs.scheduler_job.SchedulerJob")
    @mock.patch("airflow.cli.commands.scheduler.Process")
    def test_graceful_shutdown(self, mock_process, mock_scheduler_job, executor):
        with conf_vars({("core", "executor"): executor}):
            mock_scheduler_job.run.side_effect = Exception('Mock exception to trigger runtime error')
            try:
                response = self.runner.invoke(scheduler.scheduler)
            finally:
                assert response.exit_code == 0

                mock_process().terminate.assert_called()
