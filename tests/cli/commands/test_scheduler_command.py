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

from http.server import BaseHTTPRequestHandler
from unittest import mock
from unittest.mock import MagicMock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import scheduler_command
from airflow.utils.scheduler_health import HealthServer, serve_health_check
from airflow.utils.serve_logs import serve_logs
from tests.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test


class TestSchedulerCommand:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @pytest.mark.parametrize(
        "executor, expect_serve_logs",
        [
            ("CeleryExecutor", False),
            ("LocalExecutor", True),
            ("SequentialExecutor", True),
            ("KubernetesExecutor", False),
            ("LocalKubernetesExecutor", True),
        ],
    )
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_serve_logs_on_scheduler(
        self,
        mock_process,
        mock_scheduler_job,
        executor,
        expect_serve_logs,
    ):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])

        with conf_vars({("core", "executor"): executor}):
            scheduler_command.scheduler(args)
            if expect_serve_logs:
                mock_process.assert_has_calls([mock.call(target=serve_logs)])
            else:
                with pytest.raises(AssertionError):
                    mock_process.assert_has_calls([mock.call(target=serve_logs)])

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    @pytest.mark.parametrize("executor", ["LocalExecutor", "SequentialExecutor"])
    def test_skip_serve_logs(self, mock_process, mock_scheduler_job, executor):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler", "--skip-serve-logs"])
        with conf_vars({("core", "executor"): executor}):
            scheduler_command.scheduler(args)
            with pytest.raises(AssertionError):
                mock_process.assert_has_calls([mock.call(target=serve_logs)])

    @mock.patch("airflow.utils.db.check_and_run_migrations")
    @mock.patch("airflow.utils.db.synchronize_log_template")
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_check_migrations_is_false(self, mock_process, mock_scheduler_job, mock_log, mock_run_migration):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])
        with conf_vars({("database", "check_migrations"): "False"}):
            scheduler_command.scheduler(args)
            mock_run_migration.assert_not_called()
            mock_log.assert_called_once()

    @mock.patch("airflow.utils.db.check_and_run_migrations")
    @mock.patch("airflow.utils.db.synchronize_log_template")
    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_check_migrations_is_true(self, mock_process, mock_scheduler_job, mock_log, mock_run_migration):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])
        with conf_vars({("database", "check_migrations"): "True"}):
            scheduler_command.scheduler(args)
            mock_run_migration.assert_called_once()
            mock_log.assert_called_once()

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    @pytest.mark.parametrize("executor", ["LocalExecutor", "SequentialExecutor"])
    def test_graceful_shutdown(self, mock_process, mock_scheduler_job, executor):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])
        with conf_vars({("core", "executor"): executor}):
            mock_scheduler_job.run.side_effect = Exception("Mock exception to trigger runtime error")
            try:
                scheduler_command.scheduler(args)
            finally:
                mock_process().terminate.assert_called()

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_enable_scheduler_health(
        self,
        mock_process,
        mock_scheduler_job,
    ):
        with conf_vars({("scheduler", "enable_health_check"): "True"}):
            mock_scheduler_job.return_value.job_type = "SchedulerJob"
            args = self.parser.parse_args(["scheduler"])
            scheduler_command.scheduler(args)
            mock_process.assert_has_calls([mock.call(target=serve_health_check)])

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    def test_disable_scheduler_health(
        self,
        mock_process,
        mock_scheduler_job,
    ):
        mock_scheduler_job.return_value.job_type = "SchedulerJob"
        args = self.parser.parse_args(["scheduler"])
        scheduler_command.scheduler(args)
        with pytest.raises(AssertionError):
            mock_process.assert_has_calls([mock.call(target=serve_health_check)])

    @mock.patch("airflow.cli.commands.scheduler_command.SchedulerJobRunner")
    @mock.patch("airflow.cli.commands.scheduler_command.Process")
    @mock.patch("airflow.cli.commands.scheduler_command.run_job", side_effect=Exception("run_job failed"))
    @mock.patch("airflow.cli.commands.scheduler_command.log")
    def test_run_job_exception_handling(
        self,
        mock_log,
        mock_run_job,
        mock_process,
        mock_scheduler_job,
    ):
        args = self.parser.parse_args(["scheduler"])
        scheduler_command.scheduler(args)

        # Make sure that run_job is called, that the exception has been logged, and that the serve_logs
        # sub-process has been terminated
        mock_run_job.assert_called_once_with(
            job=mock_scheduler_job().job,
            execute_callable=mock_scheduler_job()._execute,
        )
        mock_log.exception.assert_called_once_with("Exception when running scheduler job")
        mock_process.assert_called_once_with(target=serve_logs)
        mock_process().terminate.assert_called_once_with()


# Creating MockServer subclass of the HealthServer handler so that we can test the do_GET logic
class MockServer(HealthServer):
    def __init__(self):
        # Overriding so we don't need to initialize with BaseHTTPRequestHandler.__init__ params
        pass

    def do_GET(self, path):
        self.path = path
        super().do_GET()


class TestSchedulerHealthServer:
    def setup_method(self) -> None:
        self.mock_server = MockServer()

    @mock.patch.object(BaseHTTPRequestHandler, "send_error")
    def test_incorrect_endpoint(self, mock_send_error):
        self.mock_server.do_GET("/incorrect")
        mock_send_error.assert_called_with(404)

    @mock.patch.object(BaseHTTPRequestHandler, "end_headers")
    @mock.patch.object(BaseHTTPRequestHandler, "send_response")
    @mock.patch("airflow.utils.scheduler_health.create_session")
    def test_healthy_scheduler(self, mock_session, mock_send_response, mock_end_headers):
        mock_scheduler_job = MagicMock()
        mock_scheduler_job.is_alive.return_value = True
        mock_session.return_value.__enter__.return_value.query.return_value = mock_scheduler_job
        self.mock_server.do_GET("/health")
        mock_send_response.assert_called_once_with(200)
        mock_end_headers.assert_called_once()

    @mock.patch.object(BaseHTTPRequestHandler, "send_error")
    @mock.patch("airflow.utils.scheduler_health.create_session")
    def test_unhealthy_scheduler(self, mock_session, mock_send_error):
        mock_scheduler_job = MagicMock()
        mock_scheduler_job.is_alive.return_value = False
        mock_session.return_value.__enter__.return_value.query.return_value = mock_scheduler_job
        self.mock_server.do_GET("/health")
        mock_send_error.assert_called_with(503)

    @mock.patch.object(BaseHTTPRequestHandler, "send_error")
    @mock.patch("airflow.utils.scheduler_health.create_session")
    def test_missing_scheduler(self, mock_session, mock_send_error):
        mock_session.return_value.__enter__.return_value.query.return_value = None
        self.mock_server.do_GET("/health")
        mock_send_error.assert_called_with(503)
