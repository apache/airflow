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

from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import celery_command
from tests.test_utils.config import conf_vars


@pytest.mark.integration("redis")
@pytest.mark.integration("rabbitmq")
@pytest.mark.backend("mysql", "postgres")
class TestWorkerServeLogs:
    @classmethod
    def setup_class(cls):
        cls.parser = cli_parser.get_parser()

    @mock.patch("airflow.cli.commands.celery_command.celery_app")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_serve_logs_on_worker_start(self, mock_celery_app):
        with mock.patch("airflow.cli.commands.celery_command.Process") as mock_process:
            args = self.parser.parse_args(["celery", "worker", "--concurrency", "1"])

            with mock.patch("celery.platforms.check_privileges") as mock_privil:
                mock_privil.return_value = 0
                celery_command.worker(args)
                mock_process.assert_called()

    @mock.patch("airflow.cli.commands.celery_command.celery_app")
    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_skip_serve_logs_on_worker_start(self, mock_celery_app):
        with mock.patch("airflow.cli.commands.celery_command.Process") as mock_popen:
            args = self.parser.parse_args(["celery", "worker", "--concurrency", "1", "--skip-serve-logs"])

            with mock.patch("celery.platforms.check_privileges") as mock_privil:
                mock_privil.return_value = 0
                celery_command.worker(args)
                mock_popen.assert_not_called()
