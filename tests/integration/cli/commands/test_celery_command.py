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

from importlib import reload
from unittest import mock

import pytest

from airflow.cli import cli_parser
from airflow.cli.commands import celery_command
from airflow.executors import executor_loader

from dev.tests_common.test_utils.config import conf_vars


@pytest.mark.integration("celery")
@pytest.mark.backend("postgres")
class TestWorkerServeLogs:
    @classmethod
    def setup_class(cls):
        with conf_vars({("core", "executor"): "CeleryExecutor"}):
            # The cli_parser module is loaded during test collection. Reload it here with the
            # executor overridden so that we get the expected commands loaded.
            reload(executor_loader)
            reload(cli_parser)
            cls.parser = cli_parser.get_parser()

    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_serve_logs_on_worker_start(self):
        with mock.patch("airflow.cli.commands.celery_command.Process") as mock_process, mock.patch(
            "airflow.providers.celery.executors.celery_executor.app"
        ):
            args = self.parser.parse_args(["celery", "worker", "--concurrency", "1"])

            with mock.patch("celery.platforms.check_privileges") as mock_privil:
                mock_privil.return_value = 0
                celery_command.worker(args)
                mock_process.assert_called()

    @conf_vars({("core", "executor"): "CeleryExecutor"})
    def test_skip_serve_logs_on_worker_start(self):
        with mock.patch("airflow.cli.commands.celery_command.Process") as mock_popen, mock.patch(
            "airflow.providers.celery.executors.celery_executor.app"
        ):
            args = self.parser.parse_args(["celery", "worker", "--concurrency", "1", "--skip-serve-logs"])

            with mock.patch("celery.platforms.check_privileges") as mock_privil:
                mock_privil.return_value = 0
                celery_command.worker(args)
                mock_popen.assert_not_called()
