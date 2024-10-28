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

import datetime
import subprocess
from unittest import mock

import pytest

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.local_executor import LocalExecutor
from airflow.utils.state import State

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]


class TestLocalExecutor:
    TEST_SUCCESS_COMMANDS = 5

    def test_supports_pickling(self):
        assert not LocalExecutor.supports_pickling

    def test_supports_sentry(self):
        assert not LocalExecutor.supports_sentry

    def test_is_local_default_value(self):
        assert LocalExecutor.is_local

    def test_serve_logs_default_value(self):
        assert LocalExecutor.serve_logs

    @mock.patch("airflow.executors.local_executor.subprocess.check_call")
    def execution_parallelism_subprocess(self, mock_check_call, parallelism=0):
        success_command = ["airflow", "tasks", "run", "true", "some_parameter", "2020-10-07"]
        fail_command = ["airflow", "tasks", "run", "false", "task_id", "2020-10-07"]

        def fake_execute_command(command, close_fds=True):
            if command != success_command:
                raise subprocess.CalledProcessError(returncode=1, cmd=command)
            else:
                return 0

        mock_check_call.side_effect = fake_execute_command

        self._test_execute(parallelism, success_command, fail_command)

    @mock.patch("airflow.cli.commands.task_command.task_run")
    def execution_parallelism_fork(self, mock_run, parallelism=0):
        success_command = ["airflow", "tasks", "run", "success", "some_parameter", "2020-10-07"]
        fail_command = ["airflow", "tasks", "run", "failure", "some_parameter", "2020-10-07"]

        def fake_task_run(args):
            if args.dag_id != "success":
                raise AirflowException("Simulate failed task")

        mock_run.side_effect = fake_task_run

        self._test_execute(parallelism, success_command, fail_command)

    def _test_execute(self, parallelism, success_command, fail_command):
        executor = LocalExecutor(parallelism=parallelism)
        executor.start()

        success_key = "success {}"
        assert executor.result_queue.empty()

        execution_date = datetime.datetime.now()
        for i in range(self.TEST_SUCCESS_COMMANDS):
            key_id, command = success_key.format(i), success_command
            key = key_id, "fake_ti", execution_date, 0
            executor.running.add(key)
            executor.execute_async(key=key, command=command)

        fail_key = "fail", "fake_ti", execution_date, 0
        executor.running.add(fail_key)
        executor.execute_async(key=fail_key, command=fail_command)

        executor.end()
        # By that time Queues are already shutdown so we cannot check if they are empty
        assert len(executor.running) == 0

        for i in range(self.TEST_SUCCESS_COMMANDS):
            key_id = success_key.format(i)
            key = key_id, "fake_ti", execution_date, 0
            assert executor.event_buffer[key][0] == State.SUCCESS
        assert executor.event_buffer[fail_key][0] == State.FAILED

        expected = self.TEST_SUCCESS_COMMANDS + 1 if parallelism == 0 else parallelism
        assert executor.workers_used == expected

    def test_execution_subprocess_unlimited_parallelism(self):
        with mock.patch.object(
            settings, "EXECUTE_TASKS_NEW_PYTHON_INTERPRETER", new_callable=mock.PropertyMock
        ) as option:
            option.return_value = True
            self.execution_parallelism_subprocess(parallelism=0)

    def test_execution_subprocess_limited_parallelism(self):
        with mock.patch.object(
            settings, "EXECUTE_TASKS_NEW_PYTHON_INTERPRETER", new_callable=mock.PropertyMock
        ) as option:
            option.return_value = True
            self.execution_parallelism_subprocess(parallelism=2)

    @mock.patch.object(settings, "EXECUTE_TASKS_NEW_PYTHON_INTERPRETER", False)
    def test_execution_unlimited_parallelism_fork(self):
        self.execution_parallelism_fork(parallelism=0)

    @mock.patch.object(settings, "EXECUTE_TASKS_NEW_PYTHON_INTERPRETER", False)
    def test_execution_limited_parallelism_fork(self):
        self.execution_parallelism_fork(parallelism=2)

    @mock.patch("airflow.executors.local_executor.LocalExecutor.sync")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.trigger_tasks")
    @mock.patch("airflow.executors.base_executor.Stats.gauge")
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = LocalExecutor()
        executor.heartbeat()
        calls = [
            mock.call(
                "executor.open_slots", value=mock.ANY, tags={"status": "open", "name": "LocalExecutor"}
            ),
            mock.call(
                "executor.queued_tasks", value=mock.ANY, tags={"status": "queued", "name": "LocalExecutor"}
            ),
            mock.call(
                "executor.running_tasks", value=mock.ANY, tags={"status": "running", "name": "LocalExecutor"}
            ),
        ]
        mock_stats_gauge.assert_has_calls(calls)
