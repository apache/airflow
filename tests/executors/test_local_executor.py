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
import multiprocessing
import os
import subprocess
from unittest import mock

import pytest
from kgb import spy_on

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.local_executor import LocalExecutor
from airflow.utils.state import State

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

# Runtime is fine, we just can't run the tests on macOS
skip_spawn_mp_start = pytest.mark.skipif(
    multiprocessing.get_context().get_start_method() == "spawn",
    reason="mock patching in test don't work with 'spawn' mode (default on macOS)",
)


class TestLocalExecutor:
    TEST_SUCCESS_COMMANDS = 5

    def test_supports_sentry(self):
        assert not LocalExecutor.supports_sentry

    def test_is_local_default_value(self):
        assert LocalExecutor.is_local

    def test_serve_logs_default_value(self):
        assert LocalExecutor.serve_logs

    @mock.patch("airflow.executors.local_executor.subprocess.check_call")
    @mock.patch("airflow.cli.commands.task_command.task_run")
    def _test_execute(self, mock_run, mock_check_call, parallelism=1):
        success_command = ["airflow", "tasks", "run", "success", "some_parameter", "2020-10-07"]
        fail_command = ["airflow", "tasks", "run", "failure", "task_id", "2020-10-07"]

        # We just mock both styles here, only one will be hit though
        def fake_execute_command(command, close_fds=True):
            if command != success_command:
                raise subprocess.CalledProcessError(returncode=1, cmd=command)
            else:
                return 0

        def fake_task_run(args):
            if args.dag_id != "success":
                raise AirflowException("Simulate failed task")

        mock_check_call.side_effect = fake_execute_command
        mock_run.side_effect = fake_task_run

        executor = LocalExecutor(parallelism=parallelism)
        executor.start()

        success_key = "success {}"
        assert executor.result_queue.empty()

        with spy_on(executor._spawn_worker) as spawn_worker:
            run_id = "manual_" + datetime.datetime.now().isoformat()
            for i in range(self.TEST_SUCCESS_COMMANDS):
                key_id, command = success_key.format(i), success_command
                key = key_id, "fake_ti", run_id, 0
                executor.running.add(key)
                executor.execute_async(key=key, command=command)

            fail_key = "fail", "fake_ti", run_id, 0
            executor.running.add(fail_key)
            executor.execute_async(key=fail_key, command=fail_command)

            executor.end()

            expected = self.TEST_SUCCESS_COMMANDS + 1 if parallelism == 0 else parallelism
            # Depending on how quickly the tasks run, we might not need to create all the workers we could
            assert 1 <= len(spawn_worker.calls) <= expected

        # By that time Queues are already shutdown so we cannot check if they are empty
        assert len(executor.running) == 0
        assert executor._unread_messages.value == 0

        for i in range(self.TEST_SUCCESS_COMMANDS):
            key_id = success_key.format(i)
            key = key_id, "fake_ti", run_id, 0
            assert executor.event_buffer[key][0] == State.SUCCESS
        assert executor.event_buffer[fail_key][0] == State.FAILED

    @skip_spawn_mp_start
    @pytest.mark.parametrize(
        ("parallelism", "fork_or_subproc"),
        [
            pytest.param(0, True, id="unlimited_subprocess"),
            pytest.param(2, True, id="limited_subprocess"),
            pytest.param(0, False, id="unlimited_fork"),
            pytest.param(2, False, id="limited_fork"),
        ],
    )
    def test_execution(self, parallelism: int, fork_or_subproc: bool, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setattr(settings, "EXECUTE_TASKS_NEW_PYTHON_INTERPRETER", fork_or_subproc)
        self._test_execute(parallelism=parallelism)

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

    @pytest.mark.execution_timeout(5)
    def test_clean_stop_on_signal(self):
        import signal

        executor = LocalExecutor(parallelism=2)
        executor.start()

        # We want to ensure we start a worker process, as we now only create them on demand
        executor._spawn_worker()

        try:
            os.kill(os.getpid(), signal.SIGINT)
        except KeyboardInterrupt:
            pass
        finally:
            executor.end()
