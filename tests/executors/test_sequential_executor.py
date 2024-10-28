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

from unittest import mock

from airflow.executors.sequential_executor import SequentialExecutor


class TestSequentialExecutor:
    def test_supports_pickling(self):
        assert not SequentialExecutor.supports_pickling

    def test_supports_sentry(self):
        assert not SequentialExecutor.supports_sentry

    def test_is_local_default_value(self):
        assert SequentialExecutor.is_local

    def test_is_production_default_value(self):
        assert not SequentialExecutor.is_production

    def test_serve_logs_default_value(self):
        assert SequentialExecutor.serve_logs

    def test_is_single_threaded_default_value(self):
        assert SequentialExecutor.is_single_threaded

    @mock.patch("airflow.executors.sequential_executor.SequentialExecutor.sync")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.trigger_tasks")
    @mock.patch("airflow.executors.base_executor.Stats.gauge")
    def test_gauge_executor_metrics(
        self, mock_stats_gauge, mock_trigger_tasks, mock_sync
    ):
        executor = SequentialExecutor()
        executor.heartbeat()
        calls = [
            mock.call(
                "executor.open_slots",
                value=mock.ANY,
                tags={"status": "open", "name": "SequentialExecutor"},
            ),
            mock.call(
                "executor.queued_tasks",
                value=mock.ANY,
                tags={"status": "queued", "name": "SequentialExecutor"},
            ),
            mock.call(
                "executor.running_tasks",
                value=mock.ANY,
                tags={"status": "running", "name": "SequentialExecutor"},
            ),
        ]
        mock_stats_gauge.assert_has_calls(calls)

    def test_execute_async(self):
        executor = SequentialExecutor()
        mock_key = mock.Mock()
        mock_command = mock.Mock()
        mock_queue = "mock_queue"
        mock_executor_config = mock.Mock()

        executor.validate_airflow_tasks_run_command = mock.Mock()
        executor.execute_async(mock_key, mock_command, mock_queue, mock_executor_config)

        executor.validate_airflow_tasks_run_command.assert_called_once_with(mock_command)
        assert executor.commands_to_run == [(mock_key, mock_command)]

    @mock.patch("airflow.executors.sequential_executor.subprocess")
    def test_sync(self, mock_subprocess):
        executor = SequentialExecutor()
        executor.commands_to_run = [("key", ["echo", "test_command"])]
        mock_subprocess.check_call.return_value = 0

        executor.success = mock.Mock()
        executor.sync()
        executor.success.assert_called_once_with("key")

        assert executor.commands_to_run == []

    def test_end(self):
        executor = SequentialExecutor()
        executor.heartbeat = mock.Mock()
        executor.end()

        executor.heartbeat.assert_called_once()
