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

    def test_serve_logs_default_value(self):
        assert SequentialExecutor.serve_logs

    @mock.patch("airflow.executors.sequential_executor.SequentialExecutor.sync")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.trigger_tasks")
    @mock.patch("airflow.executors.base_executor.Stats.gauge")
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = SequentialExecutor()
        executor.heartbeat()
        calls = [
            mock.call("executor.open_slots", mock.ANY),
            mock.call("executor.queued_tasks", mock.ANY),
            mock.call("executor.running_tasks", mock.ANY),
        ]
        mock_stats_gauge.assert_has_calls(calls)
