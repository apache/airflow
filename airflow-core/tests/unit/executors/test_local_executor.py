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

import multiprocessing
import os
from unittest import mock

import pytest
from kgb import spy_on
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.executors import workloads
from airflow.executors.local_executor import LocalExecutor, _execute_work
from airflow.settings import Session
from airflow.utils.state import State

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

pytestmark = pytest.mark.db_test

# Runtime is fine, we just can't run the tests on macOS
skip_spawn_mp_start = pytest.mark.skipif(
    multiprocessing.get_context().get_start_method() == "spawn",
    reason="mock patching in test don't work with 'spawn' mode (default on macOS)",
)


class TestLocalExecutor:
    TEST_SUCCESS_COMMANDS = 5

    def test_sentry_integration(self):
        assert not LocalExecutor.sentry_integration

    def test_is_local_default_value(self):
        assert LocalExecutor.is_local

    def test_serve_logs_default_value(self):
        assert LocalExecutor.serve_logs

    @skip_spawn_mp_start
    @mock.patch("airflow.sdk.execution_time.supervisor.supervise")
    def test_execution(self, mock_supervise):
        success_tis = [
            workloads.TaskInstance(
                id=uuid7(),
                dag_version_id=uuid7(),
                task_id=f"success_{i}",
                dag_id="mydag",
                run_id="run1",
                try_number=1,
                state="queued",
                pool_slots=1,
                queue="default",
                priority_weight=1,
                map_index=-1,
                start_date=timezone.utcnow(),
            )
            for i in range(self.TEST_SUCCESS_COMMANDS)
        ]
        fail_ti = success_tis[0].model_copy(update={"id": uuid7(), "task_id": "failure"})

        # We just mock both styles here, only one will be hit though
        def fake_supervise(ti, **kwargs):
            if ti.id == fail_ti.id:
                raise RuntimeError("fake failure")
            return 0

        mock_supervise.side_effect = fake_supervise

        executor = LocalExecutor(parallelism=2)
        executor.start()

        assert executor.result_queue.empty()

        with spy_on(executor._spawn_worker) as spawn_worker:
            for ti in success_tis:
                executor.queue_workload(
                    workloads.ExecuteTask(
                        token="",
                        ti=ti,
                        dag_rel_path="some/path",
                        log_path=None,
                        bundle_info=dict(name="hi", version="hi"),
                    ),
                    session=mock.MagicMock(spec=Session),
                )

            executor.queue_workload(
                workloads.ExecuteTask(
                    token="",
                    ti=fail_ti,
                    dag_rel_path="some/path",
                    log_path=None,
                    bundle_info=dict(name="hi", version="hi"),
                ),
                session=mock.MagicMock(spec=Session),
            )

            # Process queued workloads to trigger worker spawning
            executor._process_workloads(list(executor.queued_tasks.values()))

            executor.end()

            expected = 2
            # Depending on how quickly the tasks run, we might not need to create all the workers we could
            assert 1 <= len(spawn_worker.calls) <= expected

        # By that time Queues are already shutdown so we cannot check if they are empty
        assert len(executor.running) == 0
        assert executor._unread_messages.value == 0

        for ti in success_tis:
            assert executor.event_buffer[ti.key][0] == State.SUCCESS
        assert executor.event_buffer[fail_ti.key][0] == State.FAILED

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

    @skip_if_force_lowest_dependencies_marker
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

    @pytest.mark.parametrize(
        ("conf_values", "expected_server"),
        [
            (
                {
                    ("api", "base_url"): "http://test-server",
                    ("core", "execution_api_server_url"): None,
                },
                "http://test-server/execution/",
            ),
            (
                {
                    ("api", "base_url"): "http://test-server",
                    ("core", "execution_api_server_url"): "http://custom-server/execution/",
                },
                "http://custom-server/execution/",
            ),
            ({}, "http://localhost:8080/execution/"),
            ({("api", "base_url"): "/"}, "http://localhost:8080/execution/"),
            ({("api", "base_url"): "/airflow/"}, "http://localhost:8080/airflow/execution/"),
        ],
        ids=[
            "base_url_fallback",
            "custom_server",
            "no_base_url_no_custom",
            "base_url_no_custom",
            "relative_base_url",
        ],
    )
    @mock.patch("airflow.sdk.execution_time.supervisor.supervise")
    def test_execution_api_server_url_config(self, mock_supervise, conf_values, expected_server):
        """Test that execution_api_server_url is correctly configured with fallback"""
        with conf_vars(conf_values):
            _execute_work(log=mock.ANY, workload=mock.MagicMock())

            mock_supervise.assert_called_with(
                ti=mock.ANY,
                dag_rel_path=mock.ANY,
                bundle_info=mock.ANY,
                token=mock.ANY,
                server=expected_server,
                log_path=mock.ANY,
            )
