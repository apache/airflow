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

import gc
import multiprocessing
import os
from unittest import mock

import pytest
from kgb import spy_on
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.executors import workloads
from airflow.executors.base_executor import BaseExecutor, ExecutorConf, get_execution_api_server_url
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.callback import CallbackDTO
from airflow.executors.workloads.task import TaskInstanceDTO
from airflow.models.callback import CallbackFetchMethod
from airflow.settings import Session
from airflow.utils.state import State

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

pytestmark = pytest.mark.db_test

# Mock patching doesn't work across process boundaries with 'spawn' (default on macOS)
# or 'forkserver' (default on Linux with Python 3.14+).
skip_non_fork_mp_start = pytest.mark.skipif(
    multiprocessing.get_start_method() != "fork",
    reason="mock patching in test doesn't work with non-fork multiprocessing start methods",
)

skip_fork_mp_start = pytest.mark.skipif(
    multiprocessing.get_start_method() == "fork",
    reason="tests non-fork (lazy-spawning) behavior",
)


def _make_task_workload():
    """Create a minimal ExecuteTask workload for tests."""
    return workloads.ExecuteTask(
        ti=TaskInstanceDTO(
            id=uuid7(),
            dag_version_id=uuid7(),
            task_id="test_task",
            dag_id="test_dag",
            run_id="test_run",
            try_number=1,
            pool_slots=1,
            queue="default",
            priority_weight=1,
        ),
        dag_rel_path="some/path",
        bundle_info=BundleInfo(name="test_bundle"),
        token="test_token",
        log_path=None,
    )


class TestLocalExecutor:
    """
    When the executor is started, end() must be called before the test finishes.
    Otherwise, subprocesses will remain running, preventing the test from terminating and causing a timeout.
    """

    TEST_SUCCESS_COMMANDS = 5

    def test_sentry_integration(self):
        assert not LocalExecutor.sentry_integration

    def test_is_local_default_value(self):
        assert LocalExecutor.is_local

    def test_supports_multi_team(self):
        assert LocalExecutor.supports_multi_team

    def test_serve_logs_default_value(self):
        assert LocalExecutor.serve_logs

    @skip_non_fork_mp_start
    @mock.patch.object(gc, "unfreeze")
    @mock.patch.object(gc, "freeze")
    def test_executor_worker_spawned(self, mock_freeze, mock_unfreeze):
        executor = LocalExecutor(parallelism=5)
        executor.start()

        mock_freeze.assert_called_once()
        mock_unfreeze.assert_called_once()

        assert len(executor.workers) == 5

        executor.end()

    @skip_fork_mp_start
    @mock.patch.object(gc, "unfreeze")
    @mock.patch.object(gc, "freeze")
    def test_executor_lazy_worker_spawning(self, mock_freeze, mock_unfreeze):
        """On non-fork start methods, workers are spawned lazily and gc.freeze is not called."""
        executor = LocalExecutor(parallelism=3)
        executor.start()

        try:
            # No workers should be pre-spawned
            assert len(executor.workers) == 0
            mock_freeze.assert_not_called()
            mock_unfreeze.assert_not_called()

            # Simulate a queued message so _check_workers spawns one worker on demand
            with executor._unread_messages:
                executor._unread_messages.value = 1
            executor.activity_queue.put(None)  # poison pill so the worker exits cleanly
            executor._check_workers()

            assert len(executor.workers) == 1
            # gc.freeze is still not used for non-fork
            mock_freeze.assert_not_called()
        finally:
            executor.end()

    @skip_non_fork_mp_start
    @mock.patch("airflow.executors.base_executor.BaseExecutor.run_workload")
    def test_execution(self, mock_run_workload):
        success_tis = [
            TaskInstanceDTO(
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
        has_failed_once = False

        def fake_run_workload(workload, **kwargs):
            nonlocal has_failed_once
            if workload.ti.id == fail_ti.id and not has_failed_once:
                has_failed_once = True
                raise RuntimeError("fake failure")
            return 0

        mock_run_workload.side_effect = fake_run_workload

        executor = LocalExecutor(parallelism=2)

        with spy_on(executor._spawn_worker) as spawn_worker:
            executor.start()

            assert executor.result_queue.empty()

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
    @pytest.mark.execution_timeout(30)
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
    @mock.patch("airflow.executors.base_executor.BaseExecutor.run_workload")
    def test_execution_api_server_url_config(self, mock_run_workload, conf_values, expected_server):
        """Test that execution_api_server_url is correctly configured with fallback"""

        with conf_vars(conf_values):
            team_conf = ExecutorConf(team_name=None)
            BaseExecutor.run_workload(_make_task_workload(), server=get_execution_api_server_url(team_conf))

            mock_run_workload.assert_called_once()
            assert mock_run_workload.call_args.kwargs["server"] == expected_server

    @mock.patch("airflow.executors.base_executor.BaseExecutor.run_workload")
    def test_team_and_global_config_isolation(self, mock_run_workload):
        """Test that team-specific and global executors use correct configurations side-by-side"""

        team_name = "ml_team"
        team_server = "http://team-ml-server:8080/execution/"
        default_server = "http://default-server/execution/"

        # Set up global configuration
        config_overrides = {
            ("api", "base_url"): "http://default-server",
            ("core", "execution_api_server_url"): default_server,
        }

        # Use environment variables for team-specific config
        import os

        team_env_key = f"AIRFLOW__{team_name.upper()}___CORE__EXECUTION_API_SERVER_URL"

        with mock.patch.dict(os.environ, {team_env_key: team_server}):
            with conf_vars(config_overrides):
                # Test team-specific config
                team_conf = ExecutorConf(team_name=team_name)
                BaseExecutor.run_workload(
                    _make_task_workload(), server=get_execution_api_server_url(team_conf)
                )

                # Verify team-specific server URL was used
                assert mock_run_workload.call_count == 1
                assert mock_run_workload.call_args.kwargs["server"] == team_server

                mock_run_workload.reset_mock()

                # Test global config (no team)
                global_conf = ExecutorConf(team_name=None)
                BaseExecutor.run_workload(
                    _make_task_workload(), server=get_execution_api_server_url(global_conf)
                )

                # Verify default server URL was used
                assert mock_run_workload.call_count == 1
                assert mock_run_workload.call_args.kwargs["server"] == default_server

    def test_multiple_team_executors_isolation(self):
        """Test that multiple team executors can coexist with isolated resources"""
        team_a_executor = LocalExecutor(parallelism=2, team_name="team_a")
        team_b_executor = LocalExecutor(parallelism=3, team_name="team_b")

        team_a_executor.start()
        team_b_executor.start()

        try:
            # Verify each executor has its own queues
            assert team_a_executor.activity_queue is not team_b_executor.activity_queue
            assert team_a_executor.result_queue is not team_b_executor.result_queue

            # Verify each executor has its own workers dict
            assert team_a_executor.workers is not team_b_executor.workers

            if LocalExecutor.is_mp_using_fork:
                # fork pre-spawns all workers at start()
                assert len(team_a_executor.workers) == 2
                assert len(team_b_executor.workers) == 3
            else:
                # forkserver/spawn use lazy spawning
                assert len(team_a_executor.workers) == 0
                assert len(team_b_executor.workers) == 0

            # Verify each executor has its own unread_messages counter
            assert team_a_executor._unread_messages is not team_b_executor._unread_messages

            # Verify each has correct team config
            assert team_a_executor.conf.team_name == "team_a"
            assert team_b_executor.conf.team_name == "team_b"

        finally:
            team_a_executor.end()
            team_b_executor.end()

    def test_global_executor_without_team_name(self):
        """Test that global executor (no team) works correctly"""
        executor = LocalExecutor(parallelism=2)

        # Verify executor has conf but no team name
        assert hasattr(executor, "conf")
        assert executor.conf.team_name is None

        executor.start()

        if LocalExecutor.is_mp_using_fork:
            assert len(executor.workers) == 2
        else:
            # forkserver/spawn use lazy spawning
            assert len(executor.workers) == 0

        executor.end()


class TestLocalExecutorCallbackSupport:
    CALLBACK_UUID = "12345678-1234-5678-1234-567812345678"

    def test_supports_callbacks_flag_is_true(self):
        executor = LocalExecutor()
        assert executor.supports_callbacks is True

    @skip_non_fork_mp_start
    def test_process_callback_workload_queue_management(self):
        """Test that _process_workloads correctly removes callbacks from queued_callbacks."""
        executor = LocalExecutor(parallelism=1)
        callback_data = CallbackDTO(
            id=self.CALLBACK_UUID,
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
            data={"path": "test.func", "kwargs": {}},
        )
        callback_workload = workloads.ExecuteCallback(
            callback=callback_data,
            dag_rel_path="test.py",
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            token="test_token",
            log_path="test.log",
        )

        executor.start()

        try:
            executor.queued_callbacks[callback_data.id] = callback_workload
            executor._process_workloads([callback_workload])
            assert len(executor.queued_callbacks) == 0
            # We can't easily verify worker execution without running the worker,
            # but we can verify the helper is called via mock

        finally:
            executor.end()

    @mock.patch("airflow.sdk.execution_time.callback_supervisor.supervise_callback", return_value=0)
    def test_execute_workload_calls_supervise_callback(self, mock_supervise_callback):
        callback_data = CallbackDTO(
            id=self.CALLBACK_UUID,
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
            data={"path": "test.module.my_callback", "kwargs": {"arg1": "val1"}},
        )
        callback_workload = workloads.ExecuteCallback(
            callback=callback_data,
            dag_rel_path="test.py",
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            token="test_token",
            log_path="test.log",
        )

        BaseExecutor.run_workload(callback_workload)

        mock_supervise_callback.assert_called_once_with(
            id=self.CALLBACK_UUID,
            callback_path="test.module.my_callback",
            callback_kwargs={"arg1": "val1"},
            log_path="test.log",
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
        )

    @mock.patch(
        "airflow.sdk.execution_time.callback_supervisor.supervise_callback",
        side_effect=RuntimeError("Callback subprocess exited with code 1"),
    )
    def test_execute_workload_raises_on_callback_failure(self, mock_supervise_callback):
        callback_data = CallbackDTO(
            id=self.CALLBACK_UUID,
            fetch_method=CallbackFetchMethod.IMPORT_PATH,
            data={"path": "test.module.my_callback", "kwargs": {}},
        )
        callback_workload = workloads.ExecuteCallback(
            callback=callback_data,
            dag_rel_path="test.py",
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            token="test_token",
            log_path="test.log",
        )

        with pytest.raises(RuntimeError, match="Callback subprocess exited with code 1"):
            BaseExecutor.run_workload(callback_workload)
