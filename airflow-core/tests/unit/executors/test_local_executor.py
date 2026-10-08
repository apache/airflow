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
import signal
import time
from pathlib import Path
from unittest import mock

import pytest
from kgb import spy_on
from uuid6 import uuid7

import airflow.executors.local_executor as local_executor_module
from airflow._shared.timezones import timezone
from airflow.executors import workloads
from airflow.executors.base_executor import BaseExecutor, ExecutorConf, get_execution_api_server_url
from airflow.executors.local_executor import LocalExecutor, _run_worker
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.callback import CallbackDTO
from airflow.executors.workloads.task import TaskInstanceDTO
from airflow.executors.workloads.types import TaskInstanceUuid
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


class TestLocalExecutorMpStartMethod:
    @mock.patch("airflow.executors.local_executor.multiprocessing.get_start_method", autospec=True)
    def test_is_mp_using_fork_resolved_per_instance(self, mock_get_start_method):
        """``is_mp_using_fork`` is resolved at ``__init__`` (reflecting any configured start
        method) rather than once at import time."""
        mock_get_start_method.return_value = "fork"
        assert LocalExecutor(parallelism=1).is_mp_using_fork is True

        mock_get_start_method.return_value = "forkserver"
        assert LocalExecutor(parallelism=1).is_mp_using_fork is False


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


def _write_large_results_to_queue(result_queue, activity_queue, unread_messages, result_count, payload_size):
    payload = RuntimeError("x" * payload_size)
    for _ in range(result_count):
        workload = activity_queue.get()
        with unread_messages:
            unread_messages.value -= 1
        key = LocalExecutor.get_workload_key(workload)
        result_queue.put((os.getpid(), key, workload.running_state, None))
        result_queue.put((os.getpid(), key, State.SUCCESS, payload))


def _make_workload(kind):
    if kind == "task":
        return _make_task_workload()
    if kind == "callback":
        return workloads.ExecuteCallback(
            callback=CallbackDTO(
                id=uuid7(),
                fetch_method=CallbackFetchMethod.IMPORT_PATH,
                data={"path": "test.func", "kwargs": {}},
            ),
            dag_rel_path="test.py",
            bundle_info=BundleInfo(name="bundle"),
            token="token",
            log_path=None,
        )
    return workloads.TestConnection(
        connection_test_id=uuid7(), connection_id="test", timeout=10, token="token"
    )


def _hold_workload(workload, **kwargs):
    Path(workload.token).touch()
    signal.pause()


def _run_blocking_worker(**kwargs):
    with mock.patch.object(BaseExecutor, "run_workload", autospec=True, side_effect=_hold_workload):
        _run_worker(**kwargs)


def _add_mock_worker(executor, mocker, pid):
    proc = mocker.create_autospec(multiprocessing.Process, instance=True)
    proc.pid = pid
    proc.is_alive.return_value = True
    executor.workers[pid] = proc
    return proc


@pytest.fixture
def local_executor_with_mock_worker(mocker):
    mocker.patch.object(LocalExecutor, "_spawn_workers_with_gc_freeze", autospec=True)
    mocker.patch.object(LocalExecutor, "_spawn_worker", autospec=True)
    executor = LocalExecutor(parallelism=1)
    executor.start()
    proc = _add_mock_worker(executor, mocker, 12345)
    yield executor, proc
    executor.workers.clear()
    executor.end()


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
            executor._process_workloads(list(executor.executor_queues[WorkloadType.EXECUTE_TASK].values()))

            executor.end()

            expected = 2
            # Depending on how quickly the tasks run, we might not need to create all the workers we could
            assert 1 <= len(spawn_worker.calls) <= expected

        # By that time Queues are already shutdown so we cannot check if they are empty
        assert len(executor.running) == 0
        assert executor._unread_messages.value == 0

        for ti in success_tis:
            assert executor.event_buffer[TaskInstanceUuid(ti.id)][0] == State.SUCCESS
        assert executor.event_buffer[TaskInstanceUuid(fail_ti.id)][0] == State.FAILED

    @mock.patch("airflow.executors.local_executor.LocalExecutor.sync")
    @mock.patch("airflow.executors.base_executor.BaseExecutor.trigger_workloads")
    @mock.patch("airflow.executors.base_executor.stats.gauge")
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_workloads, mock_sync):
        executor = LocalExecutor()
        executor.heartbeat()
        calls = [
            mock.call(
                "executor.open_slots",
                value=mock.ANY,
                tags={"status": "open", "executor_class_name": "LocalExecutor"},
            ),
            mock.call(
                "executor.queued_tasks",
                value=mock.ANY,
                tags={"status": "queued", "executor_class_name": "LocalExecutor"},
            ),
            mock.call(
                "executor.running_tasks",
                value=mock.ANY,
                tags={"status": "running", "executor_class_name": "LocalExecutor"},
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

    def test_end_drains_results_while_joining_workers(self):
        executor = LocalExecutor(parallelism=1)
        executor.activity_queue = mock.MagicMock()
        executor.result_queue = mock.MagicMock()
        proc = mock.MagicMock(spec=multiprocessing.Process)
        proc.is_alive.side_effect = [True, True, True, False]
        executor.workers = {1: proc}

        with mock.patch.object(executor, "_read_results") as mock_read_results:
            executor.end()

        executor.activity_queue.put.assert_called_once_with(None)
        assert proc.join.call_args_list == [mock.call(timeout=0.05), mock.call(timeout=0.05)]
        assert mock_read_results.call_count == 3
        proc.close.assert_called_once()
        executor.activity_queue.close.assert_called_once()
        executor.result_queue.close.assert_called_once()

    def test_end_terminates_workers_and_closes_resources_on_interrupt(self):
        executor = LocalExecutor(parallelism=1)
        executor.activity_queue = mock.MagicMock()
        executor.result_queue = mock.MagicMock()
        proc = mock.MagicMock(spec=multiprocessing.Process)
        proc.is_alive.side_effect = [True, True]
        executor.workers = {1: proc}

        with (
            mock.patch.object(executor, "_read_results", side_effect=[KeyboardInterrupt, None]),
            mock.patch.object(executor, "_terminate_worker_process") as mock_terminate_worker_process,
            pytest.raises(KeyboardInterrupt),
        ):
            executor.end()

        mock_terminate_worker_process.assert_called_once_with(proc)
        proc.close.assert_called_once()
        executor.activity_queue.close.assert_called_once()
        executor.result_queue.close.assert_called_once()

    def test_terminate_joins_worker_after_sigterm(self):
        executor = LocalExecutor(parallelism=1)
        proc = mock.MagicMock(spec=multiprocessing.Process)
        proc.is_alive.side_effect = [True, False]
        executor.workers = {1: proc}

        executor.terminate()

        proc.terminate.assert_called_once_with()
        proc.join.assert_called_once_with(timeout=0.2)
        proc.kill.assert_not_called()

    def test_terminate_kills_worker_that_ignores_sigterm(self):
        executor = LocalExecutor(parallelism=1)
        proc = mock.MagicMock(spec=multiprocessing.Process)
        proc.pid = 123
        proc.is_alive.side_effect = [True, True]
        executor.workers = {1: proc}

        executor.terminate()

        proc.terminate.assert_called_once_with()
        proc.kill.assert_called_once_with()
        assert proc.join.call_args_list == [mock.call(timeout=0.2), mock.call(timeout=0.2)]

    @pytest.mark.execution_timeout(10)
    def test_end_drains_result_queue_to_avoid_join_deadlock(self, mocker):
        # Pin the worker to "fork": the drain logic under test is start-method-agnostic, but under the
        # "forkserver" default (Python 3.14+ on Linux) each spawned worker re-imports the whole airflow
        # stack before it can write a result, which intermittently exceeds the execution_timeout and
        # makes this test flaky. Forking inherits the already-imported parent, so the worker writes
        # immediately and reliably reproduces the full-result_queue scenario this test guards.
        ctx = multiprocessing.get_context("fork")
        executor = LocalExecutor(parallelism=1)
        mocker.patch.object(executor, "_spawn_workers_with_gc_freeze", autospec=True)
        executor.start()
        result_count = 8
        payload_size = 128 * 1024
        submitted = [_make_task_workload() for _ in range(result_count)]
        for workload in submitted:
            executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        with mock.patch.object(executor, "_check_workers", autospec=True):
            executor._process_workloads(submitted)
        proc = ctx.Process(
            target=_write_large_results_to_queue,
            args=(
                executor.result_queue,
                executor.activity_queue,
                executor._unread_messages,
                result_count,
                payload_size,
            ),
        )
        proc.start()
        executor.workers = {proc.pid: proc}

        executor.end()

        assert len(executor.event_buffer) == result_count
        assert set(executor.event_buffer) == {executor.get_task_key(workload.ti) for workload in submitted}
        assert all(state == State.SUCCESS for state, _ in executor.event_buffer.values())
        assert not executor.running
        assert not executor._worker_tasks
        assert executor._unread_messages.value == 0

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

            if team_a_executor.is_mp_using_fork:
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

        if executor.is_mp_using_fork:
            assert len(executor.workers) == 2
        else:
            # forkserver/spawn use lazy spawning
            assert len(executor.workers) == 0

        executor.end()


class TestLocalExecutorBookkeeping:
    def test_dispatch_keeps_task_visible_without_a_worker_result(self, mocker):
        mocker.patch.object(LocalExecutor, "_spawn_workers_with_gc_freeze", autospec=True)
        mocker.patch.object(LocalExecutor, "_check_workers", autospec=True)
        executor = LocalExecutor(parallelism=1)
        executor.start()
        workload = _make_task_workload()
        key = executor.get_task_key(workload.ti)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        try:
            executor.heartbeat()
            executor._drain_events_with_task_ids()

            assert key in executor.running
            assert executor.has_task(workload.ti)
            assert executor.slots_available == 0
            assert executor._task_coordinates[key] == workload.ti.key
        finally:
            executor.end()

    def test_running_limits_later_heartbeats_and_reports_metrics(
        self, local_executor_with_mock_worker, mocker
    ):
        executor, proc = local_executor_with_mock_worker
        gauge = mocker.patch("airflow.executors.base_executor.stats.gauge", autospec=True)
        first, second = _make_task_workload(), _make_task_workload()
        executor.queue_workload(first, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()
        assert executor.slots_available == 0
        executor.queue_workload(second, session=mock.create_autospec(Session, instance=True))

        executor.heartbeat()

        assert executor.running == {executor.get_task_key(first.ti)}
        assert executor._unread_messages.value == 1
        assert second in executor.executor_queues[second.type].values()
        assert executor.has_task(first.ti)
        metrics = {call.args[0]: call.kwargs["value"] for call in gauge.call_args_list[-3:]}
        assert metrics == {"executor.open_slots": 0, "executor.queued_tasks": 1, "executor.running_tasks": 1}

    @pytest.mark.parametrize("kind", ["task", "callback", "connection"])
    @pytest.mark.parametrize("succeeded", [True, False])
    def test_start_retains_slot_and_terminal_clears_pid(
        self, kind, succeeded, local_executor_with_mock_worker
    ):
        executor, proc = local_executor_with_mock_worker
        workload = _make_workload(kind)
        key = executor.get_workload_key(workload)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()

        executor.result_queue.put((proc.pid, key, workload.running_state, None))
        executor.sync()

        assert executor._worker_tasks == {proc.pid: key}
        assert key in executor.running
        assert executor.slots_available == 0
        if workload.running_state is None:
            assert key not in executor.event_buffer
        else:
            assert executor.event_buffer[key] == (workload.running_state, None)
        terminal = workload.success_state if succeeded else workload.failure_state
        executor.result_queue.put((proc.pid, key, terminal, None))
        executor.sync()
        assert executor.event_buffer[key] == (terminal, None)
        assert not executor._worker_tasks
        assert not executor._dispatch_counts
        assert executor.slots_available == 1

    def test_result_uses_original_submitted_uuid_after_dto_changes(self, local_executor_with_mock_worker):
        executor, proc = local_executor_with_mock_worker
        workload = _make_task_workload()
        key, coordinates = executor.get_task_key(workload.ti), workload.ti.key
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()
        submitted = executor.activity_queue.get()
        workload.ti.id = uuid7()
        workload.ti.try_number += 1
        assert executor.get_workload_key(submitted) == key
        executor.result_queue.put((proc.pid, key, None, None))
        executor.result_queue.put((proc.pid, key, workload.success_state, None))

        executor.sync()
        events, captured = executor._drain_events_with_task_ids()

        assert events == {key: (workload.success_state, None)}
        assert captured == {key: coordinates}
        assert executor.slots_available == 1

    def test_reaper_drains_start_sent_after_initial_poll(self, local_executor_with_mock_worker):
        executor, proc = local_executor_with_mock_worker
        workload = _make_task_workload()
        key = executor.get_task_key(workload.ti)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()
        executor.activity_queue.get()
        executor._unread_messages.value = 0

        def died_after_start():
            executor.result_queue.put((proc.pid, key, None, None))
            return False

        proc.is_alive.side_effect = died_after_start
        executor.sync()

        assert executor.event_buffer[key] == (workload.failure_state, None)
        assert not executor.running
        assert not executor._worker_tasks
        proc.close.assert_called_once()

    def test_revoke_task_releases_slot_of_workload_lost_before_start(self, local_executor_with_mock_worker):
        executor, proc = local_executor_with_mock_worker
        workload = _make_task_workload()
        key = executor.get_task_key(workload.ti)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()
        executor.activity_queue.get()
        executor._unread_messages.value = 0
        proc.is_alive.return_value = False
        executor.sync()
        assert not executor.workers
        assert key in executor.running

        executor.revoke_task(ti=workload.ti)

        assert not executor.running
        assert not executor._dispatch_counts
        assert executor.event_buffer == {}
        assert executor.slots_available == 1

    @pytest.mark.parametrize(
        ("stage", "worker_terminated"),
        [("queued", False), ("dispatched", False), ("started", True)],
    )
    def test_revoke_task_clears_workload_at_every_stage(
        self, stage, worker_terminated, local_executor_with_mock_worker
    ):
        executor, proc = local_executor_with_mock_worker
        workload = _make_task_workload()
        key = executor.get_task_key(workload.ti)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        if stage != "queued":
            executor.heartbeat()
        if stage == "started":
            executor.result_queue.put((proc.pid, key, None, None))
            executor.sync()
            assert executor._worker_tasks == {proc.pid: key}

        executor.revoke_task(ti=workload.ti)

        assert not executor.executor_queues[workload.type]
        assert not executor.running
        assert not executor._worker_tasks
        assert not executor._dispatch_counts
        assert executor.event_buffer == {}
        assert proc.terminate.called is worker_terminated

    @pytest.mark.parametrize("kind", ["task", "connection"])
    def test_external_timeout_clears_pid_and_rejects_late_results(
        self, kind, local_executor_with_mock_worker
    ):
        executor, proc = local_executor_with_mock_worker
        workload = _make_workload(kind)
        key = executor.get_workload_key(workload)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()
        executor.result_queue.put((proc.pid, key, workload.running_state, None))
        executor.sync()

        if kind == "connection":
            executor.fail_connection_test(key)
        else:
            executor.change_state(key, workload.failure_state, remove_running=True)
        executor.result_queue.put((proc.pid, key, workload.success_state, None))
        executor.sync()

        assert not executor._worker_tasks
        assert executor.slots_available == 1
        expected_state = workload.running_state if kind == "connection" else workload.failure_state
        expected = {key: (expected_state, None)}
        assert executor.event_buffer == expected
        assert executor.workers[proc.pid] is proc

    def test_one_worker_runs_workloads_back_to_back(self, local_executor_with_mock_worker):
        executor, proc = local_executor_with_mock_worker
        first, second = _make_task_workload(), _make_task_workload()
        first_key, second_key = executor.get_task_key(first.ti), executor.get_task_key(second.ti)
        for workload, key in ((first, first_key), (second, second_key)):
            executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
            executor.heartbeat()
            executor.result_queue.put((proc.pid, key, None, None))
            executor.sync()
            assert executor._worker_tasks == {proc.pid: key}
            executor.result_queue.put((proc.pid, key, workload.success_state, None))
            executor.sync()
            assert not executor._worker_tasks
        assert executor.event_buffer == {
            first_key: (first.success_state, None),
            second_key: (second.success_state, None),
        }
        assert executor.slots_available == 1

    def test_redispatched_key_stays_tracked_after_previous_dispatch_finishes(
        self, local_executor_with_mock_worker, mocker
    ):
        executor, first_proc = local_executor_with_mock_worker
        second_proc = _add_mock_worker(executor, mocker, 54321)
        workload = _make_task_workload()
        key = executor.get_task_key(workload.ti)
        session = mock.create_autospec(Session, instance=True)
        executor.queue_workload(workload, session=session)
        executor.heartbeat()
        executor.queue_workload(workload, session=session)
        executor._process_workloads([workload])
        executor.result_queue.put((first_proc.pid, key, None, None))
        executor.result_queue.put((first_proc.pid, key, workload.success_state, None))
        executor.result_queue.put((second_proc.pid, key, None, None))

        executor.sync()

        assert executor.event_buffer[key] == (workload.success_state, None)
        assert executor.has_task(workload.ti)
        assert executor._worker_tasks == {second_proc.pid: key}
        executor.result_queue.put((second_proc.pid, key, workload.failure_state, None))
        executor.sync()
        assert executor.event_buffer[key] == (workload.failure_state, None)
        assert not executor.running
        assert not executor._worker_tasks
        assert not executor._dispatch_counts

    def test_death_of_redispatched_workers_fails_key_after_last_dispatch(
        self, local_executor_with_mock_worker, mocker
    ):
        executor, first_proc = local_executor_with_mock_worker
        second_proc = _add_mock_worker(executor, mocker, 54321)
        workload = _make_task_workload()
        key = executor.get_task_key(workload.ti)
        session = mock.create_autospec(Session, instance=True)
        executor.queue_workload(workload, session=session)
        executor.heartbeat()
        executor.queue_workload(workload, session=session)
        executor._process_workloads([workload])
        executor.result_queue.put((first_proc.pid, key, None, None))
        executor.result_queue.put((second_proc.pid, key, None, None))
        executor.sync()
        first_proc.is_alive.return_value = False

        executor.sync()

        assert key in executor.running
        assert executor._worker_tasks == {second_proc.pid: key}
        second_proc.is_alive.return_value = False
        executor.sync()
        assert executor.event_buffer[key] == (workload.failure_state, None)
        assert not executor.running

    def test_late_start_after_connection_test_reaped_is_ignored(self, local_executor_with_mock_worker):
        executor, proc = local_executor_with_mock_worker
        workload = _make_workload("connection")
        key = executor.get_workload_key(workload)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()
        executor.fail_connection_test(key)
        executor.result_queue.put((proc.pid, key, workload.running_state, None))
        executor.result_queue.put((proc.pid, key, workload.success_state, None))

        executor.sync()

        assert executor.event_buffer == {}
        assert not executor._worker_tasks

    def test_terminal_from_worker_that_does_not_own_the_key_is_ignored(
        self, local_executor_with_mock_worker, mocker
    ):
        executor, owner = local_executor_with_mock_worker
        other = _add_mock_worker(executor, mocker, 54321)
        workload = _make_task_workload()
        key = executor.get_task_key(workload.ti)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()
        executor.result_queue.put((owner.pid, key, None, None))
        executor.result_queue.put((other.pid, key, workload.failure_state, None))

        executor.sync()

        assert executor.event_buffer == {}
        assert executor._worker_tasks == {owner.pid: key}
        assert key in executor.running

    def test_result_from_unknown_pid_is_ignored(self, local_executor_with_mock_worker):
        executor, proc = local_executor_with_mock_worker
        workload = _make_task_workload()
        key = executor.get_task_key(workload.ti)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        executor.heartbeat()
        executor.result_queue.put((proc.pid + 1, key, None, None))

        executor.sync()

        assert not executor._worker_tasks
        assert key in executor.running

    def test_start_resets_bookkeeping_of_a_reused_executor(self, mocker):
        mocker.patch.object(LocalExecutor, "_spawn_workers_with_gc_freeze", autospec=True)
        executor = LocalExecutor(parallelism=1)
        key = TaskInstanceUuid(uuid7())
        executor._worker_tasks[12345] = key
        executor._dispatch_counts[key] = 1

        executor.start()

        try:
            assert not executor._worker_tasks
            assert not executor._dispatch_counts
        finally:
            executor.end()

    @pytest.mark.parametrize("start_method", ["fork", "spawn"])
    @pytest.mark.parametrize("kind", ["task", "callback", "connection"])
    @pytest.mark.execution_timeout(60)
    def test_actual_worker_death_after_start_releases_slot(self, start_method, kind, mocker, tmp_path):
        ctx = multiprocessing.get_context(start_method)
        mocker.patch.object(
            local_executor_module.multiprocessing,
            "get_start_method",
            autospec=True,
            return_value=start_method,
        )
        mocker.patch.object(local_executor_module.multiprocessing, "Process", new=ctx.Process)
        mocker.patch.object(local_executor_module.multiprocessing, "Value", new=ctx.Value)
        mocker.patch.object(local_executor_module, "SimpleQueue", new=ctx.SimpleQueue)
        mocker.patch.object(local_executor_module, "_run_worker", new=_run_blocking_worker)
        executor = LocalExecutor(parallelism=1)
        executor.start()
        workload = _make_workload(kind)
        marker = tmp_path / "entered"
        workload.token = str(marker)
        key = executor.get_workload_key(workload)
        executor.queue_workload(workload, session=mock.create_autospec(Session, instance=True))
        try:
            executor.heartbeat()
            # Spawned workers re-import the airflow stack before dequeuing; ~10s observed on loaded CI runners.
            timeout = 30
            deadline = time.monotonic() + timeout
            while not marker.exists():
                assert time.monotonic() < deadline, f"Worker process failed to start within {timeout}s"
                assert any(proc.is_alive() for proc in executor.workers.values()), (
                    "Worker died before entering workload: "
                    f"{[proc.exitcode for proc in executor.workers.values()]}"
                )
                executor.sync()
                time.sleep(0.01)
            executor.sync()
            pid, proc = next(iter(executor.workers.items()))
            assert executor._worker_tasks == {pid: key}
            proc.kill()
            proc.join(timeout=5)
            assert not proc.is_alive(), "Worker did not exit after being killed"

            executor.sync()

            assert executor.event_buffer[key] == (workload.failure_state, None)
            assert executor.slots_available == 1
            assert not executor._worker_tasks
            assert not executor.workers
            executor.result_queue.put((pid, key, workload.success_state, None))
            executor.sync()
            assert executor.event_buffer[key] == (workload.failure_state, None)
        finally:
            executor.terminate()
            executor.end()


class TestLocalExecutorConnectionTestSupport:
    def test_test_connection_is_supported(self):
        executor = LocalExecutor()
        assert WorkloadType.TEST_CONNECTION in executor.supported_workload_types


class TestLocalExecutorCallbackSupport:
    CALLBACK_UUID = "12345678-1234-5678-1234-567812345678"
    TEST_TOKEN = "test_token"
    TEST_SERVER = "http://localhost:8080/execution/"

    def test_supports_callbacks_flag_is_true(self):
        executor = LocalExecutor()
        assert WorkloadType.EXECUTE_CALLBACK in executor.supported_workload_types

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
            executor.executor_queues[WorkloadType.EXECUTE_CALLBACK][callback_workload.key] = callback_workload
            executor._process_workloads([callback_workload])
            assert len(executor.executor_queues[WorkloadType.EXECUTE_CALLBACK]) == 0
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
            dag_rel_path=Path("test.py"),
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            token="test_token",
            log_path="test.log",
        )

        BaseExecutor.run_workload(callback_workload)

        mock_supervise_callback.assert_called_once_with(
            id=self.CALLBACK_UUID,
            callback_path="test.module.my_callback",
            callback_kwargs={"arg1": "val1"},
            dag_rel_path=Path("test.py"),
            log_path="test.log",
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            token=TestLocalExecutorCallbackSupport.TEST_TOKEN,
            server=TestLocalExecutorCallbackSupport.TEST_SERVER,
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
            dag_rel_path=Path("test.py"),
            bundle_info=BundleInfo(name="test_bundle", version="1.0"),
            token="test_token",
            log_path="test.log",
        )

        with pytest.raises(RuntimeError, match="Callback subprocess exited with code 1"):
            BaseExecutor.run_workload(callback_workload)
