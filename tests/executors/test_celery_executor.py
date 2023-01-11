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

import contextlib
import logging
import os
import signal
import sys
from datetime import datetime, timedelta
from unittest import mock

# leave this it is used by the test worker
import celery.contrib.testing.tasks  # noqa: F401
import pytest
import time_machine
from celery import Celery
from celery.result import AsyncResult
from kombu.asynchronous import set_event_loop

from airflow.configuration import conf
from airflow.executors import celery_executor
from airflow.executors.celery_executor import CeleryExecutor
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
from airflow.utils import timezone
from airflow.utils.state import State
from tests.test_utils import db

FAKE_EXCEPTION_MSG = "Fake Exception"


def _prepare_test_bodies():
    if "CELERY_BROKER_URLS" in os.environ:
        return [(url,) for url in os.environ["CELERY_BROKER_URLS"].split(",")]
    return [(conf.get("celery", "BROKER_URL"))]


class FakeCeleryResult:
    @property
    def state(self):
        raise Exception(FAKE_EXCEPTION_MSG)

    def task_id(self):
        return "task_id"


@contextlib.contextmanager
def _prepare_app(broker_url=None, execute=None):
    broker_url = broker_url or conf.get("celery", "BROKER_URL")
    execute = execute or celery_executor.execute_command.__wrapped__

    test_config = dict(celery_executor.celery_configuration)
    test_config.update({"broker_url": broker_url})
    test_app = Celery(broker_url, config_source=test_config)
    test_execute = test_app.task(execute)
    patch_app = mock.patch("airflow.executors.celery_executor.app", test_app)
    patch_execute = mock.patch("airflow.executors.celery_executor.execute_command", test_execute)

    backend = test_app.backend

    if hasattr(backend, "ResultSession"):
        # Pre-create the database tables now, otherwise SQLA vis Celery has a
        # race condition where it one of the subprocesses can die with "Table
        # already exists" error, because SQLA checks for which tables exist,
        # then issues a CREATE TABLE, rather than doing CREATE TABLE IF NOT
        # EXISTS
        session = backend.ResultSession()
        session.close()

    with patch_app, patch_execute:
        try:
            yield test_app
        finally:
            # Clear event loop to tear down each celery instance
            set_event_loop(None)


class TestCeleryExecutor:
    def setup_method(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    def teardown_method(self) -> None:
        db.clear_db_runs()
        db.clear_db_jobs()

    def test_supports_pickling(self):
        assert CeleryExecutor.supports_pickling

    def test_supports_sentry(self):
        assert CeleryExecutor.supports_sentry

    @pytest.mark.backend("mysql", "postgres")
    def test_exception_propagation(self, caplog):
        caplog.set_level(logging.ERROR, logger="airflow.executors.celery_executor.BulkStateFetcher")
        with _prepare_app():
            executor = celery_executor.CeleryExecutor()
            executor.tasks = {"key": FakeCeleryResult()}
            executor.bulk_state_fetcher._get_many_using_multiprocessing(executor.tasks.values())
        assert celery_executor.CELERY_FETCH_ERR_MSG_HEADER in caplog.text, caplog.record_tuples
        assert FAKE_EXCEPTION_MSG in caplog.text, caplog.record_tuples

    @mock.patch("airflow.executors.celery_executor.CeleryExecutor.sync")
    @mock.patch("airflow.executors.celery_executor.CeleryExecutor.trigger_tasks")
    @mock.patch("airflow.executors.base_executor.Stats.gauge")
    def test_gauge_executor_metrics(self, mock_stats_gauge, mock_trigger_tasks, mock_sync):
        executor = celery_executor.CeleryExecutor()
        executor.heartbeat()
        calls = [
            mock.call("executor.open_slots", mock.ANY),
            mock.call("executor.queued_tasks", mock.ANY),
            mock.call("executor.running_tasks", mock.ANY),
        ]
        mock_stats_gauge.assert_has_calls(calls)

    @pytest.mark.parametrize(
        "command, raise_exception",
        [
            pytest.param(["true"], True, id="wrong-command"),
            pytest.param(["airflow", "tasks"], True, id="incomplete-command"),
            pytest.param(["airflow", "tasks", "run"], False, id="complete-command"),
        ],
    )
    def test_command_validation(self, command, raise_exception):
        """Check that we validate _on the receiving_ side, not just sending side"""
        expected_context = contextlib.nullcontext()
        if raise_exception:
            expected_context = pytest.raises(
                ValueError, match=r'The command must start with \["airflow", "tasks", "run"\]\.'
            )

        with mock.patch(
            "airflow.executors.celery_executor._execute_in_subprocess"
        ) as mock_subproc, mock.patch(
            "airflow.executors.celery_executor._execute_in_fork"
        ) as mock_fork, mock.patch(
            "celery.app.task.Task.request"
        ) as mock_task:
            mock_task.id = "abcdef-124215-abcdef"
            with expected_context:
                celery_executor.execute_command(command)
            if raise_exception:
                mock_subproc.assert_not_called()
                mock_fork.assert_not_called()
            else:
                # One of these should be called.
                assert mock_subproc.call_args == (
                    (command, "abcdef-124215-abcdef"),
                ) or mock_fork.call_args == ((command, "abcdef-124215-abcdef"),)

    @pytest.mark.backend("mysql", "postgres")
    def test_try_adopt_task_instances_none(self):
        start_date = datetime.utcnow() - timedelta(days=2)

        with DAG("test_try_adopt_task_instances_none"):
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)

        key1 = TaskInstance(task=task_1, run_id=None)
        tis = [key1]
        executor = celery_executor.CeleryExecutor()

        assert executor.try_adopt_task_instances(tis) == tis

    @pytest.mark.backend("mysql", "postgres")
    @time_machine.travel("2020-01-01", tick=False)
    def test_try_adopt_task_instances(self):
        start_date = timezone.utcnow() - timedelta(days=2)

        try_number = 1

        with DAG("test_try_adopt_task_instances_none") as dag:
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)
            task_2 = BaseOperator(task_id="task_2", start_date=start_date)

        ti1 = TaskInstance(task=task_1, run_id=None)
        ti1.external_executor_id = "231"
        ti1.state = State.QUEUED
        ti2 = TaskInstance(task=task_2, run_id=None)
        ti2.external_executor_id = "232"
        ti2.state = State.QUEUED

        tis = [ti1, ti2]
        executor = celery_executor.CeleryExecutor()
        assert executor.running == set()
        assert executor.adopted_task_timeouts == {}
        assert executor.stalled_task_timeouts == {}
        assert executor.tasks == {}

        not_adopted_tis = executor.try_adopt_task_instances(tis)

        key_1 = TaskInstanceKey(dag.dag_id, task_1.task_id, None, try_number)
        key_2 = TaskInstanceKey(dag.dag_id, task_2.task_id, None, try_number)
        assert executor.running == {key_1, key_2}
        assert executor.adopted_task_timeouts == {
            key_1: timezone.utcnow() + executor.task_adoption_timeout,
            key_2: timezone.utcnow() + executor.task_adoption_timeout,
        }
        assert executor.stalled_task_timeouts == {}
        assert executor.tasks == {key_1: AsyncResult("231"), key_2: AsyncResult("232")}
        assert not_adopted_tis == []

    @pytest.fixture
    def mock_celery_revoke(self):
        with _prepare_app() as app:
            app.control.revoke = mock.MagicMock()
            yield app.control.revoke

    @pytest.mark.backend("mysql", "postgres")
    def test_check_for_timedout_adopted_tasks(self, create_dummy_dag, dag_maker, session, mock_celery_revoke):
        create_dummy_dag(dag_id="test_clear_stalled", task_id="task1", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti = dag_run.task_instances[0]
        ti.state = State.QUEUED
        ti.queued_dttm = timezone.utcnow()
        ti.queued_by_job_id = 1
        ti.external_executor_id = "231"
        session.flush()

        executor = celery_executor.CeleryExecutor()
        executor.job_id = 1
        executor.adopted_task_timeouts = {
            ti.key: timezone.utcnow() - timedelta(days=1),
        }
        executor.running = {ti.key}
        executor.tasks = {ti.key: AsyncResult("231")}
        executor.sync()
        assert executor.event_buffer == {}
        assert executor.tasks == {}
        assert executor.running == set()
        assert executor.adopted_task_timeouts == {}
        assert mock_celery_revoke.called_with("231")

        ti.refresh_from_db()
        assert ti.state == State.SCHEDULED
        assert ti.queued_by_job_id is None
        assert ti.queued_dttm is None
        assert ti.external_executor_id is None

    @pytest.mark.backend("mysql", "postgres")
    def test_check_for_stalled_tasks(self, create_dummy_dag, dag_maker, session, mock_celery_revoke):
        create_dummy_dag(dag_id="test_clear_stalled", task_id="task1", with_dagrun_type=None)
        dag_run = dag_maker.create_dagrun()

        ti = dag_run.task_instances[0]
        ti.state = State.QUEUED
        ti.queued_dttm = timezone.utcnow()
        ti.queued_by_job_id = 1
        ti.external_executor_id = "231"
        session.flush()

        executor = celery_executor.CeleryExecutor()
        executor.job_id = 1
        executor.stalled_task_timeouts = {
            ti.key: timezone.utcnow() - timedelta(days=1),
        }
        executor.running = {ti.key}
        executor.tasks = {ti.key: AsyncResult("231")}
        executor.sync()
        assert executor.event_buffer == {}
        assert executor.tasks == {}
        assert executor.running == set()
        assert executor.stalled_task_timeouts == {}
        assert mock_celery_revoke.called_with("231")

        ti.refresh_from_db()
        assert ti.state == State.SCHEDULED
        assert ti.queued_by_job_id is None
        assert ti.queued_dttm is None
        assert ti.external_executor_id is None

    @pytest.mark.backend("mysql", "postgres")
    @time_machine.travel("2020-01-01", tick=False)
    def test_pending_tasks_timeout_with_appropriate_config_setting(self):
        start_date = timezone.utcnow() - timedelta(days=2)

        with DAG("test_check_for_stalled_tasks_are_ordered"):
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)
            task_2 = BaseOperator(task_id="task_2", start_date=start_date)

        ti1 = TaskInstance(task=task_1, run_id=None)
        ti1.external_executor_id = "231"
        ti1.state = State.QUEUED
        ti2 = TaskInstance(task=task_2, run_id=None)
        ti2.external_executor_id = "232"
        ti2.state = State.QUEUED

        executor = celery_executor.CeleryExecutor()
        executor.stalled_task_timeout = timedelta(seconds=30)
        executor.queued_tasks[ti2.key] = (None, None, None, None)
        executor.try_adopt_task_instances([ti1])
        with mock.patch("airflow.executors.celery_executor.send_task_to_executor") as mock_send_task:
            mock_send_task.return_value = (ti2.key, None, mock.MagicMock())
            executor._process_tasks([(ti2.key, None, None, mock.MagicMock())])
        assert executor.stalled_task_timeouts == {
            ti2.key: timezone.utcnow() + timedelta(seconds=30),
        }
        assert executor.adopted_task_timeouts == {
            ti1.key: timezone.utcnow() + timedelta(seconds=600),
        }

    @pytest.mark.backend("mysql", "postgres")
    def test_no_pending_task_timeouts_when_configured(self):
        start_date = timezone.utcnow() - timedelta(days=2)

        with DAG("test_check_for_stalled_tasks_are_ordered"):
            task_1 = BaseOperator(task_id="task_1", start_date=start_date)
            task_2 = BaseOperator(task_id="task_2", start_date=start_date)

        ti1 = TaskInstance(task=task_1, run_id=None)
        ti1.external_executor_id = "231"
        ti1.state = State.QUEUED
        ti2 = TaskInstance(task=task_2, run_id=None)
        ti2.external_executor_id = "232"
        ti2.state = State.QUEUED

        executor = celery_executor.CeleryExecutor()
        executor.task_adoption_timeout = timedelta(0)
        executor.queued_tasks[ti2.key] = (None, None, None, None)
        executor.try_adopt_task_instances([ti1])
        with mock.patch("airflow.executors.celery_executor.send_task_to_executor") as mock_send_task:
            mock_send_task.return_value = (ti2.key, None, mock.MagicMock())
            executor._process_tasks([(ti2.key, None, None, mock.MagicMock())])
        assert executor.adopted_task_timeouts == {}
        assert executor.stalled_task_timeouts == {}


def test_operation_timeout_config():
    assert celery_executor.OPERATION_TIMEOUT == 1


class MockTask:
    """
    A picklable object used to mock tasks sent to Celery. Can't use the mock library
    here because it's not picklable.
    """

    def apply_async(self, *args, **kwargs):
        return 1


def _exit_gracefully(signum, _):
    print(f"{os.getpid()} Exiting gracefully upon receiving signal {signum}")
    sys.exit(signum)


@pytest.fixture
def register_signals():
    """
    Register the same signals as scheduler does to test celery_executor to make sure it does not
    hang.
    """
    orig_sigint = orig_sigterm = orig_sigusr2 = signal.SIG_DFL

    orig_sigint = signal.signal(signal.SIGINT, _exit_gracefully)
    orig_sigterm = signal.signal(signal.SIGTERM, _exit_gracefully)
    orig_sigusr2 = signal.signal(signal.SIGUSR2, _exit_gracefully)

    yield

    # Restore original signal handlers after test
    signal.signal(signal.SIGINT, orig_sigint)
    signal.signal(signal.SIGTERM, orig_sigterm)
    signal.signal(signal.SIGUSR2, orig_sigusr2)


@pytest.mark.execution_timeout(200)
def test_send_tasks_to_celery_hang(register_signals):
    """
    Test that celery_executor does not hang after many runs.
    """
    executor = celery_executor.CeleryExecutor()

    task = MockTask()
    task_tuples_to_send = [(None, None, None, task) for _ in range(26)]

    for _ in range(250):
        # This loop can hang on Linux if celery_executor does something wrong with
        # multiprocessing.
        results = executor._send_tasks_to_celery(task_tuples_to_send)
        assert results == [(None, None, 1) for _ in task_tuples_to_send]
