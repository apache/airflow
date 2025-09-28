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
import datetime
import logging
import os
from collections import Counter, deque
from collections.abc import Generator
from datetime import timedelta
from pathlib import Path
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch
from uuid import uuid4

import pendulum
import psutil
import pytest
import time_machine
from pytest import param
from sqlalchemy import func, select, update
from sqlalchemy.orm import joinedload

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.assets.manager import AssetManager
from airflow.callbacks.callback_requests import DagCallbackRequest, DagRunContext, TaskCallbackRequest
from airflow.callbacks.database_callback_sink import DatabaseCallbackSink
from airflow.dag_processing.collection import AssetModelOperation, DagModelOperation
from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.executor_constants import MOCK_EXECUTOR
from airflow.executors.executor_loader import ExecutorLoader
from airflow.executors.executor_utils import ExecutorName
from airflow.jobs.job import Job, run_job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.asset import AssetActive, AssetAliasModel, AssetDagRunQueue, AssetEvent, AssetModel
from airflow.models.backfill import Backfill, _create_backfill
from airflow.models.dag import DagModel, get_last_dagrun, infer_automated_data_interval
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarning
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.deadline import Deadline
from airflow.models.log import Log
from airflow.models.pool import Pool
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.models.trigger import Trigger
from airflow.observability.traces.tracer import Trace
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.temporal import DateTimeTrigger
from airflow.sdk import DAG, Asset, AssetAlias, AssetWatcher, task
from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.serialization.serialized_objects import LazyDeserializedDAG, SerializedDAG
from airflow.timetables.base import DataInterval
from airflow.utils.session import create_session, provide_session
from airflow.utils.span_status import SpanStatus
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.thread_safe_dict import ThreadSafeDict
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.pytest_plugin import AIRFLOW_ROOT_PATH
from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars, env_vars
from tests_common.test_utils.dag import create_scheduler_dag, sync_dag_to_db, sync_dags_to_db
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_backfills,
    clear_db_callbacks,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_deadline,
    clear_db_import_errors,
    clear_db_jobs,
    clear_db_pools,
    clear_db_runs,
    clear_db_serialized_dags,
    clear_db_teams,
    clear_db_triggers,
    set_default_pool_slots,
)
from tests_common.test_utils.mock_executor import MockExecutor
from tests_common.test_utils.mock_operators import CustomOperator
from tests_common.test_utils.version_compat import SQLALCHEMY_V_1_4, SQLALCHEMY_V_2_0
from unit.listeners import dag_listener
from unit.listeners.test_listeners import get_listener_manager
from unit.models import TEST_DAGS_FOLDER

pytestmark = pytest.mark.db_test

PERF_DAGS_FOLDER = AIRFLOW_ROOT_PATH / "dev" / "airflow_perf" / "dags"
ELASTIC_DAG_FILE = os.path.join(PERF_DAGS_FOLDER, "elastic_dag.py")

TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]
EXAMPLE_STANDARD_DAGS_FOLDER = (
    AIRFLOW_ROOT_PATH
    / "providers"
    / "standard"
    / "src"
    / "airflow"
    / "providers"
    / "standard"
    / "example_dags"
)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
DEFAULT_LOGICAL_DATE = timezone.coerce_datetime(DEFAULT_DATE)
TRY_NUMBER = 1


@pytest.fixture(scope="class")
def disable_load_example():
    with conf_vars({("core", "load_examples"): "false"}):
        with env_vars({"AIRFLOW__CORE__LOAD_EXAMPLES": "false"}):
            yield


# Patch the MockExecutor into the dict of known executors in the Loader
@contextlib.contextmanager
def _loader_mock(mock_executors):
    with mock.patch("airflow.executors.executor_loader.ExecutorLoader.load_executor") as loader_mock:
        # The executors are mocked, so cannot be loaded/imported. Mock load_executor and return the
        # correct object for the given input executor name.
        loader_mock.side_effect = lambda *x: {
            ("default_exec",): mock_executors[0],
            (None,): mock_executors[0],
            ("secondary_exec",): mock_executors[1],
        }[x]
        yield


@pytest.fixture
def create_dagrun(session):
    def _create_dagrun(
        dag: SerializedDAG,
        *,
        logical_date: datetime.datetime,
        data_interval: DataInterval,
        run_type: DagRunType,
        state: DagRunState = DagRunState.RUNNING,
        start_date: datetime.datetime | None = None,
    ) -> DagRun:
        run_after = logical_date or timezone.utcnow()
        run_id = DagRun.generate_run_id(
            run_type=run_type,
            logical_date=logical_date,
            run_after=run_after,
        )
        return dag.create_dagrun(
            run_id=run_id,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=run_after,
            run_type=run_type,
            state=state,
            start_date=start_date,
            triggered_by=DagRunTriggeredByType.TEST,
        )

    return _create_dagrun


@patch.dict(
    ExecutorLoader.executors, {MOCK_EXECUTOR: f"{MockExecutor.__module__}.{MockExecutor.__qualname__}"}
)
@pytest.mark.usefixtures("disable_load_example")
@pytest.mark.need_serialized_dag
class TestSchedulerJob:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_backfills()
        clear_db_pools()
        clear_db_import_errors()
        clear_db_jobs()
        clear_db_assets()
        clear_db_deadline()
        clear_db_callbacks()
        clear_db_triggers()

    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        self.clean_db()
        self.job_runner: SchedulerJobRunner | None = None

        yield

        self.clean_db()

    @pytest.fixture(autouse=True)
    def set_instance_attrs(self) -> Generator:
        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec: MockExecutor | None = MockExecutor()
        yield
        self.null_exec = None

    @pytest.fixture
    def mock_executors(self):
        mock_jwt_generator = MagicMock(spec=JWTGenerator)
        mock_jwt_generator.generate.return_value = "mock-token"

        default_executor = mock.MagicMock(name="DefaultExecutor", slots_available=8, slots_occupied=0)
        default_executor.name = ExecutorName(alias="default_exec", module_path="default.exec.module.path")
        default_executor.jwt_generator = mock_jwt_generator
        default_executor.team_name = None  # Global executor
        default_executor.sentry_integration = ""
        second_executor = mock.MagicMock(name="SeconadaryExecutor", slots_available=8, slots_occupied=0)
        second_executor.name = ExecutorName(alias="secondary_exec", module_path="secondary.exec.module.path")
        second_executor.jwt_generator = mock_jwt_generator
        second_executor.team_name = None  # Global executor
        second_executor.sentry_integration = ""

        # TODO: Task-SDK Make it look like a bound method. Needed until we remove the old queue_workload
        # interface from executors
        default_executor.queue_workload.__func__ = BaseExecutor.queue_workload
        second_executor.queue_workload.__func__ = BaseExecutor.queue_workload

        with mock.patch("airflow.jobs.job.Job.executors", new_callable=PropertyMock) as executors_mock:
            executors_mock.return_value = [default_executor, second_executor]
            yield [default_executor, second_executor]

    @pytest.fixture
    def mock_executor(self, mock_executors):
        default_executor = mock_executors[0]
        with mock.patch("airflow.jobs.job.Job.executors", new_callable=PropertyMock) as executors_mock:
            executors_mock.return_value = [default_executor]
            yield default_executor

    def test_is_alive(self):
        scheduler_job = Job(heartrate=10, state=State.RUNNING)
        self.job_runner = SchedulerJobRunner(scheduler_job)
        assert scheduler_job.is_alive()

        scheduler_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
        assert scheduler_job.is_alive()

        scheduler_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=31)
        assert not scheduler_job.is_alive()

        # test because .seconds was used before instead of total_seconds
        # internal repr of datetime is (days, seconds)
        scheduler_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(days=1)
        assert not scheduler_job.is_alive()

        scheduler_job.state = State.SUCCESS
        scheduler_job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=10)
        assert not scheduler_job.is_alive(), "Completed jobs even with recent heartbeat should not be alive"

    @pytest.mark.parametrize(
        "heartrate",
        [10, 5],
    )
    def test_heartrate(self, heartrate):
        with conf_vars({("scheduler", "scheduler_heartbeat_sec"): str(heartrate)}):
            scheduler_job = Job(executor=self.null_exec)
            _ = SchedulerJobRunner(job=scheduler_job)
            assert scheduler_job.heartrate == heartrate

    def test_no_orphan_process_will_be_left(self):
        current_process = psutil.Process()
        old_children = current_process.children(recursive=True)
        scheduler_job = Job(
            executor=MockExecutor(do_update=False),
        )
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        # Remove potential noise created by previous tests.
        current_children = set(current_process.children(recursive=True)) - set(old_children)
        assert not current_children

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.incr")
    def test_process_executor_events(self, mock_stats_incr, mock_task_callback, dag_maker):
        dag_id = "test_process_executor_events"
        task_id_1 = "dummy_task"

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id_1)
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        mock_stats_incr.reset_mock()

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock(spec=TaskCallbackRequest)
        mock_task_callback.return_value = task_callback
        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.FAILED
        scheduler_job.executor.callback_sink.send.assert_not_called()

        # ti in success state
        ti1.state = State.SUCCESS
        session.merge(ti1)
        session.commit()
        executor.event_buffer[ti1.key] = State.SUCCESS, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.SUCCESS
        scheduler_job.executor.callback_sink.send.assert_not_called()
        mock_stats_incr.assert_has_calls(
            [
                mock.call(
                    "scheduler.tasks.killed_externally",
                    tags={"dag_id": dag_id, "task_id": ti1.task_id},
                ),
                mock.call("operator_failures_EmptyOperator", tags={"dag_id": dag_id, "task_id": ti1.task_id}),
                mock.call("ti_failures", tags={"dag_id": dag_id, "task_id": ti1.task_id}),
            ],
            any_order=True,
        )

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest", spec=TaskCallbackRequest)
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.incr")
    def test_process_executor_events_restarting_cleared_task(
        self, mock_stats_incr, mock_task_callback, dag_maker
    ):
        """
        Test processing of RESTARTING task instances by scheduler's _process_executor_events.

        Simulates the complete flow when a running task is cleared:
        1. Task is RUNNING and has exhausted retries (try_number > max_tries)
        2. User clears the task → state becomes RESTARTING
        3. Executor successfully terminates the task → reports SUCCESS
        4. Scheduler processes the event and sets task to None (scheduled)
        5. max_tries is adjusted to allow retry beyond normal limits

        This test prevents regression of issue #55045 where RESTARTING tasks
        would get stuck due to scheduler not processing executor events.
        """
        dag_id = "test_restarting_max_tries"
        task_id = "test_task"

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/", max_active_runs=1):
            task1 = EmptyOperator(task_id=task_id, retries=2)
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        # Set up exhausted task scenario: try_number > max_tries
        ti1.state = TaskInstanceState.RESTARTING  # Simulates cleared running task
        ti1.try_number = 4  # Already tried 4 times
        ti1.max_tries = 3  # Originally only allowed 3 tries
        session.merge(ti1)
        session.commit()

        # Verify task is in RESTARTING state and eligible for retry
        assert ti1.state == TaskInstanceState.RESTARTING
        assert ti1.is_eligible_to_retry() is True, "RESTARTING should bypass max_tries"

        # Set up scheduler and executor
        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock(spec=TaskCallbackRequest)
        mock_task_callback.return_value = task_callback
        scheduler_job = Job(executor=executor)
        job_runner = SchedulerJobRunner(scheduler_job)

        # Simulate executor reporting task completion (this triggers the bug scenario)
        executor.event_buffer[ti1.key] = State.SUCCESS, None

        # Process the executor event
        job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)

        assert ti1.state is None, "Task should be set to None (scheduled) state after RESTARTING processing"

        # Verify max_tries was adjusted to allow retry
        expected_max_tries = 4 + 2
        assert ti1.max_tries == expected_max_tries, (
            f"max_tries should be adjusted to {expected_max_tries}, got {ti1.max_tries}"
        )

        # Verify task is now eligible for retry despite being previously exhausted
        assert ti1.is_eligible_to_retry() is True, (
            "Task should be eligible for retry after max_tries adjustment"
        )

        # Verify try_number wasn't changed (scheduler doesn't increment it here)
        assert ti1.try_number == 4, "try_number should remain unchanged"

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.incr")
    def test_process_executor_events_with_no_callback(self, mock_stats_incr, mock_task_callback, dag_maker):
        dag_id = "test_process_executor_events_with_no_callback"
        task_id = "test_task"
        run_id = "test_run"

        mock_stats_incr.reset_mock()
        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock(spec=TaskCallbackRequest)
        mock_task_callback.return_value = task_callback
        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id, retries=1)
        ti1 = dag_maker.create_dagrun(
            run_id=run_id, logical_date=DEFAULT_DATE + timedelta(hours=1)
        ).get_task_instance(task1.task_id)

        mock_stats_incr.reset_mock()

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.UP_FOR_RETRY
        scheduler_job.executor.callback_sink.send.assert_not_called()

        # ti in success state
        ti1.state = State.SUCCESS
        session.merge(ti1)
        session.commit()
        executor.event_buffer[ti1.key] = State.SUCCESS, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.SUCCESS
        scheduler_job.executor.callback_sink.send.assert_not_called()
        mock_stats_incr.assert_has_calls(
            [
                mock.call(
                    "scheduler.tasks.killed_externally",
                    tags={"dag_id": dag_id, "task_id": task_id},
                ),
                mock.call("operator_failures_EmptyOperator", tags={"dag_id": dag_id, "task_id": task_id}),
                mock.call("ti_failures", tags={"dag_id": dag_id, "task_id": task_id}),
            ],
            any_order=True,
        )

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.incr")
    def test_process_executor_events_with_callback(
        self, mock_stats_incr, mock_task_callback, dag_maker, session
    ):
        dag_id = "test_process_executor_events_with_callback"
        task_id_1 = "dummy_task"

        with dag_maker(dag_id=dag_id, fileloc="/test_path1/") as dag:
            EmptyOperator(task_id=task_id_1, on_failure_callback=lambda x: print("hi"))
        dr = dag_maker.create_dagrun()
        ti1 = dr.task_instances[0]

        mock_stats_incr.reset_mock()

        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        executor = MockExecutor(do_update=False)
        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db()
        assert ti1.state == State.FAILED
        mock_task_callback.assert_called_once_with(
            filepath=dag.relative_fileloc,
            ti=mock.ANY,
            bundle_name="dag_maker",
            bundle_version=None,
            msg=f"Executor {executor} reported that the task instance "
            "<TaskInstance: test_process_executor_events_with_callback.dummy_task test [queued]> "
            "finished with state failed, but the task instance's state attribute is queued. "
            "Learn more: https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html#task-state-changed-externally",
            context_from_server=mock.ANY,
            task_callback_type=TaskInstanceState.FAILED,
        )
        scheduler_job.executor.callback_sink.send.assert_called_once_with(task_callback)
        scheduler_job.executor.callback_sink.reset_mock()
        mock_stats_incr.assert_any_call(
            "scheduler.tasks.killed_externally",
            tags={
                "dag_id": "test_process_executor_events_with_callback",
                "task_id": "dummy_task",
            },
        )

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.incr")
    def test_process_executor_event_missing_dag(self, mock_stats_incr, mock_task_callback, dag_maker, caplog):
        dag_id = "test_process_executor_events_with_callback"
        task_id_1 = "dummy_task"

        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id_1, on_failure_callback=lambda x: print("hi"))
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        mock_stats_incr.reset_mock()

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)
        self.job_runner.scheduler_dag_bag = mock.MagicMock()
        self.job_runner.scheduler_dag_bag.get_dag_for_run.side_effect = Exception("failed")

        session = settings.Session()

        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None
        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db()
        assert ti1.state == State.FAILED

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.incr")
    def test_process_executor_events_ti_requeued(self, mock_stats_incr, mock_task_callback, dag_maker):
        dag_id = "test_process_executor_events_ti_requeued"
        task_id_1 = "dummy_task"

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id_1)
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        mock_stats_incr.reset_mock()

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        scheduler_job = Job(executor=executor)
        session.add(scheduler_job)
        session.flush()
        self.job_runner = SchedulerJobRunner(scheduler_job)

        # ti is queued with another try number - do not fail it
        ti1.state = State.QUEUED
        ti1.queued_by_job_id = scheduler_job.id
        ti1.try_number = 2
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key.with_try_number(1)] = State.SUCCESS, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.QUEUED
        scheduler_job.executor.callback_sink.send.assert_not_called()

        # ti is queued by another scheduler - do not fail it
        ti1.state = State.QUEUED
        ti1.queued_by_job_id = scheduler_job.id - 1
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.SUCCESS, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.QUEUED
        scheduler_job.executor.callback_sink.send.assert_not_called()

        # ti is queued by this scheduler but it is handed back to the executor - do not fail it
        ti1.state = State.QUEUED
        ti1.queued_by_job_id = 1
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.SUCCESS, None
        executor.has_task = mock.MagicMock(return_value=True)

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.QUEUED
        scheduler_job.executor.callback_sink.send.assert_not_called()
        mock_stats_incr.assert_not_called()

    @pytest.mark.usefixtures("testing_dag_bundle")
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.incr")
    def test_process_executor_events_with_asset_events(self, mock_stats_incr, session, dag_maker):
        """
        Test that _process_executor_events handles asset events without DetachedInstanceError.

        Regression test for scheduler crashes when task callbacks are built with
        consumed_asset_events that weren't eager-loaded.
        """
        asset1 = Asset(uri="test://asset1", name="test_asset_executor", group="test_group")
        asset_model = AssetModel(name=asset1.name, uri=asset1.uri, group=asset1.group)
        session.add(asset_model)
        session.flush()

        with dag_maker(dag_id="test_executor_events_with_assets", schedule=[asset1], fileloc="/test_path1/"):
            EmptyOperator(task_id="dummy_task", on_failure_callback=lambda ctx: None)

        dag = dag_maker.dag
        sync_dag_to_db(dag)
        DagVersion.get_latest_version(dag.dag_id)

        dr = dag_maker.create_dagrun()

        # Create asset event and attach to dag run
        asset_event = AssetEvent(
            asset_id=asset_model.id,
            source_task_id="upstream_task",
            source_dag_id="upstream_dag",
            source_run_id="upstream_run",
            source_map_index=-1,
        )
        session.add(asset_event)
        session.flush()
        dr.consumed_asset_events.append(asset_event)
        session.add(dr)
        session.flush()

        executor = MockExecutor(do_update=False)
        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        ti1 = dr.get_task_instance("dummy_task")
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        # This should not raise DetachedInstanceError
        self.job_runner._process_executor_events(executor=executor, session=session)

        ti1.refresh_from_db(session=session)
        assert ti1.state == State.FAILED

        # Verify callback was created with asset event data
        scheduler_job.executor.callback_sink.send.assert_called_once()
        callback_request = scheduler_job.executor.callback_sink.send.call_args.args[0]
        assert callback_request.context_from_server is not None
        assert len(callback_request.context_from_server.dag_run.consumed_asset_events) == 1
        assert callback_request.context_from_server.dag_run.consumed_asset_events[0].asset.uri == asset1.uri

    def test_execute_task_instances_is_paused_wont_execute(self, session, dag_maker):
        dag_id = "SchedulerJobTest.test_execute_task_instances_is_paused_wont_execute"
        task_id_1 = "dummy_task"

        with dag_maker(dag_id=dag_id, session=session) as dag:
            EmptyOperator(task_id=task_id_1)
        assert isinstance(dag, SerializedDAG)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        dr1 = dag_maker.create_dagrun(run_type=DagRunType.BACKFILL_JOB)
        (ti1,) = dr1.task_instances
        ti1.state = State.SCHEDULED

        self.job_runner._critical_section_enqueue_task_instances(session)
        session.flush()
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.SCHEDULED
        session.rollback()

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_find_and_purge_task_instances_without_heartbeats_with_asset_events(
        self, session, dag_maker, create_dagrun
    ):
        """
        Test that heartbeat purge succeeds when DagRun has consumed_asset_events.

        Regression test for DetachedInstanceError when building TaskCallbackRequest
        with asset event data after session expunge.
        """
        asset1 = Asset(uri="test://asset1", name="test_asset", group="test_group")
        asset_model = AssetModel(name=asset1.name, uri=asset1.uri, group=asset1.group)
        session.add(asset_model)
        session.flush()

        with dag_maker(dag_id="test_heartbeat_with_assets", schedule=[asset1]):
            EmptyOperator(task_id="dummy_task")

        dag = dag_maker.dag
        scheduler_dag = sync_dag_to_db(dag)
        dag_v = DagVersion.get_latest_version(dag.dag_id)

        data_interval = infer_automated_data_interval(scheduler_dag.timetable, DEFAULT_LOGICAL_DATE)
        dag_run = create_dagrun(
            scheduler_dag,
            logical_date=DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            data_interval=data_interval,
        )

        # Create asset alias and event with full relationships
        asset_alias = AssetAliasModel(name="test_alias", group="test_group")
        session.add(asset_alias)
        session.flush()

        asset_event = AssetEvent(
            asset_id=asset_model.id,
            source_task_id="upstream_task",
            source_dag_id="upstream_dag",
            source_run_id="upstream_run",
            source_map_index=-1,
        )
        session.add(asset_event)
        session.flush()

        # Attach alias to event and event to dag run
        asset_event.source_aliases.append(asset_alias)
        dag_run.consumed_asset_events.append(asset_event)
        session.add_all([asset_event, dag_run])
        session.flush()

        executor = MockExecutor()
        scheduler_job = Job(executor=executor)
        with mock.patch("airflow.executors.executor_loader.ExecutorLoader.load_executor") as loader_mock:
            loader_mock.return_value = executor
            self.job_runner = SchedulerJobRunner(job=scheduler_job)

            ti = dag_run.get_task_instance("dummy_task")
            assert ti is not None  # sanity check: dag_maker.create_dagrun created the TI

            ti.state = State.RUNNING
            ti.last_heartbeat_at = timezone.utcnow() - timedelta(minutes=6)
            ti.start_date = timezone.utcnow() - timedelta(minutes=10)
            ti.queued_by_job_id = scheduler_job.id
            ti.dag_version = dag_v
            session.merge(ti)
            session.flush()

            executor.running.add(ti.key)

            tis_without_heartbeats = self.job_runner._find_task_instances_without_heartbeats(session=session)
            assert len(tis_without_heartbeats) == 1
            ti_from_query = tis_without_heartbeats[0]
            ti_key = ti_from_query.key

            # Detach all ORM objects to mirror scheduler behaviour after session closes
            session.expunge_all()

            # This should not raise DetachedInstanceError now that eager loads are in place
            self.job_runner._purge_task_instances_without_heartbeats(tis_without_heartbeats, session=session)
            assert ti_key not in executor.running

        executor.callback_sink.send.assert_called_once()
        callback_request = executor.callback_sink.send.call_args.args[0]
        assert callback_request.context_from_server is not None
        assert len(callback_request.context_from_server.dag_run.consumed_asset_events) == 1
        consumed_event = callback_request.context_from_server.dag_run.consumed_asset_events[0]
        assert consumed_event.asset.uri == asset1.uri
        assert len(consumed_event.source_aliases) == 1
        assert consumed_event.source_aliases[0].name == "test_alias"

    # @pytest.mark.usefixtures("mock_executor")
    def test_execute_task_instances_backfill_tasks_will_execute(self, dag_maker):
        """
        Tests that backfill tasks won't get executed.
        """
        dag_id = "SchedulerJobTest.test_execute_task_instances_backfill_tasks_will_execute"
        task_id_1 = "dummy_task"

        with dag_maker(dag_id=dag_id):
            task1 = EmptyOperator(task_id=task_id_1)

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.BACKFILL_JOB)
        dag_version = DagVersion.get_latest_version(dr1.dag_id)

        ti1 = TaskInstance(task1, run_id=dr1.run_id, dag_version_id=dag_version.id)
        ti1.refresh_from_db()
        ti1.state = State.SCHEDULED
        session.merge(ti1)
        session.flush()
        assert dr1.run_type == DagRunType.BACKFILL_JOB

        self.job_runner._critical_section_enqueue_task_instances(session)
        session.flush()
        ti1.refresh_from_db()
        assert ti1.state == TaskInstanceState.QUEUED
        session.rollback()

    def test_setup_callback_sink_standalone_dag_processor(self, mock_executors):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._execute()

        assert isinstance(scheduler_job.executor.callback_sink, DatabaseCallbackSink)

    def test_setup_callback_sink_standalone_dag_processor_multiple_executors(self, mock_executors):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._execute()

        for executor in scheduler_job.executors:
            assert isinstance(executor.callback_sink, DatabaseCallbackSink)

    def test_executor_start_called(self, mock_executors):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._execute()

        scheduler_job.executor.start.assert_called_once()
        for executor in scheduler_job.executors:
            executor.start.assert_called_once()

    def test_executor_job_id_assigned(self, mock_executors, configure_testing_dag_bundle):
        with configure_testing_dag_bundle(os.devnull):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            self.job_runner._execute()

            assert scheduler_job.executor.job_id == scheduler_job.id
            for executor in scheduler_job.executors:
                assert executor.job_id == scheduler_job.id

    def test_executor_heartbeat(self, mock_executors, configure_testing_dag_bundle):
        with configure_testing_dag_bundle(os.devnull):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            self.job_runner._execute()

            for executor in scheduler_job.executors:
                executor.heartbeat.assert_called_once()

    def test_executor_events_processed(self, mock_executors, configure_testing_dag_bundle):
        with configure_testing_dag_bundle(os.devnull):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            self.job_runner._execute()

            for executor in scheduler_job.executors:
                executor.get_event_buffer.assert_called_once()

    @patch("traceback.extract_stack")
    def test_executor_debug_dump(self, patch_traceback_extract_stack, mock_executors):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._debug_dump(1, mock.MagicMock())

        for executor in scheduler_job.executors:
            executor.debug_dump.assert_called_once()

        patch_traceback_extract_stack.assert_called()

    def test_find_executable_task_instances_backfill(self, dag_maker):
        dag_id = "SchedulerJobTest.test_find_executable_task_instances_backfill"
        task_id_1 = "dummy"
        with dag_maker(dag_id=dag_id, max_active_tasks=16):
            task1 = EmptyOperator(task_id=task_id_1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dr_non_backfill = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        dr_backfill = dag_maker.create_dagrun_after(
            dr_non_backfill, run_type=DagRunType.BACKFILL_JOB, state=State.RUNNING
        )

        ti_backfill = dr_backfill.get_task_instance(task1.task_id)
        ti_non_backfill = dr_non_backfill.get_task_instance(task1.task_id)

        ti_backfill.state = State.SCHEDULED
        ti_non_backfill.state = State.SCHEDULED

        session.merge(dr_backfill)
        session.merge(ti_backfill)
        session.merge(ti_non_backfill)
        session.flush()

        queued_tis = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(queued_tis) == 2
        assert {x.key for x in queued_tis} == {ti_non_backfill.key, ti_backfill.key}
        session.rollback()

    def test_find_executable_task_instances_pool(self, dag_maker):
        dag_id = "SchedulerJobTest.test_find_executable_task_instances_pool"
        task_id_1 = "dummy"
        task_id_2 = "dummydummy"
        session = settings.Session()
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            EmptyOperator(task_id=task_id_1, pool="a", priority_weight=2)
            EmptyOperator(task_id=task_id_2, pool="b", priority_weight=1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        dr2 = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED)

        tis = [
            dr1.get_task_instance(task_id_1, session=session),
            dr1.get_task_instance(task_id_2, session=session),
            dr2.get_task_instance(task_id_1, session=session),
            dr2.get_task_instance(task_id_2, session=session),
        ]
        tis.sort(key=lambda ti: ti.key)
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        pool = Pool(pool="a", slots=1, description="haha", include_deferred=False)
        pool2 = Pool(pool="b", slots=100, description="haha", include_deferred=False)
        session.add(pool)
        session.add(pool2)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        session.flush()
        assert len(res) == 3
        res_keys = []
        for ti in res:
            res_keys.append(ti.key)
        assert tis[0].key in res_keys
        assert tis[2].key in res_keys
        assert tis[3].key in res_keys
        session.rollback()

    @pytest.mark.parametrize(
        ("state", "total_executed_ti"),
        [
            (DagRunState.SUCCESS, 0),
            (DagRunState.FAILED, 0),
            (DagRunState.RUNNING, 2),
            (DagRunState.QUEUED, 0),
        ],
    )
    def test_find_executable_task_instances_only_running_dagruns(
        self, state, total_executed_ti, dag_maker, session
    ):
        """Test that only task instances of 'running' dagruns are executed"""
        dag_id = "SchedulerJobTest.test_find_executable_task_instances_only_running_dagruns"
        task_id_1 = "dummy"
        task_id_2 = "dummydummy"

        with dag_maker(dag_id=dag_id, session=session):
            EmptyOperator(task_id=task_id_1)
            EmptyOperator(task_id=task_id_2)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun(state=state)

        tis = dr.task_instances
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        session.flush()
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        session.flush()
        assert total_executed_ti == len(res)

    def test_find_executable_task_instances_order_logical_date(self, dag_maker):
        """
        Test that task instances follow logical_date order priority. If two dagruns with
        different logical dates are scheduled, tasks with earliest dagrun logical date will first
        be executed
        """
        dag_id_1 = "SchedulerJobTest.test_find_executable_task_instances_order_logical_date-a"
        dag_id_2 = "SchedulerJobTest.test_find_executable_task_instances_order_logical_date-b"
        task_id = "task-a"
        session = settings.Session()
        with dag_maker(dag_id=dag_id_1, max_active_tasks=16, session=session):
            EmptyOperator(task_id=task_id)
        dr1 = dag_maker.create_dagrun(logical_date=DEFAULT_DATE + timedelta(hours=1))

        with dag_maker(dag_id=dag_id_2, max_active_tasks=16, session=session):
            EmptyOperator(task_id=task_id)
        dr2 = dag_maker.create_dagrun()

        dr1 = session.merge(dr1, load=False)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        tis = dr1.task_instances + dr2.task_instances
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        session.flush()
        assert [ti.key for ti in res] == [tis[1].key]
        session.rollback()

    def test_find_executable_task_instances_order_priority(self, dag_maker):
        dag_id_1 = "SchedulerJobTest.test_find_executable_task_instances_order_priority-a"
        dag_id_2 = "SchedulerJobTest.test_find_executable_task_instances_order_priority-b"
        task_id = "task-a"
        session = settings.Session()
        with dag_maker(dag_id=dag_id_1, max_active_tasks=16, session=session):
            EmptyOperator(task_id=task_id, priority_weight=1)
        dr1 = dag_maker.create_dagrun()

        with dag_maker(dag_id=dag_id_2, max_active_tasks=16, session=session):
            EmptyOperator(task_id=task_id, priority_weight=4)
        dr2 = dag_maker.create_dagrun()

        dr1 = session.merge(dr1, load=False)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        tis = dr1.task_instances + dr2.task_instances
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        session.flush()
        assert [ti.key for ti in res] == [tis[1].key]
        session.rollback()

    def test_find_executable_task_instances_executor(self, dag_maker, mock_executors):
        """
        Test that tasks for all executors are set to queued, if space allows it
        """
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id = "SchedulerJobTest.test_find_executable_task_instances_executor"

        with dag_maker(dag_id=dag_id):
            op1 = EmptyOperator(task_id="dummy1")  # No executor specified, runs on default executor
            op2 = EmptyOperator(task_id="dummy2", executor="default_exec")
            op3 = EmptyOperator(task_id="dummy3", executor="default.exec.module.path")
            op4 = EmptyOperator(task_id="dummy4", executor="secondary_exec")
            op5 = EmptyOperator(task_id="dummy5", executor="secondary.exec.module.path")

        dag_run = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ti1 = dag_run.get_task_instance(op1.task_id, session)
        ti2 = dag_run.get_task_instance(op2.task_id, session)
        ti3 = dag_run.get_task_instance(op3.task_id, session)
        ti4 = dag_run.get_task_instance(op4.task_id, session)
        ti5 = dag_run.get_task_instance(op5.task_id, session)

        tis_tuple = (ti1, ti2, ti3, ti4, ti5)
        for ti in tis_tuple:
            ti.state = State.SCHEDULED

        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

        assert len(res) == 5
        res_ti_keys = [res_ti.key for res_ti in res]
        for ti in tis_tuple:
            assert ti.key in res_ti_keys

    @conf_vars({("core", "multi_team"): "true"})
    def test_find_executable_task_instances_executor_with_teams(self, dag_maker, mock_executors, session):
        """
        Test that tasks are correctly routed to team-specific executors when multi-team is enabled
        """
        clear_db_teams()
        clear_db_dag_bundles()

        team1 = Team(name="team_a")
        team2 = Team(name="team_b")
        session.add_all([team1, team2])
        session.flush()

        bundle1 = DagBundleModel(name="bundle_a")
        bundle2 = DagBundleModel(name="bundle_b")
        bundle1.teams.append(team1)
        bundle2.teams.append(team2)
        session.add_all([bundle1, bundle2])
        session.flush()

        mock_executors[0].team_name = "team_a"
        mock_executors[1].team_name = "team_b"

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            op1 = EmptyOperator(task_id="task_a_default")  # No explicit executor - should use team's default
            op2 = EmptyOperator(
                task_id="task_a_explicit", executor="default_exec"
            )  # Team-specific explicit executor
        dr1 = dag_maker.create_dagrun()

        with dag_maker(dag_id="dag_b", bundle_name="bundle_b", session=session):
            op3 = EmptyOperator(task_id="task_b_default")  # Team b's default
            op4 = EmptyOperator(task_id="task_b_explicit", executor="secondary_exec")  # Team b explicit
        dr2 = dag_maker.create_dagrun()

        # DAG with no team (global)
        with dag_maker(dag_id="dag_global", session=session):
            op5 = EmptyOperator(task_id="task_global")  # Global task - any executor
        dr3 = dag_maker.create_dagrun()

        tis = [
            dr1.get_task_instance(op1.task_id, session),
            dr1.get_task_instance(op2.task_id, session),
            dr2.get_task_instance(op3.task_id, session),
            dr2.get_task_instance(op4.task_id, session),
            dr3.get_task_instance(op5.task_id, session),
        ]

        for ti in tis:
            ti.state = State.SCHEDULED
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

        # All tasks should be queued since they have valid executor mappings
        assert len(res) == 5

        # Verify that each task is routed to the correct executor
        executor_to_tis = self.job_runner._executor_to_tis(res, session)

        # Team pi tasks should go to mock_executors[0] (configured for team_pi)
        a_tis_in_executor = [ti for ti in executor_to_tis.get(mock_executors[0], []) if ti.dag_id == "dag_a"]
        assert len(a_tis_in_executor) == 2

        # Team rho tasks should go to mock_executors[1] (configured for team_rho)
        b_tis_in_executor = [ti for ti in executor_to_tis.get(mock_executors[1], []) if ti.dag_id == "dag_b"]
        assert len(b_tis_in_executor) == 2

        # Global task should go to the default executor (scheduler_job.executor)
        global_tis_in_executor = [
            ti for ti in executor_to_tis.get(scheduler_job.executor, []) if ti.dag_id == "dag_global"
        ]
        assert len(global_tis_in_executor) == 1

        # Verify no cross-contamination: team pi tasks should not be in team rho executor and vice versa
        a_tis_in_wrong_executor = [
            ti for ti in executor_to_tis.get(mock_executors[1], []) if ti.dag_id == "dag_a"
        ]
        assert len(a_tis_in_wrong_executor) == 0

        b_tis_in_wrong_executor = [
            ti for ti in executor_to_tis.get(mock_executors[0], []) if ti.dag_id == "dag_b"
        ]
        assert len(b_tis_in_wrong_executor) == 0

    def test_find_executable_task_instances_order_priority_with_pools(self, dag_maker):
        """
        The scheduler job should pick tasks with higher priority for execution
        even if different pools are involved.
        """
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id = "SchedulerJobTest.test_find_executable_task_instances_order_priority_with_pools"

        session.add(Pool(pool="pool1", slots=32, include_deferred=False))
        session.add(Pool(pool="pool2", slots=32, include_deferred=False))

        with dag_maker(dag_id=dag_id, max_active_tasks=2):
            op1 = EmptyOperator(task_id="dummy1", priority_weight=1, pool="pool1")
            op2 = EmptyOperator(task_id="dummy2", priority_weight=2, pool="pool2")
            op3 = EmptyOperator(task_id="dummy3", priority_weight=3, pool="pool1")

        dag_run = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ti1 = dag_run.get_task_instance(op1.task_id, session)
        ti2 = dag_run.get_task_instance(op2.task_id, session)
        ti3 = dag_run.get_task_instance(op3.task_id, session)

        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        ti3.state = State.SCHEDULED

        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

        assert len(res) == 2
        assert ti3.key == res[0].key
        assert ti2.key == res[1].key

        session.rollback()

    def test_find_executable_task_instances_order_logical_date_and_priority(self, dag_maker):
        dag_id_1 = "SchedulerJobTest.test_find_executable_task_instances_order_logical_date_and_priority-a"
        dag_id_2 = "SchedulerJobTest.test_find_executable_task_instances_order_logical_date_and_priority-b"
        task_id = "task-a"
        session = settings.Session()
        with dag_maker(dag_id=dag_id_1, max_active_tasks=16, session=session):
            EmptyOperator(task_id=task_id, priority_weight=1)
        dr1 = dag_maker.create_dagrun()

        with dag_maker(dag_id=dag_id_2, max_active_tasks=16, session=session):
            EmptyOperator(task_id=task_id, priority_weight=4)
        dr2 = dag_maker.create_dagrun(logical_date=DEFAULT_DATE + timedelta(hours=1))

        dr1 = session.merge(dr1, load=False)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        tis = dr1.task_instances + dr2.task_instances
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        session.flush()
        assert [ti.key for ti in res] == [tis[1].key]
        session.rollback()

    def test_find_executable_task_instances_in_default_pool(self, dag_maker, mock_executor):
        set_default_pool_slots(1)

        dag_id = "SchedulerJobTest.test_find_executable_task_instances_in_default_pool"
        with dag_maker(dag_id=dag_id):
            op1 = EmptyOperator(task_id="dummy1")
            op2 = EmptyOperator(task_id="dummy2")
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)

        session = settings.Session()

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        dr2 = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED, state=State.RUNNING)

        ti1 = dr1.get_task_instance(op1.task_id, session)
        ti2 = dr2.get_task_instance(op2.task_id, session)
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED

        session.flush()

        # Two tasks w/o pool up for execution and our default pool size is 1
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 1

        ti2.state = State.RUNNING
        session.flush()

        # One task w/o pool up for execution and one task running
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 0

        session.rollback()
        session.close()

    def test_queued_task_instances_fails_with_missing_dag(self, dag_maker, session):
        """Check that task instances of missing DAGs are failed"""
        dag_id = "SchedulerJobTest.test_find_executable_task_instances_not_in_dagbag"
        task_id_1 = "dummy"
        task_id_2 = "dummydummy"

        with dag_maker(dag_id=dag_id, session=session, default_args={"max_active_tis_per_dag": 1}):
            EmptyOperator(task_id=task_id_1)
            EmptyOperator(task_id=task_id_2)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner.scheduler_dag_bag = mock.MagicMock()
        self.job_runner.scheduler_dag_bag.get_dag_for_run.return_value = None

        dr = dag_maker.create_dagrun(state=DagRunState.RUNNING)

        tis = dr.task_instances
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        session.flush()
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        session.flush()
        assert len(res) == 0
        tis = dr.get_task_instances(session=session)
        assert len(tis) == 2
        assert all(ti.state == State.FAILED for ti in tis)

    def test_nonexistent_pool(self, dag_maker):
        dag_id = "SchedulerJobTest.test_nonexistent_pool"
        with dag_maker(dag_id=dag_id, max_active_tasks=16):
            EmptyOperator(task_id="dummy_wrong_pool", pool="this_pool_doesnt_exist")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dr = dag_maker.create_dagrun()

        ti = dr.task_instances[0]
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        session.flush()
        assert len(res) == 0
        session.rollback()

    def test_infinite_pool(self, dag_maker):
        dag_id = "SchedulerJobTest.test_infinite_pool"
        with dag_maker(dag_id=dag_id, max_active_tasks=16):
            EmptyOperator(task_id="dummy", pool="infinite_pool")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.state = State.SCHEDULED
        session.merge(ti)
        infinite_pool = Pool(
            pool="infinite_pool",
            slots=-1,
            description="infinite pool",
            include_deferred=False,
        )
        session.add(infinite_pool)
        session.commit()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        session.flush()
        assert len(res) == 1
        session.rollback()

    def test_not_enough_pool_slots(self, caplog, dag_maker):
        dag_id = "SchedulerJobTest.test_test_not_enough_pool_slots"
        with dag_maker(dag_id=dag_id, max_active_tasks=16):
            EmptyOperator(task_id="cannot_run", pool="some_pool", pool_slots=4)
            EmptyOperator(task_id="can_run", pool="some_pool", pool_slots=1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()
        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.state = State.SCHEDULED
        session.merge(ti)
        ti = dr.task_instances[1]
        ti.state = State.SCHEDULED
        session.merge(ti)
        some_pool = Pool(pool="some_pool", slots=2, description="my pool", include_deferred=False)
        session.add(some_pool)
        session.commit()
        with caplog.at_level(logging.WARNING):
            self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
            assert (
                "Not executing <TaskInstance: "
                "SchedulerJobTest.test_test_not_enough_pool_slots.cannot_run test [scheduled]>. "
                "Requested pool slots (4) are greater than total pool slots: '2' for pool: some_pool"
                in caplog.text
            )

        assert (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == dag_id, TaskInstance.state == State.SCHEDULED)
            .count()
            == 1
        )
        assert (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == dag_id, TaskInstance.state == State.QUEUED)
            .count()
            == 1
        )

        session.flush()
        session.rollback()

    def test_find_executable_task_instances_none(self, dag_maker):
        dag_id = "SchedulerJobTest.test_find_executable_task_instances_none"
        task_id_1 = "dummy"
        with dag_maker(dag_id=dag_id, max_active_tasks=16):
            EmptyOperator(task_id=task_id_1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        assert len(self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)) == 0
        session.rollback()

    def test_tis_for_queued_dagruns_are_not_run(self, dag_maker):
        """
        This tests that tis from queued dagruns are not queued
        """
        dag_id = "test_tis_for_queued_dagruns_are_not_run"
        task_id_1 = "dummy"

        with dag_maker(dag_id):
            task1 = EmptyOperator(task_id=task_id_1)
        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        dr2 = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()
        ti1 = dr1.get_task_instance(task1.task_id)
        ti2 = dr2.get_task_instance(task1.task_id)
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(ti2)
        session.flush()
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

        assert len(res) == 1
        assert ti2.key == res[0].key
        ti1.refresh_from_db()
        ti2.refresh_from_db()
        assert ti1.state == State.SCHEDULED
        assert ti2.state == State.QUEUED

    @pytest.mark.parametrize("active_state", [TaskInstanceState.RUNNING, TaskInstanceState.QUEUED])
    def test_find_executable_task_instances_concurrency(self, dag_maker, active_state, session):
        """
        We verify here that, with varying amounts of queued / running / scheduled tasks,
        the correct number of TIs are queued
        """
        dag_id = "check_MAT_dag"
        with dag_maker(dag_id=dag_id, max_active_tasks=2, session=session):
            EmptyOperator(task_id="task_1")
            EmptyOperator(task_id="task_2")
            EmptyOperator(task_id="task_3")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, run_id="run_1", session=session)
        dr2 = dag_maker.create_dagrun_after(
            dr1, run_type=DagRunType.SCHEDULED, run_id="run_2", session=session
        )
        dr3 = dag_maker.create_dagrun_after(
            dr2, run_type=DagRunType.SCHEDULED, run_id="run_3", session=session
        )

        # set 2 tis in dr1 to running
        # no more can be queued
        t1, t2, t3 = dr1.get_task_instances(session=session)
        t1.state = active_state
        t2.state = active_state
        t3.state = State.SCHEDULED
        session.merge(t1)
        session.merge(t2)
        session.merge(t3)
        # set 1 ti from dr1 to running
        # one can be queued
        t1, t2, t3 = dr2.get_task_instances(session=session)
        t1.state = active_state
        t2.state = State.SCHEDULED
        t3.state = State.SCHEDULED
        session.merge(t1)
        session.merge(t2)
        session.merge(t3)
        # set 0 tis from dr1 to running
        # two can be queued
        t1, t2, t3 = dr3.get_task_instances(session=session)
        t1.state = State.SCHEDULED
        t2.state = State.SCHEDULED
        t3.state = State.SCHEDULED
        session.merge(t1)
        session.merge(t2)
        session.merge(t3)

        session.flush()

        queued_tis = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        queued_runs = Counter([x.run_id for x in queued_tis])
        assert queued_runs["run_1"] == 0
        assert queued_runs["run_2"] == 1
        assert queued_runs["run_3"] == 2

        session.commit()
        session.query(TaskInstance).all()

        # now we still have max tis running so no more will be queued
        queued_tis = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert queued_tis == []

        session.rollback()

    # TODO: This is a hack, I think I need to just remove the setting and have it on always
    def test_find_executable_task_instances_max_active_tis_per_dag(self, dag_maker):
        dag_id = "SchedulerJobTest.test_find_executable_task_instances_max_active_tis_per_dag"
        with dag_maker(dag_id=dag_id, max_active_tasks=16):
            task1 = EmptyOperator(task_id="dummy", max_active_tis_per_dag=2)
            task2 = EmptyOperator(task_id="dummy2")

        executor = MockExecutor(do_update=True)

        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        dr2 = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED)
        dr3 = dag_maker.create_dagrun_after(dr2, run_type=DagRunType.SCHEDULED)

        ti1_1 = dr1.get_task_instance(task1.task_id)
        ti2 = dr1.get_task_instance(task2.task_id)

        ti1_1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti2)
        session.flush()

        with mock.patch("airflow.executors.executor_loader.ExecutorLoader.load_executor") as loader_mock:
            loader_mock.side_effect = executor.get_mock_loader_side_effect()
            res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

            assert len(res) == 2

            ti1_1.state = State.RUNNING
            ti2.state = State.RUNNING
            ti1_2 = dr2.get_task_instance(task1.task_id)
            ti1_2.state = State.SCHEDULED
            session.merge(ti1_1)
            session.merge(ti2)
            session.merge(ti1_2)
            session.flush()

            res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

            assert len(res) == 1

            ti1_2.state = State.RUNNING
            ti1_3 = dr3.get_task_instance(task1.task_id)
            ti1_3.state = State.SCHEDULED
            session.merge(ti1_2)
            session.merge(ti1_3)
            session.flush()

            res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

            assert len(res) == 0

            ti1_1.state = State.SCHEDULED
            ti1_2.state = State.SCHEDULED
            ti1_3.state = State.SCHEDULED
            session.merge(ti1_1)
            session.merge(ti1_2)
            session.merge(ti1_3)
            session.flush()

            res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

            assert len(res) == 2

            ti1_1.state = State.RUNNING
            ti1_2.state = State.SCHEDULED
            ti1_3.state = State.SCHEDULED
            session.merge(ti1_1)
            session.merge(ti1_2)
            session.merge(ti1_3)
            session.flush()

            res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

            assert len(res) == 1
            session.rollback()

    def test_change_state_for_executable_task_instances_no_tis_with_state(self, dag_maker):
        dag_id = "SchedulerJobTest.test_change_state_for__no_tis_with_state"
        task_id_1 = "dummy"
        with dag_maker(dag_id=dag_id, max_active_tasks=2):
            task1 = EmptyOperator(task_id=task_id_1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        dr2 = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED)
        dr3 = dag_maker.create_dagrun_after(dr2, run_type=DagRunType.SCHEDULED)

        ti1 = dr1.get_task_instance(task1.task_id)
        ti2 = dr2.get_task_instance(task1.task_id)
        ti3 = dr3.get_task_instance(task1.task_id)
        ti1.state = State.RUNNING
        ti2.state = State.RUNNING
        ti3.state = State.RUNNING
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=100, session=session)
        assert len(res) == 0

        session.rollback()

    def test_find_executable_task_instances_not_enough_pool_slots_for_first(self, dag_maker):
        set_default_pool_slots(1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id = "SchedulerJobTest.test_find_executable_task_instances_not_enough_pool_slots_for_first"
        with dag_maker(dag_id=dag_id):
            op1 = EmptyOperator(task_id="dummy1", priority_weight=2, pool_slots=2)
            op2 = EmptyOperator(task_id="dummy2", priority_weight=1, pool_slots=1)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ti1 = dr1.get_task_instance(op1.task_id, session)
        ti2 = dr1.get_task_instance(op2.task_id, session)
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        session.flush()

        # Schedule ti with lower priority,
        # because the one with higher priority is limited by a concurrency limit
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 1
        assert res[0].key == ti2.key

        session.rollback()

    def test_find_executable_task_instances_not_enough_dag_concurrency_for_first(self, dag_maker):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id_1 = (
            "SchedulerJobTest.test_find_executable_task_instances_not_enough_dag_concurrency_for_first-a"
        )
        dag_id_2 = (
            "SchedulerJobTest.test_find_executable_task_instances_not_enough_dag_concurrency_for_first-b"
        )

        with dag_maker(dag_id=dag_id_1, max_active_tasks=1):
            op1a = EmptyOperator(task_id="dummy1-a", priority_weight=2)
            op1b = EmptyOperator(task_id="dummy1-b", priority_weight=2)
        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        with dag_maker(dag_id=dag_id_2):
            op2 = EmptyOperator(task_id="dummy2", priority_weight=1)
        dr2 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ti1a = dr1.get_task_instance(op1a.task_id, session)
        ti1b = dr1.get_task_instance(op1b.task_id, session)
        ti2 = dr2.get_task_instance(op2.task_id, session)
        ti1a.state = State.RUNNING
        ti1b.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        session.flush()

        # Schedule ti with lower priority,
        # because the one with higher priority is limited by a concurrency limit
        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        assert len(res) == 1
        assert res[0].key == ti2.key

        session.rollback()

    def test_find_executable_task_instances_not_enough_task_concurrency_for_first(self, dag_maker):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id = "SchedulerJobTest.test_find_executable_task_instances_not_enough_task_concurrency_for_first"

        with dag_maker(dag_id=dag_id):
            op1a = EmptyOperator(task_id="dummy1-a", priority_weight=2, max_active_tis_per_dag=1)
            op1b = EmptyOperator(task_id="dummy1-b", priority_weight=1)
        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        dr2 = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED)

        ti1a = dr1.get_task_instance(op1a.task_id, session)
        ti1b = dr1.get_task_instance(op1b.task_id, session)
        ti2a = dr2.get_task_instance(op1a.task_id, session)
        ti1a.state = State.RUNNING
        ti1b.state = State.SCHEDULED
        ti2a.state = State.SCHEDULED
        session.flush()

        # Schedule ti with lower priority,
        # because the one with higher priority is limited by a concurrency limit
        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        assert len(res) == 1
        assert res[0].key == ti1b.key

        session.rollback()

    def test_find_executable_task_instances_task_concurrency_per_dagrun_for_first(self, dag_maker):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id = "SchedulerJobTest.test_find_executable_task_instances_task_concurrency_per_dagrun_for_first"

        with dag_maker(dag_id=dag_id):
            op1a = EmptyOperator(task_id="dummy1-a", priority_weight=2, max_active_tis_per_dagrun=1)
            op1b = EmptyOperator(task_id="dummy1-b", priority_weight=1)
        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        dr2 = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED)

        ti1a = dr1.get_task_instance(op1a.task_id, session)
        ti1b = dr1.get_task_instance(op1b.task_id, session)
        ti2a = dr2.get_task_instance(op1a.task_id, session)
        ti1a.state = State.RUNNING
        ti1b.state = State.SCHEDULED
        ti2a.state = State.SCHEDULED
        session.flush()

        # Schedule ti with higher priority,
        # because it's running in a different DAG run with 0 active tis
        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        assert len(res) == 1
        assert res[0].key == ti2a.key

        session.rollback()

    def test_find_executable_task_instances_not_enough_task_concurrency_per_dagrun_for_first(self, dag_maker):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id = (
            "SchedulerJobTest"
            ".test_find_executable_task_instances_not_enough_task_concurrency_per_dagrun_for_first"
        )

        with dag_maker(dag_id=dag_id):
            op1a = EmptyOperator.partial(
                task_id="dummy1-a", priority_weight=2, max_active_tis_per_dagrun=1
            ).expand_kwargs([{"inputs": 1}, {"inputs": 2}])
            op1b = EmptyOperator(task_id="dummy1-b", priority_weight=1)
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ti1a0 = dr.get_task_instance(op1a.task_id, session, map_index=0)
        ti1a1 = dr.get_task_instance(op1a.task_id, session, map_index=1)
        ti1b = dr.get_task_instance(op1b.task_id, session)
        ti1a0.state = State.RUNNING
        ti1a1.state = State.SCHEDULED
        ti1b.state = State.SCHEDULED
        session.flush()

        # Schedule ti with lower priority,
        # because the one with higher priority is limited by a concurrency limit
        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        assert len(res) == 1
        assert res[0].key == ti1b.key

        session.rollback()

    def test_find_executable_task_instances_negative_open_pool_slots(self, dag_maker):
        """
        Pools with negative open slots should not block other pools.
        Negative open slots can happen when reducing the number of total slots in a pool
        while tasks are running in that pool.
        """
        set_default_pool_slots(0)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        pool1 = Pool(pool="pool1", slots=1, include_deferred=False)
        pool2 = Pool(pool="pool2", slots=1, include_deferred=False)

        session.add(pool1)
        session.add(pool2)

        dag_id = "SchedulerJobTest.test_find_executable_task_instances_negative_open_pool_slots"
        with dag_maker(dag_id=dag_id):
            op1 = EmptyOperator(task_id="op1", pool="pool1")
            op2 = EmptyOperator(task_id="op2", pool="pool2", pool_slots=2)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ti1 = dr1.get_task_instance(op1.task_id, session)
        ti2 = dr1.get_task_instance(op2.task_id, session)
        ti1.state = State.SCHEDULED
        ti2.state = State.RUNNING
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        assert len(res) == 1
        assert res[0].key == ti1.key

        session.rollback()

    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.gauge")
    def test_emit_pool_starving_tasks_metrics(self, mock_stats_gauge, dag_maker):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id = "SchedulerJobTest.test_emit_pool_starving_tasks_metrics"
        with dag_maker(dag_id=dag_id):
            op = EmptyOperator(task_id="op", pool_slots=2)

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ti = dr.get_task_instance(op.task_id, session)
        ti.state = State.SCHEDULED

        set_default_pool_slots(1)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 0

        mock_stats_gauge.assert_has_calls(
            [
                mock.call("scheduler.tasks.starving", 1),
                mock.call(f"pool.starving_tasks.{Pool.DEFAULT_POOL_NAME}", 1),
                mock.call("pool.starving_tasks", 1, tags={"pool_name": Pool.DEFAULT_POOL_NAME}),
            ],
            any_order=True,
        )
        mock_stats_gauge.reset_mock()

        set_default_pool_slots(2)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 1

        mock_stats_gauge.assert_has_calls(
            [
                mock.call("scheduler.tasks.starving", 0),
                mock.call(f"pool.starving_tasks.{Pool.DEFAULT_POOL_NAME}", 0),
                mock.call("pool.starving_tasks", 0, tags={"pool_name": Pool.DEFAULT_POOL_NAME}),
            ],
            any_order=True,
        )

        session.rollback()
        session.close()

    def test_enqueue_task_instances_with_queued_state(self, dag_maker, session):
        dag_id = "SchedulerJobTest.test_enqueue_task_instances_with_queued_state"
        task_id_1 = "dummy"
        session = settings.Session()
        with dag_maker(dag_id=dag_id, start_date=DEFAULT_DATE, session=session):
            task1 = EmptyOperator(task_id=task_id_1)

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun()
        ti1 = dr1.get_task_instance(task1.task_id, session)

        with patch.object(BaseExecutor, "queue_workload") as mock_queue_workload:
            self.job_runner._enqueue_task_instances_with_queued_state(
                [ti1], executor=scheduler_job.executor, session=session
            )

        assert mock_queue_workload.called
        session.rollback()

    @pytest.mark.parametrize("state", [State.FAILED, State.SUCCESS])
    def test_enqueue_task_instances_sets_ti_state_to_None_if_dagrun_in_finish_state(self, state, dag_maker):
        """This tests that task instances whose dagrun is in finished state are not queued"""
        dag_id = "SchedulerJobTest.test_enqueue_task_instances_with_queued_state"
        task_id_1 = "dummy"
        session = settings.Session()
        with dag_maker(dag_id=dag_id, start_date=DEFAULT_DATE, session=session):
            task1 = EmptyOperator(task_id=task_id_1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(state=state)
        ti = dr1.get_task_instance(task1.task_id, session)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        with patch.object(BaseExecutor, "queue_workload") as mock_queue_workload:
            self.job_runner._enqueue_task_instances_with_queued_state(
                [ti], executor=scheduler_job.executor, session=session
            )
        session.flush()
        ti.refresh_from_db(session=session)
        assert ti.state == State.NONE
        mock_queue_workload.assert_not_called()

    @pytest.mark.parametrize(
        ("task1_exec", "task2_exec"),
        [
            ("default_exec", "default_exec"),
            ("default_exec", "secondary_exec"),
            ("secondary_exec", "secondary_exec"),
        ],
    )
    @pytest.mark.usefixtures("mock_executors")
    def test_critical_section_enqueue_task_instances(self, task1_exec, task2_exec, dag_maker, session):
        dag_id = "SchedulerJobTest.test_execute_task_instances"
        # important that len(tasks) is less than max_active_tasks
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if max_active_tasks was 3,
        # we could execute arbitrarily many tasks in the second run
        with dag_maker(dag_id=dag_id, max_active_tasks=3, session=session):
            task1 = EmptyOperator(task_id="t1", executor=task1_exec)
            task2 = EmptyOperator(task_id="t2", executor=task2_exec)
            task3 = EmptyOperator(task_id="t3", executor=task2_exec)
            task4 = EmptyOperator(task_id="t4", executor=task2_exec)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # create first dag run with 3 running tasks

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, session=session)

        dr1_ti1 = dr1.get_task_instance(task1.task_id, session)
        dr1_ti2 = dr1.get_task_instance(task2.task_id, session)
        dr1_ti3 = dr1.get_task_instance(task3.task_id, session)
        dr1_ti4 = dr1.get_task_instance(task4.task_id, session)
        dr1_ti1.state = State.RUNNING
        dr1_ti2.state = State.RUNNING
        dr1_ti3.state = State.RUNNING
        dr1_ti4.state = State.SCHEDULED
        session.flush()

        def _count_tis(states):
            return session.scalar(
                select(func.count(TaskInstance.task_id)).where(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.state.in_(states),
                )
            )

        assert dr1.state == State.RUNNING
        assert _count_tis([TaskInstanceState.RUNNING]) == 3

        # create second dag run
        dr2 = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED, session=session)
        dr2_ti1 = dr2.get_task_instance(task1.task_id, session)
        dr2_ti2 = dr2.get_task_instance(task2.task_id, session)
        dr2_ti3 = dr2.get_task_instance(task3.task_id, session)
        dr2_ti4 = dr2.get_task_instance(task4.task_id, session)
        # manually set to scheduled so we can pick them up
        dr2_ti1.state = State.SCHEDULED
        dr2_ti2.state = State.SCHEDULED
        dr2_ti3.state = State.SCHEDULED
        dr2_ti4.state = State.SCHEDULED
        session.flush()

        assert dr2.state == State.RUNNING

        num_queued = self.job_runner._critical_section_enqueue_task_instances(session=session)
        assert num_queued == 3

        # check that max_active_tasks is respected
        assert _count_tis([TaskInstanceState.RUNNING, TaskInstanceState.QUEUED]) == 6

        # this doesn't really tell us anything since we set these values manually, but hey
        dr1_counter = Counter(x.state for x in dr1.get_task_instances(session=session))
        assert dr1_counter[State.RUNNING] == 3
        assert dr1_counter[State.SCHEDULED] == 1

        # this is the more meaningful bit
        # three of dr2's tasks should be queued since that's max active tasks
        # and max active tasks is evaluated per-dag-run
        dr2_counter = Counter(x.state for x in dr2.get_task_instances(session=session))
        assert dr2_counter[State.QUEUED] == 3
        assert dr2_counter[State.SCHEDULED] == 1

        num_queued = self.job_runner._critical_section_enqueue_task_instances(session=session)
        assert num_queued == 0

    def test_execute_task_instances_limit_second_executor(self, dag_maker, mock_executors):
        dag_id = "SchedulerJobTest.test_execute_task_instances_limit"
        task_id_1 = "dummy_task"
        task_id_2 = "dummy_task_2"
        session = settings.Session()
        # important that len(tasks) is less than max_active_tasks
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if max_active_tasks was 3,
        # we could execute arbitrarily many tasks in the second run
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task1 = EmptyOperator(task_id=task_id_1, executor="default_exec")
            task2 = EmptyOperator(task_id=task_id_2, executor="secondary_exec")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        def _create_dagruns():
            dagrun = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.RUNNING)
            yield dagrun
            for _ in range(3):
                dagrun = dag_maker.create_dagrun_after(
                    dagrun,
                    run_type=DagRunType.SCHEDULED,
                    state=State.RUNNING,
                )
                yield dagrun

        tis1 = []
        tis2 = []
        for dr in _create_dagruns():
            ti1 = dr.get_task_instance(task1.task_id, session)
            tis1.append(ti1)
            ti2 = dr.get_task_instance(task2.task_id, session)
            tis2.append(ti2)
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.flush()
        scheduler_job.max_tis_per_query = 6
        # First pass we'll grab 6 of the 8 tasks (limited by max_tis_per_query)
        res = self.job_runner._critical_section_enqueue_task_instances(session)
        assert res == 6
        session.flush()
        for ti in tis1[:3] + tis2[:3]:
            ti.refresh_from_db(session)
            assert ti.state == TaskInstanceState.QUEUED
        for ti in tis1[3:] + tis2[3:]:
            ti.refresh_from_db(session)
            assert ti.state == TaskInstanceState.SCHEDULED

        # The remaining TIs are queued
        res = self.job_runner._critical_section_enqueue_task_instances(session)
        assert res == 2
        session.flush()

        for ti in tis1 + tis2:
            ti.refresh_from_db(session)
            assert ti.state == State.QUEUED

    @pytest.mark.parametrize(
        ("task1_exec", "task2_exec"),
        [
            ("default_exec", "default_exec"),
            ("default_exec", "secondary_exec"),
            ("secondary_exec", "secondary_exec"),
        ],
    )
    def test_execute_task_instances_limit(self, task1_exec, task2_exec, dag_maker, mock_executors):
        dag_id = "SchedulerJobTest.test_execute_task_instances_limit"
        task_id_1 = "dummy_task"
        task_id_2 = "dummy_task_2"
        session = settings.Session()
        # important that len(tasks) is less than max_active_tasks
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if max_active_tasks was 3,
        # we could execute arbitrarily many tasks in the second run
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task1 = EmptyOperator(task_id=task_id_1, executor=task1_exec)
            task2 = EmptyOperator(task_id=task_id_2, executor=task2_exec)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        def _create_dagruns():
            dagrun = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.RUNNING)
            yield dagrun
            for _ in range(3):
                dagrun = dag_maker.create_dagrun_after(
                    dagrun,
                    run_type=DagRunType.SCHEDULED,
                    state=State.RUNNING,
                )
                yield dagrun

        tis = []
        for dr in _create_dagruns():
            ti1 = dr.get_task_instance(task1.task_id, session)
            tis.append(ti1)
            ti2 = dr.get_task_instance(task2.task_id, session)
            tis.append(ti2)
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.flush()
        scheduler_job.max_tis_per_query = 2

        total_enqueued = self.job_runner._critical_section_enqueue_task_instances(session)
        assert total_enqueued == 2

    def test_execute_task_instances_limit_slots(self, dag_maker, mock_executors):
        dag_id = "SchedulerJobTest.test_execute_task_instances_limit"
        task_id_1 = "dummy_task"
        task_id_2 = "dummy_task_2"
        session = settings.Session()
        # important that len(tasks) is less than max_active_tasks
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if max_active_tasks was 3,
        # we could execute arbitrarily many tasks in the second run
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task1 = EmptyOperator(task_id=task_id_1, executor="default_exec")
            task2 = EmptyOperator(task_id=task_id_2, executor="secondary_exec")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        def _create_dagruns():
            dagrun = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.RUNNING)
            yield dagrun
            for _ in range(3):
                dagrun = dag_maker.create_dagrun_after(
                    dagrun,
                    run_type=DagRunType.SCHEDULED,
                    state=State.RUNNING,
                )
                yield dagrun

        tis = []
        for dr in _create_dagruns():
            ti1 = dr.get_task_instance(task1.task_id, session)
            tis.append(ti1)
            ti2 = dr.get_task_instance(task2.task_id, session)
            tis.append(ti2)
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.flush()

        scheduler_job.max_tis_per_query = 8
        scheduler_job.executor.slots_available = 2  # Limit only the default executor to 2 slots.
        # Check that we don't "overfill" an executor when the max tis per query is larger than slots
        # available. Of the 8 tasks returned by the query, the default executor will only take 2 and the
        # secondary executor will take 4 (since only 4 of the 8 TIs in the result will be for that executor)
        res = self.job_runner._critical_section_enqueue_task_instances(session)
        assert res == 6

        scheduler_job.executor.slots_available = 6  # The default executor has more slots freed now and
        # will take the other two TIs.
        res = self.job_runner._critical_section_enqueue_task_instances(session)
        assert res == 2

    def test_execute_task_instances_unlimited(self, dag_maker, mock_executor):
        """Test that max_tis_per_query=0 is unlimited"""
        dag_id = "SchedulerJobTest.test_execute_task_instances_unlimited"
        task_id_1 = "dummy_task"
        task_id_2 = "dummy_task_2"
        session = settings.Session()

        with dag_maker(dag_id=dag_id, max_active_tasks=1024, session=session):
            task1 = EmptyOperator(task_id=task_id_1)
            task2 = EmptyOperator(task_id=task_id_2)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        def _create_dagruns():
            dagrun = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.RUNNING)
            yield dagrun
            for _ in range(19):
                dagrun = dag_maker.create_dagrun_after(
                    dagrun,
                    run_type=DagRunType.SCHEDULED,
                    state=State.RUNNING,
                )
                yield dagrun

        for dr in _create_dagruns():
            ti1 = dr.get_task_instance(task1.task_id, session)
            ti2 = dr.get_task_instance(task2.task_id, session)
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.flush()
        scheduler_job.max_tis_per_query = 0
        scheduler_job.executor.parallelism = 32
        scheduler_job.executor.slots_available = 31

        res = self.job_runner._critical_section_enqueue_task_instances(session)
        # 20 dag runs * 2 tasks each = 40, but limited by number of slots available
        assert res == 31
        session.rollback()

    @pytest.mark.parametrize(
        ("task1_exec", "task2_exec"),
        [
            ("default_exec", "default_exec"),
            ("default_exec", "secondary_exec"),
            ("secondary_exec", "secondary_exec"),
        ],
    )
    def test_execute_task_instances_unlimited_multiple_executors(
        self, task1_exec, task2_exec, dag_maker, mock_executors
    ):
        """Test that max_tis_per_query=0 is unlimited"""
        dag_id = "SchedulerJobTest.test_execute_task_instances_unlimited"
        task_id_1 = "dummy_task"
        task_id_2 = "dummy_task_2"
        session = settings.Session()

        with dag_maker(dag_id=dag_id, max_active_tasks=1024, session=session):
            task1 = EmptyOperator(task_id=task_id_1, executor=task1_exec)
            task2 = EmptyOperator(task_id=task_id_2, executor=task2_exec)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        def _create_dagruns():
            dagrun = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.RUNNING)
            yield dagrun
            for _ in range(40):
                dagrun = dag_maker.create_dagrun_after(
                    dagrun,
                    run_type=DagRunType.SCHEDULED,
                    state=State.RUNNING,
                )
                yield dagrun

        for dr in _create_dagruns():
            ti1 = dr.get_task_instance(task1.task_id, session)
            ti2 = dr.get_task_instance(task2.task_id, session)
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.flush()
        scheduler_job.max_tis_per_query = 0
        for executor in mock_executors:
            executor.parallelism = 32
            executor.slots_available = 31

        total_enqueued = 0
        with conf_vars({("core", "parallelism"): "40"}):
            # 40 dag runs * 2 tasks each = 80. Two executors have capacity for 61 concurrent jobs, but they
            # together respect core.parallelism and will not run more in aggregate then that allows.
            total_enqueued += self.job_runner._critical_section_enqueue_task_instances(session)

        if task1_exec != task2_exec:
            # Two executors will execute up to core parallelism
            assert total_enqueued == 40
        else:
            # A single executor will only run up to its available slots
            assert total_enqueued == 31
        session.rollback()

    def test_adopt_or_reset_orphaned_tasks(self, dag_maker, session):
        with dag_maker("test_execute_helper_reset_orphaned_tasks", session=session):
            op1 = EmptyOperator(task_id="op1")

        scheduler_job = Job()
        session.add(scheduler_job)
        session.flush()

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.QUEUED
        ti.queued_by_job_id = scheduler_job.id
        session.flush()

        dr2 = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED)
        ti2 = dr2.get_task_instance(task_id=op1.task_id, session=session)
        ti2.state = State.QUEUED
        ti2.queued_by_job_id = scheduler_job.id
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=0)
        self.job_runner.adopt_or_reset_orphaned_tasks()

        ti = dr.get_task_instance(task_id=op1.task_id, session=session)

        assert ti.state == State.NONE

        ti2 = dr2.get_task_instance(task_id=op1.task_id, session=session)
        assert ti2.state == State.NONE, "Tasks run by Backfill Jobs should be treated the same"

    def test_adopt_or_reset_orphaned_tasks_multiple_executors(self, dag_maker, mock_executors):
        """
        Test that with multiple executors configured tasks are sorted correctly and handed off to the
        correct executor for adoption.
        """
        session = settings.Session()
        with dag_maker("test_execute_helper_reset_orphaned_tasks_multiple_executors"):
            op1 = EmptyOperator(task_id="op1")
            op2 = EmptyOperator(task_id="op2", executor="default_exec")
            op3 = EmptyOperator(task_id="op3", executor="secondary_exec")

        dr = dag_maker.create_dagrun()
        scheduler_job = Job()
        session.add(scheduler_job)
        session.commit()
        ti1 = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti2 = dr.get_task_instance(task_id=op2.task_id, session=session)
        ti3 = dr.get_task_instance(task_id=op3.task_id, session=session)
        tis = [ti1, ti2, ti3]
        for ti in tis:
            ti.state = State.QUEUED
            ti.queued_by_job_id = scheduler_job.id
        session.commit()

        new_scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=new_scheduler_job, num_runs=0)

        self.job_runner.adopt_or_reset_orphaned_tasks()

        # Default executor is called for ti1 (no explicit executor override uses default) and ti2 (where we
        # explicitly marked that for execution by the default executor)
        try:
            mock_executors[0].try_adopt_task_instances.assert_called_once_with([ti1, ti2])
        except AssertionError:
            # The order of the TIs given to try_adopt_task_instances is not consistent, so check the other
            # order first before allowing AssertionError to fail the test
            mock_executors[0].try_adopt_task_instances.assert_called_once_with([ti2, ti1])

        # Second executor called for ti3
        mock_executors[1].try_adopt_task_instances.assert_called_once_with([ti3])

    def test_adopt_sets_last_heartbeat_on_adopt(self, dag_maker, session, mock_executor):
        with dag_maker("test_adopt_sets_last_heartbeat_on_adopt", session=session):
            op1 = EmptyOperator(task_id="op1")

        old_scheduler_job = Job()
        session.add(old_scheduler_job)
        session.flush()

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.QUEUED
        ti.queued_by_job_id = old_scheduler_job.id
        ti.last_heartbeat_at = None
        session.commit()

        # Executor adopts all TIs (returns empty list to reset), so TI is adopted
        mock_executor.try_adopt_task_instances.return_value = []

        new_scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=new_scheduler_job, num_runs=0)
        self.job_runner.adopt_or_reset_orphaned_tasks(session=session)

        ti.refresh_from_db(session=session)
        assert ti.state == State.QUEUED
        assert ti.queued_by_job_id == new_scheduler_job.id
        assert ti.last_heartbeat_at is not None

    def test_adopt_sets_dagrun_conf_when_none(self, dag_maker, session, mock_executor):
        with dag_maker("test_adopt_sets_dagrun_conf_when_none", session=session):
            op1 = EmptyOperator(task_id="op1")

        old_scheduler_job = Job()
        session.add(old_scheduler_job)
        session.flush()

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        # Ensure conf starts as None
        dr.conf = None
        session.merge(dr)
        session.flush()
        dr = session.scalar(select(DagRun).where(DagRun.id == dr.id))
        assert dr.conf is None

        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.QUEUED
        ti.queued_by_job_id = old_scheduler_job.id
        session.commit()

        # Executor adopts all TIs (returns empty list to reset), so TI is adopted
        mock_executor.try_adopt_task_instances.return_value = []

        new_scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=new_scheduler_job, num_runs=0)
        self.job_runner.adopt_or_reset_orphaned_tasks(session=session)

        # DagRun.conf should be set to {} on adoption when it was None
        session.refresh(dr)
        assert dr.conf == {}

    def test_purge_without_heartbeat_skips_when_missing_dag_version(self, dag_maker, session, caplog):
        with dag_maker("test_purge_without_heartbeat_skips_when_missing_dag_version", session=session):
            EmptyOperator(task_id="task")

        dag_run = dag_maker.create_dagrun(run_id="test_run", state=DagRunState.RUNNING)

        mock_executor = MagicMock()
        scheduler_job = Job(executor=mock_executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        ti = dag_run.get_task_instance(task_id="task", session=session)
        ti.state = TaskInstanceState.RUNNING
        ti.queued_by_job_id = scheduler_job.id
        ti.last_heartbeat_at = timezone.utcnow() - timedelta(hours=1)
        # Simulate missing dag_version
        ti.dag_version_id = None
        session.merge(ti)
        session.commit()

        with caplog.at_level("WARNING", logger="airflow.jobs.scheduler_job_runner"):
            self.job_runner._purge_task_instances_without_heartbeats([ti], session=session)

        # Should log a warning and skip processing
        assert any("DAG Version not found for TaskInstance" in rec.message for rec in caplog.records)
        mock_executor.send_callback.assert_not_called()
        # State should be unchanged (not failed)
        ti.refresh_from_db(session=session)
        assert ti.state == TaskInstanceState.RUNNING

    @staticmethod
    def mock_failure_callback(context):
        pass

    @conf_vars({("scheduler", "num_stuck_in_queued_retries"): "2"})
    def test_handle_stuck_queued_tasks_multiple_attempts(self, dag_maker, session, mock_executors):
        """Verify that tasks stuck in queued will be rescheduled up to N times."""
        with dag_maker("test_fail_stuck_queued_tasks_multiple_executors"):
            EmptyOperator(task_id="op1", on_failure_callback=TestSchedulerJob.mock_failure_callback)
            EmptyOperator(task_id="op2", executor="default_exec")

        def _queue_tasks(tis):
            for ti in tis:
                ti.state = "queued"
                ti.queued_dttm = timezone.utcnow()
            session.commit()

        run_id = str(uuid4())
        dr = dag_maker.create_dagrun(run_id=run_id)

        tis = dr.get_task_instances(session=session)
        _queue_tasks(tis=tis)
        scheduler_job = Job()
        scheduler = SchedulerJobRunner(job=scheduler_job, num_runs=0)
        # job_runner._reschedule_stuck_task = MagicMock()
        scheduler._task_queued_timeout = -300  # always in violation of timeout

        with _loader_mock(mock_executors):
            scheduler._handle_tasks_stuck_in_queued()
        # If the task gets stuck in queued once, we reset it to scheduled
        tis = dr.get_task_instances(session=session)
        assert [x.state for x in tis] == ["scheduled", "scheduled"]
        assert [x.queued_dttm for x in tis] == [None, None]

        _queue_tasks(tis=tis)
        log_events = [
            x.event for x in session.scalars(select(Log).where(Log.run_id == run_id).order_by(Log.id)).all()
        ]
        assert log_events == [
            "stuck in queued reschedule",
            "stuck in queued reschedule",
        ]

        with _loader_mock(mock_executors):
            scheduler._handle_tasks_stuck_in_queued()

        log_events = [
            x.event for x in session.scalars(select(Log).where(Log.run_id == run_id).order_by(Log.id)).all()
        ]
        assert log_events == [
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
        ]
        mock_executors[0].fail.assert_not_called()
        tis = dr.get_task_instances(session=session)
        assert [x.state for x in tis] == ["scheduled", "scheduled"]
        _queue_tasks(tis=tis)

        with _loader_mock(mock_executors):
            scheduler._handle_tasks_stuck_in_queued()
        log_events = [
            x.event for x in session.scalars(select(Log).where(Log.run_id == run_id).order_by(Log.id)).all()
        ]
        assert log_events == [
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued tries exceeded",
            "stuck in queued tries exceeded",
        ]

        mock_executors[
            0
        ].send_callback.assert_called_once()  # this should only be called for the task that has a callback
        states = [x.state for x in dr.get_task_instances(session=session)]
        assert states == ["failed", "failed"]
        mock_executors[0].fail.assert_called()

    @conf_vars({("scheduler", "num_stuck_in_queued_retries"): "2"})
    def test_handle_stuck_queued_tasks_reschedule_sensors(self, dag_maker, session, mock_executors):
        """Reschedule sensors go in and out of running repeatedly using the same try_number
        Make sure that they get three attempts per reschedule, not 3 attempts per try_number"""
        with dag_maker("test_fail_stuck_queued_tasks_multiple_executors"):
            EmptyOperator(task_id="op1", on_failure_callback=TestSchedulerJob.mock_failure_callback)
            EmptyOperator(task_id="op2", executor="default_exec")

        def _queue_tasks(tis):
            for ti in tis:
                ti.state = "queued"
                ti.queued_dttm = timezone.utcnow()
            session.commit()

        def _add_running_event(tis):
            for ti in tis:
                updated_entry = Log(
                    dttm=timezone.utcnow(),
                    dag_id=ti.dag_id,
                    task_id=ti.task_id,
                    map_index=ti.map_index,
                    event="running",
                    run_id=ti.run_id,
                    try_number=ti.try_number,
                )
                session.add(updated_entry)

        run_id = str(uuid4())
        dr = dag_maker.create_dagrun(run_id=run_id)

        tis = dr.get_task_instances(session=session)
        _queue_tasks(tis=tis)
        scheduler_job = Job()
        scheduler = SchedulerJobRunner(job=scheduler_job, num_runs=0)
        # job_runner._reschedule_stuck_task = MagicMock()
        scheduler._task_queued_timeout = -300  # always in violation of timeout

        with _loader_mock(mock_executors):
            scheduler._handle_tasks_stuck_in_queued()
        # If the task gets stuck in queued once, we reset it to scheduled
        tis = dr.get_task_instances(session=session)
        assert [x.state for x in tis] == ["scheduled", "scheduled"]
        assert [x.queued_dttm for x in tis] == [None, None]

        _queue_tasks(tis=tis)
        log_events = [
            x.event for x in session.scalars(select(Log).where(Log.run_id == run_id).order_by(Log.id)).all()
        ]
        assert log_events == [
            "stuck in queued reschedule",
            "stuck in queued reschedule",
        ]

        with _loader_mock(mock_executors):
            scheduler._handle_tasks_stuck_in_queued()

        log_events = [
            x.event for x in session.scalars(select(Log).where(Log.run_id == run_id).order_by(Log.id)).all()
        ]
        assert log_events == [
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
        ]
        mock_executors[0].fail.assert_not_called()
        tis = dr.get_task_instances(session=session)
        assert [x.state for x in tis] == ["scheduled", "scheduled"]

        _add_running_event(tis)  # This should "reset" the count of stuck queued

        for _ in range(3):  # Should be able to be stuck 3 more times before failing
            _queue_tasks(tis=tis)
            with _loader_mock(mock_executors):
                scheduler._handle_tasks_stuck_in_queued()
            tis = dr.get_task_instances(session=session)

        log_events = [
            x.event for x in session.scalars(select(Log).where(Log.run_id == run_id).order_by(Log.id)).all()
        ]
        assert log_events == [
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "running",
            "running",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued reschedule",
            "stuck in queued tries exceeded",
            "stuck in queued tries exceeded",
        ]

        mock_executors[
            0
        ].send_callback.assert_called_once()  # this should only be called for the task that has a callback
        states = [x.state for x in dr.get_task_instances(session=session)]
        assert states == ["failed", "failed"]
        mock_executors[0].fail.assert_called()

    def test_revoke_task_not_imp_tolerated(self, dag_maker, session, caplog):
        """Test that if executor no implement revoke_task then we don't blow up."""
        with dag_maker("test_fail_stuck_queued_tasks"):
            op1 = EmptyOperator(task_id="op1")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.QUEUED
        ti.queued_dttm = timezone.utcnow() - timedelta(minutes=15)
        session.commit()
        from airflow.executors.local_executor import LocalExecutor

        assert "revoke_task" in BaseExecutor.__dict__
        # this is just verifying that LocalExecutor is good enough for this test
        # in that it does not implement revoke_task
        assert "revoke_task" not in LocalExecutor.__dict__
        scheduler_job = Job(executor=LocalExecutor())
        job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=0)
        job_runner._task_queued_timeout = 300
        job_runner._handle_tasks_stuck_in_queued()

    def test_executor_end_called(self, mock_executors):
        """
        Test to make sure executor.end gets called with a successful scheduler loop run
        """
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)
        scheduler_job.executor.end.assert_called_once()

    def test_executor_end_called_multiple_executors(self, mock_executors):
        """
        Test to make sure executor.end gets called on all executors with a successful scheduler loop run
        """
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)
        scheduler_job.executor.end.assert_called_once()
        for executor in scheduler_job.executors:
            executor.end.assert_called_once()

    def test_cleanup_methods_all_called(self):
        """
        Test to make sure all cleanup methods are called when the scheduler loop has an exception
        """
        scheduler_job = Job(executor=mock.MagicMock(slots_available=8))
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._run_scheduler_loop = mock.MagicMock(side_effect=RuntimeError("oops"))
        scheduler_job.executor.end = mock.MagicMock(side_effect=RuntimeError("triple oops"))

        with pytest.raises(RuntimeError, match="oops"):
            run_job(scheduler_job, execute_callable=self.job_runner._execute)

        scheduler_job.executor.end.assert_called_once()

    def test_cleanup_methods_all_called_multiple_executors(self, mock_executors):
        """
        Test to make sure all cleanup methods are called when the scheduler loop has an exception
        """
        scheduler_job = Job()

        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._run_scheduler_loop = mock.MagicMock(side_effect=RuntimeError("oops"))
        scheduler_job.executor.end = mock.MagicMock(side_effect=RuntimeError("triple oops"))

        with pytest.raises(RuntimeError, match="oops"):
            run_job(scheduler_job, execute_callable=self.job_runner._execute)

        for executor in scheduler_job.executors:
            executor.end.assert_called_once()

    def test_queued_dagruns_stops_creating_when_max_active_is_reached(self, dag_maker):
        """This tests that queued dagruns stops creating once max_active_runs is reached"""
        with dag_maker(max_active_runs=10) as dag:
            EmptyOperator(task_id="mytask")

        session = settings.Session()
        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        session = settings.Session()
        orm_dag = session.get(DagModel, dag.dag_id)
        assert orm_dag is not None
        for _ in range(20):
            self.job_runner._create_dag_runs([orm_dag], session)
        drs = session.query(DagRun).all()
        assert len(drs) == 10

        for dr in drs:
            dr.state = State.RUNNING
            session.merge(dr)
        session.commit()
        assert session.query(DagRun.state).filter(DagRun.state == State.RUNNING).count() == 10
        for _ in range(20):
            self.job_runner._create_dag_runs([orm_dag], session)
        assert session.query(DagRun).count() == 10
        assert session.query(DagRun.state).filter(DagRun.state == State.RUNNING).count() == 10
        assert session.query(DagRun.state).filter(DagRun.state == State.QUEUED).count() == 0
        assert orm_dag.next_dagrun_create_after is None

    def test_runs_are_created_after_max_active_runs_was_reached(self, dag_maker, session):
        """
        Test that when creating runs once max_active_runs is reached the runs does not stick
        """
        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Use catchup=True to ensure proper run creation behavior after max_active_runs is no longer reached
        with dag_maker(max_active_runs=1, session=session, catchup=True) as dag:
            # Need to use something that doesn't immediately get marked as success by the scheduler
            BashOperator(task_id="task", bash_command="true")

        dag_run = dag_maker.create_dagrun(state=State.RUNNING, session=session, run_type=DagRunType.SCHEDULED)

        # Reach max_active_runs
        for _ in range(3):
            self.job_runner._do_scheduling(session)

        # Complete dagrun
        # Add dag_run back in to the session (_do_scheduling does an expunge_all)
        dag_run = session.merge(dag_run)
        session.refresh(dag_run)
        dag_run.get_task_instance(task_id="task", session=session).state = State.SUCCESS

        # create new run
        for _ in range(3):
            self.job_runner._do_scheduling(session)

        # Assert that new runs has created
        dag_runs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(dag_runs) == 2

    @pytest.mark.parametrize(
        ("ti_state", "final_ti_span_status"),
        [
            pytest.param(State.SUCCESS, SpanStatus.ENDED, id="dr_ended_successfully"),
            pytest.param(State.RUNNING, SpanStatus.ACTIVE, id="dr_still_running"),
        ],
    )
    def test_recreate_unhealthy_scheduler_spans_if_needed(self, ti_state, final_ti_span_status, dag_maker):
        with dag_maker(
            dag_id="test_recreate_unhealthy_scheduler_spans_if_needed",
            start_date=DEFAULT_DATE,
            max_active_runs=1,
            dagrun_timeout=datetime.timedelta(seconds=60),
        ):
            EmptyOperator(task_id="dummy")

        session = settings.Session()

        old_job = Job()
        old_job.job_type = SchedulerJobRunner.job_type

        session.add(old_job)
        session.commit()

        assert old_job.is_alive() is False

        new_job = Job()
        new_job.job_type = SchedulerJobRunner.job_type
        session.add(new_job)
        session.flush()

        self.job_runner = SchedulerJobRunner(job=new_job)
        self.job_runner.active_spans = ThreadSafeDict()
        assert len(self.job_runner.active_spans.get_all()) == 0

        dr = dag_maker.create_dagrun()
        dr.state = State.RUNNING
        dr.span_status = SpanStatus.ACTIVE
        dr.scheduled_by_job_id = old_job.id

        ti = dr.get_task_instances(session=session)[0]
        ti.state = ti_state
        ti.start_date = timezone.utcnow()
        ti.span_status = SpanStatus.ACTIVE
        ti.queued_by_job_id = old_job.id
        session.merge(ti)
        session.merge(dr)
        session.commit()

        assert dr.scheduled_by_job_id != self.job_runner.job.id
        assert dr.scheduled_by_job_id == old_job.id
        assert dr.run_id is not None
        assert dr.state == State.RUNNING
        assert dr.span_status == SpanStatus.ACTIVE
        assert self.job_runner.active_spans.get("dr:" + str(dr.id)) is None

        assert self.job_runner.active_spans.get("ti:" + ti.id) is None
        assert ti.state == ti_state
        assert ti.span_status == SpanStatus.ACTIVE

        self.job_runner._recreate_unhealthy_scheduler_spans_if_needed(dr, session)

        assert self.job_runner.active_spans.get("dr:" + str(dr.id)) is not None

        if final_ti_span_status == SpanStatus.ACTIVE:
            assert self.job_runner.active_spans.get("ti:" + ti.id) is not None
            assert len(self.job_runner.active_spans.get_all()) == 2
        else:
            assert self.job_runner.active_spans.get("ti:" + ti.id) is None
            assert len(self.job_runner.active_spans.get_all()) == 1

        assert dr.span_status == SpanStatus.ACTIVE
        assert ti.span_status == final_ti_span_status

    def test_end_spans_of_externally_ended_ops(self, dag_maker):
        with dag_maker(
            dag_id="test_end_spans_of_externally_ended_ops",
            start_date=DEFAULT_DATE,
            max_active_runs=1,
            dagrun_timeout=datetime.timedelta(seconds=60),
        ):
            EmptyOperator(task_id="dummy")

        session = settings.Session()

        job = Job()
        job.job_type = SchedulerJobRunner.job_type
        session.add(job)

        self.job_runner = SchedulerJobRunner(job=job)
        self.job_runner.active_spans = ThreadSafeDict()
        assert len(self.job_runner.active_spans.get_all()) == 0

        dr = dag_maker.create_dagrun()
        dr.state = State.SUCCESS
        dr.span_status = SpanStatus.SHOULD_END

        ti = dr.get_task_instances(session=session)[0]
        ti.state = State.SUCCESS
        ti.span_status = SpanStatus.SHOULD_END
        ti.context_carrier = {}
        session.merge(ti)
        session.merge(dr)
        session.commit()

        dr_span = Trace.start_root_span(span_name="dag_run_span", start_as_current=False)
        ti_span = Trace.start_child_span(span_name="ti_span", start_as_current=False)

        self.job_runner.active_spans.set("dr:" + str(dr.id), dr_span)
        self.job_runner.active_spans.set("ti:" + ti.id, ti_span)

        assert dr.span_status == SpanStatus.SHOULD_END
        assert ti.span_status == SpanStatus.SHOULD_END

        assert self.job_runner.active_spans.get("dr:" + str(dr.id)) is not None
        assert self.job_runner.active_spans.get("ti:" + ti.id) is not None

        self.job_runner._end_spans_of_externally_ended_ops(session)

        assert dr.span_status == SpanStatus.ENDED
        assert ti.span_status == SpanStatus.ENDED

        assert self.job_runner.active_spans.get("dr:" + str(dr.id)) is None
        assert self.job_runner.active_spans.get("ti:" + ti.id) is None

    @pytest.mark.parametrize(
        ("state", "final_span_status"),
        [
            pytest.param(State.SUCCESS, SpanStatus.ENDED, id="dr_ended_successfully"),
            pytest.param(State.RUNNING, SpanStatus.NEEDS_CONTINUANCE, id="dr_still_running"),
        ],
    )
    def test_end_active_spans(self, state, final_span_status, dag_maker):
        with dag_maker(
            dag_id="test_end_active_spans",
            start_date=DEFAULT_DATE,
            max_active_runs=1,
            dagrun_timeout=datetime.timedelta(seconds=60),
        ):
            EmptyOperator(task_id="dummy")

        session = settings.Session()

        job = Job()
        job.job_type = SchedulerJobRunner.job_type

        self.job_runner = SchedulerJobRunner(job=job)
        self.job_runner.active_spans = ThreadSafeDict()
        assert len(self.job_runner.active_spans.get_all()) == 0

        dr = dag_maker.create_dagrun()
        dr.state = state
        dr.span_status = SpanStatus.ACTIVE

        ti = dr.get_task_instances(session=session)[0]
        ti.state = state
        ti.span_status = SpanStatus.ACTIVE
        ti.context_carrier = {}
        session.merge(ti)
        session.merge(dr)
        session.commit()

        dr_span = Trace.start_root_span(span_name="dag_run_span", start_as_current=False)
        ti_span = Trace.start_child_span(span_name="ti_span", start_as_current=False)

        self.job_runner.active_spans.set("dr:" + str(dr.id), dr_span)
        self.job_runner.active_spans.set("ti:" + ti.id, ti_span)

        assert dr.span_status == SpanStatus.ACTIVE
        assert ti.span_status == SpanStatus.ACTIVE

        assert self.job_runner.active_spans.get("dr:" + str(dr.id)) is not None
        assert self.job_runner.active_spans.get("ti:" + ti.id) is not None
        assert len(self.job_runner.active_spans.get_all()) == 2

        self.job_runner._end_active_spans(session)

        assert dr.span_status == final_span_status
        assert ti.span_status == final_span_status

        assert self.job_runner.active_spans.get("dr:" + str(dr.id)) is None
        assert self.job_runner.active_spans.get("ti:" + ti.id) is None
        assert len(self.job_runner.active_spans.get_all()) == 0

    def test_dagrun_timeout_verify_max_active_runs(self, dag_maker):
        """
        Test if a dagrun will not be scheduled if max_dag_runs
        has been reached and dagrun_timeout is not reached
        """
        with dag_maker(
            dag_id="test_scheduler_verify_max_active_runs_and_dagrun_timeout",
            start_date=DEFAULT_DATE,
            max_active_runs=1,
            dagrun_timeout=datetime.timedelta(seconds=60),
        ) as dag:
            EmptyOperator(task_id="dummy")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        session = settings.Session()
        orm_dag = session.get(DagModel, dag.dag_id)
        assert orm_dag is not None

        self.job_runner._create_dag_runs([orm_dag], session)
        self.job_runner._start_queued_dagruns(session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        assert orm_dag.next_dagrun_create_after is None
        # But we should record the date of _what run_ it would be
        assert isinstance(orm_dag.next_dagrun, datetime.datetime)
        assert isinstance(orm_dag.next_dagrun_data_interval_start, datetime.datetime)
        assert isinstance(orm_dag.next_dagrun_data_interval_end, datetime.datetime)

        # Should be scheduled as dagrun_timeout has passed
        dr.start_date = timezone.utcnow() - datetime.timedelta(days=1)
        session.flush()

        callback = self.job_runner._schedule_dag_run(dr, session)
        session.flush()

        session.refresh(dr)
        assert dr.state == State.FAILED
        session.refresh(orm_dag)
        assert isinstance(orm_dag.next_dagrun, datetime.datetime)
        assert isinstance(orm_dag.next_dagrun_data_interval_start, datetime.datetime)
        assert isinstance(orm_dag.next_dagrun_data_interval_end, datetime.datetime)
        assert isinstance(orm_dag.next_dagrun_create_after, datetime.datetime)

        expected_callback = DagCallbackRequest(
            filepath=dr.dag.relative_fileloc,
            dag_id=dr.dag_id,
            is_failure_callback=True,
            run_id=dr.run_id,
            bundle_name=orm_dag.bundle_name,
            bundle_version=orm_dag.bundle_version,
            context_from_server=DagRunContext(
                dag_run=dr,
                last_ti=dr.get_last_ti(dag, session),
            ),
            msg="timed_out",
        )

        # Verify dag failure callback request is sent
        assert callback == expected_callback

        session.rollback()
        session.close()

    def test_dagrun_timeout_fails_run(self, dag_maker):
        """
        Test if a dagrun will be set failed if timeout, even without max_active_runs
        """
        session = settings.Session()
        with dag_maker(
            dag_id="test_scheduler_fail_dagrun_timeout",
            dagrun_timeout=datetime.timedelta(seconds=60),
            session=session,
        ):
            EmptyOperator(task_id="dummy")

        dr = dag_maker.create_dagrun(start_date=timezone.utcnow() - datetime.timedelta(days=1))

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        callback = self.job_runner._schedule_dag_run(dr, session)
        session.flush()

        session.refresh(dr)
        assert dr.state == State.FAILED

        assert isinstance(callback, DagCallbackRequest)
        assert callback.dag_id == dr.dag_id
        assert callback.run_id == dr.run_id
        assert callback.msg == "timed_out"

        session.rollback()
        session.close()

    def test_dagrun_timeout_fails_run_and_update_next_dagrun(self, dag_maker):
        """
        Test that dagrun timeout fails run and update the next dagrun
        """
        session = settings.Session()
        # Explicitly set catchup=True as test specifically expects runs to be created in date order
        with dag_maker(
            max_active_runs=1,
            dag_id="test_scheduler_fail_dagrun_timeout",
            dagrun_timeout=datetime.timedelta(seconds=60),
            catchup=True,
        ):
            EmptyOperator(task_id="dummy")

        dr = dag_maker.create_dagrun(start_date=timezone.utcnow() - datetime.timedelta(days=1))
        # check that next_dagrun is dr.logical_date
        dag_maker.dag_model.next_dagrun == dr.logical_date
        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._schedule_dag_run(dr, session)
        session.flush()
        session.refresh(dr)
        assert dr.state == State.FAILED
        # check that next_dagrun_create_after has been updated by calculate_dagrun_date_fields
        assert dag_maker.dag_model.next_dagrun_create_after == dr.logical_date + timedelta(days=1)
        # check that no running/queued runs yet
        assert (
            session.query(DagRun).filter(DagRun.state.in_([DagRunState.RUNNING, DagRunState.QUEUED])).count()
            == 0
        )

    @pytest.mark.parametrize(
        ("state", "expected_callback_msg"), [(State.SUCCESS, "success"), (State.FAILED, "task_failure")]
    )
    def test_dagrun_callbacks_are_called(self, state, expected_callback_msg, dag_maker, session):
        """
        Test if DagRun is successful, and if Success callbacks is defined, it is sent to DagFileProcessor.
        """
        with dag_maker(
            dag_id="test_dagrun_callbacks_are_called",
            on_success_callback=lambda x: print("success"),
            on_failure_callback=lambda x: print("failed"),
            session=session,
        ) as dag:
            EmptyOperator(task_id="dummy")

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("dummy", session)
        ti.set_state(state, session)
        session.flush()

        with mock.patch.object(settings, "USE_JOB_SCHEDULE", False):
            self.job_runner._do_scheduling(session)

        expected_callback = DagCallbackRequest(
            filepath=dag.relative_fileloc,
            dag_id=dr.dag_id,
            is_failure_callback=bool(state == State.FAILED),
            run_id=dr.run_id,
            msg=expected_callback_msg,
            bundle_name="dag_maker",
            bundle_version=None,
            context_from_server=DagRunContext(
                dag_run=dr,
                last_ti=ti,
            ),
        )

        # Verify dag failure callback request is sent to file processor
        scheduler_job.executor.callback_sink.send.assert_called_once_with(expected_callback)
        session.rollback()
        session.close()

    @pytest.mark.parametrize(
        ("state", "expected_callback_msg"), [(State.SUCCESS, "success"), (State.FAILED, "task_failure")]
    )
    def test_dagrun_plugins_are_notified(self, state, expected_callback_msg, dag_maker, session):
        """
        Test if DagRun is successful, and if Success callbacks is defined, it is sent to DagFileProcessor.
        """
        with dag_maker(
            dag_id="test_dagrun_callbacks_are_called",
            on_success_callback=lambda x: print("success"),
            on_failure_callback=lambda x: print("failed"),
            session=session,
        ):
            EmptyOperator(task_id="dummy")

        dag_listener.clear()
        get_listener_manager().add_listener(dag_listener)

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun()

        ti = dr.get_task_instance("dummy", session)
        ti.set_state(state, session)

        with mock.patch.object(settings, "USE_JOB_SCHEDULE", False):
            self.job_runner._do_scheduling(session)

        assert len(dag_listener.success) or len(dag_listener.failure)

        dag_listener.success = []
        dag_listener.failure = []

        session.rollback()

    def test_dagrun_timeout_callbacks_are_stored_in_database(self, dag_maker, session):
        with dag_maker(
            dag_id="test_dagrun_timeout_callbacks_are_stored_in_database",
            on_failure_callback=lambda x: print("failed"),
            dagrun_timeout=timedelta(hours=1),
            session=session,
        ) as dag:
            EmptyOperator(task_id="empty")

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        scheduler_job.executor.callback_sink = DatabaseCallbackSink()

        dr = dag_maker.create_dagrun(start_date=DEFAULT_DATE)

        with mock.patch.object(settings, "USE_JOB_SCHEDULE", False):
            self.job_runner._do_scheduling(session)

        callback = (
            session.query(DbCallbackRequest)
            .order_by(DbCallbackRequest.id.desc())
            .first()
            .get_callback_request()
        )

        expected_callback = DagCallbackRequest(
            filepath=dag.relative_fileloc,
            dag_id=dr.dag_id,
            is_failure_callback=True,
            run_id=dr.run_id,
            msg="timed_out",
            bundle_name="dag_maker",
            bundle_version=None,
            context_from_server=DagRunContext(
                dag_run=dr,
                last_ti=dr.get_last_ti(dag, session),
            ),
        )

        assert callback == expected_callback

    def test_dagrun_callbacks_commited_before_sent(self, dag_maker):
        """
        Tests that before any callbacks are sent to the processor, the session is committed. This ensures
        that the dagrun details are up to date when the callbacks are run.
        """
        with dag_maker(dag_id="test_dagrun_callbacks_commited_before_sent"):
            EmptyOperator(task_id="dummy")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._send_dag_callbacks_to_processor = mock.Mock()
        self.job_runner._schedule_dag_run = mock.Mock()

        dr = dag_maker.create_dagrun()
        session = settings.Session()

        ti = dr.get_task_instance("dummy")
        ti.set_state(State.SUCCESS, session)

        with (
            mock.patch.object(settings, "USE_JOB_SCHEDULE", False),
            mock.patch("airflow.jobs.scheduler_job_runner.prohibit_commit") as mock_guard,
        ):
            mock_guard.return_value.__enter__.return_value.commit.side_effect = session.commit

            def mock_schedule_dag_run(*args, **kwargs):
                mock_guard.reset_mock()
                return None

            def mock_send_dag_callbacks_to_processor(*args, **kwargs):
                mock_guard.return_value.__enter__.return_value.commit.assert_called()

            self.job_runner._send_dag_callbacks_to_processor.side_effect = (
                mock_send_dag_callbacks_to_processor
            )
            self.job_runner._schedule_dag_run.side_effect = mock_schedule_dag_run

            self.job_runner._do_scheduling(session)

        # Verify dag failure callback request is sent to file processor
        self.job_runner._send_dag_callbacks_to_processor.assert_called_once()
        # and mock_send_dag_callbacks_to_processor has asserted the callback was sent after a commit

        session.rollback()
        session.close()

    @pytest.mark.parametrize("state", [State.SUCCESS, State.FAILED])
    def test_dagrun_callbacks_are_not_added_when_callbacks_are_not_defined(self, state, dag_maker, session):
        """
        Test if no on_*_callback are defined on DAG, Callbacks not registered and sent to DAG Processor
        """
        with dag_maker(
            dag_id="test_dagrun_callbacks_are_not_added_when_callbacks_are_not_defined",
            session=session,
        ):
            BashOperator(task_id="test_task", bash_command="echo hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._send_dag_callbacks_to_processor = mock.Mock()

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("test_task", session)
        ti.set_state(state, session)

        with mock.patch.object(settings, "USE_JOB_SCHEDULE", False):
            self.job_runner._do_scheduling(session)

        # Verify Callback is not set (i.e is None) when no callbacks are set on DAG
        self.job_runner._send_dag_callbacks_to_processor.assert_called_once()
        call_args = self.job_runner._send_dag_callbacks_to_processor.call_args.args
        assert call_args[0].dag_id == dr.dag_id
        assert call_args[1] is None

        session.rollback()

    @pytest.mark.parametrize(("state", "msg"), [[State.SUCCESS, "success"], [State.FAILED, "task_failure"]])
    def test_dagrun_callbacks_are_added_when_callbacks_are_defined(self, state, msg, dag_maker):
        """
        Test if on_*_callback are defined on DAG, Callbacks ARE registered and sent to DAG Processor
        """
        with dag_maker(
            dag_id="test_dagrun_callbacks_are_added_when_callbacks_are_defined",
            on_failure_callback=lambda: True,
            on_success_callback=lambda: True,
        ):
            BashOperator(task_id="test_task", bash_command="echo hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._send_dag_callbacks_to_processor = mock.Mock()

        session = settings.Session()
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("test_task")
        ti.set_state(state, session)

        with mock.patch.object(settings, "USE_JOB_SCHEDULE", False):
            self.job_runner._do_scheduling(session)

        # Verify Callback is set (i.e is None) when no callbacks are set on DAG
        self.job_runner._send_dag_callbacks_to_processor.assert_called_once()
        call_args = self.job_runner._send_dag_callbacks_to_processor.call_args.args
        assert call_args[0].dag_id == dr.dag_id
        assert call_args[1] is not None
        assert call_args[1].msg == msg
        session.rollback()
        session.close()

    def test_dagrun_notify_called_success(self, dag_maker):
        with dag_maker(
            dag_id="test_dagrun_notify_called",
            on_success_callback=lambda x: print("success"),
            on_failure_callback=lambda x: print("failed"),
        ):
            EmptyOperator(task_id="dummy")

        dag_listener.clear()
        get_listener_manager().add_listener(dag_listener)

        executor = MockExecutor(do_update=False)

        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        session = settings.Session()
        dr = dag_maker.create_dagrun()

        ti = dr.get_task_instance("dummy")
        ti.set_state(State.SUCCESS, session)

        with mock.patch.object(settings, "USE_JOB_SCHEDULE", False):
            self.job_runner._do_scheduling(session)

        assert dag_listener.success[0].dag_id == dr.dag_id
        assert dag_listener.success[0].run_id == dr.run_id
        assert dag_listener.success[0].state == DagRunState.SUCCESS

    def test_do_not_schedule_removed_task(self, dag_maker, session):
        """Test that scheduler doesn't schedule task instances for tasks removed from DAG."""
        interval = datetime.timedelta(days=1)
        dag_id = "test_scheduler_do_not_schedule_removed_task"

        # Create initial DAG with a task
        with dag_maker(
            dag_id=dag_id,
            schedule=interval,
            start_date=DEFAULT_DATE,
        ):
            EmptyOperator(task_id="dummy")

        # Create a dagrun for the initial DAG
        dr = dag_maker.create_dagrun()
        assert dr is not None

        # Verify the task instance was created
        initial_tis = (
            session.query(TaskInstance)
            .filter(TaskInstance.dag_id == dag_id, TaskInstance.task_id == "dummy")
            .all()
        )
        assert len(initial_tis) == 1

        # Update the DAG to remove the task (simulate DAG file change)
        with dag_maker(
            dag_id=dag_id,
            schedule=interval,
            start_date=DEFAULT_DATE,
        ):
            pass  # No tasks in the DAG now

        # Create a new dagrun for the updated DAG
        dr2 = dag_maker.create_dagrun(logical_date=DEFAULT_DATE + interval, run_id="test_run_2")
        assert dr2 is not None

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Try to find executable task instances - should not find any for the removed task
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

        # Should be empty because the task no longer exists in the DAG
        assert res == []

        # Verify no new task instances were created for the removed task in the new dagrun
        new_tis = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == "dummy",
                TaskInstance.run_id == "test_run_2",
            )
            .all()
        )
        assert len(new_tis) == 0

    @pytest.mark.parametrize(
        ("ti_states", "run_state"),
        [
            (["failed", "success"], "failed"),
            (["success", "success"], "success"),
        ],
    )
    def test_dagrun_state_correct(self, ti_states, run_state, dag_maker, session):
        """
        DagRuns with one failed and one incomplete root task -> FAILED
        """
        with dag_maker():

            @task
            def my_task(): ...

            for _ in ti_states:
                my_task()
        dr = dag_maker.create_dagrun(state="running", triggered_by=DagRunTriggeredByType.TIMETABLE)
        for idx, state in enumerate(ti_states):
            dr.task_instances[idx].state = state
        session.commit()
        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._do_scheduling(session)
        assert (
            session.query(DagRun).filter(DagRun.dag_id == dr.dag_id, DagRun.run_id == dr.run_id).one().state
            == run_state
        )

    def test_dagrun_root_after_dagrun_unfinished(self, mock_executor, testing_dag_bundle):
        """
        DagRuns with one successful and one future root task -> SUCCESS

        Noted: the DagRun state could be still in running state during CI.
        """
        dagbag = DagBag(TEST_DAG_FOLDER, include_examples=False)
        sync_bag_to_db(dagbag, "testing", None)
        dag_id = "test_dagrun_states_root_future"

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=2)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        first_run = DagRun.find(dag_id=dag_id)[0]
        ti_ids = [(ti.task_id, ti.state) for ti in first_run.get_task_instances()]

        assert ti_ids == [("current", State.SUCCESS)]
        assert first_run.state in [State.SUCCESS, State.RUNNING]

    def test_scheduler_start_date(self, testing_dag_bundle):
        """
        Test that the scheduler respects start_dates, even when DAGs have run
        """
        dagbag = DagBag(TEST_DAG_FOLDER, include_examples=False)
        with create_session() as session:
            dag_id = "test_start_date_scheduling"
            dag = dagbag.get_dag(dag_id)

            # Deactivate other dags in this file
            other_dag = dagbag.get_dag("test_task_start_date_scheduling")
            other_dag.is_paused_upon_creation = True

            scheduler_dag, _ = sync_dags_to_db([dag, other_dag])
            scheduler_dag.clear()
            assert scheduler_dag.start_date > datetime.datetime.now(timezone.utc)

            scheduler_job = Job(executor=self.null_exec)
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            run_job(scheduler_job, execute_callable=self.job_runner._execute)

            # zero tasks ran
            assert len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()) == 0
            session.commit()
            assert self.null_exec.sorted_tasks == []

            # previously, running this backfill would kick off the Scheduler
            # because it would take the most recent run and start from there
            # That behavior still exists, but now it will only do so if after the
            # start date
            data_interval_end = DEFAULT_DATE + timedelta(days=1)
            scheduler_dag.create_dagrun(
                state="success",
                triggered_by=DagRunTriggeredByType.TIMETABLE,
                run_id="abc123",
                logical_date=DEFAULT_DATE,
                run_type=DagRunType.BACKFILL_JOB,
                data_interval=DataInterval(DEFAULT_DATE, data_interval_end),
                run_after=data_interval_end,
            )
            # one task "ran"
            assert len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()) == 1
            session.commit()

            scheduler_job = Job(executor=self.null_exec)
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            run_job(scheduler_job, execute_callable=self.job_runner._execute)

            # still one task
            assert len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()) == 1
            session.commit()
            assert self.null_exec.sorted_tasks == []

    def test_scheduler_task_start_date_catchup_true(self, testing_dag_bundle):
        """
        Test that with catchup=True, the scheduler respects task start dates that are different from DAG start dates
        """
        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "test_scheduler_dags.py"),
            include_examples=False,
        )
        dag_id = "test_task_start_date_scheduling"
        dag = dagbag.get_dag(dag_id)
        # Explicitly set catchup=True
        dag.catchup = True
        dag.is_paused_upon_creation = False
        dagbag.bag_dag(dag=dag)

        # Deactivate other dags in this file
        other_dag = dagbag.get_dag("test_start_date_scheduling")
        other_dag.is_paused_upon_creation = True
        dagbag.bag_dag(dag=other_dag)

        sync_bag_to_db(dagbag, "testing", None)

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=3)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        session = settings.Session()
        tiq = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id)
        ti1s = tiq.filter(TaskInstance.task_id == "dummy1").all()
        ti2s = tiq.filter(TaskInstance.task_id == "dummy2").all()

        # With catchup=True, future task start dates are respected
        assert len(ti1s) == 0, "Expected no instances for dummy1 (start date in future with catchup=True)"
        assert len(ti2s) >= 2, "Expected multiple instances for dummy2"
        for ti in ti2s:
            assert ti.state == State.SUCCESS

    def test_scheduler_task_start_date_catchup_false(self, testing_dag_bundle):
        """
        Test that with catchup=False, the scheduler ignores task start dates and schedules for the most recent interval
        """
        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "test_scheduler_dags.py"),
            include_examples=False,
        )
        dag_id = "test_task_start_date_scheduling"
        dag = dagbag.get_dag(dag_id)
        dag.catchup = False
        dag.is_paused_upon_creation = False
        dagbag.bag_dag(dag=dag)

        # Deactivate other dags in this file
        other_dag = dagbag.get_dag("test_start_date_scheduling")
        other_dag.is_paused_upon_creation = True
        dagbag.bag_dag(dag=other_dag)

        sync_bag_to_db(dagbag, "testing", None)

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=3)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        session = settings.Session()
        tiq = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id)
        ti1s = tiq.filter(TaskInstance.task_id == "dummy1").all()
        ti2s = tiq.filter(TaskInstance.task_id == "dummy2").all()

        # With catchup=False, future task start dates are ignored
        assert len(ti1s) >= 1, "Expected instances for dummy1 (ignoring future start date with catchup=False)"
        assert len(ti2s) >= 1, "Expected instances for dummy2"

        # Check that both tasks are scheduled for the same recent interval
        if ti1s and ti2s:
            recent_ti1 = ti1s[0]
            recent_ti2 = ti2s[0]
            assert recent_ti1.logical_date == recent_ti2.logical_date, (
                "Both tasks should be scheduled for the same interval"
            )

    def test_scheduler_multiprocessing(self):
        """
        Test that the scheduler can successfully queue multiple dags in parallel
        """
        dagbag = DagBag(TEST_DAG_FOLDER, include_examples=False)
        dag_ids = [
            "test_start_date_scheduling",
            "test_task_start_date_scheduling",
        ]
        for dag_id in dag_ids:
            dag = dagbag.get_dag(dag_id)
            if not dag:
                raise ValueError(f"could not find dag {dag_id}")
            create_scheduler_dag(dag).clear()

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        # zero tasks ran
        dag_id = "test_start_date_scheduling"
        session = settings.Session()
        assert len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()) == 0

    def test_scheduler_verify_pool_full(self, dag_maker, mock_executor):
        """
        Test task instances not queued when pool is full
        """
        with dag_maker(dag_id="test_scheduler_verify_pool_full"):
            BashOperator(
                task_id="dummy",
                pool="test_scheduler_verify_pool_full",
                bash_command="echo hi",
            )

        session = settings.Session()
        pool = Pool(pool="test_scheduler_verify_pool_full", slots=1, include_deferred=False)
        session.add(pool)
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Create 2 dagruns, which will create 2 task instances.
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        self.job_runner._schedule_dag_run(dr, session)
        dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.RUNNING)
        self.job_runner._schedule_dag_run(dr, session)
        session.flush()
        task_instances_list = self.job_runner._executable_task_instances_to_queued(
            max_tis=32, session=session
        )

        assert len(task_instances_list) == 1

    @pytest.mark.need_serialized_dag
    def test_scheduler_verify_pool_full_2_slots_per_task(self, dag_maker, session, mock_executor):
        """
        Test task instances not queued when pool is full.

        Variation with non-default pool_slots
        """
        # Explicitly set catchup=True as tests expect runs to be created in date order
        with dag_maker(
            dag_id="test_scheduler_verify_pool_full_2_slots_per_task",
            start_date=DEFAULT_DATE,
            session=session,
            catchup=True,
        ):
            BashOperator(
                task_id="dummy",
                pool="test_scheduler_verify_pool_full_2_slots_per_task",
                pool_slots=2,
                bash_command="echo hi",
            )

        pool = Pool(pool="test_scheduler_verify_pool_full_2_slots_per_task", slots=6, include_deferred=False)
        session.add(pool)
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Create 5 dagruns, which will create 5 task instances.
        def _create_dagruns():
            dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
            yield dr
            for _ in range(4):
                dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED)
                yield dr

        for dr in _create_dagruns():
            self.job_runner._schedule_dag_run(dr, session)

        task_instances_list = self.job_runner._executable_task_instances_to_queued(
            max_tis=32, session=session
        )

        # As tasks require 2 slots, only 3 can fit into 6 available
        assert len(task_instances_list) == 3

    @pytest.mark.need_serialized_dag
    def test_scheduler_keeps_scheduling_pool_full(self, dag_maker, mock_executor):
        """
        Test task instances in a pool that isn't full keep getting scheduled even when a pool is full.
        """
        session = settings.Session()
        pool_p1 = Pool(pool="test_scheduler_keeps_scheduling_pool_full_p1", slots=1, include_deferred=False)
        pool_p2 = Pool(pool="test_scheduler_keeps_scheduling_pool_full_p2", slots=10, include_deferred=False)
        session.add(pool_p1)
        session.add(pool_p2)
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # We'll use this to create 30 dagruns for each DAG.
        # To increase the chances the TIs from the "full" pool will get
        # retrieved first, we schedule all TIs from the first dag first.
        def _create_dagruns(dag: SerializedDAG):
            next_info = dag.next_dagrun_info(None)
            assert next_info is not None
            for i in range(30):
                yield dag.create_dagrun(
                    run_id=f"scheduled_{i}",
                    run_type=DagRunType.SCHEDULED,
                    logical_date=next_info.logical_date,
                    data_interval=next_info.data_interval,
                    run_after=next_info.run_after,
                    state=DagRunState.RUNNING,
                    triggered_by=DagRunTriggeredByType.TEST,
                    session=session,
                )
                next_info = dag.next_dagrun_info(next_info.data_interval)
                if next_info is None:
                    break

        with dag_maker(
            dag_id="test_scheduler_keeps_scheduling_pool_full_d1",
            start_date=DEFAULT_DATE,
        ) as dag_d1:
            BashOperator(
                task_id="test_scheduler_keeps_scheduling_pool_full_t1",
                pool="test_scheduler_keeps_scheduling_pool_full_p1",
                bash_command="echo hi",
            )
        for dr in _create_dagruns(dag_d1):
            self.job_runner._schedule_dag_run(dr, session)

        with dag_maker(
            dag_id="test_scheduler_keeps_scheduling_pool_full_d2",
            start_date=DEFAULT_DATE,
        ) as dag_d2:
            BashOperator(
                task_id="test_scheduler_keeps_scheduling_pool_full_t2",
                pool="test_scheduler_keeps_scheduling_pool_full_p2",
                bash_command="echo hi",
            )
        for dr in _create_dagruns(dag_d2):
            self.job_runner._schedule_dag_run(dr, session)

        self.job_runner._executable_task_instances_to_queued(max_tis=2, session=session)
        task_instances_list2 = self.job_runner._executable_task_instances_to_queued(
            max_tis=2, session=session
        )

        # Make sure we get TIs from a non-full pool in the 2nd list
        assert len(task_instances_list2) > 0
        assert all(
            task_instance.pool != "test_scheduler_keeps_scheduling_pool_full_p1"
            for task_instance in task_instances_list2
        )

    def test_scheduler_verify_priority_and_slots(self, dag_maker, mock_executor):
        """
        Test task instances with higher priority are not queued
        when pool does not have enough slots.

        Though tasks with lower priority might be executed.
        """
        with dag_maker(dag_id="test_scheduler_verify_priority_and_slots"):
            # Medium priority, not enough slots
            BashOperator(
                task_id="test_scheduler_verify_priority_and_slots_t0",
                pool="test_scheduler_verify_priority_and_slots",
                pool_slots=2,
                priority_weight=2,
                bash_command="echo hi",
            )
            # High priority, occupies first slot
            BashOperator(
                task_id="test_scheduler_verify_priority_and_slots_t1",
                pool="test_scheduler_verify_priority_and_slots",
                pool_slots=1,
                priority_weight=3,
                bash_command="echo hi",
            )
            # Low priority, occupies second slot
            BashOperator(
                task_id="test_scheduler_verify_priority_and_slots_t2",
                pool="test_scheduler_verify_priority_and_slots",
                pool_slots=1,
                priority_weight=1,
                bash_command="echo hi",
            )

        session = settings.Session()
        pool = Pool(pool="test_scheduler_verify_priority_and_slots", slots=2, include_deferred=False)
        session.add(pool)
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun()
        for ti in dr.task_instances:
            ti.state = State.SCHEDULED
            session.merge(ti)
        session.flush()

        task_instances_list = self.job_runner._executable_task_instances_to_queued(
            max_tis=32, session=session
        )

        # Only second and third
        assert len(task_instances_list) == 2

        ti0 = (
            session.query(TaskInstance)
            .filter(TaskInstance.task_id == "test_scheduler_verify_priority_and_slots_t0")
            .first()
        )
        assert ti0.state == State.SCHEDULED

        ti1 = (
            session.query(TaskInstance)
            .filter(TaskInstance.task_id == "test_scheduler_verify_priority_and_slots_t1")
            .first()
        )
        assert ti1.state == State.QUEUED

        ti2 = (
            session.query(TaskInstance)
            .filter(TaskInstance.task_id == "test_scheduler_verify_priority_and_slots_t2")
            .first()
        )
        assert ti2.state == State.QUEUED

    def test_verify_integrity_if_dag_not_changed(self, dag_maker, session):
        # CleanUp
        session.query(SerializedDagModel).filter(
            SerializedDagModel.dag_id == "test_verify_integrity_if_dag_not_changed"
        ).delete(synchronize_session=False)

        with dag_maker(dag_id="test_verify_integrity_if_dag_not_changed") as dag:
            BashOperator(task_id="dummy", bash_command="echo hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        session = settings.Session()
        orm_dag = dag_maker.dag_model
        assert orm_dag is not None

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        # Verify that DagRun.verify_integrity is not called
        with mock.patch("airflow.jobs.scheduler_job_runner.DagRun.verify_integrity") as mock_verify_integrity:
            self.job_runner._schedule_dag_run(dr, session)
            mock_verify_integrity.assert_not_called()
        session.flush()

        tis_count = (
            session.query(func.count(TaskInstance.task_id))
            .filter(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.logical_date == dr.logical_date,
                TaskInstance.task_id == dr.dag.tasks[0].task_id,
                TaskInstance.state == State.SCHEDULED,
            )
            .scalar()
        )
        assert tis_count == 1

        latest_dag_version = DagVersion.get_latest_version(dr.dag_id, session=session)
        for ti in dr.task_instances:
            assert ti.dag_version_id == latest_dag_version.id

        session.rollback()
        session.close()

    def test_verify_integrity_if_dag_changed(self, dag_maker):
        # CleanUp
        with create_session() as session:
            session.query(SerializedDagModel).filter(
                SerializedDagModel.dag_id == "test_verify_integrity_if_dag_changed"
            ).delete(synchronize_session=False)

        with dag_maker(dag_id="test_verify_integrity_if_dag_changed", serialized=False) as dag:
            BashOperator(task_id="dummy", bash_command="echo hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        session = settings.Session()
        orm_dag = dag_maker.dag_model
        assert orm_dag is not None
        SerializedDagModel.write_dag(LazyDeserializedDAG.from_dag(dag), bundle_name="testing")
        assert orm_dag.bundle_version is None

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._create_dag_runs([orm_dag], session)
        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        self.job_runner._schedule_dag_run(dag_run=dr, session=session)
        len(self.job_runner.scheduler_dag_bag.get_dag_for_run(dr, session).tasks) == 1
        dag_version_1 = DagVersion.get_latest_version(dr.dag_id, session=session)
        assert dr.dag_versions[-1].id == dag_version_1.id

        # Now let's say the DAG got updated (new task got added)
        BashOperator(task_id="bash_task_1", dag=dag, bash_command="echo hi")
        SerializedDagModel.write_dag(
            LazyDeserializedDAG.from_dag(dag), bundle_name="testing", session=session
        )
        session.commit()
        dag_version_2 = DagVersion.get_latest_version(dr.dag_id, session=session)
        assert dag_version_2 != dag_version_1

        self.job_runner._schedule_dag_run(dr, session)
        session.commit()

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]
        assert dr.dag_versions[-1].id == dag_version_2.id
        assert len(self.job_runner.scheduler_dag_bag.get_dag_for_run(dr, session).tasks) == 2

        if SQLALCHEMY_V_1_4:
            tis_count = (
                session.query(func.count(TaskInstance.task_id))
                .filter(
                    TaskInstance.dag_id == dr.dag_id,
                    TaskInstance.logical_date == dr.logical_date,
                    TaskInstance.state == State.SCHEDULED,
                )
                .scalar()
            )
        if SQLALCHEMY_V_2_0:
            tis_count = session.scalar(
                select(func.count(TaskInstance.task_id)).where(
                    TaskInstance.dag_id == dr.dag_id,
                    TaskInstance.logical_date == dr.logical_date,
                    TaskInstance.state == State.SCHEDULED,
                )
            )
        assert tis_count == 2

        latest_dag_version = DagVersion.get_latest_version(dr.dag_id, session=session)
        assert dr.dag_versions[-1].id == latest_dag_version.id

        session.rollback()
        session.close()

    def test_verify_integrity_not_called_for_versioned_bundles(self, dag_maker, session):
        with dag_maker("test_verify_integrity_if_dag_not_changed") as dag:
            BashOperator(task_id="dummy", bash_command="echo hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        orm_dag = dag_maker.dag_model
        assert orm_dag is not None
        self.job_runner._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]
        # Simulate versioned bundle by adding a version to dr.bundle_version
        dr.bundle_version = "1"
        session.merge(dr)
        session.commit()
        drs = session.query(DagRun).options(joinedload(DagRun.task_instances)).all()
        dr = drs[0]
        assert dr.bundle_version == "1"
        dag_version_1 = DagVersion.get_latest_version(dr.dag_id, session=session)

        # Now let's say the DAG got updated (new task got added)
        BashOperator(task_id="bash_task_1", dag=dag_maker.dag, bash_command="echo hi")
        sync_dag_to_db(dag_maker.dag, bundle_name="dag_maker", session=session)
        session.commit()
        dag_version_2 = DagVersion.get_latest_version(dr.dag_id, session=session)
        assert dag_version_2 != dag_version_1

        # Verify that DagRun.verify_integrity is not called
        with mock.patch("airflow.jobs.scheduler_job_runner.DagRun.verify_integrity") as mock_verify_integrity:
            self.job_runner._schedule_dag_run(dr, session)
            mock_verify_integrity.assert_not_called()

    @pytest.mark.need_serialized_dag
    def test_retry_still_in_executor(self, dag_maker, session):
        """
        Checks if the scheduler does not put a task in limbo, when a task is retried
        but is still present in the executor.
        """
        executor = MockExecutor(do_update=False)

        with dag_maker(
            dag_id="test_retry_still_in_executor",
            schedule="@once",
            session=session,
        ) as dag:
            dag_task1 = BashOperator(
                task_id="test_retry_handling_op",
                bash_command="exit 1",
                retries=1,
            )
        dag_maker.dag_model.calculate_dagrun_date_fields(dag, None)

        @provide_session
        def do_schedule(session):
            # Use a empty file since the above mock will return the
            # expected DAGs. Also specify only a single file so that it doesn't
            # try to schedule the above DAG repeatedly.
            scheduler_job = Job(executor=executor)
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)

            scheduler_job.heartrate = 0
            # Since the DAG is not in the directory watched by scheduler job,
            # it would've been marked as deleted and not being scheduled.
            with mock.patch.object(DagModel, "deactivate_deleted_dags"):
                with mock.patch(
                    "airflow.executors.executor_loader.ExecutorLoader.load_executor"
                ) as loader_mock:
                    loader_mock.side_effect = executor.get_mock_loader_side_effect()
                    run_job(scheduler_job, execute_callable=self.job_runner._execute)

        do_schedule()
        with create_session() as session:
            ti = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == "test_retry_still_in_executor",
                    TaskInstance.task_id == "test_retry_handling_op",
                )
                .first()
            )
        assert ti is not None, "Task not created by scheduler"
        ti.task = dag_task1

        def run_with_error(ti, ignore_ti_state=False):
            with contextlib.suppress(AirflowException):
                ti.run(ignore_ti_state=ignore_ti_state)

        assert ti.try_number == 1
        # At this point, scheduler has tried to schedule the task once and
        # heartbeated the executor once, which moved the state of the task from
        # SCHEDULED to QUEUED and then to SCHEDULED, to fail the task execution
        # we need to ignore the TaskInstance state as SCHEDULED is not a valid state to start
        # executing task.
        run_with_error(ti, ignore_ti_state=True)
        assert ti.state == State.UP_FOR_RETRY
        assert ti.try_number == 1

        ti.refresh_from_db(lock_for_update=True, session=session)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        # To verify that task does get re-queued.
        executor.do_update = True
        do_schedule()
        ti.refresh_from_db()
        assert ti.try_number == 1
        assert ti.state == State.SUCCESS

    def test_adopt_or_reset_orphaned_tasks_nothing(self):
        """Try with nothing."""
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()
        assert self.job_runner.adopt_or_reset_orphaned_tasks(session=session) == 0

    @pytest.mark.parametrize(
        "adoptable_state",
        list(sorted(State.adoptable_states)),
    )
    def test_adopt_or_reset_resettable_tasks(self, dag_maker, adoptable_state, session):
        dag_id = "test_adopt_or_reset_adoptable_tasks_" + adoptable_state.name
        with dag_maker(dag_id=dag_id, schedule="@daily"):
            task_id = dag_id + "_task"
            EmptyOperator(task_id=task_id)
        old_job = Job()
        session.add(old_job)
        session.commit()
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.MANUAL)
        ti = dr1.get_task_instances(session=session)[0]
        ti.state = adoptable_state
        ti.queued_by_job_id = old_job.id
        session.merge(ti)
        session.merge(dr1)
        session.commit()

        num_reset_tis = self.job_runner.adopt_or_reset_orphaned_tasks(session=session)
        assert num_reset_tis == 1

    def test_adopt_or_reset_orphaned_tasks_external_triggered_dag(self, dag_maker, session):
        dag_id = "test_reset_orphaned_tasks_external_triggered_dag"
        with dag_maker(dag_id=dag_id, schedule="@daily"):
            task_id = dag_id + "_task"
            EmptyOperator(task_id=task_id)

        old_job = Job()
        session.add(old_job)
        session.flush()
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.MANUAL)
        ti = dr1.get_task_instances(session=session)[0]
        ti.state = State.QUEUED
        ti.queued_by_job_id = old_job.id
        session.merge(ti)
        session.merge(dr1)
        session.commit()

        num_reset_tis = self.job_runner.adopt_or_reset_orphaned_tasks(session=session)
        assert num_reset_tis == 1

    def test_adopt_or_reset_orphaned_tasks_backfill_dag(self, dag_maker):
        dag_id = "test_adopt_or_reset_orphaned_tasks_backfill_dag"
        with dag_maker(dag_id=dag_id, schedule="@daily"):
            task_id = dag_id + "_task"
            EmptyOperator(task_id=task_id)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()
        session.add(scheduler_job)
        session.flush()

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.BACKFILL_JOB)

        ti = dr1.get_task_instances(session=session)[0]
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.merge(dr1)
        session.flush()

        assert dr1.run_type == DagRunType.BACKFILL_JOB
        assert self.job_runner.adopt_or_reset_orphaned_tasks(session=session) == 0
        session.rollback()

    def test_reset_orphaned_tasks_no_orphans(self, dag_maker):
        dag_id = "test_reset_orphaned_tasks_no_orphans"
        with dag_maker(dag_id=dag_id, schedule="@daily"):
            task_id = dag_id + "_task"
            EmptyOperator(task_id=task_id)

        scheduler_job = Job()
        scheduler_job.state = "running"
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()
        session.add(scheduler_job)
        session.flush()

        dr1 = dag_maker.create_dagrun()
        tis = dr1.get_task_instances(session=session)
        tis[0].state = State.RUNNING
        tis[0].queued_by_job_id = scheduler_job.id
        session.merge(dr1)
        session.merge(tis[0])
        session.flush()

        assert self.job_runner.adopt_or_reset_orphaned_tasks(session=session) == 0
        tis[0].refresh_from_db()
        assert tis[0].state == State.RUNNING

    def test_reset_orphaned_tasks_non_running_dagruns(self, dag_maker):
        """Ensure orphaned tasks with non-running dagruns are not reset."""
        dag_id = "test_reset_orphaned_tasks_non_running_dagruns"
        with dag_maker(dag_id=dag_id, schedule="@daily"):
            task_id = dag_id + "_task"
            EmptyOperator(task_id=task_id)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()
        session.add(scheduler_job)
        session.flush()

        dr1 = dag_maker.create_dagrun()
        dr1.state = State.QUEUED
        tis = dr1.get_task_instances(session=session)
        assert len(tis) == 1
        tis[0].state = State.SCHEDULED
        session.merge(dr1)
        session.merge(tis[0])
        session.flush()

        assert self.job_runner.adopt_or_reset_orphaned_tasks(session=session) == 0
        session.rollback()

    def test_adopt_or_reset_orphaned_tasks_stale_scheduler_jobs(self, dag_maker):
        dag_id = "test_adopt_or_reset_orphaned_tasks_stale_scheduler_jobs"
        with dag_maker(dag_id=dag_id, schedule="@daily"):
            EmptyOperator(task_id="task1")
            EmptyOperator(task_id="task2")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()
        scheduler_job.state = State.RUNNING
        scheduler_job.latest_heartbeat = timezone.utcnow()
        session.add(scheduler_job)

        old_job = Job()
        old_job_runner = SchedulerJobRunner(job=old_job)
        old_job_runner.job.state = State.RUNNING
        old_job_runner.job.latest_heartbeat = timezone.utcnow() - timedelta(minutes=15)
        session.add(old_job)
        session.flush()

        dr1 = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
            start_date=timezone.utcnow(),
        )

        ti1, ti2 = dr1.get_task_instances(session=session)
        dr1.state = State.RUNNING
        ti1.state = State.QUEUED
        ti1.queued_by_job_id = old_job.id
        session.merge(dr1)
        session.merge(ti1)

        ti2.state = State.QUEUED
        ti2.queued_by_job_id = scheduler_job.id
        session.merge(ti2)
        session.flush()

        num_reset_tis = self.job_runner.adopt_or_reset_orphaned_tasks(session=session)

        assert num_reset_tis == 1

        session.refresh(ti1)
        assert ti1.state is None
        session.refresh(ti2)
        assert ti2.state == State.QUEUED
        session.rollback()

    def test_adopt_or_reset_orphaned_tasks_only_fails_scheduler_jobs(self, caplog):
        """Make sure we only set SchedulerJobs to failed, not all jobs"""
        session = settings.Session()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        scheduler_job.state = State.RUNNING
        scheduler_job.latest_heartbeat = timezone.utcnow()
        session.add(scheduler_job)
        session.flush()

        old_job = Job()
        self.job_runner = SchedulerJobRunner(job=old_job)
        old_job.state = State.RUNNING
        old_job.latest_heartbeat = timezone.utcnow() - timedelta(minutes=15)
        session.add(old_job)
        session.flush()

        old_task_job = Job(state=State.RUNNING)
        old_task_job.latest_heartbeat = timezone.utcnow() - timedelta(minutes=15)
        session.add(old_task_job)
        session.flush()

        with caplog.at_level("INFO", logger="airflow.jobs.scheduler_job_runner"):
            self.job_runner.adopt_or_reset_orphaned_tasks(session=session)
        session.expire_all()

        assert old_job.state == State.FAILED
        assert old_task_job.state == State.RUNNING
        assert "Marked 1 SchedulerJob instances as failed" in caplog.messages

    @pytest.mark.parametrize(
        "kwargs",
        [
            param(
                dict(
                    schedule=None,
                    backfill_runs=0,
                    other_runs=2,
                    max_active_runs=2,
                    should_update=False,
                ),
                id="no_dag_schedule",
            ),
            param(
                dict(
                    schedule="0 0 * * *",
                    backfill_runs=0,
                    other_runs=2,
                    max_active_runs=2,
                    should_update=False,
                ),
                id="dag_schedule_at_capacity",
            ),
            param(
                dict(
                    schedule="0 0 * * *",
                    backfill_runs=0,
                    other_runs=1,
                    max_active_runs=2,
                    should_update=True,
                ),
                id="dag_schedule_under_capacity",
            ),
            param(
                dict(
                    schedule="0 0 * * *",
                    backfill_runs=0,
                    other_runs=5,
                    max_active_runs=2,
                    should_update=False,
                ),
                id="dag_schedule_over_capacity",
            ),
            param(
                dict(
                    schedule="0 0 * * *",
                    number_running=None,
                    backfill_runs=5,
                    other_runs=1,
                    max_active_runs=2,
                    should_update=True,
                ),
                id="dag_schedule_under_capacity_many_backfill",
            ),
        ],
    )
    @pytest.mark.parametrize("provide_run_count", [True, False])
    def test_should_update_dag_next_dagruns(self, provide_run_count: bool, kwargs: dict, session, dag_maker):
        """Test if really required to update next dagrun or possible to save run time"""
        schedule: str | None = kwargs["schedule"]
        backfill_runs: int = kwargs["backfill_runs"]
        other_runs: int = kwargs["other_runs"]
        max_active_runs: int = kwargs["max_active_runs"]
        should_update: bool = kwargs["should_update"]

        with dag_maker(schedule=schedule, max_active_runs=max_active_runs) as dag:
            EmptyOperator(task_id="dummy")

        index = 0
        for index in range(other_runs):
            dag_maker.create_dagrun(
                run_id=f"run_{index}",
                logical_date=(DEFAULT_DATE + timedelta(days=index)),
                start_date=timezone.utcnow(),
                state=State.RUNNING,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
        for index in range(index + 1, index + 1 + backfill_runs):
            dag_maker.create_dagrun(
                run_id=f"run_{index}",
                logical_date=(DEFAULT_DATE + timedelta(days=index)),
                start_date=timezone.utcnow(),
                state=State.RUNNING,
                run_type=DagRunType.BACKFILL_JOB,
                session=session,
            )
        assert index == other_runs + backfill_runs - 1  # sanity check
        session.commit()
        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        actual = self.job_runner._should_update_dag_next_dagruns(
            dag=dag,
            dag_model=dag_maker.dag_model,
            active_non_backfill_runs=other_runs if provide_run_count else None,  # exclude backfill here
            session=session,
        )
        assert actual == should_update

    @pytest.mark.parametrize(
        ("run_type", "expected"),
        [
            (DagRunType.MANUAL, False),
            (DagRunType.SCHEDULED, True),
            (DagRunType.BACKFILL_JOB, False),
            (DagRunType.ASSET_TRIGGERED, False),
        ],
        ids=[
            DagRunType.MANUAL.name,
            DagRunType.SCHEDULED.name,
            DagRunType.BACKFILL_JOB.name,
            DagRunType.ASSET_TRIGGERED.name,
        ],
    )
    def test_should_update_dag_next_dagruns_after_run_type(self, run_type, expected, session, dag_maker):
        """Test that whether next dag run is updated depends on run type"""
        with dag_maker(
            schedule="*/1 * * * *",
            max_active_runs=3,
        ) as dag:
            EmptyOperator(task_id="dummy")

        dag_model = dag_maker.dag_model

        run = dag_maker.create_dagrun(
            run_id="run",
            run_type=run_type,
            logical_date=DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.SUCCESS,
            session=session,
        )

        session.flush()
        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        actual = self.job_runner._should_update_dag_next_dagruns(
            dag=dag,
            dag_model=dag_model,
            last_dag_run=run,
            session=session,
        )
        assert actual == expected

    def test_create_dag_runs(self, dag_maker):
        """
        Test various invariants of _create_dag_runs.

        - That the run created has the creating_job_id set
        - That the run created is on QUEUED State
        - That dag_model has next_dagrun
        """
        with dag_maker(dag_id="test_create_dag_runs") as dag:
            EmptyOperator(task_id="dummy")

        dag_model = dag_maker.dag_model

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with create_session() as session:
            self.job_runner._create_dag_runs([dag_model], session)

        dr = session.query(DagRun).filter(DagRun.dag_id == dag.dag_id).one()
        assert dr.state == State.QUEUED
        assert dr.start_date is None
        assert dr.creating_job_id == scheduler_job.id

    @pytest.mark.need_serialized_dag
    def test_create_dag_runs_assets(self, session, dag_maker):
        """
        Test various invariants of _create_dag_runs.

        - That the run created has the creating_job_id set
        - That the run created is on QUEUED State
        - That dag_model has next_dagrun
        """
        asset1 = Asset(uri="test://asset1", name="test_asset", group="test_group")
        asset2 = Asset(uri="test://asset2", name="test_asset_2", group="test_group")

        with dag_maker(dag_id="assets-1", start_date=timezone.utcnow(), session=session):
            BashOperator(task_id="task", bash_command="echo 1", outlets=[asset1])
        dr = dag_maker.create_dagrun(
            run_id="run1",
            logical_date=(DEFAULT_DATE + timedelta(days=100)),
            data_interval=(DEFAULT_DATE + timedelta(days=10), DEFAULT_DATE + timedelta(days=11)),
        )

        asset1_id = session.query(AssetModel.id).filter_by(uri=asset1.uri).scalar()

        event1 = AssetEvent(
            asset_id=asset1_id,
            source_task_id="task",
            source_dag_id=dr.dag_id,
            source_run_id=dr.run_id,
            source_map_index=-1,
        )
        session.add(event1)

        # Create a second event, creation time is more recent, but data interval is older
        dr = dag_maker.create_dagrun(
            run_id="run2",
            logical_date=(DEFAULT_DATE + timedelta(days=101)),
            data_interval=(DEFAULT_DATE + timedelta(days=5), DEFAULT_DATE + timedelta(days=6)),
        )

        event2 = AssetEvent(
            asset_id=asset1_id,
            source_task_id="task",
            source_dag_id=dr.dag_id,
            source_run_id=dr.run_id,
            source_map_index=-1,
        )
        session.add(event2)

        with dag_maker(dag_id="assets-consumer-multiple", schedule=[asset1, asset2]):
            pass
        dag2 = dag_maker.dag
        with dag_maker(dag_id="assets-consumer-single", schedule=[asset1]):
            pass
        dag3 = dag_maker.dag

        session = dag_maker.session
        session.add_all(
            [
                AssetDagRunQueue(asset_id=asset1_id, target_dag_id=dag2.dag_id),
                AssetDagRunQueue(asset_id=asset1_id, target_dag_id=dag3.dag_id),
            ]
        )
        session.flush()

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with create_session() as session:
            self.job_runner._create_dagruns_for_dags(session, session)

        def dict_from_obj(obj):
            """Get dict of column attrs from SqlAlchemy object."""
            return {k.key: obj.__dict__.get(k) for k in obj.__mapper__.column_attrs}

        # dag3 should be triggered since it only depends on asset1, and it's been queued
        created_run = session.query(DagRun).filter(DagRun.dag_id == dag3.dag_id).one()
        assert created_run.state == State.QUEUED
        assert created_run.start_date is None

        # we don't have __eq__ defined on AssetEvent because... given the fact that in the future
        # we may register events from other systems, asset_id + timestamp might not be enough PK
        assert list(map(dict_from_obj, created_run.consumed_asset_events)) == list(
            map(dict_from_obj, [event1, event2])
        )
        assert created_run.data_interval_start is None
        assert created_run.data_interval_end is None
        # dag2 ADRQ record should still be there since the dag run was *not* triggered
        assert session.query(AssetDagRunQueue).filter_by(target_dag_id=dag2.dag_id).one() is not None
        # dag2 should not be triggered since it depends on both asset 1  and 2
        assert session.query(DagRun).filter(DagRun.dag_id == dag2.dag_id).one_or_none() is None
        # dag3 ADRQ record should be deleted since the dag run was triggered
        assert session.query(AssetDagRunQueue).filter_by(target_dag_id=dag3.dag_id).one_or_none() is None

        assert created_run.creating_job_id == scheduler_job.id

    @pytest.mark.need_serialized_dag
    def test_create_dag_runs_asset_alias_with_asset_event_attached(self, session, dag_maker):
        """
        Test Dag Run trigger on AssetAlias includes the corresponding AssetEvent in `consumed_asset_events`.
        """

        # Simulate an Asset created at runtime, and it is not an active asset
        asset1 = Asset(uri="test://asset1", name="test_asset", group="test_group")
        # Create an AssetAlias, and the Asset will be attached to this AssetAlias
        asset_alias = AssetAlias(name="test_asset_alias_with_asset_event", group="test_group")

        # Add it to the DB so the event can be created from this Asset
        asm = AssetModel(name=asset1.name, uri=asset1.uri, group=asset1.group)
        session.add(asm)

        asam = AssetAliasModel(name=asset_alias.name, group=asset_alias.group)

        # Simulate a Producer dag attach an asset event at runtime to an AssetAlias
        # Don't use outlets here because the needs to associate an asset alias with an asset event in the association table
        with dag_maker(dag_id="asset-alias-producer", start_date=timezone.utcnow(), session=session):
            BashOperator(task_id="simulate-asset-alias-outlet", bash_command="echo 1")
        dr = dag_maker.create_dagrun(run_id="asset-alias-producer-run")

        asset1_id = session.query(AssetModel.id).filter_by(uri=asset1.uri).scalar()

        # Create an AssetEvent, which is associated with the Asset, and it is attached to the AssetAlias
        event = AssetEvent(
            asset_id=asset1_id,
            source_task_id="simulate-asset-alias-outlet",
            source_dag_id=dr.dag_id,
            source_run_id=dr.run_id,
            source_map_index=-1,
        )
        # Attach the Asset and the AssetEvent to the Asset Alias
        asam.assets.append(asm)
        asam.asset_events.append(event)

        session.add_all([asam, event])
        session.flush()

        # Create the Consumer DAG and Trigger it with scheduler
        with dag_maker(dag_id="asset-alias-consumer", schedule=[asset_alias]):
            pass
        consumer_dag = dag_maker.dag

        session = dag_maker.session
        session.add_all(
            [
                AssetDagRunQueue(asset_id=asset1_id, target_dag_id=consumer_dag.dag_id),
            ]
        )
        session.flush()

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with create_session() as session:
            self.job_runner._create_dagruns_for_dags(session, session)

        def dict_from_obj(obj):
            """Get dict of column attrs from SqlAlchemy object."""
            return {k.key: obj.__dict__.get(k) for k in obj.__mapper__.column_attrs}

        created_run = session.query(DagRun).filter(DagRun.dag_id == consumer_dag.dag_id).one()
        assert created_run.state == State.QUEUED
        assert created_run.start_date is None

        # The AssetEvent should be included in the consumed_asset_events when the consumer DAG is
        # triggered on AssetAlias
        assert list(map(dict_from_obj, created_run.consumed_asset_events)) == list(
            map(dict_from_obj, [event])
        )
        assert created_run.data_interval_start is None
        assert created_run.data_interval_end is None
        assert created_run.creating_job_id == scheduler_job.id

    @pytest.mark.need_serialized_dag
    @pytest.mark.parametrize(
        ("disable", "enable"),
        [
            pytest.param({"is_stale": True}, {"is_stale": False}, id="active"),
            pytest.param({"is_paused": True}, {"is_paused": False}, id="paused"),
        ],
    )
    def test_no_create_dag_runs_when_dag_disabled(self, session, dag_maker, disable, enable):
        asset = Asset(uri="test://asset_1", name="test_asset_1", group="test_group")
        with dag_maker(dag_id="consumer", schedule=[asset], session=session):
            pass
        with dag_maker(dag_id="producer", schedule="@daily", session=session):
            BashOperator(task_id="task", bash_command="echo 1", outlets=asset)
        asset_manger = AssetManager()

        asset_id = session.scalars(select(AssetModel.id).filter_by(uri=asset.uri, name=asset.name)).one()
        ase_q = select(AssetEvent).where(AssetEvent.asset_id == asset_id).order_by(AssetEvent.timestamp)
        adrq_q = select(AssetDagRunQueue).where(
            AssetDagRunQueue.asset_id == asset_id, AssetDagRunQueue.target_dag_id == "consumer"
        )

        # Simulate the consumer DAG being disabled.
        session.execute(update(DagModel).where(DagModel.dag_id == "consumer").values(**disable))

        # An ADRQ is not scheduled although an event is emitted.
        dr1: DagRun = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        asset_manger.register_asset_change(
            task_instance=dr1.get_task_instance("task", session=session),
            asset=asset,
            session=session,
        )
        session.flush()
        assert session.scalars(ase_q).one().source_run_id == dr1.run_id
        assert session.scalars(adrq_q).one_or_none() is None

        # Simulate the consumer DAG being enabled.
        session.execute(update(DagModel).where(DagModel.dag_id == "consumer").values(**enable))

        # An ADRQ should be scheduled for the new event, but not the previous one.
        dr2: DagRun = dag_maker.create_dagrun_after(dr1, run_type=DagRunType.SCHEDULED)
        asset_manger.register_asset_change(
            task_instance=dr2.get_task_instance("task", session=session),
            asset=asset,
            session=session,
        )
        session.flush()
        assert [e.source_run_id for e in session.scalars(ase_q)] == [dr1.run_id, dr2.run_id]
        assert session.scalars(adrq_q).one().target_dag_id == "consumer"

    @time_machine.travel(DEFAULT_DATE + datetime.timedelta(days=1, seconds=9), tick=False)
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.timing")
    def test_start_dagruns(self, stats_timing, dag_maker, session):
        """
        Test that _start_dagrun:

        - moves runs to RUNNING State
        - emit the right DagRun metrics
        """
        from airflow.models.dag import get_last_dagrun

        with dag_maker(dag_id="test_start_dag_runs") as dag:
            EmptyOperator(task_id="dummy")

        dag_model = dag_maker.dag_model

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._create_dag_runs([dag_model], session)
        self.job_runner._start_queued_dagruns(session)

        dr = session.query(DagRun).filter(DagRun.dag_id == dag.dag_id).first()
        # Assert dr state is running
        assert dr.state == State.RUNNING

        stats_timing.assert_has_calls(
            [
                mock.call(
                    "dagrun.schedule_delay.test_start_dag_runs",
                    datetime.timedelta(seconds=9),
                ),
                mock.call(
                    "dagrun.schedule_delay",
                    datetime.timedelta(seconds=9),
                    tags={"dag_id": "test_start_dag_runs"},
                ),
            ]
        )

        assert get_last_dagrun(dag.dag_id, session).creating_job_id == scheduler_job.id

    def test_extra_operator_links_not_loaded_in_scheduler_loop(self, dag_maker):
        """
        Test that Operator links are not loaded inside the Scheduling Loop (that does not include
        DagFileProcessorProcess) especially the critical loop of the Scheduler.

        This is to avoid running User code in the Scheduler and prevent any deadlocks
        """
        with dag_maker(dag_id="test_extra_operator_links_not_loaded_in_scheduler") as dag:
            # This CustomOperator has Extra Operator Links registered via plugins
            _ = CustomOperator(task_id="custom_task")

        custom_task = dag.task_dict["custom_task"]
        # Test that custom_task has >= 1 Operator Links (after de-serialization)
        assert custom_task.operator_extra_links

        session = settings.Session()
        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        self.job_runner._do_scheduling(session=session)
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        # assert len(self.job_runner.scheduler_dag_bag._dags) == 1  # sanity check
        # Get serialized dag
        dr = DagRun.find(dag_id=dag.dag_id)[0]
        s_dag_2 = self.job_runner.scheduler_dag_bag.get_dag_for_run(dr, session=session)
        custom_task = s_dag_2.task_dict["custom_task"]
        # Test that custom_task has no Operator Links (after de-serialization) in the Scheduling Loop
        assert not custom_task.operator_extra_links

    def test_scheduler_create_dag_runs_does_not_raise_error_when_no_serdag(self, caplog, dag_maker):
        """
        Test that scheduler._create_dag_runs does not raise an error when the DAG does not exist
        in serialized_dag table
        """
        with dag_maker(dag_id="test_scheduler_create_dag_runs_does_not_raise_error", serialized=False):
            EmptyOperator(
                task_id="dummy",
            )

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        caplog.set_level("FATAL")
        caplog.clear()
        with (
            create_session() as session,
            caplog.at_level(
                "ERROR",
                logger="airflow.jobs.scheduler_job_runner",
            ),
        ):
            self._clear_serdags(dag_id=dag_maker.dag.dag_id, session=session)
            self.job_runner._create_dag_runs([dag_maker.dag_model], session)
            assert caplog.messages == [
                "DAG 'test_scheduler_create_dag_runs_does_not_raise_error' not found in serialized_dag table",
            ]

    def _clear_serdags(self, dag_id, session):
        SDM = SerializedDagModel
        sdms = session.scalars(select(SDM).where(SDM.dag_id == dag_id))
        for sdm in sdms:
            session.delete(sdm)
        session.commit()

    def test_bulk_write_to_db_external_trigger_dont_skip_scheduled_run(self, dag_maker, testing_dag_bundle):
        """
        Test that externally triggered Dag Runs should not affect (by skipping) next
        scheduled DAG runs
        """
        with dag_maker(
            dag_id="test_bulk_write_to_db_external_trigger_dont_skip_scheduled_run",
            schedule="*/1 * * * *",
            max_active_runs=5,
            catchup=True,
        ) as dag:
            EmptyOperator(task_id="dummy")

        session = settings.Session()

        # Verify that dag_model.next_dagrun is equal to next logical_date
        dag_model = dag_maker.dag_model
        assert dag_model.next_dagrun == DEFAULT_DATE
        assert dag_model.next_dagrun_data_interval_start == DEFAULT_DATE
        assert dag_model.next_dagrun_data_interval_end == DEFAULT_DATE + timedelta(minutes=1)

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Verify a DagRun is created with the correct dates
        # when Scheduler._do_scheduling is run in the Scheduler Loop
        self.job_runner._do_scheduling(session)
        dr1 = session.scalar(
            select(DagRun).where(DagRun.dag_id == dag_model.dag_id).order_by(DagRun.id.asc()).limit(1)
        )
        assert dr1 is not None
        assert dr1.state == DagRunState.RUNNING
        assert dr1.logical_date == DEFAULT_DATE
        assert dr1.data_interval_start == DEFAULT_DATE
        assert dr1.data_interval_end == DEFAULT_DATE + timedelta(minutes=1)

        # Verify that dag_model.next_dagrun is set to next interval
        dag_model = session.get(DagModel, dag.dag_id)
        assert dag_model.next_dagrun == DEFAULT_DATE + timedelta(minutes=1)
        assert dag_model.next_dagrun_data_interval_start == DEFAULT_DATE + timedelta(minutes=1)
        assert dag_model.next_dagrun_data_interval_end == DEFAULT_DATE + timedelta(minutes=2)

        # Trigger the Dag externally
        data_interval = infer_automated_data_interval(dag.timetable, DEFAULT_LOGICAL_DATE)
        dr = dag.create_dagrun(
            run_id="test",
            state=DagRunState.RUNNING,
            logical_date=timezone.utcnow(),
            run_type=DagRunType.MANUAL,
            session=session,
            data_interval=data_interval,
            run_after=DEFAULT_LOGICAL_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )
        assert dr is not None

        # Test that 'dag_model.next_dagrun' has not been changed because of newly created external
        # triggered DagRun.
        dag_model = session.get(DagModel, dag.dag_id)
        assert dag_model.next_dagrun == DEFAULT_DATE + timedelta(minutes=1)
        assert dag_model.next_dagrun_data_interval_start == DEFAULT_DATE + timedelta(minutes=1)
        assert dag_model.next_dagrun_data_interval_end == DEFAULT_DATE + timedelta(minutes=2)

    def test_scheduler_create_dag_runs_check_existing_run(self, dag_maker, session):
        """
        Test that if a dag run exists, scheduler._create_dag_runs does not raise an error.
        And if a Dag Run does not exist it creates next Dag Run. In both cases the Scheduler
        sets next logical date as DagModel.next_dagrun
        """
        # By setting catchup=True explicitly, we ensure the test behaves as originally intended
        # using the historical date as the next_dagrun date.
        with dag_maker(
            dag_id="test_scheduler_create_dag_runs_check_existing_run",
            schedule=timedelta(days=1),
            catchup=True,
        ) as dag:
            EmptyOperator(task_id="dummy")

        assert get_last_dagrun(dag.dag_id, session) is None

        dag_model = dag_maker.dag_model

        # Assert dag_model.next_dagrun is set correctly
        assert dag_model.next_dagrun == DEFAULT_DATE

        dagrun = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            logical_date=dag_model.next_dagrun,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            session=session,
            creating_job_id=2,
        )
        session.flush()

        assert get_last_dagrun(dag.dag_id, session) == dagrun

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Test that this does not raise any error
        self.job_runner._create_dag_runs([dag_model], session)

        # Assert the next dagrun fields are set correctly to next logical date
        assert dag_model.next_dagrun_data_interval_start == DEFAULT_DATE + timedelta(days=1)
        assert dag_model.next_dagrun_data_interval_end == DEFAULT_DATE + timedelta(days=2)
        assert dag_model.next_dagrun == DEFAULT_DATE + timedelta(days=1)
        session.rollback()

    @conf_vars({("scheduler", "use_job_schedule"): "false"})
    def test_do_schedule_max_active_runs_dag_timed_out(self, dag_maker, session):
        """Test that tasks are set to a finished state when their DAG times out"""
        with dag_maker(
            dag_id="test_max_active_run_with_dag_timed_out",
            schedule="@once",
            max_active_runs=1,
            catchup=True,
            dagrun_timeout=datetime.timedelta(seconds=1),
        ) as dag:
            task1 = BashOperator(
                task_id="task1",
                bash_command=' for((i=1;i<=600;i+=1)); do sleep "$i";  done',
            )

        data_interval = infer_automated_data_interval(dag.timetable, DEFAULT_LOGICAL_DATE)
        run1 = dag.create_dagrun(
            run_id="test1",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
            state=State.RUNNING,
            start_date=timezone.utcnow() - timedelta(seconds=2),
            session=session,
            data_interval=data_interval,
            run_after=DEFAULT_LOGICAL_DATE,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        run1_ti = run1.get_task_instance(task1.task_id, session)
        run1_ti.state = State.RUNNING

        logical_date_2 = DEFAULT_DATE + timedelta(seconds=10)
        run2 = dag.create_dagrun(
            run_id="test2",
            run_type=DagRunType.SCHEDULED,
            logical_date=logical_date_2,
            state=State.QUEUED,
            session=session,
            data_interval=data_interval,
            run_after=logical_date_2,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        my_dag = session.get(DagModel, dag.dag_id)
        self.job_runner._create_dag_runs([my_dag], session)
        # Run relevant part of scheduling again to assert run2 has been scheduled
        self.job_runner._schedule_dag_run(run1, session)
        run1 = session.merge(run1)
        session.refresh(run1)
        assert run1.state == State.FAILED
        assert run1_ti.state == State.SKIPPED
        session.flush()
        # Run relevant part of scheduling again to assert run2 has been scheduled
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        run2 = session.merge(run2)
        session.refresh(run2)
        assert run2.state == State.RUNNING
        self.job_runner._schedule_dag_run(run2, session)
        session.expunge_all()
        run2_ti = run2.get_task_instance(task1.task_id, session)
        assert run2_ti.state == State.SCHEDULED

    def test_do_schedule_max_active_runs_task_removed(self, session, dag_maker):
        """Test that tasks in removed state don't count as actively running."""
        with dag_maker(
            dag_id="test_do_schedule_max_active_runs_task_removed",
            start_date=DEFAULT_DATE,
            schedule="@once",
            max_active_runs=1,
            session=session,
        ):
            # Can't use EmptyOperator as that goes straight to success
            BashOperator(task_id="dummy1", bash_command="true")

        run1 = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE + timedelta(hours=1),
            state=State.RUNNING,
        )

        executor = MockExecutor(do_update=False)
        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with mock.patch("airflow.executors.executor_loader.ExecutorLoader.load_executor") as loader_mock:
            # The executor is mocked, so cannot be loaded/imported. Mock load_executor and return the
            # correct object for the given input executor name.
            loader_mock.side_effect = executor.get_mock_loader_side_effect()

            num_queued = self.job_runner._do_scheduling(session)
        assert num_queued == 1

        session.flush()
        ti = run1.task_instances[0]
        ti.refresh_from_db(session=session)
        assert ti.state == State.QUEUED

    def test_more_runs_are_not_created_when_max_active_runs_is_reached(self, dag_maker, caplog):
        """
        This tests that when max_active_runs is reached, _create_dag_runs doesn't create
        more dagruns
        """
        # Explicitly set catchup=True as test specifically expects historical dates to be respected
        with dag_maker(max_active_runs=1, catchup=True):
            EmptyOperator(task_id="task")
        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        session = settings.Session()
        assert session.query(DagRun).count() == 0
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        self.job_runner._create_dag_runs(dag_models, session)
        dr = session.query(DagRun).one()
        dr.state == DagRunState.QUEUED
        assert session.query(DagRun).count() == 1
        assert dag_maker.dag_model.next_dagrun_create_after is None
        session.flush()
        # dags_needing_dagruns query should not return any value
        query, _ = DagModel.dags_needing_dagruns(session)
        assert len(query.all()) == 0
        self.job_runner._create_dag_runs(dag_models, session)
        assert session.query(DagRun).count() == 1
        assert dag_maker.dag_model.next_dagrun_create_after is None
        assert dag_maker.dag_model.next_dagrun == DEFAULT_DATE
        # set dagrun to success
        dr = session.query(DagRun).one()
        dr.state = DagRunState.SUCCESS
        ti = dr.get_task_instance("task", session)
        ti.state = TaskInstanceState.SUCCESS
        session.merge(ti)
        session.merge(dr)
        session.flush()
        # check that next_dagrun is set properly by Schedulerjob._update_dag_next_dagruns
        self.job_runner._schedule_dag_run(dr, session)
        session.flush()
        query, _ = DagModel.dags_needing_dagruns(session)
        assert len(query.all()) == 1
        # assert next_dagrun has been updated correctly
        assert dag_maker.dag_model.next_dagrun == DEFAULT_DATE + timedelta(days=1)
        # assert no dagruns is created yet
        assert (
            session.query(DagRun).filter(DagRun.state.in_([DagRunState.RUNNING, DagRunState.QUEUED])).count()
            == 0
        )

    def test_max_active_runs_creation_phasing(self, dag_maker, session):
        """
        Test that when creating runs once max_active_runs is reached that the runs come in the right order
        without gaps
        """

        def complete_one_dagrun():
            ti = (
                session.query(TaskInstance)
                .join(TaskInstance.dag_run)
                .filter(TaskInstance.state != State.SUCCESS)
                .order_by(DagRun.logical_date)
                .first()
            )
            if ti:
                ti.state = State.SUCCESS
                session.flush()

        self.clean_db()

        # Explicitly set catchup=True as test specifically expects runs to be created in date order
        with dag_maker(max_active_runs=3, session=session, catchup=True) as dag:
            # Need to use something that doesn't immediately get marked as success by the scheduler
            BashOperator(task_id="task", bash_command="true")

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        query, _ = DagModel.dags_needing_dagruns(session)
        query.all()
        for _ in range(3):
            self.job_runner._do_scheduling(session)

        model: DagModel = session.get(DagModel, dag.dag_id)

        # Pre-condition
        assert DagRun.active_runs_of_dags(dag_ids=["test_dag"], exclude_backfill=True, session=session) == {
            "test_dag": 3
        }

        assert model.next_dagrun == timezone.DateTime(2016, 1, 3, tzinfo=timezone.utc)
        assert model.next_dagrun_create_after is None

        complete_one_dagrun()

        assert DagRun.active_runs_of_dags(dag_ids=["test_dag"], exclude_backfill=True, session=session) == {
            "test_dag": 3
        }

        for _ in range(5):
            self.job_runner._do_scheduling(session)
            complete_one_dagrun()

        expected_logical_dates = [datetime.datetime(2016, 1, d, tzinfo=timezone.utc) for d in range(1, 6)]
        dagrun_logical_dates = [
            dr.logical_date for dr in session.query(DagRun).order_by(DagRun.logical_date).all()
        ]
        assert dagrun_logical_dates == expected_logical_dates

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_do_schedule_max_active_runs_and_manual_trigger(self, dag_maker, mock_executors):
        """
        Make sure that when a DAG is already at max_active_runs, that manually triggered
        dagruns don't start running.
        """
        # Explicitly set catchup=True as test specifically expects runs to be created in date order
        with dag_maker(
            dag_id="test_max_active_run_plus_manual_trigger",
            schedule="@once",
            max_active_runs=1,
            catchup=True,
        ) as dag:
            # Can't use EmptyOperator as that goes straight to success
            task1 = BashOperator(task_id="dummy1", bash_command="true")
            task2 = BashOperator(task_id="dummy2", bash_command="true")

            task1 >> task2

            BashOperator(task_id="dummy3", bash_command="true")

        session = settings.Session()
        dag_run = dag_maker.create_dagrun(state=State.QUEUED, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        num_queued = self.job_runner._do_scheduling(session)
        # Add it back in to the session so we can refresh it. (_do_scheduling does an expunge_all to reduce
        # memory)
        dag_run = session.merge(dag_run)
        session.refresh(dag_run)

        assert num_queued == 2
        assert dag_run.state == State.RUNNING

        # Now that this one is running, manually trigger a dag.

        dag_maker.create_dagrun(
            run_type=DagRunType.MANUAL,
            logical_date=DEFAULT_DATE + timedelta(hours=1),
            state=State.QUEUED,
            session=session,
        )
        session.flush()

        self.job_runner._do_scheduling(session)

        # Assert that only 1 dagrun is active
        assert len(DagRun.find(dag_id=dag.dag_id, state=State.RUNNING, session=session)) == 1
        # Assert that the other one is queued
        assert len(DagRun.find(dag_id=dag.dag_id, state=State.QUEUED, session=session)) == 1

    def test_max_active_runs_in_a_dag_doesnt_stop_running_dag_runs_in_other_dags(self, dag_maker):
        session = settings.Session()
        # Explicitly set catchup=True as test specifically expects historical dates to be respected
        with dag_maker(
            "test_dag1",
            start_date=DEFAULT_DATE,
            schedule=timedelta(hours=1),
            max_active_runs=1,
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(29):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        # Explicitly set catchup=True as test specifically expects historical dates to be respected
        with dag_maker(
            "test_dag2",
            start_date=timezone.datetime(2020, 1, 1),
            schedule=timedelta(hours=1),
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(9):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._start_queued_dagruns(session)
        session.flush()
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_running_count = (
            session.query(func.count(DagRun.id))
            .filter(DagRun.dag_id == "test_dag1", DagRun.state == State.RUNNING)
            .scalar()
        )
        running_count = session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
        assert dag1_running_count == 1
        assert running_count == 11

    def test_max_active_runs_in_a_dag_doesnt_prevent_backfill_from_running_catchup_true(self, dag_maker):
        session = settings.Session()
        with dag_maker(
            "test_dag1",
            start_date=DEFAULT_DATE,
            schedule=timedelta(days=1),
            max_active_runs=1,
            catchup=True,
        ) as dag:
            EmptyOperator(task_id="mytask")
        dag1_dag_id = dag.dag_id
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(29):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        with dag_maker(
            "test_dag2",
            start_date=timezone.datetime(2020, 1, 1),
            schedule=timedelta(days=1),
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(9):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._start_queued_dagruns(session)
        session.flush()
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_running_count = (
            session.query(func.count(DagRun.id))
            .filter(DagRun.dag_id == "test_dag1", DagRun.state == State.RUNNING)
            .scalar()
        )
        running_count = session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
        assert dag1_running_count == 1
        assert running_count == 11

        from_date = pendulum.parse("2021-01-01")
        to_date = pendulum.parse("2021-01-06")
        _create_backfill(
            dag_id=dag1_dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=3,
            reverse=False,
            triggering_user_name="test_user",
            dag_run_conf={},
        )
        dag1_running_count = (
            session.query(func.count(DagRun.id))
            .filter(DagRun.dag_id == "test_dag1", DagRun.state == State.RUNNING)
            .scalar()
        )
        assert dag1_running_count == 1
        total_running_count = (
            session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
        )
        assert total_running_count == 11

        # scheduler will now mark backfill runs as running
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dag1_running_count = (
            session.query(func.count(DagRun.id))
            .filter(
                DagRun.dag_id == dag1_dag_id,
                DagRun.state == State.RUNNING,
            )
            .scalar()
        )
        assert dag1_running_count == 4
        total_running_count = (
            session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
        )
        assert total_running_count == 14

        # and doing it again does not change anything
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dag1_running_count = (
            session.query(func.count(DagRun.id))
            .filter(
                DagRun.dag_id == dag1_dag_id,
                DagRun.state == State.RUNNING,
            )
            .scalar()
        )
        assert dag1_running_count == 4
        total_running_count = (
            session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
        )
        assert total_running_count == 14

    def test_max_active_runs_in_a_dag_doesnt_prevent_backfill_from_running_catchup_false(self, dag_maker):
        """Test that with catchup=False, backfills can still run even when max_active_runs is reached for normal DAG runs"""
        session = settings.Session()
        with dag_maker(
            "test_dag1",
            start_date=DEFAULT_DATE,
            schedule=timedelta(days=1),
            max_active_runs=1,
            catchup=False,
        ) as dag:
            EmptyOperator(task_id="mytask")
        dag1_dag_id = dag.dag_id
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        # Fewer DAG runs since we're only testing recent dates with catchup=False
        for _ in range(2):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        with dag_maker(
            "test_dag2",
            start_date=timezone.datetime(2020, 1, 1),
            schedule=timedelta(days=1),
            catchup=False,
        ) as dag:
            EmptyOperator(task_id="mytask")
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(2):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._start_queued_dagruns(session)
        session.flush()
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_running_count = (
            session.query(func.count(DagRun.id))
            .filter(DagRun.dag_id == "test_dag1", DagRun.state == State.RUNNING)
            .scalar()
        )
        running_count = session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
        assert dag1_running_count == 1
        # With catchup=False, only the most recent interval is scheduled for each DAG
        assert (
            running_count == 2
        )  # 1 from test_dag1 (limited by max_active_runs) + 1 from test_dag2 (only most recent with catchup=False)

        # Test that backfills can still run despite max_active_runs being reached for normal runs
        from_date = pendulum.parse("2021-01-01")
        to_date = pendulum.parse("2021-01-06")
        _backfill = _create_backfill(
            dag_id=dag1_dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=3,
            reverse=False,
            triggering_user_name="test_user",
            dag_run_conf={},
        )

        # scheduler will now mark backfill runs as running
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dag1_running_count = (
            session.query(func.count(DagRun.id))
            .filter(
                DagRun.dag_id == dag1_dag_id,
                DagRun.state == State.RUNNING,
            )
            .scalar()
        )
        # Even with catchup=False, backfill runs should start
        assert dag1_running_count == 4
        total_running_count = (
            session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
        )
        assert (
            total_running_count == 5
        )  # 4 from test_dag1 + 1 from test_dag2 (only most recent with catchup=False)

    def test_backfill_runs_are_started_with_lower_priority_catchup_true(self, dag_maker, session):
        """
        Here we are going to create all the runs at the same time and see which
        ones are scheduled first.
        On the first scheduler run, I expect that backfill runs would not be started
        due to being outside the limit in the queued runs query.
        """
        dag1_dag_id = "test_dag1"
        with dag_maker(
            dag_id=dag1_dag_id,
            start_date=DEFAULT_DATE,
            schedule=timedelta(days=1),
            max_active_runs=1,
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")

        def _running_counts():
            dag1_non_b_running = (
                session.query(func.count(DagRun.id))
                .filter(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
                .scalar()
            )
            dag1_b_running = (
                session.query(func.count(DagRun.id))
                .filter(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                )
                .scalar()
            )
            total_running_count = (
                session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
            )
            return dag1_non_b_running, dag1_b_running, total_running_count

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        from_date = pendulum.parse("2021-01-01")
        to_date = pendulum.parse("2021-01-06")
        _create_backfill(
            dag_id=dag1_dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=3,
            reverse=False,
            triggering_user_name="test_user",
            dag_run_conf={},
        )
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()

        # now let's create some "normal" dag runs and verify that they can run
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(29):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        with dag_maker(
            "test_dag2",
            start_date=timezone.datetime(2020, 1, 1),
            schedule=timedelta(days=1),
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(9):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        # initial state -- nothing is running
        assert dag1_non_b_running == 0
        assert dag1_b_running == 0
        assert total_running == 0
        assert session.query(func.count(DagRun.id)).scalar() == 46
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 36

        # now let's run it once
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        # after running the scheduler one time, observe that only one dag run is started
        # this is because there are 30 runs for dag 1 so neither the backfills nor
        # any runs for dag2 get started
        assert DagRun.DEFAULT_DAGRUNS_TO_EXAMINE == 20
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 1
        assert dag1_b_running == 0
        assert total_running == 1
        assert session.scalar(select(func.count()).select_from(DagRun)) == 46
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 36

        # we run scheduler again and observe that now all the runs are created
        # this must be because sorting is working
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 1
        assert dag1_b_running == 3
        assert total_running == 14
        assert session.scalar(select(func.count()).select_from(DagRun)) == 46
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 36

        # run it a 3rd time and nothing changes
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 1
        assert dag1_b_running == 3
        assert total_running == 14
        assert session.scalar(select(func.count()).select_from(DagRun)) == 46
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 36

    def test_backfill_runs_are_started_with_lower_priority_catchup_false(self, dag_maker, session):
        """
        Test that with catchup=False, backfill runs are still started with lower priority than regular DAG runs,
        but the scheduler processes fewer runs overall due to catchup=False behavior.
        """
        dag1_dag_id = "test_dag1"
        with dag_maker(
            dag_id=dag1_dag_id,
            start_date=DEFAULT_DATE,
            schedule=timedelta(days=1),
            max_active_runs=1,
            catchup=False,
        ):
            EmptyOperator(task_id="mytask")

        def _running_counts():
            dag1_non_b_running = (
                session.query(func.count(DagRun.id))
                .filter(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
                .scalar()
            )
            dag1_b_running = (
                session.query(func.count(DagRun.id))
                .filter(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                )
                .scalar()
            )
            total_running_count = (
                session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
            )
            return dag1_non_b_running, dag1_b_running, total_running_count

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        from_date = pendulum.parse("2021-01-01")
        to_date = pendulum.parse("2021-01-06")
        _create_backfill(
            dag_id=dag1_dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=3,
            reverse=False,
            triggering_user_name="test_user",
            dag_run_conf={},
        )
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()

        # Create fewer DAG runs since we're only testing recent dates with catchup=False
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        # With catchup=False, we only create a few runs instead of 29
        for _ in range(4):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        with dag_maker(
            "test_dag2",
            start_date=timezone.datetime(2020, 1, 1),
            schedule=timedelta(days=1),
            catchup=False,
        ):
            EmptyOperator(task_id="mytask")

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        # With catchup=False, we only create a few runs instead of 9
        for _ in range(2):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        # initial state -- nothing is running
        assert dag1_non_b_running == 0
        assert dag1_b_running == 0
        assert total_running == 0
        # Total 14 runs: 5 for dag1 + 3 for dag2 + 6 backfill runs (Jan 1-6 inclusive)
        assert session.query(func.count(DagRun.id)).scalar() == 14

        # now let's run it once
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        # With catchup=False, the scheduler behaves differently than with catchup=True
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        # One normal run starts due to max_active_runs=1
        assert dag1_non_b_running == 1
        # With catchup=False, backfill runs are started immediately alongside regular runs
        assert dag1_b_running == 3
        # Total running = 1 normal dag1 + 3 backfills + 1 from dag2
        assert total_running == 5

        # Running the scheduler again doesn't change anything since we've already reached
        # the limits for both normal runs (max_active_runs=1) and backfill runs (default max_active_runs_per_dag=16)
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        # Still only one normal run due to max_active_runs=1
        assert dag1_non_b_running == 1
        # Backfill runs remain at 3 (the maximum allowed by our test configuration)
        assert dag1_b_running == 3
        # Total running count remains the same
        assert total_running == 5

        # Total runs remain the same
        assert session.query(func.count(DagRun.id)).scalar() == 14

    def test_backfill_maxed_out_no_prevent_non_backfill_max_out(self, dag_maker):
        session = settings.Session()
        dag1_dag_id = "test_dag1"
        with dag_maker(
            dag_id=dag1_dag_id,
            start_date=DEFAULT_DATE,
            schedule=timedelta(days=1),
            max_active_runs=1,
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")

        def _running_counts():
            dag1_non_b_running = (
                session.query(func.count(DagRun.id))
                .filter(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
                .scalar()
            )
            dag1_b_running = (
                session.query(func.count(DagRun.id))
                .filter(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                )
                .scalar()
            )
            total_running_count = (
                session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
            )
            return dag1_non_b_running, dag1_b_running, total_running_count

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        from_date = pendulum.parse("2021-01-01")
        to_date = pendulum.parse("2021-01-06")
        _create_backfill(
            dag_id=dag1_dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=3,
            reverse=False,
            triggering_user_name="test_user",
            dag_run_conf={},
        )
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 0
        assert dag1_b_running == 0
        assert total_running == 0
        assert session.query(func.count(DagRun.id)).scalar() == 6
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 6

        # scheduler will now mark backfill runs as running
        # it should mark 3 of them running since that is backfill max active runs
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 0
        assert dag1_b_running == 3
        assert total_running == 3
        assert session.scalar(select(func.count()).select_from(DagRun)) == 6
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 6

        # and nothing should change if scheduler runs again
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 0
        assert dag1_b_running == 3
        assert total_running == 3
        assert session.scalar(select(func.count()).select_from(DagRun)) == 6
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 6

        # now let's create some "normal" dag runs and verify that they can run
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(29):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        with dag_maker(
            "test_dag2",
            start_date=timezone.datetime(2020, 1, 1),
            schedule=timedelta(days=1),
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        for _ in range(9):
            dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)

        # ok at this point, there are new dag runs created, but no new running runs
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 0
        assert dag1_b_running == 3
        assert total_running == 3
        # we created a lot of drs
        assert session.scalar(select(func.count()).select_from(DagRun)) == 46
        # and in particular there are 36 total runs for dag1
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 36

        # but now let's run the scheduler once
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        # now we should see one more non-backfill run running, and 11 more in total
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 1
        assert dag1_b_running == 3

        # this should be 14 but it is not. why?
        # answer: because dag2 got starved out by dag1
        # if we run the scheduler again, dag2 should get queued
        assert total_running == 4

        assert session.scalar(select(func.count()).select_from(DagRun)) == 46
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 36

        # run scheduler a second time
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 1
        assert dag1_b_running == 3

        # on the second try, dag 2's 10 runs now start running
        assert total_running == 14

        assert session.scalar(select(func.count()).select_from(DagRun)) == 46
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 36

    @pytest.mark.parametrize(
        ("pause_it", "expected_running"),
        [
            (True, 0),
            (False, 3),
        ],
    )
    def test_backfill_runs_not_started_when_backfill_paused(
        self, pause_it, expected_running, dag_maker, session
    ):
        """
        When backfill is paused, will not start.
        """
        dag1_dag_id = "test_dag1"
        # Explicitly needs catchup True for backfill test
        with dag_maker(
            dag_id=dag1_dag_id,
            start_date=DEFAULT_DATE,
            schedule=timedelta(days=1),
            max_active_runs=1,
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")

        def _running_counts():
            dag1_non_b_running = (
                session.query(func.count(DagRun.id))
                .filter(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
                .scalar()
            )
            dag1_b_running = (
                session.query(func.count(DagRun.id))
                .filter(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                )
                .scalar()
            )
            total_running_count = (
                session.query(func.count(DagRun.id)).filter(DagRun.state == State.RUNNING).scalar()
            )
            return dag1_non_b_running, dag1_b_running, total_running_count

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        from_date = pendulum.parse("2021-01-01")
        to_date = pendulum.parse("2021-01-06")
        b = _create_backfill(
            dag_id=dag1_dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=3,
            reverse=False,
            triggering_user_name="test_user",
            dag_run_conf={},
        )
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()

        # initial state -- nothing is running
        assert dag1_non_b_running == 0
        assert dag1_b_running == 0
        assert total_running == 0
        assert session.query(func.count(DagRun.id)).scalar() == 6
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 6

        if pause_it:
            b = session.get(Backfill, b.id)
            b.is_paused = True

        session.commit()

        # now let's run scheduler once
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        assert DagRun.DEFAULT_DAGRUNS_TO_EXAMINE == 20
        dag1_non_b_running, dag1_b_running, total_running = _running_counts()
        assert dag1_non_b_running == 0
        assert dag1_b_running == expected_running
        assert total_running == expected_running
        assert session.scalar(select(func.count()).select_from(DagRun)) == 6
        assert session.scalar(select(func.count()).where(DagRun.dag_id == dag1_dag_id)) == 6

    def test_backfill_runs_skipped_when_lock_held_by_another_scheduler(self, dag_maker, session):
        """Test that a scheduler skips backfill runs when another scheduler holds the lock."""
        dag_id = "test_dag1"
        backfill_max_active_runs = 3
        dag_max_active_runs = 1

        with dag_maker(
            dag_id=dag_id,
            start_date=DEFAULT_DATE,
            schedule=timedelta(days=1),
            max_active_runs=dag_max_active_runs,
            catchup=True,
        ):
            EmptyOperator(task_id="mytask")

        from_date = pendulum.parse("2021-01-01")
        to_date = pendulum.parse("2021-01-05")
        _create_backfill(
            dag_id=dag_id,
            from_date=from_date,
            to_date=to_date,
            max_active_runs=backfill_max_active_runs,
            reverse=False,
            triggering_user_name="test_user",
            dag_run_conf={},
        )

        queued_count = (
            session.query(func.count(DagRun.id))
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state == State.QUEUED,
                DagRun.run_type == DagRunType.BACKFILL_JOB,
            )
            .scalar()
        )
        assert queued_count == 5

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        job_runner = SchedulerJobRunner(job=scheduler_job)

        # Simulate another scheduler holding the lock by returning empty from _lock_backfills
        with patch.object(job_runner, "_lock_backfills", return_value={}):
            job_runner._start_queued_dagruns(session)
            session.flush()

        # No runs should be started because we couldn't acquire the lock
        running_count = (
            session.query(func.count(DagRun.id))
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state == State.RUNNING,
                DagRun.run_type == DagRunType.BACKFILL_JOB,
            )
            .scalar()
        )
        assert running_count == 0, f"Expected 0 running when lock not acquired, but got {running_count}. "
        # no locks now:
        job_runner._start_queued_dagruns(session)
        session.flush()

        running_count = (
            session.query(func.count(DagRun.id))
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state == State.RUNNING,
                DagRun.run_type == DagRunType.BACKFILL_JOB,
            )
            .scalar()
        )
        assert running_count == backfill_max_active_runs
        queued_count = (
            session.query(func.count(DagRun.id))
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.state == State.QUEUED,
                DagRun.run_type == DagRunType.BACKFILL_JOB,
            )
            .scalar()
        )
        # 2 runs are still queued
        assert queued_count == 2

    def test_start_queued_dagruns_do_follow_logical_date_order(self, dag_maker):
        session = settings.Session()
        with dag_maker("test_dag1", max_active_runs=1):
            EmptyOperator(task_id="mytask")
        date = DEFAULT_DATE
        for i in range(30):
            dr = dag_maker.create_dagrun(
                run_id=f"dagrun_{i}",
                run_type=DagRunType.SCHEDULED,
                state=State.QUEUED,
                logical_date=date,
            )
            date = dr.logical_date + timedelta(hours=1)
        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dr = DagRun.find(run_id="dagrun_0")
        ti = dr[0].get_task_instance(task_id="mytask", session=session)
        ti.state = State.SUCCESS
        session.merge(ti)
        session.commit()
        assert dr[0].state == State.RUNNING
        dr[0].state = State.SUCCESS
        session.merge(dr[0])
        session.flush()
        assert dr[0].state == State.SUCCESS
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dr = DagRun.find(run_id="dagrun_1")
        assert len(session.query(DagRun).filter(DagRun.state == State.RUNNING).all()) == 1

        assert dr[0].state == State.RUNNING

    def test_no_dagruns_would_stuck_in_running(self, dag_maker):
        # Test that running dagruns are not stuck in running.
        # Create one dagrun in 'running' state and 1 in 'queued' state from one dag(max_active_runs=1)
        # Create 16 dagruns in 'running' state and 16 in 'queued' state from another dag
        # Create 16 dagruns in 'running' state and 16 in 'queued' state from yet another dag
        # Finish the task of the first dag, and check that another dagrun starts running
        # from the first dag.

        session = settings.Session()
        # first dag and dagruns
        date = timezone.datetime(2016, 1, 1)
        logical_date = timezone.coerce_datetime(date)
        with dag_maker("test_dagrun_states_are_correct_1", max_active_runs=1, start_date=date) as dag:
            task1 = EmptyOperator(task_id="dummy_task")

        dr1_running = dag_maker.create_dagrun(run_id="dr1_run_1", logical_date=date)
        data_interval = infer_automated_data_interval(dag.timetable, logical_date)
        dag_maker.create_dagrun(
            run_id="dr1_run_2",
            state=State.QUEUED,
            logical_date=dag.next_dagrun_info(
                last_automated_dagrun=data_interval, restricted=False
            ).data_interval.start,
        )
        # second dag and dagruns
        date = timezone.datetime(2020, 1, 1)
        with dag_maker("test_dagrun_states_are_correct_2", start_date=date) as dag:
            EmptyOperator(task_id="dummy_task")
        for i in range(16):
            dr = dag_maker.create_dagrun(
                run_id=f"dr2_run_{i + 1}",
                state=State.RUNNING,
                logical_date=date,
            )
            date = dr.logical_date + timedelta(hours=1)
        dr16 = DagRun.find(run_id="dr2_run_16")
        date = dr16[0].logical_date + timedelta(hours=1)
        for i in range(16, 32):
            dr = dag_maker.create_dagrun(
                run_id=f"dr2_run_{i + 1}",
                state=State.QUEUED,
                logical_date=date,
            )
            date = dr.logical_date + timedelta(hours=1)

        # third dag and dagruns
        date = timezone.datetime(2021, 1, 1)
        with dag_maker("test_dagrun_states_are_correct_3", start_date=date) as dag:
            EmptyOperator(task_id="dummy_task")
        for i in range(16):
            dr = dag_maker.create_dagrun(
                run_id=f"dr3_run_{i + 1}",
                state=State.RUNNING,
                logical_date=date,
            )
            date = dr.logical_date + timedelta(hours=1)
        dr16 = DagRun.find(run_id="dr3_run_16")
        date = dr16[0].logical_date + timedelta(hours=1)
        for i in range(16, 32):
            dr = dag_maker.create_dagrun(
                run_id=f"dr2_run_{i + 1}",
                state=State.QUEUED,
                logical_date=date,
            )
            date = dr.logical_date + timedelta(hours=1)

        scheduler_job = Job(executor=MockExecutor(do_update=False))
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dag_version = DagVersion.get_latest_version(dag_id=dag.dag_id)
        ti = TaskInstance(task=task1, run_id=dr1_running.run_id, dag_version_id=dag_version.id)
        ti.refresh_from_db()
        ti.state = State.SUCCESS
        session.merge(ti)
        session.flush()
        # Run the scheduler loop
        with mock.patch.object(settings, "USE_JOB_SCHEDULE", False):
            self.job_runner._do_scheduling(session)
            self.job_runner._do_scheduling(session)

        assert DagRun.find(run_id="dr1_run_1")[0].state == State.SUCCESS
        assert DagRun.find(run_id="dr1_run_2")[0].state == State.RUNNING

    @pytest.mark.parametrize(
        ("state", "start_date", "end_date"),
        [
            [State.NONE, None, None],
            [
                State.UP_FOR_RETRY,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
            [
                State.UP_FOR_RESCHEDULE,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
        ],
    )
    def test_dag_file_processor_process_task_instances(self, state, start_date, end_date, dag_maker):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        with dag_maker(dag_id="test_scheduler_process_execute_task"):
            BashOperator(task_id="dummy", bash_command="echo hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        assert dr is not None

        with create_session() as session:
            ti = dr.get_task_instances(session=session)[0]
            ti.state = state
            ti.start_date = start_date
            ti.end_date = end_date

            self.job_runner._schedule_dag_run(dr, session)
            assert session.query(TaskInstance).filter_by(state=State.SCHEDULED).count() == 1

            session.refresh(ti)
            assert ti.state == State.SCHEDULED

    @pytest.mark.parametrize(
        ("state", "start_date", "end_date"),
        [
            [State.NONE, None, None],
            [
                State.UP_FOR_RETRY,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
            [
                State.UP_FOR_RESCHEDULE,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
        ],
    )
    def test_dag_file_processor_process_task_instances_with_max_active_tis_per_dag(
        self, state, start_date, end_date, dag_maker
    ):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        with dag_maker(dag_id="test_scheduler_process_execute_task_with_max_active_tis_per_dag"):
            BashOperator(task_id="dummy", max_active_tis_per_dag=2, bash_command="echo Hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
        )
        assert dr is not None

        with create_session() as session:
            ti = dr.get_task_instances(session=session)[0]
            ti.state = state
            ti.start_date = start_date
            ti.end_date = end_date

            self.job_runner._schedule_dag_run(dr, session)
            assert session.query(TaskInstance).filter_by(state=State.SCHEDULED).count() == 1

            session.refresh(ti)
            assert ti.state == State.SCHEDULED

    @pytest.mark.parametrize(
        ("state", "start_date", "end_date"),
        [
            [State.NONE, None, None],
            [
                State.UP_FOR_RETRY,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
            [
                State.UP_FOR_RESCHEDULE,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
        ],
    )
    def test_dag_file_processor_process_task_instances_with_max_active_tis_per_dagrun(
        self, state, start_date, end_date, dag_maker
    ):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        with dag_maker(dag_id="test_scheduler_process_execute_task_with_max_active_tis_per_dagrun"):
            BashOperator(task_id="dummy", max_active_tis_per_dagrun=2, bash_command="echo Hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
        )
        assert dr is not None

        with create_session() as session:
            ti = dr.get_task_instances(session=session)[0]
            ti.state = state
            ti.start_date = start_date
            ti.end_date = end_date

            self.job_runner._schedule_dag_run(dr, session)
            assert session.query(TaskInstance).filter_by(state=State.SCHEDULED).count() == 1

            session.refresh(ti)
            assert ti.state == State.SCHEDULED

    @pytest.mark.parametrize(
        ("state", "start_date", "end_date"),
        [
            [State.NONE, None, None],
            [
                State.UP_FOR_RETRY,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
            [
                State.UP_FOR_RESCHEDULE,
                timezone.utcnow() - datetime.timedelta(minutes=30),
                timezone.utcnow() - datetime.timedelta(minutes=15),
            ],
        ],
    )
    def test_dag_file_processor_process_task_instances_depends_on_past(
        self, state, start_date, end_date, dag_maker
    ):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        with dag_maker(
            dag_id="test_scheduler_process_execute_task_depends_on_past",
            default_args={
                "depends_on_past": True,
            },
        ):
            BashOperator(task_id="dummy1", bash_command="echo hi")
            BashOperator(task_id="dummy2", bash_command="echo hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
        )
        assert dr is not None

        with create_session() as session:
            tis = dr.get_task_instances(session=session)
            for ti in tis:
                ti.state = state
                ti.start_date = start_date
                ti.end_date = end_date

            self.job_runner._schedule_dag_run(dr, session)
            assert session.query(TaskInstance).filter_by(state=State.SCHEDULED).count() == 2

            session.refresh(tis[0])
            session.refresh(tis[1])
            assert tis[0].state == State.SCHEDULED
            assert tis[1].state == State.SCHEDULED

    def test_scheduler_job_add_new_task(self, dag_maker):
        """
        Test if a task instance will be added if the dag is updated
        """
        with dag_maker(dag_id="test_scheduler_add_new_task", serialized=False) as dag:
            BashOperator(task_id="dummy", bash_command="echo test")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        session = settings.Session()
        orm_dag = dag_maker.dag_model
        assert orm_dag is not None

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._create_dag_runs([orm_dag], session)

        drs = (
            session.query(DagRun)
            .options(joinedload(DagRun.task_instances).joinedload(TaskInstance.dag_version))
            .all()
        )
        assert len(drs) == 1
        dr = drs[0]

        tis = dr.get_task_instances(session=session)
        assert len(tis) == 1

        BashOperator(task_id="dummy2", dag=dag, bash_command="echo test")
        sync_dag_to_db(dag_maker.dag, bundle_name="dag_maker", session=session)
        session.commit()
        self.job_runner._schedule_dag_run(dr, session)
        session.expunge_all()
        assert session.query(TaskInstance).filter_by(state=State.SCHEDULED).count() == 2
        session.flush()

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        tis = dr.get_task_instances(session=session)
        assert len(tis) == 2

    @pytest.mark.need_serialized_dag
    def test_runs_respected_after_clear(self, dag_maker, session):
        """
        Test dag after dag.clear, max_active_runs is respected
        """
        with dag_maker(
            dag_id="test_scheduler_max_active_runs_respected_after_clear",
            start_date=DEFAULT_DATE,
            max_active_runs=1,
        ) as dag:
            BashOperator(task_id="dummy", bash_command="echo Hi")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        dr = dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        dag_maker.create_dagrun_after(dr, run_type=DagRunType.SCHEDULED, state=State.QUEUED)
        dag.clear(session=session)
        assert len(DagRun.find(dag_id=dag.dag_id, state=State.QUEUED, session=session)) == 3

        self.job_runner._start_queued_dagruns(session)
        session.flush()

        # Assert that only 1 dagrun is active
        assert len(DagRun.find(dag_id=dag.dag_id, state=State.RUNNING, session=session)) == 1
        # Assert that the other two are queued
        assert len(DagRun.find(dag_id=dag.dag_id, state=State.QUEUED, session=session)) == 2

    def test_timeout_triggers(self, dag_maker):
        """
        Tests that tasks in the deferred state, but whose trigger timeout
        has expired, are correctly failed.

        """
        session = settings.Session()
        # Create the test DAG and task
        with dag_maker(
            dag_id="test_timeout_triggers",
            start_date=DEFAULT_DATE,
            schedule="@once",
            max_active_runs=1,
            session=session,
        ):
            EmptyOperator(task_id="dummy1")

        # Create a Task Instance for the task that is allegedly deferred
        # but past its timeout, and one that is still good.
        # We don't actually need a linked trigger here; the code doesn't check.
        dr1 = dag_maker.create_dagrun()
        dr2 = dag_maker.create_dagrun(
            run_id="test2", logical_date=DEFAULT_DATE + datetime.timedelta(seconds=1)
        )
        ti1 = dr1.get_task_instance("dummy1", session)
        ti2 = dr2.get_task_instance("dummy1", session)
        ti1.state = State.DEFERRED
        ti1.trigger_timeout = timezone.utcnow() - datetime.timedelta(seconds=60)
        ti2.state = State.DEFERRED
        ti2.trigger_timeout = timezone.utcnow() + datetime.timedelta(seconds=60)
        session.flush()

        # Boot up the scheduler and make it check timeouts
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner.check_trigger_timeouts(session=session)

        # Make sure that TI1 is now scheduled to fail, and 2 wasn't touched
        session.refresh(ti1)
        session.refresh(ti2)
        assert ti1.state == State.SCHEDULED
        assert ti1.next_method == "__fail__"
        assert ti2.state == State.DEFERRED

    def test_retry_on_db_error_when_update_timeout_triggers(self, dag_maker, testing_dag_bundle, session):
        """
        Tests that it will retry on DB error like deadlock when updating timeout triggers.
        """
        from sqlalchemy.exc import OperationalError

        retry_times = 3

        # Create the test DAG and task
        with dag_maker(
            dag_id="test_retry_on_db_error_when_update_timeout_triggers",
            start_date=DEFAULT_DATE,
            schedule="@once",
            max_active_runs=1,
            session=session,
        ):
            EmptyOperator(task_id="dummy1")

        # Mock the db failure within retry times
        might_fail_session = MagicMock(wraps=session)

        def check_if_trigger_timeout(max_retries: int):
            def make_side_effect():
                call_count = 0

                def side_effect(*args, **kwargs):
                    nonlocal call_count
                    if call_count < retry_times - 1:
                        call_count += 1
                        raise OperationalError("any_statement", "any_params", "any_orig")
                    return session.execute(*args, **kwargs)

                return side_effect

            might_fail_session.execute.side_effect = make_side_effect()

            try:
                # Create a Task Instance for the task that is allegedly deferred
                # but past its timeout, and one that is still good.
                # We don't actually need a linked trigger here; the code doesn't check.
                sync_dag_to_db(dag_maker.dag, session=session)
                dr1 = dag_maker.create_dagrun()
                dr2 = dag_maker.create_dagrun(
                    run_id="test2", logical_date=DEFAULT_DATE + datetime.timedelta(seconds=1)
                )
                ti1 = dr1.get_task_instance("dummy1", session)
                ti2 = dr2.get_task_instance("dummy1", session)
                ti1.state = State.DEFERRED
                ti1.trigger_timeout = timezone.utcnow() - datetime.timedelta(seconds=60)
                ti2.state = State.DEFERRED
                ti2.trigger_timeout = timezone.utcnow() + datetime.timedelta(seconds=60)
                session.flush()

                # Boot up the scheduler and make it check timeouts
                scheduler_job = Job()
                self.job_runner = SchedulerJobRunner(job=scheduler_job)

                self.job_runner.check_trigger_timeouts(max_retries=max_retries, session=might_fail_session)

                # Make sure that TI1 is now scheduled to fail, and 2 wasn't touched
                session.refresh(ti1)
                session.refresh(ti2)
                assert ti1.state == State.SCHEDULED
                assert ti1.next_method == "__fail__"
                assert ti2.state == State.DEFERRED
            finally:
                self.clean_db()

        # Positive case, will retry until success before reach max retry times
        check_if_trigger_timeout(retry_times)

        # Negative case: no retries, execute only once.
        with pytest.raises(OperationalError):
            check_if_trigger_timeout(1)

    def test_find_and_purge_task_instances_without_heartbeats_nothing(self):
        executor = MockExecutor(do_update=False)
        scheduler_job = Job(executor=executor)
        with mock.patch("airflow.executors.executor_loader.ExecutorLoader.load_executor") as loader_mock:
            loader_mock.return_value = executor
            self.job_runner = SchedulerJobRunner(scheduler_job)

            self.job_runner._find_and_purge_task_instances_without_heartbeats()
        executor.callback_sink.send.assert_not_called()

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_find_and_purge_task_instances_without_heartbeats(self, session, create_dagrun):
        dagfile = EXAMPLE_STANDARD_DAGS_FOLDER / "example_branch_operator.py"
        dagbag = DagBag(dagfile)
        dag = dagbag.get_dag("example_branch_operator")
        scheduler_dag = sync_dag_to_db(dag)

        dag_v = DagVersion.get_latest_version(dag.dag_id)

        data_interval = infer_automated_data_interval(scheduler_dag.timetable, DEFAULT_LOGICAL_DATE)

        dag_run = create_dagrun(
            scheduler_dag,
            logical_date=DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            data_interval=data_interval,
        )

        executor = MockExecutor()
        scheduler_job = Job(executor=executor)
        with mock.patch("airflow.executors.executor_loader.ExecutorLoader.load_executor") as loader_mock:
            loader_mock.return_value = executor
            self.job_runner = SchedulerJobRunner(job=scheduler_job)

            # We will provision 2 tasks so we can check we only find task instances without heartbeats from this scheduler
            tasks_to_setup = ["branching", "run_this_first"]

            for task_id in tasks_to_setup:
                task = dag.get_task(task_id=task_id)
                ti = TaskInstance(task, run_id=dag_run.run_id, state=State.RUNNING, dag_version_id=dag_v.id)

                ti.last_heartbeat_at = timezone.utcnow() - timedelta(minutes=6)
                ti.start_date = timezone.utcnow() - timedelta(minutes=10)
                ti.queued_by_job_id = 999

                session.add(ti)
                session.flush()

            assert task.task_id == "run_this_first"  # Make sure we have the task/ti we expect

            ti.queued_by_job_id = scheduler_job.id
            session.flush()
            executor.running.add(ti.key)  # The executor normally does this during heartbeat.
            self.job_runner._find_and_purge_task_instances_without_heartbeats()
            assert ti.key not in executor.running

        executor.callback_sink.send.assert_called_once()
        callback_requests = executor.callback_sink.send.call_args.args
        assert len(callback_requests) == 1
        callback_request = callback_requests[0]
        assert callback_request.filepath == dag.relative_fileloc
        assert callback_request.msg == str(
            self.job_runner._generate_task_instance_heartbeat_timeout_message_details(ti)
        )
        assert callback_request.is_failure_callback is True
        assert callback_request.ti.dag_id == ti.dag_id
        assert callback_request.ti.task_id == ti.task_id
        assert callback_request.ti.run_id == ti.run_id
        assert callback_request.ti.map_index == ti.map_index

        # Verify context_from_server is passed
        assert callback_request.context_from_server is not None
        assert callback_request.context_from_server.dag_run.logical_date == ti.dag_run.logical_date
        assert callback_request.context_from_server.max_tries == ti.max_tries

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_task_instance_heartbeat_timeout_message(self, session, create_dagrun):
        """
        Check that the task instance heartbeat timeout message comes out as expected
        """
        dagfile = EXAMPLE_STANDARD_DAGS_FOLDER / "example_branch_operator.py"
        dagbag = DagBag(dagfile)
        dag = dagbag.get_dag("example_branch_operator")
        scheduler_dag = sync_dag_to_db(dag, session=session)
        session.query(Job).delete()

        data_interval = infer_automated_data_interval(scheduler_dag.timetable, DEFAULT_LOGICAL_DATE)
        dag_run = create_dagrun(
            scheduler_dag,
            logical_date=DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            data_interval=data_interval,
        )

        scheduler_job = Job(executor=MockExecutor())
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # We will provision 2 tasks so we can check we only find task instance heartbeat timeouts from this scheduler
        tasks_to_setup = ["branching", "run_this_first"]
        dag_version = DagVersion.get_latest_version(dag.dag_id)
        for task_id in tasks_to_setup:
            task = dag.get_task(task_id=task_id)
            ti = TaskInstance(task, run_id=dag_run.run_id, state=State.RUNNING, dag_version_id=dag_version.id)
            ti.queued_by_job_id = 999

            session.add(ti)
            session.flush()

        assert task.task_id == "run_this_first"  # Make sure we have the task/ti we expect

        ti.queued_by_job_id = scheduler_job.id
        session.flush()

        task_instance_heartbeat_timeout_message = (
            self.job_runner._generate_task_instance_heartbeat_timeout_message_details(ti)
        )
        assert task_instance_heartbeat_timeout_message == {
            "DAG Id": "example_branch_operator",
            "Task Id": "run_this_first",
            "Run Id": "scheduled__2016-01-01T00:00:00+00:00",
        }

        ti.hostname = "10.10.10.10"
        ti.map_index = 2
        ti.external_executor_id = "abcdefg"

        task_instance_heartbeat_timeout_message = (
            self.job_runner._generate_task_instance_heartbeat_timeout_message_details(ti)
        )
        assert task_instance_heartbeat_timeout_message == {
            "DAG Id": "example_branch_operator",
            "Task Id": "run_this_first",
            "Run Id": "scheduled__2016-01-01T00:00:00+00:00",
            "Hostname": "10.10.10.10",
            "Map Index": 2,
            "External Executor Id": "abcdefg",
        }

    @mock.patch.object(settings, "USE_JOB_SCHEDULE", False)
    def run_scheduler_until_dagrun_terminal(self):
        """
        Run a scheduler until any dag run reaches a terminal state, or the scheduler becomes "idle".

        This needs a DagRun to be pre-created (it can be in running or queued state) as no more will be
        created as we turn off creating new DagRuns via setting USE_JOB_SCHEDULE to false

        Note: This doesn't currently account for tasks that go into retry -- the scheduler would be detected
        as idle in that circumstance
        """

        # Spy on _do_scheduling and _process_executor_events so we can notice
        # if nothing happened, and abort early! If there is nothing
        # to schedule and no events, it means we have stalled.
        def spy_on_return(orig, result):
            def spy(*args, **kwargs):
                ret = orig(*args, **kwargs)
                result.append(ret)
                return ret

            return spy

        num_queued_tis: deque[int] = deque([], 3)
        num_finished_events: deque[int] = deque([], 3)

        do_scheduling_spy = mock.patch.object(
            self.job_runner,
            "_do_scheduling",
            side_effect=spy_on_return(self.job_runner._do_scheduling, num_queued_tis),
        )
        executor_events_spy = mock.patch.object(
            self.job_runner,
            "_process_executor_events",
            side_effect=spy_on_return(self.job_runner._process_executor_events, num_finished_events),
        )

        orig_set_state = DagRun.set_state

        def watch_set_state(dr: DagRun, state, **kwargs):
            if state in (DagRunState.SUCCESS, DagRunState.FAILED):
                # Stop the scheduler
                self.job_runner.num_runs = 1  # type: ignore[union-attr]
            orig_set_state(dr, state, **kwargs)

        def watch_heartbeat(*args, **kwargs):
            if len(num_queued_tis) < 3 or len(num_finished_events) < 3:
                return
            queued_any_tis = any(val > 0 for val in num_queued_tis)
            finished_any_events = any(val > 0 for val in num_finished_events)
            assert queued_any_tis or finished_any_events, (
                "Scheduler has stalled without setting the DagRun state!"
            )

        set_state_spy = mock.patch.object(DagRun, "set_state", new=watch_set_state)
        heartbeat_spy = mock.patch.object(self.job_runner.job, "heartbeat", new=watch_heartbeat)

        # with heartbeat_spy, set_state_spy, do_scheduling_spy, executor_events_spy:
        with heartbeat_spy, set_state_spy, do_scheduling_spy, executor_events_spy:
            run_job(self.job_runner.job, execute_callable=self.job_runner._execute)

    @pytest.mark.long_running
    @pytest.mark.parametrize("dag_id", ["test_mapped_classic", "test_mapped_taskflow"])
    def test_mapped_dag(self, dag_id, session, testing_dag_bundle):
        """End-to-end test of a simple mapped dag"""
        from airflow.executors.local_executor import LocalExecutor

        dagbag = DagBag(dag_folder=TEST_DAGS_FOLDER, include_examples=False)
        sync_bag_to_db(dagbag, "testing", None)
        dagbag.process_file(str(TEST_DAGS_FOLDER / f"{dag_id}.py"))
        dag = dagbag.get_dag(dag_id)
        assert dag
        logical_date = timezone.coerce_datetime(timezone.utcnow() - datetime.timedelta(days=2))
        data_interval = infer_automated_data_interval(dag.timetable, logical_date)

        dr = dag.create_dagrun(
            run_id=f"{dag_id}_1",
            run_type=DagRunType.MANUAL,
            start_date=timezone.utcnow(),
            state=State.RUNNING,
            session=session,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=data_interval,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        executor = LocalExecutor()

        job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(job=job)
        self.run_scheduler_until_dagrun_terminal()

        dr.refresh_from_db(session)
        assert dr.state == DagRunState.SUCCESS

    def test_should_mark_empty_task_as_success(self, testing_dag_bundle):
        dag_file = Path(__file__).parents[1] / "dags/test_only_empty_tasks.py"

        # Write DAGs to dag and serialized_dag table
        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        sync_bag_to_db(dagbag, "testing", None)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        # Create DagRun
        session = settings.Session()
        orm_dag = session.get(DagModel, "test_only_empty_tasks")
        self.job_runner._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id="test_only_empty_tasks", session=session)
        assert len(drs) == 1
        dr = drs[0]

        # Schedule TaskInstances
        self.job_runner._schedule_dag_run(dr, session)
        session.expunge_all()
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        dags = self.job_runner.scheduler_dag_bag._dags.values()
        assert [dag.dag_id for dag in dags] == ["test_only_empty_tasks"]
        assert len(tis) == 6
        assert {
            ("test_task_a", "success"),
            ("test_task_b", None),
            ("test_task_c", "success"),
            ("test_task_on_execute", "scheduled"),
            ("test_task_on_success", "scheduled"),
            ("test_task_outlets", "scheduled"),
        } == {(ti.task_id, ti.state) for ti in tis}
        for state, start_date, end_date, duration in [
            (ti.state, ti.start_date, ti.end_date, ti.duration) for ti in tis
        ]:
            if state == "success":
                assert start_date is not None
                assert end_date is not None
                assert duration == 0.0
            else:
                assert start_date is None
                assert end_date is None
                assert duration is None

        self.job_runner._schedule_dag_run(dr, session)
        session.expunge_all()
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        assert len(tis) == 6
        assert {
            ("test_task_a", "success"),
            ("test_task_b", "success"),
            ("test_task_c", "success"),
            ("test_task_on_execute", "scheduled"),
            ("test_task_on_success", "scheduled"),
            ("test_task_outlets", "scheduled"),
        } == {(ti.task_id, ti.state) for ti in tis}
        for state, start_date, end_date, duration in [
            (ti.state, ti.start_date, ti.end_date, ti.duration) for ti in tis
        ]:
            if state == "success":
                assert start_date is not None
                assert end_date is not None
                assert duration == 0.0
            else:
                assert start_date is None
                assert end_date is None
                assert duration is None

    @pytest.mark.need_serialized_dag
    def test_catchup_works_correctly(self, dag_maker, testing_dag_bundle):
        """Test that catchup works correctly"""
        session = settings.Session()
        with dag_maker(
            dag_id="test_catchup_schedule_dag",
            schedule=timedelta(days=1),
            start_date=DEFAULT_DATE,
            catchup=True,
            max_active_runs=1,
            session=session,
        ) as dag:
            EmptyOperator(task_id="dummy")

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._create_dag_runs([dag_maker.dag_model], session)
        self.job_runner._start_queued_dagruns(session)
        # first dagrun logical date is DEFAULT_DATE 2016-01-01T00:00:00+00:00
        dr = DagRun.find(logical_date=DEFAULT_DATE, session=session)[0]
        ti = dr.get_task_instance(task_id="dummy")
        ti.state = State.SUCCESS
        session.merge(ti)
        session.flush()

        self.job_runner._schedule_dag_run(dr, session)
        session.flush()

        # Run the second time so _update_dag_next_dagrun will run
        self.job_runner._schedule_dag_run(dr, session)
        session.flush()

        dag_maker.dag.catchup = False
        dag = sync_dag_to_db(dag_maker.dag, bundle_name="dag_maker", session=session)
        assert not dag.catchup

        dm = DagModel.get_dagmodel(dag.dag_id)
        self.job_runner._create_dag_runs([dm], session)

        # Check catchup worked correctly by ensuring logical_date is quite new
        # Our dag is a daily dag
        assert (
            session.query(DagRun.logical_date)
            .filter(DagRun.logical_date != DEFAULT_DATE)  # exclude the first run
            .scalar()
        ) > (timezone.utcnow() - timedelta(days=2))

    def test_update_dagrun_state_for_paused_dag(self, dag_maker, session):
        """Test that _update_dagrun_state_for_paused_dag puts DagRuns in terminal states"""
        with dag_maker("testdag") as dag:
            EmptyOperator(task_id="task1")

        scheduled_run = dag_maker.create_dagrun(
            logical_date=datetime.datetime(2022, 1, 1),
            run_type=DagRunType.SCHEDULED,
        )
        scheduled_run.last_scheduling_decision = datetime.datetime.now(timezone.utc) - timedelta(minutes=1)
        ti = scheduled_run.get_task_instances(session=session)[0]
        ti.set_state(TaskInstanceState.RUNNING)
        dm = DagModel.get_dagmodel(dag.dag_id, session)
        dm.is_paused = True
        session.flush()

        assert scheduled_run.state == State.RUNNING

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._update_dag_run_state_for_paused_dags(session=session)
        session.flush()

        # TI still running, DagRun left in running
        (scheduled_run,) = DagRun.find(dag_id=dag.dag_id, run_type=DagRunType.SCHEDULED, session=session)
        assert scheduled_run.state == State.RUNNING
        prior_last_scheduling_decision = scheduled_run.last_scheduling_decision

        # Make sure we don't constantly try dagruns over and over
        self.job_runner._update_dag_run_state_for_paused_dags(session=session)
        (scheduled_run,) = DagRun.find(dag_id=dag.dag_id, run_type=DagRunType.SCHEDULED, session=session)
        assert scheduled_run.state == State.RUNNING
        # last_scheduling_decision is bumped by update_state, so check that to determine if we tried again
        assert prior_last_scheduling_decision == scheduled_run.last_scheduling_decision

        # Once the TI is in a terminal state though, DagRun goes to success
        ti.set_state(TaskInstanceState.SUCCESS, session=session)

        self.job_runner._update_dag_run_state_for_paused_dags(session=session)
        (scheduled_run,) = DagRun.find(dag_id=dag.dag_id, run_type=DagRunType.SCHEDULED, session=session)
        assert scheduled_run.state == State.SUCCESS

    def test_update_dagrun_state_for_paused_dag_not_for_backfill(self, dag_maker, session):
        """Test that the _update_dagrun_state_for_paused_dag does not affect backfilled dagruns"""
        with dag_maker("testdag") as dag:
            EmptyOperator(task_id="task1")

        # Backfill run
        backfill_run = dag_maker.create_dagrun(run_type=DagRunType.BACKFILL_JOB)
        backfill_run.last_scheduling_decision = datetime.datetime.now(timezone.utc) - timedelta(minutes=1)
        ti = backfill_run.get_task_instances(session=session)[0]
        ti.set_state(TaskInstanceState.SUCCESS, session=session)
        dm = DagModel.get_dagmodel(dag.dag_id, session=session)
        dm.is_paused = True
        session.flush()

        assert backfill_run.state == State.RUNNING

        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        self.job_runner._update_dag_run_state_for_paused_dags(session=session)
        session.flush()

        (backfill_run,) = DagRun.find(dag_id=dag.dag_id, run_type=DagRunType.BACKFILL_JOB, session=session)
        assert backfill_run.state == State.SUCCESS

    @staticmethod
    def _find_assets_activation(session) -> tuple[list[AssetModel], list[AssetModel]]:
        assets = session.execute(
            select(AssetModel, AssetActive)
            .outerjoin(
                AssetActive,
                (AssetModel.name == AssetActive.name) & (AssetModel.uri == AssetActive.uri),
            )
            .order_by(AssetModel.uri)
        ).all()
        return [a for a, v in assets if not v], [a for a, v in assets if v]

    def test_asset_orphaning(self, dag_maker, session):
        self.job_runner = SchedulerJobRunner(job=Job())

        asset1 = Asset(uri="test://asset_1", name="test_asset_1", group="test_group")
        asset2 = Asset(uri="test://asset_2", name="test_asset_2", group="test_group")
        asset3 = Asset(uri="test://asset_3", name="test_asset_3", group="test_group")
        asset4 = Asset(uri="test://asset_4", name="test_asset_4", group="test_group")
        asset5 = Asset(uri="test://asset_5", name="test_asset_5", group="test_group")
        asset6 = Asset(uri="test://asset_5", name="test_asset_5", group="test_group")

        with dag_maker(dag_id="assets-1", schedule=[asset1, asset2], session=session):
            BashOperator(task_id="task", bash_command="echo 1", outlets=[asset3, asset4], inlets=[asset6])

        # asset5 is not registered (since it's not used anywhere).
        orphaned, active = self._find_assets_activation(session)
        assert active == [asset1, asset2, asset3, asset4, asset6]
        assert orphaned == []

        self.job_runner._update_asset_orphanage(session=session)
        session.flush()

        assert active == [asset1, asset2, asset3, asset4, asset6]
        assert orphaned == []

        # Now remove 2 asset references and add asset5.
        with dag_maker(dag_id="assets-1", schedule=[asset1], session=session):
            BashOperator(task_id="task", bash_command="echo 1", outlets=[asset3, asset5])

        # The DAG parser finds asset5.
        orphaned, active = self._find_assets_activation(session)
        assert active == [asset1, asset2, asset3, asset4, asset5]
        assert orphaned == []

        self.job_runner._update_asset_orphanage(session=session)
        session.flush()

        # Now we get the updated result.
        orphaned, active = self._find_assets_activation(session)
        assert active == [asset1, asset3, asset5]
        assert orphaned == [asset2, asset4]

    def test_asset_orphaning_ignore_orphaned_assets(self, dag_maker, session):
        self.job_runner = SchedulerJobRunner(job=Job())

        asset1 = Asset(uri="test://asset_1", name="test_asset_1", group="test_group")

        with dag_maker(dag_id="assets-1", schedule=[asset1], session=session):
            BashOperator(task_id="task", bash_command="echo 1")

        orphaned, active = self._find_assets_activation(session)
        assert active == [asset1]
        assert orphaned == []

        self.job_runner._update_asset_orphanage(session=session)
        session.flush()

        # now remove asset1 reference
        with dag_maker(dag_id="assets-1", schedule=None, session=session):
            BashOperator(task_id="task", bash_command="echo 1")

        self.job_runner._update_asset_orphanage(session=session)
        session.flush()

        orphaned, active = self._find_assets_activation(session)
        assert active == []
        assert orphaned == [asset1]
        updated_at_timestamps = [asset.updated_at for asset in orphaned]

        # when rerunning we should ignore the already orphaned assets and thus the updated_at timestamp
        # should remain the same
        self.job_runner._update_asset_orphanage(session=session)
        session.flush()

        orphaned, active = self._find_assets_activation(session)
        assert active == []
        assert orphaned == [asset1]
        assert [asset.updated_at for asset in orphaned] == updated_at_timestamps

    @pytest.mark.parametrize(
        ("paused", "stale", "expected_classpath"),
        [
            pytest.param(
                False,
                False,
                "airflow.providers.standard.triggers.temporal.DateTimeTrigger",
                id="active",
            ),
            pytest.param(False, True, None, id="stale"),
            pytest.param(True, False, None, id="paused"),
            pytest.param(True, False, None, id="stale-paused"),
        ],
    )
    @pytest.mark.need_serialized_dag(False)
    def test_delete_unreferenced_triggers(self, dag_maker, session, paused, stale, expected_classpath):
        self.job_runner = SchedulerJobRunner(job=Job())

        classpath, kwargs = DateTimeTrigger(timezone.utcnow()).serialize()
        asset1 = Asset(
            name="test_asset_1",
            watchers=[AssetWatcher(name="test", trigger={"classpath": classpath, "kwargs": kwargs})],
        )
        with dag_maker(dag_id="dag", schedule=[asset1], session=session) as dag:
            EmptyOperator(task_id="task")
        dags = {"dag": LazyDeserializedDAG.from_dag(dag)}

        def _update_references() -> None:
            asset_op = AssetModelOperation.collect(dags)
            orm_assets = asset_op.sync_assets(session=session)
            session.flush()
            asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
            asset_op.activate_assets_if_possible(orm_assets.values(), session=session)
            asset_op.add_asset_trigger_references(orm_assets, session=session)
            session.flush()

        # Initial setup.
        orm_dags = DagModelOperation({"dag": dag}, "testing", None).add_dags(session=session)
        _update_references()
        assert session.scalars(select(Trigger.classpath)).one() == classpath

        # Simulate dag state change.
        orm_dags["dag"].is_paused = paused
        orm_dags["dag"].is_stale = stale
        _update_references()
        assert session.scalars(select(Trigger.classpath)).one() == classpath

        # Unreferenced trigger should be removed.
        self.job_runner._remove_unreferenced_triggers(session=session)
        assert session.scalars(select(Trigger.classpath)).one_or_none() == expected_classpath

    def test_misconfigured_dags_doesnt_crash_scheduler(self, session, dag_maker, caplog):
        """Test that if dagrun creation throws an exception, the scheduler doesn't crash"""
        with dag_maker("testdag1", serialized=True):
            BashOperator(task_id="task", bash_command="echo 1")

        dm1 = dag_maker.dag_model
        # Here, the next_dagrun is set to None, which will cause an exception
        dm1.next_dagrun = None
        session.add(dm1)
        session.flush()

        with dag_maker("testdag2", serialized=True):
            BashOperator(task_id="task", bash_command="echo 1")
        dm2 = dag_maker.dag_model

        scheduler_job = Job()
        job_runner = SchedulerJobRunner(job=scheduler_job)
        # In the dagmodel list, the first dag should fail, but the second one should succeed
        job_runner._create_dag_runs([dm1, dm2], session)
        assert "Failed creating DagRun for testdag1" in caplog.text
        assert not DagRun.find(dag_id="testdag1", session=session)
        # Check if the second dagrun was created
        assert DagRun.find(dag_id="testdag2", session=session)

    def test_activate_referenced_assets_with_no_existing_warning(self, session, testing_dag_bundle):
        dag_warnings = session.query(DagWarning).all()
        assert dag_warnings == []

        dag_id1 = "test_asset_dag1"
        asset1_name = "asset1"
        asset_extra = {"foo": "bar"}

        asset1 = Asset(name=asset1_name, uri="s3://bucket/key/1", extra=asset_extra)
        asset1_1 = Asset(name=asset1_name, uri="it's duplicate", extra=asset_extra)
        asset1_2 = Asset(name="it's also a duplicate", uri="s3://bucket/key/1", extra=asset_extra)
        dag1 = DAG(dag_id=dag_id1, start_date=DEFAULT_DATE, schedule=[asset1, asset1_1, asset1_2])
        sync_dag_to_db(dag1, session=session)

        asset_models = session.scalars(select(AssetModel)).all()
        assert len(asset_models) == 3

        SchedulerJobRunner._activate_referenced_assets(asset_models, session=session)
        session.flush()

        dag_warning = session.scalar(
            select(DagWarning).where(
                DagWarning.dag_id == dag_id1, DagWarning.warning_type == "asset conflict"
            )
        )
        assert dag_warning.message == (
            'Cannot activate asset Asset(name="asset1", uri="it\'s duplica'
            'te", group="asset"); name is already associated to \'s3://buck'
            "et/key/1'\nCannot activate asset Asset(name=\"it's also a dup"
            'licate", uri="s3://bucket/key/1", group="asset"); uri is alrea'
            "dy associated to 'asset1'"
        )

    def test_activate_referenced_assets_with_existing_warnings(self, session, testing_dag_bundle):
        dag_ids = [f"test_asset_dag{i}" for i in range(1, 4)]
        asset1_name = "asset1"
        asset_extra = {"foo": "bar"}

        asset1 = Asset(name=asset1_name, uri="s3://bucket/key/1", extra=asset_extra)
        asset1_1 = Asset(name=asset1_name, uri="it's duplicate", extra=asset_extra)
        asset1_2 = Asset(name=asset1_name, uri="it's duplicate 2", extra=asset_extra)
        dag1 = DAG(dag_id=dag_ids[0], start_date=DEFAULT_DATE, schedule=[asset1, asset1_1])
        dag2 = DAG(dag_id=dag_ids[1], start_date=DEFAULT_DATE)
        dag3 = DAG(dag_id=dag_ids[2], start_date=DEFAULT_DATE, schedule=[asset1_2])

        sync_dags_to_db([dag1, dag2, dag3], session=session)
        session.add_all(
            DagWarning(dag_id=dag_id, warning_type="asset conflict", message="will not exist")
            for dag_id in dag_ids
        )
        session.flush()

        asset_models = session.scalars(select(AssetModel)).all()

        SchedulerJobRunner._activate_referenced_assets(asset_models, session=session)
        session.flush()

        dag_warning = session.scalar(
            select(DagWarning).where(
                DagWarning.dag_id == dag_ids[0], DagWarning.warning_type == "asset conflict"
            )
        )
        assert dag_warning.message == (
            'Cannot activate asset Asset(name="asset1", uri="it\'s duplicate", group="asset"); '
            "name is already associated to 's3://bucket/key/1'"
        )

        dag_warning = session.scalar(
            select(DagWarning).where(
                DagWarning.dag_id == dag_ids[1], DagWarning.warning_type == "asset conflict"
            )
        )
        assert dag_warning is None

        dag_warning = session.scalar(
            select(DagWarning).where(
                DagWarning.dag_id == dag_ids[2], DagWarning.warning_type == "asset conflict"
            )
        )
        assert dag_warning.message == (
            'Cannot activate asset Asset(name="asset1", uri="it\'s duplicate 2", group="asset"); '
            "name is already associated to 's3://bucket/key/1'"
        )

    def test_activate_referenced_assets_with_multiple_conflict_asset_in_one_dag(
        self, session, testing_dag_bundle
    ):
        dag_id = "test_asset_dag"
        asset1_name = "asset1"
        asset_extra = {"foo": "bar"}

        schedule = [Asset(name=asset1_name, uri="s3://bucket/key/1", extra=asset_extra)]
        schedule.extend(
            [Asset(name=asset1_name, uri=f"it's duplicate {i}", extra=asset_extra) for i in range(100)]
        )
        dag1 = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule=schedule)
        sync_dag_to_db(dag1, session=session)

        session.add(DagWarning(dag_id=dag_id, warning_type="asset conflict", message="will not exist"))
        session.flush()

        asset_models = session.scalars(select(AssetModel)).all()

        SchedulerJobRunner._activate_referenced_assets(asset_models, session=session)
        session.flush()

        dag_warning = session.scalar(
            select(DagWarning).where(DagWarning.dag_id == dag_id, DagWarning.warning_type == "asset conflict")
        )
        for i in range(100):
            assert f"it's duplicate {i}" in dag_warning.message

    def test_scheduler_passes_context_from_server_on_heartbeat_timeout(self, dag_maker, session):
        """Test that scheduler passes context_from_server when handling heartbeat timeouts."""
        with dag_maker(dag_id="test_dag", session=session):
            EmptyOperator(task_id="test_task")

        dag_run = dag_maker.create_dagrun(run_id="test_run", state=DagRunState.RUNNING)

        mock_executor = MagicMock()
        scheduler_job = Job(executor=mock_executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        # Create a task instance that appears to be running but hasn't heartbeat
        ti = dag_run.get_task_instance(task_id="test_task")
        ti.state = TaskInstanceState.RUNNING
        ti.queued_by_job_id = scheduler_job.id
        # Set last_heartbeat_at to a time that would trigger timeout
        ti.last_heartbeat_at = timezone.utcnow() - timedelta(seconds=600)  # 10 minutes ago
        session.merge(ti)
        session.commit()

        # Run the heartbeat timeout check
        self.job_runner._find_and_purge_task_instances_without_heartbeats()

        # Verify TaskCallbackRequest was created with context_from_server
        mock_executor.send_callback.assert_called_once()
        callback_request = mock_executor.send_callback.call_args[0][0]

        assert isinstance(callback_request, TaskCallbackRequest)
        assert callback_request.context_from_server is not None
        assert callback_request.context_from_server.dag_run.logical_date == dag_run.logical_date
        assert callback_request.context_from_server.max_tries == ti.max_tries

    @pytest.mark.parametrize(
        ("retries", "callback_kind", "expected"),
        [
            (1, "retry", TaskInstanceState.UP_FOR_RETRY),
            (0, "failure", TaskInstanceState.FAILED),
        ],
    )
    def test_external_kill_sets_callback_type_param(
        self, dag_maker, session, retries, callback_kind, expected
    ):
        """External kill should mark callback type based on retry eligibility."""
        with dag_maker(dag_id=f"ext_kill_{callback_kind}", fileloc="/test_path1/"):
            if callback_kind == "retry":
                EmptyOperator(task_id="t1", retries=retries, on_retry_callback=lambda ctx: None)
            else:
                EmptyOperator(task_id="t1", retries=retries, on_failure_callback=lambda ctx: None)
        dr = dag_maker.create_dagrun(state=DagRunState.RUNNING)
        ti = dr.get_task_instance(task_id="t1")

        executor = MockExecutor(do_update=False)
        scheduler_job = Job(executor=executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        ti.state = State.QUEUED
        session.merge(ti)
        session.commit()

        # Executor reports task finished (FAILED) while TI still QUEUED -> external kill path
        executor.event_buffer[ti.key] = State.FAILED, None

        self.job_runner._process_executor_events(executor=executor, session=session)

        scheduler_job.executor.callback_sink.send.assert_called()
        request = scheduler_job.executor.callback_sink.send.call_args[0][0]
        assert isinstance(request, TaskCallbackRequest)
        assert request.task_callback_type == expected

    def test_scheduler_passes_context_from_server_on_task_failure(self, dag_maker, session):
        """Test that scheduler passes context_from_server when handling task failures."""
        with dag_maker(dag_id="test_dag", session=session):
            EmptyOperator(task_id="test_task", on_failure_callback=lambda: print("failure"))

        dag_run = dag_maker.create_dagrun(run_id="test_run", state=DagRunState.RUNNING)

        # Create a task instance that's running
        ti = dag_run.get_task_instance(task_id="test_task")
        ti.state = TaskInstanceState.RUNNING
        session.merge(ti)
        session.commit()

        # Mock the executor to simulate a task failure
        mock_executor = MagicMock(spec=BaseExecutor)
        mock_executor.has_task = mock.MagicMock(return_value=False)
        scheduler_job = Job(executor=mock_executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        # Simulate executor reporting task as failed
        executor_event = {ti.key: (TaskInstanceState.FAILED, None)}
        mock_executor.get_event_buffer.return_value = executor_event

        # Process the executor events
        self.job_runner._process_executor_events(mock_executor, session)

        # Verify TaskCallbackRequest was created with context_from_server
        mock_executor.send_callback.assert_called_once()
        callback_request = mock_executor.send_callback.call_args[0][0]

        assert isinstance(callback_request, TaskCallbackRequest)
        assert callback_request.context_from_server is not None
        assert callback_request.context_from_server.dag_run.logical_date == dag_run.logical_date
        assert callback_request.context_from_server.max_tries == ti.max_tries

    def test_scheduler_passes_context_from_server_on_dag_timeout(self, dag_maker, session):
        """Test that scheduler passes context_from_server when DAG times out."""
        from airflow.callbacks.callback_requests import DagCallbackRequest, DagRunContext

        def on_failure_callback(context):
            print("DAG failed")

        with dag_maker(
            dag_id="test_dag",
            session=session,
            on_failure_callback=on_failure_callback,
            dagrun_timeout=timedelta(seconds=60),  # 1 minute timeout
        ):
            EmptyOperator(task_id="test_task")

        dag_run = dag_maker.create_dagrun(run_id="test_run", state=DagRunState.RUNNING)
        # Set the start time to make it appear timed out
        dag_run.start_date = timezone.utcnow() - timedelta(seconds=120)  # 2 minutes ago
        session.merge(dag_run)
        session.commit()

        mock_executor = MagicMock()
        scheduler_job = Job(executor=mock_executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        callback_req = self.job_runner._schedule_dag_run(dag_run, session)

        assert isinstance(callback_req, DagCallbackRequest)
        assert callback_req.is_failure_callback
        assert callback_req.msg == "timed_out"
        assert callback_req.context_from_server == DagRunContext(
            dag_run=dag_run,
            last_ti=dag_run.get_task_instance(task_id="test_task"),
        )

    @mock.patch("airflow.models.dagrun.get_listener_manager")
    def test_dag_start_notifies_with_started_msg(self, mock_get_listener_manager, dag_maker, session):
        """Test that notify_dagrun_state_changed is called with msg='started' when DAG starts."""
        mock_listener_manager = MagicMock()
        mock_get_listener_manager.return_value = mock_listener_manager

        with dag_maker(dag_id="test_dag_start_notify", session=session):
            EmptyOperator(task_id="test_task")

        # Create a QUEUED dag run that will be started
        dag_run = dag_maker.create_dagrun(run_id="test_run", state=DagRunState.QUEUED)
        session.commit()

        mock_executor = MagicMock()
        scheduler_job = Job(executor=mock_executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        self.job_runner._start_queued_dagruns(session)

        # Verify that the listener hook was called with msg="started"
        mock_listener_manager.hook.on_dag_run_running.assert_called_once()
        call_args = mock_listener_manager.hook.on_dag_run_running.call_args
        assert call_args.kwargs["msg"] == "started"
        assert call_args.kwargs["dag_run"].dag_id == dag_run.dag_id

    @mock.patch("airflow.models.dagrun.get_listener_manager")
    def test_dag_timeout_notifies_with_timed_out_msg(self, mock_get_listener_manager, dag_maker, session):
        """Test that notify_dagrun_state_changed is called with msg='timed_out' when DAG times out."""
        mock_listener_manager = MagicMock()
        mock_get_listener_manager.return_value = mock_listener_manager

        with dag_maker(
            dag_id="test_dag_timeout_notify",
            session=session,
            dagrun_timeout=timedelta(seconds=60),
        ):
            EmptyOperator(task_id="test_task")

        dag_run = dag_maker.create_dagrun(run_id="test_run", state=DagRunState.RUNNING)
        # Set the start time to make it appear timed out
        dag_run.start_date = timezone.utcnow() - timedelta(seconds=120)  # 2 minutes ago
        session.merge(dag_run)
        session.commit()

        mock_executor = MagicMock()
        scheduler_job = Job(executor=mock_executor)
        self.job_runner = SchedulerJobRunner(scheduler_job)

        self.job_runner._schedule_dag_run(dag_run, session)

        # Verify that the listener hook was called with msg="timed_out"
        mock_listener_manager.hook.on_dag_run_failed.assert_called_once()
        call_args = mock_listener_manager.hook.on_dag_run_failed.call_args
        assert call_args.kwargs["msg"] == "timed_out"
        assert call_args.kwargs["dag_run"] == dag_run

    @mock.patch("airflow.models.Deadline.handle_miss")
    def test_process_expired_deadlines(self, mock_handle_miss, session, dag_maker):
        """Verify all expired and unhandled deadlines (and only those) are processed by the scheduler."""
        scheduler_job = Job(executor=MockExecutor())
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)

        past_date = timezone.utcnow() - timedelta(minutes=5)
        future_date = timezone.utcnow() + timedelta(minutes=5)
        callback_path = "classpath.notify"

        # Create a test Dag run for Deadline
        dag_id = "test_deadline_dag"
        with dag_maker(dag_id=dag_id):
            EmptyOperator(task_id="empty")
        dagrun_id = dag_maker.create_dagrun().id

        handled_deadline_async = Deadline(
            deadline_time=past_date,
            callback=AsyncCallback(callback_path),
            dagrun_id=dagrun_id,
            dag_id=dag_id,
        )
        handled_deadline_async.missed = True

        handled_deadline_sync = Deadline(
            deadline_time=past_date,
            callback=SyncCallback(callback_path),
            dagrun_id=dagrun_id,
            dag_id=dag_id,
        )
        handled_deadline_sync.missed = True

        expired_deadline1 = Deadline(
            deadline_time=past_date, callback=AsyncCallback(callback_path), dagrun_id=dagrun_id, dag_id=dag_id
        )
        expired_deadline2 = Deadline(
            deadline_time=past_date, callback=SyncCallback(callback_path), dagrun_id=dagrun_id, dag_id=dag_id
        )
        future_deadline = Deadline(
            deadline_time=future_date,
            callback=AsyncCallback(callback_path),
            dagrun_id=dagrun_id,
            dag_id=dag_id,
        )

        session.add_all(
            [
                expired_deadline1,
                expired_deadline2,
                future_deadline,
                handled_deadline_async,
                handled_deadline_sync,
            ]
        )
        session.flush()

        self.job_runner._execute()

        # Assert that all deadlines which are both expired and unhandled get processed.
        assert mock_handle_miss.call_count == 2

    @mock.patch("airflow.models.Deadline.handle_miss")
    def test_process_expired_deadlines_no_deadlines_found(self, mock_handle_miss, session):
        """Test handling when there are no deadlines to process."""
        scheduler_job = Job(executor=MockExecutor())
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)

        self.job_runner._execute()

        # The handler should not be called, but no exceptions should be raised either.`
        mock_handle_miss.assert_not_called()

    def test_emit_running_dags_metric(self, dag_maker, monkeypatch):
        """Test that the running_dags metric is emitted correctly."""
        with dag_maker("metric_dag") as dag:
            _ = dag
        dag_maker.create_dagrun(run_id="run_1", state=DagRunState.RUNNING, logical_date=timezone.utcnow())
        dag_maker.create_dagrun(
            run_id="run_2", state=DagRunState.RUNNING, logical_date=timezone.utcnow() + timedelta(hours=1)
        )

        recorded: list[tuple[str, int]] = []

        def _fake_gauge(metric: str, value: int, *_, **__):
            recorded.append((metric, value))

        monkeypatch.setattr("airflow.jobs.scheduler_job_runner.Stats.gauge", _fake_gauge, raising=True)

        with conf_vars({("metrics", "statsd_on"): "True"}):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(scheduler_job)
            self.job_runner._emit_running_dags_metric()

        assert recorded == [("scheduler.dagruns.running", 2)]

    # Multi-team scheduling tests
    def test_multi_team_get_team_names_for_dag_ids_success(self, dag_maker, session):
        """Test successful team name resolution for multiple DAG IDs."""
        # Setup test data
        clear_db_teams()
        clear_db_dag_bundles()

        team1 = Team(name="team_a")
        team2 = Team(name="team_b")
        session.add_all([team1, team2])
        session.flush()

        bundle1 = DagBundleModel(name="bundle_a")
        bundle2 = DagBundleModel(name="bundle_b")
        bundle1.teams.append(team1)
        bundle2.teams.append(team2)
        session.add_all([bundle1, bundle2])
        session.flush()

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            EmptyOperator(task_id="task_a")

        with dag_maker(dag_id="dag_b", bundle_name="bundle_b", session=session):
            EmptyOperator(task_id="task_b")

        with dag_maker(dag_id="dag_no_team", session=session):
            EmptyOperator(task_id="task_no_team")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._get_team_names_for_dag_ids(["dag_a", "dag_b", "dag_no_team"], session)

        expected = {"dag_a": "team_a", "dag_b": "team_b", "dag_no_team": None}
        assert result == expected

    def test_multi_team_get_team_names_for_dag_ids_empty_input(self, session):
        """Test that empty input returns empty dict."""
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._get_team_names_for_dag_ids([], session)
        assert result == {}

    @mock.patch("airflow.jobs.scheduler_job_runner.SchedulerJobRunner.log")
    def test_multi_team_get_team_names_for_dag_ids_database_error(self, mock_log, dag_maker, session):
        """Test graceful error handling when team resolution fails. This code should _not_ fail the scheduler."""
        with dag_maker(dag_id="dag_test", session=session):
            EmptyOperator(task_id="task")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Mock session.execute to raise an exception using context manager
        with mock.patch.object(session, "execute", side_effect=Exception("Database error")):
            result = self.job_runner._get_team_names_for_dag_ids(["dag_test"], session)

        # Should return empty dict and log the error
        assert result == {}
        mock_log.exception.assert_called_once()

    def test_multi_team_get_task_team_name_success(self, dag_maker, session):
        """Test successful team name resolution for a single task."""
        clear_db_teams()
        clear_db_dag_bundles()

        team = Team(name="team_a")
        session.add(team)
        session.flush()

        bundle = DagBundleModel(name="bundle_a")
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            task = EmptyOperator(task_id="task_a")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._get_task_team_name(ti, session)
        assert result == "team_a"

    def test_multi_team_get_task_team_name_no_team(self, dag_maker, session):
        """Test team resolution when no team is associated with the DAG."""
        with dag_maker(dag_id="dag_no_team", session=session):
            task = EmptyOperator(task_id="task_no_team")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._get_task_team_name(ti, session)
        assert result is None

    def test_multi_team_get_task_team_name_database_error(self, dag_maker, session):
        """Test graceful error handling when individual task team resolution fails. This code should _not_ fail the scheduler."""
        with dag_maker(dag_id="dag_test", session=session):
            task = EmptyOperator(task_id="task_test")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Mock _get_team_names_for_dag_ids to return empty dict (simulates database error handling in that function)
        with mock.patch.object(self.job_runner, "_get_team_names_for_dag_ids", return_value={}) as mock_batch:
            result = self.job_runner._get_task_team_name(ti, session)
            mock_batch.assert_called_once_with([ti.dag_id], session)

        # Should return None when batch function returns empty dict
        assert result is None

    @conf_vars({("core", "multi_team"): "false"})
    def test_multi_team_try_to_load_executor_multi_team_disabled(self, dag_maker, mock_executors, session):
        """Test executor selection when multi_team is disabled (legacy behavior)."""
        with dag_maker(dag_id="test_dag", session=session):
            task = EmptyOperator(task_id="test_task", executor="secondary_exec")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with mock.patch.object(self.job_runner, "_get_task_team_name") as mock_team_resolve:
            result = self.job_runner._try_to_load_executor(ti, session)
            # Should not call team resolution when multi_team is disabled
            mock_team_resolve.assert_not_called()

        assert result == mock_executors[1]

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_try_to_load_executor_no_explicit_executor_no_team(
        self, dag_maker, mock_executors, session
    ):
        """Test executor selection when no explicit executor and no team (should use global default)."""
        with dag_maker(dag_id="test_dag", session=session):
            task = EmptyOperator(task_id="test_task")  # No explicit executor

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session)

        # Should return the global default executor (first executor in Job)
        assert result == scheduler_job.executor

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_try_to_load_executor_no_explicit_executor_with_team(
        self, dag_maker, mock_executors, session
    ):
        """Test executor selection when no explicit executor but team exists (should find team's default executor)."""
        clear_db_teams()
        clear_db_dag_bundles()

        team = Team(name="team_a")
        session.add(team)
        session.flush()

        bundle = DagBundleModel(name="bundle_a")
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        # Configure one executor to be team-specific
        mock_executors[1].team_name = "team_a"

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            task = EmptyOperator(task_id="test_task")  # No explicit executor

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session)

        # Should return the team-specific default executor set above
        assert result == mock_executors[1]

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_try_to_load_executor_explicit_executor_matches_team(
        self, dag_maker, mock_executors, session
    ):
        """Test executor selection when explicit executor matches task's team."""
        clear_db_teams()
        clear_db_dag_bundles()

        team = Team(name="team_a")
        session.add(team)
        session.flush()

        bundle = DagBundleModel(name="bundle_a")
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        # Configure executor for the team
        mock_executors[1].team_name = "team_a"

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            task = EmptyOperator(task_id="test_task", executor="secondary_exec")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session)

        # Should return the team-specific executor that matches the explicit executor name
        assert result == mock_executors[1]

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_try_to_load_executor_explicit_executor_global_fallback(
        self, dag_maker, mock_executors, session
    ):
        """Test executor selection when explicit executor is global (team_name=None)."""
        clear_db_teams()
        clear_db_dag_bundles()

        team = Team(name="team_a")
        session.add(team)
        session.flush()

        bundle = DagBundleModel(name="bundle_a")
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        # Configure one executor for the team, but keep default as global
        mock_executors[1].team_name = "team_a"

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            task = EmptyOperator(task_id="test_task", executor="default_exec")  # Global executor

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session)

        # Should return the global executor (default) even though task has a team
        assert result == mock_executors[0]

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_try_to_load_executor_explicit_executor_team_mismatch(
        self, dag_maker, mock_executors, session
    ):
        """Test executor selection when explicit executor doesn't match task's team (should return None)."""
        clear_db_teams()
        clear_db_dag_bundles()

        team1 = Team(name="team_a")
        team2 = Team(name="team_b")
        session.add_all([team1, team2])
        session.flush()

        bundle = DagBundleModel(name="bundle_a")
        bundle.teams.append(team1)
        session.add(bundle)
        session.flush()

        # Configure executors for different teams
        mock_executors[1].team_name = "team_b"  # Different team!

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):  # DAG belongs to team_a
            task = EmptyOperator(
                task_id="test_task", executor="secondary_exec"
            )  # Executor for different team

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with mock.patch("airflow.jobs.scheduler_job_runner.SchedulerJobRunner.log") as mock_log:
            result = self.job_runner._try_to_load_executor(ti, session)

            # Should log a warning when no executor is found
            mock_log.warning.assert_called_once_with(
                "Executor, %s, was not found but a Task was configured to use it", "secondary_exec"
            )

        # Should return None since we failed to resolve an executor due to the mismatch. In practice, this
        # should never happen since we assert this at DagBag validation time.
        assert result is None

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_try_to_load_executor_invalid_executor_name(self, dag_maker, mock_executors, session):
        """Test executor selection with invalid executor name (should return None and log warning)."""
        with dag_maker(dag_id="test_dag", session=session):
            task = EmptyOperator(task_id="test_task", executor="nonexistent_executor")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with mock.patch("airflow.jobs.scheduler_job_runner.SchedulerJobRunner.log") as mock_log:
            result = self.job_runner._try_to_load_executor(ti, session)

            assert result is None
            mock_log.warning.assert_called_once()

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_try_to_load_executor_team_name_pre_resolved(self, dag_maker, mock_executors, session):
        """Test executor selection when team_name is pre-resolved."""
        clear_db_teams()
        clear_db_dag_bundles()

        team = Team(name="team_a")
        session.add(team)
        session.flush()

        bundle = DagBundleModel(name="bundle_a")
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        mock_executors[1].team_name = "team_a"

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            task = EmptyOperator(task_id="test_task")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Call with pre-resolved team name (as done in the scheduling loop)
        with mock.patch.object(self.job_runner, "_get_task_team_name") as mock_team_resolve:
            result = self.job_runner._try_to_load_executor(ti, session, team_name="team_a")
            mock_team_resolve.assert_not_called()  # We don't query for the team if it is pre-resolved

        assert result == mock_executors[1]

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_scheduling_loop_batch_optimization(self, dag_maker, mock_executors, session):
        """Test that the scheduling loop uses batch team resolution optimization."""
        clear_db_teams()
        clear_db_dag_bundles()

        team1 = Team(name="team_a")
        team2 = Team(name="team_b")
        session.add_all([team1, team2])
        session.flush()

        bundle1 = DagBundleModel(name="bundle_a")
        bundle2 = DagBundleModel(name="bundle_b")
        bundle1.teams.append(team1)
        bundle2.teams.append(team2)
        session.add_all([bundle1, bundle2])
        session.flush()

        mock_executors[0].team_name = "team_a"
        mock_executors[1].team_name = "team_b"

        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            EmptyOperator(task_id="task_a")
        dr1 = dag_maker.create_dagrun()

        with dag_maker(dag_id="dag_b", bundle_name="bundle_b", session=session):
            EmptyOperator(task_id="task_b")
        dr2 = dag_maker.create_dagrun()

        ti1 = dr1.get_task_instance("task_a", session)
        ti2 = dr2.get_task_instance("task_b", session)
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # The scheduling loop should call batch resolution and pass resolved names
        with mock.patch.object(self.job_runner, "_get_team_names_for_dag_ids") as mock_batch:
            mock_batch.return_value = {"dag_a": "team_a", "dag_b": "team_b"}

            res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

            # Verify batch method was called with unique DAG IDs
            mock_batch.assert_called_once_with({"dag_a", "dag_b"}, session)
            assert len(res) == 2

    @conf_vars({("core", "multi_team"): "false"})
    def test_multi_team_config_disabled_uses_legacy_behavior(self, dag_maker, mock_executors, session):
        """Test that when multi_team config is disabled, legacy behavior is preserved."""
        with dag_maker(dag_id="test_dag", session=session):
            task1 = EmptyOperator(task_id="test_task1")  # No explicit executor
            task2 = EmptyOperator(task_id="test_task2", executor="secondary_exec")

        dr = dag_maker.create_dagrun()
        ti1 = dr.get_task_instance(task1.task_id, session)
        ti2 = dr.get_task_instance(task2.task_id, session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with mock.patch.object(self.job_runner, "_get_task_team_name") as mock_team_resolve:
            result1 = self.job_runner._try_to_load_executor(ti1, session)
            result2 = self.job_runner._try_to_load_executor(ti2, session)

            # Should use legacy logic without calling team resolution
            mock_team_resolve.assert_not_called()
            assert result1 == scheduler_job.executor  # Default for no explicit executor
            assert result2 == mock_executors[1]  # Matched by executor name


@pytest.mark.need_serialized_dag
def test_schedule_dag_run_with_upstream_skip(dag_maker, session):
    """
    Test if _schedule_dag_run puts a task instance into SKIPPED state if any of its
    upstream tasks are skipped according to TriggerRuleDep.
    """
    with dag_maker(
        dag_id="test_task_with_upstream_skip_process_task_instances",
        start_date=DEFAULT_DATE,
        session=session,
    ):
        dummy1 = EmptyOperator(task_id="dummy1")
        dummy2 = EmptyOperator(task_id="dummy2")
        dummy3 = EmptyOperator(task_id="dummy3")
        [dummy1, dummy2] >> dummy3
    # dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
    dr = dag_maker.create_dagrun(state=State.RUNNING)
    assert dr is not None

    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    # Set dummy1 to skipped and dummy2 to success. dummy3 remains as none.
    tis[dummy1.task_id].state = State.SKIPPED
    tis[dummy2.task_id].state = State.SUCCESS
    assert tis[dummy3.task_id].state == State.NONE
    session.flush()

    # dag_runs = DagRun.find(dag_id='test_task_with_upstream_skip_dag')
    # dag_file_processor._process_task_instances(dag, dag_runs=dag_runs)
    scheduler_job = Job()
    job_runner = SchedulerJobRunner(job=scheduler_job)
    job_runner._schedule_dag_run(dr, session)
    session.flush()
    tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
    assert tis[dummy1.task_id].state == State.SKIPPED
    assert tis[dummy2.task_id].state == State.SUCCESS
    # dummy3 should be skipped because dummy1 is skipped.
    assert tis[dummy3.task_id].state == State.SKIPPED

    def test_start_queued_dagruns_uses_latest_max_active_runs_from_dag_model(self, dag_maker, session):
        """
        Test that _start_queued_dagruns uses max_active_runs from DagModel (via dag_run)
        instead of stale SerializedDAG max_active_runs.

        This test verifies the fix where SerializedDAG may have stale max_active_runs,
        but DagModel has the latest value updated by version changes(versioned bundles). The scheduler should
        use the latest value from DagModel to respect user updates.
        """
        # Create a DAG with max_active_runs=1 initially
        with dag_maker(
            dag_id="test_max_active_runs_stale_serialized",
            max_active_runs=1,
            session=session,
        ) as dag:
            EmptyOperator(task_id="dummy_task")

        dag_model = dag_maker.dag_model
        assert dag_model.max_active_runs == 1

        # Create a SerializedDAG (which will have max_active_runs=1)
        # This simulates the SerializedDAG being created/updated from the DAG file
        scheduler_job = Job(executor=self.null_exec)
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        self.job_runner._create_dag_runs([dag_model], session)

        # Verify SerializedDAG has max_active_runs=1
        dag_run_1 = (
            session.query(DagRun).filter(DagRun.dag_id == dag.dag_id).order_by(DagRun.logical_date).first()
        )
        assert dag_run_1 is not None
        serialized_dag = self.job_runner.scheduler_dag_bag.get_dag_for_run(dag_run_1, session=session)
        assert serialized_dag is not None
        assert serialized_dag.max_active_runs == 1

        # Now update DagModel.max_active_runs to 2 (simulating a versioned bundle update)
        # This is the latest value, but SerializedDAG still has the old value
        dag_model.max_active_runs = 2
        session.commit()
        session.refresh(dag_model)

        # Create 1 running dag run
        dag_run_1.state = DagRunState.RUNNING
        session.commit()

        # Create 1 queued dag run
        dag_run_2 = dag_maker.create_dagrun(
            run_id="test_run_2",
            state=DagRunState.QUEUED,
            run_type=DagRunType.SCHEDULED,
            session=session,
        )

        # Ensure dag_run_2 has the updated DagModel relationship loaded
        # The association proxy dag_run.max_active_runs accesses dag_model.max_active_runs
        # so we need to ensure the relationship is loaded
        session.refresh(dag_run_2)

        # Verify we have 1 running and 1 queued
        running_count = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id, DagRun.state == DagRunState.RUNNING)
            .count()
        )
        queued_count = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id, DagRun.state == DagRunState.QUEUED)
            .count()
        )
        assert running_count == 1
        assert queued_count == 1

        # The SerializedDAG still has max_active_runs=1 (stale)
        # But DagModel has max_active_runs=2 (latest)
        assert serialized_dag.max_active_runs == 1
        assert dag_model.max_active_runs == 2

        # Call _start_queued_dagruns
        # With the fix: Should start the queued run (using DagModel max_active_runs=2, active_runs=1 < 2)
        # Without the fix: Would block the queued run (using SerializedDAG max_active_runs=1, active_runs=1 >= 1)
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        # Verify that the queued dag run started (proves it used DagModel.max_active_runs=2)
        dag_run_2 = session.get(DagRun, dag_run_2.id)
        assert dag_run_2.state == DagRunState.RUNNING, (
            "The queued dag run should have started because DagModel.max_active_runs=2 "
            "allows it (active_runs=1 < 2), even though SerializedDAG.max_active_runs=1 for that dagrun serdag version "
            "would have blocked it."
        )

        # Verify we now have 2 running dag runs
        running_count = (
            session.query(DagRun)
            .filter(DagRun.dag_id == dag.dag_id, DagRun.state == DagRunState.RUNNING)
            .count()
        )
        assert running_count == 2


class TestSchedulerJobQueriesCount:
    """
    These tests are designed to detect changes in the number of queries for
    different DAG files. These tests allow easy detection when a change is
    made that affects the performance of the SchedulerJob.
    """

    scheduler_job: Job | None

    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_pools()
        clear_db_backfills()
        clear_db_dags()
        clear_db_dag_bundles()
        clear_db_import_errors()
        clear_db_jobs()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        self.clean_db()

        yield

        self.clean_db()

    @pytest.mark.parametrize(
        ("expected_query_count", "dag_count", "task_count"),
        [
            (21, 1, 1),  # One DAG with one task per DAG file.
            (21, 1, 5),  # One DAG with five tasks per DAG file.
            (148, 10, 10),  # 10 DAGs with 10 tasks per DAG file.
        ],
    )
    def test_execute_queries_count_with_harvested_dags(
        self, expected_query_count, dag_count, task_count, testing_dag_bundle
    ):
        with (
            mock.patch.dict(
                "os.environ",
                {
                    "PERF_DAGS_COUNT": str(dag_count),
                    "PERF_TASKS_COUNT": str(task_count),
                    "PERF_START_AGO": "1d",
                    "PERF_SCHEDULE_INTERVAL": "30m",
                    "PERF_SHAPE": "no_structure",
                },
            ),
            conf_vars(
                {
                    ("scheduler", "use_job_schedule"): "True",
                    ("core", "load_examples"): "False",
                    # For longer running tests under heavy load, the min_serialized_dag_fetch_interval
                    # and min_serialized_dag_update_interval might kick-in and re-retrieve the record.
                    # This will increase the count of serliazied_dag.py.get() count.
                    # That's why we keep the values high
                    ("core", "min_serialized_dag_update_interval"): "100",
                    ("core", "min_serialized_dag_fetch_interval"): "100",
                }
            ),
        ):
            dagruns = []
            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE, include_examples=False)
            sync_bag_to_db(dagbag, "testing", None)

            for i, dag in enumerate(dagbag.dags.values()):
                dr = create_scheduler_dag(dag).create_dagrun(
                    state=State.RUNNING,
                    run_id=f"{DagRunType.MANUAL.value}__{i}",
                    run_after=pendulum.datetime(2025, 1, 1, tz="UTC"),
                    run_type=DagRunType.MANUAL,
                    triggered_by=DagRunTriggeredByType.TEST,
                )
                dagruns.append(dr)
                for ti in dr.get_task_instances():
                    ti.set_state(state=State.SCHEDULED)

            scheduler_job = Job(executor=MockExecutor(do_update=False))
            scheduler_job.heartbeat = mock.MagicMock()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)

            with assert_queries_count(expected_query_count, margin=15):
                with mock.patch.object(
                    DagRun, DagRun.get_running_dag_runs_to_examine.__name__
                ) as mock_dagruns:
                    query = MagicMock()
                    query.all.return_value = dagruns
                    mock_dagruns.return_value = query

                    self.job_runner._run_scheduler_loop()

    @pytest.mark.flaky(reruns=3, reruns_delay=5)
    @pytest.mark.parametrize(
        ("expected_query_counts", "dag_count", "task_count", "start_ago", "schedule", "shape"),
        [
            # One DAG with one task per DAG file.
            ([10, 10, 10, 10], 1, 1, "1d", "None", "no_structure"),
            ([10, 10, 10, 10], 1, 1, "1d", "None", "linear"),
            ([24, 14, 14, 14], 1, 1, "1d", "@once", "no_structure"),
            ([24, 14, 14, 14], 1, 1, "1d", "@once", "linear"),
            ([24, 26, 29, 32], 1, 1, "1d", "30m", "no_structure"),
            ([24, 26, 29, 32], 1, 1, "1d", "30m", "linear"),
            ([24, 26, 29, 32], 1, 1, "1d", "30m", "binary_tree"),
            ([24, 26, 29, 32], 1, 1, "1d", "30m", "star"),
            ([24, 26, 29, 32], 1, 1, "1d", "30m", "grid"),
            # One DAG with five tasks per DAG file.
            ([10, 10, 10, 10], 1, 5, "1d", "None", "no_structure"),
            ([10, 10, 10, 10], 1, 5, "1d", "None", "linear"),
            ([24, 14, 14, 14], 1, 5, "1d", "@once", "no_structure"),
            ([25, 15, 15, 15], 1, 5, "1d", "@once", "linear"),
            ([24, 26, 29, 32], 1, 5, "1d", "30m", "no_structure"),
            ([25, 28, 32, 36], 1, 5, "1d", "30m", "linear"),
            ([25, 28, 32, 36], 1, 5, "1d", "30m", "binary_tree"),
            ([25, 28, 32, 36], 1, 5, "1d", "30m", "star"),
            ([25, 28, 32, 36], 1, 5, "1d", "30m", "grid"),
            # 10 DAGs with 10 tasks per DAG file.
            ([10, 10, 10, 10], 10, 10, "1d", "None", "no_structure"),
            ([10, 10, 10, 10], 10, 10, "1d", "None", "linear"),
            ([218, 69, 69, 69], 10, 10, "1d", "@once", "no_structure"),
            ([228, 84, 84, 84], 10, 10, "1d", "@once", "linear"),
            ([217, 119, 119, 119], 10, 10, "1d", "30m", "no_structure"),
            ([2227, 145, 145, 145], 10, 10, "1d", "30m", "linear"),
            ([227, 139, 139, 139], 10, 10, "1d", "30m", "binary_tree"),
            ([227, 139, 139, 139], 10, 10, "1d", "30m", "star"),
            ([227, 259, 259, 259], 10, 10, "1d", "30m", "grid"),
        ],
    )
    def test_process_dags_queries_count(
        self, expected_query_counts, dag_count, task_count, start_ago, schedule, shape, testing_dag_bundle
    ):
        with (
            mock.patch.dict(
                "os.environ",
                {
                    "PERF_DAGS_COUNT": str(dag_count),
                    "PERF_TASKS_COUNT": str(task_count),
                    "PERF_START_AGO": start_ago,
                    "PERF_SCHEDULE_INTERVAL": schedule,
                    "PERF_SHAPE": shape,
                },
            ),
            conf_vars(
                {
                    ("scheduler", "use_job_schedule"): "True",
                    # For longer running tests under heavy load, the min_serialized_dag_fetch_interval
                    # and min_serialized_dag_update_interval might kick-in and re-retrieve the record.
                    # This will increase the count of serliazied_dag.py.get() count.
                    # That's why we keep the values high
                    ("core", "min_serialized_dag_update_interval"): "100",
                    ("core", "min_serialized_dag_fetch_interval"): "100",
                }
            ),
        ):
            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE, include_examples=False)
            sync_bag_to_db(dagbag, "testing", None)

            scheduler_job = Job(job_type=SchedulerJobRunner.job_type, executor=MockExecutor(do_update=False))
            scheduler_job.heartbeat = mock.MagicMock()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)

            failures = []  # Collects assertion errors and report all of them at the end.
            message = "Expected {expected_count} query, but got {current_count} located at:"
            for expected_query_count in expected_query_counts:
                with create_session() as session:
                    try:
                        with assert_queries_count(expected_query_count, message_fmt=message, margin=15):
                            self.job_runner._do_scheduling(session)
                    except AssertionError as e:
                        failures.append(str(e))
            if failures:
                prefix = "Collected database query count mismatches:"
                joined = "\n\n".join(failures)
                raise AssertionError(f"{prefix}\n\n{joined}")


def test_mark_backfills_completed(dag_maker, session):
    clear_db_backfills()
    with dag_maker(serialized=True, dag_id="test_mark_backfills_completed", schedule="@daily") as dag:
        BashOperator(task_id="hi", bash_command="echo hi")
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        reverse=False,
        triggering_user_name="test_user",
        dag_run_conf={},
    )
    session.expunge_all()
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type, executor=MockExecutor(do_update=False))
    )
    runner._mark_backfills_complete()
    b = session.get(Backfill, b.id)
    assert b.completed_at is None
    session.expunge_all()
    drs = session.scalars(select(DagRun).where(DagRun.dag_id == dag.dag_id))
    for dr in drs:
        dr.state = DagRunState.SUCCESS
    session.commit()
    runner._mark_backfills_complete()
    b = session.get(Backfill, b.id)
    assert b.completed_at.timestamp() > 0
