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
import re
from collections import Counter, deque
from collections.abc import Callable, Generator, Iterator
from contextlib import ExitStack, contextmanager
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from unittest import mock
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pendulum
import psutil
import pytest
import time_machine
from sqlalchemy import delete, func, inspect, select, update
from sqlalchemy.dialects import mysql
from sqlalchemy.orm import joinedload

from airflow import settings
from airflow._shared.module_loading import qualname
from airflow._shared.observability.metrics.base_stats_logger import StatsLogger
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
from airflow.executors.local_executor import LocalExecutor
from airflow.jobs.job import Job, run_job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.asset import (
    AssetActive,
    AssetAliasModel,
    AssetDagRunQueue,
    AssetEvent,
    AssetModel,
    AssetPartitionDagRun,
    DagScheduleAssetReference,
    PartitionedAssetKeyLog,
)
from airflow.models.backfill import Backfill, BackfillDagRun, ReprocessBehavior, _create_backfill
from airflow.models.callback import Callback, ExecutorCallback
from airflow.models.connection_test import (
    ConnectionTestKey,
    ConnectionTestRequest,
    ConnectionTestState,
)
from airflow.models.dag import DagModel, get_last_dagrun, infer_automated_data_interval
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagrun import DagRun
from airflow.models.dagwarning import DagWarning
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.deadline import Deadline
from airflow.models.deadline_alert import DeadlineAlert
from airflow.models.hitl import HITLDetail
from airflow.models.log import Log
from airflow.models.pool import Pool
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.models.team import Team
from airflow.models.trigger import Trigger
from airflow.partition_mappers.base import (
    PartitionMapper as CorePartitionMapper,
    RollupMapper as CoreRollupMapper,
)
from airflow.partition_mappers.identity import IdentityMapper as CoreIdentityMapper
from airflow.partition_mappers.temporal import (
    FanOutMapper as CoreFanOutMapper,
    StartOfDayMapper as CoreStartOfDayMapper,
    StartOfHourMapper as CoreStartOfHourMapper,
    StartOfWeekMapper as CoreStartOfWeekMapper,
)
from airflow.partition_mappers.wait_policy import WaitPolicy
from airflow.partition_mappers.window import (
    DayWindow as CoreDayWindow,
    HourWindow as CoreHourWindow,
    WeekWindow as CoreWeekWindow,
)
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.triggers.file import FileDeleteTrigger
from airflow.sdk import (
    DAG,
    Asset,
    AssetAlias,
    AssetWatcher,
    CronPartitionTimetable,
    FixedKeyMapper,
    HourWindow,
    IdentityMapper,
    MinimumCount,
    RollupMapper,
    SegmentWindow,
    StartOfDayMapper,
    StartOfHourMapper,
    task,
)
from airflow.sdk.definitions.callback import AsyncCallback, SyncCallback
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.serialization.encoders import ensure_serialized_asset
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.timetables.base import DagRunInfo, DataInterval, compute_rollup_fingerprint
from airflow.timetables.simple import (
    PartitionedAssetTimetable as CorePartitionedAssetTimetable,
    PartitionedAtRuntime,
)
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import CallbackState, DagRunState, State, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.pytest_plugin import AIRFLOW_ROOT_PATH
from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.config import conf_vars, env_vars
from tests_common.test_utils.dag import create_scheduler_dag, sync_dag_to_db, sync_dags_to_db
from tests_common.test_utils.db import (
    clear_db_apdr,
    clear_db_assets,
    clear_db_backfills,
    clear_db_callbacks,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_deadline,
    clear_db_import_errors,
    clear_db_jobs,
    clear_db_pakl,
    clear_db_pools,
    clear_db_runs,
    clear_db_teams,
    clear_db_triggers,
    set_default_pool_slots,
)
from tests_common.test_utils.mock_executor import MockExecutor
from tests_common.test_utils.mock_operators import CustomOperator
from tests_common.test_utils.taskinstance import create_task_instance, run_task_instance
from unit.listeners import dag_listener
from unit.models import TEST_DAGS_FOLDER

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from tests_common.pytest_plugin import DagMaker

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


def task_maker(
    dag_maker,
    session,
    dag_id: str,
    task_num: int,
    max_active_tasks: int,
    running_num: int,
    run_id: str | None = None,
):
    dag_tasks = {}

    with dag_maker(dag_id=dag_id):
        for i in range(task_num):
            # Assign priority weight to certain tasks.
            if (i % 10) == 0:  # 0, 10, 20, 30, 40, 50, ...
                weight = int(i / 2)
                dag_tasks[f"op{i}"] = EmptyOperator(
                    task_id=f"dummy{i}", priority_weight=weight, pool="test_pool1"
                )
            else:
                # No executor specified, runs on default executor
                dag_tasks[f"op{i}"] = EmptyOperator(task_id=f"dummy{i}", pool="test_pool2")

    # 'create_dagrun' uses a kwargs dict to check whether parameters
    # are present. Do the same for simplicity.
    # 'logical_date' is used to create the 'run_id'. Set it to 'now',
    # in order to get distinct run ids, if a value isn't provided.
    kwargs = {
        "run_type": DagRunType.SCHEDULED,
        "logical_date": timezone.utcnow(),
    }

    if run_id is not None:
        kwargs["run_id"] = run_id

    dag_run = dag_maker.create_dagrun(**kwargs)

    tis_list = []
    for i in range(task_num):
        ti = dag_run.get_task_instance(dag_tasks[f"op{i}"].task_id, session=session)
        # e.g.
        #  If running_num is 2, then for i=0 and i=1, state will be RUNNING.
        #  If running_num is 0, then state will be SCHEDULED for all.
        ti.state = State.RUNNING if i < running_num else State.SCHEDULED
        ti.dag_model.max_active_tasks = max_active_tasks
        # Add to the list.
        tis_list.append(ti)

    session.flush()

    return tis_list


def _clean_db():
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


@patch.dict(
    ExecutorLoader.executors, {MOCK_EXECUTOR: f"{MockExecutor.__module__}.{MockExecutor.__qualname__}"}
)
@pytest.mark.usefixtures("disable_load_example")
@pytest.mark.need_serialized_dag
class TestSchedulerJob:
    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        _clean_db()
        self.job_runner: SchedulerJobRunner | None = None

        yield

        _clean_db()

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

        with mock.patch("airflow.executors.executor_loader.ExecutorLoader.init_executors") as executors_mock:
            executors_mock.return_value = [default_executor, second_executor]
            yield [default_executor, second_executor]

    @pytest.fixture
    def mock_executor(self, mock_executors):
        default_executor = mock_executors[0]
        with mock.patch("airflow.executors.executor_loader.ExecutorLoader.init_executors") as loader_mock:
            loader_mock.return_value = mock_executors
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

    @patch("airflow.jobs.scheduler_job_runner.ExecutorLoader.get_default_executor")
    @patch("airflow.jobs.scheduler_job_runner.ExecutorLoader.init_executors")
    def test_executor_loaded_in_scheduler_job(self, mock_init_executors, mock_default_executor):
        mock_local_executor = LocalExecutor()
        mock_default_executor.return_value = mock_local_executor
        mock_init_executors.return_value = [mock_local_executor]

        scheduler_job = SchedulerJobRunner(Job())

        assert scheduler_job.executor == mock_local_executor
        assert scheduler_job.executors == [mock_local_executor]

    @pytest.mark.parametrize(
        "heartrate",
        [10, 5],
    )
    def test_heartrate(self, heartrate):
        with conf_vars({("scheduler", "scheduler_heartbeat_sec"): str(heartrate)}):
            scheduler_job = Job()
            _ = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])
            assert scheduler_job.heartrate == heartrate

    def test_no_orphan_process_will_be_left(self):
        current_process = psutil.Process()
        old_children = current_process.children(recursive=True)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(
            job=scheduler_job, num_runs=1, executors=[MockExecutor(do_update=False)]
        )
        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        # Remove potential noise created by previous tests.
        current_children = set(current_process.children(recursive=True)) - set(old_children)
        assert not current_children

    def test_only_idle_no_dags_exits_after_n_idle_runs(self, caplog, configure_testing_dag_bundle):
        num_runs = 5
        with caplog.at_level(logging.INFO, logger="airflow.jobs.scheduler_job_runner"):
            with configure_testing_dag_bundle(os.devnull):
                executor = MockExecutor(do_update=False)
                scheduler_job = Job()
                self.job_runner = SchedulerJobRunner(
                    job=scheduler_job,
                    num_runs=num_runs,
                    only_idle=True,
                    executors=[executor],
                )
                run_job(scheduler_job, execute_callable=self.job_runner._execute)

        match = re.search(r"\((\d+) idle, (\d+) total\)", caplog.text)
        assert match, f"Expected exit log '(N idle, M total)' in: {caplog.text}"
        idle_runs_val, total_runs_val = int(match.group(1)), int(match.group(2))
        assert total_runs_val == num_runs, "With no DAGs, total loop count should equal num_runs"
        assert idle_runs_val == num_runs, "With no DAGs, all runs are idle; idle count should equal num_runs"

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_only_idle_with_dag_exits_after_n_idle_runs(self, caplog, dag_maker, session):
        num_runs = 5
        with dag_maker(dag_id="test_only_idle_one_task", fileloc="test_only_idle_one_task.py"):
            EmptyOperator(task_id="dummy")
        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, state=State.RUNNING)
        ti = dr.get_task_instance("dummy", session=session)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        executor = MockExecutor(do_update=False)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(
            job=scheduler_job,
            num_runs=num_runs,
            only_idle=True,
            executors=[executor],
        )
        with caplog.at_level(logging.INFO, logger="airflow.jobs.scheduler_job_runner"):
            run_job(scheduler_job, execute_callable=self.job_runner._execute)

        match = re.search(r"\((\d+) idle, (\d+) total\)", caplog.text)
        assert match, f"Expected exit log '(N idle, M total)' in: {caplog.text}"
        idle_runs_val, total_runs_val = int(match.group(1)), int(match.group(2))
        assert total_runs_val >= num_runs, "Total loop count should be at least num_runs"
        assert idle_runs_val == num_runs, "Scheduler exits when idle run count reaches num_runs"
        assert total_runs_val > idle_runs_val, "Some runs should not be idle"

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_process_executor_events(self, mock_get_backend, mock_task_callback, dag_maker):
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        dag_id = "test_process_executor_events"
        task_id_1 = "dummy_task"

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id_1)
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        mock_stats.incr.reset_mock()

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock(spec=TaskCallbackRequest)
        mock_task_callback.return_value = task_callback
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[executor])
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.FAILED
        self.job_runner.executor.callback_sink.send.assert_not_called()

        # ti in success state
        ti1.state = State.SUCCESS
        session.merge(ti1)
        session.commit()
        executor.event_buffer[ti1.key] = State.SUCCESS, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.SUCCESS
        self.job_runner.executor.callback_sink.send.assert_not_called()
        mock_stats.incr.assert_has_calls(
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
    def test_process_executor_events_restarting_cleared_task(self, mock_task_callback, dag_maker):
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
        scheduler_job = Job()
        job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

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
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_process_executor_events_with_no_callback(self, mock_get_backend, mock_task_callback, dag_maker):
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        dag_id = "test_process_executor_events_with_no_callback"
        task_id = "test_task"
        run_id = "test_run"

        mock_stats.incr.reset_mock()
        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock(spec=TaskCallbackRequest)
        mock_task_callback.return_value = task_callback
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id, retries=1)
        ti1 = dag_maker.create_dagrun(
            run_id=run_id, logical_date=DEFAULT_DATE + timedelta(hours=1)
        ).get_task_instance(task1.task_id)

        mock_stats.incr.reset_mock()

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.UP_FOR_RETRY
        self.job_runner.executor.callback_sink.send.assert_not_called()

        # ti in success state
        ti1.state = State.SUCCESS
        session.merge(ti1)
        session.commit()
        executor.event_buffer[ti1.key] = State.SUCCESS, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.SUCCESS
        self.job_runner.executor.callback_sink.send.assert_not_called()
        mock_stats.incr.assert_has_calls(
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

    def test_enqueue_executor_callbacks_only_selects_pending_state(self, dag_maker, session):
        def test_callback():
            pass

        def create_callback_in_state(state: CallbackState):
            callback = Deadline(
                deadline_time=timezone.utcnow(),
                callback=SyncCallback(test_callback),
                dagrun_id=dag_run.id,
                deadline_alert_id=None,
            ).callback
            callback.state = state
            callback.data["dag_run_id"] = dag_run.id
            callback.data["dag_id"] = dag_run.dag_id
            return callback

        with dag_maker(dag_id="test_callback_states"):
            pass
        dag_run = dag_maker.create_dagrun()

        scheduled_callback = create_callback_in_state(CallbackState.SCHEDULED)
        pending_callback = create_callback_in_state(CallbackState.PENDING)
        queued_callback = create_callback_in_state(CallbackState.QUEUED)
        running_callback = create_callback_in_state(CallbackState.RUNNING)
        session.add_all([scheduled_callback, pending_callback, queued_callback, running_callback])
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Verify initial state before calling _enqueue_executor_callbacks
        assert session.get(ExecutorCallback, pending_callback.id).state == CallbackState.PENDING

        self.job_runner._enqueue_executor_callbacks(session)
        # PENDING should progress to QUEUED after _enqueue_executor_callbacks
        assert session.get(ExecutorCallback, pending_callback.id).state == CallbackState.QUEUED

        # Other callbacks should remain in their original states
        assert session.get(ExecutorCallback, scheduled_callback.id).state == CallbackState.SCHEDULED
        assert session.get(ExecutorCallback, queued_callback.id).state == CallbackState.QUEUED
        assert session.get(ExecutorCallback, running_callback.id).state == CallbackState.RUNNING

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_process_executor_events_with_callback(
        self, mock_get_backend, mock_task_callback, dag_maker, session
    ):
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        dag_id = "test_process_executor_events_with_callback"
        task_id_1 = "dummy_task"

        with dag_maker(dag_id=dag_id, fileloc="/test_path1/") as dag:
            EmptyOperator(task_id=task_id_1, on_failure_callback=lambda x: print("hi"))
        dr = dag_maker.create_dagrun()
        ti1 = dr.task_instances[0]

        mock_stats.incr.reset_mock()

        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        executor = MockExecutor(do_update=False)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

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
            version_data=None,
            msg=f"Executor {executor} reported that the task instance "
            f"<TaskInstance: test_process_executor_events_with_callback.dummy_task test [queued] ti_id={ti1.id}> "
            "finished with state failed, but the task instance's state attribute is queued. "
            "Learn more: https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html#task-state-changed-externally",
            context_from_server=mock.ANY,
            task_callback_type=TaskInstanceState.FAILED,
        )
        self.job_runner.executor.callback_sink.send.assert_called_once_with(task_callback)
        self.job_runner.executor.callback_sink.reset_mock()
        mock_stats.incr.assert_any_call(
            "scheduler.tasks.killed_externally",
            tags={
                "dag_id": "test_process_executor_events_with_callback",
                "task_id": "dummy_task",
            },
        )

    def test_process_executor_events_drains_connection_test_events(self, dag_maker, session):
        """Connection-test events in the event_buffer are drained without being treated as callbacks."""
        executor = MockExecutor(do_update=False)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        ct_key = ConnectionTestKey(id=str(uuid4()))
        executor.event_buffer[ct_key] = (ConnectionTestState.SUCCESS, None)

        with mock.patch.object(session, "get", wraps=session.get) as spy_get:
            self.job_runner._process_executor_events(executor=executor, session=session)
            callback_lookups = [c for c in spy_get.call_args_list if c.args and c.args[0] is Callback]
            assert callback_lookups == []

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_process_executor_event_missing_dag(
        self, mock_get_backend, mock_task_callback, dag_maker, caplog
    ):
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        dag_id = "test_process_executor_events_with_callback"
        task_id_1 = "dummy_task"

        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id_1, on_failure_callback=lambda x: print("hi"))
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        mock_stats.incr.reset_mock()

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])
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
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_process_executor_events_ti_requeued(
        self, mock_get_backend, mock_task_callback, dag_maker, caplog
    ):
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        dag_id = "test_process_executor_events_ti_requeued"
        task_id_1 = "dummy_task"

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id_1)
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        mock_stats.incr.reset_mock()

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        scheduler_job = Job()
        session.add(scheduler_job)
        session.flush()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        # ti is queued with another try number - do not fail it
        ti1.state = State.QUEUED
        ti1.queued_by_job_id = scheduler_job.id
        ti1.try_number = 2
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key.with_try_number(1)] = State.SUCCESS, None

        with caplog.at_level(logging.WARNING, logger="airflow.jobs.scheduler_job_runner"):
            self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.QUEUED
        self.job_runner.executor.callback_sink.send.assert_not_called()
        assert any("TI try_number mismatch:" in rec.message for rec in caplog.records)

        # ti is queued by another scheduler - do not fail it
        ti1.state = State.QUEUED
        ti1.queued_by_job_id = scheduler_job.id - 1
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.SUCCESS, None

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.QUEUED
        self.job_runner.executor.callback_sink.send.assert_not_called()

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
        self.job_runner.executor.callback_sink.send.assert_not_called()
        # Only the processed-events counter should have fired across all three sub-tests;
        # no killed_externally mismatch metric should appear.
        assert all(c.args[0] == "scheduler.executor_events.processed" for c in mock_stats.incr.call_args_list)

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_process_executor_events_stale_success_when_scheduled_after_defer(
        self, mock_get_backend, mock_task_callback, dag_maker
    ):
        """
        Trigger moved TI to scheduled (resume after defer) before executor success from defer exit arrived.

        Regression for https://github.com/apache/airflow/issues/66374 — must not treat as state mismatch.
        """
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        dag_id = "test_process_executor_events_stale_success_scheduled_after_defer"
        task_id_1 = "dummy_task"

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id_1)
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        scheduler_job = Job()
        session.add(scheduler_job)
        session.flush()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        ti1.state = State.SCHEDULED
        ti1.next_method = "execute_callback"
        ti1.queued_by_job_id = scheduler_job.id
        ti1.try_number = 1
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.SUCCESS, None
        executor.has_task = mock.MagicMock(return_value=False)
        mock_stats.incr.reset_mock()

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.SCHEDULED
        self.job_runner.executor.callback_sink.send.assert_not_called()
        # Stale success from defer exit must not trigger a mismatch metric —
        # only the standard processed-events counter should fire.
        mock_stats.incr.assert_called_once_with("scheduler.executor_events.processed", count=1)

        # Without next_method, scheduled + stale success is still a mismatch (e.g. external kill).
        ti1.next_method = None
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.SUCCESS, None
        mock_stats.incr.reset_mock()

        self.job_runner._process_executor_events(executor=executor, session=session)
        mock_stats.incr.assert_any_call(
            "scheduler.tasks.killed_externally",
            tags={"dag_id": dag_id, "task_id": ti1.task_id},
        )

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_process_executor_events_stale_success_when_queued_after_defer(
        self, mock_get_backend, mock_task_callback, dag_maker
    ):
        """
        Trigger moved TI to queued (resume after defer) before executor success from defer exit arrived.

        Regression for https://github.com/apache/airflow/issues/67287 — must not treat as state mismatch.
        The fix for #66374 (#66431) covered the scheduled-state variant; this covers the queued-state variant.
        """
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        dag_id = "test_process_executor_events_stale_success_queued_after_defer"
        task_id_1 = "dummy_task"

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task1 = EmptyOperator(task_id=task_id_1)
        ti1 = dag_maker.create_dagrun().get_task_instance(task1.task_id)

        executor = MockExecutor(do_update=False)
        task_callback = mock.MagicMock()
        mock_task_callback.return_value = task_callback
        scheduler_job = Job()
        session.add(scheduler_job)
        session.flush()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        ti1.state = State.QUEUED
        ti1.next_method = "execute_callback"
        ti1.queued_by_job_id = scheduler_job.id
        ti1.try_number = 1
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.SUCCESS, None
        executor.has_task = mock.MagicMock(return_value=False)
        mock_stats.incr.reset_mock()

        self.job_runner._process_executor_events(executor=executor, session=session)
        ti1.refresh_from_db(session=session)
        assert ti1.state == State.QUEUED
        self.job_runner.executor.callback_sink.send.assert_not_called()
        # Stale success from defer exit must not trigger a mismatch metric —
        # only the standard processed-events counter should fire.
        mock_stats.incr.assert_called_once_with("scheduler.executor_events.processed", count=1)

        # Without next_method, queued + stale success is still a mismatch (e.g. external kill).
        ti1.next_method = None
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.SUCCESS, None
        mock_stats.incr.reset_mock()

        self.job_runner._process_executor_events(executor=executor, session=session)
        mock_stats.incr.assert_any_call(
            "scheduler.tasks.killed_externally",
            tags={"dag_id": dag_id, "task_id": ti1.task_id},
        )

    @mock.patch("airflow.jobs.scheduler_job_runner.TaskCallbackRequest")
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_process_executor_events_multiple_try_numbers_warns(
        self, mock_get_backend, mock_task_callback, dag_maker, caplog
    ):
        dag_id = "test_process_executor_events_multiple_try_numbers_warns"
        task_id = "dummy_task"

        session = settings.Session()
        with dag_maker(dag_id=dag_id, fileloc="/test_path1/"):
            task = EmptyOperator(task_id=task_id)
        ti = dag_maker.create_dagrun().get_task_instance(task.task_id)

        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        executor = MockExecutor(do_update=False)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])
        mock_stats.incr.reset_mock()

        ti.state = State.QUEUED
        ti.try_number = 2
        session.merge(ti)
        session.commit()

        executor.event_buffer[ti.key.with_try_number(1)] = State.RUNNING, "first_executor_id"
        executor.event_buffer[ti.key.with_try_number(2)] = State.RUNNING, "second_executor_id"

        with caplog.at_level(logging.WARNING, logger="airflow.jobs.scheduler_job_runner"):
            self.job_runner._process_executor_events(executor=executor, session=session)

        assert any(
            "Multiple executor events for same TI with different try_numbers!" in rec.message
            for rec in caplog.records
        )
        mock_task_callback.assert_not_called()
        # Only the processed-events counter should fire; duplicate try_number events
        # must not trigger any error/mismatch metrics.
        mock_stats.incr.assert_called_once_with("scheduler.executor_events.processed", count=2)

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_process_executor_events_with_asset_events(self, session, dag_maker):
        """
        Test that _process_executor_events handles asset events without DetachedInstanceError.

        Regression test for scheduler crashes when task callbacks are built with
        consumed_asset_events that weren't eager-loaded. Exercises both
        ``AssetEvent.asset`` and ``AssetEvent.source_aliases`` so that missing
        either loader option on the TI query surfaces as a ``DetachedInstanceError``
        when the callback's ``DRDataModel`` is built.
        """
        asset1 = Asset(uri="test://asset1", name="test_asset_executor", group="test_group")
        asset_model = AssetModel(name=asset1.name, uri=asset1.uri, group=asset1.group)
        asset_alias = AssetAliasModel(name="test_alias_executor", group="test_group")
        session.add_all([asset_model, asset_alias])
        session.flush()

        with dag_maker(dag_id="test_executor_events_with_assets", schedule=[asset1], fileloc="/test_path1/"):
            EmptyOperator(task_id="dummy_task", on_failure_callback=lambda ctx: None)

        dag = dag_maker.dag
        sync_dag_to_db(dag)
        DagVersion.get_latest_version(dag.dag_id)

        dr = dag_maker.create_dagrun()

        # Create asset event with an attached source alias so the lazy-loaded
        # AssetEvent.source_aliases relationship is non-empty and must be
        # eager-loaded to survive a detached ORM instance in the callback.
        asset_event = AssetEvent(
            asset_id=asset_model.id,
            source_task_id="upstream_task",
            source_dag_id="upstream_dag",
            source_run_id="upstream_run",
            source_map_index=-1,
        )
        asset_event.source_aliases.append(asset_alias)
        session.add(asset_event)
        session.flush()
        dr.consumed_asset_events.append(asset_event)
        session.add(dr)
        session.flush()

        executor = MockExecutor(do_update=False)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        ti1 = dr.get_task_instance("dummy_task")
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        # This should not raise DetachedInstanceError
        self.job_runner._process_executor_events(executor=executor, session=session)

        ti1.refresh_from_db(session=session)
        assert ti1.state == State.FAILED

        # Verify callback was created with asset event data including aliases
        self.job_runner.executor.callback_sink.send.assert_called_once()
        callback_request = self.job_runner.executor.callback_sink.send.call_args.args[0]
        assert callback_request.context_from_server is not None
        events = callback_request.context_from_server.dag_run.consumed_asset_events
        assert len(events) == 1
        assert events[0].asset.uri == asset1.uri
        assert [alias.name for alias in events[0].source_aliases] == [asset_alias.name]

    @pytest.mark.usefixtures("testing_dag_bundle")
    def test_schedule_dag_run_with_asset_event(self, session: Session, dag_maker: DagMaker):
        """
        Verify that scheduler can build DagRunContext for a timed-out Dag run
        with consumed asset events without raising DetachedInstanceError.
        """
        asset1 = Asset(uri="test://asset1", name="test_asset_executor", group="test_group")
        asset_model = AssetModel(name=asset1.name, uri=asset1.uri, group=asset1.group)
        session.add(asset_model)
        session.flush()

        with dag_maker(
            dag_id="test_executor_events_with_assets",
            schedule=[asset1],
            fileloc="/test_path1/",
            dagrun_timeout=timedelta(minutes=1),
        ):
            EmptyOperator(task_id="dummy_task")

        dag = dag_maker.dag
        sync_dag_to_db(dag)
        DagVersion.get_latest_version(dag.dag_id)

        # Create Dag run that is guaranteed to time out
        dr = dag_maker.create_dagrun(
            start_date=timezone.utcnow() - timedelta(days=1),
            state=DagRunState.RUNNING,
        )

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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        ti1 = dr.get_task_instance("dummy_task")
        if TYPE_CHECKING:
            assert isinstance(ti1, TaskInstance)
        ti1.state = State.FAILED
        session.merge(ti1)
        session.commit()

        executor.event_buffer[ti1.key] = State.FAILED, None

        callback = self.job_runner._schedule_dag_run(dr, session)
        session.flush()

        assert callback is not None
        assert callback.is_failure_callback
        assert callback.msg == "timed_out"

        context = callback.context_from_server
        assert context is not None

        if TYPE_CHECKING:
            assert isinstance(context.dag_run, DagRun)
        events = context.dag_run.consumed_asset_events
        assert len(events) == 1
        assert events[0].asset is not None
        assert events[0].source_aliases is not None

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
        scheduler_job = Job()
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[self.null_exec])
        session = settings.Session()

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.BACKFILL_JOB)
        dag_version = DagVersion.get_latest_version(dr1.dag_id)

        ti1 = create_task_instance(task1, run_id=dr1.run_id, dag_version_id=dag_version.id)
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

        assert isinstance(self.job_runner.executor.callback_sink, DatabaseCallbackSink)

    def test_setup_callback_sink_standalone_dag_processor_multiple_executors(self, mock_executors):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._execute()

        for executor in self.job_runner.executors:
            assert isinstance(executor.callback_sink, DatabaseCallbackSink)

    def test_executor_start_called(self, mock_executors):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._execute()

        self.job_runner.executor.start.assert_called_once()
        for executor in self.job_runner.executors:
            executor.start.assert_called_once()

    def test_executor_job_id_assigned(self, mock_executors, configure_testing_dag_bundle):
        with configure_testing_dag_bundle(os.devnull):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            self.job_runner._execute()

            assert self.job_runner.executor.job_id == scheduler_job.id
            for executor in self.job_runner.executors:
                assert executor.job_id == scheduler_job.id

    def test_executor_heartbeat(self, mock_executors, configure_testing_dag_bundle):
        with configure_testing_dag_bundle(os.devnull):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            self.job_runner._execute()

            for executor in self.job_runner.executors:
                executor.heartbeat.assert_called_once()

    def test_executor_heartbeat_emits_timer(self, mock_executors, configure_testing_dag_bundle):
        with configure_testing_dag_bundle(os.devnull):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            with patch("airflow.jobs.scheduler_job_runner.stats.timer") as mock_timer:
                self.job_runner._execute()

            heartbeat_calls = [
                timer_call
                for timer_call in mock_timer.call_args_list
                if timer_call.args and timer_call.args[0] == "scheduler.executor_heartbeat_duration"
            ]
            assert len(heartbeat_calls) == len(self.job_runner.executors)
            for executor, timer_call in zip(self.job_runner.executors, heartbeat_calls):
                assert timer_call.kwargs.get("tags") == {"executor": type(executor).__name__}

    def test_executor_events_processed(self, mock_executors, configure_testing_dag_bundle):
        with configure_testing_dag_bundle(os.devnull):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
            self.job_runner._execute()

            for executor in self.job_runner.executors:
                executor.get_event_buffer.assert_called_once()

    @patch("traceback.extract_stack")
    def test_executor_debug_dump(self, patch_traceback_extract_stack, mock_executors):
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._debug_dump(1, mock.MagicMock())

        for executor in self.job_runner.executors:
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

    def test_find_executable_task_instances_mysql_hint_only_applies_to_inner_query(self, dag_maker, session):
        dag_id = "SchedulerJobTest.test_find_executable_task_instances_mysql_hint_only_applies_to_inner_query"
        task_id = "dummy"
        with dag_maker(dag_id=dag_id, max_active_tasks=16):
            task = EmptyOperator(task_id=task_id)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dag_run = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        ti = dag_run.get_task_instance(task.task_id)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.flush()

        captured_queries = []

        def capture_locked_query(query, **kwargs):
            captured_queries.append(query)
            return query

        with mock.patch("airflow.jobs.scheduler_job_runner.with_row_locks", side_effect=capture_locked_query):
            queued_tis = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)

        assert {queued_ti.key for queued_ti in queued_tis} == {ti.key}
        compiled_query = str(captured_queries[0].compile(dialect=mysql.dialect()))
        assert compiled_query.count("USE INDEX (ti_state)") == 1

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

    @conf_vars({("core", "multi_team"): "true"})
    def test_find_executable_task_instances_pool_team_enforcement(self, dag_maker, session):
        """Tasks using a pool owned by another team are not scheduled."""
        clear_db_teams()
        clear_db_dag_bundles()

        team_a = Team(name="team_a")
        team_b = Team(name="team_b")
        session.add_all([team_a, team_b])
        session.flush()

        bundle_a = DagBundleModel(name="bundle_a")
        bundle_a.teams.append(team_a)
        bundle_b = DagBundleModel(name="bundle_b")
        bundle_b.teams.append(team_b)
        session.add_all([bundle_a, bundle_b])
        session.flush()

        # Pool owned by team_a
        pool_a = Pool(pool="pool_a", slots=10, include_deferred=False, team_name="team_a")
        # Shared pool (no team)
        pool_shared = Pool(pool="pool_shared", slots=10, include_deferred=False)
        session.add_all([pool_a, pool_shared])
        session.flush()

        # DAG in team_a using pool_a (allowed)
        with dag_maker(dag_id="dag_a", bundle_name="bundle_a", session=session):
            EmptyOperator(task_id="task_a", pool="pool_a")
        dr_a = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        ti_a = dr_a.get_task_instance("task_a", session=session)
        ti_a.state = State.SCHEDULED
        session.merge(ti_a)

        # DAG in team_b using pool_a (should be blocked)
        with dag_maker(dag_id="dag_b_cross", bundle_name="bundle_b", session=session):
            EmptyOperator(task_id="task_cross", pool="pool_a")
        dr_b = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        ti_b = dr_b.get_task_instance("task_cross", session=session)
        ti_b.state = State.SCHEDULED
        session.merge(ti_b)

        # DAG in team_b using shared pool (allowed)
        with dag_maker(dag_id="dag_b_shared", bundle_name="bundle_b", session=session):
            EmptyOperator(task_id="task_shared", pool="pool_shared")
        dr_b2 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        ti_b2 = dr_b2.get_task_instance("task_shared", session=session)
        ti_b2.state = State.SCHEDULED
        session.merge(ti_b2)
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        queued_keys = {ti.key for ti in res}

        # team_a task using its own pool: allowed
        assert ti_a.key in queued_keys
        # team_b task using team_a's pool: blocked
        assert ti_b.key not in queued_keys
        # team_b task using shared pool: allowed
        assert ti_b2.key in queued_keys
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

        ti1 = dag_run.get_task_instance(op1.task_id, session=session)
        ti2 = dag_run.get_task_instance(op2.task_id, session=session)
        ti3 = dag_run.get_task_instance(op3.task_id, session=session)
        ti4 = dag_run.get_task_instance(op4.task_id, session=session)
        ti5 = dag_run.get_task_instance(op5.task_id, session=session)

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
            dr1.get_task_instance(op1.task_id, session=session),
            dr1.get_task_instance(op2.task_id, session=session),
            dr2.get_task_instance(op3.task_id, session=session),
            dr2.get_task_instance(op4.task_id, session=session),
            dr3.get_task_instance(op5.task_id, session=session),
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
        executor_to_tis = self.job_runner._executor_to_workloads(res, session)

        # Team pi tasks should go to mock_executors[0] (configured for team_pi)
        a_tis_in_executor = [ti for ti in executor_to_tis.get(mock_executors[0], []) if ti.dag_id == "dag_a"]
        assert len(a_tis_in_executor) == 2

        # Team rho tasks should go to mock_executors[1] (configured for team_rho)
        b_tis_in_executor = [ti for ti in executor_to_tis.get(mock_executors[1], []) if ti.dag_id == "dag_b"]
        assert len(b_tis_in_executor) == 2

        # Global task should go to the default executor (self.job_runner.executor)
        global_tis_in_executor = [
            ti for ti in executor_to_tis.get(self.job_runner.executor, []) if ti.dag_id == "dag_global"
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

    @conf_vars(
        {
            ("scheduler", "max_tis_per_query"): "100",
            ("scheduler", "max_dagruns_to_create_per_loop"): "10",
            ("scheduler", "max_dagruns_per_loop_to_schedule"): "20",
            ("core", "parallelism"): "100",
            ("core", "max_active_tasks_per_dag"): "4",
            ("core", "max_active_runs_per_dag"): "10",
            ("core", "default_pool_task_slot_count"): "64",
        }
    )
    def test_max_active_tasks_per_dr_limit_applied_in_task_query(self, dag_maker, mock_executors):
        scheduler_job = Job()
        scheduler_job.max_tis_per_query = 100
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        self.job_runner.executor.slots_available = 70
        self.job_runner.executor.parallelism = 100
        session = settings.Session()

        session.add(Pool(pool="test_pool1", slots=64, description="with_slots1", include_deferred=False))
        session.add(Pool(pool="test_pool2", slots=64, description="with_slots2", include_deferred=False))
        session.flush()

        # Use the same run_id.
        task_maker(
            dag_maker,
            session,
            dag_id="dag_1300_tasks",
            task_num=1300,
            max_active_tasks=4,
            running_num=0,
            run_id="run1",
        )
        task_maker(
            dag_maker,
            session,
            dag_id="dag_1200_tasks",
            task_num=1200,
            max_active_tasks=4,
            running_num=0,
            run_id="run1",
        )
        task_maker(
            dag_maker,
            session,
            dag_id="dag_1100_tasks",
            task_num=1100,
            max_active_tasks=4,
            running_num=0,
            run_id="run1",
        )
        task_maker(
            dag_maker,
            session,
            dag_id="dag_100_tasks",
            task_num=100,
            max_active_tasks=4,
            running_num=0,
            run_id="run1",
        )
        task_maker(
            dag_maker,
            session,
            dag_id="dag_90_tasks",
            task_num=90,
            max_active_tasks=4,
            running_num=0,
            run_id="run1",
        )
        task_maker(
            dag_maker,
            session,
            dag_id="dag_80_tasks",
            task_num=80,
            max_active_tasks=4,
            running_num=0,
            run_id="run1",
        )

        count = 0
        iterations = 0

        from airflow.configuration import conf

        task_num = conf.getint("core", "max_active_tasks_per_dag") * 6

        # 6 dags * 4 = 24.
        assert task_num == 24

        queued_tis = None
        while count < task_num:
            # Use `_executable_task_instances_to_queued` because it returns a list of TIs
            # while `_critical_section_enqueue_task_instances` just returns the number of the TIs.
            queued_tis = self.job_runner._executable_task_instances_to_queued(
                max_tis=self.job_runner.executor.slots_available, session=session
            )
            count += len(queued_tis)
            iterations += 1

        assert iterations == 1
        assert count == task_num

        assert queued_tis is not None

        dag_counts = Counter(ti.dag_id for ti in queued_tis)

        # Tasks from all 6 dags should have been queued.
        assert len(dag_counts) == 6
        assert dag_counts == {
            "dag_1300_tasks": 4,
            "dag_1200_tasks": 4,
            "dag_1100_tasks": 4,
            "dag_100_tasks": 4,
            "dag_90_tasks": 4,
            "dag_80_tasks": 4,
        }, "Count for each dag_id should be 4 but it isn't"

    @conf_vars(
        {
            ("scheduler", "max_tis_per_query"): "100",
            ("scheduler", "max_dagruns_to_create_per_loop"): "10",
            ("scheduler", "max_dagruns_per_loop_to_schedule"): "20",
            ("core", "parallelism"): "100",
            ("core", "max_active_tasks_per_dag"): "4",
            ("core", "max_active_runs_per_dag"): "10",
            ("core", "default_pool_task_slot_count"): "64",
        }
    )
    def test_max_active_tasks_per_dr_limit_partial_capacity(self, dag_maker, mock_executors):
        scheduler_job = Job()
        scheduler_job.max_tis_per_query = 100
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        self.job_runner.executor.parallelism = 100
        self.job_runner.executor.slots_available = 70
        session = settings.Session()

        session.add(Pool(pool="test_pool1", slots=64, description="with_slots1", include_deferred=False))
        session.add(Pool(pool="test_pool2", slots=64, description="with_slots2", include_deferred=False))
        session.flush()

        # This dag_run will have 2 RUNNING tasks and 10 SCHEDULED tasks.
        task_maker(
            dag_maker,
            session,
            dag_id="dag_12_tasks",
            task_num=12,
            max_active_tasks=4,
            running_num=2,
            run_id="run1",
        )

        queued_tis = self.job_runner._executable_task_instances_to_queued(
            max_tis=self.job_runner.executor.slots_available, session=session
        )

        assert queued_tis is not None
        # Only 2 tasks should have been queued.
        assert len(queued_tis) == 2

        dag_counts = Counter(ti.dag_id for ti in queued_tis)

        assert len(dag_counts) == 1
        assert dag_counts == {
            "dag_12_tasks": 2,
        }, "There should be only 1 dag_id with count 2 but that isn't the case"

    @conf_vars(
        {
            ("scheduler", "max_tis_per_query"): "100",
            ("scheduler", "max_dagruns_to_create_per_loop"): "10",
            ("scheduler", "max_dagruns_per_loop_to_schedule"): "20",
            ("core", "parallelism"): "100",
            ("core", "max_active_tasks_per_dag"): "4",
            ("core", "max_active_runs_per_dag"): "10",
            ("core", "default_pool_task_slot_count"): "64",
        }
    )
    def test_max_active_tasks_per_dr_limit_starvation_filter_ordering(self, dag_maker, mock_executors):
        scheduler_job = Job()
        scheduler_job.max_tis_per_query = 100
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        self.job_runner.executor.parallelism = 100
        self.job_runner.executor.slots_available = 50
        session = settings.Session()

        # Add 2 pools, one starved and one with available slots.
        session.add(Pool(pool="test_pool1", slots=0, description="starved_no_slots", include_deferred=False))
        session.add(Pool(pool="test_pool2", slots=64, description="with_slots", include_deferred=False))
        session.flush()

        # All tasks in SCHEDULED state.
        task_maker(
            dag_maker,
            session,
            dag_id="dag_12_tasks",
            task_num=12,
            max_active_tasks=4,
            running_num=0,
            run_id="run1",
        )

        queued_tis = self.job_runner._executable_task_instances_to_queued(
            max_tis=self.job_runner.executor.slots_available, session=session
        )

        assert queued_tis is not None
        # 4 tasks should have been queued.
        assert len(queued_tis) == 4

        for ti in queued_tis:
            # The dag has 12 tasks. 'dummy0' has priority weight 0,
            # 'dummy10' has priority 5 and everything else has priority 1.
            # Tasks 'dummy0' and 'dummy10' belong to a starved pool.
            # If the starved pool had any available slots, 'dummy10' would be
            # the 1st task to be queued.
            assert ti.task_id != "dummy10"
            assert ti.priority_weight != 5
            assert ti.priority_weight == 1

        dag_counts = Counter(ti.dag_id for ti in queued_tis)

        assert len(dag_counts) == 1
        assert dag_counts == {
            "dag_12_tasks": 4,
        }, "There should be only 1 dag_id with count 4 but that isn't the case"

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

        ti1 = dag_run.get_task_instance(op1.task_id, session=session)
        ti2 = dag_run.get_task_instance(op2.task_id, session=session)
        ti3 = dag_run.get_task_instance(op3.task_id, session=session)

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

        ti1 = dr1.get_task_instance(op1.task_id, session=session)
        ti2 = dr2.get_task_instance(op2.task_id, session=session)
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
        cannot_run_ti_id = next(t for t in dr.task_instances if t.task_id == "cannot_run").id
        with caplog.at_level(logging.WARNING):
            self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
            assert (
                f"Not executing <TaskInstance: "
                f"SchedulerJobTest.test_test_not_enough_pool_slots.cannot_run test [scheduled] "
                f"ti_id={cannot_run_ti_id}>. "
                "Requested pool slots (4) are greater than total pool slots: '2' for pool: some_pool"
                in caplog.text
            )

        assert (
            session.scalar(
                select(func.count())
                .select_from(TaskInstance)
                .where(TaskInstance.dag_id == dag_id, TaskInstance.state == State.SCHEDULED)
            )
            == 1
        )
        assert (
            session.scalar(
                select(func.count())
                .select_from(TaskInstance)
                .where(TaskInstance.dag_id == dag_id, TaskInstance.state == State.QUEUED)
            )
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
        # set 1 ti from dr2 to running
        # one can be queued
        t1, t2, t3 = dr2.get_task_instances(session=session)
        t1.state = active_state
        t2.state = State.SCHEDULED
        t3.state = State.SCHEDULED
        session.merge(t1)
        session.merge(t2)
        session.merge(t3)
        # set 0 tis from dr3 to running
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
        session.scalars(select(TaskInstance)).all()

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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])
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

    def test_find_executable_task_instances_max_active_tis_per_dag_deferred_blocks(self, dag_maker, session):
        """
        A DEFERRED TI should count against max_active_tis_per_dag.

        When one TI is deferred, no additional TI of the same task should be
        scheduled if the limit is already reached.
        Regression test for https://github.com/apache/airflow/issues/61700
        """
        dag_id = "SchedulerJobTest.test_max_active_tis_per_dag_deferred_blocks"
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task1 = EmptyOperator(task_id="deferrable_task", max_active_tis_per_dag=1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, run_id="run_1", session=session)
        dr2 = dag_maker.create_dagrun_after(
            dr1, run_type=DagRunType.SCHEDULED, run_id="run_2", session=session
        )

        # DR1's TI is deferred (waiting on a trigger)
        ti1 = dr1.get_task_instance(task1.task_id, session=session)
        ti1.state = TaskInstanceState.DEFERRED
        session.merge(ti1)

        # DR2's TI is scheduled and wants to run
        ti2 = dr2.get_task_instance(task1.task_id, session=session)
        ti2.state = State.SCHEDULED
        session.merge(ti2)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        # ti2 should be blocked because ti1 is deferred and counts as active
        assert len(res) == 0
        session.rollback()

    def test_find_executable_task_instances_max_active_tis_per_dag_deferred_plus_running(
        self, dag_maker, session
    ):
        """
        Deferred + running TIs together fill the max_active_tis_per_dag limit.

        With max_active_tis_per_dag=2 and one RUNNING + one DEFERRED, a third
        SCHEDULED TI should be blocked.
        """
        dag_id = "SchedulerJobTest.test_max_active_tis_per_dag_deferred_plus_running"
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task1 = EmptyOperator(task_id="task", max_active_tis_per_dag=2)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, run_id="run_1", session=session)
        dr2 = dag_maker.create_dagrun_after(
            dr1, run_type=DagRunType.SCHEDULED, run_id="run_2", session=session
        )
        dr3 = dag_maker.create_dagrun_after(
            dr2, run_type=DagRunType.SCHEDULED, run_id="run_3", session=session
        )

        ti1 = dr1.get_task_instance(task1.task_id, session=session)
        ti1.state = TaskInstanceState.RUNNING
        session.merge(ti1)

        ti2 = dr2.get_task_instance(task1.task_id, session=session)
        ti2.state = TaskInstanceState.DEFERRED
        session.merge(ti2)

        ti3 = dr3.get_task_instance(task1.task_id, session=session)
        ti3.state = State.SCHEDULED
        session.merge(ti3)
        session.flush()

        # 1 running + 1 deferred = 2, which equals the limit
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 0
        session.rollback()

    def test_find_executable_task_instances_max_active_tis_per_dag_deferred_with_room(
        self, dag_maker, session
    ):
        """
        With max_active_tis_per_dag=2 and only 1 deferred, one more TI
        should be allowed to schedule.
        """
        dag_id = "SchedulerJobTest.test_max_active_tis_per_dag_deferred_with_room"
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task1 = EmptyOperator(task_id="task", max_active_tis_per_dag=2)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, run_id="run_1", session=session)
        dr2 = dag_maker.create_dagrun_after(
            dr1, run_type=DagRunType.SCHEDULED, run_id="run_2", session=session
        )
        dr3 = dag_maker.create_dagrun_after(
            dr2, run_type=DagRunType.SCHEDULED, run_id="run_3", session=session
        )

        ti1 = dr1.get_task_instance(task1.task_id, session=session)
        ti1.state = TaskInstanceState.DEFERRED
        session.merge(ti1)

        ti2 = dr2.get_task_instance(task1.task_id, session=session)
        ti2.state = State.SCHEDULED
        session.merge(ti2)

        ti3 = dr3.get_task_instance(task1.task_id, session=session)
        ti3.state = State.SCHEDULED
        session.merge(ti3)
        session.flush()

        # 1 deferred -> room for 1 more (limit is 2)
        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 1
        session.rollback()

    def test_find_executable_task_instances_deferred_does_not_block_different_task(self, dag_maker, session):
        """
        A DEFERRED TI of task A should NOT block task B from scheduling.

        max_active_tis_per_dag is per-task, not per-DAG.
        """
        dag_id = "SchedulerJobTest.test_deferred_does_not_block_different_task"
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task_a = EmptyOperator(task_id="task_a", max_active_tis_per_dag=1)
            task_b = EmptyOperator(task_id="task_b")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, run_id="run_1", session=session)
        dr2 = dag_maker.create_dagrun_after(
            dr1, run_type=DagRunType.SCHEDULED, run_id="run_2", session=session
        )

        # task_a in DR1 is deferred
        ti_a1 = dr1.get_task_instance(task_a.task_id, session=session)
        ti_a1.state = TaskInstanceState.DEFERRED
        session.merge(ti_a1)

        # task_a in DR2 is scheduled (should be blocked by deferred ti_a1)
        ti_a2 = dr2.get_task_instance(task_a.task_id, session=session)
        ti_a2.state = State.SCHEDULED
        session.merge(ti_a2)

        # task_b in DR1 is scheduled (should NOT be blocked)
        ti_b1 = dr1.get_task_instance(task_b.task_id, session=session)
        ti_b1.state = State.SCHEDULED
        session.merge(ti_b1)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        queued_task_ids = [ti.task_id for ti in res]
        # task_b should be queued, task_a should be blocked
        assert "task_b" in queued_task_ids
        assert "task_a" not in queued_task_ids
        session.rollback()

    def test_find_executable_task_instances_deferred_to_success_unblocks(self, dag_maker, session):
        """
        When a deferred TI completes (SUCCESS), the next TI should be unblocked.
        """
        dag_id = "SchedulerJobTest.test_deferred_to_success_unblocks"
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task1 = EmptyOperator(task_id="task", max_active_tis_per_dag=1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr1 = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, run_id="run_1", session=session)
        dr2 = dag_maker.create_dagrun_after(
            dr1, run_type=DagRunType.SCHEDULED, run_id="run_2", session=session
        )

        ti1 = dr1.get_task_instance(task1.task_id, session=session)
        ti2 = dr2.get_task_instance(task1.task_id, session=session)

        # Step 1: ti1 is deferred, ti2 scheduled -> ti2 blocked
        ti1.state = TaskInstanceState.DEFERRED
        ti2.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(ti2)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 0

        # Step 2: ti1 completes -> ti2 should be unblocked
        ti1.state = TaskInstanceState.SUCCESS
        session.merge(ti1)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 1
        assert res[0].key == ti2.key
        session.rollback()

    def test_find_executable_task_instances_max_active_tis_per_dagrun_deferred(self, dag_maker, session):
        """
        DEFERRED TIs should also count against max_active_tis_per_dagrun.

        With max_active_tis_per_dagrun=1 and 2 mapped instances in the same
        dagrun, if one is deferred, the other should be blocked.
        """
        dag_id = "SchedulerJobTest.test_max_active_tis_per_dagrun_deferred"
        with dag_maker(dag_id=dag_id, max_active_tasks=16, session=session):
            task_a = EmptyOperator.partial(task_id="task_a", max_active_tis_per_dagrun=1).expand_kwargs(
                [{"inputs": 1}, {"inputs": 2}]
            )
            EmptyOperator(task_id="task_b")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, session=session)

        ti_a0 = dr.get_task_instance(task_a.task_id, session=session, map_index=0)
        ti_a1 = dr.get_task_instance(task_a.task_id, session=session, map_index=1)

        ti_a0.state = TaskInstanceState.DEFERRED
        ti_a1.state = State.SCHEDULED
        session.merge(ti_a0)
        session.merge(ti_a1)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        queued_task_ids = [(ti.task_id, ti.map_index) for ti in res]
        # ti_a1 should be blocked, task_b may be queued
        assert ("task_a", 1) not in queued_task_ids
        session.rollback()

    def test_find_executable_task_instances_deferred_does_not_affect_max_active_tasks(
        self, dag_maker, session
    ):
        """
        Deferred TIs should NOT count toward max_active_tasks.

        max_active_tasks is about worker-level parallelism, while deferred tasks
        don't consume worker slots. With max_active_tasks=2 and 1 deferred TI,
        2 more SCHEDULED TIs should be allowed.
        """
        dag_id = "SchedulerJobTest.test_deferred_does_not_affect_max_active_tasks"
        with dag_maker(dag_id=dag_id, max_active_tasks=2, session=session):
            EmptyOperator(task_id="task_1")
            EmptyOperator(task_id="task_2")
            EmptyOperator(task_id="task_3")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED, session=session)
        t1, t2, t3 = sorted(dr.get_task_instances(session=session), key=lambda t: t.task_id)

        t1.state = TaskInstanceState.DEFERRED
        t2.state = State.SCHEDULED
        t3.state = State.SCHEDULED
        session.merge(t1)
        session.merge(t2)
        session.merge(t3)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        # Deferred doesn't count toward max_active_tasks=2, so both scheduled can run
        assert len(res) == 2
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

        ti1 = dr1.get_task_instance(op1.task_id, session=session)
        ti2 = dr1.get_task_instance(op2.task_id, session=session)
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

        ti1a = dr1.get_task_instance(op1a.task_id, session=session)
        ti1b = dr1.get_task_instance(op1b.task_id, session=session)
        ti2 = dr2.get_task_instance(op2.task_id, session=session)
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

        ti1a = dr1.get_task_instance(op1a.task_id, session=session)
        ti1b = dr1.get_task_instance(op1b.task_id, session=session)
        ti2a = dr2.get_task_instance(op1a.task_id, session=session)
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

        ti1a = dr1.get_task_instance(op1a.task_id, session=session)
        ti1b = dr1.get_task_instance(op1b.task_id, session=session)
        ti2a = dr2.get_task_instance(op1a.task_id, session=session)
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

        ti1a0 = dr.get_task_instance(op1a.task_id, session=session, map_index=0)
        ti1a1 = dr.get_task_instance(op1a.task_id, session=session, map_index=1)
        ti1b = dr.get_task_instance(op1b.task_id, session=session)
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

        ti1 = dr1.get_task_instance(op1.task_id, session=session)
        ti2 = dr1.get_task_instance(op2.task_id, session=session)
        ti1.state = State.SCHEDULED
        ti2.state = State.RUNNING
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=1, session=session)
        assert len(res) == 1
        assert res[0].key == ti1.key

        session.rollback()

    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_emit_pool_starving_tasks_metrics(self, mock_get_backend, dag_maker):
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        session = settings.Session()

        dag_id = "SchedulerJobTest.test_emit_pool_starving_tasks_metrics"
        with dag_maker(dag_id=dag_id):
            op = EmptyOperator(task_id="op", pool_slots=2)

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

        ti = dr.get_task_instance(op.task_id, session=session)
        ti.state = State.SCHEDULED

        set_default_pool_slots(1)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 0

        mock_stats.gauge.assert_has_calls(
            [
                mock.call("scheduler.tasks.starving", 1),
                mock.call(f"pool.starving_tasks.{Pool.DEFAULT_POOL_NAME}", 1),
                mock.call("pool.starving_tasks", 1, tags={"pool_name": Pool.DEFAULT_POOL_NAME}),
            ],
            any_order=True,
        )
        mock_stats.gauge.reset_mock()

        set_default_pool_slots(2)
        session.flush()

        res = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        assert len(res) == 1

        mock_stats.gauge.assert_has_calls(
            [
                mock.call("scheduler.tasks.starving", 0),
                mock.call(f"pool.starving_tasks.{Pool.DEFAULT_POOL_NAME}", 0),
                mock.call("pool.starving_tasks", 0, tags={"pool_name": Pool.DEFAULT_POOL_NAME}),
            ],
            any_order=True,
        )

        session.rollback()
        session.close()

    @pytest.mark.parametrize(
        ("multi_team", "expected_tags"),
        [
            pytest.param("true", {"pool_name": "team_pool", "team_name": "team_a"}, id="with_team"),
            pytest.param("false", {"pool_name": "team_pool"}, id="without_team"),
        ],
    )
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_emit_pool_metrics_team_name(self, mock_get_backend, multi_team, expected_tags, session):
        """Pool metrics include team_name only when multi_team is enabled."""
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats

        clear_db_teams()

        team = Team(name="team_a")
        session.add(team)
        session.flush()

        pool = Pool(pool="team_pool", slots=5, include_deferred=False, team_name="team_a")
        session.add(pool)
        session.flush()

        with conf_vars({("core", "multi_team"): multi_team}):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job)
            self.job_runner._emit_pool_metrics(session=session)

        mock_stats.gauge.assert_any_call("pool.open_slots", mock.ANY, tags=expected_tags)
        mock_stats.gauge.assert_any_call("pool.queued_slots", mock.ANY, tags=expected_tags)
        mock_stats.gauge.assert_any_call("pool.running_slots", mock.ANY, tags=expected_tags)

    @pytest.mark.parametrize(
        ("multi_team", "expected_tags"),
        [
            pytest.param(
                "true",
                {"queue": "default", "dag_id": "ti_gauge_dag", "task_id": "task1", "team_name": "ti_team"},
                id="with_team",
            ),
            pytest.param(
                "false",
                {"queue": "default", "dag_id": "ti_gauge_dag", "task_id": "task1"},
                id="without_team",
            ),
        ],
    )
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_emit_ti_metrics_team_name(self, mock_get_backend, multi_team, expected_tags, dag_maker, session):
        """TI gauge metrics include team_name only when multi_team is enabled."""
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats

        clear_db_teams()

        team = Team(name="ti_team")
        session.add(team)
        session.flush()

        clear_db_dag_bundles()

        bundle = DagBundleModel(name="ti_bundle")
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        with dag_maker(dag_id="ti_gauge_dag", bundle_name="ti_bundle", session=session):
            EmptyOperator(task_id="task1")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instances(session=session)[0]
        ti.state = State.RUNNING
        session.flush()

        with conf_vars({("core", "multi_team"): multi_team}):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job)
            self.job_runner._emit_ti_metrics(session=session)

        mock_stats.gauge.assert_any_call("ti.running", mock.ANY, tags=expected_tags)

    def test_enqueue_task_instances_with_queued_state(self, dag_maker, session):
        dag_id = "SchedulerJobTest.test_enqueue_task_instances_with_queued_state"
        task_id_1 = "dummy"
        session = settings.Session()
        with dag_maker(dag_id=dag_id, start_date=DEFAULT_DATE, session=session):
            task1 = EmptyOperator(task_id=task_id_1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        dr1 = dag_maker.create_dagrun()
        ti1 = dr1.get_task_instance(task1.task_id, session=session)

        with patch.object(BaseExecutor, "queue_workload") as mock_queue_workload:
            self.job_runner._enqueue_task_instances_with_queued_state(
                [ti1], executor=self.job_runner.executor, session=session
            )

        assert mock_queue_workload.called
        session.rollback()

    def test_executable_task_instances_to_queued_sets_external_executor_id(self, dag_maker, session):
        """external_executor_id is written to the DB in the same UPDATE that sets state=QUEUED."""
        dag_id = "SchedulerJobTest.test_executable_sets_external_executor_id"
        session = settings.Session()
        with dag_maker(dag_id=dag_id, start_date=DEFAULT_DATE, session=session):
            EmptyOperator(task_id="a_task_pre_assign")
            EmptyOperator(task_id="b_task_regular")

        class PreAssigningExecutor(MockExecutor):
            pre_assigns_external_executor_id = True
            mock_module_path = "mock.pre_assigning.executor"
            mock_alias = "pre_assigning_executor"

        regular_exec = MockExecutor()
        assert regular_exec.pre_assigns_external_executor_id is False, "Pre-condition"

        pre_assigning_exec = PreAssigningExecutor()

        self.job_runner = SchedulerJobRunner(job=Job(), executors=(regular_exec, pre_assigning_exec))

        dr = dag_maker.create_dagrun()
        ti_pre_assign = dr.get_task_instance("a_task_pre_assign", session=session)
        ti_regular = dr.get_task_instance("b_task_regular", session=session)

        ti_regular.state = State.SCHEDULED
        ti_regular.executor = regular_exec.name.module_path
        ti_pre_assign.state = State.SCHEDULED
        ti_pre_assign.executor = pre_assigning_exec.name.module_path
        session.flush()

        returned_tis = self.job_runner._executable_task_instances_to_queued(max_tis=32, session=session)
        returned_tis.sort(key=lambda ti: ti.task_id)

        assert len(returned_tis) == 2

        # In-memory object (post make_transient) should carry the UUID
        assert returned_tis[0].id == ti_pre_assign.id
        assert returned_tis[0].external_executor_id is not None
        assert UUID(returned_tis[0].external_executor_id), "is valid uuid"

        # DB row should also have it (the whole point — survives a crash)
        db_value = session.scalar(
            select(TaskInstance.external_executor_id).where(TaskInstance.id == ti_pre_assign.id)
        )
        assert db_value == returned_tis[0].external_executor_id

        # In mixed-executor mode, only TIs routed to a pre-assigning executor get an external_executor_id.
        assert returned_tis[1].id == ti_regular.id
        assert returned_tis[1].external_executor_id is None

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
        ti = dr1.get_task_instance(task1.task_id, session=session)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        with patch.object(BaseExecutor, "queue_workload") as mock_queue_workload:
            self.job_runner._enqueue_task_instances_with_queued_state(
                [ti], executor=self.job_runner.executor, session=session
            )
        session.flush()
        ti.refresh_from_db(session=session)
        assert ti.state == State.NONE
        mock_queue_workload.assert_not_called()

    def test_enqueue_task_instances_skips_ti_without_dag_version_id(self, dag_maker, session, caplog):
        """Task instances without dag_version_id are not enqueued and an error is logged."""
        dag_id = "SchedulerJobTest.test_enqueue_task_instances_skips_ti_without_dag_version_id"
        task_id_1 = "dummy"
        session = settings.Session()
        with dag_maker(dag_id=dag_id, start_date=DEFAULT_DATE, session=session):
            task1 = EmptyOperator(task_id=task_id_1)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        dr1 = dag_maker.create_dagrun()
        ti = dr1.get_task_instance(task1.task_id, session=session)
        ti.state = State.SCHEDULED
        ti.dag_version_id = None
        session.merge(ti)
        session.commit()

        with patch.object(BaseExecutor, "queue_workload") as mock_queue_workload:
            with caplog.at_level("WARNING", logger="airflow.jobs.scheduler_job_runner"):
                self.job_runner._enqueue_task_instances_with_queued_state(
                    [ti], executor=self.job_runner.executor, session=session
                )

        mock_queue_workload.assert_not_called()
        assert any(
            "does not have a dag_version_id set, cannot be enqueued" in rec.message for rec in caplog.records
        )
        session.rollback()

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

        dr1_ti1 = dr1.get_task_instance(task1.task_id, session=session)
        dr1_ti2 = dr1.get_task_instance(task2.task_id, session=session)
        dr1_ti3 = dr1.get_task_instance(task3.task_id, session=session)
        dr1_ti4 = dr1.get_task_instance(task4.task_id, session=session)
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
        dr2_ti1 = dr2.get_task_instance(task1.task_id, session=session)
        dr2_ti2 = dr2.get_task_instance(task2.task_id, session=session)
        dr2_ti3 = dr2.get_task_instance(task3.task_id, session=session)
        dr2_ti4 = dr2.get_task_instance(task4.task_id, session=session)
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
            ti1 = dr.get_task_instance(task1.task_id, session=session)
            tis1.append(ti1)
            ti2 = dr.get_task_instance(task2.task_id, session=session)
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
            ti.refresh_from_db(session=session)
            assert ti.state == TaskInstanceState.QUEUED
        for ti in tis1[3:] + tis2[3:]:
            ti.refresh_from_db(session=session)
            assert ti.state == TaskInstanceState.SCHEDULED

        # The remaining TIs are queued
        res = self.job_runner._critical_section_enqueue_task_instances(session)
        assert res == 2
        session.flush()

        for ti in tis1 + tis2:
            ti.refresh_from_db(session=session)
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
            ti1 = dr.get_task_instance(task1.task_id, session=session)
            tis.append(ti1)
            ti2 = dr.get_task_instance(task2.task_id, session=session)
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
            ti1 = dr.get_task_instance(task1.task_id, session=session)
            tis.append(ti1)
            ti2 = dr.get_task_instance(task2.task_id, session=session)
            tis.append(ti2)
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.flush()

        scheduler_job.max_tis_per_query = 8
        self.job_runner.executor.slots_available = 2  # Limit only the default executor to 2 slots.
        # Check that we don't "overfill" an executor when the max tis per query is larger than slots
        # available. Of the 8 tasks returned by the query, the default executor will only take 2 and the
        # secondary executor will take 4 (since only 4 of the 8 TIs in the result will be for that executor)
        res = self.job_runner._critical_section_enqueue_task_instances(session)
        assert res == 6

        self.job_runner.executor.slots_available = 6  # The default executor has more slots freed now and
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
            ti1 = dr.get_task_instance(task1.task_id, session=session)
            ti2 = dr.get_task_instance(task2.task_id, session=session)
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.flush()
        scheduler_job.max_tis_per_query = 0
        self.job_runner.executor.parallelism = 32
        self.job_runner.executor.slots_available = 31

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
        with conf_vars({("core", "parallelism"): "40"}):
            # 40 dag runs * 2 tasks each = 80. Two executors have capacity for 61 concurrent jobs, but they
            # together respect core.parallelism and will not run more in aggregate then that allows.
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
            ti1 = dr.get_task_instance(task1.task_id, session=session)
            ti2 = dr.get_task_instance(task2.task_id, session=session)
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.flush()
        scheduler_job.max_tis_per_query = 0
        for executor in mock_executors:
            executor.parallelism = 32
            executor.slots_available = 31

        total_enqueued = 0
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

    def test_adopt_or_reset_orphaned_tasks_loads_state_for_reset_logging(
        self, dag_maker, session, mock_executor
    ):
        with dag_maker("test_adopt_or_reset_orphaned_tasks_loads_state_for_reset_logging", session=session):
            op1 = EmptyOperator(task_id="op1")

        scheduler_job = Job()
        session.add(scheduler_job)
        session.flush()

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.QUEUED
        ti.queued_by_job_id = scheduler_job.id
        session.commit()
        session.expunge_all()

        def refuse_adoption(tis):
            assert len(tis) == 1
            # ``repr(ti)`` in the reset path reads both ``state`` and ``map_index``; the query
            # must load both so the reset log stays accurate (and never lazy-loads on detach).
            unloaded = inspect(tis[0]).unloaded
            assert "state" not in unloaded
            assert "map_index" not in unloaded
            # repr must render the real state, not the ``<deferred>`` fallback.
            assert "queued" in repr(tis[0])
            return tis

        mock_executor.try_adopt_task_instances.side_effect = refuse_adoption

        self.job_runner = SchedulerJobRunner(job=Job(), num_runs=0)

        assert self.job_runner.adopt_or_reset_orphaned_tasks(session=session) == 1

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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[mock_executor])

        ti = dag_run.get_task_instance(task_id="task", session=session)
        ti.state = TaskInstanceState.RUNNING
        ti.queued_by_job_id = scheduler_job.id
        ti.last_heartbeat_at = timezone.utcnow() - timedelta(hours=1)
        # Simulate missing dag_version (legacy Airflow 2 task)
        ti.dag_version_id = None
        session.merge(ti)
        session.commit()

        with caplog.at_level("INFO", logger="airflow.jobs.scheduler_job_runner"):
            self.job_runner._purge_task_instances_without_heartbeats([ti], session=session)

        # dag_version_id should be backfilled from the latest DagVersion in the DB
        # (dag_maker creates one) and the callback should be sent
        assert any("Backfilled dag_version_id" in rec.message for rec in caplog.records)
        mock_executor.send_callback.assert_called_once()

    @pytest.mark.parametrize(
        ("multi_team", "expected_tags"),
        [
            pytest.param(
                "true",
                {"dag_id": "heartbeat_dag", "task_id": "task", "team_name": "hb_team"},
                id="with_team",
            ),
            pytest.param(
                "false",
                {"dag_id": "heartbeat_dag", "task_id": "task"},
                id="without_team",
            ),
        ],
    )
    @mock.patch("airflow._shared.observability.metrics.stats.incr")
    def test_purge_heartbeat_killed_metric_team_name(
        self, mock_incr, multi_team, expected_tags, dag_maker, session
    ):
        clear_db_teams()
        team = Team(name="hb_team")
        session.add(team)
        session.flush()

        clear_db_dag_bundles()
        bundle = DagBundleModel(name="hb_bundle")
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        with dag_maker("heartbeat_dag", bundle_name="hb_bundle", session=session):
            EmptyOperator(task_id="task")

        dag_run = dag_maker.create_dagrun(run_id="test_run", state=DagRunState.RUNNING)

        mock_executor = MagicMock()
        scheduler_job = Job()

        ti = dag_run.get_task_instance(task_id="task", session=session)
        ti.state = TaskInstanceState.RUNNING
        ti.queued_by_job_id = scheduler_job.id
        ti.last_heartbeat_at = timezone.utcnow() - timedelta(hours=1)
        session.merge(ti)
        session.commit()

        with conf_vars({("core", "multi_team"): multi_team}):
            self.job_runner = SchedulerJobRunner(scheduler_job, executors=[mock_executor])
            self.job_runner._purge_task_instances_without_heartbeats([ti], session=session)

        mock_incr.assert_any_call("task_instances_without_heartbeats_killed", tags=expected_tags)

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

        assert "revoke_task" in BaseExecutor.__dict__
        # this is just verifying that LocalExecutor is good enough for this test
        # in that it does not implement revoke_task
        assert "revoke_task" not in LocalExecutor.__dict__
        scheduler_job = Job()
        job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=0, executors=[LocalExecutor()])
        job_runner._task_queued_timeout = 300
        job_runner._handle_tasks_stuck_in_queued()

    def test_executor_end_called(self, mock_executors):
        """
        Test to make sure executor.end gets called with a successful scheduler loop run
        """
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)
        self.job_runner.executor.end.assert_called_once()

    def test_executor_end_called_multiple_executors(self, mock_executors):
        """
        Test to make sure executor.end gets called on all executors with a successful scheduler loop run
        """
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        run_job(scheduler_job, execute_callable=self.job_runner._execute)
        self.job_runner.executor.end.assert_called_once()
        for executor in self.job_runner.executors:
            executor.end.assert_called_once()

    def test_cleanup_methods_all_called(self):
        """
        Test to make sure all cleanup methods are called when the scheduler loop has an exception
        """
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(
            job=scheduler_job, num_runs=1, executors=[mock.MagicMock(slots_available=8)]
        )
        self.job_runner._run_scheduler_loop = mock.MagicMock(side_effect=RuntimeError("oops"))
        self.job_runner.executor.end = mock.MagicMock(side_effect=RuntimeError("triple oops"))

        with pytest.raises(RuntimeError, match="oops"):
            run_job(scheduler_job, execute_callable=self.job_runner._execute)

        self.job_runner.executor.end.assert_called_once()

    def test_cleanup_methods_all_called_multiple_executors(self, mock_executors):
        """
        Test to make sure all cleanup methods are called when the scheduler loop has an exception
        """
        scheduler_job = Job()

        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1)
        self.job_runner._run_scheduler_loop = mock.MagicMock(side_effect=RuntimeError("oops"))
        self.job_runner.executor.end = mock.MagicMock(side_effect=RuntimeError("triple oops"))

        with pytest.raises(RuntimeError, match="oops"):
            run_job(scheduler_job, execute_callable=self.job_runner._execute)

        for executor in self.job_runner.executors:
            executor.end.assert_called_once()

    def test_queued_dagruns_stops_creating_when_max_active_is_reached(self, dag_maker, session):
        """This tests that _create_dag_runs stops creating once max_active_runs is reached"""
        with dag_maker(max_active_runs=10) as dag:
            EmptyOperator(task_id="mytask")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        orm_dag = session.get(DagModel, dag.dag_id)
        assert orm_dag is not None
        for _num in range(20):
            self.job_runner._create_dag_runs([orm_dag], session)
        drs = session.scalars(select(DagRun)).all()
        assert len(drs) == 10

        for dr in drs:
            dr.state = State.RUNNING
            session.merge(dr)
        session.commit()
        assert session.scalar(select(func.count(DagRun.state)).where(DagRun.state == State.RUNNING)) == 10
        for _ in range(20):
            self.job_runner._create_dag_runs([orm_dag], session)
        assert session.scalar(select(func.count()).select_from(DagRun)) == 10
        assert session.scalar(select(func.count(DagRun.state)).where(DagRun.state == State.RUNNING)) == 10
        assert session.scalar(select(func.count(DagRun.state)).where(DagRun.state == State.QUEUED)) == 0
        assert orm_dag.next_dagrun_create_after is not None

    def test_runs_are_created_after_max_active_runs_was_reached(self, dag_maker, session):
        """
        Test that when creating runs once max_active_runs is reached the runs does not stick
        """
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

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

    def test_dagrun_timeout_verify_max_active_runs(self, dag_maker, session):
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

        orm_dag = session.get(DagModel, dag.dag_id)
        assert orm_dag is not None

        self.job_runner._create_dag_runs([orm_dag], session)
        self.job_runner._start_queued_dagruns(session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        assert isinstance(orm_dag.next_dagrun_create_after, datetime.datetime)
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
                last_ti=dr.get_task_instance("dummy", session=session),
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
        assert dag_maker.dag_model.next_dagrun == dr.logical_date
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        self.job_runner._schedule_dag_run(dr, session)
        session.flush()
        session.refresh(dr)
        assert dr.state == State.FAILED
        # check that next_dagrun_create_after has been updated by calculate_dagrun_date_fields
        assert dag_maker.dag_model.next_dagrun_create_after == dr.logical_date + timedelta(days=1)
        # check that no running/queued runs yet
        assert (
            session.scalar(
                select(func.count())
                .select_from(DagRun)
                .where(DagRun.state.in_([DagRunState.RUNNING, DagRunState.QUEUED]))
            )
            == 0
        )

    @pytest.mark.parametrize(
        ("state", "expected_callback_msg"), [(State.SUCCESS, "success"), (State.FAILED, "task_failure")]
    )
    @conf_vars({("scheduler", "use_job_schedule"): "False"})
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("dummy", session=session)
        ti.set_state(state, session=session)
        session.flush()

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
        self.job_runner.executor.callback_sink.send.assert_called_once_with(expected_callback)
        session.rollback()
        session.close()

    @pytest.mark.parametrize(
        ("state", "expected_callback_msg"), [(State.SUCCESS, "success"), (State.FAILED, "task_failure")]
    )
    @conf_vars({("scheduler", "use_job_schedule"): "False"})
    def test_dagrun_plugins_are_notified(
        self, state, expected_callback_msg, dag_maker, session, listener_manager
    ):
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
        listener_manager(dag_listener)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        dr = dag_maker.create_dagrun()

        ti = dr.get_task_instance("dummy", session=session)
        ti.set_state(state, session=session)

        self.job_runner._do_scheduling(session)

        assert len(dag_listener.success) or len(dag_listener.failure)

        dag_listener.success = []
        dag_listener.failure = []

        session.rollback()

    @conf_vars({("scheduler", "use_job_schedule"): "False"})
    def test_dagrun_timeout_callbacks_are_stored_in_database(self, dag_maker, session):
        with dag_maker(
            dag_id="test_dagrun_timeout_callbacks_are_stored_in_database",
            on_failure_callback=lambda x: print("failed"),
            dagrun_timeout=timedelta(hours=1),
            session=session,
        ) as dag:
            EmptyOperator(task_id="empty")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        self.job_runner.executor.callback_sink = DatabaseCallbackSink()

        dr = dag_maker.create_dagrun(start_date=DEFAULT_DATE)

        self.job_runner._do_scheduling(session)

        callback = (
            session.scalars(select(DbCallbackRequest).order_by(DbCallbackRequest.id.desc()))
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
                last_ti=dr.get_task_instance("empty", session=session),
            ),
        )

        assert callback == expected_callback

    @conf_vars({("scheduler", "use_job_schedule"): "False"})
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
        ti.set_state(State.SUCCESS, session=session)

        with mock.patch("airflow.jobs.scheduler_job_runner.prohibit_commit") as mock_guard:
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
    @conf_vars({("scheduler", "use_job_schedule"): "False"})
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
        ti = dr.get_task_instance("test_task", session=session)
        ti.set_state(state, session=session)

        self.job_runner._do_scheduling(session)

        # Verify Callback is not set (i.e is None) when no callbacks are set on DAG
        self.job_runner._send_dag_callbacks_to_processor.assert_called_once()
        call_args = self.job_runner._send_dag_callbacks_to_processor.call_args.args
        assert call_args[0].dag_id == dr.dag_id
        assert call_args[1] is None

        session.rollback()

    @pytest.mark.parametrize(("state", "msg"), [[State.SUCCESS, "success"], [State.FAILED, "task_failure"]])
    @conf_vars({("scheduler", "use_job_schedule"): "False"})
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
        ti.set_state(state, session=session)

        self.job_runner._do_scheduling(session)

        # Verify Callback is set (i.e is None) when no callbacks are set on DAG
        self.job_runner._send_dag_callbacks_to_processor.assert_called_once()
        call_args = self.job_runner._send_dag_callbacks_to_processor.call_args.args
        assert call_args[0].dag_id == dr.dag_id
        assert call_args[1] is not None
        assert call_args[1].msg == msg
        session.rollback()
        session.close()

    @conf_vars({("scheduler", "use_job_schedule"): "False"})
    def test_dagrun_notify_called_success(self, dag_maker, listener_manager):
        with dag_maker(
            dag_id="test_dagrun_notify_called",
            on_success_callback=lambda x: print("success"),
            on_failure_callback=lambda x: print("failed"),
        ):
            EmptyOperator(task_id="dummy")

        dag_listener.clear()
        listener_manager(dag_listener)

        executor = MockExecutor(do_update=False)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        session = settings.Session()
        dr = dag_maker.create_dagrun()

        ti = dr.get_task_instance("dummy")
        ti.set_state(State.SUCCESS, session=session)

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
        initial_tis = session.scalars(
            select(TaskInstance).where(TaskInstance.dag_id == dag_id, TaskInstance.task_id == "dummy")
        ).all()
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
        new_tis = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == dag_id,
                TaskInstance.task_id == "dummy",
                TaskInstance.run_id == "test_run_2",
            )
        ).all()
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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        self.job_runner._do_scheduling(session)
        assert (
            session.scalars(select(DagRun).where(DagRun.dag_id == dr.dag_id, DagRun.run_id == dr.run_id))
            .one()
            .state
            == run_state
        )

    def test_dagrun_root_after_dagrun_unfinished(self, mock_executor, testing_dag_bundle):
        """
        DagRuns with one successful and one future root task -> SUCCESS

        Noted: the DagRun state could be still in running state during CI.
        """
        dagbag = DagBag(TEST_DAG_FOLDER)
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
        dagbag = DagBag(TEST_DAG_FOLDER)
        with create_session() as session:
            dag_id = "test_start_date_scheduling"
            dag = dagbag.get_dag(dag_id)

            # Deactivate other dags in this file
            other_dag = dagbag.get_dag("test_task_start_date_scheduling")
            other_dag.is_paused_upon_creation = True

            scheduler_dag, _ = sync_dags_to_db([dag, other_dag])
            scheduler_dag.clear()
            assert scheduler_dag.start_date > datetime.datetime.now(timezone.utc)

            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1, executors=[self.null_exec])

            run_job(scheduler_job, execute_callable=self.job_runner._execute)

            # zero tasks ran
            assert len(session.scalars(select(TaskInstance).where(TaskInstance.dag_id == dag_id)).all()) == 0
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
            assert len(session.scalars(select(TaskInstance).where(TaskInstance.dag_id == dag_id)).all()) == 1
            session.commit()

            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1, executors=[self.null_exec])

            run_job(scheduler_job, execute_callable=self.job_runner._execute)

            # still one task
            assert len(session.scalars(select(TaskInstance).where(TaskInstance.dag_id == dag_id)).all()) == 1
            session.commit()
            assert self.null_exec.sorted_tasks == []

    def test_scheduler_task_start_date_catchup_true(self, testing_dag_bundle):
        """
        Test that with catchup=True, the scheduler respects task start dates that are different from DAG start dates
        """
        dagbag = DagBag(
            dag_folder=os.path.join(settings.DAGS_FOLDER, "test_scheduler_dags.py"),
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=3, executors=[self.null_exec])

        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        session = settings.Session()
        ti1s = session.scalars(
            select(TaskInstance).where(TaskInstance.dag_id == dag_id, TaskInstance.task_id == "dummy1")
        ).all()
        ti2s = session.scalars(
            select(TaskInstance).where(TaskInstance.dag_id == dag_id, TaskInstance.task_id == "dummy2")
        ).all()

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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=3, executors=[self.null_exec])

        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        session = settings.Session()
        ti1s = session.scalars(
            select(TaskInstance).where(TaskInstance.dag_id == dag_id, TaskInstance.task_id == "dummy1")
        ).all()
        ti2s = session.scalars(
            select(TaskInstance).where(TaskInstance.dag_id == dag_id, TaskInstance.task_id == "dummy2")
        ).all()

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
        dagbag = DagBag(TEST_DAG_FOLDER)
        dag_ids = [
            "test_start_date_scheduling",
            "test_task_start_date_scheduling",
        ]
        for dag_id in dag_ids:
            dag = dagbag.get_dag(dag_id)
            if not dag:
                raise ValueError(f"could not find dag {dag_id}")
            create_scheduler_dag(dag).clear()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1, executors=[self.null_exec])

        run_job(scheduler_job, execute_callable=self.job_runner._execute)

        # zero tasks ran
        dag_id = "test_start_date_scheduling"
        session = settings.Session()
        assert len(session.scalars(select(TaskInstance).where(TaskInstance.dag_id == dag_id)).all()) == 0

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
            next_info = dag.next_dagrun_info(last_automated_run_info=None)
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
                next_info = dag.next_dagrun_info(last_automated_run_info=next_info)
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

        ti0 = session.scalars(
            select(TaskInstance).where(TaskInstance.task_id == "test_scheduler_verify_priority_and_slots_t0")
        ).first()
        assert ti0.state == State.SCHEDULED

        ti1 = session.scalars(
            select(TaskInstance).where(TaskInstance.task_id == "test_scheduler_verify_priority_and_slots_t1")
        ).first()
        assert ti1.state == State.QUEUED

        ti2 = session.scalars(
            select(TaskInstance).where(TaskInstance.task_id == "test_scheduler_verify_priority_and_slots_t2")
        ).first()
        assert ti2.state == State.QUEUED

    def test_verify_integrity_if_dag_not_changed(self, dag_maker, session):
        # CleanUp
        session.execute(
            delete(SerializedDagModel).where(
                SerializedDagModel.dag_id == "test_verify_integrity_if_dag_not_changed"
            ),
            execution_options={"synchronize_session": False},
        )

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

        tis_count = session.scalar(
            select(func.count(TaskInstance.task_id)).where(
                TaskInstance.dag_id == dr.dag_id,
                TaskInstance.logical_date == dr.logical_date,
                TaskInstance.task_id == dr.dag.tasks[0].task_id,
                TaskInstance.state == State.SCHEDULED,
            )
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
            session.execute(
                delete(SerializedDagModel).where(
                    SerializedDagModel.dag_id == "test_verify_integrity_if_dag_changed"
                ),
                execution_options={"synchronize_session": False},
            )

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
        assert len(self.job_runner.scheduler_dag_bag.get_dag_for_run(dr, session).tasks) == 1
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
        drs = session.scalars(select(DagRun).options(joinedload(DagRun.task_instances))).unique().all()
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
        dag_maker.dag_model.calculate_dagrun_date_fields(dag, reference_run=None)

        @provide_session
        def do_schedule(*, session: Session = NEW_SESSION):
            # Use a empty file since the above mock will return the
            # expected DAGs. Also specify only a single file so that it doesn't
            # try to schedule the above DAG repeatedly.
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1, executors=[executor])

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
            ti = session.scalars(
                select(TaskInstance).where(
                    TaskInstance.dag_id == "test_retry_still_in_executor",
                    TaskInstance.task_id == "test_retry_handling_op",
                )
            ).first()
        assert ti is not None, "Task not created by scheduler"

        def run_with_error(ti, ignore_ti_state=False):
            ti.refresh_from_task(dag_maker.serialized_dag.get_task(dag_task1.task_id))
            with contextlib.suppress(AirflowException):
                run_task_instance(ti, dag_task1, ignore_ti_state=ignore_ti_state)

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
        from airflow.models.taskinstancehistory import TaskInstanceHistory

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
        old_ti_id = ti.id
        old_try_number = ti.try_number
        session.merge(ti)
        session.merge(dr1)
        session.commit()

        num_reset_tis = self.job_runner.adopt_or_reset_orphaned_tasks(session=session)
        assert num_reset_tis == 1

        ti.refresh_from_db(session=session)
        assert ti.id != old_ti_id
        assert (
            session.scalar(
                select(TaskInstanceHistory).where(
                    TaskInstanceHistory.dag_id == ti.dag_id,
                    TaskInstanceHistory.task_id == ti.task_id,
                    TaskInstanceHistory.run_id == ti.run_id,
                    TaskInstanceHistory.map_index == ti.map_index,
                    TaskInstanceHistory.try_number == old_try_number,
                    TaskInstanceHistory.task_instance_id == old_ti_id,
                )
            )
            is not None
        )

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
        # todo: this fails
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

    def test_should_update_dag_next_dagrun(self, session, dag_maker):
        """We should always update next_dagrun after scheduler creates a new dag run."""
        with dag_maker(schedule="0 0 * * *"):
            EmptyOperator(task_id="dummy")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        with patch("airflow.models.dag.DagModel.calculate_dagrun_date_fields") as mock_calc:
            self.job_runner._create_dag_runs(dag_models=[dag_maker.dag_model], session=session)
        assert mock_calc.called

    @pytest.mark.parametrize(
        "expected",
        [
            DagRunInfo(
                run_after=pendulum.now(),
                data_interval=None,
                partition_date=None,
                partition_key="helloooooo",
            ),
            DagRunInfo(
                run_after=pendulum.now(),
                data_interval=DataInterval(pendulum.today(), pendulum.today()),
                partition_date=None,
                partition_key=None,
            ),
        ],
    )
    @patch("airflow.timetables.base.Timetable.next_run_info_from_dag_model")
    @patch("airflow.serialization.definitions.dag.SerializedDAG.create_dagrun")
    def test_should_use_info_from_timetable(self, mock_create, mock_next, expected, session, dag_maker):
        """We should always update next_dagrun after scheduler creates a new dag run."""
        mock_next.return_value = expected
        with dag_maker(schedule="0 0 * * *"):
            EmptyOperator(task_id="dummy")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])
        self.job_runner._create_dag_runs(dag_models=[dag_maker.dag_model], session=session)
        kwargs = mock_create.call_args.kwargs
        actual = DagRunInfo(
            run_after=kwargs["run_after"],
            data_interval=kwargs["data_interval"],
            partition_key=kwargs["partition_key"],
            partition_date=None,
        )
        assert actual == expected

    @pytest.mark.parametrize(
        ("run_type", "expected"),
        [
            (DagRunType.MANUAL, True),
            (DagRunType.SCHEDULED, True),
            (DagRunType.BACKFILL_JOB, False),
            (DagRunType.ASSET_TRIGGERED, True),
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
        ):
            EmptyOperator(task_id="dummy")

        run = dag_maker.create_dagrun(
            run_id="run",
            run_type=run_type,
            logical_date=DEFAULT_DATE,
            start_date=timezone.utcnow(),
            state=State.SUCCESS,
            session=session,
        )

        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        # ensure terminal state; otherwise the run type is moot
        run.state = DagRunState.FAILED
        for ti in run.get_task_instances(session=session):
            ti.state = "failed"
        session.flush()

        check_mock = MagicMock()
        self.job_runner._set_exceeds_max_active_runs = check_mock
        with patch("airflow.models.dag.DagModel.calculate_dagrun_date_fields") as mock_calc:
            self.job_runner._schedule_dag_run(
                dag_run=run,
                session=session,
            )
            assert not mock_calc.called
        assert check_mock.called == expected

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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        with create_session() as session:
            self.job_runner._create_dag_runs([dag_model], session)

        dr = session.scalars(select(DagRun).where(DagRun.dag_id == dag.dag_id)).one()
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
        dr1 = dag_maker.create_dagrun(
            run_id="run1",
            logical_date=(DEFAULT_DATE + timedelta(days=100)),
            data_interval=(DEFAULT_DATE + timedelta(days=10), DEFAULT_DATE + timedelta(days=11)),
        )
        dr2 = dag_maker.create_dagrun(
            run_id="run2",
            logical_date=(DEFAULT_DATE + timedelta(days=101)),
            data_interval=(DEFAULT_DATE + timedelta(days=5), DEFAULT_DATE + timedelta(days=6)),
        )

        asset1_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset1.uri))

        # Consumer Dags are created before the events, so the events fall within their window.
        with dag_maker(dag_id="assets-consumer-multiple", schedule=[asset1, asset2]):
            pass
        dag2 = dag_maker.dag
        with dag_maker(dag_id="assets-consumer-single", schedule=[asset1]):
            pass
        dag3 = dag_maker.dag

        base = session.scalar(
            select(DagScheduleAssetReference.created_at).where(
                DagScheduleAssetReference.dag_id == dag3.dag_id
            )
        )
        event1 = AssetEvent(
            asset_id=asset1_id,
            source_task_id="task",
            source_dag_id=dr1.dag_id,
            source_run_id=dr1.run_id,
            source_map_index=-1,
            timestamp=base + timedelta(seconds=1),
        )
        event2 = AssetEvent(
            asset_id=asset1_id,
            source_task_id="task",
            source_dag_id=dr2.dag_id,
            source_run_id=dr2.run_id,
            source_map_index=-1,
            timestamp=base + timedelta(seconds=2),
        )
        session.add_all([event1, event2])

        session = dag_maker.session
        session.add_all(
            [
                AssetDagRunQueue(
                    asset_id=asset1_id, target_dag_id=dag2.dag_id, created_at=base + timedelta(hours=1)
                ),
                AssetDagRunQueue(
                    asset_id=asset1_id, target_dag_id=dag3.dag_id, created_at=base + timedelta(hours=1)
                ),
            ]
        )
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        with create_session() as session:
            self.job_runner._create_dagruns_for_dags(session, session)

        def dict_from_obj(obj):
            """Get dict of column attrs from SqlAlchemy object."""
            return {k.key: obj.__dict__.get(k) for k in obj.__mapper__.column_attrs}

        # dag3 should be triggered since it only depends on asset1, and it's been queued
        created_run = session.scalars(select(DagRun).where(DagRun.dag_id == dag3.dag_id)).one()
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
        assert (
            session.scalars(
                select(AssetDagRunQueue).where(AssetDagRunQueue.target_dag_id == dag2.dag_id)
            ).one()
            is not None
        )
        # dag2 should not be triggered since it depends on both asset 1  and 2
        assert session.scalars(select(DagRun).where(DagRun.dag_id == dag2.dag_id)).one_or_none() is None
        # dag3 ADRQ record should be deleted since the dag run was triggered
        assert (
            session.scalars(
                select(AssetDagRunQueue).where(AssetDagRunQueue.target_dag_id == dag3.dag_id)
            ).one_or_none()
            is None
        )

        assert created_run.creating_job_id == scheduler_job.id

    @pytest.mark.need_serialized_dag
    @pytest.mark.parametrize(
        ("catchup", "expects_old_event"),
        [
            pytest.param(False, False, id="catchup-off-ignores-backlog"),
            pytest.param(True, True, id="catchup-on-consumes-backlog"),
        ],
    )
    def test_new_asset_triggered_dag_backlog_gated_by_catchup(
        self, catchup, expects_old_event, session, dag_maker
    ):
        """Reproduces #39456: catchup gates whether a new asset-triggered Dag replays the
        pre-creation backlog. With catchup off (the default) it only consumes events after it
        started scheduling on the asset; with catchup on it replays the full history."""
        asset = Asset(uri="test://asset-historical", name="hist_asset", group="test_group")

        # Producer Dag + run that the asset events are sourced from.
        with dag_maker(dag_id="historical-producer", start_date=timezone.utcnow(), session=session):
            BashOperator(task_id="task", bash_command="echo 1", outlets=[asset])
        producer_run = dag_maker.create_dagrun(run_id="producer-run")

        asset_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset.uri))

        # Consumer Dag created now; its schedule reference's created_at is the cut-off.
        with dag_maker(dag_id="historical-consumer", schedule=[asset], catchup=catchup):
            pass
        consumer_dag = dag_maker.dag
        reference_created_at = session.scalar(
            select(DagScheduleAssetReference.created_at).where(
                DagScheduleAssetReference.dag_id == consumer_dag.dag_id
            )
        )

        def _make_event(timestamp):
            return AssetEvent(
                asset_id=asset_id,
                source_task_id="task",
                source_dag_id=producer_run.dag_id,
                source_run_id=producer_run.run_id,
                source_map_index=-1,
                timestamp=timestamp,
            )

        old_event = _make_event(reference_created_at - timedelta(days=1))
        new_event = _make_event(reference_created_at + timedelta(seconds=1))
        session.add_all([old_event, new_event])
        # Trigger time after both events so neither is excluded by the upper bound.
        session.add(
            AssetDagRunQueue(
                asset_id=asset_id,
                target_dag_id=consumer_dag.dag_id,
                created_at=reference_created_at + timedelta(hours=1),
            )
        )
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])
        with create_session() as session:
            self.job_runner._create_dagruns_for_dags(session, session)

        created_run = session.scalars(select(DagRun).where(DagRun.dag_id == consumer_dag.dag_id)).one()
        assert created_run.state == State.QUEUED
        expected = {new_event.id} | ({old_event.id} if expects_old_event else set())
        assert {e.id for e in created_run.consumed_asset_events} == expected

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

        asset1_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset1.uri))

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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        with create_session() as session:
            self.job_runner._create_dagruns_for_dags(session, session)

        def dict_from_obj(obj):
            """Get dict of column attrs from SqlAlchemy object."""
            return {k.key: obj.__dict__.get(k) for k in obj.__mapper__.column_attrs}

        created_run = session.scalars(select(DagRun).where(DagRun.dag_id == consumer_dag.dag_id)).one()
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

    @pytest.mark.parametrize(
        ("multi_team", "expect_team_tag"),
        [
            pytest.param("true", True, id="with_team"),
            pytest.param("false", False, id="without_team"),
        ],
    )
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_asset_triggered_dagruns_respects_team_name(
        self, mock_get_backend, multi_team, expect_team_tag, session, dag_maker
    ):
        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats

        suffix = "with_team" if expect_team_tag else "without_team"

        team_name = f"team_asset_trig_{suffix}"
        team = Team(name=team_name)
        session.add(team)
        session.flush()

        bundle_name = f"bundle_asset_trig_{suffix}"
        bundle = DagBundleModel(name=bundle_name)
        bundle.teams.append(team)
        session.add(bundle)
        session.commit()

        asset_name = f"test_team_asset_{suffix}"
        asset = Asset(uri=f"test://{asset_name}", name=asset_name, group="test_group")
        with dag_maker(dag_id=f"producer_{suffix}", bundle_name=bundle_name, session=session):
            BashOperator(task_id="task", bash_command="echo 1", outlets=[asset])
        dr = dag_maker.create_dagrun()

        asset_id = session.scalar(select(AssetModel.id).where(AssetModel.uri == asset.uri))
        event = AssetEvent(
            asset_id=asset_id,
            source_task_id="task",
            source_dag_id=dr.dag_id,
            source_run_id=dr.run_id,
            source_map_index=-1,
        )
        session.add(event)

        with dag_maker(
            dag_id=f"consumer_{suffix}", schedule=[asset], bundle_name=bundle_name, session=session
        ):
            pass

        session.add(AssetDagRunQueue(asset_id=asset_id, target_dag_id=f"consumer_{suffix}"))
        session.flush()

        with conf_vars({("core", "multi_team"): multi_team}):
            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])
            self.job_runner._create_dagruns_for_dags(session, session)

        if expect_team_tag:
            mock_stats.incr.assert_any_call("asset.triggered_dagruns", tags={"team_name": team_name})
        else:
            mock_stats.incr.assert_any_call("asset.triggered_dagruns")

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

        asset_id = session.scalars(
            select(AssetModel.id).where(AssetModel.uri == asset.uri, AssetModel.name == asset.name)
        ).one()
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
        if "is_stale" in disable:
            assert session.scalars(adrq_q).one_or_none() is not None
        else:
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
        assert len(session.scalars(adrq_q).all()) == 1
        assert session.scalars(adrq_q).one().target_dag_id == "consumer"

    @time_machine.travel(DEFAULT_DATE + datetime.timedelta(days=1, seconds=9), tick=False)
    @mock.patch("airflow._shared.observability.metrics.stats._get_backend")
    def test_start_dagruns(self, mock_get_backend, dag_maker, session):
        """
        Test that _start_dagrun:

        - moves runs to RUNNING State
        - emit the right DagRun metrics
        """
        from airflow.models.dag import get_last_dagrun

        with dag_maker(dag_id="test_start_dag_runs") as dag:
            EmptyOperator(task_id="dummy")

        dag_model = dag_maker.dag_model

        mock_stats = mock.MagicMock(spec=StatsLogger)
        mock_get_backend.return_value = mock_stats
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        self.job_runner._create_dag_runs([dag_model], session)
        self.job_runner._start_queued_dagruns(session)

        dr = session.scalars(select(DagRun).where(DagRun.dag_id == dag.dag_id)).first()
        # Assert dr state is running
        assert dr.state == State.RUNNING

        mock_stats.timing.assert_has_calls(
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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

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
            scheduler_messages = [
                record.message for record in caplog.records if record.levelno >= logging.ERROR
            ]
            assert scheduler_messages == ["Dag not found in serialized_dag table"]

    def _clear_serdags(self, dag_id, session):
        SDM = SerializedDagModel
        sdms = session.scalars(select(SDM).where(SDM.dag_id == dag_id))
        for sdm in sdms:
            session.delete(sdm)
        session.commit()

    def test_scheduler_create_dag_runs_does_not_crash_on_deserialization_error(self, caplog, dag_maker):
        """
        Test that scheduler._create_dag_runs does not crash when DAG deserialization fails.
        This is a guardrail to ensure the scheduler continues processing other DAGs even if
        one DAG has a deserialization error.
        """
        with dag_maker(dag_id="test_scheduler_create_dag_runs_deserialization_error"):
            EmptyOperator(task_id="dummy")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        caplog.set_level("FATAL")
        caplog.clear()
        with (
            create_session() as session,
            caplog.at_level(
                "ERROR",
                logger="airflow.jobs.scheduler_job_runner",
            ),
            patch(
                "airflow.models.serialized_dag.SerializedDagModel.get",
                side_effect=Exception("Simulated deserialization error"),
            ),
        ):
            self.job_runner._create_dag_runs([dag_maker.dag_model], session)
            scheduler_messages = [
                record.message for record in caplog.records if record.levelno >= logging.ERROR
            ]
            assert any("Failed to deserialize DAG" in msg for msg in scheduler_messages), (
                f"Expected deserialization error log, got: {scheduler_messages}"
            )

    def test_schedule_all_dag_runs_does_not_crash_on_single_dag_run_error(self, dag_maker, caplog, session):
        """Test that _schedule_all_dag_runs continues processing other DAG runs
        when one DAG run raises an exception during scheduling.

        Previously, _schedule_all_dag_runs used a list comprehension that would
        abort entirely if any single _schedule_dag_run call raised, crashing
        the entire scheduler and stopping scheduling for ALL DAGs.

        While the specific scenario used to reproduce this (a TaskInstance with
        state=UP_FOR_RETRY and end_date=NULL) is nearly impossible under normal
        operation, the lack of per-dag-run fault isolation means ANY unexpected
        exception from ANY dag run would have the same catastrophic effect.
        """
        # Create two DAGs with running DAG runs
        with dag_maker(dag_id="good_dag", schedule="@once"):
            EmptyOperator(task_id="good_task")
        good_run = dag_maker.create_dagrun(state=DagRunState.RUNNING)

        with dag_maker(dag_id="bad_dag", schedule="@once"):
            EmptyOperator(task_id="bad_task")
        bad_run = dag_maker.create_dagrun(state=DagRunState.RUNNING)

        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        caplog.clear()
        with (
            caplog.at_level("ERROR", logger="airflow.jobs.scheduler_job_runner"),
            patch.object(
                self.job_runner,
                "_schedule_dag_run",
                autospec=True,
                side_effect=[
                    TypeError("simulated crash from corrupted task instance"),  # bad_run
                    None,  # good_run
                ],
            ) as mock_schedule,
        ):
            from airflow.utils.sqlalchemy import prohibit_commit

            with prohibit_commit(session) as guard:
                result = self.job_runner._schedule_all_dag_runs(guard, [bad_run, good_run], session=session)

            # The good DAG run should have been processed despite the bad one failing
            assert len(result) == 1
            assert result[0][0] == good_run

            # Both dag runs should have been attempted
            assert mock_schedule.call_count == 2

            # The error should have been logged
            error_messages = [r.message for r in caplog.records if r.levelno >= logging.ERROR]
            assert any(
                msg == f"Error scheduling DAG run {bad_run.run_id} of {bad_run.dag_id}"
                for msg in error_messages
            )

    def test_schedule_all_dag_runs_reraises_db_errors(self, dag_maker, session):
        """Test that _schedule_all_dag_runs does not catch DBAPIError, allowing
        it to propagate to @retry_db_transaction for proper retry handling.
        """
        from sqlalchemy.exc import DBAPIError

        with dag_maker(dag_id="db_error_dag", schedule="@once"):
            EmptyOperator(task_id="task1")
        run = dag_maker.create_dagrun(state=DagRunState.RUNNING)
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        with patch.object(
            self.job_runner,
            "_schedule_dag_run",
            autospec=True,
            side_effect=DBAPIError("select 1", None, Exception("connection lost")),
        ) as mock_schedule:
            from airflow.utils.sqlalchemy import prohibit_commit

            with prohibit_commit(session) as guard:
                # Bypass @retry_db_transaction to verify the exception escapes
                # the inner function rather than being swallowed by except Exception.
                with pytest.raises(DBAPIError):
                    self.job_runner._schedule_all_dag_runs.__wrapped__(
                        self.job_runner, guard, [run], session=session
                    )

            assert mock_schedule.call_count == 1

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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

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

        run1_ti = run1.get_task_instance(task1.task_id, session=session)
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

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
        run2_ti = run2.get_task_instance(task1.task_id, session=session)
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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[executor])

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

    def test_more_runs_are_not_created_when_max_active_runs_is_reached(self, dag_maker, caplog, session):
        """
        This tests that when max_active_runs is reached, _create_dag_runs doesn't create
        more dagruns
        """
        # Explicitly set catchup=True as test specifically expects historical dates to be respected
        with dag_maker(max_active_runs=1, catchup=True):
            EmptyOperator(task_id="task")
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

        assert session.scalar(select(func.count()).select_from(DagRun)) == 0
        query, _ = DagModel.dags_needing_dagruns(session)
        dag_models = query.all()
        self.job_runner._create_dag_runs(dag_models, session)
        dr = session.scalars(select(DagRun)).one()
        assert dr.state == DagRunState.QUEUED
        assert session.scalar(select(func.count()).select_from(DagRun)) == 1
        assert dag_maker.dag_model.next_dagrun_create_after == DEFAULT_DATE + timedelta(days=2)
        assert dag_maker.dag_model.next_dagrun == DEFAULT_DATE + timedelta(days=1)
        session.flush()
        # dags_needing_dagruns query should not return any value
        query, _ = DagModel.dags_needing_dagruns(session)
        assert len(query.all()) == 0
        self.job_runner._create_dag_runs(dag_models, session)
        assert session.scalar(select(func.count()).select_from(DagRun)) == 1
        assert dag_maker.dag_model.next_dagrun_create_after == DEFAULT_DATE + timedelta(days=2)
        assert dag_maker.dag_model.next_dagrun == DEFAULT_DATE + timedelta(days=1)
        # set dagrun to success
        dr = session.scalars(select(DagRun)).one()
        dr.state = DagRunState.SUCCESS
        ti = dr.get_task_instance("task", session=session)
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
            session.scalar(
                select(func.count())
                .select_from(DagRun)
                .where(DagRun.state.in_([DagRunState.RUNNING, DagRunState.QUEUED]))
            )
            == 0
        )

    def test_max_active_runs_creation_phasing(self, dag_maker, session):
        """
        Test that when creating runs once max_active_runs is reached that the runs come in the right order
        without gaps
        """

        def complete_one_dagrun():
            ti = session.scalars(
                select(TaskInstance)
                .join(TaskInstance.dag_run)
                .where(TaskInstance.state != State.SUCCESS)
                .order_by(DagRun.logical_date)
            ).first()
            if ti:
                ti.state = State.SUCCESS
                session.flush()

        _clean_db()

        # Explicitly set catchup=True as test specifically expects runs to be created in date order
        with dag_maker(max_active_runs=3, session=session, catchup=True) as dag:
            # Need to use something that doesn't immediately get marked as success by the scheduler
            BashOperator(task_id="task", bash_command="true")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        query, _ = DagModel.dags_needing_dagruns(session)
        query.all()
        for _ in range(3):
            self.job_runner._do_scheduling(session)

        model: DagModel = session.get(DagModel, dag.dag_id)

        # Pre-condition
        assert DagRun.active_runs_of_dags(dag_ids=["test_dag"], exclude_backfill=True, session=session) == {
            "test_dag": 3
        }

        assert model.next_dagrun == timezone.DateTime(2016, 1, 4, tzinfo=timezone.utc)
        assert model.next_dagrun_create_after == timezone.DateTime(2016, 1, 5, tzinfo=timezone.utc)

        complete_one_dagrun()

        assert DagRun.active_runs_of_dags(dag_ids=["test_dag"], exclude_backfill=True, session=session) == {
            "test_dag": 3
        }

        for _ in range(5):
            self.job_runner._do_scheduling(session)
            complete_one_dagrun()

        expected_logical_dates = [datetime.datetime(2016, 1, d, tzinfo=timezone.utc) for d in range(1, 8)]
        dagrun_logical_dates = [
            dr.logical_date for dr in session.scalars(select(DagRun).order_by(DagRun.logical_date)).all()
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

        self.job_runner._start_queued_dagruns(session)
        session.flush()
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_running_count = session.scalar(
            select(func.count(DagRun.id)).where(DagRun.dag_id == "test_dag1", DagRun.state == State.RUNNING)
        )
        running_count = session.scalar(select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING))
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

        self.job_runner._start_queued_dagruns(session)
        session.flush()
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_running_count = session.scalar(
            select(func.count(DagRun.id)).where(DagRun.dag_id == "test_dag1", DagRun.state == State.RUNNING)
        )
        running_count = session.scalar(select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING))
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
        dag1_running_count = session.scalar(
            select(func.count(DagRun.id)).where(DagRun.dag_id == "test_dag1", DagRun.state == State.RUNNING)
        )
        assert dag1_running_count == 1
        total_running_count = session.scalar(
            select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING)
        )
        assert total_running_count == 11

        # scheduler will now mark backfill runs as running
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dag1_running_count = session.scalar(
            select(func.count(DagRun.id)).where(
                DagRun.dag_id == dag1_dag_id,
                DagRun.state == State.RUNNING,
            )
        )
        assert dag1_running_count == 4
        total_running_count = session.scalar(
            select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING)
        )
        assert total_running_count == 14

        # and doing it again does not change anything
        self.job_runner._start_queued_dagruns(session)
        session.flush()
        dag1_running_count = session.scalar(
            select(func.count(DagRun.id)).where(
                DagRun.dag_id == dag1_dag_id,
                DagRun.state == State.RUNNING,
            )
        )
        assert dag1_running_count == 4
        total_running_count = session.scalar(
            select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING)
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

        self.job_runner._start_queued_dagruns(session)
        session.flush()
        self.job_runner._start_queued_dagruns(session)
        session.flush()

        dag1_running_count = session.scalar(
            select(func.count(DagRun.id)).where(DagRun.dag_id == "test_dag1", DagRun.state == State.RUNNING)
        )
        running_count = session.scalar(select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING))
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
        dag1_running_count = session.scalar(
            select(func.count(DagRun.id)).where(
                DagRun.dag_id == dag1_dag_id,
                DagRun.state == State.RUNNING,
            )
        )
        # Even with catchup=False, backfill runs should start
        assert dag1_running_count == 4
        total_running_count = session.scalar(
            select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING)
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
            dag1_non_b_running = session.scalar(
                select(func.count(DagRun.id)).where(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
            )
            dag1_b_running = session.scalar(
                select(func.count(DagRun.id)).where(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                )
            )
            total_running_count = session.scalar(
                select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING)
            )
            return dag1_non_b_running, dag1_b_running, total_running_count

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

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
        assert session.scalar(select(func.count(DagRun.id))) == 46
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
            dag1_non_b_running = session.scalar(
                select(func.count(DagRun.id)).where(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
            )
            dag1_b_running = session.scalar(
                select(func.count(DagRun.id)).where(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                )
            )
            total_running_count = session.scalar(
                select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING)
            )
            return dag1_non_b_running, dag1_b_running, total_running_count

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

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
        assert session.scalar(select(func.count(DagRun.id))) == 14

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
        assert session.scalar(select(func.count(DagRun.id))) == 14

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
            dag1_non_b_running = session.scalar(
                select(func.count(DagRun.id)).where(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
            )
            dag1_b_running = session.scalar(
                select(func.count(DagRun.id)).where(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                )
            )
            total_running_count = session.scalar(
                select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING)
            )
            return dag1_non_b_running, dag1_b_running, total_running_count

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

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
        assert session.scalar(select(func.count(DagRun.id))) == 6
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
            dag1_non_b_running = session.scalar(
                select(func.count(DagRun.id)).where(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type != DagRunType.BACKFILL_JOB,
                )
            )
            dag1_b_running = session.scalar(
                select(func.count(DagRun.id)).where(
                    DagRun.dag_id == dag1_dag_id,
                    DagRun.state == State.RUNNING,
                    DagRun.run_type == DagRunType.BACKFILL_JOB,
                )
            )
            total_running_count = session.scalar(
                select(func.count(DagRun.id)).where(DagRun.state == State.RUNNING)
            )
            return dag1_non_b_running, dag1_b_running, total_running_count

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

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
        assert session.scalar(select(func.count(DagRun.id))) == 6
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

        queued_count = session.scalar(
            select(func.count(DagRun.id)).where(
                DagRun.dag_id == dag_id,
                DagRun.state == State.QUEUED,
                DagRun.run_type == DagRunType.BACKFILL_JOB,
            )
        )
        assert queued_count == 5

        scheduler_job = Job()
        job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

        # Simulate another scheduler holding the lock by returning empty from _lock_backfills
        with patch.object(job_runner, "_lock_backfills", return_value={}):
            job_runner._start_queued_dagruns(session)
            session.flush()

        # No runs should be started because we couldn't acquire the lock
        running_count = session.scalar(
            select(func.count(DagRun.id)).where(
                DagRun.dag_id == dag_id,
                DagRun.state == State.RUNNING,
                DagRun.run_type == DagRunType.BACKFILL_JOB,
            )
        )
        assert running_count == 0, f"Expected 0 running when lock not acquired, but got {running_count}. "
        # no locks now:
        job_runner._start_queued_dagruns(session)
        session.flush()

        running_count = session.scalar(
            select(func.count(DagRun.id)).where(
                DagRun.dag_id == dag_id,
                DagRun.state == State.RUNNING,
                DagRun.run_type == DagRunType.BACKFILL_JOB,
            )
        )
        assert running_count == backfill_max_active_runs
        queued_count = session.scalar(
            select(func.count(DagRun.id)).where(
                DagRun.dag_id == dag_id,
                DagRun.state == State.QUEUED,
                DagRun.run_type == DagRunType.BACKFILL_JOB,
            )
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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

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
        assert len(session.scalars(select(DagRun).where(DagRun.state == State.RUNNING)).all()) == 1

        assert dr[0].state == State.RUNNING

    @conf_vars({("scheduler", "use_job_schedule"): "False"})
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
                last_automated_run_info=DagRunInfo(
                    run_after=dr1_running.run_after,
                    data_interval=data_interval,
                    partition_date=None,
                    partition_key=None,
                ),
                restricted=False,
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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor(do_update=False)])

        dag_version = DagVersion.get_latest_version(dag_id=dag.dag_id)
        ti = create_task_instance(task=task1, run_id=dr1_running.run_id, dag_version_id=dag_version.id)
        ti.refresh_from_db()
        ti.state = State.SUCCESS
        session.merge(ti)
        session.flush()
        # Run the scheduler loop
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
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(TaskInstance)
                    .where(TaskInstance.state == State.SCHEDULED)
                )
                == 1
            )

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
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(TaskInstance)
                    .where(TaskInstance.state == State.SCHEDULED)
                )
                == 1
            )

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
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(TaskInstance)
                    .where(TaskInstance.state == State.SCHEDULED)
                )
                == 1
            )

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
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(TaskInstance)
                    .where(TaskInstance.state == State.SCHEDULED)
                )
                == 2
            )

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
            session.scalars(
                select(DagRun).options(joinedload(DagRun.task_instances).joinedload(TaskInstance.dag_version))
            )
            # The unique() method must be invoked on this Result, as it contains results that include
            # joined eager loads against collections
            .unique()
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
        assert (
            session.scalar(
                select(func.count()).select_from(TaskInstance).where(TaskInstance.state == State.SCHEDULED)
            )
            == 2
        )
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
        ti1 = dr1.get_task_instance("dummy1", session=session)
        ti2 = dr2.get_task_instance("dummy1", session=session)
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

    def test_awaiting_input_timeout_with_defaults_resumes(self, dag_maker):
        """
        A parked ``awaiting_input`` task past its deadline with defaults is resumed to SCHEDULED by
        the scheduler sweep (the default applied as the response), while a not-yet-expired one is
        left untouched. This proves HITL timeout liveness with no triggerer running.
        """
        session = settings.Session()
        with dag_maker(
            dag_id="test_awaiting_input_defaults",
            start_date=DEFAULT_DATE,
            schedule="@once",
            session=session,
        ):
            EmptyOperator(task_id="dummy1")
        dr_past = dag_maker.create_dagrun()
        dr_future = dag_maker.create_dagrun(
            run_id="future", logical_date=DEFAULT_DATE + datetime.timedelta(seconds=1)
        )
        ti_past = dr_past.get_task_instance("dummy1", session=session)
        ti_future = dr_future.get_task_instance("dummy1", session=session)
        for ti, offset in ((ti_past, -60), (ti_future, 60)):
            ti.state = State.AWAITING_INPUT
            ti.trigger_timeout = timezone.utcnow() + datetime.timedelta(seconds=offset)
            ti.next_method = "execute_complete"
            ti.next_kwargs = {}
            session.add(
                HITLDetail(
                    ti_id=ti.id,
                    options=["Approve", "Reject"],
                    subject="approve?",
                    defaults=["Approve"],
                    multiple=False,
                    params={},
                )
            )
        session.flush()

        self.job_runner = SchedulerJobRunner(job=Job())
        self.job_runner.check_awaiting_input_timeouts(session=session)

        session.refresh(ti_past)
        session.refresh(ti_future)
        # Past-deadline task resumed with the default applied; the future one is left parked.
        assert ti_past.state == State.SCHEDULED
        assert ti_past.next_method == "execute_complete"
        assert ti_past.next_kwargs["event"]["chosen_options"] == ["Approve"]
        assert ti_past.next_kwargs["event"]["timedout"] is True
        assert ti_future.state == State.AWAITING_INPUT
        hitl_detail = session.get(HITLDetail, ti_past.id)
        assert hitl_detail.chosen_options == ["Approve"]
        assert hitl_detail.responded_by is None
        assert hitl_detail.responded_at is not None

    def test_awaiting_input_timeout_without_defaults_fails(self, dag_maker):
        """A parked ``awaiting_input`` task past its deadline with no defaults is failed by the sweep."""
        session = settings.Session()
        with dag_maker(
            dag_id="test_awaiting_input_nodefaults",
            start_date=DEFAULT_DATE,
            schedule="@once",
            session=session,
        ):
            EmptyOperator(task_id="dummy1")
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("dummy1", session=session)
        ti.state = State.AWAITING_INPUT
        ti.trigger_timeout = timezone.utcnow() - datetime.timedelta(seconds=60)
        ti.next_method = "execute_complete"
        ti.next_kwargs = {}
        session.add(
            HITLDetail(
                ti_id=ti.id,
                options=["Approve", "Reject"],
                subject="approve?",
                defaults=None,
                multiple=False,
                params={},
            )
        )
        session.flush()

        self.job_runner = SchedulerJobRunner(job=Job())
        self.job_runner.check_awaiting_input_timeouts(session=session)

        session.refresh(ti)
        # Resumed into execute_complete with a timeout failure event (raises HITLTimeoutError on
        # resume), rather than the generic __fail__ deferral-timeout path.
        assert ti.state == State.SCHEDULED
        assert ti.next_method == "execute_complete"
        assert ti.next_kwargs["event"]["error_type"] == "timeout"

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
                ti1 = dr1.get_task_instance("dummy1", session=session)
                ti2 = dr2.get_task_instance("dummy1", session=session)
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
                _clean_db()

        # Positive case, will retry until success before reach max retry times
        check_if_trigger_timeout(retry_times)

        # Negative case: no retries, execute only once.
        with pytest.raises(OperationalError):
            check_if_trigger_timeout(1)

    def test_find_and_purge_task_instances_without_heartbeats_nothing(self):
        executor = MockExecutor(do_update=False)
        scheduler_job = Job()
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
        scheduler_job = Job()
        with mock.patch("airflow.executors.executor_loader.ExecutorLoader.load_executor") as loader_mock:
            loader_mock.return_value = executor
            self.job_runner = SchedulerJobRunner(job=scheduler_job)

            # We will provision 2 tasks so we can check we only find task instances without heartbeats from this scheduler
            tasks_to_setup = ["branching", "run_this_first"]

            for task_id in tasks_to_setup:
                task = dag.get_task(task_id=task_id)
                ti = create_task_instance(
                    task,
                    run_id=dag_run.run_id,
                    state=State.RUNNING,
                    dag_version_id=dag_v.id,
                )

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
        session.execute(delete(Job))

        data_interval = infer_automated_data_interval(scheduler_dag.timetable, DEFAULT_LOGICAL_DATE)
        dag_run = create_dagrun(
            scheduler_dag,
            logical_date=DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            data_interval=data_interval,
        )

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[MockExecutor()])

        # We will provision 2 tasks so we can check we only find task instance heartbeat timeouts from this scheduler
        tasks_to_setup = ["branching", "run_this_first"]
        dag_version = DagVersion.get_latest_version(dag.dag_id)
        for task_id in tasks_to_setup:
            task = dag.get_task(task_id=task_id)
            ti = create_task_instance(
                task,
                run_id=dag_run.run_id,
                state=State.RUNNING,
                dag_version_id=dag_version.id,
            )
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

    @conf_vars({("scheduler", "use_job_schedule"): "False"})
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

        dagbag = DagBag(dag_folder=TEST_DAGS_FOLDER)
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

        job = Job()
        self.job_runner = SchedulerJobRunner(job=job, executors=[executor])
        self.run_scheduler_until_dagrun_terminal()

        dr.refresh_from_db(session)
        assert dr.state == DagRunState.SUCCESS

    def test_should_mark_empty_task_as_success(self, testing_dag_bundle):
        dag_file = Path(__file__).parents[1] / "dags/test_only_empty_tasks.py"

        # Write DAGs to dag and serialized_dag table
        dagbag = DagBag(dag_folder=dag_file)
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
            tis = session.scalars(select(TaskInstance)).all()

        dags = [entry.dag for entry in self.job_runner.scheduler_dag_bag._dags.values()]
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
            tis = session.scalars(select(TaskInstance)).all()

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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

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
            session.scalar(
                select(DagRun.logical_date).where(
                    DagRun.logical_date != DEFAULT_DATE
                )  # exclude the first run
            )
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
        dm = DagModel.get_dagmodel(dag.dag_id, session=session)
        dm.is_paused = True
        session.flush()

        assert scheduled_run.state == State.RUNNING

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

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

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

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
                "airflow.providers.standard.triggers.file.FileDeleteTrigger",
                id="active",
            ),
            pytest.param(False, True, None, id="stale"),
            pytest.param(True, False, None, id="paused"),
            pytest.param(True, False, None, id="stale-paused"),
        ],
    )
    @pytest.mark.need_serialized_dag(False)
    def test_delete_unreferenced_triggers(self, dag_maker, session, paused, stale, expected_classpath):
        trigger = FileDeleteTrigger(mock.Mock())
        classpath = "airflow.providers.standard.triggers.file.FileDeleteTrigger"

        self.job_runner = SchedulerJobRunner(job=Job())

        asset1 = Asset(name="test_asset_1", watchers=[AssetWatcher(name="test", trigger=trigger)])
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

    @pytest.mark.need_serialized_dag(False)
    def test_delete_unreferenced_triggers_with_null_trigger_id_ti(self, dag_maker, session):
        """Unreferenced triggers are deleted even when other TIs have ``trigger_id IS NULL``."""
        self.job_runner = SchedulerJobRunner(job=Job())

        with dag_maker(dag_id="dag", session=session):
            EmptyOperator(task_id="task")
        dag_maker.create_dagrun()

        unreferenced = Trigger(
            classpath="airflow.providers.standard.triggers.file.FileDeleteTrigger", kwargs={}
        )
        session.add(unreferenced)
        session.flush()

        self.job_runner._remove_unreferenced_triggers(session=session)
        assert session.scalar(select(func.count()).select_from(Trigger)) == 0

    @patch("airflow.serialization.serialized_objects.SerializedDAG.create_dagrun")
    def test_misconfigured_dags_doesnt_crash_scheduler(self, mock_create, session, dag_maker, caplog):
        """Test that if dagrun creation throws an exception, the scheduler doesn't crash"""
        mock_create.side_effect = [ValueError("something bad")]
        with dag_maker("testdag1", serialized=True):
            BashOperator(task_id="task", bash_command="echo 1")

        dm1 = dag_maker.dag_model
        session.add(dm1)
        session.flush()

        scheduler_job = Job()
        job_runner = SchedulerJobRunner(job=scheduler_job)
        # In the dagmodel list, the first dag should fail, but the second one should succeed
        job_runner._create_dag_runs([dm1], session)
        assert "Failed creating DagRun" in caplog.text

    def test_activate_referenced_assets_no_in_check_inside_query(self, session, testing_dag_bundle):
        dag_id1 = "test_asset_dag1"
        asset1_name = "asset1"
        asset_extra = {"foo": "bar"}

        asset1 = Asset(name=asset1_name, uri="s3://bucket/key/1", extra=asset_extra)
        dag1 = DAG(dag_id=dag_id1, start_date=DEFAULT_DATE, schedule=[asset1])
        sync_dag_to_db(dag1, session=session)

        @contextmanager
        def assert_no_in_clause(session):
            from sqlalchemy import event

            def fail_on_in_clause_found(execute_statement):
                if " IN " in str(execute_statement).upper():
                    execute_statement = str(execute_statement).upper()
                    pytest.fail(
                        f"Query contains IN clause which was removed in PR #62114, query: {execute_statement}"
                    )

            event.listen(session, "do_orm_execute", fail_on_in_clause_found)
            try:
                yield
            finally:
                event.remove(session, "do_orm_execute", fail_on_in_clause_found)

        asset_models = select(AssetModel).cte()

        with assert_no_in_clause(session):
            SchedulerJobRunner._activate_referenced_assets(asset_models, session=session)

    def test_activate_referenced_assets_with_no_existing_warning(self, session, testing_dag_bundle):
        dag_warnings = session.scalars(select(DagWarning)).all()
        assert dag_warnings == []

        dag_id1 = "test_asset_dag1"
        asset1_name = "asset1"
        asset_extra = {"foo": "bar"}

        asset1 = Asset(name=asset1_name, uri="s3://bucket/key/1", extra=asset_extra)
        asset1_1 = Asset(name=asset1_name, uri="it's duplicate", extra=asset_extra)
        asset1_2 = Asset(name="it's also a duplicate", uri="s3://bucket/key/1", extra=asset_extra)
        dag1 = DAG(dag_id=dag_id1, start_date=DEFAULT_DATE, schedule=[asset1, asset1_1, asset1_2])
        sync_dag_to_db(dag1, session=session)

        asset_models = select(AssetModel).cte()
        assert len(session.execute(select(asset_models)).all()) == 3

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

        asset_models = select(AssetModel).cte()

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

        asset_models = select(AssetModel).cte()

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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[mock_executor])

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
        ("state", "retries", "try_number", "expected_callback_type", "expected_dispatched_callback"),
        [
            pytest.param(
                TaskInstanceState.RUNNING,
                0,
                1,
                TaskInstanceState.FAILED,
                "on_failure_callback",
                id="no_retries",
            ),
            pytest.param(
                TaskInstanceState.RUNNING,
                2,
                1,
                TaskInstanceState.UP_FOR_RETRY,
                "on_retry_callback",
                id="retries_available_first_attempt",
            ),
            pytest.param(
                TaskInstanceState.RUNNING,
                2,
                2,
                TaskInstanceState.UP_FOR_RETRY,
                "on_retry_callback",
                id="retries_available_mid_chain",
            ),
            pytest.param(
                TaskInstanceState.RUNNING,
                2,
                3,
                TaskInstanceState.FAILED,
                "on_failure_callback",
                id="retries_exhausted",
            ),
            pytest.param(
                TaskInstanceState.RESTARTING,
                1,
                5,
                TaskInstanceState.UP_FOR_RETRY,
                "on_retry_callback",
                id="restarting_stays_eligible_past_max_tries",
            ),
        ],
    )
    def test_heartbeat_timeout_sets_callback_type_by_retry_eligibility(
        self,
        dag_maker,
        session,
        state,
        retries,
        try_number,
        expected_callback_type,
        expected_dispatched_callback,
    ):
        """Heartbeat-timeout cleanup must populate ``task_callback_type`` so the Dag processor
        fires ``on_retry_callback`` when the task still has retries left, not
        ``on_failure_callback``.

        Reproduces the bug end-to-end through the actual scheduler purge path:

        1. A TI is ``RUNNING`` (or ``RESTARTING``) with a stale ``last_heartbeat_at`` (worker
           OOMKilled, node evicted, scheduler restarted, etc.).
        2. ``_find_and_purge_task_instances_without_heartbeats`` builds a
           ``TaskCallbackRequest`` and hands it to the executor's ``send_callback``.
        3. The Dag processor branches on ``request.task_callback_type``:
           ``UP_FOR_RETRY`` -> ``task.on_retry_callback``; anything else (including ``None``)
           -> ``task.on_failure_callback``. See
           ``airflow-core/src/airflow/dag_processing/processor.py``::``_execute_task_callbacks``.

        Before the fix, step 2 left ``task_callback_type`` as ``None``, so step 3 always fell
        into the ``else`` branch and ``on_failure_callback`` fired even when the task still had
        retries left -- producing spurious failure alerts for tasks that ultimately succeeded on
        retry.

        The parametrized cases cover the full ``max_tries`` / ``try_number`` matrix for a
        ``RUNNING`` TI -- no retries, retries available (first attempt and mid-chain), and
        retries exhausted (``try_number > max_tries``) -- plus a ``RESTARTING`` TI (cleared
        while running), which ``is_eligible_to_retry`` keeps retry-eligible even past
        ``max_tries``. The ``expected_dispatched_callback`` column mirrors the Dag processor's
        branch so the assertion captures the user-visible outcome, not just the field value.
        """
        with dag_maker(dag_id=f"hb_timeout_r{retries}_t{try_number}", session=session):
            EmptyOperator(task_id="test_task", retries=retries)

        dag_run = dag_maker.create_dagrun(run_id="test_run", state=DagRunState.RUNNING)

        mock_executor = MagicMock()
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[mock_executor])

        ti = dag_run.get_task_instance(task_id="test_task")
        ti.state = state
        ti.try_number = try_number
        ti.queued_by_job_id = scheduler_job.id
        ti.last_heartbeat_at = timezone.utcnow() - timedelta(seconds=600)
        session.merge(ti)
        session.commit()

        self.job_runner._find_and_purge_task_instances_without_heartbeats()

        mock_executor.send_callback.assert_called_once()
        request = mock_executor.send_callback.call_args[0][0]
        assert isinstance(request, TaskCallbackRequest)
        assert request.task_callback_type == expected_callback_type
        # Mirror processor._execute_task_callbacks: UP_FOR_RETRY -> on_retry_callback, else
        # on_failure_callback. Asserting the dispatched callback closes the loop on the
        # user-visible behaviour, not just the field value.
        dispatched_callback = (
            "on_retry_callback"
            if request.task_callback_type is TaskInstanceState.UP_FOR_RETRY
            else "on_failure_callback"
        )
        assert dispatched_callback == expected_dispatched_callback

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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        ti.state = State.QUEUED
        session.merge(ti)
        session.commit()

        # Executor reports task finished (FAILED) while TI still QUEUED -> external kill path
        executor.event_buffer[ti.key] = State.FAILED, None

        self.job_runner._process_executor_events(executor=executor, session=session)

        self.job_runner.executor.callback_sink.send.assert_called()
        request = self.job_runner.executor.callback_sink.send.call_args[0][0]
        assert isinstance(request, TaskCallbackRequest)
        assert request.task_callback_type == expected

    @pytest.mark.parametrize(
        ("dag_run_bv", "dag_version_bv", "expected_bv"),
        [
            pytest.param(None, "abc123-sha", None, id="disable_bundle_versioning"),
            pytest.param("abc123-sha", "abc123-sha", "abc123-sha", id="versioning_enabled"),
        ],
    )
    def test_external_kill_callback_bundle_version_follows_dag_run(
        self, dag_maker, session, dag_run_bv, dag_version_bv, expected_bv
    ):
        """
        TaskCallbackRequest.bundle_version must mirror dag_run.bundle_version, not
        dag_version.bundle_version. With disable_bundle_versioning=True the trigger
        path leaves dag_run.bundle_version=None even though DagVersion was written
        with a SHA — the callback must inherit None so it runs against the same
        on-disk code as the task did.
        """
        with dag_maker(dag_id=f"ext_kill_bv_{dag_run_bv or 'none'}", fileloc="/test_path1/"):
            EmptyOperator(task_id="t1", on_failure_callback=lambda ctx: None)
        dr = dag_maker.create_dagrun(state=DagRunState.RUNNING)

        ti = dr.get_task_instance(task_id="t1", session=session)
        dag_version = ti.dag_version
        dag_version.bundle_version = dag_version_bv
        dr.bundle_version = dag_run_bv
        ti.state = State.QUEUED
        session.merge(dag_version)
        session.merge(dr)
        session.merge(ti)
        session.commit()

        executor = MockExecutor(do_update=False)
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])
        executor.event_buffer[ti.key] = State.FAILED, None

        self.job_runner._process_executor_events(executor=executor, session=session)

        self.job_runner.executor.callback_sink.send.assert_called_once()
        request = self.job_runner.executor.callback_sink.send.call_args[0][0]
        assert isinstance(request, TaskCallbackRequest)
        assert request.bundle_version == expected_bv

    def test_heartbeat_timeout_callback_bundle_version_follows_dag_run(self, dag_maker, session):
        """
        Same invariant as the external-kill path, exercised through
        _find_and_purge_task_instances_without_heartbeats.
        """
        with dag_maker(dag_id="hb_timeout_bv", fileloc="/test_path1/"):
            EmptyOperator(task_id="t1")
        dr = dag_maker.create_dagrun(state=DagRunState.RUNNING)

        executor = MagicMock()
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[executor])

        ti = dr.get_task_instance(task_id="t1", session=session)
        dag_version = ti.dag_version
        # disable_bundle_versioning state: DagVersion has a SHA, dag_run is unpinned.
        dag_version.bundle_version = "abc123-sha"
        dr.bundle_version = None
        ti.state = TaskInstanceState.RUNNING
        ti.queued_by_job_id = scheduler_job.id
        ti.last_heartbeat_at = timezone.utcnow() - timedelta(seconds=600)
        session.merge(dag_version)
        session.merge(dr)
        session.merge(ti)
        session.commit()

        self.job_runner._find_and_purge_task_instances_without_heartbeats()

        executor.send_callback.assert_called_once()
        request = executor.send_callback.call_args[0][0]
        assert isinstance(request, TaskCallbackRequest)
        assert request.bundle_version is None

    @conf_vars({("scheduler", "num_stuck_in_queued_retries"): "1"})
    def test_stuck_in_queued_callback_bundle_version_follows_dag_run(
        self, dag_maker, session, mock_executors
    ):
        """
        Same invariant as the external-kill path, exercised through
        _handle_tasks_stuck_in_queued. With num_stuck_in_queued_retries=1, the
        first stuck detection exhausts the budget and emits the failure callback.
        """
        with dag_maker(dag_id="stuck_bv"):
            EmptyOperator(task_id="op1", on_failure_callback=TestSchedulerJob.mock_failure_callback)
        run_id = str(uuid4())
        dr = dag_maker.create_dagrun(run_id=run_id, state=DagRunState.RUNNING)

        ti = dr.get_task_instance(task_id="op1", session=session)
        dag_version = ti.dag_version
        dag_version.bundle_version = "abc123-sha"
        dr.bundle_version = None
        ti.state = State.QUEUED
        ti.queued_dttm = timezone.utcnow()
        session.merge(dag_version)
        session.merge(dr)
        session.merge(ti)
        session.commit()

        scheduler_job = Job()
        scheduler = SchedulerJobRunner(job=scheduler_job, num_runs=0)
        scheduler._task_queued_timeout = -300  # always in violation

        # First sweep: reschedule (budget consumed). Re-queue and sweep again to exceed.
        with _loader_mock(mock_executors):
            scheduler._handle_tasks_stuck_in_queued()
        ti = dr.get_task_instance(task_id="op1", session=session)
        ti.state = State.QUEUED
        ti.queued_dttm = timezone.utcnow()
        session.merge(ti)
        session.commit()
        with _loader_mock(mock_executors):
            scheduler._handle_tasks_stuck_in_queued()

        mock_executors[0].send_callback.assert_called_once()
        request = mock_executors[0].send_callback.call_args[0][0]
        assert isinstance(request, TaskCallbackRequest)
        assert request.bundle_version is None

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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[mock_executor])

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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[mock_executor])

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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[mock_executor])

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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(scheduler_job, executors=[mock_executor])

        self.job_runner._schedule_dag_run(dag_run, session)

        # Verify that the listener hook was called with msg="timed_out"
        mock_listener_manager.hook.on_dag_run_failed.assert_called_once()
        call_args = mock_listener_manager.hook.on_dag_run_failed.call_args
        assert call_args.kwargs["msg"] == "timed_out"
        assert call_args.kwargs["dag_run"] == dag_run

    @mock.patch("airflow.models.Deadline.handle_miss")
    def test_process_expired_deadlines(self, mock_handle_miss, session, dag_maker):
        """Verify all expired and unhandled deadlines (and only those) are processed by the scheduler."""
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1, executors=[MockExecutor()])

        past_date = timezone.utcnow() - timedelta(minutes=5)
        future_date = timezone.utcnow() + timedelta(minutes=5)
        callback_path = "classpath.notify"

        # Create a test Dag run for Deadline
        dag_id = "test_deadline_dag"
        with dag_maker(dag_id=dag_id):
            EmptyOperator(task_id="empty")
        dagrun_id = dag_maker.create_dagrun().id

        serialized_dag = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id))
        assert serialized_dag is not None

        # Create a test DeadlineAlert object for Deadline
        deadline_alert = DeadlineAlert(
            serialized_dag_id=serialized_dag.id,
            name="Test Alert",
            reference={"type": "dag", "dag_id": dag_id},
            interval=300.0,  # 5 minutes
            callback_def={"classpath": callback_path, "kwargs": {}},
        )
        session.add(deadline_alert)
        session.flush()

        handled_deadline_async = Deadline(
            deadline_time=past_date,
            callback=AsyncCallback(callback_path),
            dagrun_id=dagrun_id,
            dag_id=dag_id,
            deadline_alert_id=deadline_alert.id,
        )
        handled_deadline_async.missed = True

        handled_deadline_sync = Deadline(
            deadline_time=past_date,
            callback=SyncCallback(callback_path),
            dagrun_id=dagrun_id,
            dag_id=dag_id,
            deadline_alert_id=deadline_alert.id,
        )
        handled_deadline_sync.missed = True

        expired_deadline1 = Deadline(
            deadline_time=past_date,
            callback=AsyncCallback(callback_path),
            dagrun_id=dagrun_id,
            dag_id=dag_id,
            deadline_alert_id=deadline_alert.id,
        )
        expired_deadline2 = Deadline(
            deadline_time=past_date,
            callback=SyncCallback(callback_path),
            dagrun_id=dagrun_id,
            dag_id=dag_id,
            deadline_alert_id=deadline_alert.id,
        )
        future_deadline = Deadline(
            deadline_time=future_date,
            callback=AsyncCallback(callback_path),
            dagrun_id=dagrun_id,
            dag_id=dag_id,
            deadline_alert_id=deadline_alert.id,
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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1, executors=[MockExecutor()])

        self.job_runner._execute()

        # The handler should not be called, but no exceptions should be raised either.`
        mock_handle_miss.assert_not_called()

    @mock.patch("airflow.models.Deadline.handle_miss")
    def test_expired_deadline_locked_by_other_scheduler_is_skipped(
        self, mock_handle_miss, session, dag_maker
    ):
        """The scheduler's deadline loop must skip rows another replica already holds."""
        if session.get_bind().dialect.name == "sqlite":
            pytest.skip("SQLite does not support row-level locking (SKIP LOCKED)")

        past_date = timezone.utcnow() - timedelta(minutes=5)
        dag_id = "test_deadline_locked_by_other_scheduler"
        callback_path = "classpath.notify"

        with dag_maker(dag_id=dag_id):
            EmptyOperator(task_id="empty")
        dagrun_id = dag_maker.create_dagrun().id

        serialized_dag = session.scalar(select(SerializedDagModel).where(SerializedDagModel.dag_id == dag_id))
        assert serialized_dag is not None

        deadline_alert = DeadlineAlert(
            serialized_dag_id=serialized_dag.id,
            name="Test Skip Locked",
            reference={"type": "dag", "dag_id": dag_id},
            interval=300.0,
            callback_def={"classpath": callback_path, "kwargs": {}},
        )
        session.add(deadline_alert)
        session.flush()

        session.add(
            Deadline(
                deadline_time=past_date,
                callback=AsyncCallback(callback_path),
                dagrun_id=dagrun_id,
                dag_id=dag_id,
                deadline_alert_id=deadline_alert.id,
            )
        )
        session.commit()

        # scoped=False gives an independent session with its own connection; the
        # default scoped_session would reuse this thread's session and locks held
        # by "self" do not block "self".
        with create_session(scoped=False) as competing_session:
            locked_rows = competing_session.scalars(
                with_row_locks(
                    select(Deadline).where(~Deadline.missed),
                    of=Deadline,
                    session=competing_session,
                    skip_locked=True,
                    key_share=False,
                )
            ).all()
            assert len(locked_rows) == 1

            scheduler_job = Job()
            self.job_runner = SchedulerJobRunner(job=scheduler_job, num_runs=1, executors=[MockExecutor()])
            self.job_runner._execute()

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

        monkeypatch.setattr("airflow._shared.observability.metrics.stats.gauge", _fake_gauge, raising=True)

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

    def test_multi_team_get_workload_team_name_success(self, dag_maker, session):
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
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._get_workload_team_name(ti, session=session)
        assert result == "team_a"

    def test_multi_team_get_workload_team_name_no_team(self, dag_maker, session):
        """Test team resolution when no team is associated with the DAG."""
        with dag_maker(dag_id="dag_no_team", session=session):
            task = EmptyOperator(task_id="task_no_team")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._get_workload_team_name(ti, session=session)
        assert result is None

    def test_multi_team_get_workload_team_name_database_error(self, dag_maker, session):
        """Test graceful error handling when individual task team resolution fails. This code should _not_ fail the scheduler."""
        with dag_maker(dag_id="dag_test", session=session):
            task = EmptyOperator(task_id="task_test")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Mock _get_team_names_for_dag_ids to return empty dict (simulates database error handling in that function)
        with mock.patch.object(self.job_runner, "_get_team_names_for_dag_ids", return_value={}) as mock_batch:
            result = self.job_runner._get_workload_team_name(ti, session=session)
            mock_batch.assert_called_once_with([ti.dag_id], session)

        # Should return None when batch function returns empty dict
        assert result is None

    @conf_vars({("core", "multi_team"): "false"})
    def test_multi_team_try_to_load_executor_multi_team_disabled(self, dag_maker, mock_executors, session):
        """Test executor selection when multi_team is disabled (legacy behavior)."""
        with dag_maker(dag_id="test_dag", session=session):
            task = EmptyOperator(task_id="test_task", executor="secondary_exec")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with mock.patch.object(self.job_runner, "_get_workload_team_name") as mock_team_resolve:
            result = self.job_runner._try_to_load_executor(ti, session=session)
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
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session=session)

        # Should return the global default executor (first executor in Job)
        assert result == self.job_runner.executor

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
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session=session)

        # Should return the team-specific default executor set above
        assert result == mock_executors[1]

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_try_to_load_executor_no_explicit_executor_with_team_no_team_default(
        self, dag_maker, mock_executors, session
    ):
        """Test executor selection when no explicit executor but team exists and team has no executors (should
        fallback to the global executor)."""
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
            task = EmptyOperator(task_id="test_task")  # No explicit executor

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session=session)

        # Should return the team-specific default executor set above
        assert result == mock_executors[0]

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
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session=session)

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
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session=session)

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
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with mock.patch("airflow.jobs.scheduler_job_runner.SchedulerJobRunner.log") as mock_log:
            result = self.job_runner._try_to_load_executor(ti, session)

            # Should log a warning when no executor is found
            mock_log.warning.assert_called_once_with(
                "Executor, %s, was not found but a Task or Callback was configured to use it",
                "secondary_exec",
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
        ti = dr.get_task_instance(task.task_id, session=session)

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
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        # Call with pre-resolved team name (as done in the scheduling loop)
        with mock.patch.object(self.job_runner, "_get_workload_team_name") as mock_team_resolve:
            result = self.job_runner._try_to_load_executor(ti, session=session, team_name="team_a")
            mock_team_resolve.assert_not_called()  # We don't query for the team if it is pre-resolved

        assert result == mock_executors[1]

    @conf_vars({("core", "multi_team"): "false"})
    def test_try_to_load_executor_matches_by_classname(self, dag_maker, mock_executors, session):
        """Test that executor lookup matches by classname when alias and module_path don't match.

        This covers the edge case where a user aliases a core executor (e.g.
        ``global_exec:LocalExecutor;team1=team_exec:LocalExecutor``) but a task specifies
        ``executor="LocalExecutor"`` (the classname). The scheduler should still find the
        executor by matching the last component of the module_path (the classname).
        """
        # Set up the mock executors with aliases that differ from the classname
        mock_executors[0].name = ExecutorName(
            alias="global_exec", module_path="airflow.executors.local_executor.LocalExecutor"
        )
        mock_executors[1].name = ExecutorName(
            alias="team_exec", module_path="airflow.executors.local_executor.LocalExecutor"
        )

        with dag_maker(dag_id="test_dag", session=session):
            task = EmptyOperator(task_id="test_task", executor="LocalExecutor")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        result = self.job_runner._try_to_load_executor(ti, session=session)

        # Should match by classname (last component of module_path) and return the global executor
        assert result == mock_executors[0]

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

        ti1 = dr1.get_task_instance("task_a", session=session)
        ti2 = dr2.get_task_instance("task_b", session=session)
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

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_executor_to_tis_batch_optimization(self, dag_maker, mock_executors, session):
        """Test that executor mapping batches team resolution for task instances."""
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

        ti1 = dr1.get_task_instance("task_a", session=session)
        ti2 = dr2.get_task_instance("task_b", session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with (
            assert_queries_count(1, session=session),
            mock.patch.object(self.job_runner, "_get_workload_team_name") as mock_single,
        ):
            executor_to_workloads = self.job_runner._executor_to_workloads([ti1, ti2], session=session)

            mock_single.assert_not_called()
            assert executor_to_workloads[mock_executors[0]] == [ti1]
            assert executor_to_workloads[mock_executors[1]] == [ti2]

    @conf_vars({("core", "multi_team"): "false"})
    def test_multi_team_config_disabled_uses_legacy_behavior(self, dag_maker, mock_executors, session):
        """Test that when multi_team config is disabled, legacy behavior is preserved."""
        with dag_maker(dag_id="test_dag", session=session):
            task1 = EmptyOperator(task_id="test_task1")  # No explicit executor
            task2 = EmptyOperator(task_id="test_task2", executor="secondary_exec")

        dr = dag_maker.create_dagrun()
        ti1 = dr.get_task_instance(task1.task_id, session=session)
        ti2 = dr.get_task_instance(task2.task_id, session=session)

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with mock.patch.object(self.job_runner, "_get_workload_team_name") as mock_team_resolve:
            result1 = self.job_runner._try_to_load_executor(ti1, session=session)
            result2 = self.job_runner._try_to_load_executor(ti2, session=session)

            # Should use legacy logic without calling team resolution
            mock_team_resolve.assert_not_called()
            assert result1 == self.job_runner.executor  # Default for no explicit executor
            assert result2 == mock_executors[1]  # Matched by executor name

    @conf_vars({("core", "multi_team"): "true"})
    def test_multi_team_sets_team_name_on_task_instances(self, dag_maker, mock_executors, session):
        """Test that _team_name is set on TaskInstance objects during the scheduling loop."""
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
            EmptyOperator(task_id="task_a")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance("task_a", session=session)
        ti.state = State.SCHEDULED
        session.flush()

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        self.job_runner._multi_team = True

        # Simulate what _executable_task_instances_to_queued does
        dag_id_to_team_name = self.job_runner._get_team_names_for_dag_ids(["dag_a"], session)
        if team_name := dag_id_to_team_name.get(ti.dag_id):
            ti._team_name = team_name

        assert ti._team_name == "team_a"
        assert ti.stats_tags == {"dag_id": "dag_a", "task_id": "task_a", "team_name": "team_a"}

    @conf_vars({("core", "multi_team"): "true"})
    def test_do_scheduling_multi_team_schedules_task_instances(self, dag_maker, session):
        """Test that _do_scheduling correctly schedules tasks when multi_team is enabled.

        Regression test: the multi-team code path used to consume the ScalarResult iterator
        (returned by get_running_dag_runs_to_examine) when building the team-name mapping,
        leaving an exhausted iterator for _schedule_all_dag_runs. This caused tasks to remain
        in None state indefinitely.
        """
        clear_db_teams()
        clear_db_dag_bundles()

        team = Team(name="team_a")
        session.add(team)
        session.flush()

        bundle = DagBundleModel(name="bundle_a")
        bundle.teams.append(team)
        session.add(bundle)
        session.flush()

        with dag_maker(dag_id="test_multi_team_scheduling", bundle_name="bundle_a", session=session):
            EmptyOperator(task_id="task1")

        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])

        dr = dag_maker.create_dagrun(state=State.RUNNING)
        ti = dr.get_task_instance("task1", session=session)
        assert ti.state == State.NONE

        self.job_runner._do_scheduling(session)

        ti = session.merge(ti)
        session.refresh(ti)
        assert ti.state != State.NONE


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
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job, executors=[self.null_exec])
        self.job_runner._create_dag_runs([dag_model], session)

        # Verify SerializedDAG has max_active_runs=1
        dag_run_1 = session.scalars(
            select(DagRun).where(DagRun.dag_id == dag.dag_id).order_by(DagRun.logical_date)
        ).first()
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
        running_count = session.scalar(
            select(func.count())
            .select_from(DagRun)
            .where(DagRun.dag_id == dag.dag_id, DagRun.state == DagRunState.RUNNING)
        )
        queued_count = session.scalar(
            select(func.count())
            .select_from(DagRun)
            .where(DagRun.dag_id == dag.dag_id, DagRun.state == DagRunState.QUEUED)
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
        running_count = session.scalar(
            select(func.count())
            .select_from(DagRun)
            .where(DagRun.dag_id == dag.dag_id, DagRun.state == DagRunState.RUNNING)
        )
        assert running_count == 2


class TestSchedulerJobQueriesCount:
    """
    These tests are designed to detect changes in the number of queries for
    different DAG files. These tests allow easy detection when a change is
    made that affects the performance of the SchedulerJob.
    """

    scheduler_job: Job | None

    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        _clean_db()

        yield

        _clean_db()

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
                    # For longer running tests under heavy load, the min_serialized_dag_update_interval
                    # might kick-in and re-retrieve the record.
                    # This will increase the count of serliazied_dag.py.get() count.
                    # That's why we keep the values high
                    ("core", "min_serialized_dag_update_interval"): "100",
                }
            ),
        ):
            dagruns = []
            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE)
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

            scheduler_job = Job()
            scheduler_job.heartbeat = mock.MagicMock()
            self.job_runner = SchedulerJobRunner(
                job=scheduler_job, num_runs=1, executors=[MockExecutor(do_update=False)]
            )

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
                    # For longer running tests under heavy load, the min_serialized_dag_update_interval
                    # might kick-in and re-retrieve the record.
                    # This will increase the count of serliazied_dag.py.get() count.
                    # That's why we keep the values high
                    ("core", "min_serialized_dag_update_interval"): "100",
                }
            ),
        ):
            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE)
            sync_bag_to_db(dagbag, "testing", None)

            scheduler_job = Job(job_type=SchedulerJobRunner.job_type)
            scheduler_job.heartbeat = mock.MagicMock()
            self.job_runner = SchedulerJobRunner(
                job=scheduler_job, num_runs=1, executors=[MockExecutor(do_update=False)]
            )

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
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
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


def test_mark_backfills_complete_skips_initializing_backfill(dag_maker, session):
    clear_db_backfills()
    dag_id = "test_backfill_race_lifecycle"
    with dag_maker(serialized=True, dag_id=dag_id, schedule="@daily"):
        BashOperator(task_id="hi", bash_command="echo hi")
    b = Backfill(
        dag_id=dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.NONE,
    )
    session.add(b)
    session.commit()
    backfill_id = b.id
    session.expunge_all()
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    runner._mark_backfills_complete()
    b = session.get(Backfill, backfill_id)
    assert b.completed_at is None
    session.expunge_all()
    dr = DagRun(
        dag_id=dag_id,
        run_id="backfill__2021-01-01T00:00:00+00:00",
        run_type=DagRunType.BACKFILL_JOB,
        logical_date=pendulum.parse("2021-01-01"),
        data_interval=(pendulum.parse("2021-01-01"), pendulum.parse("2021-01-02")),
        run_after=pendulum.parse("2021-01-02"),
        state=DagRunState.SUCCESS,
        backfill_id=backfill_id,
    )
    session.add(dr)
    session.flush()
    session.add(
        BackfillDagRun(
            backfill_id=backfill_id,
            dag_run_id=dr.id,
            logical_date=pendulum.parse("2021-01-01"),
            sort_ordinal=1,
        )
    )
    session.commit()
    session.expunge_all()
    runner._mark_backfills_complete()
    b = session.get(Backfill, backfill_id)
    assert b.completed_at is not None


def test_mark_backfills_complete_cleans_orphan_after_cutoff(dag_maker, session):
    """Backfill with no BackfillDagRun rows older than 2 minutes should be auto-completed."""
    clear_db_backfills()
    dag_id = "test_backfill_orphan_cleanup"
    with dag_maker(serialized=True, dag_id=dag_id, schedule="@daily"):
        BashOperator(task_id="hi", bash_command="echo hi")
    b = Backfill(
        dag_id=dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.NONE,
    )
    session.add(b)
    session.commit()
    backfill_id = b.id
    session.expunge_all()
    # Travel 3 minutes into the future so the backfill is past the 2-minute cutoff
    with time_machine.travel(pendulum.now("UTC").add(minutes=3), tick=False):
        runner = SchedulerJobRunner(
            job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
        )
        runner._mark_backfills_complete()
    b = session.get(Backfill, backfill_id)
    assert b.completed_at is not None


def test_mark_backfills_complete_keeps_old_backfill_with_running_dagruns(dag_maker, session):
    """Old backfill (>2 min) with running DagRuns must NOT be marked complete."""
    clear_db_backfills()
    dag_id = "test_backfill_old_with_runs"
    with dag_maker(serialized=True, dag_id=dag_id, schedule="@daily"):
        BashOperator(task_id="hi", bash_command="echo hi")
    b = Backfill(
        dag_id=dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.NONE,
    )
    session.add(b)
    session.commit()
    backfill_id = b.id
    dr = DagRun(
        dag_id=dag_id,
        run_id="backfill__2021-01-01T00:00:00+00:00",
        run_type=DagRunType.BACKFILL_JOB,
        logical_date=pendulum.parse("2021-01-01"),
        data_interval=(pendulum.parse("2021-01-01"), pendulum.parse("2021-01-02")),
        run_after=pendulum.parse("2021-01-02"),
        state=DagRunState.RUNNING,
        backfill_id=backfill_id,
    )
    session.add(dr)
    session.flush()
    session.add(
        BackfillDagRun(
            backfill_id=backfill_id,
            dag_run_id=dr.id,
            logical_date=pendulum.parse("2021-01-01"),
            sort_ordinal=1,
        )
    )
    session.commit()
    session.expunge_all()
    # Travel 3 minutes into the future; backfill is old but has a RUNNING DagRun
    with time_machine.travel(pendulum.now("UTC").add(minutes=3), tick=False):
        runner = SchedulerJobRunner(
            job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
        )
        runner._mark_backfills_complete()
    b = session.get(Backfill, backfill_id)
    assert b.completed_at is None


def test_mark_backfills_complete_young_backfill_with_finished_runs(dag_maker, session):
    """Young backfill (<2 min) with all SUCCESS DagRuns completes immediately."""
    clear_db_backfills()
    dag_id = "test_backfill_young_finished"
    with dag_maker(serialized=True, dag_id=dag_id, schedule="@daily"):
        BashOperator(task_id="hi", bash_command="echo hi")
    b = Backfill(
        dag_id=dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.NONE,
    )
    session.add(b)
    session.commit()
    backfill_id = b.id
    dr = DagRun(
        dag_id=dag_id,
        run_id="backfill__2021-01-01T00:00:00+00:00",
        run_type=DagRunType.BACKFILL_JOB,
        logical_date=pendulum.parse("2021-01-01"),
        data_interval=(pendulum.parse("2021-01-01"), pendulum.parse("2021-01-02")),
        run_after=pendulum.parse("2021-01-02"),
        state=DagRunState.SUCCESS,
        backfill_id=backfill_id,
    )
    session.add(dr)
    session.flush()
    session.add(
        BackfillDagRun(
            backfill_id=backfill_id,
            dag_run_id=dr.id,
            logical_date=pendulum.parse("2021-01-01"),
            sort_ordinal=1,
        )
    )
    session.commit()
    session.expunge_all()
    # No time travel — backfill was just created, should still complete
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    runner._mark_backfills_complete()
    b = session.get(Backfill, backfill_id)
    assert b.completed_at is not None


def test_mark_backfills_complete_multiple_independent(dag_maker, session):
    """Two backfills: one finished, one running — only the finished one completes."""
    clear_db_backfills()
    with dag_maker(serialized=True, dag_id="dag_finished", schedule="@daily"):
        BashOperator(task_id="hi", bash_command="echo hi")
    with dag_maker(serialized=True, dag_id="dag_running", schedule="@daily"):
        BashOperator(task_id="hi", bash_command="echo hi")
    b_finished = Backfill(
        dag_id="dag_finished",
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.NONE,
    )
    b_running = Backfill(
        dag_id="dag_running",
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-03"),
        max_active_runs=10,
        dag_run_conf={},
        reprocess_behavior=ReprocessBehavior.NONE,
    )
    session.add_all([b_finished, b_running])
    session.commit()
    finished_id = b_finished.id
    running_id = b_running.id
    # Finished backfill: SUCCESS DagRun
    dr1 = DagRun(
        dag_id="dag_finished",
        run_id="backfill__2021-01-01T00:00:00+00:00",
        run_type=DagRunType.BACKFILL_JOB,
        logical_date=pendulum.parse("2021-01-01"),
        data_interval=(pendulum.parse("2021-01-01"), pendulum.parse("2021-01-02")),
        run_after=pendulum.parse("2021-01-02"),
        state=DagRunState.SUCCESS,
        backfill_id=finished_id,
    )
    session.add(dr1)
    session.flush()
    session.add(
        BackfillDagRun(
            backfill_id=finished_id,
            dag_run_id=dr1.id,
            logical_date=pendulum.parse("2021-01-01"),
            sort_ordinal=1,
        )
    )
    # Running backfill: RUNNING DagRun
    dr2 = DagRun(
        dag_id="dag_running",
        run_id="backfill__2021-01-01T00:00:00+00:00",
        run_type=DagRunType.BACKFILL_JOB,
        logical_date=pendulum.parse("2021-01-01"),
        data_interval=(pendulum.parse("2021-01-01"), pendulum.parse("2021-01-02")),
        run_after=pendulum.parse("2021-01-02"),
        state=DagRunState.RUNNING,
        backfill_id=running_id,
    )
    session.add(dr2)
    session.flush()
    session.add(
        BackfillDagRun(
            backfill_id=running_id,
            dag_run_id=dr2.id,
            logical_date=pendulum.parse("2021-01-01"),
            sort_ordinal=1,
        )
    )
    session.commit()
    session.expunge_all()
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    runner._mark_backfills_complete()
    b_finished = session.get(Backfill, finished_id)
    b_running = session.get(Backfill, running_id)
    assert b_finished.completed_at is not None
    assert b_running.completed_at is None


class Key1Mapper(CorePartitionMapper):
    """Partition Mapper that returns only key-1 as downstream key"""

    def to_downstream(self, key: str) -> str:
        return "key-1"


def _find_registered_custom_partition_mapper(import_string: str) -> type[CorePartitionMapper]:
    if import_string == qualname(Key1Mapper):
        return Key1Mapper
    raise ValueError(f"unexpected class {import_string!r}")


@pytest.fixture
def custom_partition_mapper_patch() -> Callable[[], ExitStack]:
    def _patch() -> ExitStack:
        stack = ExitStack()
        for mock_target in [
            "airflow.serialization.encoders.find_registered_custom_partition_mapper",
            "airflow.serialization.decoders.find_registered_custom_partition_mapper",
        ]:
            stack.enter_context(
                mock.patch(
                    mock_target,
                    _find_registered_custom_partition_mapper,
                )
            )
        return stack

    return _patch


@pytest.fixture
def clear_asset_partition_rows() -> Iterator:
    clear_db_apdr()
    clear_db_pakl()

    yield

    clear_db_apdr()
    clear_db_pakl()


def _produce_and_register_asset_event(
    *,
    dag_id: str,
    asset: Asset,
    partition_key: str,
    session: Session,
    dag_maker: DagMaker,
    expected_partition_key: str | None = None,
    partition_date: datetime.datetime | None = None,
) -> AssetPartitionDagRun:
    if expected_partition_key is None:
        expected_partition_key = partition_key

    with dag_maker(dag_id=dag_id, schedule=PartitionedAtRuntime(), session=session) as dag:
        EmptyOperator(task_id="hi", outlets=[asset])

    dr = dag_maker.create_dagrun(
        partition_key=partition_key,
        partition_date=partition_date,
        session=session,
    )
    [ti] = dr.get_task_instances(session=session)
    session.commit()

    serialized_outlets = dag.get_task("hi").outlets

    TaskInstance.register_asset_changes_in_db(
        ti=ti,
        task_outlets=[o.asprofile() for o in serialized_outlets],
        outlet_events=[],
        session=session,
    )
    session.commit()

    event = session.scalar(
        select(AssetEvent).where(
            AssetEvent.source_dag_id == dag.dag_id,
            AssetEvent.source_run_id == dr.run_id,
        )
    )
    assert event is not None
    assert event.partition_key == partition_key

    apdr = session.scalar(
        select(AssetPartitionDagRun)
        .join(
            PartitionedAssetKeyLog,
            PartitionedAssetKeyLog.asset_partition_dag_run_id == AssetPartitionDagRun.id,
        )
        .where(PartitionedAssetKeyLog.asset_event_id == event.id)
    )
    assert apdr is not None
    assert apdr.created_dag_run_id is None
    assert apdr.partition_key == expected_partition_key

    return apdr


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_with_invalid_mapping(dag_maker: DagMaker, session: Session):
    session.execute(delete(Log))
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="asset-event-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=StartOfHourMapper(),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    with dag_maker(
        dag_id="asset-event-producer",
        schedule=CronPartitionTimetable("* * * * *", timezone="UTC"),
        session=session,
    ) as dag:
        EmptyOperator(task_id="hi", outlets=[asset_1])

    partition_key = "an invalid key for HourlyMapper"
    dr = dag_maker.create_dagrun(partition_key=partition_key, session=session)
    [ti] = dr.get_task_instances(session=session)
    session.commit()

    serialized_outlets = dag.get_task("hi").outlets
    TaskInstance.register_asset_changes_in_db(
        ti=ti,
        task_outlets=[o.asprofile() for o in serialized_outlets],
        outlet_events=[],
        session=session,
    )
    session.commit()
    event = session.scalar(
        select(AssetEvent).where(
            AssetEvent.source_dag_id == dag.dag_id,
            AssetEvent.source_run_id == dr.run_id,
        )
    )
    assert event is not None
    assert event.partition_key == partition_key
    apdr = session.scalar(
        select(AssetPartitionDagRun)
        .join(
            PartitionedAssetKeyLog,
            PartitionedAssetKeyLog.asset_partition_dag_run_id == AssetPartitionDagRun.id,
        )
        .where(PartitionedAssetKeyLog.asset_event_id == event.id)
    )
    assert apdr is None

    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    assert len(partition_dags) == 0
    assert partition_dags == set()

    audit_log = session.scalar(select(Log))
    assert audit_log is not None
    assert audit_log.extra == (
        "Could not map partition_key 'an invalid key for HourlyMapper' "
        "for asset (name='asset-1', uri='asset-1') in target Dag 'asset-event-consumer'. "
        "This likely indicates that the partition mapper in the target Dag is misconfigured or "
        "does not support this partition key.\n"
        "ValueError: time data 'an invalid key for HourlyMapper' does not match format '%Y-%m-%dT%H:%M:%S'"
    )


@pytest.mark.db_test
def test_create_dag_runs_partitioned_timetable_skips_when_next_fields_none(session):
    """
    Partitioned timetables may leave next_dagrun / next_dagrun_create_after unset when no run is due.
    The scheduler must skip the Dag without resolving its serialized definition AND log why,
    so operators can diagnose a Dag held by a misconfigured timetable.
    """
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    dag_model = MagicMock()
    dag_model.dag_id = "partitioned-skip-no-next-fields"
    dag_model.exceeds_max_non_backfill = False
    dag_model.next_dagrun = None
    dag_model.timetable_partitioned = True
    dag_model.next_dagrun_create_after = None
    dag_model.next_dagrun_partition_key = None
    dag_model.max_active_runs = 16
    dag_model.allowed_run_types = None

    # ``LoggingMixin.log`` is a property that lazily fills ``_log``; preload the
    # cache with a mock so the log-emission can be asserted by call signature.
    runner._log = mock.MagicMock()
    with mock.patch.object(runner, "_get_current_dag") as mock_get_dag:
        runner._create_dag_runs([dag_model], session)

    mock_get_dag.assert_not_called()
    # Operator-visibility invariant: the skip must log why, not silently swallow.
    assert runner._log.error.mock_calls == [
        mock.call(
            "dag_model.next_dagrun_partition_key is None; expected str",
            dag_id="partitioned-skip-no-next-fields",
        )
    ]


@pytest.mark.db_test
def test_create_dag_runs_partitioned_timetable_proceeds_when_partition_key_set(session):
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    dag_model = MagicMock()
    dag_model.dag_id = "partitioned-proceed-with-partition-key"
    dag_model.exceeds_max_non_backfill = False
    dag_model.next_dagrun = None
    dag_model.timetable_partitioned = True
    dag_model.next_dagrun_create_after = None
    dag_model.next_dagrun_partition_key = "partition-a"
    dag_model.max_active_runs = 16
    dag_model.allowed_run_types = None

    with mock.patch.object(runner, "_get_current_dag", return_value=None) as mock_get_dag:
        runner._create_dag_runs([dag_model], session)

    mock_get_dag.assert_called_once()


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_with_customized_mapper(
    dag_maker: DagMaker,
    session: Session,
    custom_partition_mapper_patch: Callable[[], ExitStack],
):
    asset_1 = Asset(name="asset-1")

    # Consumer Dag "asset-event-consumer"
    with custom_partition_mapper_patch():
        with dag_maker(
            dag_id="asset-event-consumer",
            schedule=PartitionedAssetTimetable(
                assets=asset_1,
                # Most users should use the partition mapper provided by the task-SDK.
                # Advanced users can import from core and register their own partition mapper
                # via an Airflow plugin.
                # We intentionally exclude core mappers from the public typing
                # so standard users don't accidentally rely on internal implementations.
                default_partition_mapper=Key1Mapper(),  # type: ignore[arg-type]
            ),
            session=session,
        ):
            EmptyOperator(task_id="hi")
        session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    with custom_partition_mapper_patch():
        apdr = _produce_and_register_asset_event(
            dag_id="asset-event-producer",
            asset=asset_1,
            partition_key="this-is-not-key-1-before-mapped",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="key-1",
        )
        partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    # Since asset event for Asset(name="asset-2") with key "key-1" has not yet been created,
    # no Dag run will be created
    assert apdr.created_dag_run_id is not None
    assert len(partition_dags) == 1
    assert partition_dags == {"asset-event-consumer"}

    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    asset_event = dag_run.consumed_asset_events[0]
    assert asset_event.source_task_id == "hi"
    assert asset_event.source_dag_id == "asset-event-producer"
    assert asset_event.source_run_id == "test"


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_consumer_dag_run_partition_date_identity_passthrough(dag_maker: DagMaker, session: Session):
    """IdentityMapper can't reconstruct a date from its key, so the scheduler resolver falls
    back to the producer's source date carried on the APDR and stamps it on the consumer DagRun.

    Temporal and composite mappers are resolved by the scheduler via to_partition_date (covered
    by the partition_mapper and resolver tests); this exercises the IdentityMapper carry, which
    is the one case the scheduler cannot resolve from the key alone.
    """
    asset_1 = Asset(name="asset-1")
    source_partition_date = pendulum.datetime(2026, 5, 20, 1, 0, 0, tz="UTC")

    with dag_maker(
        dag_id="asset-event-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=IdentityMapper(),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    apdr = _produce_and_register_asset_event(
        dag_id="asset-event-producer",
        asset=asset_1,
        partition_key="2026-05-20T01:00:00",
        partition_date=source_partition_date,
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2026-05-20T01:00:00",
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)

    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    assert partition_dags == {"asset-event-consumer"}

    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    assert dag_run.partition_key == "2026-05-20T01:00:00"
    assert dag_run.partition_date == source_partition_date


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
@mock.patch.object(SchedulerJobRunner, "_resolve_partition_date", autospec=True, return_value=None)
def test_consumer_dag_run_partition_date_not_masked_when_resolver_suppresses(
    mock_resolve, dag_maker: DagMaker, session: Session
):
    """A carried IdentityMapper date must not mask a resolver suppression.

    When temporal mappers feeding the same APDR conflict (or one raises), the resolver
    deliberately returns None and logs it; the carried date is only ever applied inside the
    resolver, never at the call site. Here the APDR carries a date (IdentityMapper) but the
    resolver returns None, and the consumer DagRun's partition_date must stay None — a
    regression re-adding a call-site fallback to ``apdr.partition_date`` would fail this.
    """
    asset_1 = Asset(name="asset-1")
    source_partition_date = pendulum.datetime(2026, 5, 20, 1, 0, 0, tz="UTC")

    with dag_maker(
        dag_id="asset-event-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=IdentityMapper(),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    apdr = _produce_and_register_asset_event(
        dag_id="asset-event-producer",
        asset=asset_1,
        partition_key="2026-05-20T01:00:00",
        partition_date=source_partition_date,
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2026-05-20T01:00:00",
    )
    session.refresh(apdr)
    # The IdentityMapper carry is stored on the APDR...
    assert apdr.partition_date == source_partition_date

    runner._create_dagruns_for_partitioned_asset_dags(session=session)

    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    assert dag_run.partition_key == "2026-05-20T01:00:00"
    # ...but the resolver suppressed a date, so the call site must NOT substitute the carry.
    assert dag_run.partition_date is None


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_consumer_dag_run_partition_date_none_for_non_temporal_mapper(
    dag_maker: DagMaker,
    session: Session,
    custom_partition_mapper_patch: Callable[[], ExitStack],
):
    """For mappers that aren't temporal/identity, the consumer DagRun's partition_date stays None."""
    asset_1 = Asset(name="asset-1")

    with custom_partition_mapper_patch():
        with dag_maker(
            dag_id="asset-event-consumer",
            schedule=PartitionedAssetTimetable(
                assets=asset_1,
                default_partition_mapper=Key1Mapper(),  # type: ignore[arg-type]
            ),
            session=session,
        ):
            EmptyOperator(task_id="hi")
        session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    with custom_partition_mapper_patch():
        apdr = _produce_and_register_asset_event(
            dag_id="asset-event-producer",
            asset=asset_1,
            partition_key="this-is-not-key-1-before-mapped",
            partition_date=pendulum.datetime(2026, 5, 20, 1, 0, 0, tz="UTC"),
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="key-1",
        )
        runner._create_dagruns_for_partitioned_asset_dags(session=session)

    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    assert dag_run.partition_key == "key-1"
    assert dag_run.partition_date is None


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_consumer_dag_run_partition_date_is_none_when_source_has_no_date(
    dag_maker: DagMaker, session: Session
):
    """When the producer DagRun has no partition_date, IdentityMapper passes None through."""
    asset_1 = Asset(name="asset-1")

    with dag_maker(
        dag_id="asset-event-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=IdentityMapper(),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    apdr = _produce_and_register_asset_event(
        dag_id="asset-event-producer",
        asset=asset_1,
        partition_key="2026-05-20T01:00:00",
        partition_date=None,
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2026-05-20T01:00:00",
    )
    runner._create_dagruns_for_partitioned_asset_dags(session=session)

    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    assert dag_run.partition_key == "2026-05-20T01:00:00"
    assert dag_run.partition_date is None


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_consumer_dag_run_partition_date_is_none_when_task_key_diverges(
    dag_maker: DagMaker, session: Session
):
    """A task-emitted partition_key differing from the DagRun's drops the source date.

    The producer DagRun carries a partition_date, but the task emits an outlet event with a
    different partition_key. The run-level date refers to the run-level key, so it must not be
    carried onto the divergent partition: the APDR (and the consumer DagRun created from it) keep
    partition_date None even though the producer run had one.
    """
    asset_1 = Asset(name="asset-1")

    with dag_maker(
        dag_id="asset-event-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=IdentityMapper(),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    with dag_maker(
        dag_id="asset-event-producer",
        schedule=PartitionedAtRuntime(),
        session=session,
    ) as dag:
        EmptyOperator(task_id="hi", outlets=[asset_1])

    dr = dag_maker.create_dagrun(
        partition_key="scheduler-key",
        partition_date=pendulum.datetime(2026, 5, 20, 1, 0, 0, tz="UTC"),
        session=session,
    )
    [ti] = dr.get_task_instances(session=session)
    session.commit()

    serialized_outlets = dag.get_task("hi").outlets
    TaskInstance.register_asset_changes_in_db(
        ti=ti,
        task_outlets=[o.asprofile() for o in serialized_outlets],
        outlet_events=[
            {
                "dest_asset_key": {"name": "asset-1", "uri": "asset-1"},
                "extra": {},
                "partition_key": "task-key",
            },
        ],
        session=session,
    )
    session.commit()

    event = session.scalar(
        select(AssetEvent).where(
            AssetEvent.source_dag_id == dag.dag_id,
            AssetEvent.source_run_id == dr.run_id,
        )
    )
    assert event is not None
    assert event.partition_key == "task-key"

    apdr = session.scalar(
        select(AssetPartitionDagRun)
        .join(
            PartitionedAssetKeyLog,
            PartitionedAssetKeyLog.asset_partition_dag_run_id == AssetPartitionDagRun.id,
        )
        .where(PartitionedAssetKeyLog.asset_event_id == event.id)
    )
    assert apdr is not None
    # Divergent key → the threaded source date is dropped to None at APDR creation.
    assert apdr.partition_key == "task-key"
    assert apdr.partition_date is None

    runner._create_dagruns_for_partitioned_asset_dags(session=session)

    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    assert dag_run.partition_key == "task-key"
    assert dag_run.partition_date is None


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_consumer_dag_listen_to_two_partitioned_asset(
    dag_maker: DagMaker,
    session: Session,
):
    asset_1 = Asset(name="asset-1")
    asset_2 = Asset(name="asset-2")

    # Consumer Dag "asset-event-consumer"
    with dag_maker(
        dag_id="asset-event-consumer",
        schedule=PartitionedAssetTimetable(
            assets=(Asset.ref(uri="asset-1") & Asset.ref(name="asset-2")),
            default_partition_mapper=IdentityMapper(),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    # Check whether we are ready to create Dag run for "asset-event-consumer"
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    apdr = _produce_and_register_asset_event(
        dag_id="asset-event-producer-1",
        asset=asset_1,
        partition_key="key-1",
        session=session,
        dag_maker=dag_maker,
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    # Since asset event for Asset(name="asset-2") with key "key-1" has not yet been created,
    # no Dag run will be created
    assert apdr.created_dag_run_id is None
    assert len(partition_dags) == 0
    assert partition_dags == set()

    apdr = _produce_and_register_asset_event(
        dag_id="asset-event-producer-2",
        asset=asset_2,
        partition_key="key-2",
        session=session,
        dag_maker=dag_maker,
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    # Since asset event for Asset(name="asset-2") with key "key-1" has not yet been created,
    # (the one created was Asset(name="asset-2") with key "key-2")
    # no Dag run will be created
    assert apdr.created_dag_run_id is None
    assert len(partition_dags) == 0
    assert partition_dags == set()

    apdr = _produce_and_register_asset_event(
        dag_id="asset-event-producer-3",
        asset=asset_2,
        partition_key="key-1",
        session=session,
        dag_maker=dag_maker,
    )
    # Now the asset event for Asset(name="asset-2") with key "key-1" is created,
    # the Dag run should be created
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    assert len(partition_dags) == 1
    assert partition_dags == {"asset-event-consumer"}

    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    for asset_event in dag_run.consumed_asset_events:
        assert asset_event.source_task_id == "hi"
        assert "asset-event-producer-" in asset_event.source_dag_id
        assert asset_event.source_run_id == "test"


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_rollup_holds_until_window_complete(
    dag_maker: DagMaker,
    session: Session,
):
    """A rollup APDR stays pending until every upstream key in the window has arrived."""
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="rollup-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
            ),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    # First minute key arrives — only 1 / 60 upstream keys, so the APDR must
    # not fire yet.
    apdr = _produce_and_register_asset_event(
        dag_id="rollup-producer-0",
        asset=asset_1,
        partition_key="2024-01-01T00:00:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is None
    assert partition_dags == set()

    # Send the remaining 59 minute keys — once all 60 are present the rollup is
    # satisfied and the APDR creates its Dag run on the next tick.
    for minute in range(1, 60):
        _produce_and_register_asset_event(
            dag_id=f"rollup-producer-{minute}",
            asset=asset_1,
            partition_key=f"2024-01-01T00:{minute:02d}:00",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="2024-01-01T00",
        )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    assert partition_dags == {"rollup-consumer"}


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_segment_rollup_holds_until_all_segments_arrive(
    dag_maker: DagMaker,
    session: Session,
):
    """
    A categorical (segment) rollup fires once every declared segment has arrived.

    ``RollupMapper(FixedKeyMapper("all_regions"), SegmentWindow([...]))`` collapses
    each region key onto a single ``all_regions`` partition, so all three events
    accumulate into one APDR, and holds the downstream run until ``us``, ``eu``,
    and ``apac`` are all present.
    """
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="segment-rollup-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=FixedKeyMapper("all_regions"),
                window=SegmentWindow(["us", "eu", "apac"]),
            ),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    # First region arrives — only 1 / 3 segments, so the APDR must not fire.
    # Every region collapses onto the single ``all_regions`` partition.
    apdr = _produce_and_register_asset_event(
        dag_id="segment-producer-us",
        asset=asset_1,
        partition_key="us",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="all_regions",
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is None
    assert partition_dags == set()

    # The remaining two regions arrive — once all three segments are present the
    # rollup is satisfied and the APDR creates its Dag run on the next tick. All
    # three events share the one ``all_regions`` APDR.
    for region in ("eu", "apac"):
        sibling = _produce_and_register_asset_event(
            dag_id=f"segment-producer-{region}",
            asset=asset_1,
            partition_key=region,
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="all_regions",
        )
        assert sibling.id == apdr.id
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    assert apdr.partition_key == "all_regions"
    assert partition_dags == {"segment-rollup-consumer"}


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_rollup_minimum_count_negative_fires_with_tolerated_gaps(
    dag_maker: DagMaker,
    session: Session,
):
    """``MinimumCount(-3)`` fires once at most 3 of the 60 expected keys are still missing."""
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="rollup-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
                wait_policy=MinimumCount(-3),
            ),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    # 56 of 60 keys arrive — still 4 short of the 57-key threshold, so the APDR
    # must not fire yet.
    apdr = None
    for minute in range(56):
        apdr = _produce_and_register_asset_event(
            dag_id=f"rollup-producer-{minute}",
            asset=asset_1,
            partition_key=f"2024-01-01T00:{minute:02d}:00",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="2024-01-01T00",
        )
    assert apdr is not None
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is None
    assert partition_dags == set()

    # One more key arrives, bringing the matched count to 57 (= 60 - 3). The
    # policy's tolerance is met and the Dag run is created on the next tick.
    _produce_and_register_asset_event(
        dag_id="rollup-producer-56",
        asset=asset_1,
        partition_key="2024-01-01T00:56:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    assert partition_dags == {"rollup-consumer"}


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_rollup_minimum_count_fires_when_threshold_met(
    dag_maker: DagMaker,
    session: Session,
):
    """``MinimumCount(5)`` fires as soon as 5 of the 60 expected keys arrive."""
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="rollup-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
                wait_policy=MinimumCount(5),
            ),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    # 4 of 60 keys arrive — one short of the 5-key threshold, so the APDR
    # must not fire yet.
    apdr = None
    for minute in range(4):
        apdr = _produce_and_register_asset_event(
            dag_id=f"rollup-producer-{minute}",
            asset=asset_1,
            partition_key=f"2024-01-01T00:{minute:02d}:00",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="2024-01-01T00",
        )
    assert apdr is not None
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is None
    assert partition_dags == set()

    # The 5th key arrives and the threshold is met; the Dag run is created on
    # the next tick even though 55 of the 60 expected keys are still missing.
    _produce_and_register_asset_event(
        dag_id="rollup-producer-4",
        asset=asset_1,
        partition_key="2024-01-01T00:04:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    assert partition_dags == {"rollup-consumer"}


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_rollup_treats_mapper_exception_as_not_satisfied(
    dag_maker: DagMaker,
    session: Session,
):
    """
    A misconfigured rollup mapper that raises during status evaluation must not crash
    the scheduler tick — the asset is treated as not-yet-satisfied, the APDR remains
    pending, and the exception is logged in the scheduler log.
    """
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="rollup-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
            ),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    apdr = _produce_and_register_asset_event(
        dag_id="rollup-producer-0",
        asset=asset_1,
        partition_key="2024-01-01T00:00:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )

    with mock.patch.object(
        WaitPolicy,
        "is_satisfied_by_keys",
        side_effect=RuntimeError("misconfigured rollup mapper"),
    ):
        partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)

    session.refresh(apdr)
    assert apdr.created_dag_run_id is None
    assert partition_dags == set()


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_rollup_survives_scheduler_restart_partial_arrival(
    dag_maker: DagMaker,
    session: Session,
):
    """
    Rollup completion is driven by PAKL rows (the source of truth), not by any
    in-memory scheduler state.  After a simulated scheduler restart, a brand new
    SchedulerJobRunner instance can pick up where the previous one left off and
    fire the Dag run once all upstream keys have arrived.
    """
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="rollup-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
            ),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    # --- First scheduler process: send 30 of 60 required minute keys ---
    runner_1 = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    apdr = _produce_and_register_asset_event(
        dag_id="rollup-producer-0",
        asset=asset_1,
        partition_key="2024-01-01T00:00:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )
    for minute in range(1, 30):
        _produce_and_register_asset_event(
            dag_id=f"rollup-producer-{minute}",
            asset=asset_1,
            partition_key=f"2024-01-01T00:{minute:02d}:00",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="2024-01-01T00",
        )

    partition_dags = runner_1._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is None, "APDR must not fire with only 30 of 60 keys"
    assert partition_dags == set()

    # --- Simulate scheduler restart: expire session state, create new runner ---
    session.expire_all()
    del runner_1

    runner_2 = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    # Send the remaining 30 minute keys (total 60 distinct).
    for minute in range(30, 60):
        _produce_and_register_asset_event(
            dag_id=f"rollup-producer-{minute}",
            asset=asset_1,
            partition_key=f"2024-01-01T00:{minute:02d}:00",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="2024-01-01T00",
        )

    partition_dags = runner_2._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None, (
        "APDR must fire once all 60 upstream keys have arrived, even after a scheduler restart"
    )
    assert partition_dags == {"rollup-consumer"}


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_pending_apdr_select_locks_rows_for_skip_locked_claim(session: Session):
    """
    The pending-APDR select must wrap the query in ``with_row_locks(skip_locked=True)``
    so HA scheduler replicas don't both grab the same satisfied APDR and race the
    ``created_dag_run_id`` UPDATE (which would orphan whichever DagRun loses the race).
    """
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    with mock.patch(
        "airflow.jobs.scheduler_job_runner.with_row_locks",
        wraps=with_row_locks,
    ) as wrapped:
        runner._create_dagruns_for_partitioned_asset_dags(session=session)

    apdr_calls = [call for call in wrapped.mock_calls if call.kwargs.get("of") is AssetPartitionDagRun]
    assert len(apdr_calls) == 1, f"Expected exactly one with_row_locks call for APDR, got {apdr_calls}"
    call = apdr_calls[0]
    assert call.kwargs["skip_locked"] is True
    assert call.kwargs["key_share"] is False
    assert call.kwargs["session"] is session


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_pending_apdr_select_breaks_tie_on_id(session: Session):
    """
    Two APDRs sharing a ``created_at`` (possible under bulk asset-event ingestion)
    must be ordered by ``id`` ascending so concurrent HA scheduler ticks pick the
    same LIMIT slice — otherwise replicas can disagree on which APDRs the LIMIT
    selects even when the row lock would otherwise prevent the duplicate fire.
    """
    shared_ts = timezone.utcnow()
    first_inserted = AssetPartitionDagRun(target_dag_id="d1", partition_key="b", created_at=shared_ts)
    session.add(first_inserted)
    session.flush()
    second_inserted = AssetPartitionDagRun(target_dag_id="d2", partition_key="a", created_at=shared_ts)
    session.add(second_inserted)
    session.flush()

    fetched = session.scalars(
        select(AssetPartitionDagRun)
        .where(AssetPartitionDagRun.created_dag_run_id.is_(None))
        .order_by(AssetPartitionDagRun.created_at, AssetPartitionDagRun.id)
    ).all()

    assert first_inserted.id < second_inserted.id
    assert [a.id for a in fetched] == [first_inserted.id, second_inserted.id]


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param("window_changed", id="A-window-changed-clears"),
        pytest.param("unchanged", id="C-unchanged-survives"),
        pytest.param("null_fingerprint", id="D-null-fingerprint-clears"),
    ],
)
def test_partitioned_dag_run_stale_rollup_fingerprint(
    scenario: str,
    dag_maker: DagMaker,
    session: Session,
):
    """
    APDR stale-fingerprint cleanup: only mapper / window changes trigger cleanup.

    A — window changed (HourWindow → DayWindow): fingerprint differs → APDR cleared.
    C — nothing changed: fingerprint identical → APDR survives.
    D — NULL fingerprint (legacy row): treated as stale → APDR cleared.
    """
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="rollup-consumer-stale",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
            ),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    apdr = _produce_and_register_asset_event(
        dag_id="rollup-producer-a",
        asset=asset_1,
        partition_key="2024-01-01T00:00:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )

    if scenario == "window_changed":
        # Stamp the APDR with a DayWindow fingerprint — differs from the latest HourWindow.
        # Use the core timetable (has ``partitioned = True``) so compute_rollup_fingerprint works.
        day_timetable = CorePartitionedAssetTimetable(
            assets=ensure_serialized_asset(asset_1),
            default_partition_mapper=CoreRollupMapper(
                upstream_mapper=CoreStartOfHourMapper(),
                window=CoreDayWindow(),
            ),
        )
        stale_fp = compute_rollup_fingerprint(day_timetable)
        session.execute(
            update(AssetPartitionDagRun)
            .where(AssetPartitionDagRun.id == apdr.id)
            .values(rollup_fingerprint=stale_fp)
        )
    elif scenario == "null_fingerprint":
        session.execute(
            update(AssetPartitionDagRun)
            .where(AssetPartitionDagRun.id == apdr.id)
            .values(rollup_fingerprint=None)
        )
    # scenario == "unchanged": leave the fingerprint as-is (stamped at creation)

    session.commit()

    pakl_count_before = session.scalar(
        select(func.count()).where(PartitionedAssetKeyLog.asset_partition_dag_run_id == apdr.id)
    )
    assert pakl_count_before is not None
    assert pakl_count_before > 0

    runner._create_dagruns_for_partitioned_asset_dags(session=session)

    apdr_survives = scenario == "unchanged"
    assert session.scalar(select(func.count()).where(AssetPartitionDagRun.id == apdr.id)) == (
        1 if apdr_survives else 0
    ), f"APDR should {'survive' if apdr_survives else 'be deleted'} for scenario={scenario!r}"
    assert session.scalar(
        select(func.count()).where(PartitionedAssetKeyLog.asset_partition_dag_run_id == apdr.id)
    ) == (pakl_count_before if apdr_survives else 0), (
        f"PAKL rows should {'survive' if apdr_survives else 'be deleted'} for scenario={scenario!r}"
    )


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_structural_edit_does_not_clear_apdr(
    dag_maker: DagMaker,
    session: Session,
):
    """
    B — C2 core regression: a structural Dag edit that leaves the rollup definition
    unchanged must NOT clear the APDR.

    The old ``dag_version_id`` approach would bump the version on any serialization,
    causing spurious cleanup for unrelated task / description changes. The new
    fingerprint only captures mapper + window, so this test must pass.

    A new SerializedDag version is produced by re-serializing the same dag_id with an
    extra task (dag_version bumps), but since mapper / window are unchanged the
    rollup fingerprint stays identical and the APDR must survive.
    """
    asset_1 = Asset(name="asset-1")
    # sdk version — used only for dag_maker (which serializes it to the core timetable).
    rollup_schedule = PartitionedAssetTimetable(
        assets=asset_1,
        default_partition_mapper=RollupMapper(
            upstream_mapper=StartOfHourMapper(),
            window=HourWindow(),
        ),
    )
    # core version — used for compute_rollup_fingerprint (requires ``partitioned = True``).
    core_rollup_schedule = CorePartitionedAssetTimetable(
        assets=ensure_serialized_asset(asset_1),
        default_partition_mapper=CoreRollupMapper(
            upstream_mapper=CoreStartOfHourMapper(),
            window=CoreHourWindow(),
        ),
    )
    with dag_maker(
        dag_id="rollup-consumer-structural",
        schedule=rollup_schedule,
        session=session,
    ):
        EmptyOperator(task_id="task-original")
    session.commit()

    fp_before = compute_rollup_fingerprint(core_rollup_schedule)

    apdr = _produce_and_register_asset_event(
        dag_id="rollup-structural-producer",
        asset=asset_1,
        partition_key="2024-01-01T00:00:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )

    # Confirm the APDR was stamped with the correct fingerprint.
    assert apdr.rollup_fingerprint == fp_before

    # Re-serialize the same dag_id with an additional task — dag_version bumps, but
    # mapper / window are identical so rollup fingerprint must not change.
    with dag_maker(
        dag_id="rollup-consumer-structural",
        schedule=rollup_schedule,
        session=session,
    ):
        EmptyOperator(task_id="task-original")
        EmptyOperator(task_id="task-added")
    session.commit()

    # Verify the fingerprint is stable across the structural edit (guards against
    # JSON round-trip key-order regressions that would make == return False).
    fp_after = compute_rollup_fingerprint(core_rollup_schedule)
    assert fp_after == fp_before, "Fingerprint must not change for a structural-only Dag edit"

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    runner._create_dagruns_for_partitioned_asset_dags(session=session)

    assert session.scalar(select(func.count()).where(AssetPartitionDagRun.id == apdr.id)) == 1, (
        "APDR must survive: same rollup fingerprint, only structural Dag edit"
    )


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_fingerprint_compute_failure_does_not_clear_apdr(
    dag_maker: DagMaker,
    session: Session,
):
    """
    E — fingerprint compute failure: if ``compute_rollup_fingerprint`` raises for a
    dag_id, the corresponding APDR must be skipped (not deleted).

    This guards against the bug where a failed fingerprint lookup returns None from
    ``dict.get()``, making ``apdr.rollup_fingerprint != None`` evaluate as True for
    any non-None stored fingerprint — causing the APDR to be spuriously cleared.
    """
    asset_1 = Asset(name="asset-1")
    rollup_schedule = PartitionedAssetTimetable(
        assets=asset_1,
        default_partition_mapper=RollupMapper(
            upstream_mapper=StartOfHourMapper(),
            window=HourWindow(),
        ),
    )
    with dag_maker(
        dag_id="rollup-consumer-fp-fail",
        schedule=rollup_schedule,
        session=session,
    ):
        EmptyOperator(task_id="task-original")
    session.commit()

    apdr = _produce_and_register_asset_event(
        dag_id="rollup-fp-fail-producer",
        asset=asset_1,
        partition_key="2024-01-01T00:00:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    with mock.patch(
        "airflow.jobs.scheduler_job_runner.compute_rollup_fingerprint",
        side_effect=RuntimeError("simulated deserialization failure"),
    ):
        runner._create_dagruns_for_partitioned_asset_dags(session=session)

    assert session.scalar(select(func.count()).where(AssetPartitionDagRun.id == apdr.id)) == 1, (
        "APDR must survive when compute_rollup_fingerprint raises — skip, not delete"
    )


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_rollup_idempotent_with_duplicate_upstream_events(
    dag_maker: DagMaker,
    session: Session,
):
    """
    Rollup completion is based on distinct upstream partition keys, not on the
    number of PartitionedAssetKeyLog rows.

    PAKL by design does not enforce uniqueness — the same (asset, partition_key)
    can appear in multiple rows when several producer Dags emit the same key.
    The scheduler resolves this via set semantics in source_key_by_asset_per_apdr
    (a defaultdict of defaultdict of set), so a duplicate PAKL row must not
    prevent the rollup from completing once all 60 distinct keys are present.

    Do NOT add a unique constraint to PAKL to "fix" duplicates — that would break
    legitimate multi-producer workflows where two independent Dags produce the
    same asset partition.
    """
    asset_1 = Asset(name="asset-1")
    with dag_maker(
        dag_id="rollup-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=RollupMapper(
                upstream_mapper=StartOfHourMapper(),
                window=HourWindow(),
            ),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    # Emit the first minute key twice from two different producer Dags — this
    # writes two PAKL rows for the same (asset, partition_key).
    apdr = _produce_and_register_asset_event(
        dag_id="rollup-producer-dup-a",
        asset=asset_1,
        partition_key="2024-01-01T00:00:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )
    _produce_and_register_asset_event(
        dag_id="rollup-producer-dup-b",
        asset=asset_1,
        partition_key="2024-01-01T00:00:00",
        session=session,
        dag_maker=dag_maker,
        expected_partition_key="2024-01-01T00",
    )

    # Send the remaining 59 distinct minute keys (total = 60 distinct + 1 duplicate
    # = 61 PAKL rows).
    for minute in range(1, 60):
        _produce_and_register_asset_event(
            dag_id=f"rollup-producer-{minute}",
            asset=asset_1,
            partition_key=f"2024-01-01T00:{minute:02d}:00",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="2024-01-01T00",
        )

    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None, (
        "APDR must fire when 60 distinct upstream keys are present, regardless of duplicate PAKL rows"
    )
    assert partition_dags == {"rollup-consumer"}


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_consumer_dag_listen_to_two_partitioned_asset_with_key_1_mapper(
    dag_maker: DagMaker,
    session: Session,
    custom_partition_mapper_patch: Callable[[], ExitStack],
):
    asset_1 = Asset(name="asset-1")
    asset_2 = Asset(name="asset-2")

    # Consumer Dag "asset-event-consumer"
    with custom_partition_mapper_patch():
        with dag_maker(
            dag_id="asset-event-consumer",
            schedule=PartitionedAssetTimetable(
                assets=asset_1 & asset_2,
                # Most users should use the partition mapper provided by the task-SDK.
                # Advanced users can import from core and register their own partition mapper
                # via an Airflow plugin.
                # We intentionally exclude core mappers from the public typing
                # so standard users don't accidentally rely on internal implementations.
                partition_mapper_config={
                    Asset(name="asset-1"): Key1Mapper(),  # type: ignore[dict-item]
                    Asset(name="asset-2"): Key1Mapper(),  # type: ignore[dict-item]
                },
            ),
            session=session,
        ):
            EmptyOperator(task_id="hi")
    session.commit()

    # Check whether we are ready to create Dag run for "asset-event-consumer"
    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    with custom_partition_mapper_patch():
        apdr = _produce_and_register_asset_event(
            dag_id="asset-event-producer-1",
            asset=asset_1,
            partition_key="key-2",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="key-1",
        )
        partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    # Since asset event for Asset(name="asset-2") with key "key-1" has not yet been created,
    # no Dag run will be created
    assert apdr.created_dag_run_id is None
    assert len(partition_dags) == 0
    assert partition_dags == set()

    with custom_partition_mapper_patch():
        apdr = _produce_and_register_asset_event(
            dag_id="asset-event-producer-2",
            asset=asset_2,
            partition_key="key-3",
            session=session,
            dag_maker=dag_maker,
            expected_partition_key="key-1",
        )
        partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    # Even though the original key passed was key-3, the consumer Dag uses Key1Mapper to transform anything
    # into key-1. Thus, the criteria is met and a Dag run should be created
    assert apdr.created_dag_run_id is not None
    assert len(partition_dags) == 1
    assert partition_dags == {"asset-event-consumer"}

    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    for asset_event in dag_run.consumed_asset_events:
        assert asset_event.source_task_id == "hi"
        assert "asset-event-producer-" in asset_event.source_dag_id
        assert asset_event.source_run_id == "test"


def _make_n_satisfied_apdrs(
    *,
    consumer_dag_id: str,
    asset: Asset,
    partition_keys: list[str],
    session: Session,
    dag_maker: DagMaker,
) -> list[AssetPartitionDagRun]:
    """Build a consumer Dag plus *N* satisfied APDRs (one per partition_key)."""
    with dag_maker(
        dag_id=consumer_dag_id,
        schedule=PartitionedAssetTimetable(
            assets=asset,
            default_partition_mapper=IdentityMapper(),
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    return [
        _produce_and_register_asset_event(
            dag_id=f"asset-event-producer-{i}",
            asset=asset,
            partition_key=key,
            session=session,
            dag_maker=dag_maker,
        )
        for i, key in enumerate(partition_keys, start=1)
    ]


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partition_cap_at_exactly_n_processes_all(dag_maker: DagMaker, session: Session):
    """
    cap == APDR count: every pending APDR fires in a single tick.

    Pairs with :func:`test_partition_cap_at_n_minus_one_leaves_one_pending` to pin
    the ``LIMIT`` boundary against off-by-one regressions (``>`` vs ``>=``).
    """
    apdrs = _make_n_satisfied_apdrs(
        consumer_dag_id="cap-consumer-n",
        asset=Asset(name="asset-cap-n"),
        partition_keys=["k1", "k2", "k3"],
        session=session,
        dag_maker=dag_maker,
    )

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    runner._max_partition_dag_runs_per_loop = 3

    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)

    for apdr in apdrs:
        session.refresh(apdr)
        assert apdr.created_dag_run_id is not None
    assert partition_dags == {"cap-consumer-n"}


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partition_cap_at_n_minus_one_leaves_one_pending(dag_maker: DagMaker, session: Session):
    """
    cap == APDR count - 1: oldest cap APDRs fire, the newest remains pending.

    FIFO ordering means the *last-created* APDR is the one left for the next
    tick. Together with :func:`test_partition_cap_at_exactly_n_processes_all`
    this pins the ``LIMIT`` boundary — a ``LIMIT N-1`` regression would still
    pass the at-cap test, but the older sibling here would fail to fire.
    """
    apdrs = _make_n_satisfied_apdrs(
        consumer_dag_id="cap-consumer-n-minus-one",
        asset=Asset(name="asset-cap-n-minus-one"),
        partition_keys=["k1", "k2", "k3"],
        session=session,
        dag_maker=dag_maker,
    )
    # Pin created_at so FIFO ordering is stable across fast in-memory runs —
    # otherwise three APDRs created in the same microsecond tie under
    # ``ORDER BY created_at`` and "which two fire" becomes nondeterministic.
    base = timezone.utcnow()
    for i, apdr in enumerate(apdrs):
        apdr.created_at = base + timedelta(seconds=i)
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    runner._max_partition_dag_runs_per_loop = 2

    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)

    for apdr in apdrs:
        session.refresh(apdr)
    assert apdrs[0].created_dag_run_id is not None
    assert apdrs[1].created_dag_run_id is not None
    assert apdrs[2].created_dag_run_id is None
    assert partition_dags == {"cap-consumer-n-minus-one"}


def _set_asset_active(*, name: str, uri: str, session: Session, active: bool) -> None:
    """Toggle ``AssetActive`` row for an asset to simulate orphan / reactivation."""
    row = session.scalar(select(AssetActive).where(AssetActive.name == name, AssetActive.uri == uri))
    if active and row is None:
        session.add(AssetActive(name=name, uri=uri))
    elif not active and row is not None:
        session.delete(row)
    session.commit()


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_skips_when_asset_is_inactive(dag_maker: DagMaker, session: Session):
    """
    An otherwise-satisfied APDR does not fire while its upstream asset is inactive.

    If the asset becomes orphaned (no Dag declares it any more) while the consumer
    Dag still references it, firing on the stale ``PartitionedAssetKeyLog`` history
    would conflict with the declared topology — so the scheduler freezes the APDR
    instead.
    """
    asset = Asset(name="asset-inactive")
    [apdr] = _make_n_satisfied_apdrs(
        consumer_dag_id="inactive-asset-consumer",
        asset=asset,
        partition_keys=["k1"],
        session=session,
        dag_maker=dag_maker,
    )
    _set_asset_active(name=asset.name, uri=asset.uri, session=session, active=False)

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)

    session.refresh(apdr)
    assert apdr.created_dag_run_id is None
    assert partition_dags == set()


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
def test_partitioned_dag_run_resumes_when_asset_reactivates(dag_maker: DagMaker, session: Session):
    """
    A frozen APDR fires automatically once its asset is reactivated.

    Pairs with :func:`test_partitioned_dag_run_skips_when_asset_is_inactive` to pin
    the freeze ↔ resume contract: deactivation halts fire, reactivation resumes it
    without any other state change.
    """
    asset = Asset(name="asset-reactivate")
    [apdr] = _make_n_satisfied_apdrs(
        consumer_dag_id="reactivate-asset-consumer",
        asset=asset,
        partition_keys=["k1"],
        session=session,
        dag_maker=dag_maker,
    )
    _set_asset_active(name=asset.name, uri=asset.uri, session=session, active=False)

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is None
    assert partition_dags == set()

    _set_asset_active(name=asset.name, uri=asset.uri, session=session, active=True)
    partition_dags = runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)
    assert apdr.created_dag_run_id is not None
    assert partition_dags == {"reactivate-asset-consumer"}


# ---------------------------------------------------------------------------
# Tests for nullable dag_version in scheduler callbacks (AIP-66)
#
# Verifies that TaskCallbackRequest and EmailRequest are always created
# with correct bundle_name/bundle_version whether ti.dag_version is a real
# DagVersion object (normal) or None (legacy tasks migrated from Airflow 2).
#
# Fallback mirrors DagCallbackRequest in dagrun.py:
#   bundle_name    <- ti.dag_version.bundle_name  OR  ti.dag_model.bundle_name
#   bundle_version <- ti.dag_version.bundle_version  OR  ti.dag_run.bundle_version
# ---------------------------------------------------------------------------


def _make_ti_with_dag_version(
    dag_version, dag_model_bundle_name="fallback-bundle", dag_run_bundle_version="v1.0-fallback"
):
    """Build a minimal mock TaskInstance for dag_version nullable tests."""
    ti = mock.MagicMock()
    ti.dag_version = dag_version
    ti.dag_model = mock.MagicMock()
    ti.dag_model.bundle_name = dag_model_bundle_name
    ti.dag_model.relative_fileloc = "/dags/test_dag.py"
    ti.dag_run = mock.MagicMock()
    ti.dag_run.bundle_version = dag_run_bundle_version
    return ti


def _make_dag_version(bundle_name="my-bundle", bundle_version="v2.0"):
    """Create a simple mock DagVersion."""
    dv = mock.MagicMock()
    dv.bundle_name = bundle_name
    dv.bundle_version = bundle_version
    return dv


def _extract_bundle_name(ti):
    """Mirror the inline fallback logic from scheduler_job_runner.py."""
    return ti.dag_version.bundle_name if ti.dag_version else ti.dag_model.bundle_name


def _extract_bundle_version(ti):
    """Mirror the inline fallback logic from scheduler_job_runner.py."""
    return (
        ti.dag_version.bundle_version
        if ti.dag_version and ti.dag_run.bundle_version is not None
        else ti.dag_run.bundle_version
    )


class TestSchedulerCallbackBundleInfoDagVersionNullable:
    """
    Verify the bundle_name / bundle_version extraction logic used at all four
    TaskCallbackRequest / EmailRequest creation sites in scheduler_job_runner.py.

    When dag_version is present  -> use dag_version.bundle_name / bundle_version.
    When dag_version is None     -> fall back to dag_model.bundle_name / dag_run.bundle_version.
    """

    # ── With dag_version present ──────────────────────────────────────────

    @pytest.mark.parametrize(
        ("dv_bundle_name", "dv_bundle_version"),
        [
            pytest.param("my-bundle", "v2.0", id="normal"),
            pytest.param("dags-folder", None, id="version_none"),
            pytest.param("custom-bundle", "v99.0", id="custom"),
        ],
    )
    def test_bundle_info_from_dag_version_when_present(self, dv_bundle_name, dv_bundle_version):
        """When dag_version is set, bundle info must come from it."""
        dv = _make_dag_version(bundle_name=dv_bundle_name, bundle_version=dv_bundle_version)
        ti = _make_ti_with_dag_version(dag_version=dv, dag_model_bundle_name="SHOULD-NOT-USE")

        assert _extract_bundle_name(ti) == dv_bundle_name
        assert _extract_bundle_version(ti) == dv_bundle_version

    # ── With dag_version None (legacy Airflow 2 task) ─────────────────────

    @pytest.mark.parametrize(
        ("model_bundle_name", "run_bundle_version"),
        [
            pytest.param("fallback-bundle", "v1.0-fallback", id="normal_fallback"),
            pytest.param("dags-folder", None, id="version_none_fallback"),
            pytest.param("another-bundle", "v3.5", id="custom_fallback"),
        ],
    )
    def test_bundle_info_falls_back_when_dag_version_none(self, model_bundle_name, run_bundle_version):
        """When dag_version is None, bundle info must fall back to dag_model / dag_run."""
        ti = _make_ti_with_dag_version(
            dag_version=None,
            dag_model_bundle_name=model_bundle_name,
            dag_run_bundle_version=run_bundle_version,
        )

        assert _extract_bundle_name(ti) == model_bundle_name
        assert _extract_bundle_version(ti) == run_bundle_version

    # ── No AttributeError crash ────────────────────────────────────────────

    @pytest.mark.parametrize(
        "dag_version_present",
        [
            pytest.param(True, id="dag_version_present"),
            pytest.param(False, id="dag_version_none"),
        ],
    )
    def test_no_attribute_error_regardless_of_dag_version(self, dag_version_present):
        """
        The old code crashed with AttributeError when ti.dag_version was None.
        The new fallback must never raise regardless of dag_version state.
        """
        ti = _make_ti_with_dag_version(dag_version=_make_dag_version() if dag_version_present else None)

        name = _extract_bundle_name(ti)
        version = _extract_bundle_version(ti)

        assert isinstance(name, str)
        assert version is None or isinstance(version, str)

    # ── Precedence: dag_version wins over fallback ─────────────────────────

    def test_dag_version_takes_precedence_over_fallback_values(self):
        """When dag_version is set, dag_model/dag_run fallbacks must NOT be used."""
        dv = _make_dag_version(bundle_name="preferred-bundle", bundle_version="preferred-v1")
        ti = _make_ti_with_dag_version(
            dag_version=dv,
            dag_model_bundle_name="fallback-bundle",
            dag_run_bundle_version="fallback-v1",
        )

        assert _extract_bundle_name(ti) == "preferred-bundle"
        assert _extract_bundle_version(ti) == "preferred-v1"

    def test_fallback_values_used_only_when_dag_version_is_none(self):
        """When dag_version is None, fallback values must be used."""
        ti = _make_ti_with_dag_version(
            dag_version=None,
            dag_model_bundle_name="fallback-bundle",
            dag_run_bundle_version="fallback-v1",
        )

        assert _extract_bundle_name(ti) == "fallback-bundle"
        assert _extract_bundle_version(ti) == "fallback-v1"

    def test_unpinned_dag_run_overrides_dag_version_bundle_version(self):
        """
        When dag_run.bundle_version is None (e.g. dag.disable_bundle_versioning=True
        leaves it unpinned even though DagVersion was written with a SHA), the
        callback must inherit None so it runs against the same on-disk code as
        the task did, instead of pinning to the DagVersion's recorded SHA.
        """
        dv = _make_dag_version(bundle_name="my-bundle", bundle_version="abc123-sha")
        ti = _make_ti_with_dag_version(dag_version=dv, dag_run_bundle_version=None)

        # bundle_name still comes from dag_version
        assert _extract_bundle_name(ti) == "my-bundle"
        # but bundle_version follows the dag_run's unpinned state
        assert _extract_bundle_version(ti) is None


def _make_scheduler_runner_for_connection_tests(
    executors: list[BaseExecutor],
    *,
    primary: BaseExecutor | None = None,
) -> SchedulerJobRunner:
    """Build a SchedulerJobRunner wired only with the attributes the connection-test methods read."""
    mock_job = mock.MagicMock(spec=Job)
    mock_job.id = 1
    mock_job.max_tis_per_query = 16
    runner = SchedulerJobRunner.__new__(SchedulerJobRunner)
    runner.job = mock_job
    runner.executors = executors
    runner.executor = primary or executors[0]
    runner._multi_team = False
    runner._log = mock.MagicMock(spec=logging.Logger)
    return runner


@pytest.fixture
def scheduler_job_runner_for_connection_tests(session):
    """Yield a SchedulerJobRunner wired to a single LocalExecutor with a clean DB."""
    session.execute(delete(ConnectionTestRequest))
    session.commit()

    executor = LocalExecutor()
    executor.name = ExecutorName(
        module_path="airflow.executors.local_executor.LocalExecutor", alias="LocalExecutor"
    )
    executor.queued_connection_tests.clear()
    yield _make_scheduler_runner_for_connection_tests([executor])
    session.execute(delete(ConnectionTestRequest))
    session.commit()


class TestDispatchConnectionTests:
    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "4",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_pending_tests(self, scheduler_job_runner_for_connection_tests, session):
        """Pending connection tests are dispatched to a supporting executor."""
        ct = ConnectionTestRequest(conn_type="test_type", connection_id="test_conn")
        session.add(ct)
        session.commit()
        assert ct.state == ConnectionTestState.PENDING

        scheduler_job_runner_for_connection_tests._enqueue_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.QUEUED
        assert len(scheduler_job_runner_for_connection_tests.executor.queued_connection_tests) == 1

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "4",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_reads_team_from_row(self, scheduler_job_runner_for_connection_tests, session):
        """In multi-team mode the executor is loaded for the team persisted on the row."""
        runner = scheduler_job_runner_for_connection_tests
        runner._multi_team = True

        session.add(
            ConnectionTestRequest(conn_type="test_type", connection_id="team_conn", team_name="team_a")
        )
        session.commit()

        with mock.patch.object(runner, "_try_to_load_executor", return_value=None) as mock_load:
            runner._enqueue_connection_tests(session=session)

        assert mock_load.call_args.kwargs["team_name"] == "team_a"

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "1",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_respects_concurrency_limit(self, scheduler_job_runner_for_connection_tests, session):
        """Excess pending tests stay PENDING when concurrency is at capacity."""
        ct_active = ConnectionTestRequest(conn_type="test_type", connection_id="active_conn")
        ct_active.state = ConnectionTestState.QUEUED
        session.add(ct_active)

        ct_pending = ConnectionTestRequest(conn_type="test_type", connection_id="pending_conn")
        session.add(ct_pending)
        session.commit()

        scheduler_job_runner_for_connection_tests._enqueue_connection_tests(session=session)

        session.expire_all()
        ct_pending = session.get(ConnectionTestRequest, ct_pending.id)
        assert ct_pending.state == ConnectionTestState.PENDING

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "4",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_fails_fast_when_executor_does_not_support_test(
        self, scheduler_job_runner_for_connection_tests, session
    ):
        """Failure message names the executor that was tried, not 'no executor'."""
        unsupporting_executor = BaseExecutor()
        unsupporting_executor.supports_connection_test = False
        unsupporting_executor.name = ExecutorName(
            module_path="airflow.executors.base_executor.BaseExecutor", alias="celery"
        )
        scheduler_job_runner_for_connection_tests.executors = [unsupporting_executor]
        scheduler_job_runner_for_connection_tests.executor = unsupporting_executor

        ct = ConnectionTestRequest(conn_type="test_type", connection_id="test_conn", executor="celery")
        session.add(ct)
        session.commit()

        scheduler_job_runner_for_connection_tests._enqueue_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert ct.result_message == "Executor 'celery' does not support connection testing"

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "4",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_with_unmatched_executor_fails_fast(
        self, scheduler_job_runner_for_connection_tests, session
    ):
        """Tests requesting an executor with no match are failed immediately."""
        ct = ConnectionTestRequest(conn_type="test_type", connection_id="test_conn", executor="gpu_workers")
        session.add(ct)
        session.commit()

        scheduler_job_runner_for_connection_tests._enqueue_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert "gpu_workers" in ct.result_message

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "3",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_budget_dispatches_up_to_remaining_slots(
        self, scheduler_job_runner_for_connection_tests, session
    ):
        """When 1 slot is occupied, only budget (cap - active) pending tests are dispatched."""
        ct_active = ConnectionTestRequest(conn_type="test_type", connection_id="active_conn")
        ct_active.state = ConnectionTestState.RUNNING
        session.add(ct_active)

        pending_tests = []
        for i in range(3):
            ct = ConnectionTestRequest(conn_type="test_type", connection_id=f"pending_{i}")
            session.add(ct)
            pending_tests.append(ct)
        session.commit()
        pending_ids = [ct.id for ct in pending_tests]

        scheduler_job_runner_for_connection_tests._enqueue_connection_tests(session=session)

        session.expire_all()
        states = [session.get(ConnectionTestRequest, pid).state for pid in pending_ids]
        assert states.count(ConnectionTestState.QUEUED) == 2
        assert states.count(ConnectionTestState.PENDING) == 1

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "2",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_order_is_fifo_by_created_at(self, scheduler_job_runner_for_connection_tests, session):
        """Pending tests are dispatched in FIFO order based on created_at."""
        initial_time = timezone.utcnow()

        with time_machine.travel(initial_time - timedelta(minutes=5), tick=False):
            ct_old = ConnectionTestRequest(conn_type="test_type", connection_id="old_conn")
            session.add(ct_old)
            session.flush()

        with time_machine.travel(initial_time, tick=False):
            ct_new = ConnectionTestRequest(conn_type="test_type", connection_id="new_conn")
            session.add(ct_new)
            session.flush()

        with time_machine.travel(initial_time + timedelta(minutes=1), tick=False):
            ct_newest = ConnectionTestRequest(conn_type="test_type", connection_id="newest_conn")
            session.add(ct_newest)
            session.flush()

        session.commit()

        scheduler_job_runner_for_connection_tests._enqueue_connection_tests(session=session)

        session.expire_all()
        assert session.get(ConnectionTestRequest, ct_old.id).state == ConnectionTestState.QUEUED
        assert session.get(ConnectionTestRequest, ct_new.id).state == ConnectionTestState.QUEUED
        assert session.get(ConnectionTestRequest, ct_newest.id).state == ConnectionTestState.PENDING

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "4",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_fails_fast_for_unserved_executor(
        self, scheduler_job_runner_for_connection_tests, session
    ):
        """Tests requesting an executor no team serves are failed immediately."""
        with mock.patch.object(
            scheduler_job_runner_for_connection_tests,
            "_try_to_load_executor",
            return_value=None,
        ):
            ct = ConnectionTestRequest(
                conn_type="test_type", connection_id="test_conn", executor="nonexistent_executor"
            )
            session.add(ct)
            session.commit()

            scheduler_job_runner_for_connection_tests._enqueue_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert "nonexistent_executor" in ct.result_message

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "4",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_executor_matched_by_alias(self, session):
        """When executor is specified, the executor whose name.alias matches is selected."""
        session.execute(delete(ConnectionTestRequest))
        session.commit()

        executor_a = LocalExecutor()
        executor_a.name = ExecutorName(module_path="path.to.ExecutorA", alias="executor_a")
        executor_a.queued_connection_tests.clear()

        executor_b = LocalExecutor()
        executor_b.name = ExecutorName(module_path="path.to.ExecutorB", alias="executor_b")
        executor_b.queued_connection_tests.clear()

        runner = _make_scheduler_runner_for_connection_tests([executor_a, executor_b])

        ct = ConnectionTestRequest(conn_type="test_type", connection_id="team_conn", executor="executor_b")
        session.add(ct)
        session.commit()

        runner._enqueue_connection_tests(session=session)

        assert len(executor_b.queued_connection_tests) == 1
        assert len(executor_a.queued_connection_tests) == 0

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "4",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_executor_matched_by_module_path(self, session):
        """When executor is specified by module_path, the matching executor is selected."""
        session.execute(delete(ConnectionTestRequest))
        session.commit()

        executor_a = LocalExecutor()
        executor_a.name = ExecutorName(module_path="path.to.ExecutorA", alias="executor_a")
        executor_a.queued_connection_tests.clear()

        executor_b = LocalExecutor()
        executor_b.name = ExecutorName(module_path="path.to.ExecutorB", alias="executor_b")
        executor_b.queued_connection_tests.clear()

        runner = _make_scheduler_runner_for_connection_tests([executor_a, executor_b])

        ct = ConnectionTestRequest(
            conn_type="test_type", connection_id="team_conn", executor="path.to.ExecutorB"
        )
        session.add(ct)
        session.commit()

        runner._enqueue_connection_tests(session=session)

        assert len(executor_b.queued_connection_tests) == 1
        assert len(executor_a.queued_connection_tests) == 0

    def test_dispatch_executor_matched_by_class_name(self, session):
        """When executor is specified by class name only, the matching executor is selected."""
        session.execute(delete(ConnectionTestRequest))
        session.commit()

        executor_a = LocalExecutor()
        executor_a.name = ExecutorName(module_path="path.to.ExecutorA", alias="executor_a")
        executor_a.queued_connection_tests.clear()

        executor_b = LocalExecutor()
        executor_b.name = ExecutorName(module_path="path.to.ExecutorB", alias="executor_b")
        executor_b.queued_connection_tests.clear()

        runner = _make_scheduler_runner_for_connection_tests([executor_a, executor_b])

        ct = ConnectionTestRequest(conn_type="test_type", connection_id="team_conn", executor="ExecutorB")
        session.add(ct)
        session.commit()

        runner._enqueue_connection_tests(session=session)

        assert len(executor_b.queued_connection_tests) == 1
        assert len(executor_a.queued_connection_tests) == 0

    @mock.patch.dict(
        os.environ,
        {
            "AIRFLOW__CONNECTION_TEST__MAX_CONCURRENCY": "4",
            "AIRFLOW__CONNECTION_TEST__TIMEOUT": "60",
        },
    )
    def test_dispatch_fails_when_executor_does_not_support_connection_test(
        self, scheduler_job_runner_for_connection_tests, session
    ):
        """When the resolved executor does not support connection tests, the test is failed gracefully."""
        executor = scheduler_job_runner_for_connection_tests.executor
        executor.supports_connection_test = False

        ct = ConnectionTestRequest(conn_type="test_type", connection_id="test_conn")
        session.add(ct)
        session.commit()

        scheduler_job_runner_for_connection_tests._enqueue_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert "does not support connection testing" in ct.result_message


class TestReapStaleConnectionTests:
    @mock.patch.dict(os.environ, {"AIRFLOW__CONNECTION_TEST__TIMEOUT": "60"})
    def test_reap_stale_queued_test(self, scheduler_job_runner_for_connection_tests, session):
        """Stale QUEUED tests are marked as FAILED by the reaper."""
        initial_time = timezone.utcnow()

        with time_machine.travel(initial_time, tick=False):
            ct = ConnectionTestRequest(conn_type="test_type", connection_id="test_conn")
            ct.state = ConnectionTestState.QUEUED
            session.add(ct)
            session.commit()

        with time_machine.travel(initial_time + timedelta(seconds=200), tick=False):
            scheduler_job_runner_for_connection_tests._reap_stale_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert "queued but never started" in ct.result_message

    @mock.patch.dict(os.environ, {"AIRFLOW__CONNECTION_TEST__TIMEOUT": "60"})
    def test_reap_when_state_loaded_as_plain_str(self, scheduler_job_runner_for_connection_tests, session):
        """Reaper handles a state loaded from the DB as a plain str (fresh-session path), not an enum."""
        initial_time = timezone.utcnow()

        with time_machine.travel(initial_time, tick=False):
            ct = ConnectionTestRequest(conn_type="test_type", connection_id="reload_conn")
            ct.state = ConnectionTestState.RUNNING
            session.add(ct)
            session.commit()

        # Expire so the reaper reloads ``state`` as a plain ``str`` (as a fresh scheduler process would).
        session.expire_all()

        with time_machine.travel(initial_time + timedelta(seconds=200), tick=False):
            scheduler_job_runner_for_connection_tests._reap_stale_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.FAILED

    @mock.patch.dict(os.environ, {"AIRFLOW__CONNECTION_TEST__TIMEOUT": "60"})
    def test_does_not_reap_fresh_tests(self, scheduler_job_runner_for_connection_tests, session):
        """Fresh QUEUED tests are not reaped."""
        ct = ConnectionTestRequest(conn_type="test_type", connection_id="test_conn")
        ct.state = ConnectionTestState.QUEUED
        session.add(ct)
        session.commit()

        scheduler_job_runner_for_connection_tests._reap_stale_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.QUEUED

    @mock.patch.dict(os.environ, {"AIRFLOW__CONNECTION_TEST__TIMEOUT": "60"})
    def test_reap_stale_running_test(self, scheduler_job_runner_for_connection_tests, session):
        """Stale RUNNING tests are also reaped by the reaper."""
        initial_time = timezone.utcnow()
        with time_machine.travel(initial_time, tick=False):
            ct = ConnectionTestRequest(conn_type="test_type", connection_id="running_conn")
            ct.state = ConnectionTestState.RUNNING
            session.add(ct)
            session.commit()

        with time_machine.travel(initial_time + timedelta(seconds=200), tick=False):
            scheduler_job_runner_for_connection_tests._reap_stale_connection_tests(session=session)

        session.expire_all()
        ct = session.get(ConnectionTestRequest, ct.id)
        assert ct.state == ConnectionTestState.FAILED
        assert "timed out" in ct.result_message

    @mock.patch.dict(os.environ, {"AIRFLOW__CONNECTION_TEST__TIMEOUT": "60"})
    def test_reaper_ignores_terminal_states(self, scheduler_job_runner_for_connection_tests, session):
        """Tests in terminal states (SUCCESS, FAILED) are not touched by the reaper."""
        initial_time = timezone.utcnow()
        with time_machine.travel(initial_time, tick=False):
            ct_success = ConnectionTestRequest(conn_type="test_type", connection_id="success_conn")
            ct_success.state = ConnectionTestState.SUCCESS
            ct_success.result_message = "OK"
            session.add(ct_success)

            ct_failed = ConnectionTestRequest(conn_type="test_type", connection_id="failed_conn")
            ct_failed.state = ConnectionTestState.FAILED
            ct_failed.result_message = "Error"
            session.add(ct_failed)
            session.commit()

        with time_machine.travel(initial_time + timedelta(seconds=200), tick=False):
            scheduler_job_runner_for_connection_tests._reap_stale_connection_tests(session=session)

        session.expire_all()
        assert session.get(ConnectionTestRequest, ct_success.id).state == ConnectionTestState.SUCCESS
        assert session.get(ConnectionTestRequest, ct_failed.id).state == ConnectionTestState.FAILED


@pytest.mark.need_serialized_dag
@pytest.mark.usefixtures("clear_asset_partition_rows")
@pytest.mark.parametrize(
    ("sdk_mapper", "upstream_partition_key", "expected_downstream_key", "expected_partition_date"),
    [
        (
            StartOfDayMapper(),
            "2024-03-15T10:30:00",
            "2024-03-15",
            datetime.datetime(2024, 3, 15, 0, 0, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow()),
            "2024-01-01T00:00:00",
            "2024-01-01T00",
            datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        ),
        (
            IdentityMapper(),
            "key-abc",
            "key-abc",
            None,
        ),
    ],
)
def test_partition_date_populated_on_dagrun(
    dag_maker: DagMaker,
    session: Session,
    sdk_mapper,
    upstream_partition_key,
    expected_downstream_key,
    expected_partition_date,
):
    """DagRun.partition_date is set correctly for temporal / rollup-of-temporal mappers."""
    asset_1 = Asset(name="asset-pd-test")

    with dag_maker(
        dag_id="partition-date-consumer",
        schedule=PartitionedAssetTimetable(
            assets=asset_1,
            default_partition_mapper=sdk_mapper,
        ),
        session=session,
    ):
        EmptyOperator(task_id="hi")
    session.commit()

    runner = SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )

    apdr = _produce_and_register_asset_event(
        dag_id="partition-date-producer",
        asset=asset_1,
        partition_key=upstream_partition_key,
        session=session,
        dag_maker=dag_maker,
        expected_partition_key=expected_downstream_key,
    )

    # For the rollup case, send all 60 minute keys so the window is complete.
    if isinstance(sdk_mapper, RollupMapper):
        for minute in range(1, 60):
            _produce_and_register_asset_event(
                dag_id=f"partition-date-producer-{minute}",
                asset=asset_1,
                partition_key=f"2024-01-01T00:{minute:02d}:00",
                session=session,
                dag_maker=dag_maker,
                expected_partition_key=expected_downstream_key,
            )

    runner._create_dagruns_for_partitioned_asset_dags(session=session)
    session.refresh(apdr)

    assert apdr.created_dag_run_id is not None
    dag_run = session.scalar(select(DagRun).where(DagRun.id == apdr.created_dag_run_id))
    assert dag_run is not None
    assert dag_run.partition_date == expected_partition_date


def _make_runner() -> SchedulerJobRunner:
    return SchedulerJobRunner(
        job=Job(job_type=SchedulerJobRunner.job_type), executors=[MockExecutor(do_update=False)]
    )


_CARRIED_DATE = datetime.datetime(2026, 5, 20, 1, 0, 0, tzinfo=datetime.timezone.utc)


@pytest.mark.parametrize(
    ("mappers", "partition_key", "carried_partition_date", "expected"),
    [
        # Non-temporal mapper, nothing carried → None.
        pytest.param([CoreIdentityMapper()], "some-key", None, None, id="non-temporal-none"),
        # Non-temporal mapper with a carried producer date (IdentityMapper) → the carry.
        pytest.param(
            [CoreIdentityMapper()],
            "some-key",
            _CARRIED_DATE,
            _CARRIED_DATE,
            id="non-temporal-returns-carried-date",
        ),
        # StartOfDayMapper(NY): "2024-03-15" → NY midnight = 04:00 UTC (EDT, DST since 2024-03-10),
        # localised with the mapper's own timezone rather than the global default.
        pytest.param(
            [CoreStartOfDayMapper(timezone="America/New_York")],
            "2024-03-15",
            None,
            datetime.datetime(2024, 3, 15, 4, 0, 0, tzinfo=datetime.timezone.utc),
            id="non-utc-uses-mapper-timezone",
        ),
        # Key cannot be decoded by the mapper's format → caught → None, and the carried
        # date is NOT substituted (the error is logged; masking it would hide that).
        pytest.param([CoreStartOfDayMapper()], "not-a-date", _CARRIED_DATE, None, id="decode-failure-none"),
        # FanOutMapper unwraps to its downstream_mapper (daily), which owns the per-day key.
        pytest.param(
            [CoreFanOutMapper(upstream_mapper=CoreStartOfWeekMapper(), window=CoreWeekWindow())],
            "2024-01-16",
            None,
            datetime.datetime(2024, 1, 16, 0, 0, 0, tzinfo=datetime.timezone.utc),
            id="fanout-uses-downstream-mapper",
        ),
        # Two temporal mappers resolving the same instant → that single anchor.
        pytest.param(
            [CoreStartOfDayMapper(), CoreStartOfDayMapper()],
            "2024-03-15",
            None,
            datetime.datetime(2024, 3, 15, 0, 0, 0, tzinfo=datetime.timezone.utc),
            id="agreeing-mappers-anchor",
        ),
        # Same key, UTC midnight (00:00Z) vs NY midnight (04:00Z) — distinct instants → None,
        # and the carried date is NOT substituted (it would mask the logged conflict).
        pytest.param(
            [CoreStartOfDayMapper(timezone="UTC"), CoreStartOfDayMapper(timezone="America/New_York")],
            "2024-03-15",
            _CARRIED_DATE,
            None,
            id="conflicting-mappers-none",
        ),
        # Second mapper (hour format) raises on the day key → whole resolution aborts → None
        # (the first mapper's anchor is discarded; all-or-nothing).
        pytest.param(
            [CoreStartOfDayMapper(), CoreStartOfHourMapper()],
            "2024-03-15",
            _CARRIED_DATE,
            None,
            id="one-failing-mapper-aborts",
        ),
    ],
)
def test_resolve_partition_date(mappers, partition_key, carried_partition_date, expected):
    """_resolve_partition_date over mapper compositions: temporal / fan-out / agree / conflict / failure.

    The carried date (the producer's source date stamped on the APDR, set only for IdentityMapper)
    is returned only when no temporal mapper contributes an anchor. On a conflict or a mapper
    error the result is None — the carry must not mask the logged suppression. The mappers are
    consumed one per upstream asset, so ``asset_infos`` is sized to ``mappers``.
    """
    runner = _make_runner()
    timetable = mock.MagicMock()
    timetable.get_partition_mapper.side_effect = mappers
    asset_infos = [(f"asset-{i}-name", f"asset-{i}-uri") for i in range(len(mappers))]

    result = runner._resolve_partition_date(
        timetable=timetable,
        asset_infos=asset_infos,
        partition_key=partition_key,
        dag_id="test-dag",
        carried_partition_date=carried_partition_date,
    )
    assert result == expected


class TestSchedulerObservabilityMetrics:
    """Tests for the scheduler observability metrics emitted in scheduler_job_runner.py."""

    @pytest.fixture(autouse=True)
    def per_test(self) -> Generator:
        _clean_db()
        self.job_runner: SchedulerJobRunner | None = None
        yield
        _clean_db()

    # --- scheduler.loop_exceptions ---

    def test_loop_exceptions_incr_on_scheduler_loop_failure(self):
        """scheduler.loop_exceptions is emitted with exception_class tag when _run_scheduler_loop raises."""
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with (
            mock.patch("airflow.jobs.scheduler_job_runner.stats") as mock_stats,
            mock.patch.object(self.job_runner, "register_signals", return_value=MagicMock()),
            mock.patch.object(self.job_runner.executor, "start"),
            mock.patch.object(self.job_runner.executor, "end"),
            mock.patch.object(self.job_runner, "_run_scheduler_loop", side_effect=RuntimeError("loop crash")),
            pytest.raises(RuntimeError, match="loop crash"),
        ):
            self.job_runner._execute()

        mock_stats.incr.assert_any_call("scheduler.loop_exceptions", tags={"exception_class": "RuntimeError"})

    def test_loop_exceptions_not_emitted_on_clean_exit(self):
        """scheduler.loop_exceptions is NOT emitted when _run_scheduler_loop returns normally."""
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with (
            mock.patch("airflow.jobs.scheduler_job_runner.stats") as mock_stats,
            mock.patch.object(self.job_runner, "register_signals", return_value=MagicMock()),
            mock.patch.object(self.job_runner.executor, "start"),
            mock.patch.object(self.job_runner.executor, "end"),
            mock.patch.object(self.job_runner, "_run_scheduler_loop"),
        ):
            self.job_runner._execute()

        emitted_names = [c.args[0] for c in mock_stats.incr.call_args_list]
        assert "scheduler.loop_exceptions" not in emitted_names

    # --- scheduler.executor_events.{batch_size,processed,failed} ---

    def test_executor_events_batch_metrics_emitted_on_success(self):
        """batch_size gauge and processed counter are emitted via the early-return path."""
        # Empty event buffer → tis_with_right_state is empty → early return with num_events=0
        mock_executor = MagicMock()
        mock_executor.get_event_buffer.return_value = {}

        with mock.patch("airflow.jobs.scheduler_job_runner.stats") as mock_stats:
            result = SchedulerJobRunner.process_executor_events(
                executor=mock_executor, job_id=1, scheduler_dag_bag=MagicMock(), session=MagicMock()
            )

        assert result == 0
        mock_stats.gauge.assert_called_once_with("scheduler.executor_events.batch_size", 0)
        mock_stats.incr.assert_called_once_with("scheduler.executor_events.processed", 0)

    def test_executor_events_failed_metric_emitted_on_exception(self):
        """failed counter is emitted in _process_executor_events when process_executor_events raises."""
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        with (
            mock.patch("airflow.jobs.scheduler_job_runner.stats") as mock_stats,
            mock.patch.object(
                SchedulerJobRunner, "process_executor_events", side_effect=ValueError("executor boom")
            ),
            pytest.raises(ValueError, match="executor boom"),
        ):
            self.job_runner._process_executor_events(executor=MagicMock(), session=MagicMock())

        mock_stats.incr.assert_called_once_with(
            "scheduler.executor_events.failed", tags={"exception_class": "ValueError"}
        )
        mock_stats.gauge.assert_not_called()

    # --- scheduler.zombies.detected ---

    def test_zombies_detected_heartbeat_timeout_emitted(self):
        """scheduler.zombies.detected{reason:heartbeat_timeout} is emitted when zombies are found."""
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)
        fake_tis = [MagicMock(), MagicMock(), MagicMock()]

        mock_session = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        with (
            mock.patch("airflow.jobs.scheduler_job_runner.stats") as mock_stats,
            mock.patch("airflow.jobs.scheduler_job_runner.create_session", return_value=mock_ctx),
            mock.patch.object(
                self.job_runner, "_find_task_instances_without_heartbeats", return_value=fake_tis
            ),
            mock.patch.object(self.job_runner, "_purge_task_instances_without_heartbeats"),
        ):
            self.job_runner._find_and_purge_task_instances_without_heartbeats()

        mock_stats.incr.assert_called_once_with(
            "scheduler.zombies.detected", 3, tags={"reason": "heartbeat_timeout"}
        )

    def test_zombies_detected_not_emitted_when_no_heartbeat_timeout(self):
        """scheduler.zombies.detected is NOT emitted when no zombie task instances are found."""
        scheduler_job = Job()
        self.job_runner = SchedulerJobRunner(job=scheduler_job)

        mock_session = MagicMock()
        mock_ctx = MagicMock()
        mock_ctx.__enter__ = MagicMock(return_value=mock_session)
        mock_ctx.__exit__ = MagicMock(return_value=False)

        with (
            mock.patch("airflow.jobs.scheduler_job_runner.stats") as mock_stats,
            mock.patch("airflow.jobs.scheduler_job_runner.create_session", return_value=mock_ctx),
            mock.patch.object(self.job_runner, "_find_task_instances_without_heartbeats", return_value=[]),
        ):
            self.job_runner._find_and_purge_task_instances_without_heartbeats()

        mock_stats.incr.assert_not_called()
