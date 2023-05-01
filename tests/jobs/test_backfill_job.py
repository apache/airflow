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
import json
import logging
import threading
from collections import defaultdict
from unittest import mock
from unittest.mock import patch

import pytest

from airflow import settings
from airflow.cli import cli_parser
from airflow.exceptions import (
    AirflowException,
    AirflowTaskTimeout,
    BackfillUnfinished,
    DagConcurrencyLimitReached,
    NoAvailablePoolSlot,
    TaskConcurrencyLimitReached,
)
from airflow.executors.executor_constants import MOCK_EXECUTOR
from airflow.executors.executor_loader import ExecutorLoader
from airflow.jobs.backfill_job_runner import BackfillJobRunner
from airflow.jobs.job import Job, run_job
from airflow.listeners.listener import get_listener_manager
from airflow.models import DagBag, Pool, TaskInstance as TI
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstanceKey
from airflow.models.taskmap import TaskMap
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.timeout import timeout
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import DagRunType
from tests.listeners import dag_listener
from tests.models import TEST_DAGS_FOLDER
from tests.test_utils.db import (
    clear_db_dags,
    clear_db_pools,
    clear_db_runs,
    clear_db_xcom,
    set_default_pool_slots,
)
from tests.test_utils.mock_executor import MockExecutor
from tests.test_utils.timetables import cron_timetable

logger = logging.getLogger(__name__)

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


@pytest.fixture(scope="module")
def dag_bag():
    return DagBag(include_examples=True)


# Patch the MockExecutor into the dict of known executors in the Loader
@patch.dict(
    ExecutorLoader.executors, {MOCK_EXECUTOR: f"{MockExecutor.__module__}.{MockExecutor.__qualname__}"}
)
class TestBackfillJob:
    @staticmethod
    def clean_db():
        clear_db_dags()
        clear_db_runs()
        clear_db_xcom()
        clear_db_pools()

    @pytest.fixture(autouse=True)
    def set_instance_attrs(self, dag_bag):
        self.clean_db()
        self.parser = cli_parser.get_parser()
        self.dagbag = dag_bag
        # `airflow tasks run` relies on serialized_dag
        for dag in self.dagbag.dags.values():
            SerializedDagModel.write_dag(dag)

    def _get_dummy_dag(
        self,
        dag_maker_fixture,
        dag_id="test_dag",
        pool=Pool.DEFAULT_POOL_NAME,
        max_active_tis_per_dag=None,
        task_id="op",
        **kwargs,
    ):
        with dag_maker_fixture(dag_id=dag_id, schedule="@daily", **kwargs) as dag:
            EmptyOperator(task_id=task_id, pool=pool, max_active_tis_per_dag=max_active_tis_per_dag)

        return dag

    def _times_called_with(self, method, class_):
        count = 0
        for args in method.call_args_list:
            if isinstance(args[0][0], class_):
                count += 1
        return count

    def test_unfinished_dag_runs_set_to_failed(self, dag_maker):
        dag = self._get_dummy_dag(dag_maker)
        dag_run = dag_maker.create_dagrun(state=None)

        job = Job(executor=MockExecutor())
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=8),
            ignore_first_depends_on_past=True,
        )

        job_runner._set_unfinished_dag_runs_to_failed([dag_run])
        dag_run.refresh_from_db()

        assert State.FAILED == dag_run.state

    def test_dag_run_with_finished_tasks_set_to_success(self, dag_maker):
        dag = self._get_dummy_dag(dag_maker)
        dag_run = dag_maker.create_dagrun(state=None)

        for ti in dag_run.get_task_instances():
            ti.set_state(State.SUCCESS)

        job = Job(executor=MockExecutor())
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=8),
            ignore_first_depends_on_past=True,
        )
        job_runner._set_unfinished_dag_runs_to_failed([dag_run])

        dag_run.refresh_from_db()

        assert State.SUCCESS == dag_run.state

    @pytest.mark.backend("postgres", "mysql")
    def test_trigger_controller_dag(self, session):
        dag = self.dagbag.get_dag("example_trigger_controller_dag")
        target_dag = self.dagbag.get_dag("example_trigger_target_dag")
        target_dag.sync_to_db()

        target_dag_run = session.query(DagRun).filter(DagRun.dag_id == target_dag.dag_id).one_or_none()
        assert target_dag_run is None

        job = Job(executor=MockExecutor())
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_first_depends_on_past=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        dag_run = session.query(DagRun).filter(DagRun.dag_id == dag.dag_id).one_or_none()
        assert dag_run is not None

        task_instances_list = job_runner._task_instances_for_dag_run(dag=dag, dag_run=dag_run)

        assert task_instances_list

    @pytest.mark.backend("postgres", "mysql")
    def test_backfill_multi_dates(self):
        dag = self.dagbag.get_dag("miscellaneous_test_dag")

        end_date = DEFAULT_DATE + datetime.timedelta(days=1)

        executor = MockExecutor(parallelism=16)
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=end_date,
            ignore_first_depends_on_past=True,
        )

        run_job(job=job, execute_callable=job_runner._execute)

        expected_execution_order = [
            ("runme_0", DEFAULT_DATE),
            ("runme_1", DEFAULT_DATE),
            ("runme_2", DEFAULT_DATE),
            ("runme_0", end_date),
            ("runme_1", end_date),
            ("runme_2", end_date),
            ("also_run_this", DEFAULT_DATE),
            ("also_run_this", end_date),
            ("run_after_loop", DEFAULT_DATE),
            ("run_after_loop", end_date),
            ("run_this_last", DEFAULT_DATE),
            ("run_this_last", end_date),
        ]
        assert [
            ((dag.dag_id, task_id, f"backfill__{when.isoformat()}", 1, -1), (State.SUCCESS, None))
            for (task_id, when) in expected_execution_order
        ] == executor.sorted_tasks

        session = settings.Session()
        drs = session.query(DagRun).filter(DagRun.dag_id == dag.dag_id).order_by(DagRun.execution_date).all()

        assert drs[0].execution_date == DEFAULT_DATE
        assert drs[0].state == State.SUCCESS
        assert drs[1].execution_date == DEFAULT_DATE + datetime.timedelta(days=1)
        assert drs[1].state == State.SUCCESS

        dag.clear()
        session.close()

    @pytest.mark.backend("postgres", "mysql")
    @pytest.mark.parametrize(
        "dag_id, expected_execution_order",
        [
            [
                "example_branch_operator",
                (
                    "run_this_first",
                    "branching",
                    "branch_a",
                    "branch_b",
                    "branch_c",
                    "branch_d",
                    "follow_branch_a",
                    "follow_branch_b",
                    "follow_branch_c",
                    "follow_branch_d",
                    "join",
                ),
            ],
            [
                "miscellaneous_test_dag",
                ("runme_0", "runme_1", "runme_2", "also_run_this", "run_after_loop", "run_this_last"),
            ],
            [
                "example_skip_dag",
                (
                    "always_true_1",
                    "always_true_2",
                    "skip_operator_1",
                    "skip_operator_2",
                    "all_success",
                    "one_success",
                    "final_1",
                    "final_2",
                ),
            ],
            ["latest_only", ("latest_only", "task1")],
        ],
    )
    def test_backfill_examples(self, dag_id, expected_execution_order):
        """
        Test backfilling example dags

        Try to backfill some of the example dags. Be careful, not all dags are suitable
        for doing this. For example, a dag that sleeps forever, or does not have a
        schedule won't work here since you simply can't backfill them.
        """
        dag = self.dagbag.get_dag(dag_id)

        logger.info("*** Running example DAG: %s", dag.dag_id)
        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_first_depends_on_past=True,
        )

        run_job(job=job, execute_callable=job_runner._execute)
        assert [
            ((dag_id, task_id, f"backfill__{DEFAULT_DATE.isoformat()}", 1, -1), (State.SUCCESS, None))
            for task_id in expected_execution_order
        ] == executor.sorted_tasks

    def test_backfill_conf(self, dag_maker):
        dag = self._get_dummy_dag(dag_maker, dag_id="test_backfill_conf")
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        conf_ = json.loads("""{"key": "value"}""")
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
            conf=conf_,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        # We ignore the first dag_run created by fixture
        dr = DagRun.find(
            dag_id="test_backfill_conf", execution_start_date=DEFAULT_DATE + datetime.timedelta(days=1)
        )

        assert conf_ == dr[0].conf

    @patch("airflow.jobs.backfill_job_runner.BackfillJobRunner.log")
    def test_backfill_respect_max_active_tis_per_dag_limit(self, mock_log, dag_maker):
        max_active_tis_per_dag = 2
        dag = self._get_dummy_dag(
            dag_maker,
            dag_id="test_backfill_respect_max_active_tis_per_dag_limit",
            max_active_tis_per_dag=max_active_tis_per_dag,
        )
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        run_job(job=job, execute_callable=job_runner._execute)

        assert len(executor.history) > 0

        task_concurrency_limit_reached_at_least_once = False

        num_running_task_instances = 0
        for running_task_instances in executor.history:
            assert len(running_task_instances) <= max_active_tis_per_dag
            num_running_task_instances += len(running_task_instances)
            if len(running_task_instances) == max_active_tis_per_dag:
                task_concurrency_limit_reached_at_least_once = True

        assert 8 == num_running_task_instances
        assert task_concurrency_limit_reached_at_least_once

        times_dag_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            DagConcurrencyLimitReached,
        )

        times_pool_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            NoAvailablePoolSlot,
        )

        times_task_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            TaskConcurrencyLimitReached,
        )

        assert 0 == times_pool_limit_reached_in_debug
        assert 0 == times_dag_concurrency_limit_reached_in_debug
        assert times_task_concurrency_limit_reached_in_debug > 0

    @pytest.mark.parametrize("with_max_active_tis_per_dag", [False, True])
    @patch("airflow.jobs.backfill_job_runner.BackfillJobRunner.log")
    def test_backfill_respect_max_active_tis_per_dagrun_limit(
        self, mock_log, dag_maker, with_max_active_tis_per_dag
    ):
        max_active_tis_per_dag = 3
        max_active_tis_per_dagrun = 2
        kwargs = {"max_active_tis_per_dagrun": max_active_tis_per_dagrun}
        if with_max_active_tis_per_dag:
            kwargs["max_active_tis_per_dag"] = max_active_tis_per_dag

        with dag_maker(dag_id="test_backfill_respect_max_active_tis_per_dag_limit", schedule="@daily") as dag:
            EmptyOperator.partial(task_id="task1", **kwargs).expand_kwargs([{"x": i} for i in range(10)])

        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        run_job(job=job, execute_callable=job_runner._execute)

        assert len(executor.history) > 0

        task_concurrency_limit_reached_at_least_once = False

        def get_running_tis_per_dagrun(running_tis):
            running_tis_per_dagrun_dict = defaultdict(int)
            for running_ti in running_tis:
                running_tis_per_dagrun_dict[running_ti[3].dag_run.id] += 1
            return running_tis_per_dagrun_dict

        num_running_task_instances = 0
        for running_task_instances in executor.history:
            if with_max_active_tis_per_dag:
                assert len(running_task_instances) <= max_active_tis_per_dag
            running_tis_per_dagrun_dict = get_running_tis_per_dagrun(running_task_instances)
            assert all(
                [
                    num_running_tis <= max_active_tis_per_dagrun
                    for num_running_tis in running_tis_per_dagrun_dict.values()
                ]
            )
            num_running_task_instances += len(running_task_instances)
            task_concurrency_limit_reached_at_least_once = (
                task_concurrency_limit_reached_at_least_once
                or any(
                    [
                        num_running_tis == max_active_tis_per_dagrun
                        for num_running_tis in running_tis_per_dagrun_dict.values()
                    ]
                )
            )

        assert 80 == num_running_task_instances  # (7 backfill run + 1 manual run ) * 10 mapped task per run
        assert task_concurrency_limit_reached_at_least_once

        times_dag_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            DagConcurrencyLimitReached,
        )

        times_pool_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            NoAvailablePoolSlot,
        )

        times_task_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            TaskConcurrencyLimitReached,
        )

        assert 0 == times_pool_limit_reached_in_debug
        assert 0 == times_dag_concurrency_limit_reached_in_debug
        assert times_task_concurrency_limit_reached_in_debug > 0

    @patch("airflow.jobs.backfill_job_runner.BackfillJobRunner.log")
    def test_backfill_respect_dag_concurrency_limit(self, mock_log, dag_maker):
        dag = self._get_dummy_dag(dag_maker, dag_id="test_backfill_respect_concurrency_limit")
        dag_maker.create_dagrun(state=None)
        dag.max_active_tasks = 2

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )
        run_job(job=job, execute_callable=job_runner._execute)

        assert len(executor.history) > 0

        concurrency_limit_reached_at_least_once = False

        num_running_task_instances = 0

        for running_task_instances in executor.history:
            assert len(running_task_instances) <= dag.max_active_tasks
            num_running_task_instances += len(running_task_instances)
            if len(running_task_instances) == dag.max_active_tasks:
                concurrency_limit_reached_at_least_once = True

        assert 8 == num_running_task_instances
        assert concurrency_limit_reached_at_least_once

        times_dag_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            DagConcurrencyLimitReached,
        )

        times_pool_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            NoAvailablePoolSlot,
        )

        times_task_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            TaskConcurrencyLimitReached,
        )

        assert 0 == times_pool_limit_reached_in_debug
        assert 0 == times_task_concurrency_limit_reached_in_debug
        assert times_dag_concurrency_limit_reached_in_debug > 0

    @patch("airflow.jobs.backfill_job_runner.BackfillJobRunner.log")
    def test_backfill_respect_default_pool_limit(self, mock_log, dag_maker):
        default_pool_slots = 2
        set_default_pool_slots(default_pool_slots)

        dag = self._get_dummy_dag(dag_maker, dag_id="test_backfill_with_no_pool_limit")
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        run_job(job=job, execute_callable=job_runner._execute)

        assert len(executor.history) > 0

        default_pool_task_slot_count_reached_at_least_once = False

        num_running_task_instances = 0

        # if no pool is specified, the number of tasks running in
        # parallel per backfill should be less than
        # default_pool slots at any point of time.
        for running_task_instances in executor.history:
            assert len(running_task_instances) <= default_pool_slots
            num_running_task_instances += len(running_task_instances)
            if len(running_task_instances) == default_pool_slots:
                default_pool_task_slot_count_reached_at_least_once = True

        assert 8 == num_running_task_instances
        assert default_pool_task_slot_count_reached_at_least_once

        times_dag_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            DagConcurrencyLimitReached,
        )

        times_pool_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            NoAvailablePoolSlot,
        )

        times_task_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            TaskConcurrencyLimitReached,
        )

        assert 0 == times_dag_concurrency_limit_reached_in_debug
        assert 0 == times_task_concurrency_limit_reached_in_debug
        assert times_pool_limit_reached_in_debug > 0

    def test_backfill_pool_not_found(self, dag_maker):
        dag = self._get_dummy_dag(
            dag_maker,
            dag_id="test_backfill_pool_not_found",
            pool="king_pool",
        )
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        try:
            run_job(job=job, execute_callable=job_runner._execute)
        except AirflowException:
            return

    @patch("airflow.jobs.backfill_job_runner.BackfillJobRunner.log")
    def test_backfill_respect_pool_limit(self, mock_log, dag_maker):
        session = settings.Session()

        slots = 2
        pool = Pool(
            pool="pool_with_two_slots",
            slots=slots,
        )
        session.add(pool)
        session.commit()

        dag = self._get_dummy_dag(
            dag_maker,
            dag_id="test_backfill_respect_pool_limit",
            pool=pool.pool,
        )
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        run_job(job=job, execute_callable=job_runner._execute)

        assert len(executor.history) > 0

        pool_was_full_at_least_once = False
        num_running_task_instances = 0

        for running_task_instances in executor.history:
            assert len(running_task_instances) <= slots
            num_running_task_instances += len(running_task_instances)
            if len(running_task_instances) == slots:
                pool_was_full_at_least_once = True

        assert 8 == num_running_task_instances
        assert pool_was_full_at_least_once

        times_dag_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            DagConcurrencyLimitReached,
        )

        times_pool_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            NoAvailablePoolSlot,
        )

        times_task_concurrency_limit_reached_in_debug = self._times_called_with(
            mock_log.debug,
            TaskConcurrencyLimitReached,
        )

        assert 0 == times_task_concurrency_limit_reached_in_debug
        assert 0 == times_dag_concurrency_limit_reached_in_debug
        assert times_pool_limit_reached_in_debug > 0

    def test_backfill_run_rescheduled(self, dag_maker):
        dag = self._get_dummy_dag(
            dag_maker, dag_id="test_backfill_run_rescheduled", task_id="test_backfill_run_rescheduled_task-1"
        )
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
        )
        run_job(job=job, execute_callable=job_runner._execute)

        ti = TI(task=dag.get_task("test_backfill_run_rescheduled_task-1"), execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        ti.set_state(State.UP_FOR_RESCHEDULE)

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
            rerun_failed_tasks=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)
        ti = TI(task=dag.get_task("test_backfill_run_rescheduled_task-1"), execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    def test_backfill_override_conf(self, dag_maker):
        dag = self._get_dummy_dag(
            dag_maker, dag_id="test_backfill_override_conf", task_id="test_backfill_override_conf-1"
        )
        dr = dag_maker.create_dagrun(
            state=None,
            start_date=DEFAULT_DATE,
        )

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
            conf={"a": 1},
        )

        with patch.object(
            job_runner,
            "_task_instances_for_dag_run",
            wraps=job_runner._task_instances_for_dag_run,
        ) as wrapped_task_instances_for_dag_run:
            run_job(job=job, execute_callable=job_runner._execute)
            dr = wrapped_task_instances_for_dag_run.call_args_list[0][0][1]
            assert dr.conf == {"a": 1}

    def test_backfill_skip_active_scheduled_dagrun(self, dag_maker, caplog):
        dag = self._get_dummy_dag(
            dag_maker,
            dag_id="test_backfill_skip_active_scheduled_dagrun",
            task_id="test_backfill_skip_active_scheduled_dagrun-1",
        )
        dag_maker.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
        )

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
        )
        with caplog.at_level(logging.ERROR, logger="airflow.jobs.backfill_job_runner.BackfillJob"):
            caplog.clear()
            run_job(job=job, execute_callable=job_runner._execute)
            assert "Backfill cannot be created for DagRun" in caplog.messages[0]

        ti = TI(
            task=dag.get_task("test_backfill_skip_active_scheduled_dagrun-1"), execution_date=DEFAULT_DATE
        )
        ti.refresh_from_db()
        # since DAG backfill is skipped, task state should be none
        assert ti.state == State.NONE

    def test_backfill_rerun_failed_tasks(self, dag_maker):
        dag = self._get_dummy_dag(
            dag_maker, dag_id="test_backfill_rerun_failed", task_id="test_backfill_rerun_failed_task-1"
        )
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
        )
        run_job(job=job, execute_callable=job_runner._execute)

        ti = TI(task=dag.get_task("test_backfill_rerun_failed_task-1"), execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        ti.set_state(State.FAILED)

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
            rerun_failed_tasks=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)
        ti = TI(task=dag.get_task("test_backfill_rerun_failed_task-1"), execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    def test_backfill_rerun_upstream_failed_tasks(self, dag_maker):

        with dag_maker(dag_id="test_backfill_rerun_upstream_failed", schedule="@daily") as dag:
            op1 = EmptyOperator(task_id="test_backfill_rerun_upstream_failed_task-1")
            op2 = EmptyOperator(task_id="test_backfill_rerun_upstream_failed_task-2")
            op1.set_upstream(op2)
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
        )
        run_job(job=job, execute_callable=job_runner._execute)

        ti = TI(task=dag.get_task("test_backfill_rerun_upstream_failed_task-1"), execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        ti.set_state(State.UPSTREAM_FAILED)

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
            rerun_failed_tasks=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)
        ti = TI(task=dag.get_task("test_backfill_rerun_upstream_failed_task-1"), execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    def test_backfill_rerun_failed_tasks_without_flag(self, dag_maker):
        dag = self._get_dummy_dag(
            dag_maker, dag_id="test_backfill_rerun_failed", task_id="test_backfill_rerun_failed_task-1"
        )
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
        )
        run_job(job=job, execute_callable=job_runner._execute)

        ti = TI(task=dag.get_task("test_backfill_rerun_failed_task-1"), execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        ti.set_state(State.FAILED)

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
            rerun_failed_tasks=False,
        )

        with pytest.raises(AirflowException):
            run_job(job=job, execute_callable=job_runner._execute)

    def test_backfill_retry_intermittent_failed_task(self, dag_maker):
        with dag_maker(
            dag_id="test_intermittent_failure_job",
            schedule="@daily",
            default_args={
                "retries": 2,
                "retry_delay": datetime.timedelta(seconds=0),
            },
        ) as dag:
            task1 = EmptyOperator(task_id="task1")
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor(parallelism=16)
        executor.mock_task_results[
            TaskInstanceKey(dag.dag_id, task1.task_id, DEFAULT_DATE, try_number=1)
        ] = State.UP_FOR_RETRY
        executor.mock_task_results[
            TaskInstanceKey(dag.dag_id, task1.task_id, DEFAULT_DATE, try_number=2)
        ] = State.UP_FOR_RETRY
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
        )
        run_job(job=job, execute_callable=job_runner._execute)

    def test_backfill_retry_always_failed_task(self, dag_maker):
        with dag_maker(
            dag_id="test_always_failure_job",
            schedule="@daily",
            default_args={
                "retries": 1,
                "retry_delay": datetime.timedelta(seconds=0),
            },
        ) as dag:
            task1 = EmptyOperator(task_id="task1")
        dr = dag_maker.create_dagrun(state=None)

        executor = MockExecutor(parallelism=16)
        executor.mock_task_results[
            TaskInstanceKey(dag.dag_id, task1.task_id, dr.run_id, try_number=1)
        ] = State.UP_FOR_RETRY
        executor.mock_task_fail(dag.dag_id, task1.task_id, dr.run_id, try_number=2)
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
        )
        with pytest.raises(BackfillUnfinished):
            run_job(job=job, execute_callable=job_runner._execute)

    def test_backfill_ordered_concurrent_execute(self, dag_maker):

        with dag_maker(
            dag_id="test_backfill_ordered_concurrent_execute",
            schedule="@daily",
        ) as dag:
            op1 = EmptyOperator(task_id="leave1")
            op2 = EmptyOperator(task_id="leave2")
            op3 = EmptyOperator(task_id="upstream_level_1")
            op4 = EmptyOperator(task_id="upstream_level_2")
            op5 = EmptyOperator(task_id="upstream_level_3")
            # order randomly
            op2.set_downstream(op3)
            op1.set_downstream(op3)
            op4.set_downstream(op5)
            op3.set_downstream(op4)
        runid0 = f"backfill__{DEFAULT_DATE.isoformat()}"
        dag_maker.create_dagrun(run_id=runid0)

        executor = MockExecutor(parallelism=16)
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=2),
        )
        run_job(job=job, execute_callable=job_runner._execute)

        runid1 = f"backfill__{(DEFAULT_DATE + datetime.timedelta(days=1)).isoformat()}"
        runid2 = f"backfill__{(DEFAULT_DATE + datetime.timedelta(days=2)).isoformat()}"

        # test executor history keeps a list
        history = executor.history

        assert [sorted(item[-1].key[1:3] for item in batch) for batch in history] == [
            [
                ("leave1", runid0),
                ("leave1", runid1),
                ("leave1", runid2),
                ("leave2", runid0),
                ("leave2", runid1),
                ("leave2", runid2),
            ],
            [("upstream_level_1", runid0), ("upstream_level_1", runid1), ("upstream_level_1", runid2)],
            [("upstream_level_2", runid0), ("upstream_level_2", runid1), ("upstream_level_2", runid2)],
            [("upstream_level_3", runid0), ("upstream_level_3", runid1), ("upstream_level_3", runid2)],
        ]

    def test_backfill_pooled_tasks(self):
        """
        Test that queued tasks are executed by BackfillJobRunner
        """
        session = settings.Session()
        pool = Pool(pool="test_backfill_pooled_task_pool", slots=1)
        session.add(pool)
        session.commit()
        session.close()

        dag = self.dagbag.get_dag("test_backfill_pooled_task_dag")
        dag.clear()

        executor = MockExecutor(do_update=True)
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(job=job, dag=dag, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        # run with timeout because this creates an infinite loop if not
        # caught
        try:
            with timeout(seconds=5):
                run_job(job=job, execute_callable=job_runner._execute)
        except AirflowTaskTimeout:
            pass
        ti = TI(task=dag.get_task("test_backfill_pooled_task"), execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    @pytest.mark.parametrize("ignore_depends_on_past", [True, False])
    def test_backfill_depends_on_past_works_independently_on_ignore_depends_on_past(
        self, ignore_depends_on_past
    ):
        dag = self.dagbag.get_dag("test_depends_on_past")
        dag.clear()
        run_date = DEFAULT_DATE + datetime.timedelta(days=5)

        job = Job(executor=MockExecutor())
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=run_date,
            end_date=run_date,
            ignore_first_depends_on_past=ignore_depends_on_past,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        # ti should have succeeded
        ti = TI(dag.tasks[0], run_date)
        ti.refresh_from_db()
        assert ti.state == State.SUCCESS

    def test_backfill_depends_on_past_backwards(self):
        """
        Test that CLI respects -B argument and raises on interaction with depends_on_past
        """
        dag_id = "test_depends_on_past"
        start_date = DEFAULT_DATE + datetime.timedelta(days=1)
        end_date = start_date + datetime.timedelta(days=1)
        kwargs = dict(
            start_date=start_date,
            end_date=end_date,
        )
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()

        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(job=job, dag=dag, ignore_first_depends_on_past=True, **kwargs)
        run_job(job=job, execute_callable=job_runner._execute)

        ti = TI(dag.get_task("test_dop_task"), end_date)
        ti.refresh_from_db()
        # runs fine forwards
        assert ti.state == State.SUCCESS

        # raises backwards
        expected_msg = "You cannot backfill backwards because one or more tasks depend_on_past: test_dop_task"
        with pytest.raises(AirflowException, match=expected_msg):
            executor = MockExecutor()
            job = Job(executor=executor)
            job_runner = BackfillJobRunner(job=job, dag=dag, run_backwards=True, **kwargs)
            run_job(job=job, execute_callable=job_runner._execute)

    def test_cli_receives_delay_arg(self):
        """
        Tests that the --delay argument is passed correctly to the BackfillJob
        """
        dag_id = "example_bash_operator"
        run_date = DEFAULT_DATE
        args = [
            "dags",
            "backfill",
            dag_id,
            "-s",
            run_date.isoformat(),
            "--delay-on-limit",
            "0.5",
        ]
        parsed_args = self.parser.parse_args(args)
        assert 0.5 == parsed_args.delay_on_limit

    def _get_dag_test_max_active_limits(
        self, dag_maker_fixture, dag_id="test_dag", max_active_runs=1, **kwargs
    ):
        with dag_maker_fixture(
            dag_id=dag_id,
            schedule="@hourly",
            max_active_runs=max_active_runs,
            **kwargs,
        ) as dag:
            op1 = EmptyOperator(task_id="leave1")
            op2 = EmptyOperator(task_id="leave2")
            op3 = EmptyOperator(task_id="upstream_level_1")
            op4 = EmptyOperator(task_id="upstream_level_2")

            op1 >> op2 >> op3
            op4 >> op3
        return dag

    def test_backfill_max_limit_check_within_limit(self, dag_maker):
        dag = self._get_dag_test_max_active_limits(
            dag_maker, dag_id="test_backfill_max_limit_check_within_limit", max_active_runs=16
        )
        dag_maker.create_dagrun(state=None)
        start_date = DEFAULT_DATE - datetime.timedelta(hours=1)
        end_date = DEFAULT_DATE

        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=start_date,
            end_date=end_date,
            donot_pickle=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        dagruns = DagRun.find(dag_id=dag.dag_id)
        assert 2 == len(dagruns)
        assert all(run.state == State.SUCCESS for run in dagruns)

    def test_backfill_notifies_dagrun_listener(self, dag_maker):
        dag = self._get_dummy_dag(dag_maker)
        dag_run = dag_maker.create_dagrun(state=None)
        dag_listener.clear()
        get_listener_manager().add_listener(dag_listener)

        start_date = DEFAULT_DATE - datetime.timedelta(hours=1)
        end_date = DEFAULT_DATE

        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=start_date,
            end_date=end_date,
            donot_pickle=True,
        )
        job.notification_threadpool = mock.MagicMock()
        run_job(job=job, execute_callable=job_runner._execute)

        assert len(dag_listener.running) == 1
        assert len(dag_listener.success) == 1
        assert dag_listener.running[0].dag.dag_id == dag_run.dag.dag_id
        assert dag_listener.running[0].run_id == dag_run.run_id
        assert dag_listener.running[0].state == DagRunState.RUNNING

        assert dag_listener.success[0].dag.dag_id == dag_run.dag.dag_id
        assert dag_listener.success[0].run_id == dag_run.run_id
        assert dag_listener.success[0].state == DagRunState.SUCCESS

    def test_backfill_max_limit_check(self, dag_maker):
        dag_id = "test_backfill_max_limit_check"
        run_id = "test_dag_run"
        start_date = DEFAULT_DATE - datetime.timedelta(hours=1)
        end_date = DEFAULT_DATE

        dag_run_created_cond = threading.Condition()

        def run_backfill(cond):
            cond.acquire()
            # this session object is different than the one in the main thread
            with create_session() as thread_session:
                try:
                    dag = self._get_dag_test_max_active_limits(
                        dag_maker,
                        dag_id=dag_id,
                    )
                    dag_maker.create_dagrun(
                        state=State.RUNNING,
                        # Existing dagrun that is not within the backfill range
                        run_id=run_id,
                        execution_date=DEFAULT_DATE + datetime.timedelta(hours=1),
                    )
                    thread_session.commit()
                    cond.notify()
                except Exception:
                    logger.exception("Exception when creating DagRun")
                finally:
                    cond.release()
                    thread_session.close()

                executor = MockExecutor()
                job = Job(
                    executor=executor,
                )
                job_runner = BackfillJobRunner(
                    job=job,
                    dag=dag,
                    start_date=start_date,
                    end_date=end_date,
                    donot_pickle=True,
                )
                run_job(job=job, execute_callable=job_runner._execute)

        backfill_job_thread = threading.Thread(
            target=run_backfill, name="run_backfill", args=(dag_run_created_cond,)
        )

        dag_run_created_cond.acquire()
        with create_session() as session:
            backfill_job_thread.start()
            try:
                # at this point backfill can't run since the max_active_runs has been
                # reached, so it is waiting
                dag_run_created_cond.wait(timeout=1.5)
                dagruns = DagRun.find(dag_id=dag_id)
                logger.info("The dag runs retrieved: %s", dagruns)
                assert 1 == len(dagruns)
                dr = dagruns[0]
                assert dr.run_id == run_id

                # allow the backfill to execute
                # by setting the existing dag run to SUCCESS,
                # backfill will execute dag runs 1 by 1
                dr.set_state(State.SUCCESS)
                session.merge(dr)
                session.commit()

                backfill_job_thread.join()

                dagruns = DagRun.find(dag_id=dag_id)
                assert 3 == len(dagruns)  # 2 from backfill + 1 existing
                assert dagruns[-1].run_id == dr.run_id
            finally:
                dag_run_created_cond.release()

    def test_backfill_max_limit_check_no_count_existing(self, dag_maker):
        start_date = DEFAULT_DATE
        end_date = DEFAULT_DATE
        # Existing dagrun that is within the backfill range
        dag = self._get_dag_test_max_active_limits(
            dag_maker, dag_id="test_backfill_max_limit_check_no_count_existing"
        )
        dag_maker.create_dagrun(state=None)

        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job, dag=dag, start_date=start_date, end_date=end_date, donot_pickle=True
        )
        run_job(job=job, execute_callable=job_runner._execute)

        # BackfillJobRunner will run since the existing DagRun does not count for the max
        # active limit since it's within the backfill date range.
        dagruns = DagRun.find(dag_id=dag.dag_id)
        # will only be able to run 1 (the existing one) since there's just
        # one dag run slot left given the max_active_runs limit
        assert 1 == len(dagruns)
        assert State.SUCCESS == dagruns[0].state

    def test_backfill_max_limit_check_complete_loop(self, dag_maker):
        dag = self._get_dag_test_max_active_limits(
            dag_maker, dag_id="test_backfill_max_limit_check_complete_loop"
        )
        dag_maker.create_dagrun(state=None)
        start_date = DEFAULT_DATE - datetime.timedelta(hours=1)
        end_date = DEFAULT_DATE

        # Given the max limit to be 1 in active dag runs, we need to run the
        # backfill job 3 times
        success_expected = 2
        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=start_date,
            end_date=end_date,
            donot_pickle=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        success_dagruns = len(DagRun.find(dag_id=dag.dag_id, state=State.SUCCESS))
        running_dagruns = len(DagRun.find(dag_id=dag.dag_id, state=State.RUNNING))
        assert success_expected == success_dagruns
        assert 0 == running_dagruns  # no dag_runs in running state are left

    def test_sub_set_subdag(self, dag_maker):

        with dag_maker(
            "test_sub_set_subdag",
        ) as dag:
            op1 = EmptyOperator(task_id="leave1")
            op2 = EmptyOperator(task_id="leave2")
            op3 = EmptyOperator(task_id="upstream_level_1")
            op4 = EmptyOperator(task_id="upstream_level_2")
            op5 = EmptyOperator(task_id="upstream_level_3")
            # order randomly
            op2.set_downstream(op3)
            op1.set_downstream(op3)
            op4.set_downstream(op5)
            op3.set_downstream(op4)

        dr = dag_maker.create_dagrun(state=None)

        executor = MockExecutor()
        sub_dag = dag.partial_subset(
            task_ids_or_regex="leave*", include_downstream=False, include_upstream=False
        )
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(job=job, dag=sub_dag, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        run_job(job=job, execute_callable=job_runner._execute)

        for ti in dr.get_task_instances():
            if ti.task_id == "leave1" or ti.task_id == "leave2":
                assert State.SUCCESS == ti.state
            else:
                assert State.NONE == ti.state

    def test_backfill_fill_blanks(self, dag_maker):
        with dag_maker(
            "test_backfill_fill_blanks",
        ) as dag:
            op1 = EmptyOperator(task_id="op1")
            op2 = EmptyOperator(task_id="op2")
            op3 = EmptyOperator(task_id="op3")
            op4 = EmptyOperator(task_id="op4")
            op5 = EmptyOperator(task_id="op5")
            op6 = EmptyOperator(task_id="op6")

        dr = dag_maker.create_dagrun(state=None)

        executor = MockExecutor()

        session = settings.Session()

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id == op1.task_id:
                ti.state = State.UP_FOR_RETRY
                ti.end_date = DEFAULT_DATE
            elif ti.task_id == op2.task_id:
                ti.state = State.FAILED
            elif ti.task_id == op3.task_id:
                ti.state = State.SKIPPED
            elif ti.task_id == op4.task_id:
                ti.state = State.SCHEDULED
            elif ti.task_id == op5.task_id:
                ti.state = State.UPSTREAM_FAILED
            # op6 = None
            session.merge(ti)
        session.commit()
        session.close()

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(job=job, dag=dag, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        with pytest.raises(AirflowException, match="Some task instances failed"):
            run_job(job=job, execute_callable=job_runner._execute)

        dr.refresh_from_db()

        assert dr.state == State.FAILED

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id in (op1.task_id, op4.task_id, op6.task_id):
                assert ti.state == State.SUCCESS
            elif ti.task_id == op2.task_id:
                assert ti.state == State.FAILED
            elif ti.task_id == op3.task_id:
                assert ti.state == State.SKIPPED
            elif ti.task_id == op5.task_id:
                assert ti.state == State.UPSTREAM_FAILED

    def test_backfill_execute_subdag(self):
        dag = self.dagbag.get_dag("example_subdag_operator")
        subdag_op_task = dag.get_task("section-1")

        subdag = subdag_op_task.subdag
        subdag.timetable = cron_timetable("@daily")

        start_date = timezone.utcnow()
        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=subdag,
            start_date=start_date,
            end_date=start_date,
            donot_pickle=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        subdag_op_task.pre_execute(context={"execution_date": start_date})
        subdag_op_task.execute(context={"execution_date": start_date})
        subdag_op_task.post_execute(context={"execution_date": start_date})

        history = executor.history
        subdag_history = history[0]

        # check that all 5 task instances of the subdag 'section-1' were executed
        assert 5 == len(subdag_history)
        for sdh in subdag_history:
            ti = sdh[3]
            assert "section-1-task-" in ti.task_id

        with create_session() as session:
            successful_subdag_runs = (
                session.query(DagRun)
                .filter(DagRun.dag_id == subdag.dag_id)
                .filter(DagRun.execution_date == start_date)
                .filter(DagRun.state == State.SUCCESS)
                .count()
            )

            assert 1 == successful_subdag_runs

        subdag.clear()
        dag.clear()

    def test_subdag_clear_parentdag_downstream_clear(self):
        dag = self.dagbag.get_dag("clear_subdag_test_dag")
        subdag_op_task = dag.get_task("daily_job")

        subdag = subdag_op_task.subdag

        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            donot_pickle=True,
        )

        with timeout(seconds=30):
            run_job(job=job, execute_callable=job_runner._execute)

        ti_subdag = TI(task=dag.get_task("daily_job"), execution_date=DEFAULT_DATE)
        ti_subdag.refresh_from_db()
        assert ti_subdag.state == State.SUCCESS

        ti_irrelevant = TI(task=dag.get_task("daily_job_irrelevant"), execution_date=DEFAULT_DATE)
        ti_irrelevant.refresh_from_db()
        assert ti_irrelevant.state == State.SUCCESS

        ti_downstream = TI(task=dag.get_task("daily_job_downstream"), execution_date=DEFAULT_DATE)
        ti_downstream.refresh_from_db()
        assert ti_downstream.state == State.SUCCESS

        sdag = subdag.partial_subset(
            task_ids_or_regex="daily_job_subdag_task", include_downstream=True, include_upstream=False
        )

        sdag.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, include_parentdag=True)

        ti_subdag.refresh_from_db()
        assert State.NONE == ti_subdag.state

        ti_irrelevant.refresh_from_db()
        assert State.SUCCESS == ti_irrelevant.state

        ti_downstream.refresh_from_db()
        assert State.NONE == ti_downstream.state

        subdag.clear()
        dag.clear()

    def test_backfill_execute_subdag_with_removed_task(self):
        """
        Ensure that subdag operators execute properly in the case where
        an associated task of the subdag has been removed from the dag
        definition, but has instances in the database from previous runs.
        """
        dag = self.dagbag.get_dag("example_subdag_operator")
        subdag = dag.get_task("section-1").subdag

        session = settings.Session()
        executor = MockExecutor()
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=subdag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            donot_pickle=True,
        )
        dr = DagRun(
            dag_id=subdag.dag_id, execution_date=DEFAULT_DATE, run_id="test", run_type=DagRunType.BACKFILL_JOB
        )
        session.add(dr)

        removed_task_ti = TI(
            task=EmptyOperator(task_id="removed_task"), run_id=dr.run_id, state=State.REMOVED
        )
        removed_task_ti.dag_id = subdag.dag_id
        dr.task_instances.append(removed_task_ti)

        session.commit()

        with timeout(seconds=30):
            run_job(job=job, execute_callable=job_runner._execute)

        for task in subdag.tasks:
            instance = (
                session.query(TI)
                .filter(
                    TI.dag_id == subdag.dag_id, TI.task_id == task.task_id, TI.execution_date == DEFAULT_DATE
                )
                .first()
            )

            assert instance is not None
            assert instance.state == State.SUCCESS

        removed_task_ti.refresh_from_db()
        assert removed_task_ti.state == State.REMOVED

        subdag.clear()
        dag.clear()

    def test_update_counters(self, dag_maker, session):
        with dag_maker(dag_id="test_manage_executor_state", start_date=DEFAULT_DATE, session=session) as dag:
            task1 = EmptyOperator(task_id="dummy", owner="airflow")
        dr = dag_maker.create_dagrun(state=None)
        job = Job()
        job_runner = BackfillJobRunner(job=job, dag=dag)
        ti = TI(task1, dr.execution_date)
        ti.refresh_from_db()

        ti_status = BackfillJobRunner._DagRunTaskStatus()

        # Test for success
        # The in-memory task key in ti_status.running contains a try_number
        # that is not in sync with the DB. To test that _update_counters method
        # handles this, we mark the task as running in-memory and then increase
        # the try number as it would be before the raw task is executed.
        # When updating the counters the in-memory key will be used which will
        # match what's in the in-memory ti_status.running map. This is the same
        # for skipped, failed and retry states.
        ti_status.running[ti.key] = ti  # Task is queued and marked as running
        ti._try_number += 1  # Try number is increased during ti.run()
        ti.set_state(State.SUCCESS, session)  # Task finishes with success state
        job_runner._update_counters(ti_status=ti_status, session=session)  # Update counters
        assert len(ti_status.running) == 0
        assert len(ti_status.succeeded) == 1
        assert len(ti_status.skipped) == 0
        assert len(ti_status.failed) == 0
        assert len(ti_status.to_run) == 0

        ti_status.succeeded.clear()

        # Test for success when DB try_number is off from in-memory expectations
        ti_status.running[ti.key] = ti
        ti._try_number += 2
        ti.set_state(State.SUCCESS, session)
        job_runner._update_counters(ti_status=ti_status, session=session)
        assert len(ti_status.running) == 0
        assert len(ti_status.succeeded) == 1
        assert len(ti_status.skipped) == 0
        assert len(ti_status.failed) == 0
        assert len(ti_status.to_run) == 0

        ti_status.succeeded.clear()

        # Test for skipped
        ti_status.running[ti.key] = ti
        ti._try_number += 1
        ti.set_state(State.SKIPPED, session)
        job_runner._update_counters(ti_status=ti_status, session=session)
        assert len(ti_status.running) == 0
        assert len(ti_status.succeeded) == 0
        assert len(ti_status.skipped) == 1
        assert len(ti_status.failed) == 0
        assert len(ti_status.to_run) == 0

        ti_status.skipped.clear()

        # Test for failed
        ti_status.running[ti.key] = ti
        ti._try_number += 1
        ti.set_state(State.FAILED, session)
        job_runner._update_counters(ti_status=ti_status, session=session)
        assert len(ti_status.running) == 0
        assert len(ti_status.succeeded) == 0
        assert len(ti_status.skipped) == 0
        assert len(ti_status.failed) == 1
        assert len(ti_status.to_run) == 0

        ti_status.failed.clear()

        # Test for retry
        ti_status.running[ti.key] = ti
        ti._try_number += 1
        ti.set_state(State.UP_FOR_RETRY, session)
        job_runner._update_counters(ti_status=ti_status, session=session)
        assert len(ti_status.running) == 0
        assert len(ti_status.succeeded) == 0
        assert len(ti_status.skipped) == 0
        assert len(ti_status.failed) == 0
        assert len(ti_status.to_run) == 1

        ti_status.to_run.clear()

        # Test for reschedule
        # Logic in taskinstance reduces the try number for a task that's been
        # rescheduled (which makes sense because it's the _same_ try, but it's
        # just being rescheduled to a later time). This now makes the in-memory
        # and DB representation of the task try_number the _same_, which is unlike
        # the above cases. But this is okay because the in-memory key is used.
        ti_status.running[ti.key] = ti  # Task queued and marked as running
        # Note: Both the increase and decrease are kept here for context
        ti._try_number += 1  # Try number is increased during ti.run()
        ti._try_number -= 1  # Task is being rescheduled, decrement try_number
        ti.set_state(State.UP_FOR_RESCHEDULE, session)  # Task finishes with reschedule state
        job_runner._update_counters(ti_status=ti_status, session=session)
        assert len(ti_status.running) == 0
        assert len(ti_status.succeeded) == 0
        assert len(ti_status.skipped) == 0
        assert len(ti_status.failed) == 0
        assert len(ti_status.to_run) == 1

        ti_status.to_run.clear()

        # test for none
        ti.set_state(State.NONE, session)
        # Setting ti._try_number = 0 brings us to ti.try_number==1
        # so that the in-memory key access will work fine
        ti._try_number = 0
        assert ti.try_number == 1  # see ti.try_number property in taskinstance module
        session.merge(ti)
        session.commit()
        ti_status.running[ti.key] = ti
        job_runner._update_counters(ti_status=ti_status, session=session)
        assert len(ti_status.running) == 0
        assert len(ti_status.succeeded) == 0
        assert len(ti_status.skipped) == 0
        assert len(ti_status.failed) == 0
        assert len(ti_status.to_run) == 1

        ti_status.to_run.clear()

        # test for scheduled
        ti.set_state(State.SCHEDULED)
        # Deferred tasks are put into scheduled by the triggerer
        # Check that they are put into to_run
        ti_status.running[ti.key] = ti
        job_runner._update_counters(ti_status=ti_status, session=session)
        assert len(ti_status.running) == 0
        assert len(ti_status.succeeded) == 0
        assert len(ti_status.skipped) == 0
        assert len(ti_status.failed) == 0
        assert len(ti_status.to_run) == 1

        ti_status.to_run.clear()
        # test for deferred
        # if a task is deferred and it's not yet time for the triggerer
        # to reschedule it, we should leave it in ti_status.running
        ti.set_state(State.DEFERRED)
        ti_status.running[ti.key] = ti
        job_runner._update_counters(ti_status=ti_status, session=session)
        assert len(ti_status.running) == 1
        assert len(ti_status.succeeded) == 0
        assert len(ti_status.skipped) == 0
        assert len(ti_status.failed) == 0
        assert len(ti_status.to_run) == 0
        session.close()

    def test_dag_dagrun_infos_between(self, dag_maker):
        with dag_maker(
            dag_id="dagrun_infos_between", start_date=DEFAULT_DATE, schedule="@hourly"
        ) as test_dag:
            EmptyOperator(
                task_id="dummy",
                owner="airflow",
            )

        assert [DEFAULT_DATE] == [
            info.logical_date
            for info in test_dag.iter_dagrun_infos_between(
                earliest=DEFAULT_DATE,
                latest=DEFAULT_DATE,
            )
        ]
        assert [
            DEFAULT_DATE - datetime.timedelta(hours=3),
            DEFAULT_DATE - datetime.timedelta(hours=2),
            DEFAULT_DATE - datetime.timedelta(hours=1),
            DEFAULT_DATE,
        ] == [
            info.logical_date
            for info in test_dag.iter_dagrun_infos_between(
                earliest=DEFAULT_DATE - datetime.timedelta(hours=3),
                latest=DEFAULT_DATE,
            )
        ]

    def test_backfill_run_backwards(self):
        dag = self.dagbag.get_dag("test_start_date_scheduling")
        dag.clear()

        executor = MockExecutor(parallelism=16)

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            run_backwards=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        session = settings.Session()
        tis = (
            session.query(TI)
            .join(TI.dag_run)
            .filter(TI.dag_id == "test_start_date_scheduling" and TI.task_id == "dummy")
            .order_by(DagRun.execution_date)
            .all()
        )

        queued_times = [ti.queued_dttm for ti in tis]
        assert queued_times == sorted(queued_times, reverse=True)
        assert all(ti.state == State.SUCCESS for ti in tis)

        dag.clear()
        session.close()

    def test_reset_orphaned_tasks_with_orphans(self, dag_maker):
        """Create dagruns and ensure only ones with correct states are reset."""
        prefix = "backfill_job_test_test_reset_orphaned_tasks"
        states = [State.QUEUED, State.SCHEDULED, State.NONE, State.RUNNING, State.SUCCESS]
        states_to_reset = [State.QUEUED, State.SCHEDULED, State.NONE]

        tasks = []
        with dag_maker(dag_id=prefix) as dag:
            for i in range(len(states)):
                task_id = f"{prefix}_task_{i}"
                task = EmptyOperator(task_id=task_id)
                tasks.append(task)

        session = settings.Session()
        job = Job()
        job_runner = BackfillJobRunner(job=job, dag=dag)
        # create dagruns
        dr1 = dag_maker.create_dagrun(state=State.RUNNING)
        dr2 = dag.create_dagrun(run_id="test2", state=State.SUCCESS)

        # create taskinstances and set states
        dr1_tis = []
        dr2_tis = []
        for i, (task, state) in enumerate(zip(tasks, states)):
            ti1 = TI(task, dr1.execution_date)
            ti2 = TI(task, dr2.execution_date)
            ti1.refresh_from_db()
            ti2.refresh_from_db()
            ti1.state = state
            ti2.state = state
            dr1_tis.append(ti1)
            dr2_tis.append(ti2)
            session.merge(ti1)
            session.merge(ti2)
            session.commit()

        assert 2 == job_runner.reset_state_for_orphaned_tasks()

        for ti in dr1_tis + dr2_tis:
            ti.refresh_from_db()

        # running dagrun should be reset
        for state, ti in zip(states, dr1_tis):
            if state in states_to_reset:
                assert ti.state is None
            else:
                assert state == ti.state

        # otherwise not
        for state, ti in zip(states, dr2_tis):
            assert state == ti.state

        for state, ti in zip(states, dr1_tis):
            ti.state = state
        session.commit()

        job_runner.reset_state_for_orphaned_tasks(filter_by_dag_run=dr1, session=session)

        # check same for dag_run version
        for state, ti in zip(states, dr2_tis):
            assert state == ti.state

    def test_reset_orphaned_tasks_specified_dagrun(self, session, dag_maker):
        """Try to reset when we specify a dagrun and ensure nothing else is."""
        dag_id = "test_reset_orphaned_tasks_specified_dagrun"
        task_id = dag_id + "_task"
        with dag_maker(
            dag_id=dag_id,
            start_date=DEFAULT_DATE,
            schedule="@daily",
            session=session,
        ) as dag:
            EmptyOperator(task_id=task_id, dag=dag)

        job = Job()
        job_runner = BackfillJobRunner(job=job, dag=dag)
        # make two dagruns, only reset for one
        dr1 = dag_maker.create_dagrun(state=State.SUCCESS)
        dr2 = dag.create_dagrun(run_id="test2", state=State.RUNNING, session=session)
        ti1 = dr1.get_task_instances(session=session)[0]
        ti2 = dr2.get_task_instances(session=session)[0]
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED

        session.merge(ti1)
        session.merge(ti2)
        session.merge(dr1)
        session.merge(dr2)
        session.flush()

        num_reset_tis = job_runner.reset_state_for_orphaned_tasks(filter_by_dag_run=dr2, session=session)
        assert 1 == num_reset_tis
        ti1.refresh_from_db(session=session)
        ti2.refresh_from_db(session=session)
        assert State.SCHEDULED == ti1.state
        assert State.NONE == ti2.state

    def test_job_id_is_assigned_to_dag_run(self, dag_maker):
        dag_id = "test_job_id_is_assigned_to_dag_run"
        with dag_maker(dag_id=dag_id, start_date=DEFAULT_DATE, schedule="@daily") as dag:
            EmptyOperator(task_id="dummy_task", dag=dag)

        job = Job(executor=MockExecutor())
        job_runner = BackfillJobRunner(
            job=job, dag=dag, start_date=timezone.utcnow() - datetime.timedelta(days=1)
        )
        run_job(job=job, execute_callable=job_runner._execute)
        dr: DagRun = dag.get_last_dagrun()
        assert dr.creating_job_id == job.id

    def test_backfill_has_job_id_int(self):
        """Make sure that backfill jobs are assigned job_ids and that the job_id is an int."""
        dag = self.dagbag.get_dag("test_start_date_scheduling")
        dag.clear()

        executor = MockExecutor(parallelism=16)

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            run_backwards=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)
        assert isinstance(executor.job_id, int)

    @pytest.mark.long_running
    @pytest.mark.parametrize("executor_name", ["SequentialExecutor", "DebugExecutor"])
    @pytest.mark.parametrize("dag_id", ["test_mapped_classic", "test_mapped_taskflow", "test_sensor"])
    def test_backfilling_dags(self, dag_id, executor_name, session):
        """
        End-to-end test for backfilling dags with various executors.

        We test with multiple executors as they have different "execution environments" -- for instance
        DebugExecutor runs a lot more in the same process than other Executors.

        """
        # This test needs a real executor to run, so that the `make_list` task can write out the TaskMap
        from airflow.executors.executor_loader import ExecutorLoader

        self.dagbag.process_file(str(TEST_DAGS_FOLDER / f"{dag_id}.py"))
        dag = self.dagbag.get_dag(dag_id)

        when = timezone.datetime(2022, 1, 1)

        job = Job(executor=ExecutorLoader.load_executor(executor_name))
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=when,
            end_date=when,
            donot_pickle=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        dr = DagRun.find(dag_id=dag.dag_id, execution_date=when, session=session)[0]
        assert dr
        assert dr.state == DagRunState.SUCCESS

        # Check that every task has a start and end date
        for ti in dr.task_instances:
            assert ti.state == TaskInstanceState.SUCCESS
            assert ti.start_date is not None
            assert ti.end_date is not None

    def test_mapped_dag_pre_existing_tis(self, dag_maker, session):
        """If the DagRun already has some mapped TIs, ensure that we re-run them successfully"""
        from airflow.decorators import task
        from airflow.operators.python import PythonOperator

        list_result = [[1], [2], [{"a": "b"}]]

        @task
        def make_arg_lists():
            return list_result

        def consumer(value):
            print(repr(value))

        with dag_maker(session=session) as dag:
            consumer_op = PythonOperator.partial(task_id="consumer", python_callable=consumer).expand(
                op_args=make_arg_lists()
            )
            PythonOperator.partial(task_id="consumer_literal", python_callable=consumer).expand(
                op_args=[[1], [2], [3]],
            )

        dr = dag_maker.create_dagrun()

        # Create the existing mapped TIs -- this the crucial part of this test
        ti = dr.get_task_instance("consumer", session=session)
        ti.map_index = 0
        for map_index in range(1, 3):
            ti = TI(consumer_op, run_id=dr.run_id, map_index=map_index)
            ti.dag_run = dr
            session.add(ti)
        session.flush()

        executor = MockExecutor()

        ti_status = BackfillJobRunner._DagRunTaskStatus()
        ti_status.active_runs.append(dr)
        ti_status.to_run = {ti.key: ti for ti in dr.task_instances}

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=dr.execution_date,
            end_date=dr.execution_date,
            donot_pickle=True,
        )

        executor_change_state = executor.change_state

        def on_change_state(key, state, info=None):
            if key.task_id == "make_arg_lists":
                session.add(
                    TaskMap(
                        length=len(list_result),
                        keys=None,
                        dag_id=key.dag_id,
                        run_id=key.run_id,
                        task_id=key.task_id,
                        map_index=key.map_index,
                    )
                )
                session.flush()
            executor_change_state(key, state, info)

        with patch.object(executor, "change_state", side_effect=on_change_state):
            job_runner._process_backfill_task_instances(
                ti_status=ti_status,
                executor=job.executor,
                start_date=dr.execution_date,
                pickle_id=None,
                session=session,
            )
        assert ti_status.failed == set()
        assert ti_status.succeeded == {
            TaskInstanceKey(dag_id=dr.dag_id, task_id="consumer", run_id="test", try_number=1, map_index=0),
            TaskInstanceKey(dag_id=dr.dag_id, task_id="consumer", run_id="test", try_number=1, map_index=1),
            TaskInstanceKey(dag_id=dr.dag_id, task_id="consumer", run_id="test", try_number=1, map_index=2),
            TaskInstanceKey(
                dag_id=dr.dag_id, task_id="consumer_literal", run_id="test", try_number=1, map_index=0
            ),
            TaskInstanceKey(
                dag_id=dr.dag_id, task_id="consumer_literal", run_id="test", try_number=1, map_index=1
            ),
            TaskInstanceKey(
                dag_id=dr.dag_id, task_id="consumer_literal", run_id="test", try_number=1, map_index=2
            ),
            TaskInstanceKey(
                dag_id=dr.dag_id, task_id="make_arg_lists", run_id="test", try_number=1, map_index=-1
            ),
        }

    def test_mapped_dag_unexpandable(self, dag_maker, session):
        with dag_maker(session=session) as dag:

            @dag.task
            def get_things():
                return [1, 2]

            @dag.task
            def this_fails() -> None:
                raise RuntimeError("sorry!")

            @dag.task(trigger_rule=TriggerRule.ALL_DONE)
            def consumer(a, b):
                print(a, b)

            consumer.expand(a=get_things(), b=this_fails())

        executor = MockExecutor()
        when = timezone.datetime(2022, 1, 1)
        job = Job(executor=executor)
        job_runner = BackfillJobRunner(job=job, dag=dag, start_date=when, end_date=when, donot_pickle=True)
        run_job(job=job, execute_callable=job_runner._execute)
        (dr,) = DagRun.find(dag_id=dag.dag_id, execution_date=when, session=session)
        assert dr.state == DagRunState.FAILED

        # Check that every task has a start and end date
        tis = {(ti.task_id, ti.map_index): ti for ti in dr.task_instances}
        assert len(tis) == 3
        tis[("get_things", -1)].state == TaskInstanceState.SUCCESS
        tis[("this_fails", -1)].state == TaskInstanceState.FAILED
        tis[("consumer", -1)].state == TaskInstanceState.UPSTREAM_FAILED

    def test_start_date_set_for_resetted_dagruns(self, dag_maker, session, caplog):

        with dag_maker() as dag:
            EmptyOperator(task_id="task1")

        dr = dag_maker.create_dagrun()
        dr.state = State.SUCCESS
        session.merge(dr)
        session.flush()
        dag.clear()
        job = Job(executor=MockExecutor())
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            donot_pickle=True,
        )
        run_job(job=job, execute_callable=job_runner._execute)

        (dr,) = DagRun.find(dag_id=dag.dag_id, execution_date=DEFAULT_DATE, session=session)
        assert dr.start_date
        assert f"Failed to record duration of {dr}" not in caplog.text

    def test_task_instances_are_not_set_to_scheduled_when_dagrun_reset(self, dag_maker, session):
        """Test that when dagrun is reset, task instances are not set to scheduled"""

        with dag_maker() as dag:
            task1 = EmptyOperator(task_id="task1")
            task2 = EmptyOperator(task_id="task2")
            task3 = EmptyOperator(task_id="task3")
            task1 >> task2 >> task3

        for i in range(1, 4):
            dag_maker.create_dagrun(
                run_id=f"test_dagrun_{i}", execution_date=DEFAULT_DATE + datetime.timedelta(days=i)
            )

        dag.clear()

        job = Job(executor=MockExecutor())
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE + datetime.timedelta(days=1),
            end_date=DEFAULT_DATE + datetime.timedelta(days=4),
            donot_pickle=True,
        )
        for dr in DagRun.find(dag_id=dag.dag_id, session=session):
            tasks_to_run = job_runner._task_instances_for_dag_run(dag, dr, session=session)
            states = [ti.state for _, ti in tasks_to_run.items()]
            assert TaskInstanceState.SCHEDULED in states
            assert State.NONE in states

    @pytest.mark.parametrize(
        ["disable_retry", "try_number", "exception"],
        (
            (True, 1, BackfillUnfinished),
            (False, 2, AirflowException),
        ),
    )
    def test_backfill_disable_retry(self, dag_maker, disable_retry, try_number, exception):
        with dag_maker(
            dag_id="test_disable_retry",
            schedule_interval="@daily",
            default_args={
                "retries": 2,
                "retry_delay": datetime.timedelta(seconds=3),
            },
        ) as dag:
            task1 = EmptyOperator(task_id="task1")
        dag_run = dag_maker.create_dagrun(state=None)

        executor = MockExecutor(parallelism=16)
        executor.mock_task_results[
            TaskInstanceKey(dag.dag_id, task1.task_id, dag_run.run_id, try_number=1)
        ] = TaskInstanceState.UP_FOR_RETRY
        executor.mock_task_results[
            TaskInstanceKey(dag.dag_id, task1.task_id, dag_run.run_id, try_number=2)
        ] = TaskInstanceState.FAILED

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            disable_retry=disable_retry,
        )
        with pytest.raises(exception):
            run_job(job=job, execute_callable=job_runner._execute)
        ti = dag_run.get_task_instance(task_id=task1.task_id)

        assert ti._try_number == try_number

        dag_run.refresh_from_db()

        assert dag_run.state == DagRunState.FAILED

        dag.clear()
