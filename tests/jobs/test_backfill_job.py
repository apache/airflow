# -*- coding: utf-8 -*-
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
#

import datetime
import json
import logging
import threading
import unittest

import sqlalchemy
from parameterized import parameterized

from airflow import AirflowException, settings
from airflow import configuration
from airflow.bin import cli
from airflow.exceptions import AirflowTaskTimeout
from airflow.exceptions import DagConcurrencyLimitReached, NoAvailablePoolSlot, TaskConcurrencyLimitReached
from airflow.jobs import BackfillJob, SchedulerJob
from airflow.models import DAG, DagBag, DagRun, Pool, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from tests.compat import Mock, patch
from tests.executors.test_executor import TestExecutor
from tests.test_utils.db import clear_db_pools, \
    clear_db_runs, set_default_pool_slots

logger = logging.getLogger(__name__)

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestBackfillJob(unittest.TestCase):

    def _get_dummy_dag(self, dag_id, pool=Pool.DEFAULT_POOL_NAME, task_concurrency=None):
        dag = DAG(
            dag_id=dag_id,
            start_date=DEFAULT_DATE,
            schedule_interval='@daily')

        with dag:
            DummyOperator(
                task_id='op',
                pool=pool,
                task_concurrency=task_concurrency,
                dag=dag)

        dag.clear()
        return dag

    def _times_called_with(self, method, class_):
        count = 0
        for args in method.call_args_list:
            if isinstance(args[0][0], class_):
                count += 1
        return count

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag(include_examples=True)

    def setUp(self):
        clear_db_runs()
        clear_db_pools()

        self.parser = cli.CLIFactory.get_parser()

    def test_unfinished_dag_runs_set_to_failed(self):
        dag = self._get_dummy_dag('dummy_dag')

        dag_run = dag.create_dagrun(
            run_id='test',
            state=State.RUNNING,
        )

        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=8),
            ignore_first_depends_on_past=True
        )

        job._set_unfinished_dag_runs_to_failed([dag_run])

        dag_run.refresh_from_db()

        self.assertEqual(State.FAILED, dag_run.state)

    def test_dag_run_with_finished_tasks_set_to_success(self):
        dag = self._get_dummy_dag('dummy_dag')

        dag_run = dag.create_dagrun(
            run_id='test',
            state=State.RUNNING,
        )

        for ti in dag_run.get_task_instances():
            ti.set_state(State.SUCCESS)

        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=8),
            ignore_first_depends_on_past=True
        )

        job._set_unfinished_dag_runs_to_failed([dag_run])

        dag_run.refresh_from_db()

        self.assertEqual(State.SUCCESS, dag_run.state)

    @unittest.skipIf('sqlite' in configuration.conf.get('core', 'sql_alchemy_conn'),
                     "concurrent access not supported in sqlite")
    def test_trigger_controller_dag(self):
        dag = self.dagbag.get_dag('example_trigger_controller_dag')
        target_dag = self.dagbag.get_dag('example_trigger_target_dag')
        target_dag.sync_to_db()

        scheduler = SchedulerJob()
        task_instances_list = Mock()
        scheduler._process_task_instances(target_dag,
                                          task_instances_list=task_instances_list)
        self.assertFalse(task_instances_list.append.called)

        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_first_depends_on_past=True
        )
        job.run()

        scheduler._process_task_instances(target_dag,
                                          task_instances_list=task_instances_list)

        self.assertTrue(task_instances_list.append.called)

    @unittest.skipIf('sqlite' in configuration.conf.get('core', 'sql_alchemy_conn'),
                     "concurrent access not supported in sqlite")
    def test_backfill_multi_dates(self):
        dag = self.dagbag.get_dag('example_bash_operator')

        end_date = DEFAULT_DATE + datetime.timedelta(days=1)

        executor = TestExecutor()
        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=end_date,
            executor=executor,
            ignore_first_depends_on_past=True
        )

        job.run()

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
        self.maxDiff = None
        self.assertListEqual(
            [((dag.dag_id, task_id, when, 1), State.SUCCESS)
             for (task_id, when) in expected_execution_order],
            executor.sorted_tasks
        )

        session = settings.Session()
        drs = session.query(DagRun).filter(
            DagRun.dag_id == dag.dag_id
        ).order_by(DagRun.execution_date).all()

        self.assertTrue(drs[0].execution_date == DEFAULT_DATE)
        self.assertTrue(drs[0].state == State.SUCCESS)
        self.assertTrue(drs[1].execution_date ==
                        DEFAULT_DATE + datetime.timedelta(days=1))
        self.assertTrue(drs[1].state == State.SUCCESS)

        dag.clear()
        session.close()

    @unittest.skipIf(
        "sqlite" in configuration.conf.get("core", "sql_alchemy_conn"),
        "concurrent access not supported in sqlite",
    )
    @parameterized.expand(
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
                "example_bash_operator",
                ("runme_0", "runme_1", "runme_2", "also_run_this", "run_after_loop",
                 "run_this_last"),
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
        ]
    )
    def test_backfill_examples(self, dag_id, expected_execution_order):
        """
        Test backfilling example dags

        Try to backfill some of the example dags. Be careful, not all dags are suitable
        for doing this. For example, a dag that sleeps forever, or does not have a
        schedule won't work here since you simply can't backfill them.
        """
        self.maxDiff = None
        dag = self.dagbag.get_dag(dag_id)

        logger.info('*** Running example DAG: %s', dag.dag_id)
        executor = TestExecutor()
        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            executor=executor,
            ignore_first_depends_on_past=True)

        job.run()
        self.assertListEqual(
            [((dag_id, task_id, DEFAULT_DATE, 1), State.SUCCESS)
             for task_id in expected_execution_order],
            executor.sorted_tasks
        )

    def test_backfill_conf(self):
        dag = self._get_dummy_dag('test_backfill_conf')

        executor = TestExecutor()

        conf = json.loads("""{"key": "value"}""")
        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          conf=conf)
        job.run()

        dr = DagRun.find(dag_id='test_backfill_conf')

        self.assertEqual(conf, dr[0].conf)

    @patch('airflow.jobs.backfill_job.BackfillJob.log')
    def test_backfill_respect_task_concurrency_limit(self, mock_log):
        task_concurrency = 2
        dag = self._get_dummy_dag(
            'test_backfill_respect_task_concurrency_limit',
            task_concurrency=task_concurrency,
        )

        executor = TestExecutor()

        job = BackfillJob(
            dag=dag,
            executor=executor,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        job.run()

        self.assertTrue(0 < len(executor.history))

        task_concurrency_limit_reached_at_least_once = False

        num_running_task_instances = 0
        for running_task_instances in executor.history:
            self.assertLessEqual(len(running_task_instances), task_concurrency)
            num_running_task_instances += len(running_task_instances)
            if len(running_task_instances) == task_concurrency:
                task_concurrency_limit_reached_at_least_once = True

        self.assertEqual(8, num_running_task_instances)
        self.assertTrue(task_concurrency_limit_reached_at_least_once)

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

        self.assertEqual(0, times_pool_limit_reached_in_debug)
        self.assertEqual(0, times_dag_concurrency_limit_reached_in_debug)
        self.assertGreater(times_task_concurrency_limit_reached_in_debug, 0)

    @patch('airflow.jobs.backfill_job.BackfillJob.log')
    def test_backfill_respect_dag_concurrency_limit(self, mock_log):

        dag = self._get_dummy_dag('test_backfill_respect_concurrency_limit')
        dag.concurrency = 2

        executor = TestExecutor()

        job = BackfillJob(
            dag=dag,
            executor=executor,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        job.run()

        self.assertTrue(0 < len(executor.history))

        concurrency_limit_reached_at_least_once = False

        num_running_task_instances = 0

        for running_task_instances in executor.history:
            self.assertLessEqual(len(running_task_instances), dag.concurrency)
            num_running_task_instances += len(running_task_instances)
            if len(running_task_instances) == dag.concurrency:
                concurrency_limit_reached_at_least_once = True

        self.assertEqual(8, num_running_task_instances)
        self.assertTrue(concurrency_limit_reached_at_least_once)

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

        self.assertEqual(0, times_pool_limit_reached_in_debug)
        self.assertEqual(0, times_task_concurrency_limit_reached_in_debug)
        self.assertGreater(times_dag_concurrency_limit_reached_in_debug, 0)

    @patch('airflow.jobs.backfill_job.BackfillJob.log')
    def test_backfill_respect_default_pool_limit(self, mock_log):
        default_pool_slots = 2
        set_default_pool_slots(default_pool_slots)

        dag = self._get_dummy_dag('test_backfill_with_no_pool_limit')

        executor = TestExecutor()

        job = BackfillJob(
            dag=dag,
            executor=executor,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        job.run()

        self.assertTrue(0 < len(executor.history))

        default_pool_task_slot_count_reached_at_least_once = False

        num_running_task_instances = 0

        # if no pool is specified, the number of tasks running in
        # parallel per backfill should be less than
        # default_pool slots at any point of time.
        for running_task_instances in executor.history:
            self.assertLessEqual(
                len(running_task_instances),
                default_pool_slots,
            )
            num_running_task_instances += len(running_task_instances)
            if len(running_task_instances) == default_pool_slots:
                default_pool_task_slot_count_reached_at_least_once = True

        self.assertEquals(8, num_running_task_instances)
        self.assertTrue(default_pool_task_slot_count_reached_at_least_once)

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

        self.assertEqual(0, times_dag_concurrency_limit_reached_in_debug)
        self.assertEqual(0, times_task_concurrency_limit_reached_in_debug)
        self.assertGreater(times_pool_limit_reached_in_debug, 0)

    def test_backfill_pool_not_found(self):
        dag = self._get_dummy_dag(
            dag_id='test_backfill_pool_not_found',
            pool='king_pool',
        )

        executor = TestExecutor()

        job = BackfillJob(
            dag=dag,
            executor=executor,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        try:
            job.run()
        except AirflowException:
            return

        self.fail()

    @patch('airflow.jobs.backfill_job.BackfillJob.log')
    def test_backfill_respect_pool_limit(self, mock_log):
        session = settings.Session()

        slots = 2
        pool = Pool(
            pool='pool_with_two_slots',
            slots=slots,
        )
        session.add(pool)
        session.commit()

        dag = self._get_dummy_dag(
            dag_id='test_backfill_respect_pool_limit',
            pool=pool.pool,
        )

        executor = TestExecutor()

        job = BackfillJob(
            dag=dag,
            executor=executor,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=7),
        )

        job.run()

        self.assertTrue(0 < len(executor.history))

        pool_was_full_at_least_once = False
        num_running_task_instances = 0

        for running_task_instances in executor.history:
            self.assertLessEqual(len(running_task_instances), slots)
            num_running_task_instances += len(running_task_instances)
            if len(running_task_instances) == slots:
                pool_was_full_at_least_once = True

        self.assertEqual(8, num_running_task_instances)
        self.assertTrue(pool_was_full_at_least_once)

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

        self.assertEqual(0, times_task_concurrency_limit_reached_in_debug)
        self.assertEqual(0, times_dag_concurrency_limit_reached_in_debug)
        self.assertGreater(times_pool_limit_reached_in_debug, 0)

    def test_backfill_run_rescheduled(self):
        dag = DAG(
            dag_id='test_backfill_run_rescheduled',
            start_date=DEFAULT_DATE,
            schedule_interval='@daily')

        with dag:
            DummyOperator(
                task_id='test_backfill_run_rescheduled_task-1',
                dag=dag,
            )

        dag.clear()

        executor = TestExecutor()

        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          )
        job.run()

        ti = TI(task=dag.get_task('test_backfill_run_rescheduled_task-1'),
                execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        ti.set_state(State.UP_FOR_RESCHEDULE)

        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          rerun_failed_tasks=True
                          )
        job.run()
        ti = TI(task=dag.get_task('test_backfill_run_rescheduled_task-1'),
                execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    def test_backfill_rerun_failed_tasks(self):
        dag = DAG(
            dag_id='test_backfill_rerun_failed',
            start_date=DEFAULT_DATE,
            schedule_interval='@daily')

        with dag:
            DummyOperator(
                task_id='test_backfill_rerun_failed_task-1',
                dag=dag)

        dag.clear()

        executor = TestExecutor()

        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          )
        job.run()

        ti = TI(task=dag.get_task('test_backfill_rerun_failed_task-1'),
                execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        ti.set_state(State.FAILED)

        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          rerun_failed_tasks=True
                          )
        job.run()
        ti = TI(task=dag.get_task('test_backfill_rerun_failed_task-1'),
                execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    def test_backfill_rerun_upstream_failed_tasks(self):
        dag = DAG(
            dag_id='test_backfill_rerun_upstream_failed',
            start_date=DEFAULT_DATE,
            schedule_interval='@daily')

        with dag:
            t1 = DummyOperator(task_id='test_backfill_rerun_upstream_failed_task-1',
                               dag=dag)
            t2 = DummyOperator(task_id='test_backfill_rerun_upstream_failed_task-2',
                               dag=dag)
            t1.set_upstream(t2)

        dag.clear()
        executor = TestExecutor()

        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          )
        job.run()

        ti = TI(task=dag.get_task('test_backfill_rerun_upstream_failed_task-1'),
                execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        ti.set_state(State.UPSTREAM_FAILED)

        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          rerun_failed_tasks=True
                          )
        job.run()
        ti = TI(task=dag.get_task('test_backfill_rerun_upstream_failed_task-1'),
                execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    def test_backfill_rerun_failed_tasks_without_flag(self):
        dag = DAG(
            dag_id='test_backfill_rerun_failed',
            start_date=DEFAULT_DATE,
            schedule_interval='@daily')

        with dag:
            DummyOperator(
                task_id='test_backfill_rerun_failed_task-1',
                dag=dag)

        dag.clear()

        executor = TestExecutor()

        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          )
        job.run()

        ti = TI(task=dag.get_task('test_backfill_rerun_failed_task-1'),
                execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        ti.set_state(State.FAILED)

        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          rerun_failed_tasks=False
                          )

        with self.assertRaises(AirflowException):
            job.run()

    def test_backfill_ordered_concurrent_execute(self):
        dag = DAG(
            dag_id='test_backfill_ordered_concurrent_execute',
            start_date=DEFAULT_DATE,
            schedule_interval="@daily")

        with dag:
            op1 = DummyOperator(task_id='leave1')
            op2 = DummyOperator(task_id='leave2')
            op3 = DummyOperator(task_id='upstream_level_1')
            op4 = DummyOperator(task_id='upstream_level_2')
            op5 = DummyOperator(task_id='upstream_level_3')
            # order randomly
            op2.set_downstream(op3)
            op1.set_downstream(op3)
            op4.set_downstream(op5)
            op3.set_downstream(op4)

        dag.clear()

        executor = TestExecutor()
        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          )
        job.run()

        d0 = DEFAULT_DATE
        d1 = d0 + datetime.timedelta(days=1)
        d2 = d1 + datetime.timedelta(days=1)

        # test executor history keeps a list
        history = executor.history

        self.maxDiff = None
        self.assertListEqual(
            # key[0] is dag id, key[3] is try_number, we don't care about either of those here
            [sorted([item[-1].key[1:3] for item in batch]) for batch in history],
            [
                [
                    ('leave1', d0),
                    ('leave1', d1),
                    ('leave1', d2),
                    ('leave2', d0),
                    ('leave2', d1),
                    ('leave2', d2)
                ],
                [('upstream_level_1', d0), ('upstream_level_1', d1),
                 ('upstream_level_1', d2)],
                [('upstream_level_2', d0), ('upstream_level_2', d1),
                 ('upstream_level_2', d2)],
                [('upstream_level_3', d0), ('upstream_level_3', d1),
                 ('upstream_level_3', d2)],
            ]
        )

    def test_backfill_pooled_tasks(self):
        """
        Test that queued tasks are executed by BackfillJob
        """
        session = settings.Session()
        pool = Pool(pool='test_backfill_pooled_task_pool', slots=1)
        session.add(pool)
        session.commit()
        session.close()

        dag = self.dagbag.get_dag('test_backfill_pooled_task_dag')
        dag.clear()

        executor = TestExecutor(do_update=True)
        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            executor=executor)

        # run with timeout because this creates an infinite loop if not
        # caught
        try:
            with timeout(seconds=5):
                job.run()
        except AirflowTaskTimeout:
            pass
        ti = TI(
            task=dag.get_task('test_backfill_pooled_task'),
            execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    def test_backfill_depends_on_past(self):
        """
        Test that backfill respects ignore_depends_on_past
        """
        dag = self.dagbag.get_dag('test_depends_on_past')
        dag.clear()
        run_date = DEFAULT_DATE + datetime.timedelta(days=5)

        # backfill should deadlock
        self.assertRaisesRegex(
            AirflowException,
            'BackfillJob is deadlocked',
            BackfillJob(dag=dag, start_date=run_date, end_date=run_date).run)

        BackfillJob(
            dag=dag,
            start_date=run_date,
            end_date=run_date,
            executor=TestExecutor(),
            ignore_first_depends_on_past=True).run()

        # ti should have succeeded
        ti = TI(dag.tasks[0], run_date)
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    def test_run_ignores_all_dependencies(self):
        """
        Test that run respects ignore_all_dependencies
        """
        dag_id = 'test_run_ignores_all_dependencies'

        dag = self.dagbag.get_dag('test_run_ignores_all_dependencies')
        dag.clear()

        task0_id = 'test_run_dependent_task'
        args0 = ['tasks',
                 'run',
                 '-A',
                 dag_id,
                 task0_id,
                 DEFAULT_DATE.isoformat()]
        cli.run(self.parser.parse_args(args0))
        ti_dependent0 = TI(
            task=dag.get_task(task0_id),
            execution_date=DEFAULT_DATE)

        ti_dependent0.refresh_from_db()
        self.assertEqual(ti_dependent0.state, State.FAILED)

        task1_id = 'test_run_dependency_task'
        args1 = ['tasks',
                 'run',
                 '-A',
                 dag_id,
                 task1_id,
                 (DEFAULT_DATE + datetime.timedelta(days=1)).isoformat()]
        cli.run(self.parser.parse_args(args1))

        ti_dependency = TI(
            task=dag.get_task(task1_id),
            execution_date=DEFAULT_DATE + datetime.timedelta(days=1))
        ti_dependency.refresh_from_db()
        self.assertEqual(ti_dependency.state, State.FAILED)

        task2_id = 'test_run_dependent_task'
        args2 = ['tasks',
                 'run',
                 '-A',
                 dag_id,
                 task2_id,
                 (DEFAULT_DATE + datetime.timedelta(days=1)).isoformat()]
        cli.run(self.parser.parse_args(args2))

        ti_dependent = TI(
            task=dag.get_task(task2_id),
            execution_date=DEFAULT_DATE + datetime.timedelta(days=1))
        ti_dependent.refresh_from_db()
        self.assertEqual(ti_dependent.state, State.SUCCESS)

    def test_backfill_depends_on_past_backwards(self):
        """
        Test that CLI respects -B argument and raises on interaction with depends_on_past
        """
        dag_id = 'test_depends_on_past'
        start_date = DEFAULT_DATE + datetime.timedelta(days=1)
        end_date = start_date + datetime.timedelta(days=1)
        kwargs = dict(
            start_date=start_date,
            end_date=end_date,
        )
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()

        executor = TestExecutor()
        job = BackfillJob(dag=dag,
                          executor=executor,
                          ignore_first_depends_on_past=True,
                          **kwargs)
        job.run()

        ti = TI(dag.get_task('test_dop_task'), end_date)
        ti.refresh_from_db()
        # runs fine forwards
        self.assertEqual(ti.state, State.SUCCESS)

        # raises backwards
        expected_msg = 'You cannot backfill backwards because one or more tasks depend_on_past: {}'.format(
            'test_dop_task')
        with self.assertRaisesRegex(AirflowException, expected_msg):
            executor = TestExecutor()
            job = BackfillJob(dag=dag,
                              executor=executor,
                              run_backwards=True,
                              **kwargs)
            job.run()

    def test_cli_receives_delay_arg(self):
        """
        Tests that the --delay argument is passed correctly to the BackfillJob
        """
        dag_id = 'example_bash_operator'
        run_date = DEFAULT_DATE
        args = [
            'dags',
            'backfill',
            dag_id,
            '-s',
            run_date.isoformat(),
            '--delay_on_limit',
            '0.5',
        ]
        parsed_args = self.parser.parse_args(args)
        self.assertEqual(0.5, parsed_args.delay_on_limit)

    def _get_dag_test_max_active_limits(self, dag_id, max_active_runs=1):
        dag = DAG(
            dag_id=dag_id,
            start_date=DEFAULT_DATE,
            schedule_interval="@hourly",
            max_active_runs=max_active_runs
        )

        with dag:
            op1 = DummyOperator(task_id='leave1')
            op2 = DummyOperator(task_id='leave2')
            op3 = DummyOperator(task_id='upstream_level_1')
            op4 = DummyOperator(task_id='upstream_level_2')

            op1 >> op2 >> op3
            op4 >> op3

        dag.clear()
        return dag

    def test_backfill_max_limit_check_within_limit(self):
        dag = self._get_dag_test_max_active_limits(
            'test_backfill_max_limit_check_within_limit',
            max_active_runs=16)

        start_date = DEFAULT_DATE - datetime.timedelta(hours=1)
        end_date = DEFAULT_DATE

        executor = TestExecutor()
        job = BackfillJob(dag=dag,
                          start_date=start_date,
                          end_date=end_date,
                          executor=executor,
                          donot_pickle=True)
        job.run()

        dagruns = DagRun.find(dag_id=dag.dag_id)
        self.assertEqual(2, len(dagruns))
        self.assertTrue(all([run.state == State.SUCCESS for run in dagruns]))

    def test_backfill_max_limit_check(self):
        dag_id = 'test_backfill_max_limit_check'
        run_id = 'test_dagrun'
        start_date = DEFAULT_DATE - datetime.timedelta(hours=1)
        end_date = DEFAULT_DATE

        dag_run_created_cond = threading.Condition()

        def run_backfill(cond):
            cond.acquire()
            # this session object is different than the one in the main thread
            with create_session() as thread_session:
                try:
                    dag = self._get_dag_test_max_active_limits(dag_id)

                    # Existing dagrun that is not within the backfill range
                    dag.create_dagrun(
                        run_id=run_id,
                        state=State.RUNNING,
                        execution_date=DEFAULT_DATE + datetime.timedelta(hours=1),
                        start_date=DEFAULT_DATE,
                    )

                    thread_session.commit()
                    cond.notify()
                finally:
                    cond.release()
                    thread_session.close()

                executor = TestExecutor()
                job = BackfillJob(dag=dag,
                                  start_date=start_date,
                                  end_date=end_date,
                                  executor=executor,
                                  donot_pickle=True)
                job.run()

        backfill_job_thread = threading.Thread(target=run_backfill,
                                               name="run_backfill",
                                               args=(dag_run_created_cond,))

        dag_run_created_cond.acquire()
        with create_session() as session:
            backfill_job_thread.start()
            try:
                # at this point backfill can't run since the max_active_runs has been
                # reached, so it is waiting
                dag_run_created_cond.wait(timeout=1.5)
                dagruns = DagRun.find(dag_id=dag_id)
                dr = dagruns[0]
                self.assertEqual(1, len(dagruns))
                self.assertEqual(dr.run_id, run_id)

                # allow the backfill to execute
                # by setting the existing dag run to SUCCESS,
                # backfill will execute dag runs 1 by 1
                dr.set_state(State.SUCCESS)
                session.merge(dr)
                session.commit()

                backfill_job_thread.join()

                dagruns = DagRun.find(dag_id=dag_id)
                self.assertEqual(3, len(dagruns))  # 2 from backfill + 1 existing
                self.assertEqual(dagruns[-1].run_id, dr.run_id)
            finally:
                dag_run_created_cond.release()

    def test_backfill_max_limit_check_no_count_existing(self):
        dag = self._get_dag_test_max_active_limits(
            'test_backfill_max_limit_check_no_count_existing')
        start_date = DEFAULT_DATE
        end_date = DEFAULT_DATE

        # Existing dagrun that is within the backfill range
        dag.create_dagrun(run_id="test_existing_backfill",
                          state=State.RUNNING,
                          execution_date=DEFAULT_DATE,
                          start_date=DEFAULT_DATE)

        executor = TestExecutor()
        job = BackfillJob(dag=dag,
                          start_date=start_date,
                          end_date=end_date,
                          executor=executor,
                          donot_pickle=True)
        job.run()

        # BackfillJob will run since the existing DagRun does not count for the max
        # active limit since it's within the backfill date range.
        dagruns = DagRun.find(dag_id=dag.dag_id)
        # will only be able to run 1 (the existing one) since there's just
        # one dag run slot left given the max_active_runs limit
        self.assertEqual(1, len(dagruns))
        self.assertEqual(State.SUCCESS, dagruns[0].state)

    def test_backfill_max_limit_check_complete_loop(self):
        dag = self._get_dag_test_max_active_limits(
            'test_backfill_max_limit_check_complete_loop')
        start_date = DEFAULT_DATE - datetime.timedelta(hours=1)
        end_date = DEFAULT_DATE

        # Given the max limit to be 1 in active dag runs, we need to run the
        # backfill job 3 times
        success_expected = 2
        executor = TestExecutor()
        job = BackfillJob(dag=dag,
                          start_date=start_date,
                          end_date=end_date,
                          executor=executor,
                          donot_pickle=True)
        job.run()

        success_dagruns = len(DagRun.find(dag_id=dag.dag_id, state=State.SUCCESS))
        running_dagruns = len(DagRun.find(dag_id=dag.dag_id, state=State.RUNNING))
        self.assertEqual(success_expected, success_dagruns)
        self.assertEqual(0, running_dagruns)  # no dag_runs in running state are left

    def test_sub_set_subdag(self):
        dag = DAG(
            'test_sub_set_subdag',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='leave1')
            op2 = DummyOperator(task_id='leave2')
            op3 = DummyOperator(task_id='upstream_level_1')
            op4 = DummyOperator(task_id='upstream_level_2')
            op5 = DummyOperator(task_id='upstream_level_3')
            # order randomly
            op2.set_downstream(op3)
            op1.set_downstream(op3)
            op4.set_downstream(op5)
            op3.set_downstream(op4)

        dag.clear()
        dr = dag.create_dagrun(run_id="test",
                               state=State.RUNNING,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE)

        executor = TestExecutor()
        sub_dag = dag.sub_dag(task_regex="leave*",
                              include_downstream=False,
                              include_upstream=False)
        job = BackfillJob(dag=sub_dag,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE,
                          executor=executor)
        job.run()

        self.assertRaises(sqlalchemy.orm.exc.NoResultFound, dr.refresh_from_db)
        # the run_id should have changed, so a refresh won't work
        drs = DagRun.find(dag_id=dag.dag_id, execution_date=DEFAULT_DATE)
        dr = drs[0]

        self.assertEqual(BackfillJob.ID_FORMAT_PREFIX.format(DEFAULT_DATE.isoformat()),
                         dr.run_id)
        for ti in dr.get_task_instances():
            if ti.task_id == 'leave1' or ti.task_id == 'leave2':
                self.assertEqual(State.SUCCESS, ti.state)
            else:
                self.assertEqual(State.NONE, ti.state)

    def test_backfill_fill_blanks(self):
        dag = DAG(
            'test_backfill_fill_blanks',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'},
        )

        with dag:
            op1 = DummyOperator(task_id='op1')
            op2 = DummyOperator(task_id='op2')
            op3 = DummyOperator(task_id='op3')
            op4 = DummyOperator(task_id='op4')
            op5 = DummyOperator(task_id='op5')
            op6 = DummyOperator(task_id='op6')

        dag.clear()
        dr = dag.create_dagrun(run_id='test',
                               state=State.RUNNING,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE)
        executor = TestExecutor()

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

        job = BackfillJob(dag=dag,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE,
                          executor=executor)
        self.assertRaisesRegex(
            AirflowException,
            'Some task instances failed',
            job.run)

        self.assertRaises(sqlalchemy.orm.exc.NoResultFound, dr.refresh_from_db)
        # the run_id should have changed, so a refresh won't work
        drs = DagRun.find(dag_id=dag.dag_id, execution_date=DEFAULT_DATE)
        dr = drs[0]

        self.assertEqual(dr.state, State.FAILED)

        tis = dr.get_task_instances()
        for ti in tis:
            if ti.task_id in (op1.task_id, op4.task_id, op6.task_id):
                self.assertEqual(ti.state, State.SUCCESS)
            elif ti.task_id == op2.task_id:
                self.assertEqual(ti.state, State.FAILED)
            elif ti.task_id == op3.task_id:
                self.assertEqual(ti.state, State.SKIPPED)
            elif ti.task_id == op5.task_id:
                self.assertEqual(ti.state, State.UPSTREAM_FAILED)

    def test_backfill_execute_subdag(self):
        dag = self.dagbag.get_dag('example_subdag_operator')
        subdag_op_task = dag.get_task('section-1')

        subdag = subdag_op_task.subdag
        subdag.schedule_interval = '@daily'

        start_date = timezone.utcnow()
        executor = TestExecutor()
        job = BackfillJob(dag=subdag,
                          start_date=start_date,
                          end_date=start_date,
                          executor=executor,
                          donot_pickle=True)
        job.run()

        subdag_op_task.pre_execute(context={'execution_date': start_date})
        subdag_op_task.execute(context={'execution_date': start_date})
        subdag_op_task.post_execute(context={'execution_date': start_date})

        history = executor.history
        subdag_history = history[0]

        # check that all 5 task instances of the subdag 'section-1' were executed
        self.assertEqual(5, len(subdag_history))
        for sdh in subdag_history:
            ti = sdh[3]
            self.assertIn('section-1-task-', ti.task_id)

        with create_session() as session:
            successful_subdag_runs = (
                session
                .query(DagRun)
                .filter(DagRun.dag_id == subdag.dag_id)
                .filter(DagRun.execution_date == start_date)
                .filter(DagRun.state == State.SUCCESS)
                .count()
            )

            self.assertEqual(1, successful_subdag_runs)

        subdag.clear()
        dag.clear()

    def test_subdag_clear_parentdag_downstream_clear(self):
        dag = self.dagbag.get_dag('clear_subdag_test_dag')
        subdag_op_task = dag.get_task('daily_job')

        subdag = subdag_op_task.subdag

        executor = TestExecutor()
        job = BackfillJob(dag=dag,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE,
                          executor=executor,
                          donot_pickle=True)

        with timeout(seconds=30):
            job.run()

        ti_subdag = TI(
            task=dag.get_task('daily_job'),
            execution_date=DEFAULT_DATE)
        ti_subdag.refresh_from_db()
        self.assertEqual(ti_subdag.state, State.SUCCESS)

        ti_irrelevant = TI(
            task=dag.get_task('daily_job_irrelevant'),
            execution_date=DEFAULT_DATE)
        ti_irrelevant.refresh_from_db()
        self.assertEqual(ti_irrelevant.state, State.SUCCESS)

        ti_downstream = TI(
            task=dag.get_task('daily_job_downstream'),
            execution_date=DEFAULT_DATE)
        ti_downstream.refresh_from_db()
        self.assertEqual(ti_downstream.state, State.SUCCESS)

        sdag = subdag.sub_dag(
            task_regex='daily_job_subdag_task',
            include_downstream=True,
            include_upstream=False)

        sdag.clear(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            include_parentdag=True)

        ti_subdag.refresh_from_db()
        self.assertEqual(State.NONE, ti_subdag.state)

        ti_irrelevant.refresh_from_db()
        self.assertEqual(State.SUCCESS, ti_irrelevant.state)

        ti_downstream.refresh_from_db()
        self.assertEqual(State.NONE, ti_downstream.state)

        subdag.clear()
        dag.clear()

    def test_backfill_execute_subdag_with_removed_task(self):
        """
        Ensure that subdag operators execute properly in the case where
        an associated task of the subdag has been removed from the dag
        definition, but has instances in the database from previous runs.
        """
        dag = self.dagbag.get_dag('example_subdag_operator')
        subdag = dag.get_task('section-1').subdag

        executor = TestExecutor()
        job = BackfillJob(dag=subdag,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE,
                          executor=executor,
                          donot_pickle=True)

        removed_task_ti = TI(
            task=DummyOperator(task_id='removed_task'),
            execution_date=DEFAULT_DATE,
            state=State.REMOVED)
        removed_task_ti.dag_id = subdag.dag_id

        session = settings.Session()
        session.merge(removed_task_ti)

        with timeout(seconds=30):
            job.run()

        for task in subdag.tasks:
            instance = session.query(TI).filter(
                TI.dag_id == subdag.dag_id,
                TI.task_id == task.task_id,
                TI.execution_date == DEFAULT_DATE).first()

            self.assertIsNotNone(instance)
            self.assertEqual(instance.state, State.SUCCESS)

        removed_task_ti.refresh_from_db()
        self.assertEqual(removed_task_ti.state, State.REMOVED)

        subdag.clear()
        dag.clear()

    def test_update_counters(self):
        dag = DAG(
            dag_id='test_manage_executor_state',
            start_date=DEFAULT_DATE)

        task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        job = BackfillJob(dag=dag)

        session = settings.Session()
        dr = dag.create_dagrun(run_id=DagRun.ID_PREFIX,
                               state=State.RUNNING,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)
        ti = TI(task1, dr.execution_date)
        ti.refresh_from_db()

        ti_status = BackfillJob._DagRunTaskStatus()

        # test for success
        ti.set_state(State.SUCCESS, session)
        ti_status.running[ti.key] = ti
        job._update_counters(ti_status=ti_status)
        self.assertTrue(len(ti_status.running) == 0)
        self.assertTrue(len(ti_status.succeeded) == 1)
        self.assertTrue(len(ti_status.skipped) == 0)
        self.assertTrue(len(ti_status.failed) == 0)
        self.assertTrue(len(ti_status.to_run) == 0)

        ti_status.succeeded.clear()

        # test for skipped
        ti.set_state(State.SKIPPED, session)
        ti_status.running[ti.key] = ti
        job._update_counters(ti_status=ti_status)
        self.assertTrue(len(ti_status.running) == 0)
        self.assertTrue(len(ti_status.succeeded) == 0)
        self.assertTrue(len(ti_status.skipped) == 1)
        self.assertTrue(len(ti_status.failed) == 0)
        self.assertTrue(len(ti_status.to_run) == 0)

        ti_status.skipped.clear()

        # test for failed
        ti.set_state(State.FAILED, session)
        ti_status.running[ti.key] = ti
        job._update_counters(ti_status=ti_status)
        self.assertTrue(len(ti_status.running) == 0)
        self.assertTrue(len(ti_status.succeeded) == 0)
        self.assertTrue(len(ti_status.skipped) == 0)
        self.assertTrue(len(ti_status.failed) == 1)
        self.assertTrue(len(ti_status.to_run) == 0)

        ti_status.failed.clear()

        # test for retry
        ti.set_state(State.UP_FOR_RETRY, session)
        ti_status.running[ti.key] = ti
        job._update_counters(ti_status=ti_status)
        self.assertTrue(len(ti_status.running) == 0)
        self.assertTrue(len(ti_status.succeeded) == 0)
        self.assertTrue(len(ti_status.skipped) == 0)
        self.assertTrue(len(ti_status.failed) == 0)
        self.assertTrue(len(ti_status.to_run) == 1)

        ti_status.to_run.clear()

        # test for reschedule
        ti.set_state(State.UP_FOR_RESCHEDULE, session)
        ti_status.running[ti.key] = ti
        job._update_counters(ti_status=ti_status)
        self.assertTrue(len(ti_status.running) == 0)
        self.assertTrue(len(ti_status.succeeded) == 0)
        self.assertTrue(len(ti_status.skipped) == 0)
        self.assertTrue(len(ti_status.failed) == 0)
        self.assertTrue(len(ti_status.to_run) == 1)

        ti_status.to_run.clear()

        # test for none
        ti.set_state(State.NONE, session)
        ti_status.running[ti.key] = ti
        job._update_counters(ti_status=ti_status)
        self.assertTrue(len(ti_status.running) == 0)
        self.assertTrue(len(ti_status.succeeded) == 0)
        self.assertTrue(len(ti_status.skipped) == 0)
        self.assertTrue(len(ti_status.failed) == 0)
        self.assertTrue(len(ti_status.to_run) == 1)

        ti_status.to_run.clear()

        session.close()

    def test_dag_get_run_dates(self):

        def get_test_dag_for_backfill(schedule_interval=None):
            dag = DAG(
                dag_id='test_get_dates',
                start_date=DEFAULT_DATE,
                schedule_interval=schedule_interval)
            DummyOperator(
                task_id='dummy',
                dag=dag,
                owner='airflow',
            )
            return dag

        test_dag = get_test_dag_for_backfill()
        self.assertEqual([DEFAULT_DATE], test_dag.get_run_dates(
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE))

        test_dag = get_test_dag_for_backfill(schedule_interval="@hourly")
        self.assertEqual([DEFAULT_DATE - datetime.timedelta(hours=3),
                          DEFAULT_DATE - datetime.timedelta(hours=2),
                          DEFAULT_DATE - datetime.timedelta(hours=1),
                          DEFAULT_DATE],
                         test_dag.get_run_dates(
                             start_date=DEFAULT_DATE - datetime.timedelta(hours=3),
                             end_date=DEFAULT_DATE, ))

    def test_backfill_run_backwards(self):
        dag = self.dagbag.get_dag("test_start_date_scheduling")
        dag.clear()

        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            run_backwards=True
        )
        job.run()

        session = settings.Session()
        tis = session.query(TI).filter(
            TI.dag_id == 'test_start_date_scheduling' and TI.task_id == 'dummy'
        ).order_by(TI.execution_date).all()

        queued_times = [ti.queued_dttm for ti in tis]
        self.assertTrue(queued_times == sorted(queued_times, reverse=True))
        self.assertTrue(all([ti.state == State.SUCCESS for ti in tis]))

        dag.clear()
        session.close()
