# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import datetime
import logging
import os
import unittest

from airflow import AirflowException, settings
from airflow import models
from airflow.bin import cli
from airflow.executors import DEFAULT_EXECUTOR
from airflow.jobs import BackfillJob, SchedulerJob
from airflow.models import DAG, DagModel, DagBag, DagRun, Pool, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.utils.timeout import timeout

from tests.executor.test_executor import TestExecutor

from airflow import configuration
configuration.load_test_config()

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime.datetime(2016, 1, 1)


class BackfillJobTest(unittest.TestCase):

    def setUp(self):
        self.parser = cli.CLIFactory.get_parser()
        self.dagbag = DagBag(include_examples=True)

    @unittest.skipIf('sqlite' in configuration.get('core', 'sql_alchemy_conn'),
                     "concurrent access not supported in sqlite")
    def test_trigger_controller_dag(self):
        dag = self.dagbag.get_dag('example_trigger_controller_dag')
        target_dag = self.dagbag.get_dag('example_trigger_target_dag')
        dag.clear()
        target_dag.clear()

        scheduler = SchedulerJob()
        queue = mock.Mock()
        scheduler._process_task_instances(target_dag, queue=queue)
        self.assertFalse(queue.append.called)

        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            ignore_first_depends_on_past=True
        )
        job.run()

        scheduler = SchedulerJob()
        queue = mock.Mock()
        scheduler._process_task_instances(target_dag, queue=queue)

        self.assertTrue(queue.append.called)
        target_dag.clear()
        dag.clear()

    @unittest.skipIf('sqlite' in configuration.get('core', 'sql_alchemy_conn'),
                     "concurrent access not supported in sqlite")
    def test_backfill_multi_dates(self):
        dag = self.dagbag.get_dag('example_bash_operator')
        dag.clear()

        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE+datetime.timedelta(days=1),
            ignore_first_depends_on_past=True
        )
        job.run()

        session = settings.Session()
        drs = session.query(DagRun).filter(
            DagRun.dag_id=='example_bash_operator'
        ).order_by(DagRun.execution_date).all()

        self.assertTrue(drs[0].execution_date == DEFAULT_DATE)
        self.assertTrue(drs[0].state == State.SUCCESS)
        self.assertTrue(drs[1].execution_date == DEFAULT_DATE+datetime.timedelta(days=1))
        self.assertTrue(drs[1].state == State.SUCCESS)

        dag.clear()
        session.close()

    @unittest.skipIf('sqlite' in configuration.get('core', 'sql_alchemy_conn'),
                     "concurrent access not supported in sqlite")
    def test_backfill_examples(self):
        """
        Test backfilling example dags
        """

        # some DAGs really are just examples... but try to make them work!
        skip_dags = [
            'example_http_operator',
            'example_twitter_dag',
            'example_trigger_target_dag',
            'example_trigger_controller_dag',  # tested above
            'test_utils',  # sleeps forever
        ]

        logger = logging.getLogger('BackfillJobTest.test_backfill_examples')
        dags = [
            dag for dag in self.dagbag.dags.values()
            if 'example_dags' in dag.full_filepath
            and dag.dag_id not in skip_dags
            ]

        for dag in dags:
            dag.clear(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)

        for i, dag in enumerate(sorted(dags, key=lambda d: d.dag_id)):
            logger.info('*** Running example DAG #{}: {}'.format(i, dag.dag_id))
            job = BackfillJob(
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE,
                ignore_first_depends_on_past=True)
            job.run()

    def test_backfill_pooled_tasks(self):
        """
        Test that queued tasks are executed by BackfillJob

        Test for https://github.com/airbnb/airflow/pull/1225
        """
        session = settings.Session()
        pool = Pool(pool='test_backfill_pooled_task_pool', slots=1)
        session.add(pool)
        session.commit()

        dag = self.dagbag.get_dag('test_backfill_pooled_task_dag')
        dag.clear()

        job = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)

        # run with timeout because this creates an infinite loop if not
        # caught
        with timeout(seconds=30):
            job.run()

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
        self.assertRaisesRegexp(
            AirflowException,
            'BackfillJob is deadlocked',
            BackfillJob(dag=dag, start_date=run_date, end_date=run_date).run)

        BackfillJob(
            dag=dag,
            start_date=run_date,
            end_date=run_date,
            ignore_first_depends_on_past=True).run()

        # ti should have succeeded
        ti = TI(dag.tasks[0], run_date)
        ti.refresh_from_db()
        self.assertEquals(ti.state, State.SUCCESS)

    def test_cli_backfill_depends_on_past(self):
        """
        Test that CLI respects -I argument
        """
        dag_id = 'test_dagrun_states_deadlock'
        run_date = DEFAULT_DATE + datetime.timedelta(days=1)
        args = [
            'backfill',
            dag_id,
            '-l',
            '-s',
            run_date.isoformat(),
        ]
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()

        self.assertRaisesRegexp(
            AirflowException,
            'BackfillJob is deadlocked',
            cli.backfill,
            self.parser.parse_args(args))

        cli.backfill(self.parser.parse_args(args + ['-I']))
        ti = TI(dag.get_task('test_depends_on_past'), run_date)
        ti.refresh_from_db()
        # task ran
        self.assertEqual(ti.state, State.SUCCESS)
        dag.clear()


class SchedulerJobTest(unittest.TestCase):
    # These defaults make the test faster to run
    default_scheduler_args = {"file_process_interval": 0,
                              "processor_poll_interval": 0.5}

    def setUp(self):
        self.dagbag = DagBag()

    @provide_session
    def evaluate_dagrun(
            self,
            dag_id,
            expected_task_states,  # dict of task_id: state
            dagrun_state,
            run_kwargs=None,
            advance_execution_date=False,
            session=None):
        """
        Helper for testing DagRun states with simple two-task DAGS.
        This is hackish: a dag run is created but its tasks are
        run by a backfill.
        """
        if run_kwargs is None:
            run_kwargs = {}

        scheduler = SchedulerJob(**self.default_scheduler_args)
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()
        dr = scheduler.create_dag_run(dag)

        if advance_execution_date:
            # run a second time to schedule a dagrun after the start_date
            dr = scheduler.create_dag_run(dag)
        ex_date = dr.execution_date

        try:
            dag.run(start_date=ex_date, end_date=ex_date, **run_kwargs)
        except AirflowException:
            pass

        # test tasks
        for task_id, expected_state in expected_task_states.items():
            task = dag.get_task(task_id)
            ti = TI(task, ex_date)
            ti.refresh_from_db()
            self.assertEqual(ti.state, expected_state)

        # load dagrun
        dr = DagRun.find(dag_id=dag_id, execution_date=ex_date)
        dr = dr[0]
        dr.dag = dag

        self.assertEqual(dr.state, dagrun_state)

    def test_dagrun_fail(self):
        """
        DagRuns with one failed and one incomplete root task -> FAILED
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_fail',
            expected_task_states={
                'test_dagrun_fail': State.FAILED,
                'test_dagrun_succeed': State.UPSTREAM_FAILED,
            },
            dagrun_state=State.FAILED)

    def test_dagrun_success(self):
        """
        DagRuns with one failed and one successful root task -> SUCCESS
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_success',
            expected_task_states={
                'test_dagrun_fail': State.FAILED,
                'test_dagrun_succeed': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS)

    def test_dagrun_root_fail(self):
        """
        DagRuns with one successful and one failed root task -> FAILED
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_root_fail',
            expected_task_states={
                'test_dagrun_succeed': State.SUCCESS,
                'test_dagrun_fail': State.FAILED,
            },
            dagrun_state=State.FAILED)

    def test_dagrun_deadlock_ignore_depends_on_past_advance_ex_date(self):
        """
        DagRun is marked a success if ignore_first_depends_on_past=True

        Test that an otherwise-deadlocked dagrun is marked as a success
        if ignore_first_depends_on_past=True and the dagrun execution_date
        is after the start_date.
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            expected_task_states={
                'test_depends_on_past': State.SUCCESS,
                'test_depends_on_past_2': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS,
            advance_execution_date=True,
            run_kwargs=dict(ignore_first_depends_on_past=True))

    def test_dagrun_deadlock_ignore_depends_on_past(self):
        """
        Test that ignore_first_depends_on_past doesn't affect results
        (this is the same test as
        test_dagrun_deadlock_ignore_depends_on_past_advance_ex_date except
        that start_date == execution_date so depends_on_past is irrelevant).
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            expected_task_states={
                'test_depends_on_past': State.SUCCESS,
                'test_depends_on_past_2': State.SUCCESS,
            },
            dagrun_state=State.SUCCESS,
            run_kwargs=dict(ignore_first_depends_on_past=True))

    def test_scheduler_start_date(self):
        """
        Test that the scheduler respects start_dates, even when DAGS have run
        """

        dag_id = 'test_start_date_scheduling'
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()
        self.assertTrue(dag.start_date > DEFAULT_DATE)

        scheduler = SchedulerJob(dag_id,
                                 num_runs=2,
                                 **self.default_scheduler_args)
        scheduler.run()

        # zero tasks ran
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 0)

        # previously, running this backfill would kick off the Scheduler
        # because it would take the most recent run and start from there
        # That behavior still exists, but now it will only do so if after the
        # start date
        backfill = BackfillJob(
            dag=dag,
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE)
        backfill.run()

        # one task ran
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 1)

        scheduler = SchedulerJob(dag_id,
                                 num_runs=2,
                                 **self.default_scheduler_args)
        scheduler.run()

        # still one task
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 1)

    def test_scheduler_multiprocessing(self):
        """
        Test that the scheduler can successfully queue multiple dags in parallel
        """
        dag_ids = ['test_start_date_scheduling', 'test_dagrun_states_success']
        for dag_id in dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            dag.clear()

        scheduler = SchedulerJob(dag_ids=dag_ids,
                                 file_process_interval=0,
                                 processor_poll_interval=0.5,
                                 num_runs=2)
        scheduler.run()

        # zero tasks ran
        dag_id = 'test_start_date_scheduling'
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 0)

    def test_scheduler_dagrun_once(self):
        """
        Test if the scheduler does not create multiple dagruns
        if a dag is scheduled with @once and a start_date
        """
        dag = DAG(
            'test_scheduler_dagrun_once',
            start_date=datetime.datetime(2015, 1, 1),
            schedule_interval="@once")

        scheduler = SchedulerJob()
        dag.clear()
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        dr = scheduler.create_dag_run(dag)
        self.assertIsNone(dr)

    def test_scheduler_process_task_instances(self):
        """
        Test if _process_task_instances puts the right task instances into the
        queue.
        """
        dag = DAG(
            dag_id='test_scheduler_process_execute_task',
            start_date=DEFAULT_DATE)
        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag.clear()
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)

        queue = mock.Mock()
        scheduler._process_task_instances(dag, queue=queue)

        queue.append.assert_called_with(
            (dag.dag_id, dag_task1.task_id, DEFAULT_DATE)
        )

    def test_scheduler_do_not_schedule_removed_task(self):
        dag = DAG(
            dag_id='test_scheduler_do_not_schedule_removed_task',
            start_date=DEFAULT_DATE)
        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)

        dag = DAG(
            dag_id='test_scheduler_do_not_schedule_removed_task',
            start_date=DEFAULT_DATE)

        queue = mock.Mock()
        scheduler._process_task_instances(dag, queue=queue)

        queue.put.assert_not_called()

    def test_scheduler_do_not_schedule_too_early(self):
        dag = DAG(
            dag_id='test_scheduler_do_not_schedule_too_early',
            start_date=datetime.datetime(2200, 1, 1))
        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNone(dr)

        queue = mock.Mock()
        scheduler._process_task_instances(dag, queue=queue)

        queue.put.assert_not_called()

    def test_scheduler_do_not_run_finished(self):
        dag = DAG(
            dag_id='test_scheduler_do_not_run_finished',
            start_date=DEFAULT_DATE)
        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)

        tis = dr.get_task_instances(session=session)
        for ti in tis:
            ti.state = State.SUCCESS

        session.commit()
        session.close()

        queue = mock.Mock()
        scheduler._process_task_instances(dag, queue=queue)

        queue.put.assert_not_called()

    def test_scheduler_add_new_task(self):
        """
        Test if a task instance will be added if the dag is updated
        """
        dag = DAG(
            dag_id='test_scheduler_add_new_task',
            start_date=DEFAULT_DATE)

        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)

        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 1)

        dag_task2 = DummyOperator(
            task_id='dummy2',
            dag=dag,
            owner='airflow')

        queue = mock.Mock()
        scheduler._process_task_instances(dag, queue=queue)

        tis = dr.get_task_instances()
        self.assertEquals(len(tis), 2)

    def test_scheduler_verify_max_active_runs(self):
        """
        Test if a a dagrun will not be scheduled if max_dag_runs has been reached
        """
        dag = DAG(
            dag_id='test_scheduler_verify_max_active_runs',
            start_date=DEFAULT_DATE)
        dag.max_active_runs = 1

        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)

        dr = scheduler.create_dag_run(dag)
        self.assertIsNone(dr)

    def test_scheduler_fail_dagrun_timeout(self):
        """
        Test if a a dagrun wil be set failed if timeout
        """
        dag = DAG(
            dag_id='test_scheduler_fail_dagrun_timeout',
            start_date=DEFAULT_DATE)
        dag.dagrun_timeout = datetime.timedelta(seconds=60)

        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        dr.start_date = datetime.datetime.now() - datetime.timedelta(days=1)
        session.merge(dr)
        session.commit()

        dr2 = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr2)

        dr.refresh_from_db(session=session)
        self.assertEquals(dr.state, State.FAILED)

    def test_scheduler_verify_max_active_runs_and_dagrun_timeout(self):
        """
        Test if a a dagrun will not be scheduled if max_dag_runs has been reached and dagrun_timeout is not reached
        Test if a a dagrun will be scheduled if max_dag_runs has been reached but dagrun_timeout is also reached
        """
        dag = DAG(
            dag_id='test_scheduler_verify_max_active_runs_and_dagrun_timeout',
            start_date=DEFAULT_DATE)
        dag.max_active_runs = 1
        dag.dagrun_timeout = datetime.timedelta(seconds=60)

        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)

        # Should not be scheduled as DagRun has not timedout and max_active_runs is reached
        new_dr = scheduler.create_dag_run(dag)
        self.assertIsNone(new_dr)

        # Should be scheduled as dagrun_timeout has passed
        dr.start_date = datetime.datetime.now() - datetime.timedelta(days=1)
        session.merge(dr)
        session.commit()
        new_dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(new_dr)

    def test_scheduler_auto_align(self):
        """
        Test if the schedule_interval will be auto aligned with the start_date
        such that if the start_date coincides with the schedule the first
        execution_date will be start_date, otherwise it will be start_date +
        interval.
        """
        dag = DAG(
            dag_id='test_scheduler_auto_align_1',
            start_date=datetime.datetime(2016, 1, 1, 10, 10, 0),
            schedule_interval="4 5 * * *"
        )
        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEquals(dr.execution_date, datetime.datetime(2016, 1, 2, 5, 4))

        dag = DAG(
            dag_id='test_scheduler_auto_align_2',
            start_date=datetime.datetime(2016, 1, 1, 10, 10, 0),
            schedule_interval="10 10 * * *"
        )
        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()

        scheduler = SchedulerJob()
        dag.clear()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEquals(dr.execution_date, datetime.datetime(2016, 1, 1, 10, 10))

    def test_scheduler_reschedule(self):
        """
        Checks if tasks that are not taken up by the executor
        get rescheduled
        """
        executor = TestExecutor()

        dagbag = DagBag(executor=executor)
        dagbag.dags.clear()
        dagbag.executor = executor

        dag = DAG(
            dag_id='test_scheduler_reschedule',
            start_date=DEFAULT_DATE)
        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        dag.clear()
        dag.is_subdag = False

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()

        dagbag.bag_dag(dag=dag, root_dag=dag, parent_dag=dag)

        @mock.patch('airflow.models.DagBag', return_value=dagbag)
        @mock.patch('airflow.models.DagBag.collect_dags')
        def do_schedule(function, function2):
            # Use a empty file since the above mock will return the
            # expected DAGs. Also specify only a single file so that it doesn't
            # try to schedule the above DAG repeatedly.
            scheduler = SchedulerJob(num_runs=1,
                                     executor=executor,
                                     subdir=os.path.join(models.DAGS_FOLDER,
                                                         "no_dags.py"))
            scheduler.heartrate = 0
            scheduler.run()

        do_schedule()
        self.assertEquals(1, len(executor.queued_tasks))
        executor.queued_tasks.clear()

        do_schedule()
        self.assertEquals(2, len(executor.queued_tasks))

    def test_scheduler_run_duration(self):
        """
        Verifies that the scheduler run duration limit is followed.
        """
        dag_id = 'test_start_date_scheduling'
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()
        self.assertTrue(dag.start_date > DEFAULT_DATE)

        expected_run_duration = 5
        start_time = datetime.datetime.now()
        scheduler = SchedulerJob(dag_id,
                                 run_duration=expected_run_duration,
                                 **self.default_scheduler_args)
        scheduler.run()
        end_time = datetime.datetime.now()

        run_duration = (end_time - start_time).total_seconds()
        logging.info("Test ran in %.2fs, expected %.2fs",
                     run_duration,
                     expected_run_duration)
        assert run_duration - expected_run_duration < 5.0

    def test_dag_with_system_exit(self):
        """
        Test to check that a DAG with a system.exit() doesn't break the scheduler.
        """

        dag_id = 'exit_test_dag'
        dag_ids = [dag_id]
        dag_directory = os.path.join(models.DAGS_FOLDER,
                                     "..",
                                     "dags_with_system_exit")
        dag_file = os.path.join(dag_directory,
                                'b_test_scheduler_dags.py')

        dagbag = DagBag(dag_folder=dag_file)
        for dag_id in dag_ids:
            dag = dagbag.get_dag(dag_id)
            dag.clear()

        scheduler = SchedulerJob(dag_ids=dag_ids,
                                 subdir= dag_directory,
                                 num_runs=1,
                                 **self.default_scheduler_args)
        scheduler.run()
        session = settings.Session()
        self.assertEqual(
            len(session.query(TI).filter(TI.dag_id == dag_id).all()), 1)
