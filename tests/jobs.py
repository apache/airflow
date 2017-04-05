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
import shutil
import unittest
import six
import socket
from tempfile import mkdtemp

from airflow import AirflowException, settings, models
from airflow.bin import cli
from airflow.executors import SequentialExecutor
from airflow.jobs import BackfillJob, SchedulerJob, LocalTaskJob
from airflow.models import DAG, DagModel, DagBag, DagRun, Pool, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.db import provide_session
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.dag_processing import SimpleDagBag

from mock import patch
from sqlalchemy.orm.session import make_transient
from tests.executors.test_executor import TestExecutor

from tests.core import TEST_DAG_FOLDER

from airflow import configuration
configuration.load_test_config()

import sqlalchemy

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None

DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime.datetime(2016, 1, 1)

# Include the words "airflow" and "dag" in the file contents, tricking airflow into thinking these
# files contain a DAG (otherwise Airflow will skip them)
PARSEABLE_DAG_FILE_CONTENTS = '"airflow DAG"'
UNPARSEABLE_DAG_FILE_CONTENTS = 'airflow DAG'

# Filename to be used for dags that are created in an ad-hoc manner and can be removed/
# created at runtime
TEMP_DAG_FILENAME = "temp_dag.py"


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
            end_date=DEFAULT_DATE + datetime.timedelta(days=1),
            ignore_first_depends_on_past=True
        )
        job.run()

        session = settings.Session()
        drs = session.query(DagRun).filter(
            DagRun.dag_id=='example_bash_operator'
        ).order_by(DagRun.execution_date).all()

        self.assertTrue(drs[0].execution_date == DEFAULT_DATE)
        self.assertTrue(drs[0].state == State.SUCCESS)
        self.assertTrue(drs[1].execution_date ==
                        DEFAULT_DATE + datetime.timedelta(days=1))
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
            if 'example_dags' in dag.full_filepath and dag.dag_id not in skip_dags
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

        executor = TestExecutor(do_update=True)
        job = BackfillJob(dag=dag,
                          executor=executor,
                          start_date=DEFAULT_DATE,
                          end_date=DEFAULT_DATE + datetime.timedelta(days=2),
                          )
        job.run()

        # test executor history keeps a list
        history = executor.history

        # check if right order. Every loop has a 'pause' (0) to change state
        # from RUNNING to SUCCESS.
        # 6,0,3,0,3,0,3,0 = 8 loops
        self.assertEqual(8, len(history))

        loop_count = 0

        while len(history) > 0:
            queued_tasks = history.pop(0)
            if loop_count == 0:
                # first loop should contain 6 tasks (3 days x 2 tasks)
                self.assertEqual(6, len(queued_tasks))
            if loop_count == 2 or loop_count == 4 or loop_count == 6:
                # 3 days x 1 task
                self.assertEqual(3, len(queued_tasks))
            loop_count += 1

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
                               state=State.SUCCESS,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE)

        executor = TestExecutor(do_update=True)
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

    def test_backfill_execute_subdag(self):
        dag = self.dagbag.get_dag('example_subdag_operator')
        subdag_op_task = dag.get_task('section-1')

        subdag = subdag_op_task.subdag
        subdag.schedule_interval = '@daily'

        start_date = datetime.datetime.now()
        executor = TestExecutor(do_update=True)
        job = BackfillJob(dag=subdag,
                          start_date=start_date,
                          end_date=start_date,
                          executor=executor,
                          donot_pickle=True)
        job.run()

        history = executor.history
        subdag_history = history[0]

        # check that all 5 task instances of the subdag 'section-1' were executed
        self.assertEqual(5, len(subdag_history))
        for sdh in subdag_history:
            ti = sdh[3]
            self.assertIn('section-1-task-', ti.task_id)

        subdag.clear()
        dag.clear()


class LocalTaskJobTest(unittest.TestCase):
    def setUp(self):
        pass

    @patch.object(LocalTaskJob, "_is_descendant_process")
    def test_localtaskjob_heartbeat(self, is_descendant):
        session = settings.Session()
        dag = DAG(
            'test_localtaskjob_heartbeat',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='op1')

        dag.clear()
        dr = dag.create_dagrun(run_id="test",
                               state=State.SUCCESS,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.RUNNING
        ti.hostname = "blablabla"
        session.commit()

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        self.assertRaises(AirflowException, job1.heartbeat_callback)

        is_descendant.return_value = True
        ti.state = State.RUNNING
        ti.hostname = socket.getfqdn()
        ti.pid = 1
        session.merge(ti)
        session.commit()

        ret = job1.heartbeat_callback()
        self.assertEqual(ret, None)

        is_descendant.return_value = False
        self.assertRaises(AirflowException, job1.heartbeat_callback)

    def test_localtaskjob_double_trigger(self):
        dagbag = models.DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get('test_localtaskjob_double_trigger')
        task = dag.get_task('test_localtaskjob_double_trigger_task')

        session = settings.Session()

        dag.clear()
        dr = dag.create_dagrun(run_id="test",
                               state=State.SUCCESS,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)
        ti = dr.get_task_instance(task_id=task.task_id, session=session)
        ti.state = State.RUNNING
        ti.hostname = socket.getfqdn()
        ti.pid = 1
        session.commit()

        ti_run = TI(task=task, execution_date=DEFAULT_DATE)
        job1 = LocalTaskJob(task_instance=ti_run, ignore_ti_state=True, executor=SequentialExecutor())
        self.assertRaises(AirflowException, job1.run)

        ti = dr.get_task_instance(task_id=task.task_id, session=session)
        self.assertEqual(ti.pid, 1)
        self.assertEqual(ti.state, State.RUNNING)

        session.close()


class SchedulerJobTest(unittest.TestCase):
    # These defaults make the test faster to run
    default_scheduler_args = {"file_process_interval": 0,
                              "processor_poll_interval": 0.5}

    def setUp(self):
        self.dagbag = DagBag()
        session = settings.Session()
        session.query(models.ImportError).delete()
        session.commit()

    @staticmethod
    def run_single_scheduler_loop_with_no_dags(dags_folder):
        """
        Utility function that runs a single scheduler loop without actually
        changing/scheduling any dags. This is useful to simulate the other side effects of
        running a scheduler loop, e.g. to see what parse errors there are in the
        dags_folder.

        :param dags_folder: the directory to traverse
        :type directory: str
        """
        scheduler = SchedulerJob(
            dag_id='this_dag_doesnt_exist',  # We don't want to actually run anything
            num_runs=1,
            subdir=os.path.join(dags_folder))
        scheduler.heartrate = 0
        scheduler.run()

    def test_concurrency(self):
        dag_id = 'SchedulerJobTest.test_concurrency'
        task_id_1 = 'dummy_task'
        task_id_2 = 'dummy_task_nonexistent_queue'
        # important that len(tasks) is less than concurrency
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if concurrency was 3,
        # we could execute arbitrarily many tasks in the second run
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=3)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)
        dagbag = SimpleDagBag([dag])

        scheduler = SchedulerJob(**self.default_scheduler_args)
        session = settings.Session()

        # create first dag run with 1 running and 1 queued
        dr1 = scheduler.create_dag_run(dag)
        ti1 = TI(task1, dr1.execution_date)
        ti2 = TI(task2, dr1.execution_date)
        ti1.refresh_from_db()
        ti2.refresh_from_db()
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        session.merge(ti1)
        session.merge(ti2)
        session.commit()

        self.assertEqual(State.RUNNING, dr1.state)
        self.assertEqual(2, DAG.get_num_task_instances(dag_id, dag.task_ids,
            states=[State.RUNNING, State.QUEUED], session=session))

        # create second dag run
        dr2 = scheduler.create_dag_run(dag)
        ti3 = TI(task1, dr2.execution_date)
        ti4 = TI(task2, dr2.execution_date)
        ti3.refresh_from_db()
        ti4.refresh_from_db()
        # manually set to scheduled so we can pick them up
        ti3.state = State.SCHEDULED
        ti4.state = State.SCHEDULED
        session.merge(ti3)
        session.merge(ti4)
        session.commit()

        self.assertEqual(State.RUNNING, dr2.state)

        scheduler._execute_task_instances(dagbag, [State.SCHEDULED])

        # check that concurrency is respected
        ti1.refresh_from_db()
        ti2.refresh_from_db()
        ti3.refresh_from_db()
        ti4.refresh_from_db()
        self.assertEqual(3, DAG.get_num_task_instances(dag_id, dag.task_ids,
            states=[State.RUNNING, State.QUEUED], session=session))
        self.assertEqual(State.RUNNING, ti1.state)
        self.assertEqual(State.QUEUED, ti2.state)
        six.assertCountEqual(self, [State.QUEUED, State.SCHEDULED], [ti3.state, ti4.state])

        session.close()

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

    def test_dagrun_root_fail_unfinished(self):
        """
        DagRuns with one unfinished and one failed root task -> RUNNING
        """
        # Run both the failed and successful tasks
        scheduler = SchedulerJob(**self.default_scheduler_args)
        dag_id = 'test_dagrun_states_root_fail_unfinished'
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()
        dr = scheduler.create_dag_run(dag)
        try:
            dag.run(start_date=dr.execution_date, end_date=dr.execution_date)
        except AirflowException:  # Expect an exception since there is a failed task
            pass

        # Mark the successful task as never having run since we want to see if the
        # dagrun will be in a running state despite haveing an unfinished task.
        session = settings.Session()
        ti = dr.get_task_instance('test_dagrun_unfinished', session=session)
        ti.state = State.NONE
        session.commit()
        dr_state = dr.update_state()
        self.assertEqual(dr_state, State.RUNNING)

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

    def test_scheduler_max_active_runs_respected_after_clear(self):
        """
        Test if _process_task_instances only schedules ti's up to max_active_runs
        (related to issue AIRFLOW-137)
        """
        dag = DAG(
            dag_id='test_scheduler_max_active_runs_respected_after_clear',
            start_date=DEFAULT_DATE)
        dag.max_active_runs = 3

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

        # First create up to 3 dagruns in RUNNING state.
        scheduler.create_dag_run(dag)

        # Reduce max_active_runs to 1
        dag.max_active_runs = 1

        queue = mock.Mock()
        # and schedule them in, so we can check how many
        # tasks are put on the queue (should be one, not 3)
        scheduler._process_task_instances(dag, queue=queue)

        queue.append.assert_called_with(
            (dag.dag_id, dag_task1.task_id, DEFAULT_DATE)
        )

    @patch.object(TI, 'pool_full')
    def test_scheduler_verify_pool_full(self, mock_pool_full):
        """
        Test task instances not queued when pool is full
        """
        mock_pool_full.return_value = False

        dag = DAG(
            dag_id='test_scheduler_verify_pool_full',
            start_date=DEFAULT_DATE)

        DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_pool_full')

        session = settings.Session()
        pool = Pool(pool='test_scheduler_verify_pool_full', slots=1)
        session.add(pool)
        orm_dag = DagModel(dag_id=dag.dag_id)
        orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()

        scheduler = SchedulerJob()
        dag.clear()

        # Create 2 dagruns, which will create 2 task instances.
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEquals(dr.execution_date, DEFAULT_DATE)
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        queue = []
        scheduler._process_task_instances(dag, queue=queue)
        self.assertEquals(len(queue), 2)
        dagbag = SimpleDagBag([dag])

        # Recreated part of the scheduler here, to kick off tasks -> executor
        for ti_key in queue:
            task = dag.get_task(ti_key[1])
            ti = TI(task, ti_key[2])
            # Task starts out in the scheduled state. All tasks in the
            # scheduled state will be sent to the executor
            ti.state = State.SCHEDULED

            # Also save this task instance to the DB.
            session.merge(ti)
            session.commit()

        scheduler._execute_task_instances(dagbag,
                                          (State.SCHEDULED,
                                           State.UP_FOR_RETRY))

        self.assertEquals(len(scheduler.executor.queued_tasks), 1)

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
                                     subdir=os.path.join(settings.DAGS_FOLDER,
                                                         "no_dags.py"))
            scheduler.heartrate = 0
            scheduler.run()

        do_schedule()
        self.assertEquals(1, len(executor.queued_tasks))
        executor.queued_tasks.clear()

        do_schedule()
        self.assertEquals(2, len(executor.queued_tasks))

    def test_retry_still_in_executor(self):
        """
        Checks if the scheduler does not put a task in limbo, when a task is retried
        but is still present in the executor.
        """
        executor = TestExecutor()
        dagbag = DagBag(executor=executor)
        dagbag.dags.clear()
        dagbag.executor = executor

        dag = DAG(
            dag_id='test_retry_still_in_executor',
            start_date=DEFAULT_DATE,
            schedule_interval="@once")
        dag_task1 = BashOperator(
            task_id='test_retry_handling_op',
            bash_command='exit 1',
            retries=1,
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
                                     subdir=os.path.join(settings.DAGS_FOLDER,
                                                         "no_dags.py"))
            scheduler.heartrate = 0
            scheduler.run()

        do_schedule()
        self.assertEquals(1, len(executor.queued_tasks))

        def run_with_error(task):
            try:
                task.run()
            except AirflowException:
                pass

        ti_tuple = six.next(six.itervalues(executor.queued_tasks))
        (command, priority, queue, ti) = ti_tuple
        ti.task = dag_task1

        # fail execution
        run_with_error(ti)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 1)

        ti.refresh_from_db(lock_for_update=True, session=session)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        # do not schedule
        do_schedule()
        self.assertTrue(executor.has_task(ti))
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SCHEDULED)

        # now the executor has cleared and it should be allowed the re-queue
        executor.queued_tasks.clear()
        do_schedule()
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.QUEUED)

    @unittest.skipUnless("INTEGRATION" in os.environ, "Can only run end to end")
    def test_retry_handling_job(self):
        """
        Integration test of the scheduler not accidentally resetting
        the try_numbers for a task
        """
        dag = self.dagbag.get_dag('test_retry_handling_job')
        dag_task1 = dag.get_task("test_retry_handling_op")
        dag.clear()

        scheduler = SchedulerJob(dag_id=dag.dag_id,
                                 num_runs=1)
        scheduler.heartrate = 0
        scheduler.run()

        session = settings.Session()
        ti = session.query(TI).filter(TI.dag_id==dag.dag_id,
                                      TI.task_id==dag_task1.task_id).first()

        # make sure the counter has increased
        self.assertEqual(ti.try_number, 2)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)

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
        self.assertLess(run_duration - expected_run_duration, 5.0)

    def test_dag_with_system_exit(self):
        """
        Test to check that a DAG with a system.exit() doesn't break the scheduler.
        """

        dag_id = 'exit_test_dag'
        dag_ids = [dag_id]
        dag_directory = os.path.join(settings.DAGS_FOLDER,
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

    def test_dag_get_active_runs(self):
        """
        Test to check that a DAG returns it's active runs
        """

        now = datetime.datetime.now()
        six_hours_ago_to_the_hour = (now - datetime.timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)

        START_DATE = six_hours_ago_to_the_hour
        DAG_NAME1 = 'get_active_runs_test'

        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': START_DATE

        }
        dag1 = DAG(DAG_NAME1,
                   schedule_interval='* * * * *',
                   max_active_runs=1,
                   default_args=default_args
                   )

        run_this_1 = DummyOperator(task_id='run_this_1', dag=dag1)
        run_this_2 = DummyOperator(task_id='run_this_2', dag=dag1)
        run_this_2.set_upstream(run_this_1)
        run_this_3 = DummyOperator(task_id='run_this_3', dag=dag1)
        run_this_3.set_upstream(run_this_2)

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag1.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag1.clear()

        dr = scheduler.create_dag_run(dag1)

        # We had better get a dag run
        self.assertIsNotNone(dr)

        execution_date = dr.execution_date

        running_dates = dag1.get_active_runs()

        try:
            running_date = running_dates[0]
        except:
            running_date = 'Except'

        self.assertEqual(execution_date, running_date, 'Running Date must match Execution Date')

    def test_dag_catchup_option(self):
        """
        Test to check that a DAG with catchup = False only schedules beginning now, not back to the start date
        """

        now = datetime.datetime.now()
        six_hours_ago_to_the_hour = (now - datetime.timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)
        three_minutes_ago = now - datetime.timedelta(minutes=3)
        two_hours_and_three_minutes_ago = three_minutes_ago - datetime.timedelta(hours=2)

        START_DATE = six_hours_ago_to_the_hour
        DAG_NAME1 = 'no_catchup_test1'
        DAG_NAME2 = 'no_catchup_test2'
        DAG_NAME3 = 'no_catchup_test3'

        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': START_DATE

        }
        dag1 = DAG(DAG_NAME1,
                  schedule_interval='* * * * *',
                  max_active_runs=1,
                  default_args=default_args
                  )

        default_catchup = configuration.getboolean('scheduler', 'catchup_by_default')
        # Test configs have catchup by default ON

        self.assertEqual(default_catchup, True)

        # Correct default?
        self.assertEqual(dag1.catchup, True)

        dag2 = DAG(DAG_NAME2,
                  schedule_interval='* * * * *',
                  max_active_runs=1,
                  catchup=False,
                  default_args=default_args
                  )

        run_this_1 = DummyOperator(task_id='run_this_1', dag=dag2)
        run_this_2 = DummyOperator(task_id='run_this_2', dag=dag2)
        run_this_2.set_upstream(run_this_1)
        run_this_3 = DummyOperator(task_id='run_this_3', dag=dag2)
        run_this_3.set_upstream(run_this_2)

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag2.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag2.clear()

        dr = scheduler.create_dag_run(dag2)

        # We had better get a dag run
        self.assertIsNotNone(dr)

        # The DR should be scheduled in the last 3 minutes, not 6 hours ago
        self.assertGreater(dr.execution_date, three_minutes_ago)

        # The DR should be scheduled BEFORE now
        self.assertLess(dr.execution_date, datetime.datetime.now())

        dag3 = DAG(DAG_NAME3,
                   schedule_interval='@hourly',
                   max_active_runs=1,
                   catchup=False,
                   default_args=default_args
                   )

        run_this_1 = DummyOperator(task_id='run_this_1', dag=dag3)
        run_this_2 = DummyOperator(task_id='run_this_2', dag=dag3)
        run_this_2.set_upstream(run_this_1)
        run_this_3 = DummyOperator(task_id='run_this_3', dag=dag3)
        run_this_3.set_upstream(run_this_2)

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag3.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()

        scheduler = SchedulerJob()
        dag3.clear()

        dr = None
        dr = scheduler.create_dag_run(dag3)

        # We had better get a dag run
        self.assertIsNotNone(dr)

        # The DR should be scheduled in the last two hours, not 6 hours ago
        self.assertGreater(dr.execution_date, two_hours_and_three_minutes_ago)

        # The DR should be scheduled BEFORE now
        self.assertLess(dr.execution_date, datetime.datetime.now())

    def test_add_unparseable_file_before_sched_start_creates_import_error(self):
        try:
            dags_folder = mkdtemp()
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)
            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        session = settings.Session()
        import_errors = session.query(models.ImportError).all()

        self.assertEqual(len(import_errors), 1)
        import_error = import_errors[0]
        self.assertEqual(import_error.filename,
                         unparseable_filename)
        self.assertEqual(import_error.stacktrace,
                         "invalid syntax ({}, line 1)".format(TEMP_DAG_FILENAME))

    def test_add_unparseable_file_after_sched_start_creates_import_error(self):
        try:
            dags_folder = mkdtemp()
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)

            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        session = settings.Session()
        import_errors = session.query(models.ImportError).all()

        self.assertEqual(len(import_errors), 1)
        import_error = import_errors[0]
        self.assertEqual(import_error.filename,
                         unparseable_filename)
        self.assertEqual(import_error.stacktrace,
                         "invalid syntax ({}, line 1)".format(TEMP_DAG_FILENAME))

    def test_no_import_errors_with_parseable_dag(self):
        try:
            dags_folder = mkdtemp()
            parseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)

            with open(parseable_filename, 'w') as parseable_file:
                parseable_file.writelines(PARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        session = settings.Session()
        import_errors = session.query(models.ImportError).all()

        self.assertEqual(len(import_errors), 0)

    def test_new_import_error_replaces_old(self):
        try:
            dags_folder = mkdtemp()
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)

            # Generate original import error
            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)

            # Generate replacement import error (the error will be on the second line now)
            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(
                    PARSEABLE_DAG_FILE_CONTENTS +
                    os.linesep +
                    UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        session = settings.Session()
        import_errors = session.query(models.ImportError).all()

        self.assertEqual(len(import_errors), 1)
        import_error = import_errors[0]
        self.assertEqual(import_error.filename,
                         unparseable_filename)
        self.assertEqual(import_error.stacktrace,
                         "invalid syntax ({}, line 2)".format(TEMP_DAG_FILENAME))

    def test_remove_error_clears_import_error(self):
        try:
            dags_folder = mkdtemp()
            filename_to_parse = os.path.join(dags_folder, TEMP_DAG_FILENAME)

            # Generate original import error
            with open(filename_to_parse, 'w') as file_to_parse:
                file_to_parse.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)

            # Remove the import error from the file
            with open(filename_to_parse, 'w') as file_to_parse:
                file_to_parse.writelines(
                    PARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        session = settings.Session()
        import_errors = session.query(models.ImportError).all()

        self.assertEqual(len(import_errors), 0)

    def test_remove_file_clears_import_error(self):
        try:
            dags_folder = mkdtemp()
            filename_to_parse = os.path.join(dags_folder, TEMP_DAG_FILENAME)

            # Generate original import error
            with open(filename_to_parse, 'w') as file_to_parse:
                file_to_parse.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        # Rerun the scheduler once the dag file has been removed
        self.run_single_scheduler_loop_with_no_dags(dags_folder)

        session = settings.Session()
        import_errors = session.query(models.ImportError).all()

        self.assertEqual(len(import_errors), 0)
