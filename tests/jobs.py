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
import os
import unittest

from airflow import AirflowException, settings
from airflow.bin import cli
from airflow.jobs import BackfillJob, SchedulerJob
from airflow.models import DagBag, DagRun, Pool, TaskInstance as TI
from airflow.utils.state import State
from airflow.utils.tests import get_dag
from airflow.utils.timeout import timeout
from airflow.utils.db import provide_session

DEV_NULL = '/dev/null'
DEFAULT_DATE = datetime.datetime(2016, 1, 1)
TEST_DAGS_FOLDER = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), 'dags')


class BackfillJobTest(unittest.TestCase):

    def setUp(self):
        self.parser = cli.CLIFactory.get_parser()

    def test_backfill_examples(self):
        dagbag = DagBag(
            dag_folder=DEV_NULL, include_examples=True)
        dags = [
            dag for dag in dagbag.dags.values()
            if dag.dag_id in ('example_bash_operator',)]
        for dag in dags:
            dag.clear(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
        for dag in dags:
            job = BackfillJob(
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
            job.run()

    def test_trap_executor_error(self):
        """
        Test for https://github.com/airbnb/airflow/pull/1220

        Test that errors setting up tasks (before tasks run) are properly
        caught
        """
        dagbag = DagBag(dag_folder=TEST_DAGS_FOLDER)
        dags = [
            dag for dag in dagbag.dags.values()
            if dag.dag_id in ('test_raise_executor_error',)]
        for dag in dags:
            dag.clear(
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
        for dag in dags:
            job = BackfillJob(
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
            # run with timeout because this creates an infinite loop if not
            # caught
            def run_with_timeout():
                with timeout(seconds=30):
                    job.run()
            self.assertRaises(AirflowException, run_with_timeout)

    def test_backfill_pooled_task(self):
        """
        Test for https://github.com/airbnb/airflow/pull/1225

        Test that queued tasks are executed by BackfillJob
        """
        session = settings.Session()
        pool = Pool(pool='test_backfill_pooled_task_pool', slots=1)
        session.add(pool)
        session.commit()

        dag = get_dag('test_backfill_pooled_task_dag', TEST_DAGS_FOLDER)

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
        dag = get_dag('test_depends_on_past', TEST_DAGS_FOLDER)
        run_date = dag.start_date + datetime.timedelta(days=5)
        # import ipdb; ipdb.set_trace()
        BackfillJob(dag=dag, start_date=run_date, end_date=run_date).run()

        # ti should not have run
        ti = TI(dag.tasks[0], run_date)
        ti.refresh_from_db()
        self.assertIs(ti.state, None)

        BackfillJob(
            dag=dag,
            start_date=run_date,
            end_date=run_date,
            ignore_first_depends_on_past=True).run()

        # ti should have run
        ti = TI(dag.tasks[0], run_date)
        ti.refresh_from_db()
        self.assertEquals(ti.state, State.SUCCESS)

    def test_cli_backfill_depends_on_past(self):
        dag_id = 'test_dagrun_states_deadlock'
        run_date = DEFAULT_DATE + datetime.timedelta(days=1)
        args = [
            'backfill',
            dag_id,
            '-l',
            '-sd',
            TEST_DAGS_FOLDER,
            '-s',
            run_date.isoformat(),
        ]
        dag = get_dag(dag_id, TEST_DAGS_FOLDER)

        cli.backfill(self.parser.parse_args(args))
        ti = TI(dag.get_task('test_depends_on_past'), run_date)
        ti.refresh_from_db()
        # task did not run
        self.assertEqual(ti.state, State.NONE)

        cli.backfill(self.parser.parse_args(args + ['-I']))
        ti = TI(dag.get_task('test_depends_on_past'), run_date)
        ti.refresh_from_db()
        # task ran
        self.assertEqual(ti.state, State.SUCCESS)


class SchedulerJobTest(unittest.TestCase):
    @provide_session
    def evaluate_dagrun(
            self,
            dag_id,
            first_task_state,
            second_task_state,
            dagrun_state,
            run_kwargs=None,
            advance_execution_date=False,
            session=None):
        """
        Helper for testing DagRun states with simple two-task DAGS
        """
        if run_kwargs is None:
            run_kwargs = {}

        scheduler = SchedulerJob()
        dag = get_dag(dag_id, TEST_DAGS_FOLDER)
        dr = scheduler.schedule_dag(dag)
        if advance_execution_date:
            # run a second time to schedule a dagrun after the start_date
            dr = scheduler.schedule_dag(dag)
        ex_date = dr.execution_date

        # if 'test_dagrun_states_deadlock' in dag_id and run_kwargs:
        #     import ipdb; ipdb.set_trace()

        try:
            dag.run(start_date=ex_date, end_date=ex_date, **run_kwargs)
        except AirflowException:
            pass

        # test tasks
        task_1, task_2 = dag.tasks
        ti = TI(task_1, ex_date)
        ti.refresh_from_db()
        self.assertEqual(ti.state, first_task_state)
        ti = TI(task_2, ex_date)
        ti.refresh_from_db()
        self.assertEqual(ti.state, second_task_state)

        # load dagrun
        dr = session.query(DagRun).filter(
            DagRun.dag_id == dag.dag_id,
            DagRun.execution_date == ex_date
        ).first()

        # dagrun is running
        self.assertEqual(dr.state, State.RUNNING)

        # import ipdb; ipdb.set_trace()

        dag.get_active_runs()

        # dagrun failed
        self.assertEqual(dr.state, dagrun_state)

    def test_dagrun_fail(self):
        """
        Test that a DagRun with one failed task and one incomplete root task
        is marked a failure
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_fail',
            first_task_state=State.FAILED,
            second_task_state=State.UPSTREAM_FAILED,
            dagrun_state=State.FAILED)

    def test_dagrun_success(self):
        """
        Test that a DagRun with one failed task and one successful root task
        is marked a success
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_success',
            first_task_state=State.FAILED,
            second_task_state=State.SUCCESS,
            dagrun_state=State.SUCCESS)

    def test_dagrun_root_fail(self):
        """
        Test that a DagRun with one successful root task and one failed root
        task is marked a failure
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_root_fail',
            first_task_state=State.SUCCESS,
            second_task_state=State.FAILED,
            dagrun_state=State.FAILED)

    def test_dagrun_deadlock(self):
        """
        Test that a deadlocked dagrun is marked as a failure by having
        depends_on_past and an execution_date after the start_date
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            first_task_state=None,
            second_task_state=None,
            dagrun_state=State.FAILED,
            advance_execution_date=True)

    def test_dagrun_deadlock_ignore_depends_on_past_advance_ex_date(self):
        """
        Test that an otherwise-deadlocked dagrun is marked as a success
        if ignore_first_depends_on_past=True and the dagrun execution_date
        is after the start_date.
        """
        self.evaluate_dagrun(
            dag_id='test_dagrun_states_deadlock',
            first_task_state=State.SUCCESS,
            second_task_state=State.SUCCESS,
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
            first_task_state=State.SUCCESS,
            second_task_state=State.SUCCESS,
            dagrun_state=State.SUCCESS,
            run_kwargs=dict(ignore_first_depends_on_past=True))
