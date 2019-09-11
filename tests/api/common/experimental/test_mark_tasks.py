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

import unittest
import time
from datetime import datetime, timedelta

from airflow import configuration, models
from airflow.api.common.experimental.mark_tasks import (
    set_state, _create_dagruns, set_dag_run_state_to_success, set_dag_run_state_to_failed,
    set_dag_run_state_to_running)
from airflow.utils import timezone
from airflow.utils.db import create_session, provide_session
from airflow.utils.dates import days_ago
from airflow.utils.state import State
from airflow.models import DagRun
from tests.test_utils.db import clear_db_runs

DEV_NULL = "/dev/null"

configuration.load_test_config()


class TestMarkTasks(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dagbag = models.DagBag(include_examples=True)
        cls.dag1 = dagbag.dags['example_bash_operator']
        cls.dag1.sync_to_db()
        cls.dag2 = dagbag.dags['example_subdag_operator']
        cls.dag2.sync_to_db()
        cls.dag3 = dagbag.dags['example_trigger_target_dag']
        cls.dag3.sync_to_db()
        cls.execution_dates = [days_ago(2), days_ago(1)]
        start_date3 = cls.dag3.default_args["start_date"]
        cls.dag3_execution_dates = [start_date3, start_date3 + timedelta(days=1),
                                    start_date3 + timedelta(days=2)]

    def setUp(self):
        clear_db_runs()
        drs = _create_dagruns(self.dag1, self.execution_dates,
                              state=State.RUNNING,
                              run_id_template="scheduled__{}")
        for dr in drs:
            dr.dag = self.dag1
            dr.verify_integrity()

        drs = _create_dagruns(self.dag2,
                              [self.dag2.default_args['start_date']],
                              state=State.RUNNING,
                              run_id_template="scheduled__{}")

        for dr in drs:
            dr.dag = self.dag2
            dr.verify_integrity()

        drs = _create_dagruns(self.dag3,
                              self.dag3_execution_dates,
                              state=State.SUCCESS,
                              run_id_template="manual__{}")
        for dr in drs:
            dr.dag = self.dag3
            dr.verify_integrity()

    def tearDown(self):
        clear_db_runs()

    @staticmethod
    def snapshot_state(dag, execution_dates):
        TI = models.TaskInstance
        with create_session() as session:
            return session.query(TI).filter(
                TI.dag_id == dag.dag_id,
                TI.execution_date.in_(execution_dates)
            ).all()

    @provide_session
    def verify_state(self, dag, task_ids, execution_dates, state, old_tis, session=None):
        TI = models.TaskInstance

        tis = session.query(TI).filter(
            TI.dag_id == dag.dag_id,
            TI.execution_date.in_(execution_dates)
        ).all()

        self.assertTrue(len(tis) > 0)

        for ti in tis:
            if ti.task_id in task_ids and ti.execution_date in execution_dates:
                self.assertEqual(ti.state, state)
            else:
                for old_ti in old_tis:
                    if old_ti.task_id == ti.task_id and old_ti.execution_date == ti.execution_date:
                        self.assertEqual(ti.state, old_ti.state)

    def test_mark_tasks_now(self):
        # set one task to success but do not commit
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=False)
        self.assertEqual(len(altered), 1)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          None, snapshot)

        # set one and only one task to success
        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 1)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

        # set no tasks
        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 0)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

        # set task to other than success
        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.FAILED, commit=True)
        self.assertEqual(len(altered), 1)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          State.FAILED, snapshot)

        # dont alter other tasks
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_0")
        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 1)
        self.verify_state(self.dag1, [task.task_id], [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

        # set one task as FAILED. dag3 has schedule_interval None
        snapshot = TestMarkTasks.snapshot_state(self.dag3, self.dag3_execution_dates)
        task = self.dag3.get_task("run_this")
        altered = set_state(tasks=[task], execution_date=self.dag3_execution_dates[1],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.FAILED, commit=True)
        # exactly one TaskInstance should have been altered
        self.assertEqual(len(altered), 1)
        # task should have been marked as failed
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[1]],
                          State.FAILED, snapshot)
        # tasks on other days should be unchanged
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[0]],
                          None, snapshot)
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[2]],
                          None, snapshot)

    def test_mark_downstream(self):
        # test downstream
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        relatives = task.get_flat_relatives(upstream=False)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=False, downstream=True, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 3)
        self.verify_state(self.dag1, task_ids, [self.execution_dates[0]], State.SUCCESS, snapshot)

    def test_mark_upstream(self):
        # test upstream
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("run_after_loop")
        relatives = task.get_flat_relatives(upstream=True)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=True, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 4)
        self.verify_state(self.dag1, task_ids, [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

    def test_mark_tasks_future(self):
        # set one task to success towards end of scheduled dag runs
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=True,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 2)
        self.verify_state(self.dag1, [task.task_id], self.execution_dates, State.SUCCESS, snapshot)

        snapshot = TestMarkTasks.snapshot_state(self.dag3, self.dag3_execution_dates)
        task = self.dag3.get_task("run_this")
        altered = set_state(tasks=[task], execution_date=self.dag3_execution_dates[1],
                            upstream=False, downstream=False, future=True,
                            past=False, state=State.FAILED, commit=True)
        self.assertEqual(len(altered), 2)
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[0]], None, snapshot)
        self.verify_state(self.dag3, [task.task_id], self.dag3_execution_dates[1:], State.FAILED, snapshot)

    def test_mark_tasks_past(self):
        # set one task to success towards end of scheduled dag runs
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        task = self.dag1.get_task("runme_1")
        altered = set_state(tasks=[task], execution_date=self.execution_dates[1],
                            upstream=False, downstream=False, future=False,
                            past=True, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 2)
        self.verify_state(self.dag1, [task.task_id], self.execution_dates, State.SUCCESS, snapshot)

        snapshot = TestMarkTasks.snapshot_state(self.dag3, self.dag3_execution_dates)
        task = self.dag3.get_task("run_this")
        altered = set_state(tasks=[task], execution_date=self.dag3_execution_dates[1],
                            upstream=False, downstream=False, future=False,
                            past=True, state=State.FAILED, commit=True)
        self.assertEqual(len(altered), 2)
        self.verify_state(self.dag3, [task.task_id], self.dag3_execution_dates[:2], State.FAILED, snapshot)
        self.verify_state(self.dag3, [task.task_id], [self.dag3_execution_dates[2]], None, snapshot)

    def test_mark_tasks_multiple(self):
        # set multiple tasks to success
        snapshot = TestMarkTasks.snapshot_state(self.dag1, self.execution_dates)
        tasks = [self.dag1.get_task("runme_1"), self.dag1.get_task("runme_2")]
        altered = set_state(tasks=tasks, execution_date=self.execution_dates[0],
                            upstream=False, downstream=False, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 2)
        self.verify_state(self.dag1, [task.task_id for task in tasks], [self.execution_dates[0]],
                          State.SUCCESS, snapshot)

    # TODO: this skipIf should be removed once a fixing solution is found later
    #       We skip it here because this test case is working with Postgres & SQLite
    #       but not with MySQL
    @unittest.skipIf('mysql' in configuration.conf.get('core', 'sql_alchemy_conn'), "Flaky with MySQL")
    def test_mark_tasks_subdag(self):
        # set one task to success towards end of scheduled dag runs
        task = self.dag2.get_task("section-1")
        relatives = task.get_flat_relatives(upstream=False)
        task_ids = [t.task_id for t in relatives]
        task_ids.append(task.task_id)

        altered = set_state(tasks=[task], execution_date=self.execution_dates[0],
                            upstream=False, downstream=True, future=False,
                            past=False, state=State.SUCCESS, commit=True)
        self.assertEqual(len(altered), 14)

        # cannot use snapshot here as that will require drilling down the
        # the sub dag tree essentially recreating the same code as in the
        # tested logic.
        self.verify_state(self.dag2, task_ids, [self.execution_dates[0]],
                          State.SUCCESS, [])


class TestMarkDAGRun(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        dagbag = models.DagBag(include_examples=True)
        cls.dag1 = dagbag.dags['example_bash_operator']
        cls.dag1.sync_to_db()
        cls.dag2 = dagbag.dags['example_subdag_operator']
        cls.dag2.sync_to_db()
        cls.execution_dates = [days_ago(2), days_ago(1), days_ago(0)]

    def setUp(self):
        clear_db_runs()

    def _set_default_task_instance_states(self, dr):
        # success task
        dr.get_task_instance('runme_0').set_state(State.SUCCESS)
        # skipped task
        dr.get_task_instance('runme_1').set_state(State.SKIPPED)
        # retry task
        dr.get_task_instance('runme_2').set_state(State.UP_FOR_RETRY)
        # queued task
        dr.get_task_instance('also_run_this').set_state(State.QUEUED)
        # running task
        dr.get_task_instance('run_after_loop').set_state(State.RUNNING)
        # failed task
        dr.get_task_instance('run_this_last').set_state(State.FAILED)

    def _verify_task_instance_states_remain_default(self, dr):
        self.assertEqual(dr.get_task_instance('runme_0').state, State.SUCCESS)
        self.assertEqual(dr.get_task_instance('runme_1').state, State.SKIPPED)
        self.assertEqual(dr.get_task_instance('runme_2').state, State.UP_FOR_RETRY)
        self.assertEqual(dr.get_task_instance('also_run_this').state, State.QUEUED)
        self.assertEqual(dr.get_task_instance('run_after_loop').state, State.RUNNING)
        self.assertEqual(dr.get_task_instance('run_this_last').state, State.FAILED)

    @provide_session
    def _verify_task_instance_states(self, dag, date, state, session):
        TI = models.TaskInstance
        tis = session.query(TI)\
            .filter(TI.dag_id == dag.dag_id, TI.execution_date == date)
        for ti in tis:
            self.assertEqual(ti.state, state)

    def _create_test_dag_run(self, state, date):
        return self.dag1.create_dagrun(
            run_id='manual__' + datetime.now().isoformat(),
            state=state,
            execution_date=date
        )

    def _verify_dag_run_state(self, dag, date, state):
        drs = models.DagRun.find(dag_id=dag.dag_id, execution_date=date)
        dr = drs[0]

        self.assertEqual(dr.get_state(), state)

    @provide_session
    def _verify_dag_run_dates(self, dag, date, state, middle_time, session=None):
        # When target state is RUNNING, we should set start_date,
        # otherwise we should set end_date.
        DR = DagRun
        dr = session.query(DR).filter(
            DR.dag_id == dag.dag_id,
            DR.execution_date == date
        ).one()
        if state == State.RUNNING:
            # Since the DAG is running, the start_date must be updated after creation
            self.assertGreater(dr.start_date, middle_time)
            # If the dag is still running, we don't have an end date
            self.assertIsNone(dr.end_date)
        else:
            # If the dag is not running, there must be an end time
            self.assertLess(dr.start_date, middle_time)
            self.assertGreater(dr.end_date, middle_time)

    def test_set_running_dag_run_to_success(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.RUNNING, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_success(self.dag1, date, commit=True)

        # All except the SUCCESS task should be altered.
        self.assertEqual(len(altered), 5)
        self._verify_dag_run_state(self.dag1, date, State.SUCCESS)
        self._verify_task_instance_states(self.dag1, date, State.SUCCESS)
        self._verify_dag_run_dates(self.dag1, date, State.SUCCESS, middle_time)

    def test_set_running_dag_run_to_failed(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.RUNNING, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_failed(self.dag1, date, commit=True)

        # Only running task should be altered.
        self.assertEqual(len(altered), 1)
        self._verify_dag_run_state(self.dag1, date, State.FAILED)
        self.assertEqual(dr.get_task_instance('run_after_loop').state, State.FAILED)
        self._verify_dag_run_dates(self.dag1, date, State.FAILED, middle_time)

    def test_set_running_dag_run_to_running(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.RUNNING, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_running(self.dag1, date, commit=True)

        # None of the tasks should be altered, only the dag itself
        self.assertEqual(len(altered), 0)
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)
        self._verify_dag_run_dates(self.dag1, date, State.RUNNING, middle_time)

    def test_set_success_dag_run_to_success(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_success(self.dag1, date, commit=True)

        # All except the SUCCESS task should be altered.
        self.assertEqual(len(altered), 5)
        self._verify_dag_run_state(self.dag1, date, State.SUCCESS)
        self._verify_task_instance_states(self.dag1, date, State.SUCCESS)
        self._verify_dag_run_dates(self.dag1, date, State.SUCCESS, middle_time)

    def test_set_success_dag_run_to_failed(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_failed(self.dag1, date, commit=True)

        # Only running task should be altered.
        self.assertEqual(len(altered), 1)
        self._verify_dag_run_state(self.dag1, date, State.FAILED)
        self.assertEqual(dr.get_task_instance('run_after_loop').state, State.FAILED)
        self._verify_dag_run_dates(self.dag1, date, State.FAILED, middle_time)

    def test_set_success_dag_run_to_running(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_running(self.dag1, date, commit=True)

        # None of the tasks should be altered, but only the dag object should be changed
        self.assertEqual(len(altered), 0)
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)
        self._verify_dag_run_dates(self.dag1, date, State.RUNNING, middle_time)

    def test_set_failed_dag_run_to_success(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_success(self.dag1, date, commit=True)

        # All except the SUCCESS task should be altered.
        self.assertEqual(len(altered), 5)
        self._verify_dag_run_state(self.dag1, date, State.SUCCESS)
        self._verify_task_instance_states(self.dag1, date, State.SUCCESS)
        self._verify_dag_run_dates(self.dag1, date, State.SUCCESS, middle_time)

    def test_set_failed_dag_run_to_failed(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        altered = set_dag_run_state_to_failed(self.dag1, date, commit=True)

        # Only running task should be altered.
        self.assertEqual(len(altered), 1)
        self._verify_dag_run_state(self.dag1, date, State.FAILED)
        self.assertEqual(dr.get_task_instance('run_after_loop').state, State.FAILED)
        self._verify_dag_run_dates(self.dag1, date, State.FAILED, middle_time)

    def test_set_failed_dag_run_to_running(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        middle_time = timezone.utcnow()
        self._set_default_task_instance_states(dr)

        time.sleep(2)

        altered = set_dag_run_state_to_running(self.dag1, date, commit=True)

        # None of the tasks should be altered, since we've only altered the DAG itself
        self.assertEqual(len(altered), 0)
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)
        self._verify_dag_run_dates(self.dag1, date, State.RUNNING, middle_time)

    def test_set_state_without_commit(self):
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.RUNNING, date)
        self._set_default_task_instance_states(dr)

        will_be_altered = set_dag_run_state_to_running(self.dag1, date, commit=False)

        # None of the tasks will be altered.
        self.assertEqual(len(will_be_altered), 0)
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)

        will_be_altered = set_dag_run_state_to_failed(self.dag1, date, commit=False)

        # Only the running task will be altered.
        self.assertEqual(len(will_be_altered), 1)
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)

        will_be_altered = set_dag_run_state_to_success(self.dag1, date, commit=False)

        # All except the SUCCESS task should be altered.
        self.assertEqual(len(will_be_altered), 5)
        self._verify_dag_run_state(self.dag1, date, State.RUNNING)
        self._verify_task_instance_states_remain_default(dr)

    @provide_session
    def test_set_state_with_multiple_dagruns(self, session=None):
        self.dag2.create_dagrun(
            run_id='manual__' + datetime.now().isoformat(),
            state=State.FAILED,
            execution_date=self.execution_dates[0],
            session=session
        )
        self.dag2.create_dagrun(
            run_id='manual__' + datetime.now().isoformat(),
            state=State.FAILED,
            execution_date=self.execution_dates[1],
            session=session
        )
        self.dag2.create_dagrun(
            run_id='manual__' + datetime.now().isoformat(),
            state=State.RUNNING,
            execution_date=self.execution_dates[2],
            session=session
        )

        altered = set_dag_run_state_to_success(self.dag2, self.execution_dates[1], commit=True)

        # Recursively count number of tasks in the dag
        def count_dag_tasks(dag):
            count = len(dag.tasks)
            subdag_counts = [count_dag_tasks(subdag) for subdag in dag.subdags]
            count += sum(subdag_counts)
            return count

        self.assertEqual(len(altered), count_dag_tasks(self.dag2))
        self._verify_dag_run_state(self.dag2, self.execution_dates[1], State.SUCCESS)

        # Make sure other dag status are not changed
        models.DagRun.find(dag_id=self.dag2.dag_id,
                           execution_date=self.execution_dates[0])
        self._verify_dag_run_state(self.dag2, self.execution_dates[0], State.FAILED)
        models.DagRun.find(dag_id=self.dag2.dag_id,
                           execution_date=self.execution_dates[2])
        self._verify_dag_run_state(self.dag2, self.execution_dates[2], State.RUNNING)

    def test_set_dag_run_state_edge_cases(self):
        # Dag does not exist
        altered = set_dag_run_state_to_success(None, self.execution_dates[0])
        self.assertEqual(len(altered), 0)
        altered = set_dag_run_state_to_failed(None, self.execution_dates[0])
        self.assertEqual(len(altered), 0)
        altered = set_dag_run_state_to_running(None, self.execution_dates[0])
        self.assertEqual(len(altered), 0)

        # Invalid execution date
        altered = set_dag_run_state_to_success(self.dag1, None)
        self.assertEqual(len(altered), 0)
        altered = set_dag_run_state_to_failed(self.dag1, None)
        self.assertEqual(len(altered), 0)
        altered = set_dag_run_state_to_running(self.dag1, None)
        self.assertEqual(len(altered), 0)

        # This will throw ValueError since dag.latest_execution_date
        # need to be 0 does not exist.
        self.assertRaises(ValueError, set_dag_run_state_to_success, self.dag1,
                          timezone.make_naive(self.execution_dates[0]))

        # altered = set_dag_run_state_to_success(self.dag1, self.execution_dates[0])
        # DagRun does not exist
        # This will throw ValueError since dag.latest_execution_date does not exist
        self.assertRaises(ValueError, set_dag_run_state_to_success,
                          self.dag1, self.execution_dates[0])

    def test_set_dag_run_state_to_failed_no_running_tasks(self):
        """
        set_dag_run_state_to_failed when there are no running tasks to update
        """
        date = self.execution_dates[0]
        dr = self._create_test_dag_run(State.SUCCESS, date)
        for task in self.dag1.tasks:
            dr.get_task_instance(task.task_id).set_state(State.SUCCESS)

        set_dag_run_state_to_failed(self.dag1, date)

    def tearDown(self):
        self.dag1.clear()
        self.dag2.clear()

        with create_session() as session:
            session.query(models.DagRun).delete()
            session.query(models.TaskInstance).delete()


if __name__ == '__main__':
    unittest.main()
