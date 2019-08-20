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
import os
import shutil
import unittest
from tempfile import mkdtemp

import psutil
import six
from parameterized import parameterized

import airflow.example_dags
from airflow import AirflowException, models, settings
from airflow import configuration
from airflow.executors import BaseExecutor
from airflow.jobs import BackfillJob, SchedulerJob
from airflow.models import DAG, DagBag, DagModel, DagRun, Pool, SlaMiss, \
    TaskInstance as TI, errors
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import timezone
from airflow.utils.dag_processing import SimpleDag, SimpleDagBag, list_py_file_paths
from airflow.utils.dates import days_ago
from airflow.utils.db import create_session, provide_session
from airflow.utils.state import State
from tests.compat import MagicMock, Mock, PropertyMock, patch
from tests.compat import mock
from tests.core import TEST_DAG_FOLDER
from tests.executors.test_executor import TestExecutor
from tests.test_utils.db import clear_db_dags, clear_db_errors, clear_db_pools, \
    clear_db_runs, clear_db_sla_miss, set_default_pool_slots


DEFAULT_DATE = timezone.datetime(2016, 1, 1)
TRY_NUMBER = 1
# Include the words "airflow" and "dag" in the file contents,
# tricking airflow into thinking these
# files contain a DAG (otherwise Airflow will skip them)
PARSEABLE_DAG_FILE_CONTENTS = '"airflow DAG"'
UNPARSEABLE_DAG_FILE_CONTENTS = 'airflow DAG'

# Filename to be used for dags that are created in an ad-hoc manner and can be removed/
# created at runtime
TEMP_DAG_FILENAME = "temp_dag.py"


class SchedulerJobTest(unittest.TestCase):

    def setUp(self):
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_errors()

        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = TestExecutor()

    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()

        def getboolean(section, key):
            if section.lower() == 'core' and key.lower() == 'load_examples':
                return False
            else:
                return configuration.conf.getboolean(section, key)

        cls.patcher = mock.patch('airflow.jobs.scheduler_job.conf.getboolean')
        cls.mock_getboolean = cls.patcher.start()
        cls.mock_getboolean.side_effect = getboolean

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()

    def test_is_alive(self):
        job = SchedulerJob(None, heartrate=10, state=State.RUNNING)
        self.assertTrue(job.is_alive())

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
        self.assertTrue(job.is_alive())

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=31)
        self.assertFalse(job.is_alive())

        job.state = State.SUCCESS
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=10)
        self.assertFalse(job.is_alive(), "Completed jobs even with recent heartbeat should not be alive")

    def run_single_scheduler_loop_with_no_dags(self, dags_folder):
        """
        Utility function that runs a single scheduler loop without actually
        changing/scheduling any dags. This is useful to simulate the other side effects of
        running a scheduler loop, e.g. to see what parse errors there are in the
        dags_folder.

        :param dags_folder: the directory to traverse
        :type directory: str
        """
        scheduler = SchedulerJob(
            executor=self.null_exec,
            dag_id='this_dag_doesnt_exist',  # We don't want to actually run anything
            num_runs=1,
            subdir=os.path.join(dags_folder))
        scheduler.heartrate = 0
        scheduler.run()

    def _make_simple_dag_bag(self, dags):
        return SimpleDagBag([SimpleDag(dag) for dag in dags])

    def test_no_orphan_process_will_be_left(self):
        empty_dir = mkdtemp()
        current_process = psutil.Process()
        old_children = current_process.children(recursive=True)
        scheduler = SchedulerJob(subdir=empty_dir,
                                 num_runs=1,
                                 executor=TestExecutor(do_update=False))
        scheduler.run()
        shutil.rmtree(empty_dir)

        scheduler.executor.terminate()
        # Remove potential noise created by previous tests.
        current_children = set(current_process.children(recursive=True)) - set(
            old_children)
        self.assertFalse(current_children)

    def test_process_executor_events(self):
        dag_id = "test_process_executor_events"
        dag_id2 = "test_process_executor_events_2"
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        dag2 = DAG(dag_id=dag_id2, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        DummyOperator(dag=dag2, task_id=task_id_1)

        dagbag1 = self._make_simple_dag_bag([dag])
        dagbag2 = self._make_simple_dag_bag([dag2])

        scheduler = SchedulerJob()
        session = settings.Session()

        ti1 = TI(task1, DEFAULT_DATE)
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor = TestExecutor(do_update=False)
        executor.event_buffer[ti1.key] = State.FAILED

        scheduler.executor = executor

        # dag bag does not contain dag_id
        scheduler._process_executor_events(simple_dag_bag=dagbag2)
        ti1.refresh_from_db()
        self.assertEqual(ti1.state, State.QUEUED)

        # dag bag does contain dag_id
        scheduler._process_executor_events(simple_dag_bag=dagbag1)
        ti1.refresh_from_db()
        self.assertEqual(ti1.state, State.FAILED)

        ti1.state = State.SUCCESS
        session.merge(ti1)
        session.commit()
        executor.event_buffer[ti1.key] = State.SUCCESS

        scheduler._process_executor_events(simple_dag_bag=dagbag1)
        ti1.refresh_from_db()
        self.assertEqual(ti1.state, State.SUCCESS)

    def test_execute_task_instances_is_paused_wont_execute(self):
        dag_id = 'SchedulerJobTest.test_execute_task_instances_is_paused_wont_execute'
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        ti1 = TI(task1, DEFAULT_DATE)
        ti1.state = State.SCHEDULED
        dr1.state = State.RUNNING
        dagmodel = models.DagModel()
        dagmodel.dag_id = dag_id
        dagmodel.is_paused = True
        session.merge(ti1)
        session.merge(dr1)
        session.add(dagmodel)
        session.commit()

        scheduler._execute_task_instances(dagbag, [State.SCHEDULED])
        ti1.refresh_from_db()
        self.assertEqual(State.SCHEDULED, ti1.state)

    def test_execute_task_instances_no_dagrun_task_will_execute(self):
        """
        Tests that tasks without dagrun still get executed.
        """
        dag_id = 'SchedulerJobTest.test_execute_task_instances_no_dagrun_task_will_execute'
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        scheduler.create_dag_run(dag)
        ti1 = TI(task1, DEFAULT_DATE)
        ti1.state = State.SCHEDULED
        ti1.execution_date = ti1.execution_date + datetime.timedelta(days=1)
        session.merge(ti1)
        session.commit()

        scheduler._execute_task_instances(dagbag, [State.SCHEDULED])
        ti1.refresh_from_db()
        self.assertEqual(State.QUEUED, ti1.state)

    def test_execute_task_instances_backfill_tasks_wont_execute(self):
        """
        Tests that backfill tasks won't get executed.
        """
        dag_id = 'SchedulerJobTest.test_execute_task_instances_backfill_tasks_wont_execute'
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr1.run_id = BackfillJob.ID_PREFIX + '_blah'
        ti1 = TI(task1, dr1.execution_date)
        ti1.refresh_from_db()
        ti1.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(dr1)
        session.commit()

        self.assertTrue(dr1.is_backfill)

        scheduler._execute_task_instances(dagbag, [State.SCHEDULED])
        ti1.refresh_from_db()
        self.assertEqual(State.SCHEDULED, ti1.state)

    def test_find_executable_task_instances_backfill_nodagrun(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_backfill_nodagrun'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)
        dr2.run_id = BackfillJob.ID_PREFIX + 'asdf'

        ti_no_dagrun = TI(task1, DEFAULT_DATE - datetime.timedelta(days=1))
        ti_backfill = TI(task1, dr2.execution_date)
        ti_with_dagrun = TI(task1, dr1.execution_date)
        # ti_with_paused
        ti_no_dagrun.state = State.SCHEDULED
        ti_backfill.state = State.SCHEDULED
        ti_with_dagrun.state = State.SCHEDULED

        session.merge(dr2)
        session.merge(ti_no_dagrun)
        session.merge(ti_backfill)
        session.merge(ti_with_dagrun)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(2, len(res))
        res_keys = map(lambda x: x.key, res)
        self.assertIn(ti_no_dagrun.key, res_keys)
        self.assertIn(ti_with_dagrun.key, res_keys)

    def test_find_executable_task_instances_pool(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_pool'
        task_id_1 = 'dummy'
        task_id_2 = 'dummydummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1, pool='a')
        task2 = DummyOperator(dag=dag, task_id=task_id_2, pool='b')
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)

        tis = ([
            TI(task1, dr1.execution_date),
            TI(task2, dr1.execution_date),
            TI(task1, dr2.execution_date),
            TI(task2, dr2.execution_date)
        ])
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        pool = models.Pool(pool='a', slots=1, description='haha')
        pool2 = models.Pool(pool='b', slots=100, description='haha')
        session.add(pool)
        session.add(pool2)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)
        session.commit()
        self.assertEqual(3, len(res))
        res_keys = []
        for ti in res:
            res_keys.append(ti.key)
        self.assertIn(tis[0].key, res_keys)
        self.assertIn(tis[1].key, res_keys)
        self.assertIn(tis[3].key, res_keys)

    def test_find_executable_task_instances_in_default_pool(self):
        set_default_pool_slots(1)

        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_in_default_pool'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        t1 = DummyOperator(dag=dag, task_id='dummy1')
        t2 = DummyOperator(dag=dag, task_id='dummy2')
        dagbag = self._make_simple_dag_bag([dag])

        executor = TestExecutor(do_update=True)
        scheduler = SchedulerJob(executor=executor)
        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)

        ti1 = TI(task=t1, execution_date=dr1.execution_date)
        ti2 = TI(task=t2, execution_date=dr2.execution_date)
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED

        session = settings.Session()
        session.merge(ti1)
        session.merge(ti2)
        session.commit()

        # Two tasks w/o pool up for execution and our default pool size is 1
        res = scheduler._find_executable_task_instances(
            dagbag,
            states=(State.SCHEDULED,),
            session=session)
        self.assertEqual(1, len(res))

        ti2.state = State.RUNNING
        session.merge(ti2)
        session.commit()

        # One task w/o pool up for execution and one task task running
        res = scheduler._find_executable_task_instances(
            dagbag,
            states=(State.SCHEDULED,),
            session=session)
        self.assertEqual(0, len(res))

        session.close()

    def test_nonexistent_pool(self):
        dag_id = 'SchedulerJobTest.test_nonexistent_pool'
        task_id = 'dummy_wrong_pool'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task = DummyOperator(dag=dag, task_id=task_id, pool="this_pool_doesnt_exist")
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr = scheduler.create_dag_run(dag)

        ti = TI(task, dr.execution_date)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)
        session.commit()
        self.assertEqual(0, len(res))

    def test_find_executable_task_instances_none(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_none'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        DummyOperator(dag=dag, task_id=task_id_1)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        scheduler.create_dag_run(dag)
        session.commit()

        self.assertEqual(0, len(scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)))

    def test_find_executable_task_instances_concurrency(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_concurrency'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=2)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)
        dr3 = scheduler.create_dag_run(dag)

        ti1 = TI(task1, dr1.execution_date)
        ti2 = TI(task1, dr2.execution_date)
        ti3 = TI(task1, dr3.execution_date)
        ti1.state = State.RUNNING
        ti2.state = State.SCHEDULED
        ti3.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(1, len(res))
        res_keys = map(lambda x: x.key, res)
        self.assertIn(ti2.key, res_keys)

        ti2.state = State.RUNNING
        session.merge(ti2)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(0, len(res))

    def test_find_executable_task_instances_concurrency_queued(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_concurrency_queued'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=3)
        task1 = DummyOperator(dag=dag, task_id='dummy1')
        task2 = DummyOperator(dag=dag, task_id='dummy2')
        task3 = DummyOperator(dag=dag, task_id='dummy3')
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()
        dag_run = scheduler.create_dag_run(dag)

        ti1 = TI(task1, dag_run.execution_date)
        ti2 = TI(task2, dag_run.execution_date)
        ti3 = TI(task3, dag_run.execution_date)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        ti3.state = State.SCHEDULED

        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(1, len(res))
        self.assertEqual(res[0].key, ti3.key)

    def test_find_executable_task_instances_task_concurrency(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_task_concurrency'
        task_id_1 = 'dummy'
        task_id_2 = 'dummy2'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1, task_concurrency=2)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)
        dagbag = self._make_simple_dag_bag([dag])

        executor = TestExecutor(do_update=True)
        scheduler = SchedulerJob(executor=executor)
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)
        dr3 = scheduler.create_dag_run(dag)

        ti1_1 = TI(task1, dr1.execution_date)
        ti2 = TI(task2, dr1.execution_date)

        ti1_1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti2)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(2, len(res))

        ti1_1.state = State.RUNNING
        ti2.state = State.RUNNING
        ti1_2 = TI(task1, dr2.execution_date)
        ti1_2.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti2)
        session.merge(ti1_2)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(1, len(res))

        ti1_2.state = State.RUNNING
        ti1_3 = TI(task1, dr3.execution_date)
        ti1_3.state = State.SCHEDULED
        session.merge(ti1_2)
        session.merge(ti1_3)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(0, len(res))

        ti1_1.state = State.SCHEDULED
        ti1_2.state = State.SCHEDULED
        ti1_3.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti1_2)
        session.merge(ti1_3)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(2, len(res))

        ti1_1.state = State.RUNNING
        ti1_2.state = State.SCHEDULED
        ti1_3.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti1_2)
        session.merge(ti1_3)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED],
            session=session)

        self.assertEqual(1, len(res))

        ti1_1.state = State.QUEUED
        ti1_2.state = State.SCHEDULED
        ti1_3.state = State.SUCCESS
        session.merge(ti1_1)
        session.merge(ti1_2)
        session.merge(ti1_3)
        session.commit()

        executor.queued_tasks[ti1_1.key] = ti1_1

        res = scheduler._find_executable_task_instances(
            dagbag,
            states=[State.SCHEDULED, State.QUEUED],
            session=session)

        self.assertEqual(1, len(res))

    def test_change_state_for_executable_task_instances_no_tis(self):
        scheduler = SchedulerJob()
        session = settings.Session()
        res = scheduler._change_state_for_executable_task_instances(
            [], [State.NONE], session)
        self.assertEqual(0, len(res))

    def test_change_state_for_executable_task_instances_no_tis_with_state(self):
        dag_id = 'SchedulerJobTest.test_change_state_for__no_tis_with_state'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=2)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)
        dr3 = scheduler.create_dag_run(dag)

        ti1 = TI(task1, dr1.execution_date)
        ti2 = TI(task1, dr2.execution_date)
        ti3 = TI(task1, dr3.execution_date)
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        ti3.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.commit()

        res = scheduler._change_state_for_executable_task_instances(
            [ti1, ti2, ti3],
            [State.RUNNING],
            session)
        self.assertEqual(0, len(res))

    def test_change_state_for_executable_task_instances_none_state(self):
        dag_id = 'SchedulerJobTest.test_change_state_for__none_state'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=2)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)
        dr3 = scheduler.create_dag_run(dag)

        ti1 = TI(task1, dr1.execution_date)
        ti2 = TI(task1, dr2.execution_date)
        ti3 = TI(task1, dr3.execution_date)
        ti1.state = State.SCHEDULED
        ti2.state = State.QUEUED
        ti3.state = State.NONE
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.commit()

        res = scheduler._change_state_for_executable_task_instances(
            [ti1, ti2, ti3],
            [State.NONE, State.SCHEDULED],
            session)
        self.assertEqual(2, len(res))
        ti1.refresh_from_db()
        ti3.refresh_from_db()
        self.assertEqual(State.QUEUED, ti1.state)
        self.assertEqual(State.QUEUED, ti3.state)

    def test_enqueue_task_instances_with_queued_state(self):
        dag_id = 'SchedulerJobTest.test_enqueue_task_instances_with_queued_state'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)

        ti1 = TI(task1, dr1.execution_date)
        session.merge(ti1)
        session.commit()

        with patch.object(BaseExecutor, 'queue_command') as mock_queue_command:
            scheduler._enqueue_task_instances_with_queued_state(dagbag, [ti1])

        assert mock_queue_command.called

    def test_execute_task_instances_nothing(self):
        dag_id = 'SchedulerJobTest.test_execute_task_instances_nothing'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=2)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dagbag = SimpleDagBag([])

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        ti1 = TI(task1, dr1.execution_date)
        ti1.state = State.SCHEDULED
        session.merge(ti1)
        session.commit()

        self.assertEqual(0, scheduler._execute_task_instances(dagbag, states=[State.SCHEDULED]))

    def test_execute_task_instances(self):
        dag_id = 'SchedulerJobTest.test_execute_task_instances'
        task_id_1 = 'dummy_task'
        task_id_2 = 'dummy_task_nonexistent_queue'
        # important that len(tasks) is less than concurrency
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if concurrency was 3,
        # we could execute arbitrarily many tasks in the second run
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=3)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        # create first dag run with 1 running and 1 queued
        dr1 = scheduler.create_dag_run(dag)
        ti1 = TI(task1, dr1.execution_date)
        ti2 = TI(task2, dr1.execution_date)
        ti1.refresh_from_db()
        ti2.refresh_from_db()
        ti1.state = State.RUNNING
        ti2.state = State.RUNNING
        session.merge(ti1)
        session.merge(ti2)
        session.commit()

        self.assertEqual(State.RUNNING, dr1.state)
        self.assertEqual(
            2,
            DAG.get_num_task_instances(
                dag_id, dag.task_ids, states=[State.RUNNING], session=session
            )
        )

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

        res = scheduler._execute_task_instances(dagbag, [State.SCHEDULED])

        # check that concurrency is respected
        ti1.refresh_from_db()
        ti2.refresh_from_db()
        ti3.refresh_from_db()
        ti4.refresh_from_db()
        self.assertEqual(
            3,
            DAG.get_num_task_instances(
                dag_id, dag.task_ids, states=[State.RUNNING, State.QUEUED], session=session
            )
        )
        self.assertEqual(State.RUNNING, ti1.state)
        self.assertEqual(State.RUNNING, ti2.state)
        six.assertCountEqual(self, [State.QUEUED, State.SCHEDULED], [ti3.state, ti4.state])
        self.assertEqual(1, res)

    def test_execute_task_instances_limit(self):
        dag_id = 'SchedulerJobTest.test_execute_task_instances_limit'
        task_id_1 = 'dummy_task'
        task_id_2 = 'dummy_task_2'
        # important that len(tasks) is less than concurrency
        # because before scheduler._execute_task_instances would only
        # check the num tasks once so if concurrency was 3,
        # we could execute arbitrarily many tasks in the second run
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)
        dagbag = self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        scheduler.max_tis_per_query = 3
        session = settings.Session()

        tis = []
        for _ in range(0, 4):
            dr = scheduler.create_dag_run(dag)
            ti1 = TI(task1, dr.execution_date)
            ti2 = TI(task2, dr.execution_date)
            tis.append(ti1)
            tis.append(ti2)
            ti1.refresh_from_db()
            ti2.refresh_from_db()
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.merge(ti1)
            session.merge(ti2)
            session.commit()
        res = scheduler._execute_task_instances(dagbag, [State.SCHEDULED])

        self.assertEqual(8, res)
        for ti in tis:
            ti.refresh_from_db()
            self.assertEqual(State.QUEUED, ti.state)

    @unittest.skipUnless("INTEGRATION" in os.environ,
                         "The test is flaky with nondeterministic result")
    def test_change_state_for_tis_without_dagrun(self):
        dag1 = DAG(dag_id='test_change_state_for_tis_without_dagrun', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag1, owner='airflow')

        DummyOperator(task_id='dummy_b', dag=dag1, owner='airflow')

        dag2 = DAG(dag_id='test_change_state_for_tis_without_dagrun_dont_change', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag2, owner='airflow')

        dag3 = DAG(dag_id='test_change_state_for_tis_without_dagrun_no_dagrun', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag3, owner='airflow')

        session = settings.Session()
        dr1 = dag1.create_dagrun(run_id=DagRun.ID_PREFIX,
                                 state=State.RUNNING,
                                 execution_date=DEFAULT_DATE,
                                 start_date=DEFAULT_DATE,
                                 session=session)

        dr2 = dag2.create_dagrun(run_id=DagRun.ID_PREFIX,
                                 state=State.RUNNING,
                                 execution_date=DEFAULT_DATE,
                                 start_date=DEFAULT_DATE,
                                 session=session)

        ti1a = dr1.get_task_instance(task_id='dummy', session=session)
        ti1a.state = State.SCHEDULED
        ti1b = dr1.get_task_instance(task_id='dummy_b', session=session)
        ti1b.state = State.SUCCESS
        session.commit()

        ti2 = dr2.get_task_instance(task_id='dummy', session=session)
        ti2.state = State.SCHEDULED
        session.commit()

        ti3 = TI(dag3.get_task('dummy'), DEFAULT_DATE)
        ti3.state = State.SCHEDULED
        session.merge(ti3)
        session.commit()

        dagbag = self._make_simple_dag_bag([dag1, dag2, dag3])
        scheduler = SchedulerJob(num_runs=0)
        scheduler._change_state_for_tis_without_dagrun(
            simple_dag_bag=dagbag,
            old_states=[State.SCHEDULED, State.QUEUED],
            new_state=State.NONE,
            session=session)

        ti1a = dr1.get_task_instance(task_id='dummy', session=session)
        ti1a.refresh_from_db(session=session)
        self.assertEqual(ti1a.state, State.SCHEDULED)

        ti1b = dr1.get_task_instance(task_id='dummy_b', session=session)
        ti1b.refresh_from_db(session=session)
        self.assertEqual(ti1b.state, State.SUCCESS)

        ti2 = dr2.get_task_instance(task_id='dummy', session=session)
        ti2.refresh_from_db(session=session)
        self.assertEqual(ti2.state, State.SCHEDULED)

        ti3.refresh_from_db(session=session)
        self.assertEqual(ti3.state, State.NONE)

        dr1.refresh_from_db(session=session)
        dr1.state = State.FAILED

        # why o why
        session.merge(dr1)
        session.commit()

        scheduler._change_state_for_tis_without_dagrun(
            simple_dag_bag=dagbag,
            old_states=[State.SCHEDULED, State.QUEUED],
            new_state=State.NONE,
            session=session)
        ti1a.refresh_from_db(session=session)
        self.assertEqual(ti1a.state, State.SCHEDULED)

        # don't touch ti1b
        ti1b.refresh_from_db(session=session)
        self.assertEqual(ti1b.state, State.SUCCESS)

        # don't touch ti2
        ti2.refresh_from_db(session=session)
        self.assertEqual(ti2.state, State.SCHEDULED)

    def test_change_state_for_tasks_failed_to_execute(self):
        dag = DAG(
            dag_id='dag_id',
            start_date=DEFAULT_DATE)

        task = DummyOperator(
            task_id='task_id',
            dag=dag,
            owner='airflow')

        # If there's no left over task in executor.queued_tasks, nothing happens
        session = settings.Session()
        scheduler_job = SchedulerJob()
        mock_logger = mock.MagicMock()
        test_executor = TestExecutor(do_update=False)
        scheduler_job.executor = test_executor
        scheduler_job._logger = mock_logger
        scheduler_job._change_state_for_tasks_failed_to_execute()
        mock_logger.info.assert_not_called()

        # Tasks failed to execute with QUEUED state will be set to SCHEDULED state.
        session.query(TI).delete()
        session.commit()
        key = 'dag_id', 'task_id', DEFAULT_DATE, 1
        test_executor.queued_tasks[key] = 'value'
        ti = TI(task, DEFAULT_DATE)
        ti.state = State.QUEUED
        session.merge(ti)
        session.commit()

        scheduler_job._change_state_for_tasks_failed_to_execute()

        ti.refresh_from_db()
        self.assertEqual(State.SCHEDULED, ti.state)

        # Tasks failed to execute with RUNNING state will not be set to SCHEDULED state.
        session.query(TI).delete()
        session.commit()
        ti.state = State.RUNNING

        session.merge(ti)
        session.commit()

        scheduler_job._change_state_for_tasks_failed_to_execute()

        ti.refresh_from_db()
        self.assertEqual(State.RUNNING, ti.state)

    def test_execute_helper_reset_orphaned_tasks(self):
        session = settings.Session()
        dag = DAG(
            'test_execute_helper_reset_orphaned_tasks',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='op1')

        dag.clear()
        dr = dag.create_dagrun(run_id=DagRun.ID_PREFIX,
                               state=State.RUNNING,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)
        dr2 = dag.create_dagrun(run_id=BackfillJob.ID_PREFIX,
                                state=State.RUNNING,
                                execution_date=DEFAULT_DATE + datetime.timedelta(1),
                                start_date=DEFAULT_DATE,
                                session=session)
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.SCHEDULED
        ti2 = dr2.get_task_instance(task_id=op1.task_id, session=session)
        ti2.state = State.SCHEDULED
        session.commit()

        processor = mock.MagicMock()

        scheduler = SchedulerJob(num_runs=0)
        executor = TestExecutor(do_update=False)
        scheduler.executor = executor
        scheduler.processor_agent = processor

        scheduler._execute_helper()

        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        self.assertEqual(ti.state, State.NONE)

        ti2 = dr2.get_task_instance(task_id=op1.task_id, session=session)
        self.assertEqual(ti2.state, State.SCHEDULED)

    @parameterized.expand([
        [State.UP_FOR_RETRY, State.FAILED],
        [State.QUEUED, State.NONE],
        [State.SCHEDULED, State.NONE],
        [State.UP_FOR_RESCHEDULE, State.NONE],
    ])
    def test_execute_helper_should_change_state_for_tis_without_dagrun(
            self, initial_task_state, expected_task_state):
        session = settings.Session()
        dag = DAG(
            'test_execute_helper_should_change_state_for_tis_without_dagrun',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='op1')

        # Create DAG run with FAILED state
        dag.clear()
        dr = dag.create_dagrun(run_id=DagRun.ID_PREFIX,
                               state=State.FAILED,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = initial_task_state
        session.commit()

        # Create scheduler and mock calls to processor. Run duration is set
        # to a high value to ensure loop is entered. Poll interval is 0 to
        # avoid sleep. Done flag is set to true to exist the loop immediately.
        scheduler = SchedulerJob(num_runs=0, processor_poll_interval=0)
        executor = TestExecutor(do_update=False)
        executor.queued_tasks
        scheduler.executor = executor
        processor = mock.MagicMock()
        processor.harvest_simple_dags.return_value = [dag]
        processor.done = True
        scheduler.processor_agent = processor

        scheduler._execute_helper()

        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        self.assertEqual(ti.state, expected_task_state)

    @provide_session
    def evaluate_dagrun(
            self,
            dag_id,
            expected_task_states,  # dict of task_id: state
            dagrun_state,
            run_kwargs=None,
            advance_execution_date=False,
            session=None):  # pylint: disable=unused-argument
        """
        Helper for testing DagRun states with simple two-task DAGS.
        This is hackish: a dag run is created but its tasks are
        run by a backfill.
        """
        if run_kwargs is None:
            run_kwargs = {}

        scheduler = SchedulerJob()
        dag = self.dagbag.get_dag(dag_id)
        dr = scheduler.create_dag_run(dag)

        if advance_execution_date:
            # run a second time to schedule a dagrun after the start_date
            dr = scheduler.create_dag_run(dag)
        ex_date = dr.execution_date

        for tid, state in expected_task_states.items():
            if state != State.FAILED:
                continue
            self.null_exec.mock_task_fail(dag_id, tid, ex_date)

        try:
            dag.run(start_date=ex_date, end_date=ex_date, executor=self.null_exec, **run_kwargs)
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
        # TODO: this should live in test_dagrun.py
        # Run both the failed and successful tasks
        scheduler = SchedulerJob()
        dag_id = 'test_dagrun_states_root_fail_unfinished'
        dag = self.dagbag.get_dag(dag_id)
        dr = scheduler.create_dag_run(dag)
        self.null_exec.mock_task_fail(dag_id, 'test_dagrun_fail', DEFAULT_DATE)

        with self.assertRaises(AirflowException):
            dag.run(start_date=dr.execution_date, end_date=dr.execution_date, executor=self.null_exec)

        # Mark the successful task as never having run since we want to see if the
        # dagrun will be in a running state despite haveing an unfinished task.
        with create_session() as session:
            ti = dr.get_task_instance('test_dagrun_unfinished', session=session)
            ti.state = State.NONE
            session.commit()
        dr_state = dr.update_state()
        self.assertEqual(dr_state, State.RUNNING)

    def test_dagrun_root_after_dagrun_unfinished(self):
        """
        DagRuns with one successful and one future root task -> SUCCESS

        Noted: the DagRun state could be still in running state during CI.
        """
        dag_id = 'test_dagrun_states_root_future'
        dag = self.dagbag.get_dag(dag_id)
        scheduler = SchedulerJob(
            dag_id,
            num_runs=1,
            executor=self.null_exec,
            subdir=dag.fileloc)
        scheduler.run()

        first_run = DagRun.find(dag_id=dag_id, execution_date=DEFAULT_DATE)[0]
        ti_ids = [(ti.task_id, ti.state) for ti in first_run.get_task_instances()]

        self.assertEqual(ti_ids, [('current', State.SUCCESS)])
        self.assertIn(first_run.state, [State.SUCCESS, State.RUNNING])

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
        with create_session() as session:
            dag_id = 'test_start_date_scheduling'
            dag = self.dagbag.get_dag(dag_id)
            dag.clear()
            self.assertTrue(dag.start_date > datetime.datetime.utcnow())

            scheduler = SchedulerJob(dag_id,
                                     executor=self.null_exec,
                                     subdir=dag.fileloc,
                                     num_runs=1)
            scheduler.run()

            # zero tasks ran
            self.assertEqual(
                len(session.query(TI).filter(TI.dag_id == dag_id).all()), 0)
            session.commit()
            self.assertListEqual([], self.null_exec.sorted_tasks)

            # previously, running this backfill would kick off the Scheduler
            # because it would take the most recent run and start from there
            # That behavior still exists, but now it will only do so if after the
            # start date
            bf_exec = TestExecutor()
            backfill = BackfillJob(
                executor=bf_exec,
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
            backfill.run()

            # one task ran
            self.assertEqual(
                len(session.query(TI).filter(TI.dag_id == dag_id).all()), 1)
            self.assertListEqual(
                [
                    ((dag.dag_id, 'dummy', DEFAULT_DATE, 1), State.SUCCESS),
                ],
                bf_exec.sorted_tasks
            )
            session.commit()

            scheduler = SchedulerJob(dag_id,
                                     executor=self.null_exec,
                                     subdir=dag.fileloc,
                                     num_runs=1)
            scheduler.run()

            # still one task
            self.assertEqual(
                len(session.query(TI).filter(TI.dag_id == dag_id).all()), 1)
            session.commit()
            self.assertListEqual([], self.null_exec.sorted_tasks)

    def test_scheduler_task_start_date(self):
        """
        Test that the scheduler respects task start dates that are different from DAG start dates
        """

        dag_id = 'test_task_start_date_scheduling'
        dag = self.dagbag.get_dag(dag_id)
        dag.clear()
        scheduler = SchedulerJob(dag_id,
                                 executor=self.null_exec,
                                 subdir=os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py'),
                                 num_runs=2)
        scheduler.run()

        session = settings.Session()
        tiq = session.query(TI).filter(TI.dag_id == dag_id)
        ti1s = tiq.filter(TI.task_id == 'dummy1').all()
        ti2s = tiq.filter(TI.task_id == 'dummy2').all()
        self.assertEqual(len(ti1s), 0)
        self.assertEqual(len(ti2s), 2)
        for t in ti2s:
            self.assertEqual(t.state, State.SUCCESS)

    def test_scheduler_multiprocessing(self):
        """
        Test that the scheduler can successfully queue multiple dags in parallel
        """
        dag_ids = ['test_start_date_scheduling', 'test_dagrun_states_success']
        for dag_id in dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            dag.clear()

        scheduler = SchedulerJob(dag_ids=dag_ids,
                                 executor=self.null_exec,
                                 subdir=os.path.join(TEST_DAG_FOLDER, 'test_scheduler_dags.py'),
                                 num_runs=1)
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
            start_date=timezone.datetime(2015, 1, 1),
            schedule_interval="@once")

        scheduler = SchedulerJob()
        dag.clear()
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        dr = scheduler.create_dag_run(dag)
        self.assertIsNone(dr)

    @parameterized.expand([
        [State.NONE, None, None],
        [State.UP_FOR_RETRY, timezone.utcnow() - datetime.timedelta(minutes=30),
            timezone.utcnow() - datetime.timedelta(minutes=15)],
        [State.UP_FOR_RESCHEDULE, timezone.utcnow() - datetime.timedelta(minutes=30),
            timezone.utcnow() - datetime.timedelta(minutes=15)],
    ])
    def test_scheduler_process_task_instances(self, state, start_date, end_date):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        dag = DAG(
            dag_id='test_scheduler_process_execute_task',
            start_date=DEFAULT_DATE)
        dag_task1 = DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        scheduler = SchedulerJob()
        dag.clear()
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)

        with create_session() as session:
            tis = dr.get_task_instances(session=session)
            for ti in tis:
                ti.state = state
                ti.start_date = start_date
                ti.end_date = end_date

        mock_list = Mock()
        scheduler._process_task_instances(dag, task_instances_list=mock_list)

        mock_list.append.assert_called_with(
            (dag.dag_id, dag_task1.task_id, DEFAULT_DATE, TRY_NUMBER)
        )

    def test_scheduler_do_not_schedule_removed_task(self):
        dag = DAG(
            dag_id='test_scheduler_do_not_schedule_removed_task',
            start_date=DEFAULT_DATE)
        DummyOperator(
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

        mock_list = Mock()
        scheduler._process_task_instances(dag, task_instances_list=mock_list)

        mock_list.put.assert_not_called()

    def test_scheduler_do_not_schedule_too_early(self):
        dag = DAG(
            dag_id='test_scheduler_do_not_schedule_too_early',
            start_date=timezone.datetime(2200, 1, 1))
        DummyOperator(
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

        mock_list = Mock()
        scheduler._process_task_instances(dag, task_instances_list=mock_list)

        mock_list.put.assert_not_called()

    def test_scheduler_do_not_schedule_without_tasks(self):
        dag = DAG(
            dag_id='test_scheduler_do_not_schedule_without_tasks',
            start_date=DEFAULT_DATE)

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)
            scheduler = SchedulerJob()
            dag.clear(session=session)
            dag.start_date = None
            dr = scheduler.create_dag_run(dag, session=session)
            self.assertIsNone(dr)

    def test_scheduler_do_not_run_finished(self):
        dag = DAG(
            dag_id='test_scheduler_do_not_run_finished',
            start_date=DEFAULT_DATE)
        DummyOperator(
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

        mock_list = Mock()
        scheduler._process_task_instances(dag, task_instances_list=mock_list)

        mock_list.put.assert_not_called()

    def test_scheduler_add_new_task(self):
        """
        Test if a task instance will be added if the dag is updated
        """
        dag = DAG(
            dag_id='test_scheduler_add_new_task',
            start_date=DEFAULT_DATE)

        DummyOperator(
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
        self.assertEqual(len(tis), 1)

        DummyOperator(
            task_id='dummy2',
            dag=dag,
            owner='airflow')

        task_instances_list = Mock()
        scheduler._process_task_instances(dag, task_instances_list=task_instances_list)

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)

    def test_scheduler_verify_max_active_runs(self):
        """
        Test if a a dagrun will not be scheduled if max_dag_runs has been reached
        """
        dag = DAG(
            dag_id='test_scheduler_verify_max_active_runs',
            start_date=DEFAULT_DATE)
        dag.max_active_runs = 1

        DummyOperator(
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

        DummyOperator(
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
        dr.start_date = timezone.utcnow() - datetime.timedelta(days=1)
        session.merge(dr)
        session.commit()

        dr2 = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr2)

        dr.refresh_from_db(session=session)
        self.assertEqual(dr.state, State.FAILED)

    def test_scheduler_verify_max_active_runs_and_dagrun_timeout(self):
        """
        Test if a a dagrun will not be scheduled if max_dag_runs
        has been reached and dagrun_timeout is not reached

        Test if a a dagrun will be scheduled if max_dag_runs has
        been reached but dagrun_timeout is also reached
        """
        dag = DAG(
            dag_id='test_scheduler_verify_max_active_runs_and_dagrun_timeout',
            start_date=DEFAULT_DATE)
        dag.max_active_runs = 1
        dag.dagrun_timeout = datetime.timedelta(seconds=60)

        DummyOperator(
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
        dr.start_date = timezone.utcnow() - datetime.timedelta(days=1)
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

        task_instances_list = Mock()
        # and schedule them in, so we can check how many
        # tasks are put on the task_instances_list (should be one, not 3)
        scheduler._process_task_instances(dag, task_instances_list=task_instances_list)

        task_instances_list.append.assert_called_with(
            (dag.dag_id, dag_task1.task_id, DEFAULT_DATE, TRY_NUMBER)
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

        scheduler = SchedulerJob(executor=self.null_exec)

        # Create 2 dagruns, which will create 2 task instances.
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEqual(dr.execution_date, DEFAULT_DATE)
        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        task_instances_list = []
        scheduler._process_task_instances(dag, task_instances_list=task_instances_list)
        self.assertEqual(len(task_instances_list), 2)
        dagbag = self._make_simple_dag_bag([dag])

        # Recreated part of the scheduler here, to kick off tasks -> executor
        for ti_key in task_instances_list:
            task = dag.get_task(ti_key[1])
            ti = TI(task, ti_key[2])
            # Task starts out in the scheduled state. All tasks in the
            # scheduled state will be sent to the executor
            ti.state = State.SCHEDULED

            # Also save this task instance to the DB.
            session.merge(ti)
        session.commit()

        self.assertEqual(len(scheduler.executor.queued_tasks), 0, "Check test pre-condition")
        scheduler._execute_task_instances(dagbag,
                                          (State.SCHEDULED, State.UP_FOR_RETRY),
                                          session=session)

        self.assertEqual(len(scheduler.executor.queued_tasks), 1)

    def test_scheduler_auto_align(self):
        """
        Test if the schedule_interval will be auto aligned with the start_date
        such that if the start_date coincides with the schedule the first
        execution_date will be start_date, otherwise it will be start_date +
        interval.
        """
        dag = DAG(
            dag_id='test_scheduler_auto_align_1',
            start_date=timezone.datetime(2016, 1, 1, 10, 10, 0),
            schedule_interval="4 5 * * *"
        )
        DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()

        scheduler = SchedulerJob()

        dr = scheduler.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEqual(dr.execution_date, timezone.datetime(2016, 1, 2, 5, 4))

        dag = DAG(
            dag_id='test_scheduler_auto_align_2',
            start_date=timezone.datetime(2016, 1, 1, 10, 10, 0),
            schedule_interval="10 10 * * *"
        )
        DummyOperator(
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
        self.assertEqual(dr.execution_date, timezone.datetime(2016, 1, 1, 10, 10))

    def test_scheduler_reschedule(self):
        """
        Checks if tasks that are not taken up by the executor
        get rescheduled
        """
        executor = TestExecutor(do_update=False)

        dagbag = DagBag(executor=executor)
        dagbag.dags.clear()
        dagbag.executor = executor

        dag = DAG(
            dag_id='test_scheduler_reschedule',
            start_date=DEFAULT_DATE)
        DummyOperator(
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
        def do_schedule(mock_dagbag, mock_collect_dags):
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
        self.assertEqual(1, len(executor.queued_tasks))
        executor.queued_tasks.clear()

        do_schedule()
        self.assertEqual(2, len(executor.queued_tasks))

    def test_scheduler_sla_miss_callback(self):
        """
        Test that the scheduler calls the sla miss callback
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        test_start_date = days_ago(1)
        dag = DAG(dag_id='test_sla_miss',
                  sla_miss_callback=sla_callback,
                  default_args={'start_date': test_start_date,
                                'sla': datetime.timedelta()})

        task = DummyOperator(task_id='dummy',
                             dag=dag,
                             owner='airflow')

        session.merge(models.TaskInstance(task=task,
                                          execution_date=test_start_date,
                                          state='success'))

        session.merge(SlaMiss(task_id='dummy',
                              dag_id='test_sla_miss',
                              execution_date=test_start_date))

        scheduler = SchedulerJob(dag_id='test_sla_miss',
                                 num_runs=1)
        scheduler.manage_slas(dag=dag, session=session)

        assert sla_callback.called

    def test_scheduler_sla_miss_callback_invalid_sla(self):
        """
        Test that the scheduler does not call the sla miss callback when
        given an invalid sla
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        # Pass anything besides a timedelta object to the sla argument.
        test_start_date = days_ago(1)
        dag = DAG(dag_id='test_sla_miss',
                  sla_miss_callback=sla_callback,
                  default_args={'start_date': test_start_date,
                                'sla': None})

        task = DummyOperator(task_id='dummy',
                             dag=dag,
                             owner='airflow')

        session.merge(models.TaskInstance(task=task,
                                          execution_date=test_start_date,
                                          state='success'))

        session.merge(SlaMiss(task_id='dummy',
                              dag_id='test_sla_miss',
                              execution_date=test_start_date))

        scheduler = SchedulerJob(dag_id='test_sla_miss',
                                 num_runs=1)
        scheduler.manage_slas(dag=dag, session=session)

        sla_callback.assert_not_called()

    def test_scheduler_sla_miss_callback_sent_notification(self):
        """
        Test that the scheduler does not call the sla_miss_callback when a notification has already been sent
        """
        session = settings.Session()

        # Mock the callback function so we can verify that it was not called
        sla_callback = MagicMock()

        # Create dag with a start of 2 days ago, but an sla of 1 day
        # ago so we'll already have an sla_miss on the books
        test_start_date = days_ago(2)
        dag = DAG(dag_id='test_sla_miss',
                  sla_miss_callback=sla_callback,
                  default_args={'start_date': test_start_date,
                                'sla': datetime.timedelta(days=1)})

        task = DummyOperator(task_id='dummy',
                             dag=dag,
                             owner='airflow')

        # Create a TaskInstance for two days ago
        session.merge(models.TaskInstance(task=task,
                                          execution_date=test_start_date,
                                          state='success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy',
                              dag_id='test_sla_miss',
                              execution_date=test_start_date,
                              email_sent=False,
                              notification_sent=True))

        # Now call manage_slas and see if the sla_miss callback gets called
        scheduler = SchedulerJob(dag_id='test_sla_miss',
                                 num_runs=1)
        scheduler.manage_slas(dag=dag, session=session)

        sla_callback.assert_not_called()

    def test_scheduler_sla_miss_callback_exception(self):
        """
        Test that the scheduler gracefully logs an exception if there is a problem
         calling the sla_miss_callback
        """
        session = settings.Session()

        sla_callback = MagicMock(side_effect=RuntimeError('Could not call function'))

        test_start_date = days_ago(2)
        dag = DAG(dag_id='test_sla_miss',
                  sla_miss_callback=sla_callback,
                  default_args={'start_date': test_start_date})

        task = DummyOperator(task_id='dummy',
                             dag=dag,
                             owner='airflow',
                             sla=datetime.timedelta(hours=1))

        session.merge(models.TaskInstance(task=task,
                                          execution_date=test_start_date,
                                          state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy',
                              dag_id='test_sla_miss',
                              execution_date=test_start_date))

        # Now call manage_slas and see if the sla_miss callback gets called
        scheduler = SchedulerJob(dag_id='test_sla_miss')

        with mock.patch('airflow.jobs.SchedulerJob.log',
                        new_callable=PropertyMock) as mock_log:
            scheduler.manage_slas(dag=dag, session=session)
            assert sla_callback.called
            mock_log().exception.assert_called_with(
                'Could not call sla_miss_callback for DAG %s',
                'test_sla_miss')

    @mock.patch("airflow.utils.email.send_email")
    def test_scheduler_sla_miss_email_exception(self, mock_send_email):
        """
        Test that the scheduler gracefully logs an exception if there is a problem
          sending an email
        """
        session = settings.Session()

        # Mock the callback function so we can verify that it was not called
        mock_send_email.side_effect = RuntimeError('Could not send an email')

        test_start_date = days_ago(2)
        dag = DAG(dag_id='test_sla_miss',
                  default_args={'start_date': test_start_date,
                                'sla': datetime.timedelta(days=1)})

        task = DummyOperator(task_id='dummy',
                             dag=dag,
                             owner='airflow',
                             email='test@test.com',
                             sla=datetime.timedelta(hours=1))

        session.merge(models.TaskInstance(task=task,
                                          execution_date=test_start_date,
                                          state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy',
                              dag_id='test_sla_miss',
                              execution_date=test_start_date))

        scheduler = SchedulerJob(dag_id='test_sla_miss',
                                 num_runs=1)

        with mock.patch('airflow.jobs.SchedulerJob.log',
                        new_callable=PropertyMock) as mock_log:
            scheduler.manage_slas(dag=dag, session=session)
            mock_log().exception.assert_called_with(
                'Could not send SLA Miss email notification for DAG %s',
                'test_sla_miss')

    def test_retry_still_in_executor(self):
        """
        Checks if the scheduler does not put a task in limbo, when a task is retried
        but is still present in the executor.
        """
        executor = TestExecutor(do_update=False)
        dagbag = DagBag(executor=executor, dag_folder=os.path.join(settings.DAGS_FOLDER,
                                                                   "no_dags.py"))
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
        def do_schedule(mock_dagbag, mock_collect_dags):
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
        self.assertEqual(1, len(executor.queued_tasks))

        def run_with_error(task, ignore_ti_state=False):
            try:
                task.run(ignore_ti_state=ignore_ti_state)
            except AirflowException:
                pass

        ti_tuple = next(iter(executor.queued_tasks.values()))
        (_, _, _, simple_ti) = ti_tuple
        ti = simple_ti.construct_task_instance()
        ti.task = dag_task1

        self.assertEqual(ti.try_number, 1)
        # At this point, scheduler has tried to schedule the task once and
        # heartbeated the executor once, which moved the state of the task from
        # SCHEDULED to QUEUED and then to SCHEDULED, to fail the task execution
        # we need to ignore the TI state as SCHEDULED is not a valid state to start
        # executing task.
        run_with_error(ti, ignore_ti_state=True)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 2)

        ti.refresh_from_db(lock_for_update=True, session=session)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        # do not schedule
        do_schedule()
        self.assertTrue(executor.has_task(ti))
        ti.refresh_from_db()
        # removing self.assertEqual(ti.state, State.SCHEDULED)
        # as scheduler will move state from SCHEDULED to QUEUED

        # now the executor has cleared and it should be allowed the re-queue,
        # but tasks stay in the executor.queued_tasks after executor.heartbeat()
        # will be set back to SCHEDULED state
        executor.queued_tasks.clear()
        do_schedule()
        ti.refresh_from_db()

        self.assertEqual(ti.state, State.SCHEDULED)

        # To verify that task does get re-queued.
        executor.queued_tasks.clear()
        executor.do_update = True
        do_schedule()
        ti.refresh_from_db()
        self.assertIn(ti.state, [State.RUNNING, State.SUCCESS])

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
        ti = session.query(TI).filter(TI.dag_id == dag.dag_id,
                                      TI.task_id == dag_task1.task_id).first()

        # make sure the counter has increased
        self.assertEqual(ti.try_number, 2)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)

    def test_dag_with_system_exit(self):
        """
        Test to check that a DAG with a system.exit() doesn't break the scheduler.
        """

        dag_id = 'exit_test_dag'
        dag_ids = [dag_id]
        dag_directory = os.path.join(settings.DAGS_FOLDER, "..", "dags_with_system_exit")
        dag_file = os.path.join(dag_directory,
                                'b_test_scheduler_dags.py')

        dagbag = DagBag(dag_folder=dag_file)
        for dag_id in dag_ids:
            dag = dagbag.get_dag(dag_id)
            dag.clear()

        scheduler = SchedulerJob(dag_ids=dag_ids,
                                 executor=self.null_exec,
                                 subdir=dag_directory,
                                 num_runs=1)
        scheduler.run()
        with create_session() as session:
            self.assertEqual(
                len(session.query(TI).filter(TI.dag_id == dag_id).all()), 1)

    def test_dag_get_active_runs(self):
        """
        Test to check that a DAG returns its active runs
        """

        now = timezone.utcnow()
        six_hours_ago_to_the_hour = \
            (now - datetime.timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)

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
        except Exception:
            running_date = 'Except'

        self.assertEqual(execution_date, running_date, 'Running Date must match Execution Date')

    def test_dag_catchup_option(self):
        """
        Test to check that a DAG with catchup = False only schedules beginning now, not back to the start date
        """

        def setup_dag(dag_id, schedule_interval, start_date, catchup):
            default_args = {
                'owner': 'airflow',
                'depends_on_past': False,
                'start_date': start_date
            }
            dag = DAG(dag_id,
                      schedule_interval=schedule_interval,
                      max_active_runs=1,
                      catchup=catchup,
                      default_args=default_args)

            t1 = DummyOperator(task_id='t1', dag=dag)
            t2 = DummyOperator(task_id='t2', dag=dag)
            t2.set_upstream(t1)
            t3 = DummyOperator(task_id='t3', dag=dag)
            t3.set_upstream(t2)

            session = settings.Session()
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)
            session.commit()
            session.close()

            return dag

        now = timezone.utcnow()
        six_hours_ago_to_the_hour = (now - datetime.timedelta(hours=6)).replace(
            minute=0, second=0, microsecond=0)
        half_an_hour_ago = now - datetime.timedelta(minutes=30)
        two_hours_ago = now - datetime.timedelta(hours=2)

        scheduler = SchedulerJob()

        dag1 = setup_dag(dag_id='dag_with_catchup',
                         schedule_interval='* * * * *',
                         start_date=six_hours_ago_to_the_hour,
                         catchup=True)
        default_catchup = configuration.conf.getboolean('scheduler', 'catchup_by_default')
        self.assertEqual(default_catchup, True)
        self.assertEqual(dag1.catchup, True)

        dag2 = setup_dag(dag_id='dag_without_catchup_ten_minute',
                         schedule_interval='*/10 * * * *',
                         start_date=six_hours_ago_to_the_hour,
                         catchup=False)
        dr = scheduler.create_dag_run(dag2)
        # We had better get a dag run
        self.assertIsNotNone(dr)
        # The DR should be scheduled in the last half an hour, not 6 hours ago
        self.assertGreater(dr.execution_date, half_an_hour_ago)
        # The DR should be scheduled BEFORE now
        self.assertLess(dr.execution_date, timezone.utcnow())

        dag3 = setup_dag(dag_id='dag_without_catchup_hourly',
                         schedule_interval='@hourly',
                         start_date=six_hours_ago_to_the_hour,
                         catchup=False)
        dr = scheduler.create_dag_run(dag3)
        # We had better get a dag run
        self.assertIsNotNone(dr)
        # The DR should be scheduled in the last 2 hours, not 6 hours ago
        self.assertGreater(dr.execution_date, two_hours_ago)
        # The DR should be scheduled BEFORE now
        self.assertLess(dr.execution_date, timezone.utcnow())

        dag4 = setup_dag(dag_id='dag_without_catchup_once',
                         schedule_interval='@once',
                         start_date=six_hours_ago_to_the_hour,
                         catchup=False)
        dr = scheduler.create_dag_run(dag4)
        self.assertIsNotNone(dr)

    def test_add_unparseable_file_before_sched_start_creates_import_error(self):
        dags_folder = mkdtemp()
        try:
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)
            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        self.assertEqual(len(import_errors), 1)
        import_error = import_errors[0]
        self.assertEqual(import_error.filename,
                         unparseable_filename)
        self.assertEqual(import_error.stacktrace,
                         "invalid syntax ({}, line 1)".format(TEMP_DAG_FILENAME))

    def test_add_unparseable_file_after_sched_start_creates_import_error(self):
        dags_folder = mkdtemp()
        try:
            unparseable_filename = os.path.join(dags_folder, TEMP_DAG_FILENAME)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)

            with open(unparseable_filename, 'w') as unparseable_file:
                unparseable_file.writelines(UNPARSEABLE_DAG_FILE_CONTENTS)
            self.run_single_scheduler_loop_with_no_dags(dags_folder)
        finally:
            shutil.rmtree(dags_folder)

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

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

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

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
        import_errors = session.query(errors.ImportError).all()

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
        import_errors = session.query(errors.ImportError).all()

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

        with create_session() as session:
            import_errors = session.query(errors.ImportError).all()

        self.assertEqual(len(import_errors), 0)

    def test_list_py_file_paths(self):
        """
        [JIRA-1357] Test the 'list_py_file_paths' function used by the
        scheduler to list and load DAGs.
        """
        detected_files = set()
        expected_files = set()
        # No_dags is empty, _invalid_ is ignored by .airflowignore
        ignored_files = [
            'no_dags.py',
            'test_invalid_cron.py',
            'test_zip_invalid_cron.zip',
        ]
        for file_name in os.listdir(TEST_DAG_FOLDER):
            if file_name.endswith('.py') or file_name.endswith('.zip'):
                if file_name not in ignored_files:
                    expected_files.add(
                        '{}/{}'.format(TEST_DAG_FOLDER, file_name))
        for file_path in list_py_file_paths(TEST_DAG_FOLDER, include_examples=False):
            detected_files.add(file_path)
        self.assertEqual(detected_files, expected_files)

        example_dag_folder = airflow.example_dags.__path__[0]
        for root, _, files in os.walk(example_dag_folder):
            for file_name in files:
                if file_name.endswith('.py') or file_name.endswith('.zip'):
                    if file_name not in ['__init__.py']:
                        expected_files.add(os.path.join(root, file_name))
        detected_files.clear()
        for file_path in list_py_file_paths(TEST_DAG_FOLDER, include_examples=True):
            detected_files.add(file_path)
        self.assertEqual(detected_files, expected_files)

    def test_reset_orphaned_tasks_nothing(self):
        """Try with nothing. """
        scheduler = SchedulerJob()
        session = settings.Session()
        self.assertEqual(
            0, len(scheduler.reset_state_for_orphaned_tasks(session=session)))

    def test_reset_orphaned_tasks_external_triggered_dag(self):
        dag_id = 'test_reset_orphaned_tasks_external_triggered_dag'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag, session=session)
        ti = dr1.get_task_instances(session=session)[0]
        dr1.state = State.RUNNING
        ti.state = State.SCHEDULED
        dr1.external_trigger = True
        session.merge(ti)
        session.merge(dr1)
        session.commit()

        reset_tis = scheduler.reset_state_for_orphaned_tasks(session=session)
        self.assertEqual(1, len(reset_tis))

    def test_reset_orphaned_tasks_backfill_dag(self):
        dag_id = 'test_reset_orphaned_tasks_backfill_dag'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag, session=session)
        ti = dr1.get_task_instances(session=session)[0]
        ti.state = State.SCHEDULED
        dr1.state = State.RUNNING
        dr1.run_id = BackfillJob.ID_PREFIX + '_sdfsfdfsd'
        session.merge(ti)
        session.merge(dr1)
        session.commit()

        self.assertTrue(dr1.is_backfill)
        self.assertEqual(0, len(scheduler.reset_state_for_orphaned_tasks(session=session)))

    def test_reset_orphaned_tasks_specified_dagrun(self):
        """Try to reset when we specify a dagrun and ensure nothing else is."""
        dag_id = 'test_reset_orphaned_tasks_specified_dagrun'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        scheduler = SchedulerJob()
        session = settings.Session()
        # make two dagruns, only reset for one
        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)
        dr1.state = State.SUCCESS
        dr2.state = State.RUNNING
        ti1 = dr1.get_task_instances(session=session)[0]
        ti2 = dr2.get_task_instances(session=session)[0]
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED

        session.merge(ti1)
        session.merge(ti2)
        session.merge(dr1)
        session.merge(dr2)
        session.commit()

        reset_tis = scheduler.reset_state_for_orphaned_tasks(filter_by_dag_run=dr2, session=session)
        self.assertEqual(1, len(reset_tis))
        ti1.refresh_from_db(session=session)
        ti2.refresh_from_db(session=session)
        self.assertEqual(State.SCHEDULED, ti1.state)
        self.assertEqual(State.NONE, ti2.state)

    def test_reset_orphaned_tasks_nonexistent_dagrun(self):
        """Make sure a task in an orphaned state is not reset if it has no dagrun. """
        dag_id = 'test_reset_orphaned_tasks_nonexistent_dagrun'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        task = DummyOperator(task_id=task_id, dag=dag)

        scheduler = SchedulerJob()
        session = settings.Session()

        ti = models.TaskInstance(task=task, execution_date=DEFAULT_DATE)
        session.add(ti)
        session.commit()

        ti.refresh_from_db()
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        self.assertEqual(0, len(scheduler.reset_state_for_orphaned_tasks(session=session)))

    def test_reset_orphaned_tasks_no_orphans(self):
        dag_id = 'test_reset_orphaned_tasks_no_orphans'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr1.state = State.RUNNING
        tis = dr1.get_task_instances(session=session)
        tis[0].state = State.RUNNING
        session.merge(dr1)
        session.merge(tis[0])
        session.commit()

        self.assertEqual(0, len(scheduler.reset_state_for_orphaned_tasks(session=session)))
        tis[0].refresh_from_db()
        self.assertEqual(State.RUNNING, tis[0].state)

    def test_reset_orphaned_tasks_non_running_dagruns(self):
        """Ensure orphaned tasks with non-running dagruns are not reset."""
        dag_id = 'test_reset_orphaned_tasks_non_running_dagruns'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, schedule_interval='@daily')
        task_id = dag_id + '_task'
        DummyOperator(task_id=task_id, dag=dag)

        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = scheduler.create_dag_run(dag)
        dr1.state = State.SUCCESS
        tis = dr1.get_task_instances(session=session)
        self.assertEqual(1, len(tis))
        tis[0].state = State.SCHEDULED
        session.merge(dr1)
        session.merge(tis[0])
        session.commit()

        self.assertEqual(0, len(scheduler.reset_state_for_orphaned_tasks(session=session)))

    def test_reset_orphaned_tasks_with_orphans(self):
        """Create dagruns and esnure only ones with correct states are reset."""
        prefix = 'scheduler_job_test_test_reset_orphaned_tasks'
        states = [State.QUEUED, State.SCHEDULED, State.NONE, State.RUNNING, State.SUCCESS]
        states_to_reset = [State.QUEUED, State.SCHEDULED, State.NONE]

        dag = DAG(dag_id=prefix,
                  start_date=DEFAULT_DATE,
                  schedule_interval="@daily")
        tasks = []
        for i in range(len(states)):
            task_id = "{}_task_{}".format(prefix, i)
            task = DummyOperator(task_id=task_id, dag=dag)
            tasks.append(task)

        scheduler = SchedulerJob()

        session = settings.Session()

        # create dagruns
        dr1 = scheduler.create_dag_run(dag)
        dr2 = scheduler.create_dag_run(dag)
        dr1.state = State.RUNNING
        dr2.state = State.SUCCESS
        session.merge(dr1)
        session.merge(dr2)
        session.commit()

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

        self.assertEqual(2, len(scheduler.reset_state_for_orphaned_tasks(session=session)))

        for ti in dr1_tis + dr2_tis:
            ti.refresh_from_db()

        # running dagrun should be reset
        for state, ti in zip(states, dr1_tis):
            if state in states_to_reset:
                self.assertIsNone(ti.state)
            else:
                self.assertEqual(state, ti.state)

        # otherwise not
        for state, ti in zip(states, dr2_tis):
            self.assertEqual(state, ti.state)

        for state, ti in zip(states, dr1_tis):
            ti.state = state
        session.commit()

        scheduler.reset_state_for_orphaned_tasks(filter_by_dag_run=dr1, session=session)

        # check same for dag_run version
        for state, ti in zip(states, dr2_tis):
            self.assertEqual(state, ti.state)

        session.close()

    def test_process_dags_not_create_dagrun_for_subdags(self):
        dag = self.dagbag.get_dag('test_subdag_operator')

        scheduler = SchedulerJob()
        scheduler._process_task_instances = mock.MagicMock()
        scheduler.manage_slas = mock.MagicMock()

        scheduler._process_dags(self.dagbag, [dag] + dag.subdags, None)

        with create_session() as session:
            sub_dagruns = (
                session
                .query(DagRun)
                .filter(DagRun.dag_id == dag.subdags[0].dag_id)
                .count()
            )

            self.assertEqual(0, sub_dagruns)

            parent_dagruns = (
                session
                .query(DagRun)
                .filter(DagRun.dag_id == dag.dag_id)
                .count()
            )

            self.assertGreater(parent_dagruns, 0)

    def test_find_dags_to_run_includes_subdags(self):
        dag = self.dagbag.get_dag('test_subdag_operator')
        print(self.dagbag.dag_ids)
        print(self.dagbag.dag_folder)
        self.assertGreater(len(dag.subdags), 0)
        scheduler = SchedulerJob()
        dags = scheduler._find_dags_to_process(self.dagbag.dags.values(), paused_dag_ids=())

        self.assertIn(dag, dags)
        for subdag in dag.subdags:
            self.assertIn(subdag, dags)

    def test_find_dags_to_run_skip_paused_dags(self):
        dagbag = DagBag(include_examples=False)

        dag = dagbag.get_dag('test_subdag_operator')
        scheduler = SchedulerJob()
        dags = scheduler._find_dags_to_process(dagbag.dags.values(), paused_dag_ids=[dag.dag_id])

        self.assertNotIn(dag, dags)
