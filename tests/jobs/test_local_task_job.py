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

import multiprocessing
import time
import unittest

from airflow import AirflowException, models, settings
from airflow import configuration
from airflow.executors import SequentialExecutor
from airflow.jobs import LocalTaskJob
from airflow.models import DAG, TaskInstance as TI
from airflow.operators.dummy_operator import DummyOperator
from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils import timezone
from airflow.utils.db import create_session
from airflow.utils.net import get_hostname
from airflow.utils.state import State
from tests.compat import patch
from tests.core import TEST_DAG_FOLDER
from tests.executors.test_executor import TestExecutor
from tests.test_utils.db import clear_db_runs

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class LocalTaskJobTest(unittest.TestCase):
    def setUp(self):
        clear_db_runs()

    def test_localtaskjob_essential_attr(self):
        """
        Check whether essential attributes
        of LocalTaskJob can be assigned with
        proper values without intervention
        """
        dag = DAG(
            'test_localtaskjob_essential_attr',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='op1')

        dag.clear()
        dr = dag.create_dagrun(run_id="test",
                               state=State.SUCCESS,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE)
        ti = dr.get_task_instance(task_id=op1.task_id)

        job1 = LocalTaskJob(task_instance=ti,
                            ignore_ti_state=True,
                            executor=SequentialExecutor())

        essential_attr = ["dag_id", "job_type", "start_date", "hostname"]

        check_result_1 = [hasattr(job1, attr) for attr in essential_attr]
        self.assertTrue(all(check_result_1))

        check_result_2 = [getattr(job1, attr) is not None for attr in essential_attr]
        self.assertTrue(all(check_result_2))

    @patch('os.getpid')
    def test_localtaskjob_heartbeat(self, mock_pid):
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

        job1 = LocalTaskJob(task_instance=ti,
                            ignore_ti_state=True,
                            executor=SequentialExecutor())
        self.assertRaises(AirflowException, job1.heartbeat_callback)

        mock_pid.return_value = 1
        ti.state = State.RUNNING
        ti.hostname = get_hostname()
        ti.pid = 1
        session.merge(ti)
        session.commit()

        ret = job1.heartbeat_callback()
        self.assertEqual(ret, None)

        mock_pid.return_value = 2
        self.assertRaises(AirflowException, job1.heartbeat_callback)

    @patch('os.getpid')
    def test_heartbeat_failed_fast(self, mock_getpid):
        """
        Test that task heartbeat will sleep when it fails fast
        """
        mock_getpid.return_value = 1

        heartbeat_records = []

        def heartbeat_recorder():
            heartbeat_records.append(timezone.utcnow())

        with create_session() as session:
            dagbag = models.DagBag(
                dag_folder=TEST_DAG_FOLDER,
                include_examples=False,
            )
            dag_id = 'test_heartbeat_failed_fast'
            task_id = 'test_heartbeat_failed_fast_op'
            dag = dagbag.get_dag(dag_id)
            task = dag.get_task(task_id)

            dag.create_dagrun(run_id="test_heartbeat_failed_fast_run",
                              state=State.RUNNING,
                              execution_date=DEFAULT_DATE,
                              start_date=DEFAULT_DATE,
                              session=session)
            ti = TI(task=task, execution_date=DEFAULT_DATE)
            ti.refresh_from_db()
            ti.state = State.RUNNING
            ti.hostname = get_hostname()
            ti.pid = 1
            session.commit()

            job = LocalTaskJob(task_instance=ti, executor=TestExecutor(do_update=False))
            job.heartrate = 2
            job.heartbeat = heartbeat_recorder
            job._execute()
            self.assertGreater(len(heartbeat_records), 1)
            for i in range(1, len(heartbeat_records)):
                time1 = heartbeat_records[i - 1]
                time2 = heartbeat_records[i]
                self.assertGreaterEqual((time2 - time1).total_seconds(), job.heartrate)

    @unittest.skipIf('mysql' in configuration.conf.get('core', 'sql_alchemy_conn'),
                     "flaky when run on mysql")
    @unittest.skipIf('postgresql' in configuration.conf.get('core', 'sql_alchemy_conn'),
                     'flaky when run on postgresql')
    def test_mark_success_no_kill(self):
        """
        Test that ensures that mark_success in the UI doesn't cause
        the task to fail, and that the task exits
        """
        dagbag = models.DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get('test_mark_success')
        task = dag.get_task('task1')

        session = settings.Session()

        dag.clear()
        dag.create_dagrun(run_id="test",
                          state=State.RUNNING,
                          execution_date=DEFAULT_DATE,
                          start_date=DEFAULT_DATE,
                          session=session)
        ti = TI(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True)
        process = multiprocessing.Process(target=job1.run)
        process.start()
        ti.refresh_from_db()
        for _ in range(0, 50):
            if ti.state == State.RUNNING:
                break
            time.sleep(0.1)
            ti.refresh_from_db()
        self.assertEqual(State.RUNNING, ti.state)
        ti.state = State.SUCCESS
        session.merge(ti)
        session.commit()

        process.join(timeout=10)
        self.assertFalse(process.is_alive())
        ti.refresh_from_db()
        self.assertEqual(State.SUCCESS, ti.state)

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
        ti.hostname = get_hostname()
        ti.pid = 1
        session.commit()

        ti_run = TI(task=task, execution_date=DEFAULT_DATE)
        job1 = LocalTaskJob(task_instance=ti_run,
                            ignore_ti_state=True,
                            executor=SequentialExecutor())
        with patch.object(BaseTaskRunner, 'start', return_value=None) as mock_method:
            job1.run()
            mock_method.assert_not_called()

        ti = dr.get_task_instance(task_id=task.task_id, session=session)
        self.assertEqual(ti.pid, 1)
        self.assertEqual(ti.state, State.RUNNING)

        session.close()
