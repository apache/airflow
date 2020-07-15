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
from datetime import timedelta
from tempfile import NamedTemporaryFile, mkdtemp

import mock
import psutil
import pytest
import six
from freezegun import freeze_time
from mock import MagicMock, patch
from parameterized import parameterized

import airflow.example_dags
from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.jobs.backfill_job import BackfillJob
from airflow.jobs.scheduler_job import DagFileProcessor, SchedulerJob
from airflow.models import DAG, DagBag, DagModel, Pool, SlaMiss, TaskInstance, errors
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKey
from airflow.operators.bash import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import timezone
from airflow.utils.dag_processing import FailureCallbackRequest, SimpleDag, SimpleDagBag
from airflow.utils.dates import days_ago
from airflow.utils.file import list_py_file_paths
from airflow.utils.session import create_session, provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars, env_vars
from tests.test_utils.db import (
    clear_db_dags, clear_db_errors, clear_db_jobs, clear_db_pools, clear_db_runs, clear_db_sla_miss,
    set_default_pool_slots,
)
from tests.test_utils.mock_executor import MockExecutor

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)
PERF_DAGS_FOLDER = os.path.join(ROOT_FOLDER, "scripts", "perf", "dags")
ELASTIC_DAG_FILE = os.path.join(PERF_DAGS_FOLDER, "elastic_dag.py")

TEST_DAG_FOLDER = os.environ['AIRFLOW__CORE__DAGS_FOLDER']
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


@pytest.fixture(scope="class")
def disable_load_example():
    with conf_vars({('core', 'load_examples'): 'false'}):
        with env_vars({('core', 'load_examples'): 'false'}):
            yield


@pytest.mark.usefixtures("disable_load_example")
class TestDagFileProcessor(unittest.TestCase):

    @staticmethod
    def clean_db():
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_errors()
        clear_db_jobs()

    def setUp(self):
        self.clean_db()

        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = MockExecutor()

    def tearDown(self) -> None:
        self.clean_db()

    def create_test_dag(self, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE + timedelta(hours=1), **kwargs):
        dag = DAG(
            dag_id='test_scheduler_reschedule',
            start_date=start_date,
            # Make sure it only creates a single DAG Run
            end_date=end_date)
        dag.clear()
        dag.is_subdag = False
        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            orm_dag.is_paused = False
            session.merge(orm_dag)
            session.commit()
        return dag

    @classmethod
    @patch("airflow.models.dagbag.settings.STORE_SERIALIZED_DAGS", True)
    def setUpClass(cls):
        # Ensure the DAGs we are looking at from the DB are up-to-date
        non_serialized_dagbag = DagBag(store_serialized_dags=False, include_examples=False)
        non_serialized_dagbag.sync_to_db()
        cls.dagbag = DagBag(store_serialized_dags=True)

    def test_dag_file_processor_sla_miss_callback(self):
        """
        Test that the dag file processor calls the sla miss callback
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

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)

        assert sla_callback.called

    def test_dag_file_processor_sla_miss_callback_invalid_sla(self):
        """
        Test that the dag file processor does not call the sla miss callback when
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

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)
        sla_callback.assert_not_called()

    def test_scheduler_executor_overflow(self):
        """
        Test that tasks that are set back to scheduled and removed from the executor
        queue in the case of an overflow.
        """
        executor = MockExecutor(do_update=True, parallelism=3)

        with create_session() as session:
            dagbag = DagBag(dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"))
            dag = self.create_test_dag()
            dag.clear()
            dagbag.bag_dag(dag=dag, root_dag=dag)
            dag = self.create_test_dag()
            dag.clear()
            task = DummyOperator(
                task_id='dummy',
                dag=dag,
                owner='airflow')
            tis = []
            for i in range(1, 10):
                ti = TaskInstance(task, DEFAULT_DATE + timedelta(days=i))
                ti.state = State.SCHEDULED
                tis.append(ti)
                session.merge(ti)

            # scheduler._process_dags(simple_dag_bag)
            @mock.patch('airflow.jobs.scheduler_job.DagBag', return_value=dagbag)
            @mock.patch('airflow.jobs.scheduler_job.SchedulerJob._change_state_for_tis_without_dagrun')
            def do_schedule(mock_dagbag, mock_change_state):
                # Use a empty file since the above mock will return the
                # expected DAGs. Also specify only a single file so that it doesn't
                # try to schedule the above DAG repeatedly.
                with conf_vars({('core', 'mp_start_method'): 'fork'}):
                    scheduler = SchedulerJob(num_runs=1,
                                             executor=executor,
                                             subdir=os.path.join(settings.DAGS_FOLDER,
                                                                 "no_dags.py"))
                    scheduler.heartrate = 0
                    scheduler.run()

            do_schedule()  # pylint: disable=no-value-for-parameter
            for ti in tis:
                ti.refresh_from_db()
            self.assertEqual(len(executor.queued_tasks), 0)

            successful_tasks = [ti for ti in tis if ti.state == State.SUCCESS]
            scheduled_tasks = [ti for ti in tis if ti.state == State.SCHEDULED]
            self.assertEqual(3, len(successful_tasks))
            self.assertEqual(6, len(scheduled_tasks))

    def test_dag_file_processor_sla_miss_callback_sent_notification(self):
        """
        Test that the dag file processor does not call the sla_miss_callback when a
        notification has already been sent
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

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        # Create a TaskInstance for two days ago
        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy',
                              dag_id='test_sla_miss',
                              execution_date=test_start_date,
                              email_sent=False,
                              notification_sent=True))

        # Now call manage_slas and see if the sla_miss callback gets called
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)

        sla_callback.assert_not_called()

    def test_dag_file_processor_sla_miss_callback_exception(self):
        """
        Test that the dag file processor gracefully logs an exception if there is a problem
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

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy',
                              dag_id='test_sla_miss',
                              execution_date=test_start_date))

        # Now call manage_slas and see if the sla_miss callback gets called
        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)
        assert sla_callback.called
        mock_log.exception.assert_called_once_with(
            'Could not call sla_miss_callback for DAG %s',
            'test_sla_miss')

    @mock.patch('airflow.jobs.scheduler_job.send_email')
    def test_dag_file_processor_only_collect_emails_from_sla_missed_tasks(self, mock_send_email):
        session = settings.Session()

        test_start_date = days_ago(2)
        dag = DAG(dag_id='test_sla_miss',
                  default_args={'start_date': test_start_date,
                                'sla': datetime.timedelta(days=1)})

        email1 = 'test1@test.com'
        task = DummyOperator(task_id='sla_missed',
                             dag=dag,
                             owner='airflow',
                             email=email1,
                             sla=datetime.timedelta(hours=1))

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        email2 = 'test2@test.com'
        DummyOperator(task_id='sla_not_missed',
                      dag=dag,
                      owner='airflow',
                      email=email2)

        session.merge(SlaMiss(task_id='sla_missed', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())

        dag_file_processor.manage_slas(dag=dag, session=session)

        self.assertTrue(len(mock_send_email.call_args_list), 1)

        send_email_to = mock_send_email.call_args_list[0][0][0]
        self.assertIn(email1, send_email_to)
        self.assertNotIn(email2, send_email_to)

    @mock.patch('airflow.jobs.scheduler_job.Stats.incr')
    @mock.patch("airflow.utils.email.send_email")
    def test_dag_file_processor_sla_miss_email_exception(self, mock_send_email, mock_stats_incr):
        """
        Test that the dag file processor gracefully logs an exception if there is a problem
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

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)

        dag_file_processor.manage_slas(dag=dag, session=session)
        mock_log.exception.assert_called_once_with(
            'Could not send SLA Miss email notification for DAG %s',
            'test_sla_miss')
        mock_stats_incr.assert_called_once_with('sla_email_notification_failure')

    def test_dag_file_processor_sla_miss_deleted_task(self):
        """
        Test that the dag file processor will not crash when trying to send
        sla miss notification for a deleted task
        """
        session = settings.Session()

        test_start_date = days_ago(2)
        dag = DAG(dag_id='test_sla_miss',
                  default_args={'start_date': test_start_date,
                                'sla': datetime.timedelta(days=1)})

        task = DummyOperator(task_id='dummy',
                             dag=dag,
                             owner='airflow',
                             email='test@test.com',
                             sla=datetime.timedelta(hours=1))

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy_deleted', dag_id='test_sla_miss',
                              execution_date=test_start_date))

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)

    def test_dag_file_processor_dagrun_once(self):
        """
        Test if the dag file proccessor does not create multiple dagruns
        if a dag is scheduled with @once and a start_date
        """
        dag = DAG(
            'test_scheduler_dagrun_once',
            start_date=timezone.datetime(2015, 1, 1),
            schedule_interval="@once")

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dag.clear()
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNone(dr)

    @freeze_time(timezone.datetime(2020, 1, 5))
    def test_dag_file_processor_dagrun_with_timedelta_schedule_and_catchup_false(self):
        """
        Test that the dag file processor does not create multiple dagruns
        if a dag is scheduled with 'timedelta' and catchup=False
        """
        dag = DAG(
            'test_scheduler_dagrun_once_with_timedelta_and_catchup_false',
            start_date=timezone.datetime(2015, 1, 1),
            schedule_interval=timedelta(days=1),
            catchup=False)

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEqual(dr.execution_date, timezone.datetime(2020, 1, 4))
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNone(dr)

    @freeze_time(timezone.datetime(2020, 5, 4))
    def test_dag_file_processor_dagrun_with_timedelta_schedule_and_catchup_true(self):
        """
        Test that the dag file processor creates multiple dagruns
        if a dag is scheduled with 'timedelta' and catchup=True
        """
        dag = DAG(
            'test_scheduler_dagrun_once_with_timedelta_and_catchup_true',
            start_date=timezone.datetime(2020, 5, 1),
            schedule_interval=timedelta(days=1),
            catchup=True)

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEqual(dr.execution_date, timezone.datetime(2020, 5, 1))
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEqual(dr.execution_date, timezone.datetime(2020, 5, 2))
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEqual(dr.execution_date, timezone.datetime(2020, 5, 3))
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNone(dr)

    @parameterized.expand([
        [State.NONE, None, None],
        [State.UP_FOR_RETRY, timezone.utcnow() - datetime.timedelta(minutes=30),
         timezone.utcnow() - datetime.timedelta(minutes=15)],
        [State.UP_FOR_RESCHEDULE, timezone.utcnow() - datetime.timedelta(minutes=30),
         timezone.utcnow() - datetime.timedelta(minutes=15)],
    ])
    def test_dag_file_processor_process_task_instances(self, state, start_date, end_date):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)

        with create_session() as session:
            tis = dr.get_task_instances(session=session)
            for ti in tis:
                ti.state = state
                ti.start_date = start_date
                ti.end_date = end_date

        mock_list = dag_file_processor._process_task_instances(dag, dag_runs=[dr])

        self.assertEqual(
            [(dag.dag_id, dag_task1.task_id, DEFAULT_DATE, TRY_NUMBER)],
            mock_list
        )

    @parameterized.expand([
        [State.NONE, None, None],
        [State.UP_FOR_RETRY, timezone.utcnow() - datetime.timedelta(minutes=30),
         timezone.utcnow() - datetime.timedelta(minutes=15)],
        [State.UP_FOR_RESCHEDULE, timezone.utcnow() - datetime.timedelta(minutes=30),
         timezone.utcnow() - datetime.timedelta(minutes=15)],
    ])
    def test_dag_file_processor_process_task_instances_with_task_concurrency(
        self, state, start_date, end_date,
    ):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        dag = DAG(
            dag_id='test_scheduler_process_execute_task_with_task_concurrency',
            start_date=DEFAULT_DATE)
        dag_task1 = DummyOperator(
            task_id='dummy',
            task_concurrency=2,
            dag=dag,
            owner='airflow')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)

        with create_session() as session:
            tis = dr.get_task_instances(session=session)
            for ti in tis:
                ti.state = state
                ti.start_date = start_date
                ti.end_date = end_date

        ti_to_schedule = dag_file_processor._process_task_instances(dag, dag_runs=[dr])

        assert ti_to_schedule == [
            (dag.dag_id, dag_task1.task_id, DEFAULT_DATE, TRY_NUMBER),
        ]

    @parameterized.expand([
        [State.NONE, None, None],
        [State.UP_FOR_RETRY, timezone.utcnow() - datetime.timedelta(minutes=30),
         timezone.utcnow() - datetime.timedelta(minutes=15)],
        [State.UP_FOR_RESCHEDULE, timezone.utcnow() - datetime.timedelta(minutes=30),
         timezone.utcnow() - datetime.timedelta(minutes=15)],
    ])
    def test_dag_file_processor_process_task_instances_depends_on_past(self, state, start_date, end_date):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        dag = DAG(
            dag_id='test_scheduler_process_execute_task_depends_on_past',
            start_date=DEFAULT_DATE,
            default_args={
                'depends_on_past': True,
            },
        )
        dag_task1 = DummyOperator(
            task_id='dummy1',
            dag=dag,
            owner='airflow')
        dag_task2 = DummyOperator(
            task_id='dummy2',
            dag=dag,
            owner='airflow')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)

        with create_session() as session:
            tis = dr.get_task_instances(session=session)
            for ti in tis:
                ti.state = state
                ti.start_date = start_date
                ti.end_date = end_date

        ti_to_schedule = dag_file_processor._process_task_instances(dag, dag_runs=[dr])

        assert sorted(ti_to_schedule) == [
            (dag.dag_id, dag_task1.task_id, DEFAULT_DATE, TRY_NUMBER),
            (dag.dag_id, dag_task2.task_id, DEFAULT_DATE, TRY_NUMBER),
        ]

    def test_dag_file_processor_do_not_schedule_removed_task(self):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)

        dr = DagRun.find(run_id=dr.run_id)[0]
        # Re-create the DAG, but remove the task
        dag = DAG(
            dag_id='test_scheduler_do_not_schedule_removed_task',
            start_date=DEFAULT_DATE)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        mock_list = dag_file_processor._process_task_instances(dag, dag_runs=[dr])

        self.assertEqual([], mock_list)

    def test_dag_file_processor_do_not_schedule_too_early(self):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNone(dr)

        mock_list = dag_file_processor._process_task_instances(dag, dag_runs=[])
        self.assertEqual([], mock_list)

    def test_dag_file_processor_do_not_schedule_without_tasks(self):
        dag = DAG(
            dag_id='test_scheduler_do_not_schedule_without_tasks',
            start_date=DEFAULT_DATE)

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

            dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
            dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
            dag.clear(session=session)
            dag.start_date = None
            dr = dag_file_processor.create_dag_run(dag, session=session)
            self.assertIsNone(dr)

    def test_dag_file_processor_do_not_run_finished(self):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)

        tis = dr.get_task_instances(session=session)
        for ti in tis:
            ti.state = State.SUCCESS

        session.commit()
        session.close()

        mock_list = dag_file_processor._process_task_instances(dag, dag_runs=[dr])

        self.assertEqual([], mock_list)

    def test_dag_file_processor_add_new_task(self):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 1)

        DummyOperator(
            task_id='dummy2',
            dag=dag,
            owner='airflow')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor._process_task_instances(dag, dag_runs=[dr])

        tis = dr.get_task_instances()
        self.assertEqual(len(tis), 2)

    def test_dag_file_processor_verify_max_active_runs(self):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNone(dr)

    def test_dag_file_processor_fail_dagrun_timeout(self):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        dr.start_date = timezone.utcnow() - datetime.timedelta(days=1)
        session.merge(dr)
        session.commit()

        dr2 = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr2)

        dr.refresh_from_db(session=session)
        self.assertEqual(dr.state, State.FAILED)

    def test_dag_file_processor_verify_max_active_runs_and_dagrun_timeout(self):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)

        # Should not be scheduled as DagRun has not timedout and max_active_runs is reached
        new_dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNone(new_dr)

        # Should be scheduled as dagrun_timeout has passed
        dr.start_date = timezone.utcnow() - datetime.timedelta(days=1)
        session.merge(dr)
        session.commit()
        new_dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(new_dr)

    def test_dag_file_processor_max_active_runs_respected_after_clear(self):
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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag.clear()

        # First create up to 3 dagruns in RUNNING state.
        dr1 = dag_file_processor.create_dag_run(dag)
        assert dr1 is not None
        dr2 = dag_file_processor.create_dag_run(dag)
        assert dr2 is not None
        dr3 = dag_file_processor.create_dag_run(dag)
        assert dr3 is not None
        assert len(DagRun.find(dag_id=dag.dag_id, state=State.RUNNING, session=session)) == 3

        # Reduce max_active_runs to 1
        dag.max_active_runs = 1

        # and schedule them in, so we can check how many
        # tasks are put on the task_instances_list (should be one, not 3)
        task_instances_list = dag_file_processor._process_task_instances(dag, dag_runs=[dr1, dr2, dr3])

        self.assertEqual([(dag.dag_id, dag_task1.task_id, DEFAULT_DATE, TRY_NUMBER)], task_instances_list)

    def test_find_dags_to_run_includes_subdags(self):
        dag = self.dagbag.get_dag('test_subdag_operator')
        self.assertGreater(len(dag.subdags), 0)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dags = dag_file_processor._find_dags_to_process(self.dagbag.dags.values())

        self.assertIn(dag, dags)
        for subdag in dag.subdags:
            self.assertIn(subdag, dags)

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

            op1 = DummyOperator(task_id='t1', dag=dag)
            op2 = DummyOperator(task_id='t2', dag=dag)
            op2.set_upstream(op1)
            op3 = DummyOperator(task_id='t3', dag=dag)
            op3.set_upstream(op2)

            session = settings.Session()
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)
            session.commit()
            session.close()

            return SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        now = timezone.utcnow()
        six_hours_ago_to_the_hour = (now - datetime.timedelta(hours=6)).replace(
            minute=0, second=0, microsecond=0)
        half_an_hour_ago = now - datetime.timedelta(minutes=30)
        two_hours_ago = now - datetime.timedelta(hours=2)

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())

        dag1 = setup_dag(dag_id='dag_with_catchup',
                         schedule_interval='* * * * *',
                         start_date=six_hours_ago_to_the_hour,
                         catchup=True)
        default_catchup = conf.getboolean('scheduler', 'catchup_by_default')
        self.assertEqual(default_catchup, True)
        self.assertEqual(dag1.catchup, True)

        dag2 = setup_dag(dag_id='dag_without_catchup_ten_minute',
                         schedule_interval='*/10 * * * *',
                         start_date=six_hours_ago_to_the_hour,
                         catchup=False)
        dr = dag_file_processor.create_dag_run(dag2)
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
        dr = dag_file_processor.create_dag_run(dag3)
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
        dr = dag_file_processor.create_dag_run(dag4)
        self.assertIsNotNone(dr)

    def test_dag_file_processor_auto_align(self):
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())

        dr = dag_file_processor.create_dag_run(dag)
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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag.clear()

        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEqual(dr.execution_date, timezone.datetime(2016, 1, 1, 10, 10))

    def test_process_dags_not_create_dagrun_for_subdags(self):
        dag = self.dagbag.get_dag('test_subdag_operator')

        scheduler = DagFileProcessor(dag_ids=[dag.dag_id], log=mock.MagicMock())
        scheduler._process_task_instances = mock.MagicMock()
        scheduler.manage_slas = mock.MagicMock()

        scheduler._process_dags([dag] + dag.subdags)

        with create_session() as session:
            sub_dagruns = (
                session.query(DagRun).filter(DagRun.dag_id == dag.subdags[0].dag_id).count()
            )

            self.assertEqual(0, sub_dagruns)

            parent_dagruns = (
                session.query(DagRun).filter(DagRun.dag_id == dag.dag_id).count()
            )

            self.assertGreater(parent_dagruns, 0)

    @patch.object(TaskInstance, 'handle_failure')
    def test_execute_on_failure_callbacks(self, mock_ti_handle_failure):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        with create_session() as session:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag('example_branch_operator')
            task = dag.get_task(task_id='run_this_first')

            ti = TaskInstance(task, DEFAULT_DATE, State.RUNNING)

            session.add(ti)
            session.commit()

            requests = [
                FailureCallbackRequest(
                    full_filepath="A",
                    simple_task_instance=SimpleTaskInstance(ti),
                    msg="Message"
                )
            ]
            dag_file_processor.execute_on_failure_callbacks(dagbag, requests)
            mock_ti_handle_failure.assert_called_once_with(
                "Message",
                conf.getboolean('core', 'unit_test_mode'),
                mock.ANY
            )

    def test_process_file_should_failure_callback(self):
        dag_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '../dags/test_on_failure_callback.py'
        )
        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        with create_session() as session, NamedTemporaryFile(delete=False) as callback_file:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag('test_om_failure_callback_dag')
            task = dag.get_task(task_id='test_om_failure_callback_task')

            ti = TaskInstance(task, DEFAULT_DATE, State.RUNNING)

            session.add(ti)
            session.commit()

            requests = [
                FailureCallbackRequest(
                    full_filepath=dag.full_filepath,
                    simple_task_instance=SimpleTaskInstance(ti),
                    msg="Message"
                )
            ]
            callback_file.close()

            with mock.patch.dict("os.environ", {"AIRFLOW_CALLBACK_FILE": callback_file.name}):
                dag_file_processor.process_file(dag_file, requests)
            with open(callback_file.name) as callback_file2:
                content = callback_file2.read()
            self.assertEqual("Callback fired", content)
            os.remove(callback_file.name)

    def test_should_parse_only_unpaused_dags(self):
        dag_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '../dags/test_multiple_dags.py'
        )
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        dagbag.sync_to_db()
        with create_session() as session:
            session.query(TaskInstance).delete()
            (
                session.query(DagModel)
                .filter(DagModel.dag_id == "test_multiple_dags__dag_1")
                .update({DagModel.is_paused: True}, synchronize_session=False)
            )

        simple_dags, import_errors_count = dag_file_processor.process_file(
            file_path=dag_file, failure_callback_requests=[]
        )
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        self.assertEqual(0, import_errors_count)
        self.assertEqual(['test_multiple_dags__dag_2'], [dag.dag_id for dag in simple_dags])
        self.assertEqual({'test_multiple_dags__dag_2'}, {ti.dag_id for ti in tis})

    def test_should_mark_dummy_task_as_success(self):
        dag_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '../dags/test_only_dummy_tasks.py'
        )
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        with create_session() as session:
            session.query(TaskInstance).delete()
            session.query(DagModel).delete()

        dagbag = DagBag(dag_folder=dag_file, include_examples=False)
        dagbag.sync_to_db()

        simple_dags, import_errors_count = dag_file_processor.process_file(
            file_path=dag_file, failure_callback_requests=[]
        )
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        self.assertEqual(0, import_errors_count)
        self.assertEqual(['test_only_dummy_tasks'], [dag.dag_id for dag in simple_dags])
        self.assertEqual(5, len(tis))
        self.assertEqual({
            ('test_task_a', 'success'),
            ('test_task_b', None),
            ('test_task_c', 'success'),
            ('test_task_on_execute', 'scheduled'),
            ('test_task_on_success', 'scheduled'),
        }, {(ti.task_id, ti.state) for ti in tis})
        for state, start_date, end_date, duration in [(ti.state, ti.start_date, ti.end_date, ti.duration) for
                                                      ti in tis]:
            if state == 'success':
                self.assertIsNotNone(start_date)
                self.assertIsNotNone(end_date)
                self.assertEqual(0.0, duration)
            else:
                self.assertIsNone(start_date)
                self.assertIsNone(end_date)
                self.assertIsNone(duration)

        dag_file_processor.process_file(
            file_path=dag_file, failure_callback_requests=[]
        )
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        self.assertEqual(5, len(tis))
        self.assertEqual({
            ('test_task_a', 'success'),
            ('test_task_b', 'success'),
            ('test_task_c', 'success'),
            ('test_task_on_execute', 'scheduled'),
            ('test_task_on_success', 'scheduled'),
        }, {(ti.task_id, ti.state) for ti in tis})
        for state, start_date, end_date, duration in [(ti.state, ti.start_date, ti.end_date, ti.duration) for
                                                      ti in tis]:
            if state == 'success':
                self.assertIsNotNone(start_date)
                self.assertIsNotNone(end_date)
                self.assertEqual(0.0, duration)
            else:
                self.assertIsNone(start_date)
                self.assertIsNone(end_date)
                self.assertIsNone(duration)


@pytest.mark.quarantined
class TestDagFileProcessorQueriesCount(unittest.TestCase):
    """
    These tests are designed to detect changes in the number of queries for different DAG files.

    Each test has saved queries count in the table/spreadsheets. If you make a change that affected the number
    of queries, please update the tables.

    These tests allow easy detection when a change is made that affects the performance of the
    DagFileProcessor.
    """

    def setUp(self) -> None:
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_errors()

    @parameterized.expand(
        [
            # pylint: disable=bad-whitespace
            # expected, dag_count, task_count, start_ago, schedule_interval, shape
            # One DAG with one task per DAG file
            ([ 1,   1,   1,  1],  1,  1, "1d",  "None",  "no_structure"),  # noqa
            ([ 1,   1,   1,  1],  1,  1, "1d",  "None",        "linear"),  # noqa
            ([ 9,   5,   5,  5],  1,  1, "1d", "@once",  "no_structure"),  # noqa
            ([ 9,   5,   5,  5],  1,  1, "1d", "@once",        "linear"),  # noqa
            ([ 9,  12,  15, 18],  1,  1, "1d",   "30m",  "no_structure"),  # noqa
            ([ 9,  12,  15, 18],  1,  1, "1d",   "30m",        "linear"),  # noqa
            ([ 9,  12,  15, 18],  1,  1, "1d",   "30m",   "binary_tree"),  # noqa
            ([ 9,  12,  15, 18],  1,  1, "1d",   "30m",          "star"),  # noqa
            ([ 9,  12,  15, 18],  1,  1, "1d",   "30m",          "grid"),  # noqa
            # One DAG with five tasks per DAG  file
            ([ 1,   1,   1,  1],  1,  5, "1d",  "None",  "no_structure"),  # noqa
            ([ 1,   1,   1,  1],  1,  5, "1d",  "None",        "linear"),  # noqa
            ([ 9,   5,   5,  5],  1,  5, "1d", "@once",  "no_structure"),  # noqa
            ([10,   6,   6,  6],  1,  5, "1d", "@once",        "linear"),  # noqa
            ([ 9,  12,  15, 18],  1,  5, "1d",   "30m",  "no_structure"),  # noqa
            ([10,  14,  18, 22],  1,  5, "1d",   "30m",        "linear"),  # noqa
            ([10,  14,  18, 22],  1,  5, "1d",   "30m",   "binary_tree"),  # noqa
            ([10,  14,  18, 22],  1,  5, "1d",   "30m",          "star"),  # noqa
            ([10,  14,  18, 22],  1,  5, "1d",   "30m",          "grid"),  # noqa
            # 10 DAGs with 10 tasks per DAG file
            ([ 1,   1,   1,   1], 10, 10, "1d",  "None",  "no_structure"),  # noqa
            ([ 1,   1,   1,   1], 10, 10, "1d",  "None",        "linear"),  # noqa
            ([81,  41,  41,  41], 10, 10, "1d", "@once",  "no_structure"),  # noqa
            ([91,  51,  51,  51], 10, 10, "1d", "@once",        "linear"),  # noqa
            ([81, 111, 111, 111], 10, 10, "1d",   "30m",  "no_structure"),  # noqa
            ([91, 131, 131, 131], 10, 10, "1d",   "30m",        "linear"),  # noqa
            ([91, 131, 131, 131], 10, 10, "1d",   "30m",   "binary_tree"),  # noqa
            ([91, 131, 131, 131], 10, 10, "1d",   "30m",          "star"),  # noqa
            ([91, 131, 131, 131], 10, 10, "1d",   "30m",          "grid"),  # noqa
            # pylint: enable=bad-whitespace
        ]
    )
    def test_process_dags_queries_count(
        self, expected_query_counts, dag_count, task_count, start_ago, schedule_interval, shape
    ):
        with mock.patch.dict("os.environ", {
            "PERF_DAGS_COUNT": str(dag_count),
            "PERF_TASKS_COUNT": str(task_count),
            "PERF_START_AGO": start_ago,
            "PERF_SCHEDULE_INTERVAL": schedule_interval,
            "PERF_SHAPE": shape,
        }), conf_vars({
            ('scheduler', 'use_job_schedule'): 'True',
        }):
            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE, include_examples=False)
            processor = DagFileProcessor([], mock.MagicMock())
            for expected_query_count in expected_query_counts:
                with assert_queries_count(expected_query_count):
                    processor._process_dags(dagbag.dags.values())

    @parameterized.expand(
        [
            # pylint: disable=bad-whitespace
            # expected, dag_count, task_count, start_ago, schedule_interval, shape
            # One DAG with two tasks per DAG file
            ([ 5,   5,   5,   5],  1,  1, "1d",   "None", "no_structure"),  # noqa
            ([ 5,   5,   5,   5],  1,  1, "1d",   "None",       "linear"),  # noqa
            ([15,   9,   9,   9],  1,  1, "1d",  "@once", "no_structure"),  # noqa
            ([15,   9,   9,   9],  1,  1, "1d",  "@once",       "linear"),  # noqa
            ([15,  18,  21,  24],  1,  1, "1d",    "30m", "no_structure"),  # noqa
            ([15,  18,  21,  24],  1,  1, "1d",    "30m",       "linear"),  # noqa
            # One DAG with five tasks per DAG file
            ([ 5,   5,   5,   5],  1,  5, "1d",   "None", "no_structure"),  # noqa
            ([ 5,   5,   5,   5],  1,  5, "1d",   "None",       "linear"),  # noqa
            ([15,   9,   9,   9],  1,  5, "1d",  "@once", "no_structure"),  # noqa
            ([16,  10,  10,  10],  1,  5, "1d",  "@once",       "linear"),  # noqa
            ([15,  18,  21,  24],  1,  5, "1d",    "30m", "no_structure"),  # noqa
            ([16,  20,  24,  28],  1,  5, "1d",    "30m",       "linear"),  # noqa
            # 10 DAGs with 10 tasks per DAG file
            ([ 5,   5,   5,   5], 10, 10, "1d",  "None",  "no_structure"),  # noqa
            ([ 5,   5,   5,   5], 10, 10, "1d",  "None",        "linear"),  # noqa
            ([87,  45,  45,  45], 10, 10, "1d", "@once",  "no_structure"),  # noqa
            ([97,  55,  55,  55], 10, 10, "1d", "@once",        "linear"),  # noqa
            ([87, 117, 117, 117], 10, 10, "1d",   "30m",  "no_structure"),  # noqa
            ([97, 137, 137, 137], 10, 10, "1d",   "30m",        "linear"),  # noqa
            # pylint: enable=bad-whitespace
        ]
    )
    def test_process_file_queries_count(
        self, expected_query_counts, dag_count, task_count, start_ago, schedule_interval, shape
    ):
        with mock.patch.dict("os.environ", {
            "PERF_DAGS_COUNT": str(dag_count),
            "PERF_TASKS_COUNT": str(task_count),
            "PERF_START_AGO": start_ago,
            "PERF_SCHEDULE_INTERVAL": schedule_interval,
            "PERF_SHAPE": shape,
        }), conf_vars({
            ('scheduler', 'use_job_schedule'): 'True'
        }):
            processor = DagFileProcessor([], mock.MagicMock())
            for expected_query_count in expected_query_counts:
                with assert_queries_count(expected_query_count):
                    processor.process_file(ELASTIC_DAG_FILE, [])


@pytest.mark.usefixtures("disable_load_example")
class TestSchedulerJob(unittest.TestCase):

    def setUp(self):
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_errors()

        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = MockExecutor()

    @classmethod
    @patch("airflow.models.dagbag.settings.STORE_SERIALIZED_DAGS", True)
    def setUpClass(cls):
        # Ensure the DAGs we are looking at from the DB are up-to-date
        non_serialized_dagbag = DagBag(store_serialized_dags=False, include_examples=False)
        non_serialized_dagbag.sync_to_db()
        cls.dagbag = DagBag(store_serialized_dags=True)

    def test_is_alive(self):
        job = SchedulerJob(None, heartrate=10, state=State.RUNNING)
        self.assertTrue(job.is_alive())

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=20)
        self.assertTrue(job.is_alive())

        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(seconds=31)
        self.assertFalse(job.is_alive())

        # test because .seconds was used before instead of total_seconds
        # internal repr of datetime is (days, seconds)
        job.latest_heartbeat = timezone.utcnow() - datetime.timedelta(days=1)
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
        :type dags_folder: str
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
                                 executor=MockExecutor(do_update=False))
        scheduler.run()
        shutil.rmtree(empty_dir)

        # Remove potential noise created by previous tests.
        current_children = set(current_process.children(recursive=True)) - set(
            old_children)
        self.assertFalse(current_children)

    @mock.patch('airflow.jobs.scheduler_job.Stats.incr')
    def test_process_executor_events(self, mock_stats_incr):
        dag_id = "test_process_executor_events"
        dag_id2 = "test_process_executor_events_2"
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, full_filepath="/test_path1/")
        dag2 = DAG(dag_id=dag_id2, start_date=DEFAULT_DATE, full_filepath="/test_path1/")
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        DummyOperator(dag=dag2, task_id=task_id_1)

        dagbag1 = self._make_simple_dag_bag([dag])
        dagbag2 = self._make_simple_dag_bag([dag2])

        scheduler = SchedulerJob()
        session = settings.Session()

        ti1 = TaskInstance(task1, DEFAULT_DATE)
        ti1.state = State.QUEUED
        session.merge(ti1)
        session.commit()

        executor = MockExecutor(do_update=False)
        executor.event_buffer[ti1.key] = State.FAILED, None

        scheduler.executor = executor

        scheduler.processor_agent = mock.MagicMock()
        # dag bag does not contain dag_id
        scheduler._process_executor_events(simple_dag_bag=dagbag2)
        ti1.refresh_from_db()
        self.assertEqual(ti1.state, State.QUEUED)
        scheduler.processor_agent.send_callback_to_execute.assert_not_called()

        # dag bag does contain dag_id
        scheduler._process_executor_events(simple_dag_bag=dagbag1)
        ti1.refresh_from_db()
        self.assertEqual(ti1.state, State.QUEUED)
        scheduler.processor_agent.send_callback_to_execute.assert_called_once_with(
            full_filepath='/test_path1/',
            task_instance=mock.ANY,
            msg='Executor reports task instance finished (failed) '
                'although the task says its queued. (Info: None) Was the task killed externally?'
        )
        scheduler.processor_agent.reset_mock()

        # ti in success state
        ti1.state = State.SUCCESS
        session.merge(ti1)
        session.commit()
        executor.event_buffer[ti1.key] = State.SUCCESS, None

        scheduler._process_executor_events(simple_dag_bag=dagbag1)
        ti1.refresh_from_db()
        self.assertEqual(ti1.state, State.SUCCESS)
        scheduler.processor_agent.send_callback_to_execute.assert_not_called()

        mock_stats_incr.assert_called_once_with('scheduler.tasks.killed_externally')

    def test_process_executor_events_uses_inmemory_try_number(self):
        execution_date = DEFAULT_DATE
        dag_id = "dag_id"
        task_id = "task_id"
        try_number = 42

        scheduler = SchedulerJob()
        executor = MagicMock()
        event_buffer = {
            TaskInstanceKey(dag_id, task_id, execution_date, try_number): (State.SUCCESS, None)
        }
        executor.get_event_buffer.return_value = event_buffer
        scheduler.executor = executor

        processor_agent = MagicMock()
        scheduler.processor_agent = processor_agent

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task = DummyOperator(dag=dag, task_id=task_id)

        with create_session() as session:
            ti = TaskInstance(task, DEFAULT_DATE)
            ti.state = State.SUCCESS
            session.merge(ti)

        scheduler._process_executor_events(simple_dag_bag=MagicMock())
        # Assert that the even_buffer is empty so the task was popped using right
        # task instance key
        self.assertEqual(event_buffer, {})

    def test_execute_task_instances_is_paused_wont_execute(self):
        dag_id = 'SchedulerJobTest.test_execute_task_instances_is_paused_wont_execute'
        task_id_1 = 'dummy_task'

        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
        ti1 = TaskInstance(task1, DEFAULT_DATE)
        ti1.state = State.SCHEDULED
        dr1.state = State.RUNNING
        dagmodel = DagModel()
        dagmodel.dag_id = dag_id
        dagmodel.is_paused = True
        session.merge(ti1)
        session.merge(dr1)
        session.add(dagmodel)
        session.commit()

        scheduler._execute_task_instances(dagbag)
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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dag_file_processor.create_dag_run(dag)
        ti1 = TaskInstance(task1, DEFAULT_DATE)
        ti1.state = State.SCHEDULED
        ti1.execution_date = ti1.execution_date + datetime.timedelta(days=1)
        session.merge(ti1)
        session.commit()

        scheduler._execute_task_instances(dagbag)
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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
        dr1.run_type = DagRunType.BACKFILL_JOB.value
        ti1 = TaskInstance(task1, dr1.execution_date)
        ti1.refresh_from_db()
        ti1.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(dr1)
        session.commit()

        self.assertTrue(dr1.is_backfill)

        scheduler._execute_task_instances(dagbag)
        ti1.refresh_from_db()
        self.assertEqual(State.SCHEDULED, ti1.state)

    def test_find_executable_task_instances_backfill_nodagrun(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_backfill_nodagrun'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
        dr2 = dag_file_processor.create_dag_run(dag)
        dr2.run_type = DagRunType.BACKFILL_JOB.value

        ti_no_dagrun = TaskInstance(task1, DEFAULT_DATE - datetime.timedelta(days=1))
        ti_backfill = TaskInstance(task1, dr2.execution_date)
        ti_with_dagrun = TaskInstance(task1, dr1.execution_date)
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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
        dr2 = dag_file_processor.create_dag_run(dag)

        tis = ([
            TaskInstance(task1, dr1.execution_date),
            TaskInstance(task2, dr1.execution_date),
            TaskInstance(task1, dr2.execution_date),
            TaskInstance(task2, dr2.execution_date)
        ])
        for ti in tis:
            ti.state = State.SCHEDULED
            session.merge(ti)
        pool = Pool(pool='a', slots=1, description='haha')
        pool2 = Pool(pool='b', slots=100, description='haha')
        session.add(pool)
        session.add(pool2)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
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
        op1 = DummyOperator(dag=dag, task_id='dummy1')
        op2 = DummyOperator(dag=dag, task_id='dummy2')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        executor = MockExecutor(do_update=True)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob(executor=executor)
        dr1 = dag_file_processor.create_dag_run(dag)
        dr2 = dag_file_processor.create_dag_run(dag)

        ti1 = TaskInstance(task=op1, execution_date=dr1.execution_date)
        ti2 = TaskInstance(task=op2, execution_date=dr2.execution_date)
        ti1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED

        session = settings.Session()
        session.merge(ti1)
        session.merge(ti2)
        session.commit()

        # Two tasks w/o pool up for execution and our default pool size is 1
        res = scheduler._find_executable_task_instances(
            dagbag,
            session=session)
        self.assertEqual(1, len(res))

        ti2.state = State.RUNNING
        session.merge(ti2)
        session.commit()

        # One task w/o pool up for execution and one task task running
        res = scheduler._find_executable_task_instances(
            dagbag,
            session=session)
        self.assertEqual(0, len(res))

        session.close()

    def test_nonexistent_pool(self):
        dag_id = 'SchedulerJobTest.test_nonexistent_pool'
        task_id = 'dummy_wrong_pool'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task = DummyOperator(dag=dag, task_id=task_id, pool="this_pool_doesnt_exist")
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr = dag_file_processor.create_dag_run(dag)

        ti = TaskInstance(task, dr.execution_date)
        ti.state = State.SCHEDULED
        session.merge(ti)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            session=session)
        session.commit()
        self.assertEqual(0, len(res))

    def test_find_executable_task_instances_none(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_none'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dag_file_processor.create_dag_run(dag)
        session.commit()

        self.assertEqual(0, len(scheduler._find_executable_task_instances(
            dagbag,
            session=session)))

    def test_find_executable_task_instances_concurrency(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_concurrency'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=2)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
        dr2 = dag_file_processor.create_dag_run(dag)
        dr3 = dag_file_processor.create_dag_run(dag)

        ti1 = TaskInstance(task1, dr1.execution_date)
        ti2 = TaskInstance(task1, dr2.execution_date)
        ti3 = TaskInstance(task1, dr3.execution_date)
        ti1.state = State.RUNNING
        ti2.state = State.SCHEDULED
        ti3.state = State.SCHEDULED
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            session=session)

        self.assertEqual(1, len(res))
        res_keys = map(lambda x: x.key, res)
        self.assertIn(ti2.key, res_keys)

        ti2.state = State.RUNNING
        session.merge(ti2)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            session=session)

        self.assertEqual(0, len(res))

    def test_find_executable_task_instances_concurrency_queued(self):
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_concurrency_queued'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=3)
        task1 = DummyOperator(dag=dag, task_id='dummy1')
        task2 = DummyOperator(dag=dag, task_id='dummy2')
        task3 = DummyOperator(dag=dag, task_id='dummy3')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()
        dag_run = dag_file_processor.create_dag_run(dag)

        ti1 = TaskInstance(task1, dag_run.execution_date)
        ti2 = TaskInstance(task2, dag_run.execution_date)
        ti3 = TaskInstance(task3, dag_run.execution_date)
        ti1.state = State.RUNNING
        ti2.state = State.QUEUED
        ti3.state = State.SCHEDULED

        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            session=session)

        self.assertEqual(1, len(res))
        self.assertEqual(res[0].key, ti3.key)

    def test_find_executable_task_instances_task_concurrency(self):  # pylint: disable=too-many-statements
        dag_id = 'SchedulerJobTest.test_find_executable_task_instances_task_concurrency'
        task_id_1 = 'dummy'
        task_id_2 = 'dummy2'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=16)
        task1 = DummyOperator(dag=dag, task_id=task_id_1, task_concurrency=2)
        task2 = DummyOperator(dag=dag, task_id=task_id_2)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        executor = MockExecutor(do_update=True)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob(executor=executor)
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
        dr2 = dag_file_processor.create_dag_run(dag)
        dr3 = dag_file_processor.create_dag_run(dag)

        ti1_1 = TaskInstance(task1, dr1.execution_date)
        ti2 = TaskInstance(task2, dr1.execution_date)

        ti1_1.state = State.SCHEDULED
        ti2.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti2)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            session=session)

        self.assertEqual(2, len(res))

        ti1_1.state = State.RUNNING
        ti2.state = State.RUNNING
        ti1_2 = TaskInstance(task1, dr2.execution_date)
        ti1_2.state = State.SCHEDULED
        session.merge(ti1_1)
        session.merge(ti2)
        session.merge(ti1_2)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
            session=session)

        self.assertEqual(1, len(res))

        ti1_2.state = State.RUNNING
        ti1_3 = TaskInstance(task1, dr3.execution_date)
        ti1_3.state = State.SCHEDULED
        session.merge(ti1_2)
        session.merge(ti1_3)
        session.commit()

        res = scheduler._find_executable_task_instances(
            dagbag,
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
            session=session)

        self.assertEqual(1, len(res))

    def test_change_state_for_executable_task_instances_no_tis(self):
        scheduler = SchedulerJob()
        session = settings.Session()
        res = scheduler._change_state_for_executable_task_instances(
            [], session)
        self.assertEqual(0, len(res))

    def test_change_state_for_executable_task_instances_no_tis_with_state(self):
        dag_id = 'SchedulerJobTest.test_change_state_for__no_tis_with_state'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE, concurrency=2)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        self._make_simple_dag_bag([dag])

        scheduler = SchedulerJob()
        session = settings.Session()

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dr1 = dag_file_processor.create_dag_run(dag)
        dr2 = dag_file_processor.create_dag_run(dag)
        dr3 = dag_file_processor.create_dag_run(dag)

        ti1 = TaskInstance(task1, dr1.execution_date)
        ti2 = TaskInstance(task1, dr2.execution_date)
        ti3 = TaskInstance(task1, dr3.execution_date)
        ti1.state = State.RUNNING
        ti2.state = State.RUNNING
        ti3.state = State.RUNNING
        session.merge(ti1)
        session.merge(ti2)
        session.merge(ti3)

        session.commit()

        res = scheduler._change_state_for_executable_task_instances(
            [ti1, ti2, ti3],
            session)
        self.assertEqual(0, len(res))

    def test_enqueue_task_instances_with_queued_state(self):
        dag_id = 'SchedulerJobTest.test_enqueue_task_instances_with_queued_state'
        task_id_1 = 'dummy'
        dag = DAG(dag_id=dag_id, start_date=DEFAULT_DATE)
        task1 = DummyOperator(dag=dag, task_id=task_id_1)
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)

        ti1 = TaskInstance(task1, dr1.execution_date)
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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = SimpleDagBag([])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
        ti1 = TaskInstance(task1, dr1.execution_date)
        ti1.state = State.SCHEDULED
        session.merge(ti1)
        session.commit()

        self.assertEqual(0, scheduler._execute_task_instances(dagbag))

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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        # create first dag run with 1 running and 1 queued
        dr1 = dag_file_processor.create_dag_run(dag)
        ti1 = TaskInstance(task1, dr1.execution_date)
        ti2 = TaskInstance(task2, dr1.execution_date)
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
        dr2 = dag_file_processor.create_dag_run(dag)
        ti3 = TaskInstance(task1, dr2.execution_date)
        ti4 = TaskInstance(task2, dr2.execution_date)
        ti3.refresh_from_db()
        ti4.refresh_from_db()
        # manually set to scheduled so we can pick them up
        ti3.state = State.SCHEDULED
        ti4.state = State.SCHEDULED
        session.merge(ti3)
        session.merge(ti4)
        session.commit()

        self.assertEqual(State.RUNNING, dr2.state)

        res = scheduler._execute_task_instances(dagbag)

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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dagbag = self._make_simple_dag_bag([dag])

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        scheduler.max_tis_per_query = 3
        session = settings.Session()

        tis = []
        for _ in range(0, 4):
            dr = dag_file_processor.create_dag_run(dag)
            ti1 = TaskInstance(task1, dr.execution_date)
            ti2 = TaskInstance(task2, dr.execution_date)
            tis.append(ti1)
            tis.append(ti2)
            ti1.refresh_from_db()
            ti2.refresh_from_db()
            ti1.state = State.SCHEDULED
            ti2.state = State.SCHEDULED
            session.merge(ti1)
            session.merge(ti2)
            session.commit()
        res = scheduler._execute_task_instances(dagbag)

        self.assertEqual(8, res)
        for ti in tis:
            ti.refresh_from_db()
            self.assertEqual(State.QUEUED, ti.state)

    @pytest.mark.quarantined
    @pytest.mark.xfail(condition=True, reason="The test is flaky with nondeterministic result")
    def test_change_state_for_tis_without_dagrun(self):
        dag1 = DAG(dag_id='test_change_state_for_tis_without_dagrun', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag1, owner='airflow')

        DummyOperator(task_id='dummy_b', dag=dag1, owner='airflow')
        dag1 = SerializedDAG.from_dict(SerializedDAG.to_dict(dag1))

        dag2 = DAG(dag_id='test_change_state_for_tis_without_dagrun_dont_change', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag2, owner='airflow')

        dag2 = SerializedDAG.from_dict(SerializedDAG.to_dict(dag2))

        dag3 = DAG(dag_id='test_change_state_for_tis_without_dagrun_no_dagrun', start_date=DEFAULT_DATE)

        DummyOperator(task_id='dummy', dag=dag3, owner='airflow')

        dag3 = SerializedDAG.from_dict(SerializedDAG.to_dict(dag3))

        session = settings.Session()
        dr1 = dag1.create_dagrun(run_type=DagRunType.SCHEDULED,
                                 state=State.RUNNING,
                                 execution_date=DEFAULT_DATE,
                                 start_date=DEFAULT_DATE,
                                 session=session)

        dr2 = dag2.create_dagrun(run_type=DagRunType.SCHEDULED,
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

        ti3 = TaskInstance(dag3.get_task('dummy'), DEFAULT_DATE)
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
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        # If there's no left over task in executor.queued_tasks, nothing happens
        session = settings.Session()
        scheduler_job = SchedulerJob()
        mock_logger = mock.MagicMock()
        test_executor = MockExecutor(do_update=False)
        scheduler_job.executor = test_executor
        scheduler_job._logger = mock_logger
        scheduler_job._change_state_for_tasks_failed_to_execute()  # pylint: disable=no-value-for-parameter
        mock_logger.info.assert_not_called()

        # Tasks failed to execute with QUEUED state will be set to SCHEDULED state.
        session.query(TaskInstance).delete()
        session.commit()
        key = 'dag_id', 'task_id', DEFAULT_DATE, 1
        test_executor.queued_tasks[key] = 'value'
        ti = TaskInstance(task, DEFAULT_DATE)
        ti.state = State.QUEUED
        session.merge(ti)  # pylint: disable=no-value-for-parameter
        session.commit()

        scheduler_job._change_state_for_tasks_failed_to_execute()  # pylint: disable=no-value-for-parameter

        ti.refresh_from_db()
        self.assertEqual(State.SCHEDULED, ti.state)

        # Tasks failed to execute with RUNNING state will not be set to SCHEDULED state.
        session.query(TaskInstance).delete()
        session.commit()
        ti.state = State.RUNNING

        session.merge(ti)
        session.commit()

        scheduler_job._change_state_for_tasks_failed_to_execute()  # pylint: disable=no-value-for-parameter

        ti.refresh_from_db()
        self.assertEqual(State.RUNNING, ti.state)

    def test_reset_state_for_orphaned_tasks(self):
        session = settings.Session()
        dag = DAG(
            'test_execute_helper_reset_orphaned_tasks',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='op1')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag.clear()
        dr = dag.create_dagrun(run_type=DagRunType.SCHEDULED,
                               state=State.RUNNING,
                               execution_date=DEFAULT_DATE,
                               start_date=DEFAULT_DATE,
                               session=session)
        dr2 = dag.create_dagrun(run_type=DagRunType.BACKFILL_JOB,
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
        scheduler.processor_agent = processor

        scheduler.reset_state_for_orphaned_tasks()

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
    def test_scheduler_loop_should_change_state_for_tis_without_dagrun(self,
                                                                       initial_task_state,
                                                                       expected_task_state):
        session = settings.Session()
        dag = DAG(
            'test_execute_helper_should_change_state_for_tis_without_dagrun',
            start_date=DEFAULT_DATE,
            default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='op1')
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        # Create DAG run with FAILED state
        dag.clear()
        dr = dag.create_dagrun(run_type=DagRunType.SCHEDULED,
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
        executor = MockExecutor(do_update=False)
        executor.queued_tasks
        scheduler.executor = executor
        processor = mock.MagicMock()
        processor.harvest_simple_dags.return_value = [dag]
        processor.done = True
        scheduler.processor_agent = processor

        scheduler._run_scheduler_loop()

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

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag = self.dagbag.get_dag(dag_id)
        dr = dag_file_processor.create_dag_run(dag)

        if advance_execution_date:
            # run a second time to schedule a dagrun after the start_date
            dr = dag_file_processor.create_dag_run(dag)
        ex_date = dr.execution_date

        for tid, state in expected_task_states.items():
            if state != State.FAILED:
                continue
            self.null_exec.mock_task_fail(dag_id, tid, ex_date)

        try:
            # This needs a _REAL_ dag, not the serialized version
            dag.run(start_date=ex_date, end_date=ex_date, executor=self.null_exec, **run_kwargs)
        except AirflowException:
            pass

        # test tasks
        for task_id, expected_state in expected_task_states.items():
            task = dag.get_task(task_id)
            ti = TaskInstance(task, ex_date)
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
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_id = 'test_dagrun_states_root_fail_unfinished'
        dag = self.dagbag.get_dag(dag_id)
        dr = dag_file_processor.create_dag_run(dag)
        self.null_exec.mock_task_fail(dag_id, 'test_dagrun_fail', DEFAULT_DATE)

        with self.assertRaises(AirflowException):
            dag.run(start_date=dr.execution_date, end_date=dr.execution_date, executor=self.null_exec)

        # Mark the successful task as never having run since we want to see if the
        # dagrun will be in a running state despite haveing an unfinished task.
        with create_session() as session:
            ti = dr.get_task_instance('test_dagrun_unfinished', session=session)
            ti.state = State.NONE
            session.commit()
        dr.update_state()
        self.assertEqual(dr.state, State.RUNNING)

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
            self.assertGreater(dag.start_date, datetime.datetime.now(timezone.utc))

            scheduler = SchedulerJob(dag_id,
                                     executor=self.null_exec,
                                     subdir=dag.fileloc,
                                     num_runs=1)
            scheduler.run()

            # zero tasks ran
            self.assertEqual(
                len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()), 0)
            session.commit()
            self.assertListEqual([], self.null_exec.sorted_tasks)

            # previously, running this backfill would kick off the Scheduler
            # because it would take the most recent run and start from there
            # That behavior still exists, but now it will only do so if after the
            # start date
            bf_exec = MockExecutor()
            backfill = BackfillJob(
                executor=bf_exec,
                dag=dag,
                start_date=DEFAULT_DATE,
                end_date=DEFAULT_DATE)
            backfill.run()

            # one task ran
            self.assertEqual(
                len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()), 1)
            self.assertListEqual(
                [
                    (TaskInstanceKey(dag.dag_id, 'dummy', DEFAULT_DATE, 1), (State.SUCCESS, None)),
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
                len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()), 1)
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
        tiq = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id)
        ti1s = tiq.filter(TaskInstance.task_id == 'dummy1').all()
        ti2s = tiq.filter(TaskInstance.task_id == 'dummy2').all()
        self.assertEqual(len(ti1s), 0)
        self.assertEqual(len(ti2s), 2)
        for task in ti2s:
            self.assertEqual(task.state, State.SUCCESS)

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
            len(session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()), 0)

    @conf_vars({("core", "mp_start_method"): "spawn"})
    def test_scheduler_multiprocessing_with_spawn_method(self):
        """
        Test that the scheduler can successfully queue multiple dags in parallel
        when using "spawn" mode of multiprocessing. (Fork is default on Linux and older OSX)
        """
        dag_ids = ['test_start_date_scheduling', 'test_dagrun_states_success']
        for dag_id in dag_ids:
            dag = self.dagbag.get_dag(dag_id)
            dag.clear()

        scheduler = SchedulerJob(dag_ids=dag_ids,
                                 executor=self.null_exec,
                                 subdir=os.path.join(
                                     TEST_DAG_FOLDER, 'test_scheduler_dags.py'),
                                 num_runs=1)

        scheduler.run()

        # zero tasks ran
        dag_id = 'test_start_date_scheduling'
        with create_session() as session:
            self.assertEqual(
                session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).count(), 0)

    def test_scheduler_verify_pool_full(self):
        """
        Test task instances not queued when pool is full
        """
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

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob(executor=self.null_exec)

        # Create 2 dagruns, which will create 2 task instances.
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        self.assertEqual(dr.execution_date, DEFAULT_DATE)
        dr = dag_file_processor.create_dag_run(dag)
        self.assertIsNotNone(dr)
        dag_runs = DagRun.find(dag_id="test_scheduler_verify_pool_full")
        task_instances_list = dag_file_processor._process_task_instances(dag, dag_runs=dag_runs)
        self.assertEqual(len(task_instances_list), 2)
        dagbag = self._make_simple_dag_bag([dag])

        # Recreated part of the scheduler here, to kick off tasks -> executor
        for ti_key in task_instances_list:
            task = dag.get_task(ti_key[1])
            ti = TaskInstance(task, ti_key[2])
            # Task starts out in the scheduled state. All tasks in the
            # scheduled state will be sent to the executor
            ti.state = State.SCHEDULED

            # Also save this task instance to the DB.
            session.merge(ti)
        session.commit()

        self.assertEqual(len(scheduler.executor.queued_tasks), 0, "Check test pre-condition")
        scheduler._execute_task_instances(dagbag, session=session)

        self.assertEqual(len(scheduler.executor.queued_tasks), 1)

    def test_scheduler_verify_pool_full_2_slots_per_task(self):
        """
        Test task instances not queued when pool is full.

        Variation with non-default pool_slots
        """
        dag = DAG(
            dag_id='test_scheduler_verify_pool_full_2_slots_per_task',
            start_date=DEFAULT_DATE)

        DummyOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_pool_full_2_slots_per_task',
            pool_slots=2,
        )

        session = settings.Session()
        pool = Pool(pool='test_scheduler_verify_pool_full_2_slots_per_task', slots=6)
        session.add(pool)
        orm_dag = DagModel(dag_id=dag.dag_id)
        orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob(executor=self.null_exec)

        # Create 5 dagruns, which will create 5 task instances.
        for _ in range(5):
            dag_file_processor.create_dag_run(dag)
        dag_runs = DagRun.find(dag_id="test_scheduler_verify_pool_full_2_slots_per_task")
        task_instances_list = dag_file_processor._process_task_instances(dag, dag_runs=dag_runs)
        self.assertEqual(len(task_instances_list), 5)
        dagbag = self._make_simple_dag_bag([dag])

        # Recreated part of the scheduler here, to kick off tasks -> executor
        for ti_key in task_instances_list:
            task = dag.get_task(ti_key[1])
            ti = TaskInstance(task, ti_key[2])
            # Task starts out in the scheduled state. All tasks in the
            # scheduled state will be sent to the executor
            ti.state = State.SCHEDULED

            # Also save this task instance to the DB.
            session.merge(ti)
        session.commit()

        self.assertEqual(len(scheduler.executor.queued_tasks), 0, "Check test pre-condition")
        scheduler._execute_task_instances(dagbag, session=session)

        # As tasks require 2 slots, only 3 can fit into 6 available
        self.assertEqual(len(scheduler.executor.queued_tasks), 3)

    def test_scheduler_verify_priority_and_slots(self):
        """
        Test task instances with higher priority are not queued
        when pool does not have enough slots.

        Though tasks with lower priority might be executed.
        """
        dag = DAG(
            dag_id='test_scheduler_verify_priority_and_slots',
            start_date=DEFAULT_DATE)

        # Medium priority, not enough slots
        DummyOperator(
            task_id='test_scheduler_verify_priority_and_slots_t0',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_priority_and_slots',
            pool_slots=2,
            priority_weight=2,
        )
        # High priority, occupies first slot
        DummyOperator(
            task_id='test_scheduler_verify_priority_and_slots_t1',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_priority_and_slots',
            pool_slots=1,
            priority_weight=3,
        )
        # Low priority, occupies second slot
        DummyOperator(
            task_id='test_scheduler_verify_priority_and_slots_t2',
            dag=dag,
            owner='airflow',
            pool='test_scheduler_verify_priority_and_slots',
            pool_slots=1,
            priority_weight=1,
        )

        session = settings.Session()
        pool = Pool(pool='test_scheduler_verify_priority_and_slots', slots=2)
        session.add(pool)
        orm_dag = DagModel(dag_id=dag.dag_id)
        orm_dag.is_paused = False
        session.merge(orm_dag)
        session.commit()

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob(executor=self.null_exec)

        dag_file_processor.create_dag_run(dag)
        dag_runs = DagRun.find(dag_id="test_scheduler_verify_priority_and_slots")
        task_instances_list = dag_file_processor._process_task_instances(dag, dag_runs=dag_runs)
        self.assertEqual(len(task_instances_list), 3)
        dagbag = self._make_simple_dag_bag([dag])

        # Recreated part of the scheduler here, to kick off tasks -> executor
        for ti_key in task_instances_list:
            task = dag.get_task(ti_key[1])
            ti = TaskInstance(task, ti_key[2])
            # Task starts out in the scheduled state. All tasks in the
            # scheduled state will be sent to the executor
            ti.state = State.SCHEDULED

            # Also save this task instance to the DB.
            session.merge(ti)
        session.commit()

        self.assertEqual(len(scheduler.executor.queued_tasks), 0, "Check test pre-condition")
        scheduler._execute_task_instances(dagbag, session=session)

        # Only second and third
        self.assertEqual(len(scheduler.executor.queued_tasks), 2)

        ti0 = session.query(TaskInstance)\
            .filter(TaskInstance.task_id == 'test_scheduler_verify_priority_and_slots_t0').first()
        self.assertEqual(ti0.state, State.SCHEDULED)

        ti1 = session.query(TaskInstance)\
            .filter(TaskInstance.task_id == 'test_scheduler_verify_priority_and_slots_t1').first()
        self.assertEqual(ti1.state, State.QUEUED)

        ti2 = session.query(TaskInstance)\
            .filter(TaskInstance.task_id == 'test_scheduler_verify_priority_and_slots_t2').first()
        self.assertEqual(ti2.state, State.QUEUED)

    def test_scheduler_reschedule(self):
        """
        Checks if tasks that are not taken up by the executor
        get rescheduled
        """
        executor = MockExecutor(do_update=False)
        dagbag = DagBag(dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"))
        dagbag.dags.clear()

        dag = DAG(
            dag_id='test_scheduler_reschedule',
            start_date=DEFAULT_DATE)
        dummy_task = BashOperator(
            task_id='dummy',
            dag=dag,
            owner='airflow',
            bash_command='echo 1',
        )

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))
        dag.clear()
        dag.is_subdag = False

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            orm_dag.is_paused = False
            session.merge(orm_dag)

        dagbag.bag_dag(dag=dag, root_dag=dag)

        @mock.patch('airflow.jobs.scheduler_job.DagBag', return_value=dagbag)
        def do_schedule(mock_dagbag):
            # Use a empty file since the above mock will return the
            # expected DAGs. Also specify only a single file so that it doesn't
            # try to schedule the above DAG repeatedly.
            with conf_vars({('core', 'mp_start_method'): 'fork'}):
                scheduler = SchedulerJob(num_runs=1,
                                         executor=executor,
                                         subdir=os.path.join(settings.DAGS_FOLDER,
                                                             "no_dags.py"))
                scheduler.heartrate = 0
                scheduler.run()

        do_schedule()  # pylint: disable=no-value-for-parameter
        with create_session() as session:
            ti = session.query(TaskInstance).filter(TaskInstance.dag_id == dag.dag_id,
                                                    TaskInstance.task_id == dummy_task.task_id).first()
        self.assertEqual(0, len(executor.queued_tasks))
        self.assertEqual(State.SCHEDULED, ti.state)

        executor.do_update = True
        do_schedule()  # pylint: disable=no-value-for-parameter
        self.assertEqual(0, len(executor.queued_tasks))
        ti.refresh_from_db()
        self.assertEqual(State.SUCCESS, ti.state)

    @pytest.mark.quarantined
    def test_retry_still_in_executor(self):
        """
        Checks if the scheduler does not put a task in limbo, when a task is retried
        but is still present in the executor.
        """
        executor = MockExecutor(do_update=False)
        dagbag = DagBag(dag_folder=os.path.join(settings.DAGS_FOLDER, "no_dags.py"))
        dagbag.dags.clear()

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

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            orm_dag.is_paused = False
            session.merge(orm_dag)

        dagbag.bag_dag(dag=dag, root_dag=dag)

        @mock.patch('airflow.jobs.scheduler_job.DagBag', return_value=dagbag)
        def do_schedule(mock_dagbag):
            # Use a empty file since the above mock will return the
            # expected DAGs. Also specify only a single file so that it doesn't
            # try to schedule the above DAG repeatedly.
            scheduler = SchedulerJob(num_runs=1,
                                     executor=executor,
                                     subdir=os.path.join(settings.DAGS_FOLDER,
                                                         "no_dags.py"))
            scheduler.heartrate = 0
            scheduler.run()

        do_schedule()  # pylint: disable=no-value-for-parameter
        with create_session() as session:
            ti = session.query(TaskInstance).filter(TaskInstance.dag_id == 'test_retry_still_in_executor',
                                                    TaskInstance.task_id == 'test_retry_handling_op').first()
        ti.task = dag_task1

        # Nothing should be left in the queued_tasks as we don't do update in MockExecutor yet,
        # and the queued_tasks will be cleared by scheduler job.
        self.assertEqual(0, len(executor.queued_tasks))

        def run_with_error(ti, ignore_ti_state=False):
            try:
                ti.run(ignore_ti_state=ignore_ti_state)
            except AirflowException:
                pass

        self.assertEqual(ti.try_number, 1)
        # At this point, scheduler has tried to schedule the task once and
        # heartbeated the executor once, which moved the state of the task from
        # SCHEDULED to QUEUED and then to SCHEDULED, to fail the task execution
        # we need to ignore the TaskInstance state as SCHEDULED is not a valid state to start
        # executing task.
        run_with_error(ti, ignore_ti_state=True)
        self.assertEqual(ti.state, State.UP_FOR_RETRY)
        self.assertEqual(ti.try_number, 2)

        with create_session() as session:
            ti.refresh_from_db(lock_for_update=True, session=session)
            ti.state = State.SCHEDULED
            session.merge(ti)

        # do schedule
        do_schedule()  # pylint: disable=no-value-for-parameter
        # MockExecutor is not aware of the TaskInstance since we don't do update yet
        # and no trace of this TaskInstance will be left in the executor.
        self.assertFalse(executor.has_task(ti))
        self.assertEqual(ti.state, State.SCHEDULED)

        # To verify that task does get re-queued.
        executor.do_update = True
        do_schedule()  # pylint: disable=no-value-for-parameter
        ti.refresh_from_db()
        self.assertEqual(ti.state, State.SUCCESS)

    @pytest.mark.quarantined
    @pytest.mark.xfail(condition=True, reason="This test is failing!")
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
        ti = session.query(TaskInstance).filter(TaskInstance.dag_id == dag.dag_id,
                                                TaskInstance.task_id == dag_task1.task_id).first()

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
            tis = session.query(TaskInstance).filter(TaskInstance.dag_id == dag_id).all()
            # Since this dag has no end date, and there's a chance that we'll
            # start a and finish two dag parsing processes twice in one loop!
            self.assertGreaterEqual(
                len(tis), 1,
                repr(tis))

    def test_dag_get_active_runs(self):
        """
        Test to check that a DAG returns its active runs
        """

        now = timezone.utcnow()
        six_hours_ago_to_the_hour = \
            (now - datetime.timedelta(hours=6)).replace(minute=0, second=0, microsecond=0)

        start_date = six_hours_ago_to_the_hour
        dag_name1 = 'get_active_runs_test'

        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'start_date': start_date

        }
        dag1 = DAG(dag_name1,
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

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag1.clear()

        dr = dag_file_processor.create_dag_run(dag1)

        # We had better get a dag run
        self.assertIsNotNone(dr)

        execution_date = dr.execution_date

        running_dates = dag1.get_active_runs()

        try:
            running_date = running_dates[0]
        except Exception:  # pylint: disable=broad-except
            running_date = 'Except'

        self.assertEqual(execution_date, running_date, 'Running Date must match Execution Date')

    def test_add_unparseable_file_before_sched_start_creates_import_error(self):
        dags_folder = mkdtemp()
        try:
            with env_vars({('core', 'dags_folder'): dags_folder}):
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
        ignored_files = {
            'no_dags.py',
            'test_invalid_cron.py',
            'test_zip_invalid_cron.zip',
            'test_ignore_this.py',
        }
        for root, _, files in os.walk(TEST_DAG_FOLDER):  # pylint: disable=too-many-nested-blocks
            for file_name in files:
                if file_name.endswith('.py') or file_name.endswith('.zip'):
                    if file_name not in ignored_files:
                        expected_files.add(
                            '{}/{}'.format(root, file_name))
        for file_path in list_py_file_paths(TEST_DAG_FOLDER, include_examples=False):
            detected_files.add(file_path)
        self.assertEqual(detected_files, expected_files)

        ignored_files = {
            'helper.py',
        }
        example_dag_folder = airflow.example_dags.__path__[0]
        for root, _, files in os.walk(example_dag_folder):  # pylint: disable=too-many-nested-blocks
            for file_name in files:
                if file_name.endswith('.py') or file_name.endswith('.zip'):
                    if file_name not in ['__init__.py'] and file_name not in ignored_files:
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

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag, session=session)
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

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag, session=session)
        ti = dr1.get_task_instances(session=session)[0]
        ti.state = State.SCHEDULED
        dr1.state = State.RUNNING
        dr1.run_type = DagRunType.BACKFILL_JOB.value
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
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dr1 = dag_file_processor.create_dag_run(dag)
        dr2 = dag_file_processor.create_dag_run(dag)
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

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
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

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
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

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        scheduler = SchedulerJob()
        session = settings.Session()

        dr1 = dag_file_processor.create_dag_run(dag)
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
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dr1 = dag_file_processor.create_dag_run(dag)
        dr2 = dag_file_processor.create_dag_run(dag)
        dr1.state = State.RUNNING
        dr2.state = State.SUCCESS
        session.merge(dr1)
        session.merge(dr2)
        session.commit()

        # create taskinstances and set states
        dr1_tis = []
        dr2_tis = []
        for i, (task, state) in enumerate(zip(tasks, states)):
            ti1 = TaskInstance(task, dr1.execution_date)
            ti2 = TaskInstance(task, dr2.execution_date)
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


def test_task_with_upstream_skip_process_task_instances():
    """
    Test if _process_task_instances puts a task instance into SKIPPED state if any of its
    upstream tasks are skipped according to TriggerRuleDep.
    """
    clear_db_runs()
    with DAG(
        dag_id='test_task_with_upstream_skip_dag',
        start_date=DEFAULT_DATE,
        schedule_interval=None
    ) as dag:
        dummy1 = DummyOperator(task_id='dummy1')
        dummy2 = DummyOperator(task_id="dummy2")
        dummy3 = DummyOperator(task_id="dummy3")
        [dummy1, dummy2] >> dummy3

    dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
    dag.clear()
    dr = dag.create_dagrun(run_type=DagRunType.MANUAL,
                           state=State.RUNNING,
                           execution_date=DEFAULT_DATE)
    assert dr is not None

    with create_session() as session:
        tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
        # Set dummy1 to skipped and dummy2 to success. dummy3 remains as none.
        tis[dummy1.task_id].state = State.SKIPPED
        tis[dummy2.task_id].state = State.SUCCESS
        assert tis[dummy3.task_id].state == State.NONE

    dag_runs = DagRun.find(dag_id='test_task_with_upstream_skip_dag')
    dag_file_processor._process_task_instances(dag, dag_runs=dag_runs)

    with create_session() as session:
        tis = {ti.task_id: ti for ti in dr.get_task_instances(session=session)}
        assert tis[dummy1.task_id].state == State.SKIPPED
        assert tis[dummy2.task_id].state == State.SUCCESS
        # dummy3 should be skipped because dummy1 is skipped.
        assert tis[dummy3.task_id].state == State.SKIPPED


class TestSchedulerJobQueriesCount(unittest.TestCase):
    """
    These tests are designed to detect changes in the number of queries for
    different DAG files. These tests allow easy detection when a change is
    made that affects the performance of the SchedulerJob.
    """

    def setUp(self) -> None:
        clear_db_runs()
        clear_db_pools()
        clear_db_dags()
        clear_db_sla_miss()
        clear_db_errors()

    @parameterized.expand(
        [
            # pylint: disable=bad-whitespace
            # expected, dag_count, task_count
            # One DAG with one task per DAG file
            (13, 1, 1),  # noqa
            # One DAG with five tasks per DAG  file
            (21, 1, 5),  # noqa
            # 10 DAGs with 10 tasks per DAG file
            (77, 10, 10),  # noqa
        ]
    )
    def test_execute_queries_count_with_harvested_dags(self, expected_query_count, dag_count, task_count):
        with mock.patch.dict("os.environ", {
            "PERF_DAGS_COUNT": str(dag_count),
            "PERF_TASKS_COUNT": str(task_count),
            "PERF_START_AGO": "1d",
            "PERF_SCHEDULE_INTERVAL": "30m",
            "PERF_SHAPE": "no_structure",
        }), conf_vars({
            ('scheduler', 'use_job_schedule'): 'True',
            ('core', 'load_examples'): 'False',
        }):

            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE, include_examples=False)
            for i, dag in enumerate(dagbag.dags.values()):
                dr = dag.create_dagrun(state=State.RUNNING, run_id=f"{DagRunType.MANUAL.value}__{i}")
                for ti in dr.get_task_instances():
                    ti.set_state(state=State.SCHEDULED)

            mock_agent = mock.MagicMock()
            mock_agent.harvest_simple_dags.return_value = [SimpleDag(d) for d in dagbag.dags.values()]

            job = SchedulerJob(subdir=PERF_DAGS_FOLDER)
            job.executor = MockExecutor()
            job.heartbeat = mock.MagicMock()
            job.processor_agent = mock_agent

            with assert_queries_count(expected_query_count):
                job._run_scheduler_loop()

    @parameterized.expand(
        [
            # pylint: disable=bad-whitespace
            # expected, dag_count, task_count
            # One DAG with one task per DAG file
            (2, 1, 1),  # noqa
            # One DAG with five tasks per DAG  file
            (2, 1, 5),  # noqa
            # 10 DAGs with 10 tasks per DAG file
            (2, 10, 10),  # noqa
        ]
    )
    def test_execute_queries_count_no_harvested_dags(self, expected_query_count, dag_count, task_count):
        with mock.patch.dict("os.environ", {
            "PERF_DAGS_COUNT": str(dag_count),
            "PERF_TASKS_COUNT": str(task_count),
            "PERF_START_AGO": "1d",
            "PERF_SCHEDULE_INTERVAL": "30m",
            "PERF_SHAPE": "no_structure",
        }), conf_vars({
            ('scheduler', 'use_job_schedule'): 'True',
            ('core', 'load_examples'): 'False',
        }):

            dagbag = DagBag(dag_folder=ELASTIC_DAG_FILE, include_examples=False)
            for i, dag in enumerate(dagbag.dags.values()):
                dr = dag.create_dagrun(state=State.RUNNING, run_id=f"{DagRunType.MANUAL.value}__{i}")
                for ti in dr.get_task_instances():
                    ti.set_state(state=State.SCHEDULED)

            mock_agent = mock.MagicMock()
            mock_agent.harvest_simple_dags.return_value = []

            job = SchedulerJob(subdir=PERF_DAGS_FOLDER)
            job.executor = MockExecutor()
            job.heartbeat = mock.MagicMock()
            job.processor_agent = mock_agent

            with assert_queries_count(expected_query_count):
                job._run_scheduler_loop()
