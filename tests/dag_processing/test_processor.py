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
# pylint: disable=attribute-defined-outside-init
import datetime
import os
import unittest
from datetime import timedelta
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from parameterized import parameterized

from airflow import settings
from airflow.configuration import conf
from airflow.dag_processing.processor import DagFileProcessor
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.models import DAG, DagBag, DagModel, SlaMiss, TaskInstance
from airflow.models.dagrun import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import SimpleTaskInstance
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils import timezone
from airflow.utils.callback_requests import TaskCallbackRequest
from airflow.utils.dates import days_ago
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from tests.test_utils.config import conf_vars, env_vars
from tests.test_utils.db import (
    clear_db_dags,
    clear_db_import_errors,
    clear_db_jobs,
    clear_db_pools,
    clear_db_runs,
    clear_db_serialized_dags,
    clear_db_sla_miss,
)
from tests.test_utils.mock_executor import MockExecutor

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


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
        clear_db_import_errors()
        clear_db_jobs()
        clear_db_serialized_dags()

    def setUp(self):
        self.clean_db()

        # Speed up some tests by not running the tasks, just look at what we
        # enqueue!
        self.null_exec = MockExecutor()
        self.scheduler_job = None

    def tearDown(self) -> None:
        if self.scheduler_job and self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
            self.scheduler_job = None
        self.clean_db()

    def create_test_dag(self, start_date=DEFAULT_DATE, end_date=DEFAULT_DATE + timedelta(hours=1), **kwargs):
        dag = DAG(
            dag_id='test_scheduler_reschedule',
            start_date=start_date,
            # Make sure it only creates a single DAG Run
            end_date=end_date,
        )
        dag.clear()
        dag.is_subdag = False
        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id, is_paused=False)
            session.merge(orm_dag)
            session.commit()
        return dag

    @classmethod
    def setUpClass(cls):
        # Ensure the DAGs we are looking at from the DB are up-to-date
        non_serialized_dagbag = DagBag(read_dags_from_db=False, include_examples=False)
        non_serialized_dagbag.sync_to_db()
        cls.dagbag = DagBag(read_dags_from_db=True)

    def test_dag_file_processor_sla_miss_callback(self):
        """
        Test that the dag file processor calls the sla miss callback
        """
        session = settings.Session()

        sla_callback = MagicMock()

        # Create dag with a start of 1 day ago, but an sla of 0
        # so we'll already have an sla_miss on the books.
        test_start_date = days_ago(1)
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta()},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

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
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': None},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        dag_file_processor.manage_slas(dag=dag, session=session)
        sla_callback.assert_not_called()

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
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow')

        # Create a TaskInstance for two days ago
        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(
            SlaMiss(
                task_id='dummy',
                dag_id='test_sla_miss',
                execution_date=test_start_date,
                email_sent=False,
                notification_sent=True,
            )
        )

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
        dag = DAG(
            dag_id='test_sla_miss',
            sla_miss_callback=sla_callback,
            default_args={'start_date': test_start_date},
        )

        task = DummyOperator(task_id='dummy', dag=dag, owner='airflow', sla=datetime.timedelta(hours=1))

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        # Now call manage_slas and see if the sla_miss callback gets called
        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)
        assert sla_callback.called
        mock_log.exception.assert_called_once_with(
            'Could not call sla_miss_callback for DAG %s', 'test_sla_miss'
        )

    @mock.patch('airflow.dag_processing.processor.send_email')
    def test_dag_file_processor_only_collect_emails_from_sla_missed_tasks(self, mock_send_email):
        session = settings.Session()

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        email1 = 'test1@test.com'
        task = DummyOperator(
            task_id='sla_missed', dag=dag, owner='airflow', email=email1, sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        email2 = 'test2@test.com'
        DummyOperator(task_id='sla_not_missed', dag=dag, owner='airflow', email=email2)

        session.merge(SlaMiss(task_id='sla_missed', dag_id='test_sla_miss', execution_date=test_start_date))

        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())

        dag_file_processor.manage_slas(dag=dag, session=session)

        assert len(mock_send_email.call_args_list) == 1

        send_email_to = mock_send_email.call_args_list[0][0][0]
        assert email1 in send_email_to
        assert email2 not in send_email_to

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
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(
            task_id='dummy', dag=dag, owner='airflow', email='test@test.com', sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(SlaMiss(task_id='dummy', dag_id='test_sla_miss', execution_date=test_start_date))

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)

        dag_file_processor.manage_slas(dag=dag, session=session)
        mock_log.exception.assert_called_once_with(
            'Could not send SLA Miss email notification for DAG %s', 'test_sla_miss'
        )
        mock_stats_incr.assert_called_once_with('sla_email_notification_failure')

    def test_dag_file_processor_sla_miss_deleted_task(self):
        """
        Test that the dag file processor will not crash when trying to send
        sla miss notification for a deleted task
        """
        session = settings.Session()

        test_start_date = days_ago(2)
        dag = DAG(
            dag_id='test_sla_miss',
            default_args={'start_date': test_start_date, 'sla': datetime.timedelta(days=1)},
        )

        task = DummyOperator(
            task_id='dummy', dag=dag, owner='airflow', email='test@test.com', sla=datetime.timedelta(hours=1)
        )

        session.merge(TaskInstance(task=task, execution_date=test_start_date, state='Success'))

        # Create an SlaMiss where notification was sent, but email was not
        session.merge(
            SlaMiss(task_id='dummy_deleted', dag_id='test_sla_miss', execution_date=test_start_date)
        )

        mock_log = mock.MagicMock()
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock_log)
        dag_file_processor.manage_slas(dag=dag, session=session)

    @parameterized.expand(
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
        ]
    )
    def test_dag_file_processor_process_task_instances(self, state, start_date, end_date):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        dag = DAG(dag_id='test_scheduler_process_execute_task', start_date=DEFAULT_DATE)
        BashOperator(task_id='dummy', dag=dag, owner='airflow', bash_command='echo hi')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        dag.clear()
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        assert dr is not None

        with create_session() as session:
            ti = dr.get_task_instances(session=session)[0]
            ti.state = state
            ti.start_date = start_date
            ti.end_date = end_date

            count = self.scheduler_job._schedule_dag_run(dr, set(), session)
            assert count == 1

            session.refresh(ti)
            assert ti.state == State.SCHEDULED

    @parameterized.expand(
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
        ]
    )
    def test_dag_file_processor_process_task_instances_with_task_concurrency(
        self,
        state,
        start_date,
        end_date,
    ):
        """
        Test if _process_task_instances puts the right task instances into the
        mock_list.
        """
        dag = DAG(dag_id='test_scheduler_process_execute_task_with_task_concurrency', start_date=DEFAULT_DATE)
        BashOperator(task_id='dummy', task_concurrency=2, dag=dag, owner='airflow', bash_command='echo Hi')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        dag.clear()
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        assert dr is not None

        with create_session() as session:
            ti = dr.get_task_instances(session=session)[0]
            ti.state = state
            ti.start_date = start_date
            ti.end_date = end_date

            count = self.scheduler_job._schedule_dag_run(dr, set(), session)
            assert count == 1

            session.refresh(ti)
            assert ti.state == State.SCHEDULED

    @parameterized.expand(
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
        ]
    )
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
        BashOperator(task_id='dummy1', dag=dag, owner='airflow', bash_command='echo hi')
        BashOperator(task_id='dummy2', dag=dag, owner='airflow', bash_command='echo hi')

        with create_session() as session:
            orm_dag = DagModel(dag_id=dag.dag_id)
            session.merge(orm_dag)

        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        dag.clear()
        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        assert dr is not None

        with create_session() as session:
            tis = dr.get_task_instances(session=session)
            for ti in tis:
                ti.state = state
                ti.start_date = start_date
                ti.end_date = end_date

            count = self.scheduler_job._schedule_dag_run(dr, set(), session)
            assert count == 2

            session.refresh(tis[0])
            session.refresh(tis[1])
            assert tis[0].state == State.SCHEDULED
            assert tis[1].state == State.SCHEDULED

    def test_scheduler_job_add_new_task(self):
        """
        Test if a task instance will be added if the dag is updated
        """
        dag = DAG(dag_id='test_scheduler_add_new_task', start_date=DEFAULT_DATE)
        BashOperator(task_id='dummy', dag=dag, owner='airflow', bash_command='echo test')

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)

        # Since we don't want to store the code for the DAG defined in this file
        with mock.patch.object(settings, "STORE_DAG_CODE", False):
            self.scheduler_job.dagbag.sync_to_db()

        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        assert orm_dag is not None

        if self.scheduler_job.processor_agent:
            self.scheduler_job.processor_agent.end()
        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        dag = self.scheduler_job.dagbag.get_dag('test_scheduler_add_new_task', session=session)
        self.scheduler_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        tis = dr.get_task_instances()
        assert len(tis) == 1

        BashOperator(task_id='dummy2', dag=dag, owner='airflow', bash_command='echo test')
        SerializedDagModel.write_dag(dag=dag)

        scheduled_tis = self.scheduler_job._schedule_dag_run(dr, set(), session)
        session.flush()
        assert scheduled_tis == 2

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        tis = dr.get_task_instances()
        assert len(tis) == 2

    def test_runs_respected_after_clear(self):
        """
        Test if _process_task_instances only schedules ti's up to max_active_runs
        (related to issue AIRFLOW-137)
        """
        dag = DAG(dag_id='test_scheduler_max_active_runs_respected_after_clear', start_date=DEFAULT_DATE)
        dag.max_active_runs = 3

        BashOperator(task_id='dummy', dag=dag, owner='airflow', bash_command='echo Hi')

        session = settings.Session()
        orm_dag = DagModel(dag_id=dag.dag_id)
        session.merge(orm_dag)
        session.commit()
        session.close()
        dag = SerializedDAG.from_dict(SerializedDAG.to_dict(dag))

        self.scheduler_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job.processor_agent = mock.MagicMock()
        self.scheduler_job.dagbag.bag_dag(dag, root_dag=dag)
        dag.clear()

        date = DEFAULT_DATE
        dr1 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )
        date = dag.following_schedule(date)
        dr2 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )
        date = dag.following_schedule(date)
        dr3 = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            execution_date=date,
            state=State.RUNNING,
        )

        # First create up to 3 dagruns in RUNNING state.
        assert dr1 is not None
        assert dr2 is not None
        assert dr3 is not None
        assert len(DagRun.find(dag_id=dag.dag_id, state=State.RUNNING, session=session)) == 3

        # Reduce max_active_runs to 1
        dag.max_active_runs = 1

        # and schedule them in, so we can check how many
        # tasks are put on the task_instances_list (should be one, not 3)
        with create_session() as session:
            num_scheduled = self.scheduler_job._schedule_dag_run(dr1, set(), session)
            assert num_scheduled == 1
            num_scheduled = self.scheduler_job._schedule_dag_run(dr2, {dr1.execution_date}, session)
            assert num_scheduled == 0
            num_scheduled = self.scheduler_job._schedule_dag_run(dr3, {dr1.execution_date}, session)
            assert num_scheduled == 0

    @patch.object(TaskInstance, 'handle_failure_with_callback')
    def test_execute_on_failure_callbacks(self, mock_ti_handle_failure):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True, read_dags_from_db=False)
        dag_file_processor = DagFileProcessor(dag_ids=[], log=mock.MagicMock())
        with create_session() as session:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag('example_branch_operator')
            task = dag.get_task(task_id='run_this_first')

            ti = TaskInstance(task, DEFAULT_DATE, State.RUNNING)

            session.add(ti)
            session.commit()

            requests = [
                TaskCallbackRequest(
                    full_filepath="A", simple_task_instance=SimpleTaskInstance(ti), msg="Message"
                )
            ]
            dag_file_processor.execute_callbacks(dagbag, requests)
            mock_ti_handle_failure.assert_called_once_with(
                error="Message",
                test_mode=conf.getboolean('core', 'unit_test_mode'),
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
                TaskCallbackRequest(
                    full_filepath=dag.full_filepath,
                    simple_task_instance=SimpleTaskInstance(ti),
                    msg="Message",
                )
            ]
            callback_file.close()

            with mock.patch.dict("os.environ", {"AIRFLOW_CALLBACK_FILE": callback_file.name}):
                dag_file_processor.process_file(dag_file, requests)
            with open(callback_file.name) as callback_file2:
                content = callback_file2.read()
            assert "Callback fired" == content
            os.remove(callback_file.name)

    def test_should_mark_dummy_task_as_success(self):
        dag_file = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), '../dags/test_only_dummy_tasks.py'
        )

        # Write DAGs to dag and serialized_dag table
        dagbag = DagBag(dag_folder=dag_file, include_examples=False, read_dags_from_db=False)
        dagbag.sync_to_db()

        self.scheduler_job_job = SchedulerJob(subdir=os.devnull)
        self.scheduler_job_job.processor_agent = mock.MagicMock()
        dag = self.scheduler_job_job.dagbag.get_dag("test_only_dummy_tasks")

        # Create DagRun
        session = settings.Session()
        orm_dag = session.query(DagModel).get(dag.dag_id)
        self.scheduler_job_job._create_dag_runs([orm_dag], session)

        drs = DagRun.find(dag_id=dag.dag_id, session=session)
        assert len(drs) == 1
        dr = drs[0]

        # Schedule TaskInstances
        self.scheduler_job_job._schedule_dag_run(dr, {}, session)
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        dags = self.scheduler_job_job.dagbag.dags.values()
        assert ['test_only_dummy_tasks'] == [dag.dag_id for dag in dags]
        assert 5 == len(tis)
        assert {
            ('test_task_a', 'success'),
            ('test_task_b', None),
            ('test_task_c', 'success'),
            ('test_task_on_execute', 'scheduled'),
            ('test_task_on_success', 'scheduled'),
        } == {(ti.task_id, ti.state) for ti in tis}
        for state, start_date, end_date, duration in [
            (ti.state, ti.start_date, ti.end_date, ti.duration) for ti in tis
        ]:
            if state == 'success':
                assert start_date is not None
                assert end_date is not None
                assert 0.0 == duration
            else:
                assert start_date is None
                assert end_date is None
                assert duration is None

        self.scheduler_job_job._schedule_dag_run(dr, {}, session)
        with create_session() as session:
            tis = session.query(TaskInstance).all()

        assert 5 == len(tis)
        assert {
            ('test_task_a', 'success'),
            ('test_task_b', 'success'),
            ('test_task_c', 'success'),
            ('test_task_on_execute', 'scheduled'),
            ('test_task_on_success', 'scheduled'),
        } == {(ti.task_id, ti.state) for ti in tis}
        for state, start_date, end_date, duration in [
            (ti.state, ti.start_date, ti.end_date, ti.duration) for ti in tis
        ]:
            if state == 'success':
                assert start_date is not None
                assert end_date is not None
                assert 0.0 == duration
            else:
                assert start_date is None
                assert end_date is None
                assert duration is None
