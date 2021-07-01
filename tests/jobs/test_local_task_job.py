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
import os
import signal
import time
import unittest
import uuid
from multiprocessing import Lock, Value
from unittest import mock
from unittest.mock import patch

import pytest
from parameterized import parameterized

from airflow import settings
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.jobs.local_task_job import LocalTaskJob
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.models.dag import DAG, DagModel
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.task.task_runner.standard_task_runner import StandardTaskRunner
from airflow.utils import timezone
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from airflow.utils.types import DagRunType
from tests.test_utils import db
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars
from tests.test_utils.mock_executor import MockExecutor

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
TEST_DAG_FOLDER = os.environ['AIRFLOW__CORE__DAGS_FOLDER']


class TestLocalTaskJob(unittest.TestCase):
    def setUp(self):
        db.clear_db_dags()
        db.clear_db_jobs()
        db.clear_db_runs()
        db.clear_db_task_fail()
        patcher = patch('airflow.jobs.base_job.sleep')
        self.addCleanup(patcher.stop)
        self.mock_base_job_sleep = patcher.start()

    def tearDown(self) -> None:
        db.clear_db_dags()
        db.clear_db_jobs()
        db.clear_db_runs()
        db.clear_db_task_fail()

    def validate_ti_states(self, dag_run, ti_state_mapping, error_message):
        for task_id, expected_state in ti_state_mapping.items():
            task_instance = dag_run.get_task_instance(task_id=task_id)
            task_instance.refresh_from_db()
            assert task_instance.state == expected_state, error_message

    def test_localtaskjob_essential_attr(self):
        """
        Check whether essential attributes
        of LocalTaskJob can be assigned with
        proper values without intervention
        """
        dag = DAG(
            'test_localtaskjob_essential_attr', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}
        )

        with dag:
            op1 = DummyOperator(task_id='op1')

        dag.clear()
        dr = dag.create_dagrun(
            run_id="test", state=State.SUCCESS, execution_date=DEFAULT_DATE, start_date=DEFAULT_DATE
        )
        ti = dr.get_task_instance(task_id=op1.task_id)

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())

        essential_attr = ["dag_id", "job_type", "start_date", "hostname"]

        check_result_1 = [hasattr(job1, attr) for attr in essential_attr]
        assert all(check_result_1)

        check_result_2 = [getattr(job1, attr) is not None for attr in essential_attr]
        assert all(check_result_2)

    def test_localtaskjob_heartbeat(self):
        session = settings.Session()
        dag = DAG('test_localtaskjob_heartbeat', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        with dag:
            op1 = DummyOperator(task_id='op1')

        dag.clear()
        dr = dag.create_dagrun(
            run_id="test",
            state=State.SUCCESS,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.RUNNING
        ti.hostname = "blablabla"
        session.commit()

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        ti.task = op1
        ti.refresh_from_task(op1)
        job1.task_runner = StandardTaskRunner(job1)
        job1.task_runner.process = mock.Mock()
        with pytest.raises(AirflowException):
            job1.heartbeat_callback()

        job1.task_runner.process.pid = 1
        ti.state = State.RUNNING
        ti.hostname = get_hostname()
        ti.pid = 1
        session.merge(ti)
        session.commit()
        assert ti.pid != os.getpid()
        job1.heartbeat_callback(session=None)

        job1.task_runner.process.pid = 2
        with pytest.raises(AirflowException):
            job1.heartbeat_callback()

    def test_heartbeat_failed_fast(self):
        """
        Test that task heartbeat will sleep when it fails fast
        """
        self.mock_base_job_sleep.side_effect = time.sleep

        with create_session() as session:
            dagbag = DagBag(
                dag_folder=TEST_DAG_FOLDER,
                include_examples=False,
            )
            dag_id = 'test_heartbeat_failed_fast'
            task_id = 'test_heartbeat_failed_fast_op'
            dag = dagbag.get_dag(dag_id)
            task = dag.get_task(task_id)

            dag.create_dagrun(
                run_id="test_heartbeat_failed_fast_run",
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                start_date=DEFAULT_DATE,
                session=session,
            )
            ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
            ti.refresh_from_db()
            ti.state = State.RUNNING
            ti.hostname = get_hostname()
            ti.pid = 1
            session.commit()

            job = LocalTaskJob(task_instance=ti, executor=MockExecutor(do_update=False))
            job.heartrate = 2
            heartbeat_records = []
            job.heartbeat_callback = lambda session: heartbeat_records.append(job.latest_heartbeat)
            job._execute()
            assert len(heartbeat_records) > 2
            for i in range(1, len(heartbeat_records)):
                time1 = heartbeat_records[i - 1]
                time2 = heartbeat_records[i]
                # Assert that difference small enough
                delta = (time2 - time1).total_seconds()
                assert abs(delta - job.heartrate) < 0.5

    @pytest.mark.quarantined
    def test_mark_success_no_kill(self):
        """
        Test that ensures that mark_success in the UI doesn't cause
        the task to fail, and that the task exits
        """
        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get('test_mark_success')
        task = dag.get_task('task1')

        session = settings.Session()

        dag.clear()
        dag.create_dagrun(
            run_id="test",
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
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
        assert State.RUNNING == ti.state
        ti.state = State.SUCCESS
        session.merge(ti)
        session.commit()

        process.join(timeout=10)
        assert not process.is_alive()
        ti.refresh_from_db()
        assert State.SUCCESS == ti.state

    def test_localtaskjob_double_trigger(self):
        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get('test_localtaskjob_double_trigger')
        task = dag.get_task('test_localtaskjob_double_trigger_task')

        session = settings.Session()

        dag.clear()
        dr = dag.create_dagrun(
            run_id="test",
            state=State.SUCCESS,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        ti = dr.get_task_instance(task_id=task.task_id, session=session)
        ti.state = State.RUNNING
        ti.hostname = get_hostname()
        ti.pid = 1
        session.merge(ti)
        session.commit()

        ti_run = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti_run.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti_run, executor=SequentialExecutor())
        with patch.object(StandardTaskRunner, 'start', return_value=None) as mock_method:
            job1.run()
            mock_method.assert_not_called()

        ti = dr.get_task_instance(task_id=task.task_id, session=session)
        assert ti.pid == 1
        assert ti.state == State.RUNNING

        session.close()

    @pytest.mark.quarantined
    def test_localtaskjob_maintain_heart_rate(self):
        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get('test_localtaskjob_double_trigger')
        task = dag.get_task('test_localtaskjob_double_trigger_task')

        session = settings.Session()

        dag.clear()
        dag.create_dagrun(
            run_id="test",
            state=State.SUCCESS,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )

        ti_run = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti_run.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti_run, executor=SequentialExecutor())

        # this should make sure we only heartbeat once and exit at the second
        # loop in _execute()
        return_codes = [None, 0]

        def multi_return_code():
            return return_codes.pop(0)

        time_start = time.time()
        with patch.object(StandardTaskRunner, 'start', return_value=None) as mock_start:
            with patch.object(StandardTaskRunner, 'return_code') as mock_ret_code:
                mock_ret_code.side_effect = multi_return_code
                job1.run()
                assert mock_start.call_count == 1
                assert mock_ret_code.call_count == 2
        time_end = time.time()

        assert self.mock_base_job_sleep.call_count == 1
        assert job1.state == State.SUCCESS

        # Consider we have patched sleep call, it should not be sleeping to
        # keep up with the heart rate in other unpatched places
        #
        # We already make sure patched sleep call is only called once
        assert time_end - time_start < job1.heartrate
        session.close()

    def test_mark_failure_on_failure_callback(self):
        """
        Test that ensures that mark_failure in the UI fails
        the task, and executes on_failure_callback
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        failure_callback_called = Value('i', 0)
        task_terminated_externally = Value('i', 1)

        def check_failure(context):
            with failure_callback_called.get_lock():
                failure_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_mark_failure'
            assert context['exception'] == "task marked as failed externally"

        def task_function(ti):
            with create_session() as session:
                assert State.RUNNING == ti.state
                ti.log.info("Marking TI as failed 'externally'")
                ti.state = State.FAILED
                session.merge(ti)
                session.commit()

            time.sleep(10)
            # This should not happen -- the state change should be noticed and the task should get killed
            with task_terminated_externally.get_lock():
                task_terminated_externally.value = 0

        with DAG(dag_id='test_mark_failure', start_date=DEFAULT_DATE) as dag:
            task = PythonOperator(
                task_id='test_state_succeeded1',
                python_callable=task_function,
                on_failure_callback=check_failure,
            )

        dag.clear()
        with create_session() as session:
            dag.create_dagrun(
                run_id="test",
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                start_date=DEFAULT_DATE,
                session=session,
            )
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        with timeout(30):
            # This should be _much_ shorter to run.
            # If you change this limit, make the timeout in the callable above bigger
            job1.run()

        ti.refresh_from_db()
        assert ti.state == State.FAILED
        assert failure_callback_called.value == 1
        assert task_terminated_externally.value == 1

    @patch('airflow.utils.process_utils.subprocess.check_call')
    @patch.object(StandardTaskRunner, 'return_code')
    def test_failure_callback_only_called_once(self, mock_return_code, _check_call):
        """
        Test that ensures that when a task exits with failure by itself,
        failure callback is only called once
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        failure_callback_called = Value('i', 0)
        callback_count_lock = Lock()

        def failure_callback(context):
            with callback_count_lock:
                failure_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_failure_callback_race'
            assert isinstance(context['exception'], AirflowFailException)

        def task_function(ti):
            raise AirflowFailException()

        dag = DAG(dag_id='test_failure_callback_race', start_date=DEFAULT_DATE)
        task = PythonOperator(
            task_id='test_exit_on_failure',
            python_callable=task_function,
            on_failure_callback=failure_callback,
            dag=dag,
        )

        dag.clear()
        with create_session() as session:
            dag.create_dagrun(
                run_id="test",
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                start_date=DEFAULT_DATE,
                session=session,
            )
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())

        # Simulate race condition where job1 heartbeat ran right after task
        # state got set to failed by ti.handle_failure but before task process
        # fully exits. See _execute loop in airflow/jobs/local_task_job.py.
        # In this case, we have:
        #  * task_runner.return_code() is None
        #  * ti.state == State.Failed
        #
        # We also need to set return_code to a valid int after job1.terminating
        # is set to True so _execute loop won't loop forever.
        def dummy_return_code(*args, **kwargs):
            return None if not job1.terminating else -9

        mock_return_code.side_effect = dummy_return_code

        with timeout(10):
            # This should be _much_ shorter to run.
            # If you change this limit, make the timeout in the callable above bigger
            job1.run()

        ti.refresh_from_db()
        assert ti.state == State.FAILED  # task exits with failure state
        assert failure_callback_called.value == 1

    @pytest.mark.quarantined
    def test_mark_success_on_success_callback(self):
        """
        Test that ensures that where a task is marked success in the UI
        on_success_callback gets executed
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        success_callback_called = Value('i', 0)
        task_terminated_externally = Value('i', 1)
        shared_mem_lock = Lock()

        def success_callback(context):
            with shared_mem_lock:
                success_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_mark_success'

        dag = DAG(dag_id='test_mark_success', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        def task_function(ti):

            time.sleep(60)
            # This should not happen -- the state change should be noticed and the task should get killed
            with shared_mem_lock:
                task_terminated_externally.value = 0

        task = PythonOperator(
            task_id='test_state_succeeded1',
            python_callable=task_function,
            on_success_callback=success_callback,
            dag=dag,
        )

        session = settings.Session()

        dag.clear()
        dag.create_dagrun(
            run_id="test",
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        job1.task_runner = StandardTaskRunner(job1)

        settings.engine.dispose()
        process = multiprocessing.Process(target=job1.run)
        process.start()

        for _ in range(0, 25):
            ti.refresh_from_db()
            if ti.state == State.RUNNING:
                break
            time.sleep(0.2)
        assert ti.state == State.RUNNING
        ti.state = State.SUCCESS
        session.merge(ti)
        session.commit()

        process.join(timeout=10)
        assert success_callback_called.value == 1
        assert task_terminated_externally.value == 1
        assert not process.is_alive()

    @parameterized.expand(
        [
            (signal.SIGTERM,),
            (signal.SIGKILL,),
        ]
    )
    @pytest.mark.quarantined
    def test_process_kill_calls_on_failure_callback(self, signal_type):
        """
        Test that ensures that when a task is killed with sigterm or sigkill
        on_failure_callback gets executed
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        failure_callback_called = Value('i', 0)
        task_terminated_externally = Value('i', 1)
        shared_mem_lock = Lock()

        def failure_callback(context):
            with shared_mem_lock:
                failure_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_mark_failure'

        dag = DAG(dag_id='test_mark_failure', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'})

        def task_function(ti):

            time.sleep(60)
            # This should not happen -- the state change should be noticed and the task should get killed
            with shared_mem_lock:
                task_terminated_externally.value = 0

        task = PythonOperator(
            task_id='test_on_failure',
            python_callable=task_function,
            on_failure_callback=failure_callback,
            dag=dag,
        )

        session = settings.Session()

        dag.clear()
        dag.create_dagrun(
            run_id="test",
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        job1.task_runner = StandardTaskRunner(job1)

        settings.engine.dispose()
        process = multiprocessing.Process(target=job1.run)
        process.start()

        for _ in range(0, 20):
            ti.refresh_from_db()
            if ti.state == State.RUNNING and ti.pid is not None:
                break
            time.sleep(0.2)
        assert ti.pid is not None
        assert ti.state == State.RUNNING
        os.kill(ti.pid, signal_type)
        process.join(timeout=10)
        assert failure_callback_called.value == 1
        assert task_terminated_externally.value == 1
        assert not process.is_alive()

    @parameterized.expand(
        [
            (
                {('scheduler', 'schedule_after_task_execution'): 'True'},
                {'A': 'B', 'B': 'C'},
                {'A': State.QUEUED, 'B': State.NONE, 'C': State.NONE},
                {'A': State.SUCCESS, 'B': State.SCHEDULED, 'C': State.NONE},
                {'A': State.SUCCESS, 'B': State.SUCCESS, 'C': State.SCHEDULED},
                "A -> B -> C, with fast-follow ON when A runs, B should be QUEUED. Same for B and C.",
            ),
            (
                {('scheduler', 'schedule_after_task_execution'): 'False'},
                {'A': 'B', 'B': 'C'},
                {'A': State.QUEUED, 'B': State.NONE, 'C': State.NONE},
                {'A': State.SUCCESS, 'B': State.NONE, 'C': State.NONE},
                None,
                "A -> B -> C, with fast-follow OFF, when A runs, B shouldn't be QUEUED.",
            ),
            (
                {('scheduler', 'schedule_after_task_execution'): 'True'},
                {'A': 'B', 'C': 'B', 'D': 'C'},
                {'A': State.QUEUED, 'B': State.NONE, 'C': State.NONE, 'D': State.NONE},
                {'A': State.SUCCESS, 'B': State.NONE, 'C': State.NONE, 'D': State.NONE},
                None,
                "D -> C -> B & A -> B, when A runs but C isn't QUEUED yet, B shouldn't be QUEUED.",
            ),
            (
                {('scheduler', 'schedule_after_task_execution'): 'True'},
                {'A': 'C', 'B': 'C'},
                {'A': State.QUEUED, 'B': State.FAILED, 'C': State.NONE},
                {'A': State.SUCCESS, 'B': State.FAILED, 'C': State.UPSTREAM_FAILED},
                None,
                "A -> C & B -> C, when A is QUEUED but B has FAILED, C is marked UPSTREAM_FAILED.",
            ),
        ]
    )
    def test_fast_follow(
        self, conf, dependencies, init_state, first_run_state, second_run_state, error_message
    ):

        with conf_vars(conf):
            session = settings.Session()

            dag = DAG('test_dagrun_fast_follow', start_date=DEFAULT_DATE)

            dag_model = DagModel(
                dag_id=dag.dag_id,
                next_dagrun=dag.start_date,
                is_active=True,
            )
            session.add(dag_model)
            session.flush()

            python_callable = lambda: True
            with dag:
                task_a = PythonOperator(task_id='A', python_callable=python_callable)
                task_b = PythonOperator(task_id='B', python_callable=python_callable)
                task_c = PythonOperator(task_id='C', python_callable=python_callable)
                if 'D' in init_state:
                    task_d = PythonOperator(task_id='D', python_callable=python_callable)
                for upstream, downstream in dependencies.items():
                    dag.set_dependency(upstream, downstream)

            scheduler_job = SchedulerJob(subdir=os.devnull)
            scheduler_job.dagbag.bag_dag(dag, root_dag=dag)

            dag_run = dag.create_dagrun(run_id='test_dagrun_fast_follow', state=State.RUNNING)

            task_instance_a = TaskInstance(task_a, dag_run.execution_date, init_state['A'])

            task_instance_b = TaskInstance(task_b, dag_run.execution_date, init_state['B'])

            task_instance_c = TaskInstance(task_c, dag_run.execution_date, init_state['C'])

            if 'D' in init_state:
                task_instance_d = TaskInstance(task_d, dag_run.execution_date, init_state['D'])
                session.merge(task_instance_d)

            session.merge(task_instance_a)
            session.merge(task_instance_b)
            session.merge(task_instance_c)
            session.flush()

            job1 = LocalTaskJob(
                task_instance=task_instance_a, ignore_ti_state=True, executor=SequentialExecutor()
            )
            job1.task_runner = StandardTaskRunner(job1)

            job2 = LocalTaskJob(
                task_instance=task_instance_b, ignore_ti_state=True, executor=SequentialExecutor()
            )
            job2.task_runner = StandardTaskRunner(job2)

            settings.engine.dispose()
            job1.run()
            self.validate_ti_states(dag_run, first_run_state, error_message)
            if second_run_state:
                job2.run()
                self.validate_ti_states(dag_run, second_run_state, error_message)
            if scheduler_job.processor_agent:
                scheduler_job.processor_agent.end()

    def test_task_exit_should_update_state_of_finished_dagruns_with_dag_paused(self):
        """Test that with DAG paused, DagRun state will update when the tasks finishes the run"""
        dag = DAG(dag_id='test_dags', start_date=DEFAULT_DATE)
        op1 = PythonOperator(task_id='dummy', dag=dag, owner='airflow', python_callable=lambda: True)

        session = settings.Session()
        orm_dag = DagModel(
            dag_id=dag.dag_id,
            has_task_concurrency_limits=False,
            next_dagrun=dag.start_date,
            next_dagrun_create_after=dag.following_schedule(DEFAULT_DATE),
            is_active=True,
            is_paused=True,
        )
        session.add(orm_dag)
        session.flush()
        # Write Dag to DB
        dagbag = DagBag(dag_folder="/dev/null", include_examples=False, read_dags_from_db=False)
        dagbag.bag_dag(dag, root_dag=dag)
        dagbag.sync_to_db()

        dr = dag.create_dagrun(
            run_type=DagRunType.SCHEDULED,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
        )
        assert dr.state == State.RUNNING
        ti = TaskInstance(op1, dr.execution_date)
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        job1.task_runner = StandardTaskRunner(job1)
        job1.run()
        session.add(dr)
        session.refresh(dr)
        assert dr.state == State.SUCCESS


@pytest.fixture()
def clean_db_helper():
    yield
    db.clear_db_jobs()
    db.clear_db_runs()


@pytest.mark.usefixtures("clean_db_helper")
class TestLocalTaskJobPerformance:
    @pytest.mark.parametrize("return_codes", [[0], 9 * [None] + [0]])  # type: ignore
    @mock.patch("airflow.jobs.local_task_job.get_task_runner")
    def test_number_of_queries_single_loop(self, mock_get_task_runner, return_codes):
        unique_prefix = str(uuid.uuid4())
        dag = DAG(dag_id=f'{unique_prefix}_test_number_of_queries', start_date=DEFAULT_DATE)
        task = DummyOperator(task_id='test_state_succeeded1', dag=dag)

        dag.clear()
        dag.create_dagrun(run_id=unique_prefix, execution_date=DEFAULT_DATE, state=State.NONE)

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)

        mock_get_task_runner.return_value.return_code.side_effects = return_codes

        job = LocalTaskJob(task_instance=ti, executor=MockExecutor())
        with assert_queries_count(16):
            job.run()
