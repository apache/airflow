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
import signal
import time
import uuid
from multiprocessing import Lock, Value
from typing import List, Union
from unittest import mock
from unittest.mock import patch

import psutil
import pytest

from airflow import settings
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.jobs.local_task_job import LocalTaskJob
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
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


@pytest.fixture
def clear_db():
    db.clear_db_dags()
    db.clear_db_jobs()
    db.clear_db_runs()
    db.clear_db_task_fail()
    yield


@pytest.fixture(scope='class')
def clear_db_class():
    yield
    db.clear_db_dags()
    db.clear_db_jobs()
    db.clear_db_runs()
    db.clear_db_task_fail()


@pytest.fixture(scope='module')
def dagbag():
    return DagBag(
        dag_folder=TEST_DAG_FOLDER,
        include_examples=False,
    )


@pytest.mark.usefixtures('clear_db_class', 'clear_db')
class TestLocalTaskJob:
    @pytest.fixture(autouse=True)
    def set_instance_attrs(self, dagbag):
        self.dagbag = dagbag
        with patch('airflow.jobs.base_job.sleep') as self.mock_base_job_sleep:
            yield

    def validate_ti_states(self, dag_run, ti_state_mapping, error_message):
        for task_id, expected_state in ti_state_mapping.items():
            task_instance = dag_run.get_task_instance(task_id=task_id)
            task_instance.refresh_from_db()
            assert task_instance.state == expected_state, error_message

    def test_localtaskjob_essential_attr(self, dag_maker):
        """
        Check whether essential attributes
        of LocalTaskJob can be assigned with
        proper values without intervention
        """
        with dag_maker('test_localtaskjob_essential_attr'):
            op1 = EmptyOperator(task_id='op1')

        dr = dag_maker.create_dagrun()

        ti = dr.get_task_instance(task_id=op1.task_id)

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())

        essential_attr = ["dag_id", "job_type", "start_date", "hostname"]

        check_result_1 = [hasattr(job1, attr) for attr in essential_attr]
        assert all(check_result_1)

        check_result_2 = [getattr(job1, attr) is not None for attr in essential_attr]
        assert all(check_result_2)

    def test_localtaskjob_heartbeat(self, dag_maker):
        session = settings.Session()
        with dag_maker('test_localtaskjob_heartbeat'):
            op1 = EmptyOperator(task_id='op1')

        dr = dag_maker.create_dagrun()
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
        assert not ti.run_as_user
        assert not job1.task_runner.run_as_user
        job1.heartbeat_callback(session=None)

        job1.task_runner.process.pid = 2
        with pytest.raises(AirflowException):
            job1.heartbeat_callback()

    @mock.patch('subprocess.check_call')
    @mock.patch('airflow.jobs.local_task_job.psutil')
    def test_localtaskjob_heartbeat_with_run_as_user(self, psutil_mock, _, dag_maker):
        session = settings.Session()
        with dag_maker('test_localtaskjob_heartbeat'):
            op1 = EmptyOperator(task_id='op1', run_as_user='myuser')
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.RUNNING
        ti.pid = 2
        ti.hostname = get_hostname()
        session.commit()

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        ti.task = op1
        ti.refresh_from_task(op1)
        job1.task_runner = StandardTaskRunner(job1)
        job1.task_runner.process = mock.Mock()
        job1.task_runner.process.pid = 2
        # Here, ti.pid is 2, the parent process of ti.pid is a mock(different).
        # And task_runner process is 2. Should fail
        with pytest.raises(AirflowException, match='PID of job runner does not match'):
            job1.heartbeat_callback()

        job1.task_runner.process.pid = 1
        # We make the parent process of ti.pid to equal the task_runner process id
        psutil_mock.Process.return_value.ppid.return_value = 1
        ti.state = State.RUNNING
        ti.pid = 2
        # The task_runner process id is 1, same as the parent process of ti.pid
        # as seen above
        assert ti.run_as_user
        session.merge(ti)
        session.commit()
        job1.heartbeat_callback(session=None)

        # Here the task_runner process id is changed to 2
        # while parent process of ti.pid is kept at 1, which is different
        job1.task_runner.process.pid = 2
        with pytest.raises(AirflowException, match='PID of job runner does not match'):
            job1.heartbeat_callback()

    @conf_vars({('core', 'default_impersonation'): 'testuser'})
    @mock.patch('subprocess.check_call')
    @mock.patch('airflow.jobs.local_task_job.psutil')
    def test_localtaskjob_heartbeat_with_default_impersonation(self, psutil_mock, _, dag_maker):
        session = settings.Session()
        with dag_maker('test_localtaskjob_heartbeat'):
            op1 = EmptyOperator(task_id='op1')
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.RUNNING
        ti.pid = 2
        ti.hostname = get_hostname()
        session.commit()

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        ti.task = op1
        ti.refresh_from_task(op1)
        job1.task_runner = StandardTaskRunner(job1)
        job1.task_runner.process = mock.Mock()
        job1.task_runner.process.pid = 2
        # Here, ti.pid is 2, the parent process of ti.pid is a mock(different).
        # And task_runner process is 2. Should fail
        with pytest.raises(AirflowException, match='PID of job runner does not match'):
            job1.heartbeat_callback()

        job1.task_runner.process.pid = 1
        # We make the parent process of ti.pid to equal the task_runner process id
        psutil_mock.Process.return_value.ppid.return_value = 1
        ti.state = State.RUNNING
        ti.pid = 2
        # The task_runner process id is 1, same as the parent process of ti.pid
        # as seen above
        assert job1.task_runner.run_as_user == 'testuser'
        session.merge(ti)
        session.commit()
        job1.heartbeat_callback(session=None)

        # Here the task_runner process id is changed to 2
        # while parent process of ti.pid is kept at 1, which is different
        job1.task_runner.process.pid = 2
        with pytest.raises(AirflowException, match='PID of job runner does not match'):
            job1.heartbeat_callback()

    def test_heartbeat_failed_fast(self):
        """
        Test that task heartbeat will sleep when it fails fast
        """
        self.mock_base_job_sleep.side_effect = time.sleep
        dag_id = 'test_heartbeat_failed_fast'
        task_id = 'test_heartbeat_failed_fast_op'
        with create_session() as session:

            dag_id = 'test_heartbeat_failed_fast'
            task_id = 'test_heartbeat_failed_fast_op'
            dag = self.dagbag.get_dag(dag_id)
            task = dag.get_task(task_id)

            dr = dag.create_dagrun(
                run_id="test_heartbeat_failed_fast_run",
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                start_date=DEFAULT_DATE,
                session=session,
            )

            ti = dr.task_instances[0]
            ti.refresh_from_task(task)
            ti.state = State.QUEUED
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

    @patch('airflow.utils.process_utils.subprocess.check_call')
    @patch.object(StandardTaskRunner, 'return_code')
    def test_mark_success_no_kill(self, mock_return_code, _check_call, caplog, dag_maker):
        """
        Test that ensures that mark_success in the UI doesn't cause
        the task to fail, and that the task exits
        """
        session = settings.Session()

        def task_function(ti):
            assert ti.state == State.RUNNING
            # Simulate marking this successful in the UI
            ti.state = State.SUCCESS
            session.merge(ti)
            session.commit()
            # The below code will not run as heartbeat will detect change of state
            time.sleep(10)

        with dag_maker('test_mark_success'):
            task = PythonOperator(task_id="task1", python_callable=task_function)
        dr = dag_maker.create_dagrun()

        ti = dr.task_instances[0]
        ti.refresh_from_task(task)

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True)

        def dummy_return_code(*args, **kwargs):
            return None if not job1.terminating else -9

        # The return code when we mark success in the UI is None
        mock_return_code.side_effect = dummy_return_code

        with timeout(30):
            job1.run()
        ti.refresh_from_db()
        assert State.SUCCESS == ti.state
        assert (
            "State of this instance has been externally set to success. Terminating instance." in caplog.text
        )

    def test_localtaskjob_double_trigger(self):

        dag = self.dagbag.dags.get('test_localtaskjob_double_trigger')
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

        ti_run = TaskInstance(task=task, run_id=dr.run_id)
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
    @patch.object(StandardTaskRunner, 'return_code')
    def test_localtaskjob_maintain_heart_rate(self, mock_return_code, caplog, create_dummy_dag):

        _, task = create_dummy_dag('test_localtaskjob_double_trigger')

        ti_run = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti_run.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti_run, executor=SequentialExecutor())

        time_start = time.time()

        # this should make sure we only heartbeat once and exit at the second
        # loop in _execute(). While the heartbeat exits at second loop, return_code
        # is also called by task_runner.terminate method for proper clean up,
        # hence the extra value after 0.
        mock_return_code.side_effect = [None, 0, None]

        with timeout(10):
            job1.run()
        assert mock_return_code.call_count == 3
        time_end = time.time()

        assert self.mock_base_job_sleep.call_count == 1
        assert job1.state == State.SUCCESS

        # Consider we have patched sleep call, it should not be sleeping to
        # keep up with the heart rate in other unpatched places
        #
        # We already make sure patched sleep call is only called once
        assert time_end - time_start < job1.heartrate
        assert "Task exited with return code 0" in caplog.text

    def test_mark_failure_on_failure_callback(self, caplog, dag_maker):
        """
        Test that ensures that mark_failure in the UI fails
        the task, and executes on_failure_callback
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        failure_callback_called = Value('i', 0)
        session = settings.Session()

        def check_failure(context):
            with failure_callback_called.get_lock():
                failure_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_mark_failure'
            assert context['exception'] == "task marked as failed externally"

        def task_function(ti):
            assert State.RUNNING == ti.state
            ti.log.info("Marking TI as failed 'externally'")
            ti.state = State.FAILED
            session.merge(ti)
            session.commit()

            # This should not happen -- the state change should be noticed and the task should get killed
            time.sleep(10)
            assert False

        with dag_maker("test_mark_failure", start_date=DEFAULT_DATE):
            task = PythonOperator(
                task_id='test_state_succeeded1',
                python_callable=task_function,
                on_failure_callback=check_failure,
            )
        dag_maker.create_dagrun()
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
        assert "State of this instance has been externally set to failed. "
        "Terminating instance." in caplog.text

    def test_dagrun_timeout_logged_in_task_logs(self, caplog, dag_maker):
        """
        Test that ensures that if a running task is externally skipped (due to a dagrun timeout)
        It is logged in the task logs.
        """

        session = settings.Session()

        def task_function(ti):
            assert State.RUNNING == ti.state
            time.sleep(0.1)
            ti.log.info("Marking TI as skipped externally")
            ti.state = State.SKIPPED
            session.merge(ti)
            session.commit()

            # This should not happen -- the state change should be noticed and the task should get killed
            time.sleep(10)
            assert False

        with dag_maker(
            "test_mark_failure", start_date=DEFAULT_DATE, dagrun_timeout=datetime.timedelta(microseconds=1)
        ):
            task = PythonOperator(
                task_id='skipped_externally',
                python_callable=task_function,
            )
        dag_maker.create_dagrun()
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        with timeout(30):
            # This should be _much_ shorter to run.
            # If you change this limit, make the timeout in the callable above bigger
            job1.run()

        ti.refresh_from_db()
        assert ti.state == State.SKIPPED
        assert "DagRun timed out after " in caplog.text

    @patch('airflow.utils.process_utils.subprocess.check_call')
    @patch.object(StandardTaskRunner, 'return_code')
    def test_failure_callback_only_called_once(self, mock_return_code, _check_call, dag_maker):
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

        with dag_maker("test_failure_callback_race"):
            task = PythonOperator(
                task_id='test_exit_on_failure',
                python_callable=task_function,
                on_failure_callback=failure_callback,
            )
        dag_maker.create_dagrun()
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

    @patch('airflow.utils.process_utils.subprocess.check_call')
    @patch.object(StandardTaskRunner, 'return_code')
    def test_mark_success_on_success_callback(self, mock_return_code, _check_call, caplog, dag_maker):
        """
        Test that ensures that where a task is marked success in the UI
        on_success_callback gets executed
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        success_callback_called = Value('i', 0)
        session = settings.Session()

        def success_callback(context):
            with success_callback_called.get_lock():
                success_callback_called.value = 1
            assert context['dag_run'].dag_id == 'test_mark_success'

        def task_function(ti):
            assert ti.state == State.RUNNING
            # mark it success in the UI
            ti.state = State.SUCCESS
            session.merge(ti)
            session.commit()
            # This should not happen -- the state change should be noticed and the task should get killed
            time.sleep(10)
            assert False

        with dag_maker(dag_id='test_mark_success', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}):
            task = PythonOperator(
                task_id='test_state_succeeded1',
                python_callable=task_function,
                on_success_callback=success_callback,
            )
        dag_maker.create_dagrun()
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())

        def dummy_return_code(*args, **kwargs):
            return None if not job1.terminating else -9

        # The return code when we mark success in the UI is None
        mock_return_code.side_effect = dummy_return_code

        settings.engine.dispose()
        with timeout(30):
            job1.run()  # This should run fast because of the return_code=None
        ti.refresh_from_db()
        assert success_callback_called.value == 1
        assert "State of this instance has been externally set to success. "
        "Terminating instance." in caplog.text

    @patch('airflow.utils.process_utils.subprocess.check_call')
    def test_task_sigkill_calls_on_failure_callback(self, _check_call, caplog, dag_maker):
        """
        Test that ensures that when a task is killed with sigkill
        on_failure_callback gets executed
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        failure_callback_called = Value('i', 0)

        def failure_callback(context):
            with failure_callback_called.get_lock():
                failure_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_send_sigkill'

        def task_function(ti):
            assert ti.state == State.RUNNING
            os.kill(os.getpid(), signal.SIGKILL)

        with dag_maker(dag_id='test_send_sigkill'):
            task = PythonOperator(
                task_id='test_on_failure',
                python_callable=task_function,
                on_failure_callback=failure_callback,
            )
        dag_maker.create_dagrun()

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        settings.engine.dispose()
        with timeout(10):
            job1.run()  # This should be fast because the signal is sent right away
        ti.refresh_from_db()
        assert failure_callback_called.value == 1
        assert "Task exited with return code Negsignal.SIGKILL" in caplog.text

    @pytest.mark.quarantined
    def test_process_sigterm_calls_on_failure_callback(self, caplog, dag_maker):
        """
        Test that ensures that when a task runner is killed with sigterm
        on_failure_callback gets executed
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        failure_callback_called = Value('i', 0)

        def failure_callback(context):
            with failure_callback_called.get_lock():
                failure_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_mark_failure'

        def task_function(ti):
            assert ti.state == State.RUNNING
            os.kill(psutil.Process(os.getpid()).ppid(), signal.SIGTERM)

        with dag_maker(dag_id='test_mark_failure', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}):
            task = PythonOperator(
                task_id='test_on_failure',
                python_callable=task_function,
                on_failure_callback=failure_callback,
            )
        dag_maker.create_dagrun()

        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        settings.engine.dispose()
        with timeout(10):
            job1.run()
        ti.refresh_from_db()
        assert failure_callback_called.value == 1
        assert "Received SIGTERM. Terminating subprocesses" in caplog.text
        assert "Task exited with return code 143" in caplog.text

    @pytest.mark.parametrize(
        "conf, dependencies, init_state, first_run_state, second_run_state, error_message",
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
        ],
    )
    def test_fast_follow(
        self, conf, dependencies, init_state, first_run_state, second_run_state, error_message, dag_maker
    ):

        with conf_vars(conf):
            session = settings.Session()

            python_callable = lambda: True
            with dag_maker('test_dagrun_fast_follow') as dag:
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

            task_instance_a = TaskInstance(task_a, run_id=dag_run.run_id, state=init_state['A'])

            task_instance_b = TaskInstance(task_b, run_id=dag_run.run_id, state=init_state['B'])

            task_instance_c = TaskInstance(task_c, run_id=dag_run.run_id, state=init_state['C'])

            if 'D' in init_state:
                task_instance_d = TaskInstance(task_d, run_id=dag_run.run_id, state=init_state['D'])
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

    @conf_vars({('scheduler', 'schedule_after_task_execution'): 'True'})
    def test_mini_scheduler_works_with_wait_for_upstream(self, caplog, dag_maker):
        session = settings.Session()
        with dag_maker(default_args={'wait_for_downstream': True}, catchup=False) as dag:
            task_a = PythonOperator(task_id='A', python_callable=lambda: True)
            task_b = PythonOperator(task_id='B', python_callable=lambda: True)
            task_c = PythonOperator(task_id='C', python_callable=lambda: True)
            task_a >> task_b >> task_c

        scheduler_job = SchedulerJob(subdir=os.devnull)
        scheduler_job.dagbag.bag_dag(dag, root_dag=dag)

        dr = dag.create_dagrun(run_id='test_1', state=State.RUNNING, execution_date=DEFAULT_DATE)
        dr2 = dag.create_dagrun(
            run_id='test_2', state=State.RUNNING, execution_date=DEFAULT_DATE + datetime.timedelta(hours=1)
        )
        ti_a = TaskInstance(task_a, run_id=dr.run_id, state=State.SUCCESS)
        ti_b = TaskInstance(task_b, run_id=dr.run_id, state=State.SUCCESS)
        ti_c = TaskInstance(task_c, run_id=dr.run_id, state=State.RUNNING)
        ti2_a = TaskInstance(task_a, run_id=dr2.run_id, state=State.NONE)
        ti2_b = TaskInstance(task_b, run_id=dr2.run_id, state=State.NONE)
        ti2_c = TaskInstance(task_c, run_id=dr2.run_id, state=State.NONE)
        session.merge(ti_a)
        session.merge(ti_b)
        session.merge(ti_c)
        session.merge(ti2_a)
        session.merge(ti2_b)
        session.merge(ti2_c)
        session.flush()

        job1 = LocalTaskJob(task_instance=ti2_a, ignore_ti_state=True, executor=SequentialExecutor())
        job1.task_runner = StandardTaskRunner(job1)
        job1.run()

        ti2_a.refresh_from_db(session)
        ti2_b.refresh_from_db(session)
        assert ti2_a.state == State.SUCCESS
        assert ti2_b.state == State.NONE
        assert "0 downstream tasks scheduled from follow-on schedule" in caplog.text

        failed_deps = list(ti2_b.get_failed_dep_statuses(session=session))
        assert len(failed_deps) == 1
        assert failed_deps[0].dep_name == "Previous Dagrun State"
        assert not failed_deps[0].passed

    @patch('airflow.utils.process_utils.subprocess.check_call')
    def test_task_sigkill_works_with_retries(self, _check_call, caplog, dag_maker):
        """
        Test that ensures that tasks are retried when they receive sigkill
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        retry_callback_called = Value('i', 0)

        def retry_callback(context):
            with retry_callback_called.get_lock():
                retry_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_mark_failure_2'

        def task_function(ti):
            os.kill(os.getpid(), signal.SIGKILL)

        with dag_maker(
            dag_id='test_mark_failure_2', start_date=DEFAULT_DATE, default_args={'owner': 'owner1'}
        ):
            task = PythonOperator(
                task_id='test_on_failure',
                python_callable=task_function,
                retries=1,
                on_retry_callback=retry_callback,
            )
        dr = dag_maker.create_dagrun()
        ti = dr.task_instances[0]
        ti.refresh_from_task(task)
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        settings.engine.dispose()
        with timeout(10):
            job1.run()
        assert retry_callback_called.value == 1
        assert "Task exited with return code Negsignal.SIGKILL" in caplog.text

    @pytest.mark.quarantined
    def test_process_sigterm_works_with_retries(self, caplog, dag_maker):
        """
        Test that ensures that task runner sets tasks to retry when they(task runner)
         receive sigterm
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        retry_callback_called = Value('i', 0)

        def retry_callback(context):
            with retry_callback_called.get_lock():
                retry_callback_called.value += 1
            assert context['dag_run'].dag_id == 'test_mark_failure_2'

        def task_function(ti):
            while not ti.pid:
                time.sleep(0.1)
            os.kill(psutil.Process(os.getpid()).ppid(), signal.SIGTERM)

        with dag_maker(dag_id='test_mark_failure_2'):
            task = PythonOperator(
                task_id='test_on_failure',
                python_callable=task_function,
                retries=1,
                on_retry_callback=retry_callback,
            )
        dag_maker.create_dagrun()
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        settings.engine.dispose()
        with timeout(10):
            job1.run()
        assert retry_callback_called.value == 1
        assert "Received SIGTERM. Terminating subprocesses" in caplog.text
        assert "Task exited with return code 143" in caplog.text

    def test_task_exit_should_update_state_of_finished_dagruns_with_dag_paused(self, dag_maker):
        """Test that with DAG paused, DagRun state will update when the tasks finishes the run"""
        schedule_interval = datetime.timedelta(days=1)
        with dag_maker(dag_id='test_dags', schedule_interval=schedule_interval) as dag:
            op1 = PythonOperator(task_id='dummy', python_callable=lambda: True)

        session = settings.Session()
        dagmodel = dag_maker.dag_model
        dagmodel.next_dagrun_create_after = DEFAULT_DATE + schedule_interval
        dagmodel.is_paused = True
        session.merge(dagmodel)
        session.flush()
        # Write Dag to DB
        dagbag = DagBag(dag_folder="/dev/null", include_examples=False, read_dags_from_db=False)
        dagbag.bag_dag(dag, root_dag=dag)
        dagbag.sync_to_db()

        dr = dag_maker.create_dagrun(run_type=DagRunType.SCHEDULED)

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
@mock.patch("airflow.jobs.local_task_job.get_task_runner")
def test_number_of_queries_single_loop(mock_get_task_runner, dag_maker):
    codes: List[Union[int, None]] = 9 * [None] + [0]
    mock_get_task_runner.return_value.return_code.side_effects = [[0], codes]

    unique_prefix = str(uuid.uuid4())
    with dag_maker(dag_id=f'{unique_prefix}_test_number_of_queries'):
        task = EmptyOperator(task_id='test_state_succeeded1')

    dr = dag_maker.create_dagrun(run_id=unique_prefix, state=State.NONE)

    ti = dr.task_instances[0]
    ti.refresh_from_task(task)

    job = LocalTaskJob(task_instance=ti, executor=MockExecutor())
    with assert_queries_count(18):
        job.run()
