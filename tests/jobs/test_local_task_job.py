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
from __future__ import annotations

import datetime
import logging
import multiprocessing as mp
import os
import re
import signal
import threading
import time
import uuid
from unittest import mock
from unittest.mock import patch

import psutil
import pytest

from airflow import settings
from airflow.exceptions import AirflowException, AirflowTaskTimeout
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.jobs.job import Job, run_job
from airflow.jobs.local_task_job_runner import SIGSEGV_MESSAGE, LocalTaskJobRunner
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.listeners.listener import get_listener_manager
from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag
from airflow.models.serialized_dag import SerializedDagModel
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

from tests_common.test_utils import db
from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.mock_executor import MockExecutor

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
DEFAULT_LOGICAL_DATE = timezone.coerce_datetime(DEFAULT_DATE)
TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]


@pytest.fixture
def clear_db():
    db.clear_db_dags()
    db.clear_db_jobs()
    db.clear_db_runs()
    db.clear_db_task_fail()


@pytest.fixture(scope="class")
def clear_db_class():
    yield
    db.clear_db_dags()
    db.clear_db_jobs()
    db.clear_db_runs()
    db.clear_db_task_fail()


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(
        dag_folder=TEST_DAG_FOLDER,
        include_examples=False,
    )


@pytest.mark.usefixtures("clear_db_class", "clear_db")
class TestLocalTaskJob:
    @pytest.fixture(autouse=True)
    def set_instance_attrs(self, dagbag):
        self.dagbag = dagbag
        with patch("airflow.jobs.job.sleep") as self.mock_base_job_sleep:
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
        with dag_maker("test_localtaskjob_essential_attr", serialized=True):
            op1 = EmptyOperator(task_id="op1")

        dr = dag_maker.create_dagrun()

        ti = dr.get_task_instance(task_id=op1.task_id)

        job1 = Job(dag_id=ti.dag_id, executor=SequentialExecutor())

        LocalTaskJobRunner(job=job1, task_instance=ti, ignore_ti_state=True)
        essential_attr = ["dag_id", "job_type", "start_date", "hostname"]

        check_result_1 = [hasattr(job1, attr) for attr in essential_attr]
        assert all(check_result_1)

        check_result_2 = [getattr(job1, attr) is not None for attr in essential_attr]
        assert all(check_result_2)

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    def test_localtaskjob_heartbeat(self, dag_maker):
        session = settings.Session()
        with dag_maker("test_localtaskjob_heartbeat"):
            op1 = EmptyOperator(task_id="op1")

        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.RUNNING
        ti.hostname = "blablabla"
        session.commit()

        job1 = Job(dag_id=ti.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti, ignore_ti_state=True)
        ti.task = op1
        ti.refresh_from_task(op1)
        job1.task_runner = StandardTaskRunner(job_runner)
        job1.task_runner.process = mock.Mock()
        job_runner.task_runner = job1.task_runner
        with pytest.raises(AirflowException):
            job_runner.heartbeat_callback()

        job1.task_runner.process.pid = 1
        ti.state = State.RUNNING
        ti.hostname = get_hostname()
        ti.pid = 1
        session.merge(ti)
        session.commit()
        assert ti.pid != os.getpid()
        assert not ti.run_as_user
        assert not job1.task_runner.run_as_user
        job_runner.heartbeat_callback(session=None)

        job1.task_runner.process.pid = 2
        with pytest.raises(AirflowException):
            job_runner.heartbeat_callback()

        # Now, set the ti.pid to None and test that no error
        # is raised.
        ti.pid = None
        session.merge(ti)
        session.commit()
        assert ti.pid != job1.task_runner.process.pid
        assert not ti.run_as_user
        assert not job1.task_runner.run_as_user
        job_runner.heartbeat_callback()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @mock.patch("subprocess.check_call")
    @mock.patch("airflow.jobs.local_task_job_runner.psutil")
    def test_localtaskjob_heartbeat_with_run_as_user(self, psutil_mock, _, dag_maker):
        session = settings.Session()
        with dag_maker("test_localtaskjob_heartbeat"):
            op1 = EmptyOperator(task_id="op1", run_as_user="myuser")
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.RUNNING
        ti.pid = 2
        ti.hostname = get_hostname()
        session.commit()

        job1 = Job(dag_id=ti.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti, ignore_ti_state=True)
        ti.task = op1
        ti.refresh_from_task(op1)
        job1.task_runner = StandardTaskRunner(job_runner)
        job1.task_runner.process = mock.Mock()
        job1.task_runner.process.pid = 2
        job_runner.task_runner = job1.task_runner
        # Here, ti.pid is 2, the parent process of ti.pid is a mock(different).
        # And task_runner process is 2. Should fail
        with pytest.raises(AirflowException, match="PID of job runner does not match"):
            job_runner.heartbeat_callback()

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
        job_runner.heartbeat_callback(session=None)

        # Here the task_runner process id is changed to 2
        # while parent process of ti.pid is kept at 1, which is different
        job1.task_runner.process.pid = 2
        with pytest.raises(AirflowException, match="PID of job runner does not match"):
            job_runner.heartbeat_callback()

        # Here we set the ti.pid to None and test that no error is
        # raised
        ti.pid = None
        session.merge(ti)
        session.commit()
        assert ti.run_as_user
        assert job1.task_runner.run_as_user == ti.run_as_user
        assert ti.pid != job1.task_runner.process.pid
        job_runner.heartbeat_callback()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @conf_vars({("core", "default_impersonation"): "testuser"})
    @mock.patch("subprocess.check_call")
    @mock.patch("airflow.jobs.local_task_job_runner.psutil")
    def test_localtaskjob_heartbeat_with_default_impersonation(
        self, psutil_mock, _, dag_maker
    ):
        session = settings.Session()
        with dag_maker("test_localtaskjob_heartbeat"):
            op1 = EmptyOperator(task_id="op1")
        dr = dag_maker.create_dagrun()
        ti = dr.get_task_instance(task_id=op1.task_id, session=session)
        ti.state = State.RUNNING
        ti.pid = 2
        ti.hostname = get_hostname()
        session.commit()

        job1 = Job(dag_id=ti.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job1, task_instance=ti, ignore_ti_state=True)
        ti.task = op1
        ti.refresh_from_task(op1)
        job1.task_runner = StandardTaskRunner(job_runner)
        job1.task_runner.process = mock.Mock()
        job1.task_runner.process.pid = 2
        job_runner.task_runner = job1.task_runner
        # Here, ti.pid is 2, the parent process of ti.pid is a mock(different).
        # And task_runner process is 2. Should fail
        with pytest.raises(AirflowException, match="PID of job runner does not match"):
            job_runner.heartbeat_callback()

        job1.task_runner.process.pid = 1
        # We make the parent process of ti.pid to equal the task_runner process id
        psutil_mock.Process.return_value.ppid.return_value = 1
        ti.state = State.RUNNING
        ti.pid = 2
        # The task_runner process id is 1, same as the parent process of ti.pid
        # as seen above
        assert job1.task_runner.run_as_user == "testuser"
        session.merge(ti)
        session.commit()
        job_runner.heartbeat_callback(session=None)

        # Here the task_runner process id is changed to 2
        # while parent process of ti.pid is kept at 1, which is different
        job1.task_runner.process.pid = 2
        with pytest.raises(AirflowException, match="PID of job runner does not match"):
            job_runner.heartbeat_callback()

        # Now, set the ti.pid to None and test that no error
        # is raised.
        ti.pid = None
        session.merge(ti)
        session.commit()
        assert job1.task_runner.run_as_user == "testuser"
        assert ti.run_as_user is None
        assert ti.pid != job1.task_runner.process.pid
        job_runner.heartbeat_callback()

    @pytest.mark.flaky(reruns=5)
    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    def test_heartbeat_failed_fast(self):
        """
        Test that task heartbeat will sleep when it fails fast
        """
        self.mock_base_job_sleep.side_effect = time.sleep
        dag_id = "test_heartbeat_failed_fast"
        task_id = "test_heartbeat_failed_fast_op"
        with create_session() as session:
            dag_id = "test_heartbeat_failed_fast"
            task_id = "test_heartbeat_failed_fast_op"
            dag = self.dagbag.get_dag(dag_id)
            task = dag.get_task(task_id)
            data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
            triggered_by_kwargs = (
                {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
            )
            dr = dag.create_dagrun(
                run_id="test_heartbeat_failed_fast_run",
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                start_date=DEFAULT_DATE,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )

            ti = dr.task_instances[0]
            ti.refresh_from_task(task)
            ti.state = State.QUEUED
            ti.hostname = get_hostname()
            ti.pid = 1
            session.commit()

            job = Job(dag_id=ti.dag_id, executor=MockExecutor(do_update=False))
            job_runner = LocalTaskJobRunner(job=job, task_instance=ti)
            job.heartrate = 2
            heartbeat_records = []
            job_runner.heartbeat_callback = lambda session: heartbeat_records.append(
                job.latest_heartbeat
            )
            run_job(job=job, execute_callable=job_runner._execute)
            assert len(heartbeat_records) > 2
            for time1, time2 in zip(heartbeat_records, heartbeat_records[1:]):
                # Assert that difference small enough
                delta = (time2 - time1).total_seconds()
                assert abs(delta - job.heartrate) < 0.8

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @conf_vars({("core", "task_success_overtime"): "1"})
    def test_mark_success_no_kill(self, caplog, get_test_dag, session):
        """
        Test that ensures that mark_success in the UI doesn't cause
        the task to fail, and that the task exits
        """
        dag = get_test_dag("test_mark_state")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        dr = dag.create_dagrun(
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            session=session,
            data_interval=data_interval,
            **triggered_by_kwargs,
        )
        task = dag.get_task(task_id="test_mark_success_no_kill")

        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job1 = Job(dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti, ignore_ti_state=True)

        with timeout(30):
            run_job(job=job1, execute_callable=job_runner._execute)
        ti.refresh_from_db()
        assert State.SUCCESS == ti.state
        assert (
            "State of this instance has been externally set to success. Terminating instance."
            in caplog.text
        )

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    def test_localtaskjob_double_trigger(self):
        dag = self.dagbag.dags.get("test_localtaskjob_double_trigger")
        task = dag.get_task("test_localtaskjob_double_trigger_task")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)

        session = settings.Session()

        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )

        dag.clear()
        dr = dag.create_dagrun(
            run_id="test",
            state=State.SUCCESS,
            execution_date=DEFAULT_DATE,
            start_date=DEFAULT_DATE,
            session=session,
            data_interval=data_interval,
            **triggered_by_kwargs,
        )

        ti = dr.get_task_instance(task_id=task.task_id, session=session)
        ti.state = State.RUNNING
        ti.hostname = get_hostname()
        ti.pid = 1
        session.merge(ti)
        session.commit()

        ti_run = TaskInstance(task=task, run_id=dr.run_id)
        ti_run.refresh_from_db()
        job1 = Job(dag_id=ti_run.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti_run)
        with patch.object(StandardTaskRunner, "start", return_value=None) as mock_method:
            run_job(job=job1, execute_callable=job_runner._execute)
            mock_method.assert_not_called()

        ti = dr.get_task_instance(task_id=task.task_id, session=session)
        assert ti.pid == 1
        assert ti.state == State.RUNNING

        session.close()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @patch.object(StandardTaskRunner, "return_code")
    @mock.patch("airflow.jobs.scheduler_job_runner.Stats.incr", autospec=True)
    def test_local_task_return_code_metric(
        self, mock_stats_incr, mock_return_code, create_dummy_dag
    ):
        dag, task = create_dummy_dag("test_localtaskjob_code")
        dag_run = dag.get_last_dagrun()

        ti_run = TaskInstance(task=task, run_id=dag_run.run_id)
        ti_run.refresh_from_db()
        job1 = Job(dag_id=ti_run.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti_run)
        job1.id = 95

        mock_return_code.side_effect = [None, -9, None]

        with timeout(10):
            run_job(job=job1, execute_callable=job_runner._execute)

        mock_stats_incr.assert_has_calls(
            [
                mock.call("local_task_job.task_exit.95.test_localtaskjob_code.op1.-9"),
                mock.call(
                    "local_task_job.task_exit",
                    tags={
                        "job_id": 95,
                        "dag_id": "test_localtaskjob_code",
                        "task_id": "op1",
                        "return_code": -9,
                    },
                ),
            ]
        )

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @patch.object(StandardTaskRunner, "return_code")
    def test_localtaskjob_maintain_heart_rate(
        self, mock_return_code, caplog, create_dummy_dag
    ):
        dag, task = create_dummy_dag("test_localtaskjob_double_trigger")
        dag_run = dag.get_last_dagrun()

        ti_run = TaskInstance(task=task, run_id=dag_run.run_id)
        ti_run.refresh_from_db()
        job1 = Job(dag_id=ti_run.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti_run)
        time_start = time.time()

        # this should make sure we only heartbeat once and exit at the second
        # loop in _execute(). While the heartbeat exits at second loop, return_code
        # is also called by task_runner.terminate method for proper clean up,
        # hence the extra value after 0.
        mock_return_code.side_effect = [None, 0, None]

        with timeout(10):
            run_job(job=job1, execute_callable=job_runner._execute)
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

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    def test_mark_failure_on_failure_callback(self, caplog, get_test_dag):
        """
        Test that ensures that mark_failure in the UI fails
        the task, and executes on_failure_callback
        """
        dag = get_test_dag("test_mark_state")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )
        task = dag.get_task(task_id="test_mark_failure_externally")
        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job1 = Job(dag_id=ti.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti, ignore_ti_state=True)
        with timeout(30):
            # This should be _much_ shorter to run.
            # If you change this limit, make the timeout in the callable above bigger
            run_job(job=job1, execute_callable=job_runner._execute)

        ti.refresh_from_db()
        assert ti.state == State.FAILED
        assert (
            "State of this instance has been externally set to failed. Terminating instance."
        ) in caplog.text

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    def test_dagrun_timeout_logged_in_task_logs(self, caplog, get_test_dag):
        """
        Test that ensures that if a running task is externally skipped (due to a dagrun timeout)
        It is logged in the task logs.
        """
        dag = get_test_dag("test_mark_state")
        dag.dagrun_timeout = datetime.timedelta(microseconds=1)
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                start_date=DEFAULT_DATE,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )
        task = dag.get_task(task_id="test_mark_skipped_externally")
        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job1 = Job(dag_id=ti.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti, ignore_ti_state=True)
        with timeout(30):
            # This should be _much_ shorter to run.
            # If you change this limit, make the timeout in the callable above bigger
            run_job(job=job1, execute_callable=job_runner._execute)

        ti.refresh_from_db()
        assert ti.state == State.SKIPPED
        assert "DagRun timed out after " in caplog.text

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    def test_failure_callback_called_by_airflow_run_raw_process(
        self, monkeypatch, tmp_path, get_test_dag
    ):
        """
        Ensure failure callback of a task is run by the airflow run --raw process
        """
        callback_file = tmp_path.joinpath("callback.txt")
        callback_file.touch()
        monkeypatch.setenv("AIRFLOW_CALLBACK_FILE", str(callback_file))
        dag = get_test_dag("test_on_failure_callback")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )
        task = dag.get_task(task_id="test_on_failure_callback_task")
        ti = TaskInstance(task=task, run_id=dr.run_id)
        ti.refresh_from_db()

        job1 = Job(executor=SequentialExecutor(), dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job1, task_instance=ti, ignore_ti_state=True)
        run_job(job=job1, execute_callable=job_runner._execute)

        ti.refresh_from_db()
        assert ti.state == State.FAILED  # task exits with failure state
        with open(callback_file) as f:
            lines = f.readlines()
        assert len(lines) == 1  # invoke once
        assert lines[0].startswith(ti.key.primary)
        m = re.match(r"^.+pid: (\d+)$", lines[0])
        assert m, "pid expected in output."
        assert os.getpid() != int(m.group(1))

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @conf_vars({("core", "task_success_overtime"): "5"})
    def test_mark_success_on_success_callback(self, caplog, get_test_dag):
        """
        Test that ensures that where a task is marked success in the UI
        on_success_callback gets executed
        """
        dag = get_test_dag("test_mark_state")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )
        task = dag.get_task(task_id="test_mark_success_no_kill")

        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job = Job(executor=SequentialExecutor(), dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        with timeout(30):
            # This should run fast because of the return_code=None
            run_job(job=job, execute_callable=job_runner._execute)
        ti.refresh_from_db()
        assert (
            "State of this instance has been externally set to success. Terminating instance."
            in caplog.text
        )

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    def test_success_listeners_executed(self, caplog, get_test_dag):
        """
        Test that ensures that when listeners are executed, the task is not killed before they finish
        or timeout
        """
        from tests.listeners import slow_listener

        lm = get_listener_manager()
        lm.clear()
        lm.add_listener(slow_listener)

        dag = get_test_dag("test_mark_state")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )
        task = dag.get_task(task_id="sleep_execution")

        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job = Job(executor=SequentialExecutor(), dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        with timeout(30):
            run_job(job=job, execute_callable=job_runner._execute)
        ti.refresh_from_db()
        assert (
            "State of this instance has been externally set to success. Terminating instance."
            not in caplog.text
        )
        lm.clear()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @conf_vars({("core", "task_success_overtime"): "3"})
    def test_success_slow_listeners_executed_kill(self, caplog, get_test_dag):
        """
        Test that ensures that when there are too slow listeners, the task is killed
        """
        from tests.listeners import very_slow_listener

        lm = get_listener_manager()
        lm.clear()
        lm.add_listener(very_slow_listener)

        dag = get_test_dag("test_mark_state")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )
        task = dag.get_task(task_id="sleep_execution")

        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job = Job(executor=SequentialExecutor(), dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        with timeout(15):
            run_job(job=job, execute_callable=job_runner._execute)
        ti.refresh_from_db()
        assert (
            "State of this instance has been externally set to success. Terminating instance."
            in caplog.text
        )
        lm.clear()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @conf_vars({("core", "task_success_overtime"): "3"})
    def test_success_slow_task_not_killed_by_overtime_but_regular_timeout(
        self, caplog, get_test_dag
    ):
        """
        Test that ensures that when there are listeners, but the task is taking a long time anyways,
        it's not killed by the overtime mechanism.
        """
        from tests.listeners import slow_listener

        lm = get_listener_manager()
        lm.clear()
        lm.add_listener(slow_listener)

        dag = get_test_dag("test_mark_state")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )
        task = dag.get_task(task_id="slow_execution")

        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job = Job(executor=SequentialExecutor(), dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        with pytest.raises(AirflowTaskTimeout):
            with timeout(5):
                run_job(job=job, execute_callable=job_runner._execute)
        ti.refresh_from_db()
        assert (
            "State of this instance has been externally set to success. Terminating instance."
            not in caplog.text
        )
        lm.clear()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.parametrize("signal_type", [signal.SIGTERM, signal.SIGKILL])
    def test_process_os_signal_calls_on_failure_callback(
        self, monkeypatch, tmp_path, get_test_dag, signal_type
    ):
        """
        Test that ensures that when a task is killed with sigkill or sigterm
        on_failure_callback does not get executed by LocalTaskJob.

        Callbacks should not be executed by LocalTaskJob.  If the task killed via sigkill,
        it will be reaped as zombie, then the callback is executed
        """
        callback_file = tmp_path.joinpath("callback.txt")
        # callback_file will be created by the task: bash_sleep
        monkeypatch.setenv("AIRFLOW_CALLBACK_FILE", str(callback_file))
        dag = get_test_dag("test_on_failure_callback")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        with create_session() as session:
            dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
                data_interval=data_interval,
                **triggered_by_kwargs,
            )
        task = dag.get_task(task_id="bash_sleep")
        dag_run = dag.get_last_dagrun()
        ti = TaskInstance(task=task, run_id=dag_run.run_id)
        ti.refresh_from_db()

        signal_sent_status = {"sent": False}

        def get_ti_current_pid(ti) -> str:
            with create_session() as session:
                pid = (
                    session.query(TaskInstance.pid)
                    .filter(
                        TaskInstance.dag_id == ti.dag_id,
                        TaskInstance.task_id == ti.task_id,
                        TaskInstance.run_id == ti.run_id,
                    )
                    .one_or_none()
                )
                return pid[0]

        def send_signal(ti, signal_sent, sig):
            while True:
                task_pid = get_ti_current_pid(
                    ti
                )  # get pid from the db, which is the pid of airflow run --raw
                if (
                    task_pid
                    and ti.current_state() == State.RUNNING
                    and os.path.isfile(callback_file)
                ):  # ensure task is running before sending sig
                    signal_sent["sent"] = True
                    os.kill(task_pid, sig)
                    break
                time.sleep(1)

        thread = threading.Thread(
            name="signaler",
            target=send_signal,
            args=(ti, signal_sent_status, signal_type),
        )
        thread.daemon = True
        thread.start()

        job1 = Job(dag_id=ti.dag_id, executor=SequentialExecutor())
        job_runner = LocalTaskJobRunner(job=job1, task_instance=ti, ignore_ti_state=True)
        run_job(job=job1, execute_callable=job_runner._execute)

        ti.refresh_from_db()

        assert signal_sent_status["sent"]

        if signal_type == signal.SIGTERM:
            assert ti.state == State.FAILED
            with open(callback_file) as f:
                lines = f.readlines()

            assert len(lines) == 1
            assert lines[0].startswith(ti.key.primary)

            m = re.match(r"^.+pid: (\d+)$", lines[0])
            assert m, "pid expected in output."
            pid = int(m.group(1))
            assert os.getpid() != pid  # ensures callback is NOT run by LocalTaskJob
            assert (
                ti.pid == pid
            )  # ensures callback is run by airflow run --raw (TaskInstance#_run_raw_task)
        elif signal_type == signal.SIGKILL:
            assert (
                ti.state == State.RUNNING
            )  # task exits with running state, will be reaped as zombie by scheduler
            with open(callback_file) as f:
                lines = f.readlines()
            assert len(lines) == 0

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.parametrize(
        "conf, init_state, first_run_state, second_run_state, task_ids_to_run, error_message",
        [
            (
                {("scheduler", "schedule_after_task_execution"): "True"},
                {"A": State.QUEUED, "B": State.NONE, "C": State.NONE},
                {"A": State.SUCCESS, "B": State.SCHEDULED, "C": State.NONE},
                {"A": State.SUCCESS, "B": State.SUCCESS, "C": State.SCHEDULED},
                ["A", "B"],
                "A -> B -> C, with fast-follow ON when A runs, B should be QUEUED. Same for B and C.",
            ),
            (
                {("scheduler", "schedule_after_task_execution"): "False"},
                {"A": State.QUEUED, "B": State.NONE, "C": State.NONE},
                {"A": State.SUCCESS, "B": State.NONE, "C": State.NONE},
                None,
                ["A", "B"],
                "A -> B -> C, with fast-follow OFF, when A runs, B shouldn't be QUEUED.",
            ),
            (
                {("scheduler", "schedule_after_task_execution"): "True"},
                {"D": State.QUEUED, "E": State.NONE, "F": State.NONE, "G": State.NONE},
                {"D": State.SUCCESS, "E": State.NONE, "F": State.NONE, "G": State.NONE},
                None,
                ["D", "E"],
                "G -> F -> E & D -> E, when D runs but F isn't QUEUED yet, E shouldn't be QUEUED.",
            ),
            (
                {("scheduler", "schedule_after_task_execution"): "True"},
                {"H": State.QUEUED, "I": State.FAILED, "J": State.NONE},
                {"H": State.SUCCESS, "I": State.FAILED, "J": State.UPSTREAM_FAILED},
                None,
                ["H", "I"],
                "H -> J & I -> J, when H is QUEUED but I has FAILED, J is marked UPSTREAM_FAILED.",
            ),
        ],
    )
    def test_fast_follow(
        self,
        conf,
        init_state,
        first_run_state,
        second_run_state,
        task_ids_to_run,
        error_message,
        get_test_dag,
    ):
        with conf_vars(conf):
            dag = get_test_dag(
                "test_dagrun_fast_follow",
            )

            scheduler_job = Job()
            scheduler_job_runner = SchedulerJobRunner(
                job=scheduler_job, subdir=os.devnull
            )
            scheduler_job_runner.dagbag.bag_dag(dag)
            triggered_by_kwargs = (
                {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
            )

            dag_run = dag.create_dagrun(
                run_id="test_dagrun_fast_follow",
                state=State.RUNNING,
                **triggered_by_kwargs,
            )

            ti_by_task_id = {}
            with create_session() as session:
                for task_id in init_state:
                    ti = TaskInstance(dag.get_task(task_id), run_id=dag_run.run_id)
                    ti.refresh_from_db()
                    ti.state = init_state[task_id]
                    session.merge(ti)
                    ti_by_task_id[task_id] = ti

            ti = TaskInstance(
                task=dag.get_task(task_ids_to_run[0]), run_id=dag_run.run_id
            )
            ti.refresh_from_db()
            job1 = Job(executor=SequentialExecutor(), dag_id=ti.dag_id)
            job_runner = LocalTaskJobRunner(
                job=job1, task_instance=ti, ignore_ti_state=True
            )
            job1.task_runner = StandardTaskRunner(job_runner)

            run_job(job=job1, execute_callable=job_runner._execute)
            self.validate_ti_states(dag_run, first_run_state, error_message)
            if second_run_state:
                ti = TaskInstance(
                    task=dag.get_task(task_ids_to_run[1]), run_id=dag_run.run_id
                )
                ti.refresh_from_db()
                job2 = Job(dag_id=ti.dag_id, executor=SequentialExecutor())
                job_runner = LocalTaskJobRunner(
                    job=job2, task_instance=ti, ignore_ti_state=True
                )
                job2.task_runner = StandardTaskRunner(job_runner)
                run_job(job2, execute_callable=job_runner._execute)
                self.validate_ti_states(dag_run, second_run_state, error_message)
            if scheduler_job_runner.processor_agent:
                scheduler_job_runner.processor_agent.end()

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @conf_vars({("scheduler", "schedule_after_task_execution"): "True"})
    def test_mini_scheduler_works_with_wait_for_upstream(self, caplog, get_test_dag):
        dag = get_test_dag("test_dagrun_fast_follow")
        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        dag.catchup = False
        SerializedDagModel.write_dag(dag)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )

        dr = dag.create_dagrun(
            run_id="test_1",
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=data_interval,
            **triggered_by_kwargs,
        )
        dr2 = dag.create_dagrun(
            run_id="test_2",
            state=State.RUNNING,
            execution_date=DEFAULT_DATE + datetime.timedelta(hours=1),
            data_interval=data_interval,
            **triggered_by_kwargs,
        )
        task_k = dag.get_task("K")
        task_l = dag.get_task("L")
        with create_session() as session:
            ti_k = dr.get_task_instance(task_k.task_id, session=session)
            ti_k.refresh_from_task(task_k)
            ti_k.state = State.SUCCESS

            ti_b = dr.get_task_instance(task_l.task_id, session=session)
            ti_b.refresh_from_task(task_l)
            ti_b.state = State.SUCCESS

            ti2_k = dr2.get_task_instance(task_k.task_id, session=session)
            ti2_k.refresh_from_task(task_k)
            ti2_k.state = State.NONE

            ti2_l = dr2.get_task_instance(task_l.task_id, session=session)
            ti2_l.refresh_from_task(task_l)
            ti2_l.state = State.NONE

            session.merge(ti_k)
            session.merge(ti_b)

            session.merge(ti2_k)
            session.merge(ti2_l)

        job1 = Job(
            executor=SequentialExecutor(),
            dag_id=ti2_k.dag_id,
        )
        job_runner = LocalTaskJobRunner(
            job=job1, task_instance=ti2_k, ignore_ti_state=True
        )
        job1.task_runner = StandardTaskRunner(job_runner)
        run_job(job=job1, execute_callable=job_runner._execute)

        ti2_k.refresh_from_db()
        ti2_l.refresh_from_db()
        assert ti2_k.state == State.SUCCESS
        assert ti2_l.state == State.NONE

        failed_deps = list(ti2_l.get_failed_dep_statuses())
        assert len(failed_deps) == 1
        assert failed_deps[0].dep_name == "Previous Dagrun State"
        assert not failed_deps[0].passed

    def test_process_sigsegv_error_message(self, caplog, dag_maker):
        """Test that shows error if process failed with segmentation fault."""
        caplog.set_level(logging.CRITICAL, logger="local_task_job.py")

        def task_function(ti):
            # pytest enable faulthandler by default unless `-p no:faulthandler` is given.
            # It can not be disabled on the test level out of the box and
            # that mean debug traceback would show in pytest output.
            # For avoid this we disable it within the task which run in separate process.
            import faulthandler

            if faulthandler.is_enabled():
                faulthandler.disable()

            while not ti.pid:
                time.sleep(0.1)

            os.kill(psutil.Process(os.getpid()).ppid(), signal.SIGSEGV)

        with dag_maker(dag_id="test_segmentation_fault", serialized=True):
            task = PythonOperator(
                task_id="test_sigsegv",
                python_callable=task_function,
            )
        dag_run = dag_maker.create_dagrun()
        ti = TaskInstance(task=task, run_id=dag_run.run_id)
        ti.refresh_from_db()
        job = Job(executor=SequentialExecutor(), dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        settings.engine.dispose()
        with timeout(10):
            with pytest.raises(AirflowException, match=r"Segmentation Fault detected"):
                run_job(job=job, execute_callable=job_runner._execute)
        assert SIGSEGV_MESSAGE in caplog.messages


@pytest.fixture
def clean_db_helper():
    yield
    db.clear_db_jobs()
    db.clear_db_runs()


@pytest.mark.usefixtures("clean_db_helper")
@mock.patch("airflow.task.task_runner.get_task_runner")
def test_number_of_queries_single_loop(mock_get_task_runner, dag_maker):
    codes: list[int | None] = 9 * [None] + [0]
    mock_get_task_runner.return_value.return_code.side_effects = [[0], codes]

    unique_prefix = str(uuid.uuid4())
    with dag_maker(dag_id=f"{unique_prefix}_test_number_of_queries", serialized=True):
        task = EmptyOperator(task_id="test_state_succeeded1")

    dr = dag_maker.create_dagrun(run_id=unique_prefix, state=State.NONE)

    ti = dr.task_instances[0]
    ti.refresh_from_task(task)

    job = Job(dag_id=ti.dag_id, executor=MockExecutor())
    job_runner = LocalTaskJobRunner(job=job, task_instance=ti)
    with assert_queries_count(18):
        run_job(job=job, execute_callable=job_runner._execute)


class TestSigtermOnRunner:
    """Test receive SIGTERM on Task Runner."""

    @pytest.mark.skip_if_database_isolation_mode  # Does not work in db isolation mode
    @pytest.mark.parametrize(
        "daemon", [pytest.param(True, id="daemon"), pytest.param(False, id="non-daemon")]
    )
    @pytest.mark.parametrize(
        "mp_method, wait_timeout",
        [
            pytest.param(
                "fork",
                10,
                marks=pytest.mark.skipif(
                    not hasattr(os, "fork"), reason="Forking not available"
                ),
                id="fork",
            ),
            pytest.param("spawn", 30, id="spawn"),
        ],
    )
    def test_process_sigterm_works_with_retries(
        self, mp_method, wait_timeout, daemon, clear_db, tmp_path
    ):
        """Test that ensures that task runner sets tasks to retry when task runner receive SIGTERM."""
        mp_context = mp.get_context(mp_method)

        # Use shared memory value, so we can properly track value change
        # even if it's been updated across processes.
        retry_callback_called = mp_context.Value("i", 0)
        task_started = mp_context.Value("i", 0)

        dag_id = f"test_task_runner_sigterm_{mp_method}_{'' if daemon else 'non_'}daemon"
        task_id = "test_on_retry_callback"
        execution_date = DEFAULT_DATE
        run_id = f"test-{execution_date.date().isoformat()}"
        tmp_file = tmp_path / "test.txt"
        # Run LocalTaskJob in separate process
        proc = mp_context.Process(
            target=self._sigterm_local_task_runner,
            args=(
                tmp_file,
                dag_id,
                task_id,
                run_id,
                execution_date,
                task_started,
                retry_callback_called,
            ),
            name="LocalTaskJob-TestProcess",
            daemon=daemon,
        )
        proc.start()

        try:
            with timeout(wait_timeout, "Timeout during waiting start LocalTaskJob"):
                while task_started.value == 0:
                    time.sleep(0.2)
            os.kill(proc.pid, signal.SIGTERM)

            with timeout(wait_timeout, "Timeout during waiting callback"):
                while retry_callback_called.value == 0:
                    time.sleep(0.2)
        finally:
            proc.kill()

        assert retry_callback_called.value == 1
        # Internally callback finished before TaskInstance commit changes in DB (as of Jan 2022).
        # So we can't easily check TaskInstance.state without any race conditions drawbacks,
        # and fact that process with LocalTaskJob could be already killed.
        # We could add state validation (`UP_FOR_RETRY`) if callback mechanism changed.

        captured = tmp_file.read_text()
        expected = (
            "Received SIGTERM. Terminating subprocesses",
            "Task exited with return code 143",
        )
        # It might not appear in case if a process killed before it writes into the logs
        if not all(msg in captured for msg in expected):
            reason = (
                f"https://github.com/apache/airflow/issues/39051: "
                f"Expected to find all messages {', '.join(map(repr, expected,))} "
                f"in the captured logs\n{captured!r}"
            )
            pytest.xfail(reason)

    @staticmethod
    def _sigterm_local_task_runner(
        tmpfile_path,
        dag_id,
        task_id,
        run_id,
        execution_date,
        is_started,
        callback_value,
    ):
        """Helper function which create infinity task and run it by LocalTaskJob."""
        settings.engine.pool.dispose()
        settings.engine.dispose()

        def retry_callback(context):
            assert context["dag_run"].dag_id == dag_id
            with callback_value.get_lock():
                callback_value.value += 1

        def task_function():
            with is_started.get_lock():
                is_started.value = 1

            while True:
                time.sleep(0.25)

        with DAG(dag_id=dag_id, schedule=None, start_date=execution_date) as dag:
            task = PythonOperator(
                task_id=task_id,
                python_callable=task_function,
                retries=1,
                on_retry_callback=retry_callback,
            )
        logger = logging.getLogger()
        tmpfile_handler = logging.FileHandler(tmpfile_path)
        logger.addHandler(tmpfile_handler)

        data_interval = dag.infer_automated_data_interval(DEFAULT_LOGICAL_DATE)
        triggered_by_kwargs = (
            {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        )
        dag_run = dag.create_dagrun(
            state=State.RUNNING,
            run_id=run_id,
            execution_date=execution_date,
            data_interval=data_interval,
            **triggered_by_kwargs,
        )
        ti = TaskInstance(task=task, run_id=dag_run.run_id)
        ti.refresh_from_db()
        job = Job(executor=SequentialExecutor(), dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        run_job(job=job, execute_callable=job_runner._execute)
