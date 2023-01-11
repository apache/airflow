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
import os
import re
import signal
import threading
import time
import uuid
from multiprocessing import Value
from unittest import mock
from unittest.mock import patch

import psutil
import pytest

from airflow import settings
from airflow.exceptions import AirflowException
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.jobs.local_task_job import SIGSEGV_MESSAGE, LocalTaskJob
from airflow.jobs.scheduler_job import SchedulerJob
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
from tests.test_utils import db
from tests.test_utils.asserts import assert_queries_count
from tests.test_utils.config import conf_vars
from tests.test_utils.mock_executor import MockExecutor

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]


@pytest.fixture
def clear_db():
    db.clear_db_dags()
    db.clear_db_jobs()
    db.clear_db_runs()
    db.clear_db_task_fail()
    yield


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
        with patch("airflow.jobs.base_job.sleep") as self.mock_base_job_sleep:
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
        with dag_maker("test_localtaskjob_essential_attr"):
            op1 = EmptyOperator(task_id="op1")

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
        with dag_maker("test_localtaskjob_heartbeat"):
            op1 = EmptyOperator(task_id="op1")

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

        # Now, set the ti.pid to None and test that no error
        # is raised.
        ti.pid = None
        session.merge(ti)
        session.commit()
        assert ti.pid != job1.task_runner.process.pid
        assert not ti.run_as_user
        assert not job1.task_runner.run_as_user
        job1.heartbeat_callback()

    @mock.patch("subprocess.check_call")
    @mock.patch("airflow.jobs.local_task_job.psutil")
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

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        ti.task = op1
        ti.refresh_from_task(op1)
        job1.task_runner = StandardTaskRunner(job1)
        job1.task_runner.process = mock.Mock()
        job1.task_runner.process.pid = 2
        # Here, ti.pid is 2, the parent process of ti.pid is a mock(different).
        # And task_runner process is 2. Should fail
        with pytest.raises(AirflowException, match="PID of job runner does not match"):
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
        with pytest.raises(AirflowException, match="PID of job runner does not match"):
            job1.heartbeat_callback()

        # Here we set the ti.pid to None and test that no error is
        # raised
        ti.pid = None
        session.merge(ti)
        session.commit()
        assert ti.run_as_user
        assert job1.task_runner.run_as_user == ti.run_as_user
        assert ti.pid != job1.task_runner.process.pid
        job1.heartbeat_callback()

    @conf_vars({("core", "default_impersonation"): "testuser"})
    @mock.patch("subprocess.check_call")
    @mock.patch("airflow.jobs.local_task_job.psutil")
    def test_localtaskjob_heartbeat_with_default_impersonation(self, psutil_mock, _, dag_maker):
        session = settings.Session()
        with dag_maker("test_localtaskjob_heartbeat"):
            op1 = EmptyOperator(task_id="op1")
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
        with pytest.raises(AirflowException, match="PID of job runner does not match"):
            job1.heartbeat_callback()

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
        job1.heartbeat_callback(session=None)

        # Here the task_runner process id is changed to 2
        # while parent process of ti.pid is kept at 1, which is different
        job1.task_runner.process.pid = 2
        with pytest.raises(AirflowException, match="PID of job runner does not match"):
            job1.heartbeat_callback()

        # Now, set the ti.pid to None and test that no error
        # is raised.
        ti.pid = None
        session.merge(ti)
        session.commit()
        assert job1.task_runner.run_as_user == "testuser"
        assert ti.run_as_user is None
        assert ti.pid != job1.task_runner.process.pid
        job1.heartbeat_callback()

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
                assert abs(delta - job.heartrate) < 0.8

    def test_mark_success_no_kill(self, caplog, get_test_dag, session):
        """
        Test that ensures that mark_success in the UI doesn't cause
        the task to fail, and that the task exits
        """
        dag = get_test_dag("test_mark_state")
        dr = dag.create_dagrun(
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            run_type=DagRunType.SCHEDULED,
            session=session,
        )
        task = dag.get_task(task_id="test_mark_success_no_kill")

        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True)

        with timeout(30):
            job1.run()
        ti.refresh_from_db()
        assert State.SUCCESS == ti.state
        assert (
            "State of this instance has been externally set to success. Terminating instance." in caplog.text
        )

    def test_localtaskjob_double_trigger(self):

        dag = self.dagbag.dags.get("test_localtaskjob_double_trigger")
        task = dag.get_task("test_localtaskjob_double_trigger_task")

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
        with patch.object(StandardTaskRunner, "start", return_value=None) as mock_method:
            job1.run()
            mock_method.assert_not_called()

        ti = dr.get_task_instance(task_id=task.task_id, session=session)
        assert ti.pid == 1
        assert ti.state == State.RUNNING

        session.close()

    @patch.object(StandardTaskRunner, "return_code")
    @mock.patch("airflow.jobs.scheduler_job.Stats.incr", autospec=True)
    def test_local_task_return_code_metric(self, mock_stats_incr, mock_return_code, create_dummy_dag):

        _, task = create_dummy_dag("test_localtaskjob_code")

        ti_run = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti_run.refresh_from_db()
        job1 = LocalTaskJob(task_instance=ti_run, executor=SequentialExecutor())
        job1.id = 95

        mock_return_code.side_effect = [None, -9, None]

        with timeout(10):
            job1.run()

        mock_stats_incr.assert_has_calls(
            [
                mock.call("local_task_job.task_exit.95.test_localtaskjob_code.op1.-9"),
            ]
        )

    @patch.object(StandardTaskRunner, "return_code")
    def test_localtaskjob_maintain_heart_rate(self, mock_return_code, caplog, create_dummy_dag):

        _, task = create_dummy_dag("test_localtaskjob_double_trigger")

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

    def test_mark_failure_on_failure_callback(self, caplog, get_test_dag):
        """
        Test that ensures that mark_failure in the UI fails
        the task, and executes on_failure_callback
        """
        dag = get_test_dag("test_mark_state")
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
        task = dag.get_task(task_id="test_mark_failure_externally")
        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        with timeout(30):
            # This should be _much_ shorter to run.
            # If you change this limit, make the timeout in the callable above bigger
            job1.run()

        ti.refresh_from_db()
        assert ti.state == State.FAILED
        assert (
            "State of this instance has been externally set to failed. Terminating instance."
        ) in caplog.text

    def test_dagrun_timeout_logged_in_task_logs(self, caplog, get_test_dag):
        """
        Test that ensures that if a running task is externally skipped (due to a dagrun timeout)
        It is logged in the task logs.
        """
        dag = get_test_dag("test_mark_state")
        dag.dagrun_timeout = datetime.timedelta(microseconds=1)
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                start_date=DEFAULT_DATE,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
        task = dag.get_task(task_id="test_mark_skipped_externally")
        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        with timeout(30):
            # This should be _much_ shorter to run.
            # If you change this limit, make the timeout in the callable above bigger
            job1.run()

        ti.refresh_from_db()
        assert ti.state == State.SKIPPED
        assert "DagRun timed out after " in caplog.text

    def test_failure_callback_called_by_airflow_run_raw_process(self, monkeypatch, tmp_path, get_test_dag):
        """
        Ensure failure callback of a task is run by the airflow run --raw process
        """
        callback_file = tmp_path.joinpath("callback.txt")
        callback_file.touch()
        monkeypatch.setenv("AIRFLOW_CALLBACK_FILE", str(callback_file))
        dag = get_test_dag("test_on_failure_callback")
        with create_session() as session:
            dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
        task = dag.get_task(task_id="test_on_failure_callback_task")
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
        ti.refresh_from_db()

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        job1.run()

        ti.refresh_from_db()
        assert ti.state == State.FAILED  # task exits with failure state
        with open(callback_file) as f:
            lines = f.readlines()
        assert len(lines) == 1  # invoke once
        assert lines[0].startswith(ti.key.primary)
        m = re.match(r"^.+pid: (\d+)$", lines[0])
        assert m, "pid expected in output."
        assert os.getpid() != int(m.group(1))

    def test_mark_success_on_success_callback(self, caplog, get_test_dag):
        """
        Test that ensures that where a task is marked success in the UI
        on_success_callback gets executed
        """
        dag = get_test_dag("test_mark_state")
        with create_session() as session:
            dr = dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
        task = dag.get_task(task_id="test_mark_success_no_kill")

        ti = dr.get_task_instance(task.task_id)
        ti.refresh_from_task(task)

        job = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())

        with timeout(30):
            job.run()  # This should run fast because of the return_code=None
        ti.refresh_from_db()
        assert (
            "State of this instance has been externally set to success. Terminating instance." in caplog.text
        )

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
        with create_session() as session:
            dag.create_dagrun(
                state=State.RUNNING,
                execution_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                session=session,
            )
        task = dag.get_task(task_id="bash_sleep")
        ti = TaskInstance(task=task, execution_date=DEFAULT_DATE)
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
                    task_pid and ti.current_state() == State.RUNNING and os.path.isfile(callback_file)
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

        job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        job1.run()

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
            assert ti.pid == pid  # ensures callback is run by airflow run --raw (TaskInstance#_run_raw_task)
        elif signal_type == signal.SIGKILL:
            assert (
                ti.state == State.RUNNING
            )  # task exits with running state, will be reaped as zombie by scheduler
            with open(callback_file) as f:
                lines = f.readlines()
            assert len(lines) == 0

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

            scheduler_job = SchedulerJob(subdir=os.devnull)
            scheduler_job.dagbag.bag_dag(dag, root_dag=dag)

            dag_run = dag.create_dagrun(run_id="test_dagrun_fast_follow", state=State.RUNNING)

            ti_by_task_id = {}
            with create_session() as session:
                for task_id in init_state:
                    ti = TaskInstance(dag.get_task(task_id), run_id=dag_run.run_id, state=init_state[task_id])
                    session.merge(ti)
                    ti_by_task_id[task_id] = ti

            ti = TaskInstance(task=dag.get_task(task_ids_to_run[0]), execution_date=dag_run.execution_date)
            ti.refresh_from_db()
            job1 = LocalTaskJob(
                task_instance=ti,
                ignore_ti_state=True,
                executor=SequentialExecutor(),
            )
            job1.task_runner = StandardTaskRunner(job1)

            job1.run()
            self.validate_ti_states(dag_run, first_run_state, error_message)
            if second_run_state:
                ti = TaskInstance(
                    task=dag.get_task(task_ids_to_run[1]), execution_date=dag_run.execution_date
                )
                ti.refresh_from_db()
                job2 = LocalTaskJob(
                    task_instance=ti,
                    ignore_ti_state=True,
                    executor=SequentialExecutor(),
                )
                job2.task_runner = StandardTaskRunner(job2)
                job2.run()
                self.validate_ti_states(dag_run, second_run_state, error_message)
            if scheduler_job.processor_agent:
                scheduler_job.processor_agent.end()

    @conf_vars({("scheduler", "schedule_after_task_execution"): "True"})
    def test_mini_scheduler_works_with_wait_for_upstream(self, caplog, get_test_dag):
        dag = get_test_dag("test_dagrun_fast_follow")
        dag.catchup = False
        SerializedDagModel.write_dag(dag)

        dr = dag.create_dagrun(run_id="test_1", state=State.RUNNING, execution_date=DEFAULT_DATE)
        dr2 = dag.create_dagrun(
            run_id="test_2", state=State.RUNNING, execution_date=DEFAULT_DATE + datetime.timedelta(hours=1)
        )
        task_k = dag.get_task("K")
        task_l = dag.get_task("L")
        with create_session() as session:
            ti_k = TaskInstance(task_k, run_id=dr.run_id, state=State.SUCCESS)
            ti_b = TaskInstance(task_l, run_id=dr.run_id, state=State.SUCCESS)

            ti2_k = TaskInstance(task_k, run_id=dr2.run_id, state=State.NONE)
            ti2_l = TaskInstance(task_l, run_id=dr2.run_id, state=State.NONE)

            session.merge(ti_k)
            session.merge(ti_b)

            session.merge(ti2_k)
            session.merge(ti2_l)

        job1 = LocalTaskJob(task_instance=ti2_k, ignore_ti_state=True, executor=SequentialExecutor())
        job1.task_runner = StandardTaskRunner(job1)
        job1.run()

        ti2_k.refresh_from_db()
        ti2_l.refresh_from_db()
        assert ti2_k.state == State.SUCCESS
        assert ti2_l.state == State.NONE

        failed_deps = list(ti2_l.get_failed_dep_statuses())
        assert len(failed_deps) == 1
        assert failed_deps[0].dep_name == "Previous Dagrun State"
        assert not failed_deps[0].passed

    @pytest.mark.quarantined
    def test_process_sigterm_works_with_retries(self, caplog, dag_maker):
        """
        Test that ensures that task runner sets tasks to retry when they(task runner)
         receive sigterm
        """
        # use shared memory value so we can properly track value change even if
        # it's been updated across processes.
        retry_callback_called = Value("i", 0)

        def retry_callback(context):
            with retry_callback_called.get_lock():
                retry_callback_called.value += 1
            assert context["dag_run"].dag_id == "test_mark_failure_2"

        def task_function(ti):
            while not ti.pid:
                time.sleep(0.1)
            os.kill(psutil.Process(os.getpid()).ppid(), signal.SIGTERM)

        with dag_maker(dag_id="test_mark_failure_2"):
            task = PythonOperator(
                task_id="test_on_failure",
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

        with dag_maker(dag_id="test_segmentation_fault"):
            task = PythonOperator(
                task_id="test_sigsegv",
                python_callable=task_function,
            )
        dag_run = dag_maker.create_dagrun()
        ti = TaskInstance(task=task, run_id=dag_run.run_id)
        ti.refresh_from_db()
        job = LocalTaskJob(task_instance=ti, ignore_ti_state=True, executor=SequentialExecutor())
        settings.engine.dispose()
        with timeout(10):
            with pytest.raises(AirflowException, match=r"Segmentation Fault detected"):
                job.run()
        assert SIGSEGV_MESSAGE in caplog.messages


@pytest.fixture()
def clean_db_helper():
    yield
    db.clear_db_jobs()
    db.clear_db_runs()


@pytest.mark.usefixtures("clean_db_helper")
@mock.patch("airflow.jobs.local_task_job.get_task_runner")
def test_number_of_queries_single_loop(mock_get_task_runner, dag_maker):
    codes: list[int | None] = 9 * [None] + [0]
    mock_get_task_runner.return_value.return_code.side_effects = [[0], codes]

    unique_prefix = str(uuid.uuid4())
    with dag_maker(dag_id=f"{unique_prefix}_test_number_of_queries"):
        task = EmptyOperator(task_id="test_state_succeeded1")

    dr = dag_maker.create_dagrun(run_id=unique_prefix, state=State.NONE)

    ti = dr.task_instances[0]
    ti.refresh_from_task(task)

    job = LocalTaskJob(task_instance=ti, executor=MockExecutor())
    with assert_queries_count(18):
        job.run()
