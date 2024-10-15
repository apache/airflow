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

import logging
import os
import time
from contextlib import contextmanager
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import psutil
import pytest

from airflow.exceptions import AirflowTaskTimeout
from airflow.jobs.job import Job
from airflow.jobs.local_task_job_runner import LocalTaskJobRunner
from airflow.listeners.listener import get_listener_manager
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.task.task_runner.standard_task_runner import StandardTaskRunner
from airflow.utils import timezone
from airflow.utils.log.file_task_handler import FileTaskHandler
from airflow.utils.platform import getuser
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests_common.test_utils.db import clear_db_runs

from tests.listeners import xcom_listener
from tests.listeners.file_write_listener import FileWriteListener

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
TASK_FORMAT = "%(filename)s:%(lineno)d %(levelname)s - %(message)s"

logger = logging.getLogger(__name__)


@contextmanager
def propagate_task_logger():
    """
    Set `airflow.task` logger to propagate.

    Apparently, caplog doesn't work if you don't propagate messages to root.

    But the normal behavior of the `airflow.task` logger is not to propagate.

    When freshly configured, the logger is set to propagate.  However,
    ordinarily when set_context is called, this is set to False.

    To override this behavior, so that the messages make it to caplog, we
    must tell the handler to maintain its current setting.
    """
    logger = logging.getLogger("airflow.task")
    h = logger.handlers[0]
    assert isinstance(h, FileTaskHandler)  # just to make sure / document
    _propagate = h.maintain_propagate
    if _propagate is False:
        h.maintain_propagate = True
    try:
        yield
    finally:
        if _propagate is False:
            h.maintain_propagate = _propagate


@pytest.mark.usefixtures("reset_logging_config")
class TestStandardTaskRunner:
    def setup_class(self):
        """
        This fixture sets up logging to have a different setup on the way in
        (as the test environment does not have enough context for the normal
        way to run) and ensures they reset back to normal on the way out.
        """
        clear_db_runs()
        yield
        clear_db_runs()

    @pytest.fixture(autouse=True)
    def clean_listener_manager(self):
        get_listener_manager().clear()
        yield
        get_listener_manager().clear()

    @mock.patch.object(StandardTaskRunner, "_read_task_utilization")
    @patch("airflow.utils.log.file_task_handler.FileTaskHandler._init_file")
    def test_start_and_terminate(self, mock_init, mock_read_task_utilization):
        mock_init.return_value = "/tmp/any"
        Job = mock.Mock()
        Job.job_type = None
        Job.task_instance = mock.MagicMock()
        Job.task_instance.run_as_user = None
        Job.task_instance.command_as_list.return_value = [
            "airflow",
            "tasks",
            "run",
            "test_on_kill",
            "task1",
            "2016-01-01",
        ]
        job_runner = LocalTaskJobRunner(job=Job, task_instance=Job.task_instance)
        task_runner = StandardTaskRunner(job_runner)
        task_runner.start()
        # Wait until process sets its pgid to be equal to pid
        with timeout(seconds=1):
            while True:
                runner_pgid = os.getpgid(task_runner.process.pid)
                if runner_pgid == task_runner.process.pid:
                    break
                time.sleep(0.01)

        assert runner_pgid > 0
        assert runner_pgid != os.getpgid(0), "Task should be in a different process group to us"
        processes = list(self._procs_in_pgroup(runner_pgid))
        task_runner.terminate()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

        assert task_runner.return_code() is not None
        mock_read_task_utilization.assert_called()

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.db_test
    def test_notifies_about_start_and_stop(self, tmp_path):
        path_listener_writer = tmp_path / "test_notifies_about_start_and_stop"

        lm = get_listener_manager()
        lm.add_listener(FileWriteListener(os.fspath(path_listener_writer)))

        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get("test_example_bash_operator")
        task = dag.get_task("runme_1")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dag.create_dagrun(
            run_id="test",
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=State.RUNNING,
            start_date=DEFAULT_DATE,
            **triggered_by_kwargs,
        )
        ti = TaskInstance(task=task, run_id="test")
        job = Job(dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        task_runner = StandardTaskRunner(job_runner)
        task_runner.start()

        # Wait until process makes itself the leader of its own process group
        with timeout(seconds=1):
            while True:
                runner_pgid = os.getpgid(task_runner.process.pid)
                if runner_pgid == task_runner.process.pid:
                    break
                time.sleep(0.01)

            # Wait till process finishes
        assert task_runner.return_code(timeout=10) is not None
        with path_listener_writer.open() as f:
            assert f.readline() == "on_starting\n"
            assert f.readline() == "on_task_instance_running\n"
            assert f.readline() == "on_task_instance_success\n"
            assert f.readline() == "before_stopping\n"

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.db_test
    def test_notifies_about_fail(self, tmp_path):
        path_listener_writer = tmp_path / "test_notifies_about_fail"

        lm = get_listener_manager()
        lm.add_listener(FileWriteListener(os.fspath(path_listener_writer)))

        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get("test_failing_bash_operator")
        task = dag.get_task("failing_task")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dag.create_dagrun(
            run_id="test",
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=State.RUNNING,
            start_date=DEFAULT_DATE,
            **triggered_by_kwargs,
        )
        ti = TaskInstance(task=task, run_id="test")
        job = Job(dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        task_runner = StandardTaskRunner(job_runner)
        task_runner.start()

        # Wait until process makes itself the leader of its own process group
        with timeout(seconds=1):
            while True:
                runner_pgid = os.getpgid(task_runner.process.pid)
                if runner_pgid == task_runner.process.pid:
                    break
                time.sleep(0.01)

            # Wait till process finishes
        assert task_runner.return_code(timeout=10) is not None
        with path_listener_writer.open() as f:
            assert f.readline() == "on_starting\n"
            assert f.readline() == "on_task_instance_running\n"
            assert f.readline() == "on_task_instance_failed\n"
            assert f.readline() == "before_stopping\n"

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.db_test
    def test_ol_does_not_block_xcoms(self, tmp_path):
        """
        Test that ensures that pushing and pulling xcoms both in listener and task does not collide
        """

        path_listener_writer = tmp_path / "test_ol_does_not_block_xcoms"

        listener = xcom_listener.XComListener(os.fspath(path_listener_writer), "push_and_pull")
        get_listener_manager().add_listener(listener)

        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get("test_dag_xcom_openlineage")
        task = dag.get_task("push_and_pull")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dag.create_dagrun(
            run_id="test",
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=State.RUNNING,
            start_date=DEFAULT_DATE,
            **triggered_by_kwargs,
        )

        ti = TaskInstance(task=task, run_id="test")
        job = Job(dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        task_runner = StandardTaskRunner(job_runner)
        task_runner.start()

        # Wait until process makes itself the leader of its own process group
        with timeout(seconds=1):
            while True:
                runner_pgid = os.getpgid(task_runner.process.pid)
                if runner_pgid == task_runner.process.pid:
                    break
                time.sleep(0.01)

        # Wait till process finishes
        assert task_runner.return_code(timeout=10) is not None

        with path_listener_writer.open() as f:
            assert f.readline() == "on_task_instance_running\n"
            assert f.readline() == "on_task_instance_success\n"
            assert f.readline() == "listener\n"

    @mock.patch.object(StandardTaskRunner, "_read_task_utilization")
    @patch("airflow.utils.log.file_task_handler.FileTaskHandler._init_file")
    def test_start_and_terminate_run_as_user(self, mock_init, mock_read_task_utilization):
        mock_init.return_value = "/tmp/any"
        Job = mock.Mock()
        Job.job_type = None
        Job.task_instance = mock.MagicMock()
        Job.task_instance.task_id = "task_id"
        Job.task_instance.dag_id = "dag_id"
        Job.task_instance.run_as_user = getuser()
        Job.task_instance.command_as_list.return_value = [
            "airflow",
            "tasks",
            "test",
            "test_on_kill",
            "task1",
            "2016-01-01",
        ]
        job_runner = LocalTaskJobRunner(job=Job, task_instance=Job.task_instance)
        task_runner = StandardTaskRunner(job_runner)

        task_runner.start()
        try:
            time.sleep(0.5)

            pgid = os.getpgid(task_runner.process.pid)
            assert pgid > 0
            assert pgid != os.getpgid(0), "Task should be in a different process group to us"

            processes = list(self._procs_in_pgroup(pgid))
        finally:
            task_runner.terminate()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

        assert task_runner.return_code() is not None
        mock_read_task_utilization.assert_called()

    @propagate_task_logger()
    @patch("airflow.utils.log.file_task_handler.FileTaskHandler._init_file")
    def test_early_reap_exit(self, mock_init, caplog):
        """
        Tests that when a child process running a task is killed externally
        (e.g. by an OOM error, which we fake here), then we get return code
        -9 and a log message.
        """
        mock_init.return_value = "/tmp/any"
        Job = mock.Mock()
        Job.job_type = None
        Job.task_instance = mock.MagicMock()
        Job.task_instance.task_id = "task_id"
        Job.task_instance.dag_id = "dag_id"
        Job.task_instance.run_as_user = getuser()
        Job.task_instance.command_as_list.return_value = [
            "airflow",
            "tasks",
            "test",
            "test_on_kill",
            "task1",
            "2016-01-01",
        ]
        job_runner = LocalTaskJobRunner(job=Job, task_instance=Job.task_instance)

        # Kick off the runner
        task_runner = StandardTaskRunner(job_runner)
        task_runner.start()
        time.sleep(0.2)

        # Kill the child process externally from the runner
        # Note that we have to do this from ANOTHER process, as if we just
        # call os.kill here we're doing it from the parent process and it
        # won't be the same as an external kill in terms of OS tracking.
        pgid = os.getpgid(task_runner.process.pid)
        os.system(f"kill -s KILL {pgid}")
        time.sleep(0.2)

        task_runner.terminate()

        assert task_runner.return_code() == -9
        assert "running out of memory" in caplog.text

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.db_test
    def test_on_kill(self):
        """
        Test that ensures that clearing in the UI SIGTERMS
        the task
        """
        path_on_kill_running = Path("/tmp/airflow_on_kill_running")
        path_on_kill_killed = Path("/tmp/airflow_on_kill_killed")
        path_on_kill_running.unlink(missing_ok=True)
        path_on_kill_killed.unlink(missing_ok=True)

        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get("test_on_kill")
        task = dag.get_task("task1")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dag.create_dagrun(
            run_id="test",
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=State.RUNNING,
            start_date=DEFAULT_DATE,
            **triggered_by_kwargs,
        )
        ti = TaskInstance(task=task, run_id="test")
        job = Job(dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        task_runner = StandardTaskRunner(job_runner)
        task_runner.start()

        with timeout(seconds=3):
            while True:
                runner_pgid = os.getpgid(task_runner.process.pid)
                if runner_pgid == task_runner.process.pid:
                    break
                time.sleep(0.01)

        processes = list(self._procs_in_pgroup(runner_pgid))

        logger.info("Waiting for the task to start")
        with timeout(seconds=20):
            while not path_on_kill_running.exists():
                time.sleep(0.01)
        logger.info("Task started. Give the task some time to settle")
        time.sleep(3)
        logger.info("Terminating processes %s belonging to %s group", processes, runner_pgid)
        task_runner.terminate()

        logger.info("Waiting for the on kill killed file to appear")
        with timeout(seconds=4):
            while not path_on_kill_killed.exists():
                time.sleep(0.01)
        logger.info("The file appeared")

        with path_on_kill_killed.open() as f:
            assert "ON_KILL_TEST" == f.readline()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

    @pytest.mark.skip_if_database_isolation_mode  # Test is broken in db isolation mode
    @pytest.mark.db_test
    def test_parsing_context(self):
        context_file = Path("/tmp/airflow_parsing_context")
        context_file.unlink(missing_ok=True)
        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get("test_parsing_context")
        task = dag.get_task("task1")
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}

        dag.create_dagrun(
            run_id="test",
            data_interval=(DEFAULT_DATE, DEFAULT_DATE),
            state=State.RUNNING,
            start_date=DEFAULT_DATE,
            **triggered_by_kwargs,
        )
        ti = TaskInstance(task=task, run_id="test")
        job = Job(dag_id=ti.dag_id)
        job_runner = LocalTaskJobRunner(job=job, task_instance=ti, ignore_ti_state=True)
        task_runner = StandardTaskRunner(job_runner)
        task_runner.start()

        # Wait until process sets its pgid to be equal to pid
        with timeout(seconds=1):
            while True:
                runner_pgid = os.getpgid(task_runner.process.pid)
                if runner_pgid == task_runner.process.pid:
                    break
                time.sleep(0.01)

        assert runner_pgid > 0
        assert runner_pgid != os.getpgid(0), "Task should be in a different process group to us"
        processes = list(self._procs_in_pgroup(runner_pgid))
        psutil.wait_procs([task_runner.process])

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"
        assert task_runner.return_code() == 0
        text = context_file.read_text()
        assert (
            text == "_AIRFLOW_PARSING_CONTEXT_DAG_ID=test_parsing_context\n"
            "_AIRFLOW_PARSING_CONTEXT_TASK_ID=task1\n"
        )

    @pytest.mark.db_test
    @mock.patch("airflow.task.task_runner.standard_task_runner.Stats.gauge")
    @patch("airflow.utils.log.file_task_handler.FileTaskHandler._init_file")
    def test_read_task_utilization(self, mock_init, mock_stats, tmp_path):
        mock_init.return_value = (tmp_path / "test_read_task_utilization.log").as_posix()
        Job = mock.Mock()
        Job.job_type = None
        Job.task_instance = mock.MagicMock()
        Job.task_instance.task_id = "task_id"
        Job.task_instance.dag_id = "dag_id"
        Job.task_instance.run_as_user = None
        Job.task_instance.command_as_list.return_value = [
            "airflow",
            "tasks",
            "run",
            "test_on_kill",
            "task1",
            "2016-01-01",
        ]
        job_runner = LocalTaskJobRunner(job=Job, task_instance=Job.task_instance)
        task_runner = StandardTaskRunner(job_runner)
        task_runner.start()
        try:
            with timeout(1):
                task_runner._read_task_utilization()
        except AirflowTaskTimeout:
            pass
        assert mock_stats.call_count == 2

    @staticmethod
    def _procs_in_pgroup(pgid):
        for proc in psutil.process_iter(attrs=["pid", "name"]):
            try:
                if os.getpgid(proc.pid) == pgid and proc.pid != 0:
                    yield proc
            except OSError:
                pass
