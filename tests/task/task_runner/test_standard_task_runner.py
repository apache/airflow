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
from logging.config import dictConfig
from pathlib import Path
from unittest import mock

import psutil
import pytest

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.jobs.local_task_job import LocalTaskJob
from airflow.listeners.listener import get_listener_manager
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.task.task_runner.standard_task_runner import StandardTaskRunner
from airflow.utils import timezone
from airflow.utils.platform import getuser
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timeout import timeout
from tests.listeners.file_write_listener import FileWriteListener
from tests.test_utils.db import clear_db_runs

TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

TASK_FORMAT = "{{%(filename)s:%(lineno)d}} %(levelname)s - %(message)s"

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "airflow.task": {"format": TASK_FORMAT},
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "formatter": "airflow.task",
            "stream": "ext://sys.stdout",
        },
    },
    "loggers": {"airflow": {"handlers": ["console"], "level": "INFO", "propagate": True}},
}


class TestStandardTaskRunner:
    @pytest.fixture(autouse=True, scope="class")
    def logging_and_db(self):
        """
        This fixture sets up logging to have a different setup on the way in
        (as the test environment does not have enough context for the normal
        way to run) and ensures they reset back to normal on the way out.
        """
        get_listener_manager().clear()
        clear_db_runs()
        dictConfig(LOGGING_CONFIG)
        yield
        airflow_logger = logging.getLogger("airflow")
        airflow_logger.handlers = []
        clear_db_runs()
        dictConfig(DEFAULT_LOGGING_CONFIG)
        get_listener_manager().clear()

    def test_start_and_terminate(self):
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.run_as_user = None
        local_task_job.task_instance.command_as_list.return_value = [
            "airflow",
            "tasks",
            "run",
            "test_on_kill",
            "task1",
            "2016-01-01",
        ]

        runner = StandardTaskRunner(local_task_job)
        runner.start()
        # Wait until process sets its pgid to be equal to pid
        with timeout(seconds=1):
            while True:
                runner_pgid = os.getpgid(runner.process.pid)
                if runner_pgid == runner.process.pid:
                    break
                time.sleep(0.01)

        assert runner_pgid > 0
        assert runner_pgid != os.getpgid(0), "Task should be in a different process group to us"
        processes = list(self._procs_in_pgroup(runner_pgid))
        runner.terminate()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

        assert runner.return_code() is not None

    def test_notifies_about_start_and_stop(self):
        path_listener_writer = "/tmp/path_listener_writer"
        try:
            os.unlink(path_listener_writer)
        except OSError:
            pass

        lm = get_listener_manager()
        lm.add_listener(FileWriteListener(path_listener_writer))

        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get("test_example_bash_operator")
        task = dag.get_task("runme_1")

        with create_session() as session:
            dag.create_dagrun(
                run_id="test",
                data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                state=State.RUNNING,
                start_date=DEFAULT_DATE,
                session=session,
            )
            ti = TaskInstance(task=task, run_id="test")
            job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True)
            session.commit()
            ti.refresh_from_task(task)

            runner = StandardTaskRunner(job1)
            runner.start()

            # Wait until process sets its pgid to be equal to pid
            with timeout(seconds=1):
                while True:
                    runner_pgid = os.getpgid(runner.process.pid)
                    if runner_pgid == runner.process.pid:
                        break
                    time.sleep(0.01)

                # Wait till process finishes
            assert runner.return_code(timeout=10) is not None
            with open(path_listener_writer) as f:
                assert f.readline() == "on_starting\n"
                assert f.readline() == "before_stopping\n"

    def test_start_and_terminate_run_as_user(self):
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.task_id = "task_id"
        local_task_job.task_instance.dag_id = "dag_id"
        local_task_job.task_instance.run_as_user = getuser()
        local_task_job.task_instance.command_as_list.return_value = [
            "airflow",
            "tasks",
            "test",
            "test_on_kill",
            "task1",
            "2016-01-01",
        ]

        runner = StandardTaskRunner(local_task_job)

        runner.start()
        time.sleep(0.5)

        pgid = os.getpgid(runner.process.pid)
        assert pgid > 0
        assert pgid != os.getpgid(0), "Task should be in a different process group to us"

        processes = list(self._procs_in_pgroup(pgid))

        runner.terminate()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

        assert runner.return_code() is not None

    def test_early_reap_exit(self, caplog):
        """
        Tests that when a child process running a task is killed externally
        (e.g. by an OOM error, which we fake here), then we get return code
        -9 and a log message.
        """
        # Set up mock task
        local_task_job = mock.Mock()
        local_task_job.task_instance = mock.MagicMock()
        local_task_job.task_instance.task_id = "task_id"
        local_task_job.task_instance.dag_id = "dag_id"
        local_task_job.task_instance.run_as_user = getuser()
        local_task_job.task_instance.command_as_list.return_value = [
            "airflow",
            "tasks",
            "test",
            "test_on_kill",
            "task1",
            "2016-01-01",
        ]

        # Kick off the runner
        runner = StandardTaskRunner(local_task_job)
        runner.start()
        time.sleep(0.2)

        # Kill the child process externally from the runner
        # Note that we have to do this from ANOTHER process, as if we just
        # call os.kill here we're doing it from the parent process and it
        # won't be the same as an external kill in terms of OS tracking.
        pgid = os.getpgid(runner.process.pid)
        os.system(f"kill -s KILL {pgid}")
        time.sleep(0.2)

        runner.terminate()

        assert runner.return_code() == -9
        assert "running out of memory" in caplog.text

    def test_on_kill(self):
        """
        Test that ensures that clearing in the UI SIGTERMS
        the task
        """
        path_on_kill_running = "/tmp/airflow_on_kill_running"
        path_on_kill_killed = "/tmp/airflow_on_kill_killed"
        try:
            os.unlink(path_on_kill_running)
        except OSError:
            pass
        try:
            os.unlink(path_on_kill_killed)
        except OSError:
            pass

        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get("test_on_kill")
        task = dag.get_task("task1")

        with create_session() as session:
            dag.create_dagrun(
                run_id="test",
                data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                state=State.RUNNING,
                start_date=DEFAULT_DATE,
                session=session,
            )
            ti = TaskInstance(task=task, run_id="test")
            job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True)
            session.commit()
            ti.refresh_from_task(task)

            runner = StandardTaskRunner(job1)
            runner.start()

            with timeout(seconds=3):
                while True:
                    runner_pgid = os.getpgid(runner.process.pid)
                    if runner_pgid == runner.process.pid:
                        break
                    time.sleep(0.01)

            processes = list(self._procs_in_pgroup(runner_pgid))

            logging.info("Waiting for the task to start")
            with timeout(seconds=20):
                while True:
                    if os.path.exists(path_on_kill_running):
                        break
                    time.sleep(0.01)
            logging.info("Task started. Give the task some time to settle")
            time.sleep(3)
            logging.info("Terminating processes %s belonging to %s group", processes, runner_pgid)
            runner.terminate()
            session.close()  # explicitly close as `create_session`s commit will blow up otherwise

        ti.refresh_from_db()

        logging.info("Waiting for the on kill killed file to appear")
        with timeout(seconds=4):
            while True:
                if os.path.exists(path_on_kill_killed):
                    break
                time.sleep(0.01)
        logging.info("The file appeared")

        with open(path_on_kill_killed) as f:
            assert "ON_KILL_TEST" == f.readline()

        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"

    def test_parsing_context(self):
        context_file = Path("/tmp/airflow_parsing_context")
        try:
            context_file.unlink()
        except FileNotFoundError:
            pass
        dagbag = DagBag(
            dag_folder=TEST_DAG_FOLDER,
            include_examples=False,
        )
        dag = dagbag.dags.get("test_parsing_context")
        task = dag.get_task("task1")

        with create_session() as session:
            dag.create_dagrun(
                run_id="test",
                data_interval=(DEFAULT_DATE, DEFAULT_DATE),
                state=State.RUNNING,
                start_date=DEFAULT_DATE,
                session=session,
            )
            ti = TaskInstance(task=task, run_id="test")
            job1 = LocalTaskJob(task_instance=ti, ignore_ti_state=True)
            session.commit()
            ti.refresh_from_task(task)

            runner = StandardTaskRunner(job1)
            runner.start()

            # Wait until process sets its pgid to be equal to pid
            with timeout(seconds=1):
                while True:
                    runner_pgid = os.getpgid(runner.process.pid)
                    if runner_pgid == runner.process.pid:
                        break
                    time.sleep(0.01)

            assert runner_pgid > 0
            assert runner_pgid != os.getpgid(0), "Task should be in a different process group to us"
            processes = list(self._procs_in_pgroup(runner_pgid))
            psutil.wait_procs([runner.process])
            session.close()
        for process in processes:
            assert not psutil.pid_exists(process.pid), f"{process} is still alive"
        assert runner.return_code() == 0
        text = context_file.read_text()
        assert (
            text == "_AIRFLOW_PARSING_CONTEXT_DAG_ID=test_parsing_context\n"
            "_AIRFLOW_PARSING_CONTEXT_TASK_ID=task1\n"
        )

    @staticmethod
    def _procs_in_pgroup(pgid):
        for proc in psutil.process_iter(attrs=["pid", "name"]):
            try:
                if os.getpgid(proc.pid) == pgid and proc.pid != 0:
                    yield proc
            except OSError:
                pass
