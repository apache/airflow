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
import logging.config
import os
import re
from http import HTTPStatus
from importlib import reload
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pendulum
import pytest
from pydantic.v1.utils import deep_update
from requests.adapters import Response

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.executors import executor_loader
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.trigger import Trigger
from airflow.operators.python import PythonOperator
from airflow.utils.log.file_task_handler import (
    FileTaskHandler,
    LogType,
    _fetch_logs_from_service,
    _interleave_logs,
    _parse_timestamps_in_log_file,
)
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType
from tests.test_utils.compat import AIRFLOW_V_3_0_PLUS
from tests.test_utils.config import conf_vars

if AIRFLOW_V_3_0_PLUS:
    from airflow.utils.types import DagRunTriggeredByType

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

DEFAULT_DATE = datetime(2016, 1, 1)
TASK_LOGGER = "airflow.task"
FILE_TASK_HANDLER = "task"


class TestFileTaskLogHandler:
    def clean_up(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TaskInstance).delete()

    def setup_method(self):
        logging.config.dictConfig(DEFAULT_LOGGING_CONFIG)
        logging.root.disabled = False
        self.clean_up()
        # We use file task handler by default.

    def teardown_method(self):
        self.clean_up()

    def test_default_task_logging_setup(self):
        # file task handler is used by default.
        logger = logging.getLogger(TASK_LOGGER)
        handlers = logger.handlers
        assert len(handlers) == 1
        handler = handlers[0]
        assert handler.name == FILE_TASK_HANDLER

    def test_file_task_handler_when_ti_value_is_invalid(self):
        def task_callable(ti):
            ti.log.info("test")

        dag = DAG("dag_for_testing_file_task_handler", schedule=None, start_date=DEFAULT_DATE)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        task = PythonOperator(
            task_id="task_for_testing_file_log_handler",
            dag=dag,
            python_callable=task_callable,
        )
        ti = TaskInstance(task=task, run_id=dagrun.run_id)

        logger = ti.log
        ti.log.disabled = False

        file_handler = next(
            (handler for handler in logger.handlers if handler.name == FILE_TASK_HANDLER), None
        )
        assert file_handler is not None

        set_context(logger, ti)
        assert file_handler.handler is not None
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        assert os.path.isfile(log_filename)
        assert log_filename.endswith("0.log"), log_filename

        ti.run(ignore_ti_state=True)

        file_handler.flush()
        file_handler.close()

        assert hasattr(file_handler, "read")
        # Return value of read must be a tuple of list and list.
        # passing invalid `try_number` to read function
        logs, metadatas = file_handler.read(ti, 0)
        assert isinstance(logs, list)
        assert isinstance(metadatas, list)
        assert len(logs) == 1
        assert len(logs) == len(metadatas)
        assert isinstance(metadatas[0], dict)
        assert logs[0][0][0] == "default_host"
        assert logs[0][0][1] == "Error fetching the logs. Try number 0 is invalid."

        # Remove the generated tmp log file.
        os.remove(log_filename)

    def test_file_task_handler(self):
        def task_callable(ti):
            ti.log.info("test")

        dag = DAG("dag_for_testing_file_task_handler", schedule=None, start_date=DEFAULT_DATE)
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        task = PythonOperator(
            task_id="task_for_testing_file_log_handler",
            dag=dag,
            python_callable=task_callable,
        )
        ti = TaskInstance(task=task, run_id=dagrun.run_id)
        ti.try_number += 1
        logger = ti.log
        ti.log.disabled = False

        file_handler = next(
            (handler for handler in logger.handlers if handler.name == FILE_TASK_HANDLER), None
        )
        assert file_handler is not None

        set_context(logger, ti)
        assert file_handler.handler is not None
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        assert os.path.isfile(log_filename)
        assert log_filename.endswith("1.log"), log_filename

        ti.run(ignore_ti_state=True)

        file_handler.flush()
        file_handler.close()

        assert hasattr(file_handler, "read")
        # Return value of read must be a tuple of list and list.
        logs, metadatas = file_handler.read(ti)
        assert isinstance(logs, list)
        assert isinstance(metadatas, list)
        assert len(logs) == 1
        assert len(logs) == len(metadatas)
        assert isinstance(metadatas[0], dict)
        target_re = r"\n\[[^\]]+\] {test_log_handlers.py:\d+} INFO - test\n"

        # We should expect our log line from the callable above to appear in
        # the logs we read back
        assert re.search(target_re, logs[0][0][-1]), "Logs were " + str(logs)

        # Remove the generated tmp log file.
        os.remove(log_filename)

    def test_file_task_handler_running(self):
        def task_callable(ti):
            ti.log.info("test")

        dag = DAG("dag_for_testing_file_task_handler", schedule=None, start_date=DEFAULT_DATE)
        task = PythonOperator(
            task_id="task_for_testing_file_log_handler",
            python_callable=task_callable,
            dag=dag,
        )
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        ti = TaskInstance(task=task, run_id=dagrun.run_id)

        ti.try_number = 2
        ti.state = State.RUNNING

        logger = ti.log
        ti.log.disabled = False

        file_handler = next(
            (handler for handler in logger.handlers if handler.name == FILE_TASK_HANDLER), None
        )
        assert file_handler is not None

        set_context(logger, ti)
        assert file_handler.handler is not None
        # We expect set_context generates a file locally.
        log_filename = file_handler.handler.baseFilename
        assert os.path.isfile(log_filename)
        assert log_filename.endswith("2.log"), log_filename

        logger.info("Test")

        # Return value of read must be a tuple of list and list.
        logs, metadatas = file_handler.read(ti)
        assert isinstance(logs, list)
        # Logs for running tasks should show up too.
        assert isinstance(logs, list)
        assert isinstance(metadatas, list)
        assert len(logs) == 2
        assert len(logs) == len(metadatas)
        assert isinstance(metadatas[0], dict)

        # Remove the generated tmp log file.
        os.remove(log_filename)

    def test_file_task_handler_rotate_size_limit(self):
        def reset_log_config(update_conf):
            import logging.config

            logging_config = DEFAULT_LOGGING_CONFIG
            logging_config = deep_update(logging_config, update_conf)
            logging.config.dictConfig(logging_config)

        def task_callable(ti):
            pass

        max_bytes_size = 60000
        update_conf = {"handlers": {"task": {"max_bytes": max_bytes_size, "backup_count": 1}}}
        reset_log_config(update_conf)
        dag = DAG("dag_for_testing_file_task_handler_rotate_size_limit", start_date=DEFAULT_DATE)
        task = PythonOperator(
            task_id="task_for_testing_file_log_handler_rotate_size_limit",
            python_callable=task_callable,
            dag=dag,
        )
        triggered_by_kwargs = {"triggered_by": DagRunTriggeredByType.TEST} if AIRFLOW_V_3_0_PLUS else {}
        dagrun = dag.create_dagrun(
            run_type=DagRunType.MANUAL,
            state=State.RUNNING,
            execution_date=DEFAULT_DATE,
            data_interval=dag.timetable.infer_manual_data_interval(run_after=DEFAULT_DATE),
            **triggered_by_kwargs,
        )
        ti = TaskInstance(task=task, run_id=dagrun.run_id)

        ti.try_number = 1
        ti.state = State.RUNNING

        logger = ti.log
        ti.log.disabled = False

        file_handler = next(
            (handler for handler in logger.handlers if handler.name == FILE_TASK_HANDLER), None
        )
        assert file_handler is not None

        set_context(logger, ti)
        assert file_handler.handler is not None
        # We expect set_context generates a file locally, this is the first log file
        # in this test, it should generate 2 when it finishes.
        log_filename = file_handler.handler.baseFilename
        assert os.path.isfile(log_filename)
        assert log_filename.endswith("1.log"), log_filename

        # mock to generate 2000 lines of log, the total size is larger than max_bytes_size
        for i in range(1, 2000):
            logger.info("this is a Test. %s", i)

        # this is the rotate log file
        log_rotate_1_name = log_filename + ".1"
        assert os.path.isfile(log_rotate_1_name)

        current_file_size = os.path.getsize(log_filename)
        rotate_file_1_size = os.path.getsize(log_rotate_1_name)
        assert rotate_file_1_size > max_bytes_size * 0.9
        assert rotate_file_1_size < max_bytes_size
        assert current_file_size < max_bytes_size

        # Return value of read must be a tuple of list and list.
        logs, metadatas = file_handler.read(ti)

        # the log content should have the filename of both current log file and rotate log file.
        find_current_log = False
        find_rotate_log_1 = False
        for log in logs:
            if log_filename in str(log):
                find_current_log = True
            if log_rotate_1_name in str(log):
                find_rotate_log_1 = True
        assert find_current_log is True
        assert find_rotate_log_1 is True

        assert isinstance(logs, list)
        # Logs for running tasks should show up too.
        assert isinstance(logs, list)
        assert isinstance(metadatas, list)
        assert len(logs) == len(metadatas)
        assert isinstance(metadatas[0], dict)

        # Remove the two generated tmp log files.
        os.remove(log_filename)
        os.remove(log_rotate_1_name)

    @patch("airflow.utils.log.file_task_handler.FileTaskHandler._read_from_local")
    def test__read_when_local(self, mock_read_local, create_task_instance):
        """
        Test if local log file exists, then values returned from _read_from_local should be incorporated
        into returned log.
        """
        path = Path(
            "dag_id=dag_for_testing_local_log_read/run_id=scheduled__2016-01-01T00:00:00+00:00/task_id=task_for_testing_local_log_read/attempt=1.log"
        )
        mock_read_local.return_value = (["the messages"], ["the log"])
        local_log_file_read = create_task_instance(
            dag_id="dag_for_testing_local_log_read",
            task_id="task_for_testing_local_log_read",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )
        fth = FileTaskHandler("")
        actual = fth._read(ti=local_log_file_read, try_number=1)
        mock_read_local.assert_called_with(path)
        assert actual == ("*** the messages\nthe log", {"end_of_log": True, "log_pos": 7})

    def test__read_from_local(self, tmp_path):
        """Tests the behavior of method _read_from_local"""

        path1 = tmp_path / "hello1.log"
        path2 = tmp_path / "hello1.log.suffix.log"
        path1.write_text("file1 content")
        path2.write_text("file2 content")
        fth = FileTaskHandler("")
        assert fth._read_from_local(path1) == (
            [
                "Found local files:",
                f"  * {path1}",
                f"  * {path2}",
            ],
            ["file1 content", "file2 content"],
        )

    @pytest.mark.parametrize(
        "remote_logs, local_logs, served_logs_checked",
        [
            (True, True, False),
            (True, False, False),
            (False, True, False),
            (False, False, True),
        ],
    )
    def test__read_served_logs_checked_when_done_and_no_local_or_remote_logs(
        self, create_task_instance, remote_logs, local_logs, served_logs_checked
    ):
        """
        Generally speaking when a task is done we should not read from logs server,
        because we assume for log persistence that users will either set up shared
        drive or enable remote logging.  But if they don't do that, and therefore
        we don't find remote or local logs, we'll check worker for served logs as
        a fallback.
        """
        executor_name = "CeleryExecutor"

        ti = create_task_instance(
            dag_id="dag_for_testing_celery_executor_log_read",
            task_id="task_for_testing_celery_executor_log_read",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )
        ti.state = TaskInstanceState.SUCCESS  # we're testing scenario when task is done
        with conf_vars({("core", "executor"): executor_name}):
            reload(executor_loader)
            fth = FileTaskHandler("")
            if remote_logs:
                fth._read_remote_logs = mock.Mock()
                fth._read_remote_logs.return_value = ["found remote logs"], ["remote\nlog\ncontent"]
            if local_logs:
                fth._read_from_local = mock.Mock()
                fth._read_from_local.return_value = ["found local logs"], ["local\nlog\ncontent"]
            fth._read_from_logs_server = mock.Mock()
            fth._read_from_logs_server.return_value = ["this message"], ["this\nlog\ncontent"]
            actual = fth._read(ti=ti, try_number=1)
        if served_logs_checked:
            fth._read_from_logs_server.assert_called_once()
            assert actual == ("*** this message\nthis\nlog\ncontent", {"end_of_log": True, "log_pos": 16})
        else:
            fth._read_from_logs_server.assert_not_called()
            assert actual[0]
            assert actual[1]

    def test_add_triggerer_suffix(self):
        sample = "any/path/to/thing.txt"
        assert FileTaskHandler.add_triggerer_suffix(sample) == sample + ".trigger"
        assert FileTaskHandler.add_triggerer_suffix(sample, job_id=None) == sample + ".trigger"
        assert FileTaskHandler.add_triggerer_suffix(sample, job_id=123) == sample + ".trigger.123.log"
        assert FileTaskHandler.add_triggerer_suffix(sample, job_id="123") == sample + ".trigger.123.log"

    @pytest.mark.parametrize("is_a_trigger", [True, False])
    def test_set_context_trigger(self, create_dummy_dag, dag_maker, is_a_trigger, session, tmp_path):
        create_dummy_dag(dag_id="test_fth", task_id="dummy")
        (ti,) = dag_maker.create_dagrun(execution_date=pendulum.datetime(2023, 1, 1, tz="UTC")).task_instances
        assert isinstance(ti, TaskInstance)
        if is_a_trigger:
            ti.is_trigger_log_context = True
            job = Job()
            t = Trigger("", {})
            t.triggerer_job = job
            session.add(t)
            ti.triggerer = t
            t.task_instance = ti
        h = FileTaskHandler(base_log_folder=os.fspath(tmp_path))
        h.set_context(ti)
        expected = "dag_id=test_fth/run_id=test/task_id=dummy/attempt=0.log"
        if is_a_trigger:
            expected += f".trigger.{job.id}.log"
        actual = h.handler.baseFilename
        assert actual == os.fspath(tmp_path / expected)


class TestFilenameRendering:
    def test_python_formatting(self, create_log_template, create_task_instance):
        create_log_template("{dag_id}/{task_id}/{execution_date}/{try_number}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{DEFAULT_DATE.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert expected_filename == rendered_filename

    def test_jinja_rendering(self, create_log_template, create_task_instance):
        create_log_template("{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{DEFAULT_DATE.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert expected_filename == rendered_filename


class TestLogUrl:
    def test_log_retrieval_valid(self, create_task_instance):
        log_url_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )
        log_url_ti.hostname = "hostname"
        actual = FileTaskHandler("")._get_log_retrieval_url(log_url_ti, "DYNAMIC_PATH")
        assert actual == ("http://hostname:8793/log/DYNAMIC_PATH", "DYNAMIC_PATH")

    def test_log_retrieval_valid_trigger(self, create_task_instance):
        ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            execution_date=DEFAULT_DATE,
        )
        ti.hostname = "hostname"
        trigger = Trigger("", {})
        job = Job(TriggererJobRunner.job_type)
        job.id = 123
        trigger.triggerer_job = job
        ti.trigger = trigger
        actual = FileTaskHandler("")._get_log_retrieval_url(ti, "DYNAMIC_PATH", log_type=LogType.TRIGGER)
        hostname = get_hostname()
        assert actual == (
            f"http://{hostname}:8794/log/DYNAMIC_PATH.trigger.123.log",
            "DYNAMIC_PATH.trigger.123.log",
        )


log_sample = """[2022-11-16T00:05:54.278-0800] {taskinstance.py:1257} INFO -
--------------------------------------------------------------------------------
[2022-11-16T00:05:54.278-0800] {taskinstance.py:1258} INFO - Starting attempt 1 of 1
[2022-11-16T00:05:54.279-0800] {taskinstance.py:1259} INFO -
--------------------------------------------------------------------------------
[2022-11-16T00:05:54.295-0800] {taskinstance.py:1278} INFO - Executing <Task(TimeDeltaSensorAsync): wait> on 2022-11-16 08:05:52.324532+00:00
[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task
[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task
[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task
[2022-11-16T00:05:54.306-0800] {standard_task_runner.py:82} INFO - Running: ['airflow', 'tasks', 'run', 'simple_async_timedelta', 'wait', 'manual__2022-11-16T08:05:52.324532+00:00', '--job-id', '33648', '--raw', '--subdir', '/Users/dstandish/code/airflow/airflow/example_dags/example_time_delta_sensor_async.py', '--cfg-path', '/var/folders/7_/1xx0hqcs3txd7kqt0ngfdjth0000gn/T/tmp725r305n']
[2022-11-16T00:05:54.309-0800] {standard_task_runner.py:83} INFO - Job 33648: Subtask wait
[2022-11-16T00:05:54.457-0800] {task_command.py:376} INFO - Running <TaskInstance: simple_async_timedelta.wait manual__2022-11-16T08:05:52.324532+00:00 [running]> on host daniels-mbp-2.lan
[2022-11-16T00:05:54.592-0800] {taskinstance.py:1485} INFO - Exporting the following env vars:
AIRFLOW_CTX_DAG_OWNER=airflow
AIRFLOW_CTX_DAG_ID=simple_async_timedelta
AIRFLOW_CTX_TASK_ID=wait
AIRFLOW_CTX_EXECUTION_DATE=2022-11-16T08:05:52.324532+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=manual__2022-11-16T08:05:52.324532+00:00
[2022-11-16T00:05:54.604-0800] {taskinstance.py:1360} INFO - Pausing task as DEFERRED. dag_id=simple_async_timedelta, task_id=wait, execution_date=20221116T080552, start_date=20221116T080554
"""


def test_parse_timestamps():
    actual = []
    for timestamp, _, _ in _parse_timestamps_in_log_file(log_sample.splitlines()):
        actual.append(timestamp)
    assert actual == [
        pendulum.parse("2022-11-16T00:05:54.278000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.278000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.278000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.279000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.279000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.295000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.300000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.300000-08:00"),  # duplicate
        pendulum.parse("2022-11-16T00:05:54.300000-08:00"),  # duplicate
        pendulum.parse("2022-11-16T00:05:54.306000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.309000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.457000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.592000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.592000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.592000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.592000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.592000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.592000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.592000-08:00"),
        pendulum.parse("2022-11-16T00:05:54.604000-08:00"),
    ]


def test_interleave_interleaves():
    log_sample1 = "\n".join(
        [
            "[2022-11-16T00:05:54.278-0800] {taskinstance.py:1258} INFO - Starting attempt 1 of 1",
        ]
    )
    log_sample2 = "\n".join(
        [
            "[2022-11-16T00:05:54.295-0800] {taskinstance.py:1278} INFO - Executing <Task(TimeDeltaSensorAsync): wait> on 2022-11-16 08:05:52.324532+00:00",
            "[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task",
            "[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task",
            "[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task",
            "[2022-11-16T00:05:54.306-0800] {standard_task_runner.py:82} INFO - Running: ['airflow', 'tasks', 'run', 'simple_async_timedelta', 'wait', 'manual__2022-11-16T08:05:52.324532+00:00', '--job-id', '33648', '--raw', '--subdir', '/Users/dstandish/code/airflow/airflow/example_dags/example_time_delta_sensor_async.py', '--cfg-path', '/var/folders/7_/1xx0hqcs3txd7kqt0ngfdjth0000gn/T/tmp725r305n']",
            "[2022-11-16T00:05:54.309-0800] {standard_task_runner.py:83} INFO - Job 33648: Subtask wait",
        ]
    )
    log_sample3 = "\n".join(
        [
            "[2022-11-16T00:05:54.457-0800] {task_command.py:376} INFO - Running <TaskInstance: simple_async_timedelta.wait manual__2022-11-16T08:05:52.324532+00:00 [running]> on host daniels-mbp-2.lan",
            "[2022-11-16T00:05:54.592-0800] {taskinstance.py:1485} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER=airflow",
            "AIRFLOW_CTX_DAG_ID=simple_async_timedelta",
            "AIRFLOW_CTX_TASK_ID=wait",
            "AIRFLOW_CTX_EXECUTION_DATE=2022-11-16T08:05:52.324532+00:00",
            "AIRFLOW_CTX_TRY_NUMBER=1",
            "AIRFLOW_CTX_DAG_RUN_ID=manual__2022-11-16T08:05:52.324532+00:00",
            "[2022-11-16T00:05:54.604-0800] {taskinstance.py:1360} INFO - Pausing task as DEFERRED. dag_id=simple_async_timedelta, task_id=wait, execution_date=20221116T080552, start_date=20221116T080554",
        ]
    )
    expected = "\n".join(
        [
            "[2022-11-16T00:05:54.278-0800] {taskinstance.py:1258} INFO - Starting attempt 1 of 1",
            "[2022-11-16T00:05:54.295-0800] {taskinstance.py:1278} INFO - Executing <Task(TimeDeltaSensorAsync): wait> on 2022-11-16 08:05:52.324532+00:00",
            "[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task",
            "[2022-11-16T00:05:54.306-0800] {standard_task_runner.py:82} INFO - Running: ['airflow', 'tasks', 'run', 'simple_async_timedelta', 'wait', 'manual__2022-11-16T08:05:52.324532+00:00', '--job-id', '33648', '--raw', '--subdir', '/Users/dstandish/code/airflow/airflow/example_dags/example_time_delta_sensor_async.py', '--cfg-path', '/var/folders/7_/1xx0hqcs3txd7kqt0ngfdjth0000gn/T/tmp725r305n']",
            "[2022-11-16T00:05:54.309-0800] {standard_task_runner.py:83} INFO - Job 33648: Subtask wait",
            "[2022-11-16T00:05:54.457-0800] {task_command.py:376} INFO - Running <TaskInstance: simple_async_timedelta.wait manual__2022-11-16T08:05:52.324532+00:00 [running]> on host daniels-mbp-2.lan",
            "[2022-11-16T00:05:54.592-0800] {taskinstance.py:1485} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER=airflow",
            "AIRFLOW_CTX_DAG_ID=simple_async_timedelta",
            "AIRFLOW_CTX_TASK_ID=wait",
            "AIRFLOW_CTX_EXECUTION_DATE=2022-11-16T08:05:52.324532+00:00",
            "AIRFLOW_CTX_TRY_NUMBER=1",
            "AIRFLOW_CTX_DAG_RUN_ID=manual__2022-11-16T08:05:52.324532+00:00",
            "[2022-11-16T00:05:54.604-0800] {taskinstance.py:1360} INFO - Pausing task as DEFERRED. dag_id=simple_async_timedelta, task_id=wait, execution_date=20221116T080552, start_date=20221116T080554",
        ]
    )
    assert "\n".join(_interleave_logs(log_sample2, log_sample1, log_sample3)) == expected


long_sample = """
*** yoyoyoyo
[2023-01-15T22:36:46.474-0800] {taskinstance.py:1131} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [queued]>
[2023-01-15T22:36:46.482-0800] {taskinstance.py:1131} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [queued]>
[2023-01-15T22:36:46.483-0800] {taskinstance.py:1332} INFO - Starting attempt 1 of 1
[2023-01-15T22:36:46.516-0800] {taskinstance.py:1351} INFO - Executing <Task(TimeDeltaSensorAsync): wait> on 2023-01-16 06:36:43.044492+00:00
[2023-01-15T22:36:46.522-0800] {standard_task_runner.py:56} INFO - Started process 38807 to run task
[2023-01-15T22:36:46.530-0800] {standard_task_runner.py:83} INFO - Running: ['airflow', 'tasks', 'run', 'example_time_delta_sensor_async', 'wait', 'manual__2023-01-16T06:36:43.044492+00:00', '--job-id', '487', '--raw', '--subdir', '/Users/dstandish/code/airflow/airflow/example_dags/example_time_delta_sensor_async.py', '--cfg-path', '/var/folders/7_/1xx0hqcs3txd7kqt0ngfdjth0000gn/T/tmpiwyl54bn', '--no-shut-down-logging']
[2023-01-15T22:36:46.536-0800] {standard_task_runner.py:84} INFO - Job 487: Subtask wait
[2023-01-15T22:36:46.624-0800] {task_command.py:417} INFO - Running <TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [running]> on host daniels-mbp-2.lan
[2023-01-15T22:36:46.918-0800] {taskinstance.py:1558} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='airflow' AIRFLOW_CTX_DAG_ID='example_time_delta_sensor_async' AIRFLOW_CTX_TASK_ID='wait' AIRFLOW_CTX_EXECUTION_DATE='2023-01-16T06:36:43.044492+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2023-01-16T06:36:43.044492+00:00'
[2023-01-15T22:36:46.929-0800] {taskinstance.py:1433} INFO - Pausing task as DEFERRED. dag_id=example_time_delta_sensor_async, task_id=wait, execution_date=20230116T063643, start_date=20230116T063646
[2023-01-15T22:36:46.981-0800] {local_task_job.py:218} INFO - Task exited with return code 100 (task deferral)

[2023-01-15T22:36:46.474-0800] {taskinstance.py:1131} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [queued]>
[2023-01-15T22:36:46.482-0800] {taskinstance.py:1131} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [queued]>
[2023-01-15T22:36:46.483-0800] {taskinstance.py:1332} INFO - Starting attempt 1 of 1
[2023-01-15T22:36:46.516-0800] {taskinstance.py:1351} INFO - Executing <Task(TimeDeltaSensorAsync): wait> on 2023-01-16 06:36:43.044492+00:00
[2023-01-15T22:36:46.522-0800] {standard_task_runner.py:56} INFO - Started process 38807 to run task
[2023-01-15T22:36:46.530-0800] {standard_task_runner.py:83} INFO - Running: ['airflow', 'tasks', 'run', 'example_time_delta_sensor_async', 'wait', 'manual__2023-01-16T06:36:43.044492+00:00', '--job-id', '487', '--raw', '--subdir', '/Users/dstandish/code/airflow/airflow/example_dags/example_time_delta_sensor_async.py', '--cfg-path', '/var/folders/7_/1xx0hqcs3txd7kqt0ngfdjth0000gn/T/tmpiwyl54bn', '--no-shut-down-logging']
[2023-01-15T22:36:46.536-0800] {standard_task_runner.py:84} INFO - Job 487: Subtask wait
[2023-01-15T22:36:46.624-0800] {task_command.py:417} INFO - Running <TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [running]> on host daniels-mbp-2.lan
[2023-01-15T22:36:46.918-0800] {taskinstance.py:1558} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER='airflow' AIRFLOW_CTX_DAG_ID='example_time_delta_sensor_async' AIRFLOW_CTX_TASK_ID='wait' AIRFLOW_CTX_EXECUTION_DATE='2023-01-16T06:36:43.044492+00:00' AIRFLOW_CTX_TRY_NUMBER='1' AIRFLOW_CTX_DAG_RUN_ID='manual__2023-01-16T06:36:43.044492+00:00'
[2023-01-15T22:36:46.929-0800] {taskinstance.py:1433} INFO - Pausing task as DEFERRED. dag_id=example_time_delta_sensor_async, task_id=wait, execution_date=20230116T063643, start_date=20230116T063646
[2023-01-15T22:36:46.981-0800] {local_task_job.py:218} INFO - Task exited with return code 100 (task deferral)
[2023-01-15T22:37:17.673-0800] {taskinstance.py:1131} INFO - Dependencies all met for dep_context=non-requeueable deps ti=<TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [queued]>
[2023-01-15T22:37:17.681-0800] {taskinstance.py:1131} INFO - Dependencies all met for dep_context=requeueable deps ti=<TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [queued]>
[2023-01-15T22:37:17.682-0800] {taskinstance.py:1330} INFO - resuming after deferral
[2023-01-15T22:37:17.693-0800] {taskinstance.py:1351} INFO - Executing <Task(TimeDeltaSensorAsync): wait> on 2023-01-16 06:36:43.044492+00:00
[2023-01-15T22:37:17.697-0800] {standard_task_runner.py:56} INFO - Started process 39090 to run task
[2023-01-15T22:37:17.703-0800] {standard_task_runner.py:83} INFO - Running: ['airflow', 'tasks', 'run', 'example_time_delta_sensor_async', 'wait', 'manual__2023-01-16T06:36:43.044492+00:00', '--job-id', '488', '--raw', '--subdir', '/Users/dstandish/code/airflow/airflow/example_dags/example_time_delta_sensor_async.py', '--cfg-path', '/var/folders/7_/1xx0hqcs3txd7kqt0ngfdjth0000gn/T/tmp_sa9sau4', '--no-shut-down-logging']
[2023-01-15T22:37:17.707-0800] {standard_task_runner.py:84} INFO - Job 488: Subtask wait
[2023-01-15T22:37:17.771-0800] {task_command.py:417} INFO - Running <TaskInstance: example_time_delta_sensor_async.wait manual__2023-01-16T06:36:43.044492+00:00 [running]> on host daniels-mbp-2.lan
[2023-01-15T22:37:18.043-0800] {taskinstance.py:1369} INFO - Marking task as SUCCESS. dag_id=example_time_delta_sensor_async, task_id=wait, execution_date=20230116T063643, start_date=20230116T063646, end_date=20230116T063718
[2023-01-15T22:37:18.117-0800] {local_task_job.py:220} INFO - Task exited with return code 0
[2023-01-15T22:37:18.147-0800] {taskinstance.py:2648} INFO - 0 downstream tasks scheduled from follow-on schedule check
[2023-01-15T22:37:18.173-0800] {:0} Level None - end_of_log

*** hihihi!
[2023-01-15T22:36:48.348-0800] {temporal.py:62} INFO - trigger starting
[2023-01-15T22:36:48.348-0800] {temporal.py:66} INFO - 24 seconds remaining; sleeping 10 seconds
[2023-01-15T22:36:58.349-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:36:59.349-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:00.349-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:01.350-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:02.350-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:03.351-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:04.351-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:05.353-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:06.354-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:07.355-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:08.356-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:09.357-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:10.358-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:11.359-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:12.359-0800] {temporal.py:71} INFO - sleeping 1 second...
[2023-01-15T22:37:13.360-0800] {temporal.py:74} INFO - yielding event with payload DateTime(2023, 1, 16, 6, 37, 13, 44492, tzinfo=Timezone('UTC'))
[2023-01-15T22:37:13.361-0800] {triggerer_job.py:540} INFO - Trigger <airflow.triggers.temporal.DateTimeTrigger moment=2023-01-16T06:37:13.044492+00:00> (ID 106) fired: TriggerEvent<DateTime(2023, 1, 16, 6, 37, 13, 44492, tzinfo=Timezone('UTC'))>
"""


def test_interleave_logs_correct_ordering():
    """
    Notice there are two messages with timestamp `2023-01-17T12:47:11.883-0800`.
    In this case, these should appear in correct order and be deduped in result.
    """
    sample_with_dupe = """[2023-01-17T12:46:55.868-0800] {temporal.py:62} INFO - trigger starting
    [2023-01-17T12:46:55.868-0800] {temporal.py:71} INFO - sleeping 1 second...
    [2023-01-17T12:47:09.882-0800] {temporal.py:71} INFO - sleeping 1 second...
    [2023-01-17T12:47:10.882-0800] {temporal.py:71} INFO - sleeping 1 second...
    [2023-01-17T12:47:11.883-0800] {temporal.py:74} INFO - yielding event with payload DateTime(2023, 1, 17, 20, 47, 11, 254388, tzinfo=Timezone('UTC'))
    [2023-01-17T12:47:11.883-0800] {triggerer_job.py:540} INFO - Trigger <airflow.triggers.temporal.DateTimeTrigger moment=2023-01-17T20:47:11.254388+00:00> (ID 1) fired: TriggerEvent<DateTime(2023, 1, 17, 20, 47, 11, 254388, tzinfo=Timezone('UTC'))>
    """

    assert sample_with_dupe == "\n".join(_interleave_logs(sample_with_dupe, "", sample_with_dupe))


def test_interleave_logs_correct_dedupe():
    sample_without_dupe = """test,
    test,
    test,
    test,
    test,
    test,
    test,
    test,
    test,
    test"""

    assert sample_without_dupe == "\n".join(_interleave_logs(",\n    ".join(["test"] * 10)))


def test_permissions_for_new_directories(tmp_path):
    # Set umask to 0o027: owner rwx, group rx-w, other -rwx
    old_umask = os.umask(0o027)
    try:
        base_dir = tmp_path / "base"
        base_dir.mkdir()
        log_dir = base_dir / "subdir1" / "subdir2"
        # force permissions for the new folder to be owner rwx, group -rxw, other -rwx
        new_folder_permissions = 0o700
        # default permissions are owner rwx, group rx-w, other -rwx (umask bit negative)
        default_permissions = 0o750
        FileTaskHandler._prepare_log_folder(log_dir, new_folder_permissions)
        assert log_dir.exists()
        assert log_dir.is_dir()
        assert log_dir.stat().st_mode % 0o1000 == new_folder_permissions
        assert log_dir.parent.stat().st_mode % 0o1000 == new_folder_permissions
        assert base_dir.stat().st_mode % 0o1000 == default_permissions
    finally:
        os.umask(old_umask)


worker_url = "http://10.240.5.168:8793"
log_location = "dag_id=sample/run_id=manual__2024-05-23T07:18:59.298882+00:00/task_id=sourcing/attempt=1.log"
log_url = f"{worker_url}/log/{log_location}"


@mock.patch("requests.adapters.HTTPAdapter.send")
def test_fetch_logs_from_service_with_not_matched_no_proxy(mock_send, monkeypatch):
    monkeypatch.setenv("http_proxy", "http://proxy.example.com")
    monkeypatch.setenv("no_proxy", "localhost")

    response = Response()
    response.status_code = HTTPStatus.OK
    mock_send.return_value = response

    _fetch_logs_from_service(log_url, log_location)

    mock_send.assert_called()
    _, kwargs = mock_send.call_args
    assert "proxies" in kwargs
    proxies = kwargs["proxies"]
    assert "http" in proxies.keys()
    assert "no" in proxies.keys()


@mock.patch("requests.adapters.HTTPAdapter.send")
def test_fetch_logs_from_service_with_cidr_no_proxy(mock_send, monkeypatch):
    monkeypatch.setenv("http_proxy", "http://proxy.example.com")
    monkeypatch.setenv("no_proxy", "10.0.0.0/8")

    response = Response()
    response.status_code = HTTPStatus.OK
    mock_send.return_value = response

    _fetch_logs_from_service(log_url, log_location)

    mock_send.assert_called()
    _, kwargs = mock_send.call_args
    assert "proxies" in kwargs
    proxies = kwargs["proxies"]
    assert "http" not in proxies.keys()
    assert "no" not in proxies.keys()
