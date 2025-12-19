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

import itertools
import logging
import logging.config
import os
import re
from collections.abc import Iterable
from http import HTTPStatus
from importlib import reload
from pathlib import Path
from unittest import mock
from unittest.mock import patch

import pendulum
import pendulum.tz
import pytest
from pydantic import TypeAdapter
from pydantic.v1.utils import deep_update
from requests.adapters import Response

from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.executors import executor_constants, executor_loader
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.trigger import Trigger
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.log.file_task_handler import (
    FileTaskHandler,
    LogType,
    StructuredLogMessage,
    _fetch_logs_from_service,
    _interleave_logs,
    _parse_log_lines,
)
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

DEFAULT_DATE = datetime(2016, 1, 1)
TASK_LOGGER = "airflow.task"
FILE_TASK_HANDLER = "task"


def events(logs: Iterable[StructuredLogMessage], skip_source_info=True) -> list[str]:
    """Helper function to return just the event (a.k.a message) from a list of StructuredLogMessage"""
    logs = iter(logs)
    if skip_source_info:

        def is_source_group(log: StructuredLogMessage):
            return not hasattr(log, "timestamp") or log.event == "::endgroup"

        logs = itertools.dropwhile(is_source_group, logs)

    return [s.event for s in logs]


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

    def test_file_task_handler_when_ti_value_is_invalid(self, dag_maker):
        def task_callable(ti):
            ti.log.info("test")

        with dag_maker("dag_for_testing_file_task_handler", schedule=None):
            task = PythonOperator(
                task_id="task_for_testing_file_log_handler",
                python_callable=task_callable,
            )

        dagrun = dag_maker.create_dagrun()
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
        log, metadata = file_handler.read(ti, 0)
        assert isinstance(metadata, dict)
        assert log[0].event == "Error fetching the logs. Try number 0 is invalid."

        # Remove the generated tmp log file.
        os.remove(log_filename)

    def test_file_task_handler(self, dag_maker, session):
        def task_callable(ti):
            ti.log.info("test")

        with dag_maker("dag_for_testing_file_task_handler", schedule=None, session=session):
            PythonOperator(
                task_id="task_for_testing_file_log_handler",
                python_callable=task_callable,
            )

        dagrun = dag_maker.create_dagrun()

        (ti,) = dagrun.get_task_instances(session=session)
        ti.try_number += 1
        session.flush()
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
        log, metadata = file_handler.read(ti, 1)
        assert isinstance(metadata, dict)
        target_re = re.compile(r"\A\[[^\]]+\] {test_log_handlers.py:\d+} INFO - test\Z")

        # We should expect our log line from the callable above to appear in
        # the logs we read back

        assert any(re.search(target_re, e) for e in events(log)), "Logs were " + str(log)

        # Remove the generated tmp log file.
        os.remove(log_filename)

    @pytest.mark.parametrize(
        "executor_name",
        [
            (executor_constants.KUBERNETES_EXECUTOR),
            (None),
        ],
    )
    @conf_vars(
        {
            ("core", "EXECUTOR"): executor_constants.KUBERNETES_EXECUTOR,
        }
    )
    @patch(
        "airflow.executors.executor_loader.ExecutorLoader.load_executor",
        wraps=executor_loader.ExecutorLoader.load_executor,
    )
    @patch(
        "airflow.executors.executor_loader.ExecutorLoader.get_default_executor",
        wraps=executor_loader.ExecutorLoader.get_default_executor,
    )
    def test_file_task_handler_with_multiple_executors(
        self,
        mock_get_default_executor,
        mock_load_executor,
        executor_name,
        create_task_instance,
        clean_executor_loader,
    ):
        executors_mapping = executor_loader.ExecutorLoader.executors
        default_executor_name = executor_loader.ExecutorLoader.get_default_executor_name()
        path_to_executor_class: str
        if executor_name is None:
            path_to_executor_class = executors_mapping.get(default_executor_name.alias)
        else:
            path_to_executor_class = executors_mapping.get(executor_name)

        with patch(f"{path_to_executor_class}.get_task_log", return_value=([], [])) as mock_get_task_log:
            mock_get_task_log.return_value = ([], [])
            ti = create_task_instance(
                dag_id="dag_for_testing_multiple_executors",
                task_id="task_for_testing_multiple_executors",
                run_type=DagRunType.SCHEDULED,
                logical_date=DEFAULT_DATE,
            )
            if executor_name is not None:
                ti.executor = executor_name
            ti.try_number = 1
            ti.state = TaskInstanceState.RUNNING
            logger = ti.log
            ti.log.disabled = False

            file_handler = next(
                (handler for handler in logger.handlers if handler.name == FILE_TASK_HANDLER), None
            )
            assert file_handler is not None

            set_context(logger, ti)
            # clear executor_instances cache
            file_handler.executor_instances = {}
            assert file_handler.handler is not None
            # We expect set_context generates a file locally.
            log_filename = file_handler.handler.baseFilename
            assert os.path.isfile(log_filename)
            assert log_filename.endswith("1.log"), log_filename

            file_handler.flush()
            file_handler.close()

            assert hasattr(file_handler, "read")
            file_handler.read(ti)
            os.remove(log_filename)
            mock_get_task_log.assert_called_once()

            if executor_name is None:
                mock_get_default_executor.assert_called_once()
                # will be called in `ExecutorLoader.get_default_executor` method
                mock_load_executor.assert_called_once_with(default_executor_name)
            else:
                mock_get_default_executor.assert_not_called()
                mock_load_executor.assert_called_once_with(executor_name)

    def test_file_task_handler_running(self, dag_maker):
        def task_callable(ti):
            ti.log.info("test")

        with dag_maker("dag_for_testing_file_task_handler", schedule=None):
            task = PythonOperator(
                task_id="task_for_testing_file_log_handler",
                python_callable=task_callable,
            )
        dagrun = dag_maker.create_dagrun()
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
        logs, metadata = file_handler.read(ti)
        assert isinstance(logs, list)
        # Logs for running tasks should show up too.
        assert isinstance(metadata, dict)

        # Remove the generated tmp log file.
        os.remove(log_filename)

    def test_file_task_handler_rotate_size_limit(self, dag_maker):
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
        with dag_maker("dag_for_testing_file_task_handler_rotate_size_limit"):
            task = PythonOperator(
                task_id="task_for_testing_file_log_handler_rotate_size_limit",
                python_callable=task_callable,
            )
        dagrun = dag_maker.create_dagrun()
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
        logs, metadata = file_handler.read(ti)

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

        # Logs for running tasks should show up too.
        assert isinstance(logs, list)

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
            logical_date=DEFAULT_DATE,
        )
        fth = FileTaskHandler("")
        logs, metadata = fth._read(ti=local_log_file_read, try_number=1)
        mock_read_local.assert_called_with(path)
        as_text = events(logs)
        assert logs[0].sources == ["the messages"]
        assert as_text[-1] == "the log"
        assert metadata == {"end_of_log": True, "log_pos": 1}

    def test__read_from_local(self, tmp_path):
        """Tests the behavior of method _read_from_local"""

        path1 = tmp_path / "hello1.log"
        path2 = tmp_path / "hello1.log.suffix.log"
        path1.write_text("file1 content")
        path2.write_text("file2 content")
        fth = FileTaskHandler("")
        assert fth._read_from_local(path1) == (
            [str(path1), str(path2)],
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
            logical_date=DEFAULT_DATE,
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
            logs, metadata = fth._read(ti=ti, try_number=1)
        if served_logs_checked:
            fth._read_from_logs_server.assert_called_once()
            assert events(logs) == [
                "::group::Log message source details",
                "::endgroup::",
                "this",
                "log",
                "content",
            ]
            assert metadata == {"end_of_log": True, "log_pos": 3}
        else:
            fth._read_from_logs_server.assert_not_called()
            assert logs
            assert metadata

    def test_add_triggerer_suffix(self):
        sample = "any/path/to/thing.txt"
        assert FileTaskHandler.add_triggerer_suffix(sample) == sample + ".trigger"
        assert FileTaskHandler.add_triggerer_suffix(sample, job_id=None) == sample + ".trigger"
        assert FileTaskHandler.add_triggerer_suffix(sample, job_id=123) == sample + ".trigger.123.log"
        assert FileTaskHandler.add_triggerer_suffix(sample, job_id="123") == sample + ".trigger.123.log"

    @pytest.mark.parametrize("is_a_trigger", [True, False])
    def test_set_context_trigger(self, create_dummy_dag, dag_maker, is_a_trigger, session, tmp_path):
        create_dummy_dag(dag_id="test_fth", task_id="dummy")
        (ti,) = dag_maker.create_dagrun(logical_date=pendulum.datetime(2023, 1, 1, tz="UTC")).task_instances
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


@pytest.mark.parametrize("logical_date", ((None), (DEFAULT_DATE)))
class TestFilenameRendering:
    def test_python_formatting(self, create_log_template, create_task_instance, logical_date):
        create_log_template("{dag_id}/{task_id}/{logical_date}/{try_number}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            run_after=DEFAULT_DATE,
            logical_date=logical_date,
        )

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{DEFAULT_DATE.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert expected_filename == rendered_filename

    def test_jinja_rendering(self, create_log_template, create_task_instance, logical_date):
        create_log_template("{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            run_after=DEFAULT_DATE,
            logical_date=logical_date,
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
            logical_date=DEFAULT_DATE,
        )
        log_url_ti.hostname = "hostname"
        actual = FileTaskHandler("")._get_log_retrieval_url(log_url_ti, "DYNAMIC_PATH")
        assert actual == ("http://hostname:8793/log/DYNAMIC_PATH", "DYNAMIC_PATH")

    def test_log_retrieval_valid_trigger(self, create_task_instance):
        ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
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
AIRFLOW_CTX_LOGICAL_DATE=2022-11-16T08:05:52.324532+00:00
AIRFLOW_CTX_TRY_NUMBER=1
AIRFLOW_CTX_DAG_RUN_ID=manual__2022-11-16T08:05:52.324532+00:00
[2022-11-16T00:05:54.604-0800] {taskinstance.py:1360} INFO - Pausing task as DEFERRED. dag_id=simple_async_timedelta, task_id=wait, execution_date=20221116T080552, start_date=20221116T080554
"""


def test_parse_timestamps():
    actual = []
    for timestamp, _, _ in _parse_log_lines(log_sample.splitlines()):
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
            "AIRFLOW_CTX_LOGICAL_DATE=2022-11-16T08:05:52.324532+00:00",
            "AIRFLOW_CTX_TRY_NUMBER=1",
            "AIRFLOW_CTX_DAG_RUN_ID=manual__2022-11-16T08:05:52.324532+00:00",
            "[2022-11-16T00:05:54.604-0800] {taskinstance.py:1360} INFO - Pausing task as DEFERRED. dag_id=simple_async_timedelta, task_id=wait, execution_date=20221116T080552, start_date=20221116T080554",
        ]
    )

    # -08:00
    tz = pendulum.tz.fixed_timezone(-28800)
    DateTime = pendulum.DateTime
    expected = [
        {
            "event": "[2022-11-16T00:05:54.278-0800] {taskinstance.py:1258} INFO - Starting attempt 1 of 1",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 278000, tzinfo=tz),
        },
        {
            "event": "[2022-11-16T00:05:54.295-0800] {taskinstance.py:1278} INFO - "
            "Executing <Task(TimeDeltaSensorAsync): wait> on 2022-11-16 "
            "08:05:52.324532+00:00",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 295000, tzinfo=tz),
        },
        {
            "event": "[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - "
            "Started process 52536 to run task",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 300000, tzinfo=tz),
        },
        {
            "event": "[2022-11-16T00:05:54.306-0800] {standard_task_runner.py:82} INFO - "
            "Running: ['airflow', 'tasks', 'run', 'simple_async_timedelta', "
            "'wait', 'manual__2022-11-16T08:05:52.324532+00:00', '--job-id', "
            "'33648', '--raw', '--subdir', "
            "'/Users/dstandish/code/airflow/airflow/example_dags/example_time_delta_sensor_async.py', "
            "'--cfg-path', "
            "'/var/folders/7_/1xx0hqcs3txd7kqt0ngfdjth0000gn/T/tmp725r305n']",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 306000, tzinfo=tz),
        },
        {
            "event": "[2022-11-16T00:05:54.309-0800] {standard_task_runner.py:83} INFO - "
            "Job 33648: Subtask wait",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 309000, tzinfo=tz),
        },
        {
            "event": "[2022-11-16T00:05:54.457-0800] {task_command.py:376} INFO - "
            "Running <TaskInstance: simple_async_timedelta.wait "
            "manual__2022-11-16T08:05:52.324532+00:00 [running]> on host "
            "daniels-mbp-2.lan",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 457000, tzinfo=tz),
        },
        {
            "event": "[2022-11-16T00:05:54.592-0800] {taskinstance.py:1485} INFO - "
            "Exporting env vars: AIRFLOW_CTX_DAG_OWNER=airflow",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 592000, tzinfo=tz),
        },
        {
            "event": "AIRFLOW_CTX_DAG_ID=simple_async_timedelta",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 592000, tzinfo=tz),
        },
        {
            "event": "AIRFLOW_CTX_TASK_ID=wait",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 592000, tzinfo=tz),
        },
        {
            "event": "AIRFLOW_CTX_LOGICAL_DATE=2022-11-16T08:05:52.324532+00:00",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 592000, tzinfo=tz),
        },
        {
            "event": "AIRFLOW_CTX_TRY_NUMBER=1",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 592000, tzinfo=tz),
        },
        {
            "event": "AIRFLOW_CTX_DAG_RUN_ID=manual__2022-11-16T08:05:52.324532+00:00",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 592000, tzinfo=tz),
        },
        {
            "event": "[2022-11-16T00:05:54.604-0800] {taskinstance.py:1360} INFO - "
            "Pausing task as DEFERRED. dag_id=simple_async_timedelta, "
            "task_id=wait, execution_date=20221116T080552, "
            "start_date=20221116T080554",
            "timestamp": DateTime(2022, 11, 16, 0, 5, 54, 604000, tzinfo=tz),
        },
    ]
    # Use a type adapter to durn it in to dicts -- makes it easier to compare/test than a bunch of objects
    results = TypeAdapter(list[StructuredLogMessage]).dump_python(
        _interleave_logs(log_sample2, log_sample1, log_sample3)
    )
    # TypeAdapter gives us a generator out when it's generator is an input. Nice, but not useful for testing
    results = list(results)
    assert results == expected


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

    logs = events(_interleave_logs(sample_with_dupe, "", sample_with_dupe))
    assert sample_with_dupe == "\n".join(logs)


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

    logs = events(_interleave_logs(",\n    ".join(["test"] * 10)))
    assert sample_without_dupe == "\n".join(logs)


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


class TestFileTaskHandlerSSL:
    """Tests for SSL configuration in FileTaskHandler."""

    @pytest.mark.parametrize(
        "ssl_verify_conf,expected_verify",
        [
            ("True", True),  # Default: verify enabled
            ("true", True),  # Case insensitive
            ("1", True),  # Alternative true value
            ("yes", True),  # Alternative true value
            ("False", False),  # Verification disabled
            ("false", False),  # Case insensitive
            ("0", False),  # Alternative false value
            ("no", False),  # Alternative false value
            ("/path/to/ca-bundle.crt", "/path/to/ca-bundle.crt"),  # Custom CA bundle
        ],
    )
    @mock.patch("requests.get")
    def test_fetch_logs_ssl_verify_configuration(self, mock_get, ssl_verify_conf, expected_verify):
        """Test SSL verification configuration in _fetch_logs_from_service."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.encoding = "utf-8"
        mock_get.return_value = mock_response

        with conf_vars(
            {
                ("logging", "worker_log_server_ssl_verify"): ssl_verify_conf,
                ("webserver", "secret_key"): "test_secret_key",
            }
        ):
            _fetch_logs_from_service(log_url, log_location)

        # Verify that requests.get was called with correct verify parameter
        mock_get.assert_called_once()
        _, kwargs = mock_get.call_args
        assert "verify" in kwargs
        assert kwargs["verify"] == expected_verify

    @pytest.mark.parametrize(
        "ssl_cert,ssl_key,expected_protocol",
        [
            ("", "", "http"),  # No SSL configured
            ("/path/to/cert.pem", "", "http"),  # Only cert, no key
            ("", "/path/to/key.pem", "http"),  # Only key, no cert
            ("/path/to/cert.pem", "/path/to/key.pem", "https"),  # Both provided
        ],
    )
    def test_log_retrieval_url_protocol(self, create_task_instance, ssl_cert, ssl_key, expected_protocol):
        """Test that correct protocol (http/https) is used based on SSL configuration."""
        ti = create_task_instance(
            dag_id="dag_for_testing_ssl_url",
            task_id="task_for_testing_ssl_url",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
        )
        ti.hostname = "test-hostname"

        with conf_vars(
            {
                ("logging", "worker_log_server_ssl_cert"): ssl_cert,
                ("logging", "worker_log_server_ssl_key"): ssl_key,
            }
        ):
            fth = FileTaskHandler("")
            url, _ = fth._get_log_retrieval_url(ti, "test/path.log")

        assert url.startswith(f"{expected_protocol}://")
        assert "test-hostname" in url
        assert ":8793/log/test/path.log" in url

    def test_log_retrieval_url_trigger_with_ssl(self, create_task_instance):
        """Test that trigger log URLs use correct protocol with SSL."""
        from airflow.jobs.job import Job
        from airflow.jobs.triggerer_job_runner import TriggererJobRunner
        from airflow.models.trigger import Trigger

        ti = create_task_instance(
            dag_id="dag_for_testing_trigger_ssl",
            task_id="task_for_testing_trigger_ssl",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
        )
        ti.hostname = "test-hostname"

        trigger = Trigger("", {})
        job = Job(TriggererJobRunner.job_type)
        job.id = 123
        trigger.triggerer_job = job
        ti.trigger = trigger

        with conf_vars(
            {
                ("logging", "worker_log_server_ssl_cert"): "/path/to/cert.pem",
                ("logging", "worker_log_server_ssl_key"): "/path/to/key.pem",
            }
        ):
            fth = FileTaskHandler("")
            url, path = fth._get_log_retrieval_url(ti, "test/path.log", log_type=LogType.TRIGGER)

        assert url.startswith("https://")
        assert ":8794/log/" in url
        assert path.endswith(".trigger.123.log")

    @mock.patch("requests.get")
    def test_fetch_logs_ssl_verify_default(self, mock_get):
        """Test that SSL verification is enabled by default."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.encoding = "utf-8"
        mock_get.return_value = mock_response

        with conf_vars({("webserver", "secret_key"): "test_secret_key"}):
            # Don't set worker_log_server_ssl_verify, use default
            _fetch_logs_from_service(log_url, log_location)

        mock_get.assert_called_once()
        _, kwargs = mock_get.call_args
        assert kwargs["verify"] is True
