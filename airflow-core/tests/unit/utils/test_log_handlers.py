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

import heapq
import io
import itertools
import logging
import os
import re
from http import HTTPStatus
from importlib import reload
from pathlib import Path
from typing import cast
from unittest import mock
from unittest.mock import patch

import pendulum
import pendulum.tz
import pytest
from pydantic import TypeAdapter
from pydantic.v1.utils import deep_update
from requests.adapters import Response

from airflow import settings
from airflow.config_templates.airflow_local_settings import DEFAULT_LOGGING_CONFIG
from airflow.executors import executor_constants, executor_loader
from airflow.jobs.job import Job
from airflow.jobs.triggerer_job_runner import TriggererJobRunner
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.taskinstancehistory import TaskInstanceHistory
from airflow.models.trigger import Trigger
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.log.file_task_handler import (
    DEFAULT_SORT_DATETIME,
    FileTaskHandler,
    LogType,
    ParsedLogStream,
    StructuredLogMessage,
    _add_log_from_parsed_log_streams_to_heap,
    _create_sort_key,
    _fetch_logs_from_service,
    _flush_logs_out_of_heap,
    _interleave_logs,
    _is_logs_stream_like,
    _is_sort_key_with_default_timestamp,
    _log_stream_to_parsed_log_stream,
    _stream_lines_by_chunk,
)
from airflow.utils.log.logging_mixin import set_context
from airflow.utils.net import get_hostname
from airflow.utils.session import create_session
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections, clear_db_runs
from tests_common.test_utils.file_task_handler import (
    convert_list_to_stream,
    extract_events,
    mock_parsed_logs_factory,
)
from tests_common.test_utils.markers import skip_if_force_lowest_dependencies_marker

pytestmark = [pytest.mark.db_test]

DEFAULT_DATE = pendulum.datetime(2016, 1, 1)
TASK_LOGGER = "airflow.task"
FILE_TASK_HANDLER = "task"


@pytest.fixture(autouse=True)
def cleanup_tables():
    clear_db_runs()
    clear_db_connections()
    yield
    clear_db_runs()
    clear_db_connections()


class TestFileTaskLogHandler:
    def clean_up(self):
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TaskInstance).delete()

    def setup_method(self):
        settings.configure_logging()
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

    @pytest.mark.xfail(reason="TODO: Needs to be ported over to the new structlog based logging")
    def test_file_task_handler_when_ti_value_is_invalid(self, dag_maker):
        def task_callable(ti):
            ti.log.info("test")

        with dag_maker("dag_for_testing_file_task_handler", schedule=None):
            task = PythonOperator(
                task_id="task_for_testing_file_log_handler",
                python_callable=task_callable,
            )

        dagrun = dag_maker.create_dagrun()
        dag_version = DagVersion.get_latest_version(dagrun.dag_id)
        ti = TaskInstance(task=task, run_id=dagrun.run_id, dag_version_id=dag_version.id)

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
        log_handler_output_stream, metadata = file_handler.read(ti, 0)
        assert isinstance(metadata, dict)
        assert extract_events(log_handler_output_stream) == [
            "Error fetching the logs. Try number 0 is invalid."
        ]

        # Remove the generated tmp log file.
        os.remove(log_filename)

    @pytest.mark.parametrize("ti_state", [TaskInstanceState.SKIPPED, TaskInstanceState.UPSTREAM_FAILED])
    def test_file_task_handler_when_ti_is_not_run(self, dag_maker, ti_state):
        def task_callable(ti):
            ti.log.info("test")

        with dag_maker("dag_for_testing_file_task_handler", schedule=None):
            task = PythonOperator(
                task_id="task_for_testing_file_log_handler",
                python_callable=task_callable,
            )
        dagrun = dag_maker.create_dagrun()
        dag_version = DagVersion.get_latest_version(dagrun.dag_id)
        ti = TaskInstance(task=task, run_id=dagrun.run_id, dag_version_id=dag_version.id)

        ti.try_number = 0
        ti.state = ti_state

        logger = logging.getLogger(TASK_LOGGER)
        logger.disabled = False

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

        # Return value of read must be a tuple of list and list.
        log_handler_output_stream, metadata = file_handler.read(ti)
        logs = list(log_handler_output_stream)
        assert logs[0].event == "Task was skipped, no logs available."

        # Remove the generated tmp log file.
        os.remove(log_filename)

    @pytest.mark.xfail(reason="TODO: Needs to be ported over to the new structlog based logging")
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
        log_handler_output_stream, metadata = file_handler.read(ti, 1)
        assert isinstance(metadata, dict)
        target_re = re.compile(r"\A\[[^\]]+\] {test_log_handlers.py:\d+} INFO - test\Z")

        # We should expect our log line from the callable above to appear in
        # the logs we read back

        assert any(re.search(target_re, e) for e in extract_events(log_handler_output_stream)), (
            f"Logs were {log_handler_output_stream}"
        )

        # Remove the generated tmp log file.
        os.remove(log_filename)

    @skip_if_force_lowest_dependencies_marker
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
            logger = logging.getLogger(TASK_LOGGER)
            logger.disabled = False

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
        dag_version = DagVersion.get_latest_version(dagrun.dag_id)
        ti = TaskInstance(task=task, run_id=dagrun.run_id, dag_version_id=dag_version.id)

        ti.try_number = 2
        ti.state = State.RUNNING

        logger = logging.getLogger(TASK_LOGGER)
        logger.disabled = False

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
        log_handler_output_stream, metadata = file_handler.read(ti)
        assert _is_logs_stream_like(log_handler_output_stream)
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
        dag_version = DagVersion.get_latest_version(dagrun.dag_id)
        ti = TaskInstance(task=task, run_id=dagrun.run_id, dag_version_id=dag_version.id)

        ti.try_number = 1
        ti.state = State.RUNNING

        logger = logging.getLogger(TASK_LOGGER)
        logger.disabled = False

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
        for i in range(1, 3000):
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
        assert _is_logs_stream_like(logs)

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
        mock_read_local.return_value = (["the messages"], [convert_list_to_stream(["the log"])])
        local_log_file_read = create_task_instance(
            dag_id="dag_for_testing_local_log_read",
            task_id="task_for_testing_local_log_read",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
        )
        fth = FileTaskHandler("")
        log_handler_output_stream, metadata = fth._read(ti=local_log_file_read, try_number=1)
        mock_read_local.assert_called_with(path)
        assert extract_events(log_handler_output_stream) == ["the log"]
        assert metadata == {"end_of_log": True, "log_pos": 1}

    def test__read_from_local(self, tmp_path):
        """Tests the behavior of method _read_from_local"""
        path1 = tmp_path / "hello1.log"
        path2 = tmp_path / "hello1.log.suffix.log"
        path1.write_text("file1 content\nfile1 content2")
        path2.write_text("file2 content\nfile2 content2")
        fth = FileTaskHandler("")
        log_source_info, log_streams = fth._read_from_local(path1)
        assert log_source_info == [str(path1), str(path2)]
        assert len(log_streams) == 2
        assert list(log_streams[0]) == ["file1 content", "file1 content2"]
        assert list(log_streams[1]) == ["file2 content", "file2 content2"]

    @pytest.mark.parametrize(
        ("remote_logs", "local_logs", "served_logs_checked"),
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
        expected_logs = ["::group::Log message source details", "::endgroup::"]
        with conf_vars({("core", "executor"): executor_name}):
            reload(executor_loader)
            fth = FileTaskHandler("")
            if remote_logs:
                fth._read_remote_logs = mock.Mock()
                fth._read_remote_logs.return_value = ["found remote logs"], ["remote\nlog\ncontent"]
                expected_logs.extend(
                    [
                        "remote",
                        "log",
                        "content",
                    ]
                )
            if local_logs:
                fth._read_from_local = mock.Mock()
                fth._read_from_local.return_value = (
                    ["found local logs"],
                    [convert_list_to_stream("local\nlog\ncontent".splitlines())],
                )
                # only when not read from remote and TI is unfinished will read from local
                if not remote_logs:
                    expected_logs.extend(
                        [
                            "local",
                            "log",
                            "content",
                        ]
                    )
            fth._read_from_logs_server = mock.Mock()
            fth._read_from_logs_server.return_value = (
                ["this message"],
                [convert_list_to_stream("this\nlog\ncontent".splitlines())],
            )
            # only when not read from remote and not read from local will read from logs server
            if served_logs_checked:
                expected_logs.extend(
                    [
                        "this",
                        "log",
                        "content",
                    ]
                )

            logs, metadata = fth._read(ti=ti, try_number=1)
        if served_logs_checked:
            fth._read_from_logs_server.assert_called_once()
        else:
            fth._read_from_logs_server.assert_not_called()
        assert extract_events(logs, False) == expected_logs
        assert metadata == {"end_of_log": True, "log_pos": 3}

    @pytest.mark.parametrize("is_tih", [False, True])
    def test_read_served_logs(self, is_tih, create_task_instance):
        ti = create_task_instance(
            state=TaskInstanceState.SUCCESS,
            hostname="test_hostname",
        )
        if is_tih:
            ti = TaskInstanceHistory(ti, ti.state)
        fth = FileTaskHandler("")
        sources, _ = fth._read_from_logs_server(ti, "test.log")
        assert len(sources) > 0

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

    @skip_if_force_lowest_dependencies_marker
    def test_read_remote_logs_with_real_s3_remote_log_io(self, create_task_instance, session):
        """Test _read_remote_logs method using real S3RemoteLogIO with mock AWS"""
        import tempfile

        import boto3
        from moto import mock_aws

        from airflow.models.connection import Connection
        from airflow.providers.amazon.aws.log.s3_task_handler import S3RemoteLogIO

        def setup_mock_aws():
            """Set up mock AWS S3 bucket and connection."""
            s3_client = boto3.client("s3", region_name="us-east-1")
            s3_client.create_bucket(Bucket="test-airflow-logs")
            return s3_client

        with mock_aws():
            aws_conn = Connection(
                conn_id="aws_s3_conn",
                conn_type="aws",
                login="test_access_key",
                password="test_secret_key",
                extra='{"region_name": "us-east-1"}',
            )
            session.add(aws_conn)
            session.commit()
            s3_client = setup_mock_aws()

            ti = create_task_instance(
                dag_id="test_dag_s3_remote_logs",
                task_id="test_task_s3_remote_logs",
                run_type=DagRunType.SCHEDULED,
                logical_date=DEFAULT_DATE,
            )
            ti.try_number = 1

            with tempfile.TemporaryDirectory() as temp_dir:
                s3_remote_log_io = S3RemoteLogIO(
                    remote_base="s3://test-airflow-logs/logs",
                    base_log_folder=temp_dir,
                    delete_local_copy=False,
                )

                with conf_vars({("logging", "REMOTE_LOG_CONN_ID"): "aws_s3_conn"}):
                    fth = FileTaskHandler("")
                    log_relative_path = fth._render_filename(ti, 1)

                    log_content = "Log line 1 from S3\nLog line 2 from S3\nLog line 3 from S3"
                    s3_client.put_object(
                        Bucket="test-airflow-logs",
                        Key=f"logs/{log_relative_path}",
                        Body=log_content.encode("utf-8"),
                    )

                    import airflow.logging_config

                    airflow.logging_config.REMOTE_TASK_LOG = s3_remote_log_io

                    sources, logs = fth._read_remote_logs(ti, try_number=1)

                    assert len(sources) > 0, f"Expected sources but got: {sources}"
                    assert len(logs) > 0, f"Expected logs but got: {logs}"
                    assert logs[0] == log_content
                    assert f"s3://test-airflow-logs/logs/{log_relative_path}" in sources[0]


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
            catchup=True,
        )

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{DEFAULT_DATE.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert expected_filename == rendered_filename

    def test_python_formatting_catchup_false(self, create_log_template, create_task_instance, logical_date):
        """Test the filename rendering with catchup=False (the new default behavior)"""
        create_log_template("{dag_id}/{task_id}/{logical_date}/{try_number}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            run_after=DEFAULT_DATE,
            logical_date=logical_date,
        )

        # With catchup=False:
        # - If logical_date is None, it will use current date as the logical date
        # - If logical_date is explicitly provided, it will use that date regardless of catchup setting
        expected_date = logical_date if logical_date is not None else filename_rendering_ti.run_after

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{expected_date.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert rendered_filename == expected_filename

    def test_jinja_rendering(self, create_log_template, create_task_instance, logical_date):
        create_log_template("{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            run_after=DEFAULT_DATE,
            logical_date=logical_date,
            catchup=True,
        )

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{DEFAULT_DATE.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert expected_filename == rendered_filename

    def test_jinja_rendering_catchup_false(self, create_log_template, create_task_instance, logical_date):
        """Test the Jinja template rendering with catchup=False (the new default behavior)"""
        create_log_template("{{ ti.dag_id }}/{{ ti.task_id }}/{{ ts }}/{{ try_number }}.log")
        filename_rendering_ti = create_task_instance(
            dag_id="dag_for_testing_filename_rendering",
            task_id="task_for_testing_filename_rendering",
            run_type=DagRunType.SCHEDULED,
            run_after=DEFAULT_DATE,
            logical_date=logical_date,
        )

        # With catchup=False:
        # - If logical_date is None, it will use current date as the logical date
        # - If logical_date is explicitly provided, it will use that date regardless of catchup setting
        expected_date = logical_date if logical_date is not None else filename_rendering_ti.run_after

        expected_filename = (
            f"dag_for_testing_filename_rendering/task_for_testing_filename_rendering/"
            f"{expected_date.isoformat()}/42.log"
        )
        fth = FileTaskHandler("")
        rendered_filename = fth._render_filename(filename_rendering_ti, 42)
        assert expected_filename == rendered_filename

    def test_jinja_id_in_template_for_history(
        self, create_log_template, create_task_instance, logical_date, session
    ):
        """Test that Jinja template using ti.id works for both TaskInstance and TaskInstanceHistory"""
        create_log_template("{{ ti.id }}.log")
        ti = create_task_instance(
            dag_id="dag_history_test",
            task_id="history_task",
            run_type=DagRunType.SCHEDULED,
            logical_date=DEFAULT_DATE,
            catchup=True,
        )
        TaskInstanceHistory.record_ti(ti, session=session)
        session.flush()
        tih = (
            session.query(TaskInstanceHistory)
            .filter_by(
                dag_id=ti.dag_id,
                task_id=ti.task_id,
                run_id=ti.run_id,
                map_index=ti.map_index,
                try_number=ti.try_number,
            )
            .one()
        )
        fth = FileTaskHandler("")
        rendered_ti = fth._render_filename(ti, ti.try_number, session=session)
        rendered_tih = fth._render_filename(tih, ti.try_number, session=session)
        expected = f"{ti.id}.log"
        assert rendered_ti == expected
        assert rendered_tih == expected


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


@pytest.mark.parametrize(
    ("chunk_size", "expected_read_calls"),
    [
        (10, 4),
        (20, 3),
        # will read all logs in one call, but still need another call to get empty string at the end to escape the loop
        (50, 2),
        (100, 2),
    ],
)
def test__stream_lines_by_chunk(chunk_size, expected_read_calls):
    # Mock CHUNK_SIZE to a smaller value to test
    with mock.patch("airflow.utils.log.file_task_handler.CHUNK_SIZE", chunk_size):
        log_io = io.StringIO("line1\nline2\nline3\nline4\n")
        log_io.read = mock.MagicMock(wraps=log_io.read)

        # Stream lines using the function
        streamed_lines = list(_stream_lines_by_chunk(log_io))

        # Verify the output matches the input split by lines
        expected_output = ["line1", "line2", "line3", "line4"]
        assert log_io.read.call_count == expected_read_calls, (
            f"Expected {expected_read_calls} calls to read, got {log_io.read.call_count}"
        )
        assert streamed_lines == expected_output, f"Expected {expected_output}, got {streamed_lines}"


@pytest.mark.parametrize(
    "seekable",
    [
        pytest.param(True, id="seekable_stream"),
        pytest.param(False, id="non_seekable_stream"),
    ],
)
@pytest.mark.parametrize(
    "closed",
    [
        pytest.param(False, id="not_closed_stream"),
        pytest.param(True, id="closed_stream"),
    ],
)
@pytest.mark.parametrize(
    "unexpected_exception",
    [
        pytest.param(None, id="no_exception"),
        pytest.param(ValueError, id="value_error"),
        pytest.param(IOError, id="io_error"),
        pytest.param(Exception, id="generic_exception"),
    ],
)
@mock.patch(
    "airflow.utils.log.file_task_handler.CHUNK_SIZE", 10
)  # Mock CHUNK_SIZE to a smaller value for testing
def test__stream_lines_by_chunk_error_handling(seekable, closed, unexpected_exception):
    """
    Test that _stream_lines_by_chunk handles errors correctly.
    """
    log_io = io.StringIO("line1\nline2\nline3\nline4\n")
    log_io.seekable = mock.MagicMock(return_value=seekable)
    log_io.seek = mock.MagicMock(wraps=log_io.seek)
    # Mock the read method to check the call count and handle exceptions
    if unexpected_exception:
        expected_error = unexpected_exception("An error occurred while reading the log stream.")
        log_io.read = mock.MagicMock(side_effect=expected_error)
    else:
        log_io.read = mock.MagicMock(wraps=log_io.read)

    # Setup closed state if needed - must be done before starting the test
    if closed:
        log_io.close()

    # If an exception is expected, we mock the read method to raise it
    if unexpected_exception and not closed:
        # Only expect logger error if stream is not closed and there's an exception
        with mock.patch("airflow.utils.log.file_task_handler.logger.error") as mock_logger_error:
            result = list(_stream_lines_by_chunk(log_io))
            mock_logger_error.assert_called_once_with("Error reading log stream: %s", expected_error)
    else:
        # For normal case or closed stream with exception, collect the output
        result = list(_stream_lines_by_chunk(log_io))

    # Check if seekable was called properly
    if seekable and not closed:
        log_io.seek.assert_called_once_with(0)
    if not seekable:
        log_io.seek.assert_not_called()

    # Validate the results based on the conditions
    if not closed and not unexpected_exception:  # Non-seekable streams without errors should still get lines
        assert log_io.read.call_count > 1, "Expected read method to be called at least once."
        assert result == ["line1", "line2", "line3", "line4"]
    elif closed:
        assert log_io.read.call_count == 0, "Read method should not be called on a closed stream."
        assert result == [], "Expected no lines to be yield from a closed stream."
    elif unexpected_exception:  # If an exception was raised
        assert log_io.read.call_count == 1, "Read method should be called once."
        assert result == [], "Expected no lines to be yield from a stream that raised an exception."


def test__log_stream_to_parsed_log_stream():
    parsed_log_stream = _log_stream_to_parsed_log_stream(io.StringIO(log_sample))

    actual_timestamps = []
    last_idx = -1
    for parsed_log in parsed_log_stream:
        timestamp, idx, structured_log = parsed_log
        actual_timestamps.append(timestamp)
        if last_idx != -1:
            assert idx > last_idx
        last_idx = idx
        assert isinstance(structured_log, StructuredLogMessage)

    assert actual_timestamps == [
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


def test__create_sort_key():
    # assert _sort_key should return int
    sort_key = _create_sort_key(pendulum.parse("2022-11-16T00:05:54.278000-08:00"), 10)
    assert sort_key == 16685859542780000010


@pytest.mark.parametrize(
    ("timestamp", "line_num", "expected"),
    [
        pytest.param(
            pendulum.parse("2022-11-16T00:05:54.278000-08:00"),
            10,
            False,
            id="normal_timestamp_1",
        ),
        pytest.param(
            pendulum.parse("2022-11-16T00:05:54.457000-08:00"),
            2025,
            False,
            id="normal_timestamp_2",
        ),
        pytest.param(
            DEFAULT_SORT_DATETIME,
            200,
            True,
            id="default_timestamp",
        ),
    ],
)
def test__is_sort_key_with_default_timestamp(timestamp, line_num, expected):
    assert _is_sort_key_with_default_timestamp(_create_sort_key(timestamp, line_num)) == expected


@pytest.mark.parametrize(
    ("log_stream", "expected"),
    [
        pytest.param(
            convert_list_to_stream(
                [
                    "2022-11-16T00:05:54.278000-08:00",
                    "2022-11-16T00:05:54.457000-08:00",
                ]
            ),
            True,
            id="normal_log_stream",
        ),
        pytest.param(
            itertools.chain(
                [
                    "2022-11-16T00:05:54.278000-08:00",
                    "2022-11-16T00:05:54.457000-08:00",
                ],
                convert_list_to_stream(
                    [
                        "2022-11-16T00:05:54.278000-08:00",
                        "2022-11-16T00:05:54.457000-08:00",
                    ]
                ),
            ),
            True,
            id="chain_log_stream",
        ),
        pytest.param(
            [
                "2022-11-16T00:05:54.278000-08:00",
                "2022-11-16T00:05:54.457000-08:00",
            ],
            False,
            id="non_stream_log",
        ),
    ],
)
def test__is_logs_stream_like(log_stream, expected):
    assert _is_logs_stream_like(log_stream) == expected


def test__add_log_from_parsed_log_streams_to_heap():
    """
    Test cases:

    Timestamp: 26 27 28 29 30 31
    Source 1:     --
    Source 2:           -- --
    Source 3:        -- -- --
    """
    heap: list[tuple[int, StructuredLogMessage]] = []
    input_parsed_log_streams: dict[int, ParsedLogStream] = {
        0: convert_list_to_stream(
            mock_parsed_logs_factory("Source 1", pendulum.parse("2022-11-16T00:05:54.270000-08:00"), 1)
        ),
        1: convert_list_to_stream(
            mock_parsed_logs_factory("Source 2", pendulum.parse("2022-11-16T00:05:54.290000-08:00"), 2)
        ),
        2: convert_list_to_stream(
            mock_parsed_logs_factory("Source 3", pendulum.parse("2022-11-16T00:05:54.380000-08:00"), 3)
        ),
    }

    # Check that we correctly get the first line of each non-empty log stream

    # First call: should add log records for all log streams
    _add_log_from_parsed_log_streams_to_heap(heap, input_parsed_log_streams)
    assert len(input_parsed_log_streams) == 3
    assert len(heap) == 3
    # Second call: source 1 is empty, should add log records for source 2 and source 3
    _add_log_from_parsed_log_streams_to_heap(heap, input_parsed_log_streams)
    assert len(input_parsed_log_streams) == 2  # Source 1 should be removed
    assert len(heap) == 5
    # Third call: source 1 and source 2 are empty, should add log records for source 3
    _add_log_from_parsed_log_streams_to_heap(heap, input_parsed_log_streams)
    assert len(input_parsed_log_streams) == 1  # Source 2 should be removed
    assert len(heap) == 6
    # Fourth call: source 1, source 2, and source 3 are empty, should not add any log records
    _add_log_from_parsed_log_streams_to_heap(heap, input_parsed_log_streams)
    assert len(input_parsed_log_streams) == 0  # Source 3 should be removed
    assert len(heap) == 6
    # Fifth call: all sources are empty, should not add any log records
    assert len(input_parsed_log_streams) == 0  # remains empty
    assert len(heap) == 6  # no change in heap size
    # Check heap
    expected_logs: list[str] = [
        "Source 1 Event 0",
        "Source 2 Event 0",
        "Source 3 Event 0",
        "Source 2 Event 1",
        "Source 3 Event 1",
        "Source 3 Event 2",
    ]
    actual_logs: list[str] = []
    for _ in range(len(heap)):
        _, log = heapq.heappop(heap)
        actual_logs.append(log.event)
    assert actual_logs == expected_logs


@pytest.mark.parametrize(
    ("heap_setup", "flush_size", "last_log", "expected_events"),
    [
        pytest.param(
            [("msg1", "2023-01-01"), ("msg2", "2023-01-02")],
            2,
            None,
            ["msg1", "msg2"],
            id="exact_size_flush",
        ),
        pytest.param(
            [
                ("msg1", "2023-01-01"),
                ("msg2", "2023-01-02"),
                ("msg3", "2023-01-03"),
                ("msg3", "2023-01-03"),
                ("msg5", "2023-01-05"),
            ],
            5,
            None,
            ["msg1", "msg2", "msg3", "msg5"],  # msg3 is deduplicated, msg5 has default timestamp
            id="flush_with_duplicates",
        ),
        pytest.param(
            [("msg1", "2023-01-01"), ("msg1", "2023-01-01"), ("msg2", "2023-01-02")],
            3,
            "msg1",
            ["msg2"],  # The last_log is "msg1", so any duplicates of "msg1" should be skipped
            id="flush_with_last_log",
        ),
        pytest.param(
            [("msg1", "DEFAULT"), ("msg1", "DEFAULT"), ("msg2", "DEFAULT")],
            3,
            "msg1",
            [
                "msg1",
                "msg1",
                "msg2",
            ],  # All messages have default timestamp, so they should be flushed even if last_log is "msg1"
            id="flush_with_default_timestamp_and_last_log",
        ),
        pytest.param(
            [("msg1", "2023-01-01"), ("msg2", "2023-01-02"), ("msg3", "2023-01-03")],
            2,
            None,
            ["msg1", "msg2"],  # Only the first two messages should be flushed
            id="flush_size_smaller_than_heap",
        ),
    ],
)
def test__flush_logs_out_of_heap(heap_setup, flush_size, last_log, expected_events):
    """Test the _flush_logs_out_of_heap function with different scenarios."""

    # Create structured log messages from the test setup
    heap = []
    messages = {}
    for i, (event, timestamp_str) in enumerate(heap_setup):
        if timestamp_str == "DEFAULT":
            timestamp = DEFAULT_SORT_DATETIME
        else:
            timestamp = pendulum.parse(timestamp_str)

        msg = StructuredLogMessage(event=event, timestamp=timestamp)
        messages[event] = msg
        heapq.heappush(heap, (_create_sort_key(msg.timestamp, i), msg))

    # Set last_log if specified in the test case
    last_log_obj = messages.get(last_log) if last_log is not None else None
    last_log_container = [last_log_obj]

    # Run the function under test
    result = list(_flush_logs_out_of_heap(heap, flush_size, last_log_container))

    # Verify the results
    assert len(result) == len(expected_events)
    assert len(heap) == (len(heap_setup) - flush_size)
    for i, expected_event in enumerate(expected_events):
        assert result[i].event == expected_event, f"result = {result}, expected_event = {expected_events}"

    # verify that the last log is updated correctly
    last_log_obj = last_log_container[0]
    assert last_log_obj is not None
    last_log_obj = cast("StructuredLogMessage", last_log_obj)
    assert last_log_obj.event == expected_events[-1]


def test_interleave_interleaves():
    log_sample1 = [
        "[2022-11-16T00:05:54.278-0800] {taskinstance.py:1258} INFO - Starting attempt 1 of 1",
    ]
    log_sample2 = [
        "[2022-11-16T00:05:54.295-0800] {taskinstance.py:1278} INFO - Executing <Task(TimeDeltaSensorAsync): wait> on 2022-11-16 08:05:52.324532+00:00",
        "[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task",
        "[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task",
        "[2022-11-16T00:05:54.300-0800] {standard_task_runner.py:55} INFO - Started process 52536 to run task",
        "[2022-11-16T00:05:54.306-0800] {standard_task_runner.py:82} INFO - Running: ['airflow', 'tasks', 'run', 'simple_async_timedelta', 'wait', 'manual__2022-11-16T08:05:52.324532+00:00', '--job-id', '33648', '--raw', '--subdir', '/Users/dstandish/code/airflow/airflow/example_dags/example_time_delta_sensor_async.py', '--cfg-path', '/var/folders/7_/1xx0hqcs3txd7kqt0ngfdjth0000gn/T/tmp725r305n']",
        "[2022-11-16T00:05:54.309-0800] {standard_task_runner.py:83} INFO - Job 33648: Subtask wait",
    ]
    log_sample3 = [
        "[2022-11-16T00:05:54.457-0800] {task_command.py:376} INFO - Running <TaskInstance: simple_async_timedelta.wait manual__2022-11-16T08:05:52.324532+00:00 [running]> on host daniels-mbp-2.lan",
        "[2022-11-16T00:05:54.592-0800] {taskinstance.py:1485} INFO - Exporting env vars: AIRFLOW_CTX_DAG_OWNER=airflow",
        "AIRFLOW_CTX_DAG_ID=simple_async_timedelta",
        "AIRFLOW_CTX_TASK_ID=wait",
        "AIRFLOW_CTX_LOGICAL_DATE=2022-11-16T08:05:52.324532+00:00",
        "AIRFLOW_CTX_TRY_NUMBER=1",
        "AIRFLOW_CTX_DAG_RUN_ID=manual__2022-11-16T08:05:52.324532+00:00",
        "[2022-11-16T00:05:54.604-0800] {taskinstance.py:1360} INFO - Pausing task as DEFERRED. dag_id=simple_async_timedelta, task_id=wait, execution_date=20221116T080552, start_date=20221116T080554",
    ]

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
    results: list[StructuredLogMessage] = list(
        _interleave_logs(
            convert_list_to_stream(log_sample2),
            convert_list_to_stream(log_sample1),
            convert_list_to_stream(log_sample3),
        )
    )
    results: list[dict] = TypeAdapter(list[StructuredLogMessage]).dump_python(results)
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

    logs = extract_events(
        _interleave_logs(
            convert_list_to_stream(sample_with_dupe.splitlines()),
            convert_list_to_stream([]),
            convert_list_to_stream(sample_with_dupe.splitlines()),
        )
    )
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

    input_logs = ",\n    ".join(["test"] * 10)
    logs = extract_events(
        _interleave_logs(
            convert_list_to_stream(input_logs.splitlines()),
        )
    )
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
