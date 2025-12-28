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
import textwrap
import time
from datetime import datetime as dt, timedelta, timezone
from unittest import mock
from unittest.mock import ANY, call

import boto3
import pendulum
import pytest
import time_machine
from moto import mock_aws
from pydantic import TypeAdapter
from watchtower import CloudWatchLogHandler

from airflow.models import DAG, DagRun, TaskInstance
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.log.cloudwatch_task_handler import (
    CloudWatchRemoteLogIO,
    CloudwatchTaskHandler,
)
from airflow.providers.amazon.aws.utils import datetime_to_epoch_utc_ms
from airflow.utils.state import State
from airflow.utils.timezone import datetime

from tests_common.test_utils.compat import EmptyOperator
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_2_PLUS


def get_time_str(time_in_milliseconds):
    dt_time = dt.fromtimestamp(time_in_milliseconds / 1000.0, tz=timezone.utc)
    return dt_time.strftime("%Y-%m-%dT%H:%M:%SZ")


@pytest.fixture(autouse=True)
def logmock():
    with mock_aws():
        yield


# We only test this directly on Airflow 3
@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="This path only works on Airflow 3")
class TestCloudRemoteLogIO:
    # We use the cap_structlog so that our changes get reverted for us
    @pytest.fixture(autouse=True)
    def setup_tests(self, create_runtime_ti, tmp_path, monkeypatch):
        import structlog

        import airflow.logging_config
        import airflow.sdk.log
        from airflow.sdk import BaseOperator

        task = BaseOperator(task_id="task_1")
        self.ti = create_runtime_ti(task)

        self.remote_log_group = "log_group_name"
        self.region_name = "us-west-2"
        self.task_log_path = "dag_id=a/0:0.log"
        self.local_log_location = tmp_path / "local-cloudwatch-log-location"
        self.local_log_location.mkdir()
        # Create the local log file structure
        task_log_path_parts = self.task_log_path.split("/")
        dag_dir = self.local_log_location / task_log_path_parts[0]
        dag_dir.mkdir()
        task_log_file = dag_dir / task_log_path_parts[1]
        task_log_file.touch()

        # The subject under test
        self.subject = CloudWatchRemoteLogIO(
            base_log_folder=self.local_log_location,
            log_group_arn=f"arn:aws:logs:{self.region_name}:11111111:log-group:{self.remote_log_group}",
        )

        conn = boto3.client("logs", region_name=self.region_name)
        conn.create_log_group(logGroupName=self.remote_log_group)

        processors = structlog.get_config()["processors"]
        logger_factory = structlog.get_config()["logger_factory"]
        old_processors = processors.copy()

        try:
            # Modify `_Configuration.default_processors` set via `configure` but always
            # keep the list instance intact to not break references held by bound
            # loggers.

            # Set up the right chain of processors so the event looks like we want for our full test
            if AIRFLOW_V_3_2_PLUS:
                monkeypatch.setattr(
                    airflow.logging_config._ActiveLoggingConfig, "remote_task_log", self.subject
                )
            else:
                monkeypatch.setattr(airflow.logging_config, "REMOTE_TASK_LOG", self.subject)
            try:
                procs = airflow.sdk.log.logging_processors(colors=False, json_output=False)
            except TypeError:
                # Compat issue only comes up in the tests, not in the real code
                procs, _ = airflow.sdk.log.logging_processors(enable_pretty_log=False)
            processors.clear()
            processors.extend(procs)

            # Replace the last "output" one with a DropEvent one instead - else we get the output on stdout
            def drop(*args):
                raise structlog.DropEvent()

            processors[-1] = drop
            structlog.configure(
                processors=processors,
                # Create a logger factory and pass in the file path we want it to use
                # This is because we use the logger to determine the streamname/filepath
                # in the CloudWatchRemoteLogIO processor.
                logger_factory=structlog.PrintLoggerFactory(file=task_log_file.open("w+")),
            )
            yield
        finally:
            # remove LogCapture and restore original processors
            processors.clear()
            processors.extend(old_processors)
            structlog.configure(processors=old_processors, logger_factory=logger_factory)

    @time_machine.travel(datetime(2025, 3, 27, 21, 58, 1, 2345), tick=False)
    def test_log_message(self):
        # Use a context instead of a decorator on the test method because we need access to self to
        # get the path from the setup method.
        with conf_vars({("logging", "base_log_folder"): self.local_log_location.as_posix()}):
            import structlog

            log = structlog.get_logger()
            log.info("Hi", foo="bar")
            # We need to close in order to flush the logs etc.
            self.subject.close()

            # close call should only flush the logs and not set shutting_down to True
            assert self.subject.handler.shutting_down is False

            # Inside the Cloudwatch logger we swap colons for underscores since colons are not allowed in
            # stream names.
            stream_name = self.task_log_path.replace(":", "_")
            logs = self.subject.read(stream_name, self.ti)

            metadata, logs = logs

            assert metadata == [
                f"Reading remote log from Cloudwatch log_group: log_group_name log_stream: {stream_name}"
            ]
            assert logs == [
                '{"foo": "bar", "event": "Hi", "level": "info", "timestamp": "2025-03-27T21:58:01.002000+00:00"}\n'
            ]


@pytest.mark.db_test
class TestCloudwatchTaskHandler:
    def clear_db(self):
        if AIRFLOW_V_3_0_PLUS:
            clear_db_runs()
            clear_db_dags()
            clear_db_dag_bundles()

    @pytest.fixture(autouse=True)
    def setup(self, create_log_template, tmp_path_factory, session, testing_dag_bundle):
        # self.clear_db()
        with conf_vars({("logging", "remote_log_conn_id"): "aws_default"}):
            self.remote_log_group = "log_group_name"
            self.region_name = "us-west-2"
            self.local_log_location = str(tmp_path_factory.mktemp("local-cloudwatch-log-location"))
            if AIRFLOW_V_3_0_PLUS:
                create_log_template("{dag_id}/{task_id}/{logical_date}/{try_number}.log")
            else:
                create_log_template("{dag_id}/{task_id}/{execution_date}/{try_number}.log")
            self.cloudwatch_task_handler = CloudwatchTaskHandler(
                self.local_log_location,
                f"arn:aws:logs:{self.region_name}:11111111:log-group:{self.remote_log_group}",
            )

        date = datetime(2020, 1, 1)
        dag_id = "dag_for_testing_cloudwatch_task_handler"
        task_id = "task_for_testing_cloudwatch_log_handler"
        self.dag = DAG(dag_id=dag_id, schedule=None, start_date=date)
        task = EmptyOperator(task_id=task_id, dag=self.dag)
        if AIRFLOW_V_3_0_PLUS:
            sync_dag_to_db(self.dag)
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                logical_date=date,
                run_id="test",
                run_type="scheduled",
            )
        else:
            dag_run = DagRun(
                dag_id=self.dag.dag_id,
                execution_date=date,
                run_id="test",
                run_type="scheduled",
            )
        session.add(dag_run)
        session.commit()
        session.refresh(dag_run)

        if AIRFLOW_V_3_0_PLUS:
            from airflow.models.dag_version import DagVersion

            dag_version = DagVersion.get_latest_version(self.dag.dag_id, session=session)
            self.ti = TaskInstance(task=task, run_id=dag_run.run_id, dag_version_id=dag_version.id)
        else:
            self.ti = TaskInstance(task=task, run_id=dag_run.run_id)
        self.ti.dag_run = dag_run
        self.ti.try_number = 1
        self.ti.state = State.RUNNING
        session.add(self.ti)
        session.commit()

        self.remote_log_stream = (f"{dag_id}/{task_id}/{date.isoformat()}/{self.ti.try_number}.log").replace(
            ":", "_"
        )
        self.conn = boto3.client("logs", region_name=self.region_name)

        yield

        self.cloudwatch_task_handler.handler = None
        del self.cloudwatch_task_handler

        self.clear_db()

    def test_hook(self):
        assert isinstance(self.cloudwatch_task_handler.hook, AwsLogsHook)

    def test_handler(self):
        self.cloudwatch_task_handler.set_context(self.ti)
        assert isinstance(self.cloudwatch_task_handler.handler, CloudWatchLogHandler)

    def test_write(self):
        handler = self.cloudwatch_task_handler
        handler.set_context(self.ti)
        messages = [str(i) for i in range(10)]

        with mock.patch("watchtower.CloudWatchLogHandler.emit") as mock_emit:
            for message in messages:
                handler.handle(message)
            mock_emit.assert_has_calls([call(message) for message in messages])

    @time_machine.travel(datetime(2025, 3, 27, 21, 58, 1, 2345), tick=False)
    def test_read(self, monkeypatch):
        # Confirmed via AWS Support call:
        # CloudWatch events must be ordered chronologically otherwise
        # boto3 put_log_event API throws InvalidParameterException
        # (moto does not throw this exception)
        current_time = int(time.time()) * 1000
        generate_log_events(
            self.conn,
            self.remote_log_group,
            self.remote_log_stream,
            [
                {"timestamp": current_time - 2000, "message": "First"},
                {"timestamp": current_time - 1000, "message": "Second"},
                {"timestamp": current_time, "message": "Third"},
            ],
        )
        monkeypatch.setattr(
            self.cloudwatch_task_handler,
            "_read_from_logs_server",
            lambda ti, worker_log_rel_path: ([], []),
        )
        msg_template = textwrap.dedent("""
             INFO - ::group::Log message source details
            *** Reading remote log from Cloudwatch log_group: {} log_stream: {}
             INFO - ::endgroup::
            {}
        """)[1:][:-1]  # Strip off leading and trailing new lines, but not spaces

        logs, metadata = self.cloudwatch_task_handler.read(self.ti)
        if AIRFLOW_V_3_0_PLUS:
            from airflow.utils.log.file_task_handler import StructuredLogMessage

            logs = list(logs)
            results = TypeAdapter(list[StructuredLogMessage]).dump_python(logs)
            assert results[-4:] == [
                {"event": "::endgroup::", "timestamp": None},
                {
                    "event": "[2025-03-27T21:57:59Z] First",
                    "timestamp": pendulum.datetime(2025, 3, 27, 21, 57, 59),
                },
                {
                    "event": "[2025-03-27T21:58:00Z] Second",
                    "timestamp": pendulum.datetime(2025, 3, 27, 21, 58, 0),
                },
                {
                    "event": "[2025-03-27T21:58:01Z] Third",
                    "timestamp": pendulum.datetime(2025, 3, 27, 21, 58, 1),
                },
            ]
            assert metadata == {"end_of_log": False, "log_pos": 3}
        else:
            events = "\n".join(
                [
                    f"[{get_time_str(current_time - 2000)}] First",
                    f"[{get_time_str(current_time - 1000)}] Second",
                    f"[{get_time_str(current_time)}] Third",
                ]
            )
            assert logs == [
                [
                    (
                        "",
                        msg_template.format(self.remote_log_group, self.remote_log_stream, events),
                    )
                ]
            ]

    @pytest.mark.parametrize(
        ("end_date", "expected_end_time"),
        [
            (None, None),
            (
                datetime(2020, 1, 2),
                datetime_to_epoch_utc_ms(datetime(2020, 1, 2) + timedelta(seconds=30)),
            ),
        ],
    )
    @mock.patch.object(AwsLogsHook, "get_log_events")
    def test_get_cloudwatch_logs(self, mock_get_log_events, end_date, expected_end_time):
        self.ti.end_date = end_date
        self.cloudwatch_task_handler.io.get_cloudwatch_logs(self.remote_log_stream, self.ti)
        mock_get_log_events.assert_called_once_with(
            log_group=self.remote_log_group,
            log_stream_name=self.remote_log_stream,
            end_time=expected_end_time,
        )

    @pytest.mark.parametrize(
        ("conf_json_serialize", "expected_serialized_output"),
        [
            pytest.param(
                "airflow.providers.amazon.aws.log.cloudwatch_task_handler.json_serialize_legacy",
                '{"datetime": "2023-01-01T00:00:00+00:00", "customObject": null}',
                id="json-serialize-legacy",
            ),
            pytest.param(
                "airflow.providers.amazon.aws.log.cloudwatch_task_handler.json_serialize",
                '{"datetime": "2023-01-01T00:00:00+00:00", "customObject": "SomeCustomSerialization(...)"}',
                id="json-serialize",
            ),
            pytest.param(
                None,
                '{"datetime": "2023-01-01T00:00:00+00:00", "customObject": null}',
                id="not-set",
            ),
        ],
    )
    @mock.patch.object(AwsLogsHook, "get_log_events")
    def test_write_json_logs(self, mock_get_log_events, conf_json_serialize, expected_serialized_output):
        class ToSerialize:
            def __init__(self):
                pass

            def __repr__(self):
                return "SomeCustomSerialization(...)"

        with conf_vars({("aws", "cloudwatch_task_handler_json_serializer"): conf_json_serialize}):
            handler = self.cloudwatch_task_handler
            handler.set_context(self.ti)
            message = logging.LogRecord(
                name="test_log_record",
                level=logging.DEBUG,
                pathname="fake.path",
                lineno=42,
                args=None,
                exc_info=None,
                msg={
                    "datetime": datetime(2023, 1, 1),
                    "customObject": ToSerialize(),
                },
            )
            with (
                mock.patch("watchtower.threading.Thread"),
                mock.patch("watchtower.queue.Queue") as mq,
            ):
                handler.handle(message)
                mq.return_value.put.assert_called_once_with(
                    {"message": expected_serialized_output, "timestamp": ANY}
                )

    def test_close_prevents_duplicate_calls(self):
        with mock.patch("watchtower.CloudWatchLogHandler.close") as mock_log_handler_close:
            with mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.set_context"):
                self.cloudwatch_task_handler.set_context(self.ti)
                for _ in range(5):
                    self.cloudwatch_task_handler.close()

                mock_log_handler_close.assert_called_once()

    def test_filename_template_for_backward_compatibility(self):
        # filename_template arg support for running the latest provider on airflow 2
        CloudwatchTaskHandler(
            self.local_log_location,
            f"arn:aws:logs:{self.region_name}:11111111:log-group:{self.remote_log_group}",
            filename_template=None,
        )

    def test_event_to_str(self):
        handler = self.cloudwatch_task_handler
        current_time = int(time.time()) * 1000
        events = [
            {"timestamp": current_time - 2000, "message": "First"},
            {"timestamp": current_time - 1000, "message": "Second"},
            {"timestamp": current_time, "message": "Third"},
        ]
        assert [handler._event_to_str(event) for event in events] == (
            [
                f"[{get_time_str(current_time - 2000)}] First",
                f"[{get_time_str(current_time - 1000)}] Second",
                f"[{get_time_str(current_time)}] Third",
            ]
        )


def generate_log_events(conn, log_group_name, log_stream_name, log_events):
    conn.create_log_group(logGroupName=log_group_name)
    conn.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
    conn.put_log_events(logGroupName=log_group_name, logStreamName=log_stream_name, logEvents=log_events)
