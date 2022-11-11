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

import time
from datetime import datetime as dt
from unittest import mock
from unittest.mock import call

import boto3
import moto
import pytest
from watchtower import CloudWatchLogHandler

from airflow.models import DAG, DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.log.cloudwatch_task_handler import CloudwatchTaskHandler
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from tests.test_utils.config import conf_vars


def get_time_str(time_in_milliseconds):
    dt_time = dt.utcfromtimestamp(time_in_milliseconds / 1000.0)
    return dt_time.strftime("%Y-%m-%d %H:%M:%S,000")


@pytest.fixture(autouse=True, scope="module")
def logmock():
    with moto.mock_logs():
        yield


class TestCloudwatchTaskHandler:
    @conf_vars({("logging", "remote_log_conn_id"): "aws_default"})
    @pytest.fixture(autouse=True)
    def setup(self, create_log_template, tmp_path_factory):
        self.remote_log_group = "log_group_name"
        self.region_name = "us-west-2"
        self.local_log_location = str(tmp_path_factory.mktemp("local-cloudwatch-log-location"))
        create_log_template("{dag_id}/{task_id}/{execution_date}/{try_number}.log")
        self.cloudwatch_task_handler = CloudwatchTaskHandler(
            self.local_log_location,
            f"arn:aws:logs:{self.region_name}:11111111:log-group:{self.remote_log_group}",
        )
        self.cloudwatch_task_handler.hook

        date = datetime(2020, 1, 1)
        dag_id = "dag_for_testing_cloudwatch_task_handler"
        task_id = "task_for_testing_cloudwatch_log_handler"
        self.dag = DAG(dag_id=dag_id, start_date=date)
        task = EmptyOperator(task_id=task_id, dag=self.dag)
        dag_run = DagRun(dag_id=self.dag.dag_id, execution_date=date, run_id="test", run_type="scheduled")
        with create_session() as session:
            session.add(dag_run)
            session.commit()
            session.refresh(dag_run)

        self.ti = TaskInstance(task=task, run_id=dag_run.run_id)
        self.ti.dag_run = dag_run
        self.ti.try_number = 1
        self.ti.state = State.RUNNING

        self.remote_log_stream = (f"{dag_id}/{task_id}/{date.isoformat()}/{self.ti.try_number}.log").replace(
            ":", "_"
        )

        moto.moto_api._internal.models.moto_api_backend.reset()
        self.conn = boto3.client("logs", region_name=self.region_name)

        yield

        self.cloudwatch_task_handler.handler = None
        with create_session() as session:
            session.query(DagRun).delete()

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
                f"[{get_time_str(current_time-2000)}] First",
                f"[{get_time_str(current_time-1000)}] Second",
                f"[{get_time_str(current_time)}] Third",
            ]
        )

    def test_read(self):
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

        msg_template = "*** Reading remote log from Cloudwatch log_group: {} log_stream: {}.\n{}\n"
        events = "\n".join(
            [
                f"[{get_time_str(current_time-2000)}] First",
                f"[{get_time_str(current_time-1000)}] Second",
                f"[{get_time_str(current_time)}] Third",
            ]
        )
        assert self.cloudwatch_task_handler.read(self.ti) == (
            [[("", msg_template.format(self.remote_log_group, self.remote_log_stream, events))]],
            [{"end_of_log": True}],
        )

    def test_should_read_from_local_on_failure_to_fetch_remote_logs(self):
        """Check that local logs are displayed on failure to fetch remote logs"""
        self.cloudwatch_task_handler.set_context(self.ti)
        with mock.patch.object(self.cloudwatch_task_handler, "get_cloudwatch_logs") as mock_get_logs:
            mock_get_logs.side_effect = Exception("Failed to connect")
            log, metadata = self.cloudwatch_task_handler._read(self.ti, self.ti.try_number)
        expected_log = (
            f"*** Unable to read remote logs from Cloudwatch (log_group: {self.remote_log_group}, "
            f"log_stream: {self.remote_log_stream})\n*** Failed to connect\n\n"
            # The value of "log_pos" is equal to the length of this next line
            f"*** Reading local file: {self.local_log_location}/{self.remote_log_stream}\n"
        )
        assert log == expected_log
        expected_log_pos = 26 + len(self.local_log_location) + len(self.remote_log_stream)
        assert metadata == {"end_of_log": False, "log_pos": expected_log_pos}
        mock_get_logs.assert_called_once_with(stream_name=self.remote_log_stream)

    def test_close_prevents_duplicate_calls(self):
        with mock.patch("watchtower.CloudWatchLogHandler.close") as mock_log_handler_close:
            with mock.patch("airflow.utils.log.file_task_handler.FileTaskHandler.set_context"):
                self.cloudwatch_task_handler.set_context(self.ti)
                for _ in range(5):
                    self.cloudwatch_task_handler.close()

                mock_log_handler_close.assert_called_once()


def generate_log_events(conn, log_group_name, log_stream_name, log_events):
    conn.create_log_group(logGroupName=log_group_name)
    conn.create_log_stream(logGroupName=log_group_name, logStreamName=log_stream_name)
    conn.put_log_events(logGroupName=log_group_name, logStreamName=log_stream_name, logEvents=log_events)
