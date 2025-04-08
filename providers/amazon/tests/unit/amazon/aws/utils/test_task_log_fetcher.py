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

from datetime import timedelta
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher


class TestAwsTaskLogFetcher:
    @mock.patch("logging.Logger")
    def set_up_log_fetcher(self, logger_mock):
        self.logger_mock = logger_mock

        self.log_fetcher = AwsTaskLogFetcher(
            log_group="test_log_group",
            log_stream_name="test_log_stream_name",
            fetch_interval=timedelta(milliseconds=1),
            logger=logger_mock,
        )

    def setup_method(self):
        self.set_up_log_fetcher()

    @mock.patch(
        "threading.Event.is_set",
        side_effect=(False, False, False, True),
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events",
        side_effect=(
            iter(
                [
                    {"timestamp": 1617400267123, "message": "First"},
                    {"timestamp": 1617400367456, "message": "Second"},
                ]
            ),
            iter(
                [
                    {"timestamp": 1617400467789, "message": "Third"},
                ]
            ),
            iter([]),
        ),
    )
    def test_run(self, get_log_events_mock, event_is_set_mock):
        self.log_fetcher.run()

        self.logger_mock.info.assert_has_calls(
            [
                mock.call("[2021-04-02 21:51:07,123] First"),
                mock.call("[2021-04-02 21:52:47,456] Second"),
                mock.call("[2021-04-02 21:54:27,789] Third"),
            ]
        )

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events",
        side_effect=ClientError({"Error": {"Code": "ResourceNotFoundException"}}, None),
    )
    def test_get_log_events_with_expected_error(self, get_log_events_mock):
        with pytest.raises(StopIteration):
            next(self.log_fetcher._get_log_events())

    @mock.patch("airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events")
    def test_get_log_events_with_unexpected_error(self, get_log_events_mock):
        get_log_events_mock.side_effect = ConnectionError("Fake: Failed to connect")
        with pytest.raises(ConnectionError, match="Fake: Failed to connect"):
            next(self.log_fetcher._get_log_events())

    @mock.patch.object(AwsLogsHook, "conn", new_callable=PropertyMock)
    def test_get_log_events_updates_token(self, logs_conn_mock):
        logs_conn_mock().get_log_events.return_value = {
            "events": ["my_event"],
            "nextForwardToken": "my_next_token",
        }

        token = AwsLogsHook.ContinuationToken()
        list(self.log_fetcher._get_log_events(token))

        assert token.value == "my_next_token"
        # 2 calls expected, it's only on the second one that the stop condition old_token == next_token is met
        assert logs_conn_mock().get_log_events.call_count == 2

    def test_event_to_str(self):
        events = [
            {"timestamp": 1617400267123, "message": "First"},
            {"timestamp": 1617400367456, "message": "Second"},
            {"timestamp": 1617400467789, "message": "Third"},
        ]
        assert [self.log_fetcher.event_to_str(event) for event in events] == (
            [
                "[2021-04-02 21:51:07,123] First",
                "[2021-04-02 21:52:47,456] Second",
                "[2021-04-02 21:54:27,789] Third",
            ]
        )

    @mock.patch.object(AwsLogsHook, "conn")
    def test_get_last_log_message_with_no_log_events(self, mock_conn):
        assert self.log_fetcher.get_last_log_message() is None

    @mock.patch.object(AwsLogsHook, "conn")
    def test_get_last_log_message_with_log_events(self, log_conn_mock):
        log_conn_mock.get_log_events.return_value = {
            "events": [
                {"timestamp": 1617400267123, "message": "Last"},
            ]
        }
        assert self.log_fetcher.get_last_log_message() == "Last"

    @mock.patch.object(AwsLogsHook, "conn")
    def test_get_last_log_messages_with_log_events(self, log_conn_mock):
        log_conn_mock.get_log_events.return_value = {
            "events": [
                {"timestamp": 1617400267123, "message": "First"},
                {"timestamp": 1617400367456, "message": "Second"},
                {"timestamp": 1617400367458, "message": "Third"},
            ]
        }
        assert self.log_fetcher.get_last_log_messages(2) == ["First", "Second", "Third"]

    @mock.patch.object(AwsLogsHook, "conn")
    def test_get_last_log_messages_with_no_log_events(self, mock_conn):
        assert self.log_fetcher.get_last_log_messages(2) == []
