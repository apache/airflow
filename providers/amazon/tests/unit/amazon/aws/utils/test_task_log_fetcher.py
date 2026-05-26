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
from datetime import timedelta
from unittest import mock
from unittest.mock import PropertyMock

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.logs import AwsLogsHook
from airflow.providers.amazon.aws.utils.task_log_fetcher import AwsTaskLogFetcher, _parse_log_level


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

        self.logger_mock.log.assert_has_calls(
            [
                mock.call(logging.INFO, "[2021-04-02 21:51:07,123] First"),
                mock.call(logging.INFO, "[2021-04-02 21:52:47,456] Second"),
                mock.call(logging.INFO, "[2021-04-02 21:54:27,789] Third"),
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

    @mock.patch(
        "threading.Event.is_set",
        side_effect=(False, True),
    )
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.logs.AwsLogsHook.get_log_events",
        side_effect=(
            iter(
                [
                    {
                        "timestamp": 1617400267123,
                        "message": '{"levelname": "ERROR", "message": "Something failed"}',
                    },
                    {
                        "timestamp": 1617400367456,
                        "message": "WARNING: disk space low",
                    },
                    {
                        "timestamp": 1617400467789,
                        "message": "Just a plain message",
                    },
                ]
            ),
        ),
    )
    def test_run_with_log_level_detection(self, get_log_events_mock, event_is_set_mock):
        self.log_fetcher.run()

        self.logger_mock.log.assert_has_calls(
            [
                mock.call(
                    logging.ERROR,
                    '[2021-04-02 21:51:07,123] {"levelname": "ERROR", "message": "Something failed"}',
                ),
                mock.call(logging.WARNING, "[2021-04-02 21:52:47,456] WARNING: disk space low"),
                mock.call(logging.INFO, "[2021-04-02 21:54:27,789] Just a plain message"),
            ]
        )


class TestParseLogLevel:
    @pytest.mark.parametrize(
        ("message", "expected_level"),
        [
            ('{"levelname": "ERROR", "message": "fail"}', logging.ERROR),
            ('{"levelname": "WARNING", "message": "warn"}', logging.WARNING),
            ('{"levelname": "DEBUG", "message": "dbg"}', logging.DEBUG),
            ('{"levelname": "CRITICAL", "message": "crit"}', logging.CRITICAL),
            ('{"levelname": "INFO", "message": "ok"}', logging.INFO),
            ('{"level": "error", "msg": "fail"}', logging.ERROR),
            ('{"level": "WARNING", "msg": "warn"}', logging.WARNING),
        ],
        ids=[
            "json-error",
            "json-warning",
            "json-debug",
            "json-critical",
            "json-info",
            "json-level-field-lowercase",
            "json-level-field-uppercase",
        ],
    )
    def test_json_structured_logs(self, message, expected_level):
        assert _parse_log_level(message) == expected_level

    @pytest.mark.parametrize(
        ("message", "expected_level"),
        [
            ("ERROR: something broke", logging.ERROR),
            ("WARNING: watch out", logging.WARNING),
            ("WARN: also watch out", logging.WARNING),
            ("DEBUG: details", logging.DEBUG),
            ("CRITICAL: very bad", logging.CRITICAL),
            ("FATAL: system down", logging.CRITICAL),
            ("[ERROR] something broke", logging.ERROR),
            ("[WARNING] watch out", logging.WARNING),
            ("INFO - all good", logging.INFO),
        ],
        ids=[
            "prefix-error",
            "prefix-warning",
            "prefix-warn",
            "prefix-debug",
            "prefix-critical",
            "prefix-fatal",
            "bracketed-error",
            "bracketed-warning",
            "prefix-info-dash",
        ],
    )
    def test_plain_text_prefix(self, message, expected_level):
        assert _parse_log_level(message) == expected_level

    @pytest.mark.parametrize(
        "message",
        [
            "Just a regular log message",
            "This message mentions ERROR in the middle",
            "",
            "2026-05-18 08:06:04 some log",
            "{invalid json",
        ],
        ids=[
            "plain-text",
            "error-in-middle",
            "empty",
            "timestamp-prefix",
            "invalid-json",
        ],
    )
    def test_defaults_to_info(self, message):
        assert _parse_log_level(message) == logging.INFO
