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

from pathlib import Path
from unittest import mock

import pytest
import structlog

from airflow.sdk import log as sdk_log
from airflow.sdk.log import upload_to_remote


class TestConfigureLogging:
    @pytest.mark.parametrize("sending_to_supervisor", [True, False])
    @mock.patch("airflow.sdk.log.load_remote_log_handler")
    @mock.patch("airflow.sdk._shared.logging.configure_logging")
    def test_injects_remote_processors_after_dictconfig(
        self, mock_configure_logging, mock_load_remote_log_handler, sending_to_supervisor
    ):
        initial_processor = mock.Mock()
        final_renderer = mock.Mock()
        remote_processor = mock.Mock()
        remote_handler = mock.Mock(spec=["processors"])
        remote_handler.processors = (remote_processor,)
        calls = []

        def configure_structlog(**_):
            calls.append("dictConfig")
            structlog.configure(processors=[initial_processor, final_renderer])

        def load_remote_handler():
            calls.append("load_remote_log_handler")
            return remote_handler

        mock_configure_logging.side_effect = configure_structlog
        mock_load_remote_log_handler.side_effect = load_remote_handler
        original_processors = structlog.get_config()["processors"]
        sdk_log.configure_logging.cache_clear()

        try:
            sdk_log.configure_logging(sending_to_supervisor=sending_to_supervisor)

            assert mock_configure_logging.called
            if sending_to_supervisor:
                assert calls == ["dictConfig"]
                assert structlog.get_config()["processors"] == [initial_processor, final_renderer]
            else:
                assert calls == ["dictConfig", "load_remote_log_handler"]
                assert structlog.get_config()["processors"] == [
                    initial_processor,
                    remote_processor,
                    final_renderer,
                ]
        finally:
            structlog.configure(processors=original_processors)
            sdk_log.configure_logging.cache_clear()

    @mock.patch("airflow.sdk._shared.logging.structlog.structlog_processors")
    @mock.patch("airflow.sdk.log.load_remote_log_handler", return_value=object())
    def test_allows_remote_handler_without_processors(
        self, mock_load_remote_log_handler, mock_structlog_processors
    ):
        initial_processor = mock.Mock()
        final_renderer = mock.Mock()
        mock_structlog_processors.return_value = ([initial_processor], None, final_renderer)
        sdk_log.logging_processors.cache_clear()

        try:
            processors = sdk_log.logging_processors(json_output=False)
        finally:
            sdk_log.logging_processors.cache_clear()

        mock_load_remote_log_handler.assert_called_once_with()
        assert processors == (initial_processor, sdk_log.mask_logs, final_renderer)


class LegacyRemoteLogIO:
    """A handler whose ``upload`` predates the ``ti_context`` keyword."""

    def __init__(self):
        self.calls: list[tuple] = []

    def upload(self, path, ti):
        self.calls.append((path, ti))


class ContextAwareRemoteLogIO:
    """A handler that opts in to receiving the run context."""

    def __init__(self):
        self.calls: list[tuple] = []

    def upload(self, path, ti, *, ti_context=None):
        self.calls.append((path, ti, ti_context))


@mock.patch("airflow.sdk.log.relative_path_from_logger", return_value=Path("dag/task/1.log"))
@mock.patch("airflow.sdk.log.load_remote_log_handler")
class TestUploadToRemoteTIContext:
    def test_legacy_handler_does_not_receive_ti_context(self, mock_load_handler, mock_relative_path):
        handler = LegacyRemoteLogIO()
        mock_load_handler.return_value = handler
        ti, ti_context = mock.Mock(), mock.Mock()

        upload_to_remote(mock.MagicMock(), ti, ti_context=ti_context)

        assert handler.calls == [("dag/task/1.log", ti)]

    def test_opted_in_handler_receives_ti_context(self, mock_load_handler, mock_relative_path):
        handler = ContextAwareRemoteLogIO()
        mock_load_handler.return_value = handler
        ti, ti_context = mock.Mock(), mock.Mock()

        upload_to_remote(mock.MagicMock(), ti, ti_context=ti_context)

        assert handler.calls == [("dag/task/1.log", ti, ti_context)]

    def test_no_ti_context_keeps_handler_default(self, mock_load_handler, mock_relative_path):
        handler = ContextAwareRemoteLogIO()
        mock_load_handler.return_value = handler
        ti = mock.Mock()

        upload_to_remote(mock.MagicMock(), ti)

        assert handler.calls == [("dag/task/1.log", ti, None)]
