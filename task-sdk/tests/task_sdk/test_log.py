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

from unittest import mock

import pytest
import structlog

from airflow.sdk import log as sdk_log


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
