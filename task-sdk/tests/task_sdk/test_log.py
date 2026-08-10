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

from airflow.sdk.log import upload_to_remote


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
