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
from unittest.mock import MagicMock, patch

from airflow.utils.log.file_task_handler import FileTaskHandler


class TestFileTaskHandlerLogServer:
    """Tests for _read_from_logs_server 404 handling."""

    def setup_method(self):
        self.handler = FileTaskHandler(base_log_folder="/tmp/test_logs")
        self.ti = MagicMock()
        self.ti.hostname = "worker-1"
        self.ti.triggerer_job = None
        self.ti.task = None

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    @patch.object(FileTaskHandler, "_get_log_retrieval_url")
    @patch.object(FileTaskHandler, "_read_from_local")
    def test_404_falls_back_to_local_when_available(self, mock_read_local, mock_get_url, mock_fetch):
        """When log server returns 404 and local logs exist, use local logs."""
        mock_get_url.return_value = ("http://worker-1/log", "dag/run/task/1.log")
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_fetch.return_value = mock_response
        mock_read_local.return_value = (["Found local files:"], ["log content"])

        messages, logs = self.handler._read_from_logs_server(self.ti, "dag/run/task/1.log")

        assert logs == ["log content"]
        assert "Found local files:" in messages
        mock_read_local.assert_called_once_with(Path("/tmp/test_logs", "dag/run/task/1.log"))

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    @patch.object(FileTaskHandler, "_get_log_retrieval_url")
    @patch.object(FileTaskHandler, "_read_from_local")
    def test_404_shows_clear_message_when_no_local_fallback(self, mock_read_local, mock_get_url, mock_fetch):
        """When log server returns 404 and no local logs exist, show helpful message."""
        mock_get_url.return_value = ("http://worker-1/log", "dag/run/task/1.log")
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_fetch.return_value = mock_response
        mock_read_local.return_value = ([], [])

        messages, logs = self.handler._read_from_logs_server(self.ti, "dag/run/task/1.log")

        assert len(messages) == 1
        assert "worker-1" in messages[0]
        assert "no longer accessible" in messages[0]
        assert "remote logging" in messages[0]
        assert logs == []

    @patch("airflow.utils.log.file_task_handler._fetch_logs_from_service")
    @patch.object(FileTaskHandler, "_get_log_retrieval_url")
    def test_403_shows_secret_key_message(self, mock_get_url, mock_fetch):
        """When log server returns 403, show secret key configuration message."""
        mock_get_url.return_value = ("http://worker-1/log", "dag/run/task/1.log")
        mock_response = MagicMock()
        mock_response.status_code = 403
        mock_fetch.return_value = mock_response
        mock_response.raise_for_status.side_effect = Exception("403")

        messages, logs = self.handler._read_from_logs_server(self.ti, "dag/run/task/1.log")

        assert any("secret_key" in m for m in messages)
