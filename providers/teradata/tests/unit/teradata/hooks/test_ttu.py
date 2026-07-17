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

import subprocess
from unittest import mock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.teradata.hooks.ttu import TtuHook


class TestTtuHook:
    @mock.patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    def test_get_conn_with_valid_params(self, mock_get_connection):
        # Setup
        mock_conn = mock.MagicMock()
        mock_conn.login = "test_user"
        mock_conn.password = "test_pass"
        mock_conn.host = "test_host"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        # Execute
        hook = TtuHook()
        conn = hook.get_conn()

        # Assert
        assert conn["login"] == "test_user"
        assert conn["password"] == "test_pass"
        assert conn["host"] == "test_host"

    @mock.patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    def test_get_conn_missing_params(self, mock_get_connection):
        # Setup
        mock_conn = mock.MagicMock()
        mock_conn.login = None
        mock_conn.password = "test_pass"
        mock_conn.host = "test_host"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        # Execute and Assert
        hook = TtuHook()
        with pytest.raises(AirflowException, match="Missing required connection parameters"):
            hook.get_conn()

    @mock.patch("subprocess.Popen")
    @mock.patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    def test_close_conn_subprocess_running(self, mock_get_connection, mock_popen):
        # Setup
        mock_conn = mock.MagicMock()
        mock_conn.login = "test_user"
        mock_conn.password = "test_pass"
        mock_conn.host = "test_host"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        mock_process = mock.MagicMock()
        mock_process.poll.return_value = None
        mock_popen.return_value = mock_process

        # Execute
        hook = TtuHook()
        conn = hook.get_conn()
        conn["sp"] = mock_process
        hook.close_conn()

        # Assert
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called_once_with(timeout=5)
        assert hook.conn is None

    @mock.patch("subprocess.Popen")
    @mock.patch("airflow.providers.teradata.hooks.ttu.TtuHook.get_connection")
    def test_close_conn_subprocess_timeout(self, mock_get_connection, mock_popen):
        # Setup
        mock_conn = mock.MagicMock()
        mock_conn.login = "test_user"
        mock_conn.password = "test_pass"
        mock_conn.host = "test_host"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        mock_process = mock.MagicMock()
        mock_process.poll.return_value = None
        mock_process.wait.side_effect = subprocess.TimeoutExpired(cmd="test", timeout=5)
        mock_popen.return_value = mock_process

        # Execute
        hook = TtuHook()
        conn = hook.get_conn()
        conn["sp"] = mock_process
        hook.close_conn()

        # Assert
        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called_once()
        mock_process.kill.assert_called_once()
        assert hook.conn is None

    @mock.patch("airflow.providers.teradata.hooks.ttu.TtuHook.__exit__")
    @mock.patch("airflow.providers.teradata.hooks.ttu.TtuHook.__enter__")
    def test_hook_context_manager(self, mock_enter, mock_exit):
        # Setup
        hook = TtuHook()
        mock_enter.return_value = hook

        # Execute
        with hook as h:
            assert h == hook

        # Assert
        mock_exit.assert_called_once()
        # Ensure the exit method was called with the correct parameters
        # Context manager's __exit__ is called with (exc_type, exc_val, exc_tb)
        args = mock_exit.call_args[0]
        assert len(args) == 3  # Verify we have the correct number of arguments
        assert args[0] is None  # type should be None
        assert args[1] is None  # value should be None
        assert args[2] is None  # traceback should be None
