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

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.sftp.exceptions import ConnectionNotOpenedException
from airflow.providers.sftp.hooks.sftp import SFTPHook, handle_connection_management


class TestConnectionNotOpenedException:
    """Tests for ConnectionNotOpenedException."""

    def test_exception_is_airflow_exception(self):
        """ConnectionNotOpenedException should derive from AirflowException."""
        assert issubclass(ConnectionNotOpenedException, AirflowException)

    def test_exception_can_be_caught_as_airflow_exception(self):
        """Callers catching AirflowException should also catch ConnectionNotOpenedException."""
        with pytest.raises(AirflowException):
            raise ConnectionNotOpenedException("test message")

    def test_exception_message(self):
        """Exception should preserve the message."""
        msg = "Connection not open"
        exc = ConnectionNotOpenedException(msg)
        assert str(exc) == msg


class StubSFTPHook(SFTPHook):
    """A stub hook for testing the handle_connection_management decorator."""

    def __init__(self, use_managed_conn=False, conn=None):
        self.use_managed_conn = use_managed_conn
        self.conn = conn
        self._stub_conn = mock.MagicMock()

    @handle_connection_management
    def stub_method(self):
        return self.conn

    @handle_connection_management
    def stub_method_with_args(self, arg1, arg2=None):
        return (arg1, arg2)


class TestHandleConnectionManagement:
    """Tests for the handle_connection_management decorator."""

    def test_raises_when_no_connection_and_not_managed(self):
        """Should raise ConnectionNotOpenedException when conn is None and use_managed_conn is False."""
        hook = StubSFTPHook(use_managed_conn=False, conn=None)
        with pytest.raises(ConnectionNotOpenedException) as exc_info:
            hook.stub_method()
        assert "get_managed_conn()" in str(exc_info.value)

    def test_delegates_when_connection_open(self):
        """Should delegate to the wrapped function when conn is open."""
        conn = mock.MagicMock()
        hook = StubSFTPHook(use_managed_conn=False, conn=conn)
        result = hook.stub_method()
        assert result is conn

    def test_delegates_with_args(self):
        """Should pass through arguments correctly."""
        conn = mock.MagicMock()
        hook = StubSFTPHook(use_managed_conn=False, conn=conn)
        result = hook.stub_method_with_args("arg1", arg2="arg2")
        assert result == ("arg1", "arg2")

    def test_uses_managed_conn_when_enabled(self):
        """Should use get_managed_conn context manager when use_managed_conn is True."""
        hook = StubSFTPHook(use_managed_conn=True, conn=None)
        mock_conn = mock.MagicMock()
        hook._stub_conn = mock_conn

        with mock.patch.object(hook, "get_managed_conn") as mock_get_conn:
            mock_get_conn.return_value.__enter__ = mock.MagicMock(return_value=mock_conn)
            mock_get_conn.return_value.__exit__ = mock.MagicMock(return_value=False)
            result = hook.stub_method()
            mock_get_conn.assert_called_once()