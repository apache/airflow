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

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.sftp.exceptions import ConnectionNotOpenedException
from airflow.providers.sftp.hooks.sftp import handle_connection_management


class _StubHook:
    """Minimal stand-in for SFTPHook that exercises the connection-management
    decorator without needing an SSH/SFTP server.

    Only the attributes the decorator reads are defined: ``use_managed_conn``,
    ``conn`` and ``get_managed_conn``. ``wrapped`` is decorated the same way the
    real hook methods are.
    """

    def __init__(self, *, use_managed_conn: bool, conn: object | None = None) -> None:
        self.use_managed_conn = use_managed_conn
        self.conn = conn
        self.managed_conn = MagicMock(name="managed_conn")
        self.managed_conn_entered = False

    @contextmanager
    def get_managed_conn(self):
        self.managed_conn_entered = True
        yield self.managed_conn

    @handle_connection_management
    def wrapped(self, *args, **kwargs):
        # Return the connection the decorator made current, plus the call args,
        # so tests can assert both the delegation and what the method saw.
        return self.conn, args, kwargs


class TestConnectionNotOpenedException:
    def test_is_airflow_exception(self):
        # Callers catch the base class; narrowing this later would be a breaking
        # change, so the inheritance is part of the contract.
        assert issubclass(ConnectionNotOpenedException, AirflowException)
        assert isinstance(ConnectionNotOpenedException("boom"), AirflowException)


class TestHandleConnectionManagement:
    def test_raises_when_unmanaged_and_no_open_connection(self):
        hook = _StubHook(use_managed_conn=False, conn=None)

        with pytest.raises(ConnectionNotOpenedException) as exc_info:
            hook.wrapped()

        # The message must point the caller at the supported entry point.
        assert "get_managed_conn()" in str(exc_info.value)
        assert hook.managed_conn_entered is False

    def test_delegates_when_unmanaged_but_connection_already_open(self):
        existing_conn = MagicMock(name="existing_conn")
        hook = _StubHook(use_managed_conn=False, conn=existing_conn)

        conn_seen, args, kwargs = hook.wrapped("a", key="b")

        # No managed connection is opened; the wrapped function runs and returns.
        assert hook.managed_conn_entered is False
        assert conn_seen is existing_conn
        assert args == ("a",)
        assert kwargs == {"key": "b"}

    def test_opens_managed_connection_when_managed(self):
        hook = _StubHook(use_managed_conn=True, conn=None)

        conn_seen, _, _ = hook.wrapped()

        # The decorator opens a managed connection and makes it current on the
        # hook for the duration of the wrapped call instead of raising.
        assert hook.managed_conn_entered is True
        assert conn_seen is hook.managed_conn
        assert hook.conn is hook.managed_conn
