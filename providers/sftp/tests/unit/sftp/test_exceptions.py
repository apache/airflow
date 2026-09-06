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

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.sftp.exceptions import ConnectionNotOpenedException
from airflow.providers.sftp.hooks.sftp import handle_connection_management


class FakeSFTPHook:
    """
    Minimal stand-in for :class:`~airflow.providers.sftp.hooks.sftp.SFTPHook`.

    ``handle_connection_management`` only reads ``use_managed_conn`` and ``conn`` and calls
    ``get_managed_conn``, so the decorator can be exercised without reaching an SFTP server.
    """

    def __init__(self, *, use_managed_conn: bool, conn: Any = None) -> None:
        self.use_managed_conn = use_managed_conn
        self.conn = conn
        self.managed_conn = "managed-conn"
        self.managed_conn_opened = 0
        self.managed_conn_released = 0
        self.conn_seen_by_wrapped: Any = None

    @contextmanager
    def get_managed_conn(self) -> Generator[str, None, None]:
        self.managed_conn_opened += 1
        try:
            yield self.managed_conn
        finally:
            self.managed_conn_released += 1

    @handle_connection_management
    def do_work(self, path: str, recursive: bool = False) -> str:
        self.conn_seen_by_wrapped = self.conn
        return f"{path}:{recursive}"

    @handle_connection_management
    def do_failing_work(self) -> None:
        raise RuntimeError("wrapped call failed")


class TestConnectionNotOpenedException:
    def test_subclasses_airflow_exception(self) -> None:
        # Callers catch the base class, so narrowing this later would be a breaking change.
        assert issubclass(ConnectionNotOpenedException, AirflowException)

    def test_keeps_its_message(self) -> None:
        assert str(ConnectionNotOpenedException("connection is not open")) == "connection is not open"

    def test_can_be_caught_as_airflow_exception(self) -> None:
        with pytest.raises(AirflowException, match="connection is not open"):
            raise ConnectionNotOpenedException("connection is not open")


class TestHandleConnectionManagement:
    def test_raises_when_unmanaged_and_no_connection_is_open(self) -> None:
        hook = FakeSFTPHook(use_managed_conn=False)

        with pytest.raises(ConnectionNotOpenedException, match=r"use with hook\.get_managed_conn\(\)"):
            hook.do_work("/tmp")

        assert hook.managed_conn_opened == 0

    def test_delegates_when_unmanaged_and_a_connection_is_already_open(self) -> None:
        hook = FakeSFTPHook(use_managed_conn=False, conn="already-open")

        assert hook.do_work("/tmp", recursive=True) == "/tmp:True"
        assert hook.conn_seen_by_wrapped == "already-open"
        assert hook.managed_conn_opened == 0

    def test_opens_a_managed_connection_instead_of_raising(self) -> None:
        hook = FakeSFTPHook(use_managed_conn=True)

        assert hook.do_work("/tmp") == "/tmp:False"
        assert hook.managed_conn_opened == 1
        assert hook.managed_conn_released == 1

    def test_managed_connection_is_set_on_the_hook_for_the_call(self) -> None:
        hook = FakeSFTPHook(use_managed_conn=True)

        hook.do_work("/tmp")

        assert hook.conn_seen_by_wrapped == hook.managed_conn

    def test_managed_connection_is_released_when_the_wrapped_call_raises(self) -> None:
        hook = FakeSFTPHook(use_managed_conn=True)

        with pytest.raises(RuntimeError, match="wrapped call failed"):
            hook.do_failing_work()

        assert hook.managed_conn_released == 1
