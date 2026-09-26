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

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.sftp.exceptions import ConnectionNotOpenedException
from airflow.providers.sftp.hooks.sftp import handle_connection_management


class StubHook:
    """Minimal stand-in for ``SFTPHook`` that exercises only the decorator."""

    def __init__(self, *, use_managed_conn: bool, conn=None, managed_conn=None):
        self.use_managed_conn = use_managed_conn
        self.conn = conn
        self._managed_conn = managed_conn
        self.conn_seen_by_call: list[object] = []
        self.managed_conn_entries = 0

    @contextmanager
    def get_managed_conn(self):
        self.managed_conn_entries += 1
        yield self._managed_conn

    @handle_connection_management
    def do_work(self, value: int) -> int:
        self.conn_seen_by_call.append(self.conn)
        return value * 2


class TestConnectionNotOpenedException:
    def test_derives_from_airflow_exception(self):
        # Callers catch the base class, so narrowing this later would be a breaking change.
        assert issubclass(ConnectionNotOpenedException, AirflowException)


class TestHandleConnectionManagement:
    def test_unmanaged_without_open_connection_raises(self):
        hook = StubHook(use_managed_conn=False, conn=None)

        with pytest.raises(ConnectionNotOpenedException, match=r"hook\.get_managed_conn\(\)"):
            hook.do_work(21)

        assert hook.conn_seen_by_call == []
        assert hook.managed_conn_entries == 0

    def test_unmanaged_with_open_connection_delegates(self):
        conn = MagicMock()
        hook = StubHook(use_managed_conn=False, conn=conn)

        assert hook.do_work(21) == 42
        assert hook.conn_seen_by_call == [conn]
        assert hook.managed_conn_entries == 0

    def test_managed_opens_connection_instead_of_raising(self):
        managed_conn = MagicMock()
        hook = StubHook(use_managed_conn=True, conn=None, managed_conn=managed_conn)

        assert hook.do_work(21) == 42
        assert hook.managed_conn_entries == 1
        # The managed connection is set on the hook for the duration of the call.
        assert hook.conn_seen_by_call == [managed_conn]
