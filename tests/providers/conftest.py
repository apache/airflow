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

from typing import TYPE_CHECKING
from unittest import mock

import pytest

from airflow.models import Connection

if TYPE_CHECKING:
    from airflow.hooks.base import BaseHook


@pytest.fixture
def hook_conn(request):
    """
    Patch ``BaseHook.get_connection()`` by mock value.

    This fixture optionally parametrized, if ``param`` not set or empty it just mock method.
    If param is dictionary or :class:`~airflow.models.Connection` than return it,
    If param is exception than add side effect.
    Otherwise, it raises an error
    """
    try:
        conn = request.param
    except AttributeError:
        conn = None

    with mock.patch("airflow.hooks.base.BaseHook.get_connection") as m:
        if not conn:
            pass  # Don't do anything if param not specified or empty
        elif isinstance(conn, dict):
            m.return_value = Connection(**conn)
        elif not isinstance(conn, Connection):
            m.return_value = conn
        elif isinstance(conn, Exception):
            m.side_effect = conn
        else:
            raise TypeError(
                f"{request.node.name!r}: expected dict, Connection object or Exception, "
                f"but got {type(conn).__name__}"
            )

        yield m


def mock_hook(hook_class: type[BaseHook], hook_params=None, conn_params=None):
    hook_params = hook_params or {}
    conn_params = conn_params or {}
    connection = Connection(
        **{
            **dict(login="login", password="password", host="host", schema="schema", port=1234),
            **conn_params,
        }
    )

    cursor = mock.MagicMock(
        rowcount=0, spec=["description", "rowcount", "execute", "fetchall", "fetchone", "close"]
    )
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    class MockedHook(hook_class):
        conn_name_attr = "test_conn_id"

        @classmethod
        def get_connection(cls, conn_id: str):
            return connection

        def get_conn(self):
            return conn

    return MockedHook(**hook_params)
