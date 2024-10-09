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

from dev.tests_common.test_utils.compat import AIRFLOW_V_2_8_PLUS

pytestmark = [
    pytest.mark.skipif(not AIRFLOW_V_2_8_PLUS, reason="Tests for Airflow 2.8.0+ only"),
]


if TYPE_CHECKING:
    from airflow.hooks.base import BaseHook


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

    class MockedHook(hook_class):  # type: ignore[misc, valid-type]
        conn_name_attr = "test_conn_id"
        connection_invocations = 0

        @classmethod
        def get_connection(cls, conn_id: str):
            cls.connection_invocations += 1
            return connection

        def get_conn(self):
            return conn

    return MockedHook(**hook_params)
