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
from unittest.mock import AsyncMock

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.triggers.sql import SQLExecuteQueryTrigger
from airflow.providers.common.sql.version_compat import BaseHook
from airflow.triggers.base import TriggerEvent

from tests_common.test_utils.operators.run_deferrable import run_trigger


class TestSQLExecuteQueryTrigger:
    @classmethod
    def get_connection(cls, conn_id: str, records: list | None = None):
        mock_connection = mock.MagicMock(spec=Connection)
        mock_hook = mock.MagicMock(spec=DbApiHook)
        if records:
            mock_hook.get_records.side_effect = lambda sql: records
        mock_connection.get_hook.side_effect = lambda hook_params: mock_hook
        return mock_connection

    def test_run(self):
        data = [(1, "Alice"), (2, "Bob")]

        with (
            mock.patch.object(BaseHook, "get_connection", side_effect=lambda conn_id: self.get_connection(conn_id, data)),
            mock.patch.object(BaseHook, "aget_connection", side_effect=lambda: AsyncMock(side_effect=lambda conn_id: self.get_connection(conn_id, data))),
        ):
            trigger = SQLExecuteQueryTrigger(sql="SELECT * FROM users;", conn_id="test_conn_id")
            actual = run_trigger(trigger)

            assert len(actual) == 1
            assert isinstance(actual[0], TriggerEvent)
            assert actual[0].payload["status"] == "success"
            assert actual[0].payload["results"] == data

    @pytest.mark.asyncio
    async def test_get_hook(self):
        with (
            mock.patch.object(BaseHook, "get_connection", side_effect=self.get_connection),
            mock.patch.object(BaseHook, "aget_connection", side_effect=lambda: AsyncMock(side_effect=self.get_connection)),
        ):
            trigger = SQLExecuteQueryTrigger(sql="SELECT * FROM users;", conn_id="test_conn_id")
            actual = await trigger.get_hook()

            assert isinstance(actual, DbApiHook)
