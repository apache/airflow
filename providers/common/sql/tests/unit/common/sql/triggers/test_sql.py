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

from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.triggers.sql import SQLExecuteQueryTrigger
from airflow.triggers.base import TriggerEvent

from tests_common.test_utils.operators.run_deferrable import run_trigger


class TestSQLExecuteQueryTrigger:
    @mock.patch("airflow.hooks.base.BaseHook.get_connection")
    def test_run(self, mock_get_connection):
        data = [(1, "Alice"), (2, "Bob")]
        mock_connection = mock.MagicMock(spec=Connection)
        mock_hook = mock.MagicMock(spec=DbApiHook)
        mock_hook.get_records.side_effect = lambda sql: data
        mock_get_connection.return_value = mock_connection
        mock_connection.get_hook.side_effect = lambda hook_params: mock_hook

        trigger = SQLExecuteQueryTrigger(sql="SELECT * FROM users;", conn_id="test_conn_id")
        actual = run_trigger(trigger)

        assert len(actual) == 1
        assert isinstance(actual[0], TriggerEvent)
        assert actual[0].payload["status"] == "success"
        assert actual[0].payload["results"] == data
