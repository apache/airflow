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
from airflow.providers.common.sql.hooks.handlers import fetch_all_handler
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.triggers.sql import SQLExecuteQueryTrigger, SQLGenericTransferTrigger
from airflow.triggers.base import TriggerEvent

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"
from tests_common.test_utils.operators.run_deferrable import run_trigger


class TestSQLGenericTransferTrigger:
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_run(self, mock_get_connection):
        data = [(1, "Alice"), (2, "Bob")]
        mock_connection = mock.MagicMock(spec=Connection)
        mock_hook = mock.MagicMock(spec=DbApiHook)
        mock_hook.get_records.side_effect = lambda sql: data
        mock_get_connection.return_value = mock_connection
        mock_connection.get_hook.side_effect = lambda hook_params: mock_hook

        trigger = SQLGenericTransferTrigger(sql="SELECT * FROM users;", conn_id="test_conn_id")
        actual = run_trigger(trigger)

        assert len(actual) == 1
        assert isinstance(actual[0], TriggerEvent)
        assert actual[0].payload["status"] == "success"
        assert actual[0].payload["results"] == data


class TestSQLExecuteQueryTrigger:
    def _make_trigger(self, **kwargs):
        defaults = {
            "sql": "SELECT 1",
            "conn_id": "test_conn",
            "autocommit": False,
            "split_statements": False,
            "return_last": True,
        }
        defaults.update(kwargs)
        return SQLExecuteQueryTrigger(**defaults)

    def _make_mock_hook(self):
        mock_hook = mock.MagicMock(spec=DbApiHook)
        mock_hook.arun = mock.AsyncMock()
        return mock_hook

    def test_serialize(self):
        trigger = self._make_trigger(
            autocommit=True,
            parameters={"p": 1},
            fetch_results=True,
            split_statements=True,
            return_last=False,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow.providers.common.sql.triggers.sql.SQLExecuteQueryTrigger"
        assert kwargs == {
            "sql": "SELECT 1",
            "conn_id": "test_conn",
            "autocommit": True,
            "parameters": {"p": 1},
            "fetch_results": True,
            "split_statements": True,
            "return_last": False,
            "read_only": False,
        }

    def test_run_fetch_results_returns_rows_and_descriptions(self):
        rows = [("val1",), ("val2",)]
        descriptions = [(("col1", 23, None, None, None, None, None),)]
        mock_hook = self._make_mock_hook()
        mock_hook.arun.return_value = rows
        mock_hook.descriptions = descriptions
        trigger = self._make_trigger(fetch_results=True)
        with mock.patch.object(trigger, "aget_hook", new=mock.AsyncMock(return_value=mock_hook)):
            events = run_trigger(trigger)

        # The built-in fetch handler is used; no user handler runs in the triggerer.
        assert mock_hook.arun.await_args.kwargs["handler"] is fetch_all_handler
        assert len(events) == 1
        assert events[0].payload == {
            "status": "success",
            "results": rows,
            "descriptions": [[["col1", 23, None, None, None, None, None]]],
        }

    def test_run_without_fetch_results_returns_no_results(self):
        mock_hook = self._make_mock_hook()
        trigger = self._make_trigger(fetch_results=False)
        with mock.patch.object(trigger, "aget_hook", new=mock.AsyncMock(return_value=mock_hook)):
            events = run_trigger(trigger)

        assert mock_hook.arun.await_args.kwargs["handler"] is None
        assert len(events) == 1
        assert events[0].payload == {"status": "success"}

    def test_jsonsafe_descriptions_stringifies_non_native_type_codes(self):
        class _Type:
            def __str__(self):
                return "CUSTOM_TYPE"

        descriptions = [(("col", _Type(), None, None, None, None, None),), None]
        assert SQLExecuteQueryTrigger._jsonsafe_descriptions(descriptions) == [
            [["col", "CUSTOM_TYPE", None, None, None, None, None]],
            None,
        ]

    def test_run_yields_error_event_on_exception(self):
        mock_hook = self._make_mock_hook()
        mock_hook.arun.side_effect = Exception("DB error")
        trigger = self._make_trigger(fetch_results=True)
        with mock.patch.object(trigger, "aget_hook", new=mock.AsyncMock(return_value=mock_hook)):
            events = run_trigger(trigger)

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
        assert "DB error" in events[0].payload["message"]

    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
    def test_get_hook_raises_when_hook_is_not_dbapihook(self, mock_get_connection):
        mock_connection = mock.MagicMock(spec=Connection)
        mock_connection.get_hook.return_value = mock.MagicMock()  # not a DbApiHook
        mock_get_connection.return_value = mock_connection
        trigger = self._make_trigger()
        events = run_trigger(trigger)

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
