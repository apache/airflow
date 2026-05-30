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
        mock_hook.run_async = mock.AsyncMock()
        return mock_hook

    def test_serialize(self):
        trigger = self._make_trigger(
            autocommit=True,
            parameters={"p": 1},
            handler_path="some.module:handler",
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
            "handler_path": "some.module:handler",
            "split_statements": True,
            "return_last": False,
        }

    def test_run_success_with_handler(self):
        rows = [("val1",), ("val2",)]
        mock_hook = self._make_mock_hook()
        mock_hook.run_async.return_value = rows
        trigger = self._make_trigger(
            handler_path="airflow.providers.common.sql.hooks.handlers:fetch_all_handler"
        )
        with mock.patch.object(trigger, "get_hook", new=mock.AsyncMock(return_value=mock_hook)):
            with mock.patch.object(
                trigger, "_import_from_handler_path", new=mock.AsyncMock(return_value=fetch_all_handler)
            ):
                events = run_trigger(trigger)

        assert len(events) == 1
        assert events[0].payload == {"status": "success", "results": rows}

    def test_run_success_without_handler(self):
        mock_hook = self._make_mock_hook()
        trigger = self._make_trigger(handler_path=None)
        with mock.patch.object(trigger, "get_hook", new=mock.AsyncMock(return_value=mock_hook)):
            events = run_trigger(trigger)

        assert len(events) == 1
        assert events[0].payload == {"status": "success"}

    def test_run_yields_error_event_on_exception(self):
        mock_hook = self._make_mock_hook()
        mock_hook.run_async.side_effect = Exception("DB error")
        trigger = self._make_trigger()
        with mock.patch.object(trigger, "get_hook", new=mock.AsyncMock(return_value=mock_hook)):
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

    def test_run_yields_error_on_invalid_handler_path(self):
        mock_hook = self._make_mock_hook()
        trigger = self._make_trigger(handler_path="nonexistent.module:handler")
        with mock.patch.object(trigger, "get_hook", new=mock.AsyncMock(return_value=mock_hook)):
            events = run_trigger(trigger)

        assert len(events) == 1
        assert events[0].payload["status"] == "error"
