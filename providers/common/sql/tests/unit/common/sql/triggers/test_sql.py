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
from asgiref.sync import sync_to_async

from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.common.sql.triggers.sql import SQLExecuteQueryTrigger, SqlSensorTrigger
from airflow.triggers.base import TriggerEvent

try:
    import importlib.util

    if not importlib.util.find_spec("airflow.sdk.bases.hook"):
        raise ImportError

    BASEHOOK_PATCH_PATH = "airflow.sdk.bases.hook.BaseHook"
except ImportError:
    BASEHOOK_PATCH_PATH = "airflow.hooks.base.BaseHook"
from tests_common.test_utils.operators.run_deferrable import run_trigger


@pytest.fixture
def mock_get_connection():
    with mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection") as get_connection:
        yield get_connection


@pytest.fixture
def db_hook(mock_get_connection):
    hook = mock.MagicMock(spec=DbApiHook)
    mock_get_connection.return_value.get_hook.return_value = hook
    return hook


class TestSQLExecuteQueryTrigger:
    @mock.patch(f"{BASEHOOK_PATCH_PATH}.get_connection")
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

    def test_run_passes_parameters(self, db_hook):
        db_hook.get_records.return_value = [(1,)]

        run_trigger(SQLExecuteQueryTrigger(sql="SELECT %(id)s", conn_id="test_conn_id", parameters={"id": 1}))

        db_hook.get_records.assert_called_once_with("SELECT %(id)s", {"id": 1})


def make_sensor_trigger(**kwargs) -> SqlSensorTrigger:
    return SqlSensorTrigger(
        sql="SELECT count(*) FROM orders WHERE ds = %(ds)s",
        conn_id="test_conn_id",
        poke_interval=30,
        parameters={"ds": "2026-01-01"},
        **kwargs,
    )


class TestSqlSensorTrigger:
    def test_serialize_round_trip(self):
        trigger = make_sensor_trigger(hook_params={"schema": "s"}, fail_on_empty=True)

        classpath, kwargs = trigger.serialize()

        assert classpath == "airflow.providers.common.sql.triggers.sql.SqlSensorTrigger"
        assert kwargs == {
            "sql": "SELECT count(*) FROM orders WHERE ds = %(ds)s",
            "conn_id": "test_conn_id",
            "hook_params": {"schema": "s"},
            "parameters": {"ds": "2026-01-01"},
            "poke_interval": 30,
            "fail_on_empty": True,
        }
        assert SqlSensorTrigger(**kwargs).serialize() == (classpath, kwargs)

    def test_polls_in_a_thread_until_first_cell_is_truthy(self, mock_get_connection, db_hook):
        db_hook.get_records.side_effect = [[], [(0,)], [(None,)], [(7,), (8,)]]
        module = "airflow.providers.common.sql.triggers.sql"
        trigger = make_sensor_trigger()

        with (
            mock.patch(f"{module}.sync_to_async", wraps=sync_to_async) as mock_sync_to_async,
            mock.patch(f"{module}.asyncio.sleep") as sleep,
        ):
            events = run_trigger(trigger)

        assert [event.payload for event in events] == [{"status": "success"}]
        assert (
            db_hook.get_records.call_args_list
            == [mock.call("SELECT count(*) FROM orders WHERE ds = %(ds)s", {"ds": "2026-01-01"})] * 4
        )
        assert sleep.await_args_list == [mock.call(30)] * 4
        assert mock_get_connection.call_count == 4
        assert mock_sync_to_async.call_args_list == [mock.call(trigger._poll, thread_sensitive=False)] * 4

    @pytest.mark.parametrize(
        ("get_records", "message"),
        [
            pytest.param(
                {"side_effect": [[], [(1,)]]},
                "No rows returned, raising as per fail_on_empty flag",
                id="empty",
            ),
            pytest.param(
                {"side_effect": RuntimeError("connection refused")}, "connection refused", id="error"
            ),
        ],
    )
    def test_yields_failure(self, db_hook, get_records, message):
        db_hook.get_records.configure_mock(**get_records)

        with mock.patch("airflow.providers.common.sql.triggers.sql.asyncio.sleep"):
            events = run_trigger(make_sensor_trigger(fail_on_empty=True))

        assert [event.payload for event in events] == [{"status": "failure", "message": message}]
