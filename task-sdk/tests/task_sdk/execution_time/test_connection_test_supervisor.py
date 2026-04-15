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
"""Tests for the connection-test supervisor module."""

from __future__ import annotations

from unittest import mock

import pytest
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import ConnectionResponse, ConnectionTestState
from airflow.sdk.execution_time.connection_test_supervisor import supervise_connection_test

SERVER = "http://localhost:8080/execution/"


def _call(**overrides):
    kwargs = {
        "connection_test_id": uuid7(),
        "connection_id": "test_conn",
        "timeout": 60,
        "token": "test-token",
        "server": SERVER,
    }
    kwargs.update(overrides)
    return supervise_connection_test(**kwargs)


@mock.patch("airflow.sdk.execution_time.connection_test_supervisor.signal", autospec=True)
@mock.patch("airflow.sdk.execution_time.connection_test_supervisor.Client", autospec=True)
class TestSuperviseConnectionTest:
    @pytest.mark.parametrize(
        ("hook_result", "expected_final"),
        [
            ((True, "Connection OK"), (ConnectionTestState.SUCCESS, "Connection OK")),
            ((False, "Connection refused"), (ConnectionTestState.FAILED, "Connection refused")),
        ],
        ids=["success", "failure"],
    )
    def test_reports_state_from_hook_result(self, MockClient, mock_signal, hook_result, expected_final):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
            host="httpbin.org",
            port=443,
        )
        test_id = uuid7()

        with mock.patch(
            "airflow.models.connection.Connection.test_connection",
            autospec=True,
            return_value=hook_result,
        ):
            _call(connection_test_id=test_id)

        calls = mock_client.connection_tests.update_state.call_args_list
        assert len(calls) == 2
        assert calls[0].args == (test_id, ConnectionTestState.RUNNING)
        assert calls[1].args == (test_id, *expected_final)

    @pytest.mark.parametrize(
        ("exception", "msg_substring"),
        [
            (RuntimeError("Something broke"), "Connection test failed unexpectedly: RuntimeError"),
            (TimeoutError("Connection test timed out after 30s"), "timed out"),
        ],
        ids=["generic_exception", "timeout"],
    )
    def test_reports_failed_on_hook_exception(self, MockClient, mock_signal, exception, msg_substring):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
        )

        with mock.patch(
            "airflow.models.connection.Connection.test_connection",
            autospec=True,
            side_effect=exception,
        ):
            _call()

        last = mock_client.connection_tests.update_state.call_args_list[-1]
        assert last.args[1] == ConnectionTestState.FAILED
        assert msg_substring in last.args[2]

    def test_connection_not_found_via_execution_api(self, MockClient, mock_signal):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.side_effect = RuntimeError("not found")

        _call(connection_id="missing_conn")

        last = mock_client.connection_tests.update_state.call_args_list[-1]
        assert last.args[1] == ConnectionTestState.FAILED
        assert "Connection test failed unexpectedly" in last.args[2]

    def test_connection_fields_passed_correctly(self, MockClient, mock_signal):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionResponse(
            conn_id="full_conn",
            conn_type="postgres",
            host="db.example.com",
            login="admin",
            password="s3cret",
            schema="mydb",
            port=5432,
            extra='{"sslmode": "require"}',
        )

        with mock.patch(
            "airflow.models.connection.Connection.test_connection",
            autospec=True,
            return_value=(True, "OK"),
        ) as mock_test_connection:
            _call(connection_id="full_conn")

        captured = mock_test_connection.call_args.args[0]
        assert captured.conn_id == "full_conn"
        assert captured.conn_type == "postgres"
        assert captured.host == "db.example.com"
        assert captured.login == "admin"
        assert captured.password == "s3cret"
        assert captured.schema == "mydb"
        assert captured.port == 5432
        assert captured.extra == '{"sslmode": "require"}'

    def test_alarm_is_cancelled_in_finally(self, MockClient, mock_signal):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
        )

        with mock.patch(
            "airflow.models.connection.Connection.test_connection",
            autospec=True,
            return_value=(True, "OK"),
        ):
            _call(timeout=60)

        alarm_calls = mock_signal.alarm.call_args_list
        assert alarm_calls[0].args == (60,)
        assert alarm_calls[-1].args == (0,)

    def test_hook_lookup_resolves_via_preset_connections(self, MockClient, mock_signal):
        from airflow.sdk.execution_time.context import _get_connection

        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionResponse(
            conn_id="never_in_secrets",
            conn_type="fs",
            extra='{"path": "/tmp"}',
        )
        observed: dict = {}

        def capture(self):
            observed["resolved"] = _get_connection("never_in_secrets")
            return True, "OK"

        with mock.patch(
            "airflow.models.connection.Connection.test_connection",
            autospec=True,
            side_effect=capture,
        ):
            _call(connection_id="never_in_secrets")

        assert observed["resolved"].conn_id == "never_in_secrets"
        assert observed["resolved"].extra == '{"path": "/tmp"}'
        assert (
            mock_client.connection_tests.update_state.call_args_list[-1].args[1]
            == ConnectionTestState.SUCCESS
        )

    def test_preset_contextvar_is_reset_on_exception(self, MockClient, mock_signal):
        from airflow.sdk.execution_time.context import _preset_connections

        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionResponse(
            conn_id="isolated_conn",
            conn_type="fs",
            extra='{"path": "/tmp"}',
        )

        before = _preset_connections.get()

        with mock.patch(
            "airflow.models.connection.Connection.test_connection",
            autospec=True,
            side_effect=RuntimeError("boom"),
        ):
            _call(connection_id="isolated_conn")

        assert _preset_connections.get() == before, "preset must be cleared after exception"

    def test_preset_does_not_leak_for_other_conn_ids(self, MockClient, mock_signal):
        from airflow.sdk.execution_time.context import _get_connection

        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionResponse(
            conn_id="target_conn",
            conn_type="fs",
            extra='{"path": "/tmp"}',
        )
        observed: dict = {}

        def capture(self):
            try:
                observed["unrelated"] = _get_connection("some_unrelated_id")
            except Exception as e:
                observed["unrelated_error"] = type(e).__name__
            return True, "OK"

        with (
            mock.patch(
                "airflow.models.connection.Connection.test_connection",
                autospec=True,
                side_effect=capture,
            ),
            mock.patch(
                "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded",
                return_value=[],
            ),
        ):
            _call(connection_id="target_conn")

        assert "unrelated" not in observed
        assert observed.get("unrelated_error") == "AirflowNotFoundException"
