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

import os
from unittest import mock

import pytest
from uuid6 import uuid7

from airflow.sdk.api.datamodels._generated import ConnectionTestConnectionResponse, ConnectionTestState
from airflow.sdk.exceptions import AirflowTaskTimeout
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
    def test_reports_state_from_hook_result(self, MockClient, hook_result, expected_final):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionTestConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
            host="httpbin.org",
            port=443,
        )
        test_id = uuid7()

        with mock.patch(
            "airflow.sdk.definitions.connection.Connection.test_connection",
            autospec=True,
            return_value=hook_result,
        ):
            _call(connection_test_id=test_id)

        calls = mock_client.connection_tests.update_state.call_args_list
        assert len(calls) == 1
        assert calls[0].args == (test_id, *expected_final)

    @pytest.mark.parametrize(
        ("exception", "msg_substring"),
        [
            (RuntimeError("Something broke"), "Connection test failed unexpectedly: RuntimeError"),
            (AirflowTaskTimeout("Connection test timed out"), "timed out"),
        ],
        ids=["generic_exception", "timeout"],
    )
    def test_reports_failed_on_hook_exception(self, MockClient, exception, msg_substring):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionTestConnectionResponse(
            conn_id="test_conn",
            conn_type="http",
        )

        with mock.patch(
            "airflow.sdk.definitions.connection.Connection.test_connection",
            autospec=True,
            side_effect=exception,
        ):
            _call()

        last = mock_client.connection_tests.update_state.call_args_list[-1]
        assert last.args[1] == ConnectionTestState.FAILED
        assert msg_substring in last.args[2]

    def test_connection_not_found_via_execution_api(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.side_effect = RuntimeError("not found")

        _call(connection_id="missing_conn")

        last = mock_client.connection_tests.update_state.call_args_list[-1]
        assert last.args[1] == ConnectionTestState.FAILED
        assert "Connection test failed unexpectedly" in last.args[2]

    def test_connection_fields_passed_correctly(self, MockClient):
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionTestConnectionResponse(
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
            "airflow.sdk.definitions.connection.Connection.test_connection",
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

    def test_hook_lookup_resolves_via_preset_connections(self, MockClient):
        from airflow.sdk.execution_time.context import _get_connection

        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionTestConnectionResponse(
            conn_id="never_in_secrets",
            conn_type="fs",
            extra='{"path": "/tmp"}',
        )
        observed: dict = {}

        def capture():
            observed["resolved"] = _get_connection("never_in_secrets")
            return True, "OK"

        with mock.patch(
            "airflow.sdk.definitions.connection.Connection.get_hook",
            autospec=True,
        ) as mock_get_hook:
            hook = mock.MagicMock()
            hook.test_connection.side_effect = capture
            mock_get_hook.return_value = hook
            _call(connection_id="never_in_secrets")

        assert observed["resolved"].conn_id == "never_in_secrets"
        assert observed["resolved"].extra == '{"path": "/tmp"}'
        assert (
            mock_client.connection_tests.update_state.call_args_list[-1].args[1]
            == ConnectionTestState.SUCCESS
        )

    def test_preset_contextvar_is_reset_on_exception(self, MockClient):
        from airflow.sdk.execution_time.context import _preset_connections

        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionTestConnectionResponse(
            conn_id="isolated_conn",
            conn_type="fs",
            extra='{"path": "/tmp"}',
        )

        before = _preset_connections.get()

        with mock.patch(
            "airflow.sdk.definitions.connection.Connection.get_hook",
            autospec=True,
            side_effect=RuntimeError("boom"),
        ):
            _call(connection_id="isolated_conn")

        assert _preset_connections.get() == before, "preset must be cleared after exception"

    def test_env_var_set_during_test_and_cleaned_up(self, MockClient):
        """AIRFLOW_CONN_<ID> is exposed during the test (for env/secrets-backend hooks) and removed after."""
        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionTestConnectionResponse(
            conn_id="env_resolved_conn",
            conn_type="http",
            host="example.com",
        )
        env_key = "AIRFLOW_CONN_ENV_RESOLVED_CONN"
        ctx_key = "_AIRFLOW_PROCESS_CONTEXT"
        assert env_key not in os.environ
        old_ctx = os.environ.get(ctx_key)
        observed: dict = {}

        def capture(conn_self):
            observed["during_conn"] = os.environ.get(env_key)
            observed["during_ctx"] = os.environ.get(ctx_key)
            return True, "OK"

        with mock.patch(
            "airflow.sdk.definitions.connection.Connection.test_connection",
            autospec=True,
            side_effect=capture,
        ):
            _call(connection_id="env_resolved_conn")

        # During the hook call: both env vars are set so env/secrets-backend resolvers find
        # the preset and Connection.from_uri picks up the SDK Connection class.
        assert observed["during_conn"] is not None
        assert observed["during_conn"].startswith("http://")
        assert observed["during_ctx"] == "client"
        # After: both env vars are restored to their prior values (here: both unset).
        assert env_key not in os.environ
        assert os.environ.get(ctx_key) == old_ctx

    def test_preset_does_not_leak_for_other_conn_ids(self, MockClient):
        from airflow.sdk.execution_time.context import _get_connection

        mock_client = MockClient.return_value
        mock_client.connection_tests.get_connection.return_value = ConnectionTestConnectionResponse(
            conn_id="target_conn",
            conn_type="fs",
            extra='{"path": "/tmp"}',
        )
        observed: dict = {}

        def capture():
            try:
                observed["unrelated"] = _get_connection("some_unrelated_id")
            except Exception as e:
                observed["unrelated_error"] = type(e).__name__
            return True, "OK"

        with (
            mock.patch(
                "airflow.sdk.definitions.connection.Connection.get_hook",
                autospec=True,
            ) as mock_get_hook,
            mock.patch(
                "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded",
                return_value=[],
            ),
        ):
            hook = mock.MagicMock()
            hook.test_connection.side_effect = capture
            mock_get_hook.return_value = hook
            _call(connection_id="target_conn")

        assert "unrelated" not in observed
        assert observed.get("unrelated_error") == "AirflowNotFoundException"
