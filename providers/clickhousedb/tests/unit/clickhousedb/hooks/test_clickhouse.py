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

import json
import re
from unittest.mock import ANY, MagicMock, patch

import pytest

from airflow.models import Connection
from airflow.providers.clickhousedb.hooks.clickhouse import (
    ClickHouseConnection,
    ClickHouseHook,
)

# ---------------------------------------------------------------------------
# Fixtures / shared connection definitions
# ---------------------------------------------------------------------------

BASE_CONN = Connection(
    conn_id="clickhouse_test",
    conn_type="clickhouse",
    host="clickhouse-host",
    port=8123,
    login="user",
    password="secret",
    schema="analytics",
)

BASE_CONN_EXTRA = Connection(
    conn_id="clickhouse_test_extra",
    conn_type="clickhouse",
    host="secure-host",
    port=8443,
    login="admin",
    password="topsecret",
    schema="prod",
    extra=json.dumps(
        {
            "secure": True,
            "verify": False,
            "connect_timeout": 30,
            "send_receive_timeout": 600,
            "compress": False,
            "client_name": "my-airflow",
        }
    ),
)

MINIMAL_CONN = Connection(
    conn_id="clickhouse_minimal",
    conn_type="clickhouse",
)

CONN_WITH_CLIENT_KWARGS = Connection(
    conn_id="clickhouse_client_kwargs",
    conn_type="clickhouse",
    host="ch-host",
    port=8123,
    login="user",
    password="pass",
    schema="db",
    extra=json.dumps(
        {
            "client_kwargs": {
                "http_proxy": "http://proxy:8080",
                "pool_mgr_params": {"num_pools": 4},
            }
        }
    ),
)

CONN_WITH_CLIENT_KWARGS_STR = Connection(
    conn_id="clickhouse_client_kwargs_str",
    conn_type="clickhouse",
    host="ch-host",
    port=8123,
    login="user",
    password="pass",
    schema="db",
    extra=json.dumps(
        {
            # client_kwargs stored as a JSON string (as the UI text field stores it)
            "client_kwargs": '{"http_proxy": "http://proxy:8080"}'
        }
    ),
)

CONN_WITH_SESSION_SETTINGS = Connection(
    conn_id="clickhouse_session",
    conn_type="clickhouse",
    host="ch-host",
    port=8123,
    login="user",
    password="pass",
    schema="db",
    extra=json.dumps(
        {
            "session_settings": {
                "max_execution_time": 120,
                "max_threads": 4,
            }
        }
    ),
)

CONN_WITH_SESSION_SETTINGS_STR = Connection(
    conn_id="clickhouse_session_str",
    conn_type="clickhouse",
    host="ch-host",
    port=8123,
    login="user",
    password="pass",
    schema="db",
    extra=json.dumps(
        {
            # session_settings stored as a JSON string (as the UI text field stores it)
            "session_settings": '{"max_execution_time": 60}'
        }
    ),
)


# ---------------------------------------------------------------------------
# Tests: get_conn
# ---------------------------------------------------------------------------


class TestClickHouseHookGetConn:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_get_conn_basic(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        conn = hook.get_conn()

        mock_get_client.assert_called_once_with(
            host="clickhouse-host",
            port=8123,
            username="user",
            password="secret",
            database="analytics",
            secure=False,
            verify=True,
            client_name=ANY,  # always set; format verified in TestClickHouseHookClientName
        )
        assert isinstance(conn, ClickHouseConnection)

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_get_conn_with_extra_params(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN_EXTRA
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test_extra")
        hook.get_conn()

        mock_get_client.assert_called_once_with(
            host="secure-host",
            port=8443,
            username="admin",
            password="topsecret",
            database="prod",
            secure=True,
            verify=False,
            connect_timeout=30,
            send_receive_timeout=600,
            compress=False,
            client_name=ANY,  # contains Airflow version + "(my-airflow)" comment
        )

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_get_conn_minimal_defaults(self, mock_get_client, mock_get_connection):
        """All connection fields absent → sensible defaults applied."""
        mock_get_connection.return_value = MINIMAL_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_minimal")
        hook.get_conn()

        mock_get_client.assert_called_once_with(
            host="localhost",
            port=8123,
            username="default",
            password="",
            database="default",
            secure=False,
            verify=True,
            client_name=ANY,  # always set; format verified in TestClickHouseHookClientName
        )

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_get_conn_database_overrides_connection_schema(self, mock_get_client, mock_get_connection):
        """database constructor arg must override the schema field from the connection."""
        mock_get_connection.return_value = BASE_CONN  # schema="analytics"
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test", database="overridden_db")
        hook.get_conn()

        call_kwargs = mock_get_client.call_args.kwargs
        assert call_kwargs["database"] == "overridden_db"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_get_conn_no_database_falls_back_to_connection_schema(self, mock_get_client, mock_get_connection):
        """Without a database constructor arg the connection schema is used."""
        mock_get_connection.return_value = BASE_CONN  # schema="analytics"
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.get_conn()

        call_kwargs = mock_get_client.call_args.kwargs
        assert call_kwargs["database"] == "analytics"


# ---------------------------------------------------------------------------
# Tests: get_uri
# ---------------------------------------------------------------------------


class TestClickHouseHookGetUri:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_with_password(self, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        uri = hook.get_uri()

        assert uri == "clickhousedb://user:secret@clickhouse-host:8123/analytics"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_without_password(self, mock_get_connection):
        conn = Connection(
            conn_id="clickhouse_nopass",
            conn_type="clickhouse",
            host="myhost",
            port=8123,
            login="reader",
            schema="logs",
        )
        mock_get_connection.return_value = conn
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_nopass")
        uri = hook.get_uri()

        assert uri == "clickhousedb://reader@myhost:8123/logs"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_password_url_encoded(self, mock_get_connection):
        """Special characters in the password must be percent-encoded."""
        conn = Connection(
            conn_id="clickhouse_special",
            conn_type="clickhouse",
            host="host",
            port=8123,
            login="user",
            password="p@ss/w0rd!",
            schema="db",
        )
        mock_get_connection.return_value = conn
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_special")
        uri = hook.get_uri()

        assert "@" not in uri.split("://")[1].split("@")[0]  # password part is encoded
        assert "p%40ss" in uri or "p%40" in uri  # @ is encoded

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_defaults(self, mock_get_connection):
        mock_get_connection.return_value = MINIMAL_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_minimal")
        uri = hook.get_uri()

        assert uri == "clickhousedb://default@localhost:8123/default"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_database_overrides_connection_schema(self, mock_get_connection):
        """database constructor arg must appear in the URI instead of the connection schema."""
        mock_get_connection.return_value = BASE_CONN  # schema="analytics"
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test", database="overridden_db")
        uri = hook.get_uri()

        assert uri.endswith("/overridden_db")
        assert "analytics" not in uri

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_secure_adds_query_param(self, mock_get_connection):
        """secure=True must produce clickhousedb://...?secure=true (not a separate scheme)."""
        conn = Connection(
            conn_id="ch_secure",
            conn_type="clickhouse",
            host="secure-host",
            port=8443,
            login="user",
            password="pass",
            schema="db",
            extra=json.dumps({"secure": True}),
        )
        mock_get_connection.return_value = conn
        uri = ClickHouseHook(clickhouse_conn_id="ch_secure").get_uri()
        assert uri == "clickhousedb://user:pass@secure-host:8443/db?secure=true"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_verify_false_adds_query_param(self, mock_get_connection):
        """verify=False must appear as ?verify=false in the URI."""
        conn = Connection(
            conn_id="ch_verify",
            conn_type="clickhouse",
            host="host",
            port=8443,
            login="user",
            password="pass",
            schema="db",
            extra=json.dumps({"secure": True, "verify": False}),
        )
        mock_get_connection.return_value = conn
        uri = ClickHouseHook(clickhouse_conn_id="ch_verify").get_uri()
        assert "secure=true" in uri
        assert "verify=false" in uri

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_tuning_params_forwarded(self, mock_get_connection):
        """connect_timeout, send_receive_timeout and compress must appear in the URI query string."""
        conn = Connection(
            conn_id="ch_tuning",
            conn_type="clickhouse",
            host="host",
            port=8123,
            login="user",
            schema="db",
            extra=json.dumps({"connect_timeout": 30, "send_receive_timeout": 600, "compress": False}),
        )
        mock_get_connection.return_value = conn
        uri = ClickHouseHook(clickhouse_conn_id="ch_tuning").get_uri()
        assert "connect_timeout=30" in uri
        assert "send_receive_timeout=600" in uri
        assert "compress=false" in uri

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_uri_no_params_when_defaults(self, mock_get_connection):
        """When no extra settings are present the URI must have no query string."""
        mock_get_connection.return_value = MINIMAL_CONN
        uri = ClickHouseHook(clickhouse_conn_id="clickhouse_minimal").get_uri()
        assert "?" not in uri


# ---------------------------------------------------------------------------
# Tests: hook metadata / class attributes
# ---------------------------------------------------------------------------


class TestClickHouseHookClassAttributes:
    def test_conn_name_attr(self):
        assert ClickHouseHook.conn_name_attr == "clickhouse_conn_id"

    def test_default_conn_name(self):
        assert ClickHouseHook.default_conn_name == "clickhouse_default"

    def test_conn_type(self):
        assert ClickHouseHook.conn_type == "clickhouse"

    def test_hook_name(self):
        assert ClickHouseHook.hook_name == "ClickHouse"

    def test_supports_autocommit(self):
        assert ClickHouseHook.supports_autocommit is True

    def test_test_connection_sql(self):
        assert ClickHouseHook._test_connection_sql == "SELECT 1"


# ---------------------------------------------------------------------------
# Tests: _get_client_kwargs
# ---------------------------------------------------------------------------


class TestClickHouseHookClientKwargs:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_optional_keys_not_forwarded_when_absent(self, mock_get_connection):
        """Tuning kwargs must not appear when not set in extra; client_name is always set."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        kwargs = hook._get_client_kwargs()

        for optional_key in ("connect_timeout", "send_receive_timeout", "compress"):
            assert optional_key not in kwargs
        # client_name is always injected (contains Airflow version info)
        assert "client_name" in kwargs

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_optional_keys_forwarded_when_present(self, mock_get_connection):
        """Optional kwargs must appear in params when set in extra."""
        mock_get_connection.return_value = BASE_CONN_EXTRA
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test_extra")
        kwargs = hook._get_client_kwargs()

        assert kwargs["connect_timeout"] == 30
        assert kwargs["send_receive_timeout"] == 600
        assert kwargs["compress"] is False
        # client_name is always the full built string; the extra value becomes a comment
        assert "apache-airflow/" in kwargs["client_name"]
        assert "(my-airflow)" in kwargs["client_name"]

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_kwargs_passthrough_forwarded(self, mock_get_connection):
        """Arbitrary client_kwargs must reach get_client() as-is."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(
            clickhouse_conn_id="clickhouse_test",
            client_kwargs={"http_proxy": "http://proxy:8080", "pool_mgr_params": {"num_pools": 4}},
        )
        kwargs = hook._get_client_kwargs()

        assert kwargs["http_proxy"] == "http://proxy:8080"
        assert kwargs["pool_mgr_params"] == {"num_pools": 4}

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_hook_managed_keys_in_client_kwargs_logged_at_debug(self, mock_get_connection):
        """Dropped hook-managed keys must be logged at DEBUG level."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(
            clickhouse_conn_id="clickhouse_test",
            client_kwargs={"host": "attacker-host", "http_proxy": "http://proxy:8080"},
        )
        with patch.object(hook.log, "debug") as mock_debug:
            hook._get_client_kwargs()

        logged_messages = [str(call) for call in mock_debug.call_args_list]
        assert any("host" in msg for msg in logged_messages)

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_kwargs_cannot_override_hook_managed_keys(self, mock_get_connection):
        """Hook-managed keys in client_kwargs must be silently dropped."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(
            clickhouse_conn_id="clickhouse_test",
            client_kwargs={
                "host": "attacker-host",
                "password": "stolen",
                "client_name": "override-attempt",
                "settings": {"malicious": True},
            },
        )
        kwargs = hook._get_client_kwargs()

        # Hook-managed values from the connection must win
        assert kwargs["host"] == "clickhouse-host"
        assert kwargs["password"] == "secret"
        # client_name is always the Airflow-versioned string
        assert "apache-airflow/" in kwargs["client_name"]
        assert "attacker" not in kwargs["client_name"]

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_kwargs_empty_by_default(self, mock_get_connection):
        """No client_kwargs constructor arg → no unexpected extra keys in output."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        kwargs = hook._get_client_kwargs()

        # Only expected keys should be present (no leakage from a default dict)
        expected_keys = {
            "host",
            "port",
            "username",
            "password",
            "database",
            "secure",
            "verify",
            "client_name",
        }
        assert expected_keys.issubset(kwargs.keys())

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_no_settings_key_when_session_settings_absent(self, mock_get_connection):
        """'settings' must NOT appear in kwargs when no session_settings anywhere."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        kwargs = hook._get_client_kwargs()

        assert "settings" not in kwargs

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_optional_keys_with_null_values_not_forwarded(self, mock_get_connection):
        """Optional kwargs set to null/None in extra must not be forwarded to the driver."""
        conn = Connection(
            conn_id="ch_null_extras",
            conn_type="clickhouse",
            host="host",
            extra=json.dumps({"send_receive_timeout": None, "connect_timeout": None, "compress": None}),
        )
        mock_get_connection.return_value = conn
        hook = ClickHouseHook(clickhouse_conn_id="ch_null_extras")
        kwargs = hook._get_client_kwargs()

        assert "send_receive_timeout" not in kwargs
        assert "connect_timeout" not in kwargs
        assert "compress" not in kwargs


# ---------------------------------------------------------------------------
# Tests: client_kwargs from connection extra
# ---------------------------------------------------------------------------


class TestClickHouseHookClientKwargsFromExtra:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_kwargs_from_extra_dict(self, mock_get_connection):
        """client_kwargs dict in extra is forwarded to get_client()."""
        mock_get_connection.return_value = CONN_WITH_CLIENT_KWARGS
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_client_kwargs")
        kwargs = hook._get_client_kwargs()

        assert kwargs["http_proxy"] == "http://proxy:8080"
        assert kwargs["pool_mgr_params"] == {"num_pools": 4}

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_kwargs_from_extra_string(self, mock_get_connection):
        """client_kwargs stored as a JSON string in extra is parsed correctly."""
        mock_get_connection.return_value = CONN_WITH_CLIENT_KWARGS_STR
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_client_kwargs_str")
        kwargs = hook._get_client_kwargs()

        assert kwargs["http_proxy"] == "http://proxy:8080"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_constructor_client_kwargs_override_extra(self, mock_get_connection):
        """Constructor client_kwargs take precedence over extra on conflicting keys."""
        mock_get_connection.return_value = CONN_WITH_CLIENT_KWARGS
        hook = ClickHouseHook(
            clickhouse_conn_id="clickhouse_client_kwargs",
            client_kwargs={"http_proxy": "http://override:9090", "verify_ssl": False},
        )
        kwargs = hook._get_client_kwargs()

        # constructor value wins on conflicting key
        assert kwargs["http_proxy"] == "http://override:9090"
        # extra value survives for non-conflicting key
        assert kwargs["pool_mgr_params"] == {"num_pools": 4}
        # constructor-only key is present
        assert kwargs["verify_ssl"] is False

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_extra_client_kwargs_cannot_override_hook_managed_keys(self, mock_get_connection):
        """Hook-managed keys in extra client_kwargs must be silently dropped."""
        conn = Connection(
            conn_id="ch_malicious_extra",
            conn_type="clickhouse",
            host="real-host",
            extra=json.dumps(
                {
                    "client_kwargs": {
                        "host": "attacker-host",
                        "password": "stolen",
                    }
                }
            ),
        )
        mock_get_connection.return_value = conn
        hook = ClickHouseHook(clickhouse_conn_id="ch_malicious_extra")
        kwargs = hook._get_client_kwargs()

        assert kwargs["host"] == "real-host"
        assert kwargs["password"] == ""

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_invalid_client_kwargs_string_raises(self, mock_get_connection):
        """Malformed JSON string in client_kwargs raises ValueError with a clear message."""
        conn = Connection(
            conn_id="ch_bad_client_kwargs",
            conn_type="clickhouse",
            host="host",
            extra=json.dumps({"client_kwargs": "not-valid-json"}),
        )
        mock_get_connection.return_value = conn
        hook = ClickHouseHook(clickhouse_conn_id="ch_bad_client_kwargs")

        with pytest.raises(ValueError, match="Invalid JSON in extra.client_kwargs"):
            hook._get_client_kwargs()

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_no_extra_client_kwargs_no_leakage(self, mock_get_connection):
        """Absent client_kwargs in extra must not add unexpected keys."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        kwargs = hook._get_client_kwargs()

        assert "http_proxy" not in kwargs
        assert "pool_mgr_params" not in kwargs


# ---------------------------------------------------------------------------
# Tests: session_settings
# ---------------------------------------------------------------------------


class TestClickHouseHookSessionSettings:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_session_settings_from_extra_dict(self, mock_get_connection):
        """session_settings dict in extra is forwarded as 'settings'."""
        mock_get_connection.return_value = CONN_WITH_SESSION_SETTINGS
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_session")
        kwargs = hook._get_client_kwargs()

        assert kwargs["settings"] == {"max_execution_time": 120, "max_threads": 4}

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_session_settings_from_extra_string(self, mock_get_connection):
        """session_settings stored as a JSON string in extra is parsed correctly."""
        mock_get_connection.return_value = CONN_WITH_SESSION_SETTINGS_STR
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_session_str")
        kwargs = hook._get_client_kwargs()

        assert kwargs["settings"] == {"max_execution_time": 60}

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_constructor_session_settings_override_extra(self, mock_get_connection):
        """Constructor session_settings take precedence over extra on conflicting keys."""
        mock_get_connection.return_value = CONN_WITH_SESSION_SETTINGS
        hook = ClickHouseHook(
            clickhouse_conn_id="clickhouse_session",
            session_settings={"max_execution_time": 999, "readonly": 1},
        )
        kwargs = hook._get_client_kwargs()

        # max_execution_time from constructor overrides the extra value (120 → 999)
        assert kwargs["settings"]["max_execution_time"] == 999
        # max_threads comes from extra unchanged
        assert kwargs["settings"]["max_threads"] == 4
        # readonly added by constructor
        assert kwargs["settings"]["readonly"] == 1

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_constructor_only_session_settings(self, mock_get_connection):
        """Constructor session_settings work when extra has none."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(
            clickhouse_conn_id="clickhouse_test",
            session_settings={"max_threads": 2},
        )
        kwargs = hook._get_client_kwargs()

        assert kwargs["settings"] == {"max_threads": 2}

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_session_settings_passed_to_get_client(self, mock_get_client, mock_get_connection):
        """settings dict is forwarded to clickhouse_connect.get_client()."""
        mock_get_connection.return_value = CONN_WITH_SESSION_SETTINGS
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_session")
        hook.get_conn()

        call_kwargs = mock_get_client.call_args.kwargs
        assert call_kwargs["settings"] == {"max_execution_time": 120, "max_threads": 4}

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_empty_constructor_session_settings_ignored(self, mock_get_connection):
        """Passing session_settings={} is equivalent to not passing it."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test", session_settings={})
        kwargs = hook._get_client_kwargs()

        assert "settings" not in kwargs

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_invalid_session_settings_string_raises(self, mock_get_connection):
        """Malformed JSON string in session_settings raises ValueError with a clear message."""
        conn = Connection(
            conn_id="clickhouse_bad_json",
            conn_type="clickhouse",
            host="host",
            extra=json.dumps({"session_settings": "not-valid-json"}),
        )
        mock_get_connection.return_value = conn
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_bad_json")

        with pytest.raises(ValueError, match="Invalid JSON in extra.session_settings"):
            hook._get_client_kwargs()


# ---------------------------------------------------------------------------
# Tests: ClickHouseConnection adapter
# ---------------------------------------------------------------------------


class TestClickHouseConnection:
    def test_cursor_returns_dbapi_cursor(self):
        """cursor() must return the clickhouse_connect.dbapi.Cursor, not a home-grown one."""
        from clickhouse_connect.dbapi.cursor import Cursor

        conn = ClickHouseConnection(MagicMock())
        assert isinstance(conn.cursor(), Cursor)

    def test_multiple_cursors_are_independent_instances(self):
        """Each cursor() call must return a fresh Cursor backed by the same client."""
        from clickhouse_connect.dbapi.cursor import Cursor

        mock_client = MagicMock()
        conn = ClickHouseConnection(mock_client)
        c1, c2 = conn.cursor(), conn.cursor()
        assert c1 is not c2
        assert isinstance(c1, Cursor)
        assert isinstance(c2, Cursor)

    def test_close_delegates_to_client(self):
        mock_client = MagicMock()
        conn = ClickHouseConnection(mock_client)
        conn.close()
        mock_client.close.assert_called_once()

    def test_commit_is_noop(self):
        ClickHouseConnection(MagicMock()).commit()  # must not raise

    def test_rollback_is_noop(self):
        ClickHouseConnection(MagicMock()).rollback()  # must not raise

    def test_autocommit_attribute_is_true(self):
        assert ClickHouseConnection(MagicMock()).autocommit is True


# ---------------------------------------------------------------------------
# Tests: autocommit / transaction management
# ---------------------------------------------------------------------------


class TestClickHouseHookAutocommit:
    def test_set_autocommit_is_noop(self):
        hook = ClickHouseHook.__new__(ClickHouseHook)
        mock_conn = MagicMock(spec=ClickHouseConnection)
        # Must not raise regardless of the autocommit value
        hook.set_autocommit(mock_conn, True)
        hook.set_autocommit(mock_conn, False)

    def test_get_autocommit_always_true(self):
        hook = ClickHouseHook.__new__(ClickHouseHook)
        mock_conn = MagicMock(spec=ClickHouseConnection)
        assert hook.get_autocommit(mock_conn) is True

    def test_run_does_not_call_commit(self):
        """DbApiHook.run() must not call conn.commit() when get_autocommit returns True."""
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_conn = MagicMock(spec=ClickHouseConnection)
        mock_conn.cursor.return_value = mock_cursor

        class _TestHook(ClickHouseHook):
            def get_conn(self):
                return mock_conn

        hook = _TestHook()
        hook.get_connection = MagicMock(return_value=BASE_CONN)
        hook.run("SELECT 1")

        mock_conn.commit.assert_not_called()


# ---------------------------------------------------------------------------
# Tests: DbApiHook integration (get_first, get_records, run, lineage, insert)
#
# Uses the "UnitTestHook" pattern: override get_conn() so we can inject a
# controlled mock cursor and verify DbApiHook's higher-level methods work
# correctly end-to-end through ClickHouseConnection.
# ---------------------------------------------------------------------------


class TestClickHouseHookDbApiMethods:
    """Tests for DbApiHook-level methods surfaced through ClickHouseHook."""

    def setup_method(self):
        self.mock_cursor = MagicMock()
        self.mock_cursor.rowcount = 2
        self.mock_cursor.description = None

        self.mock_conn = MagicMock(spec=ClickHouseConnection)
        self.mock_conn.cursor.return_value = self.mock_cursor
        # Replicate the autocommit attribute so DbApiHook skips conn.commit()
        self.mock_conn.autocommit = True

        mock_conn = self.mock_conn

        class _TestHook(ClickHouseHook):
            conn_name_attr = "clickhouse_conn_id"
            log = MagicMock()

            def get_conn(self):
                return mock_conn

        self.hook = _TestHook()
        self.hook.get_connection = MagicMock(return_value=BASE_CONN)

    # ------------------------------------------------------------------
    # get_first / get_records
    # ------------------------------------------------------------------

    def test_get_first_record(self):
        result = ("row1",)
        self.mock_cursor.description = [("col", None, None, None, None, None, None)]
        self.mock_cursor.fetchone.return_value = result

        assert self.hook.get_first("SELECT 1") == result
        self.mock_cursor.execute.assert_called_once_with("SELECT 1")
        self.mock_conn.close.assert_called_once_with()
        self.mock_cursor.close.assert_called_once_with()

    def test_get_records(self):
        results = [("row1",), ("row2",)]
        self.mock_cursor.description = [("col", None, None, None, None, None, None)]
        self.mock_cursor.fetchall.return_value = results

        assert self.hook.get_records("SELECT 1") == results
        self.mock_cursor.execute.assert_called_once_with("SELECT 1")
        self.mock_conn.close.assert_called_once_with()
        self.mock_cursor.close.assert_called_once_with()

    # ------------------------------------------------------------------
    # run
    # ------------------------------------------------------------------

    def test_run_executes_sql(self):
        self.hook.run("DROP TABLE IF EXISTS t")
        self.mock_cursor.execute.assert_called_once_with("DROP TABLE IF EXISTS t")

    def test_run_closes_conn_on_success(self):
        self.hook.run("SELECT 1")
        self.mock_conn.close.assert_called_once_with()

    def test_run_closes_conn_on_error(self):
        self.mock_cursor.execute.side_effect = RuntimeError("query failed")
        with pytest.raises(RuntimeError, match="query failed"):
            self.hook.run("BAD SQL")
        self.mock_conn.close.assert_called_once_with()

    def test_run_with_parameters(self):
        self.hook.run("INSERT INTO t VALUES (%s)", parameters=("val",))
        self.mock_cursor.execute.assert_called_once_with("INSERT INTO t VALUES (%s)", ("val",))

    # ------------------------------------------------------------------
    # _generate_insert_sql
    # ------------------------------------------------------------------

    def test_generate_insert_sql(self):
        sql = self.hook._generate_insert_sql(
            table="events",
            values=("2024-01-01", "click"),
            target_fields=["ts", "action"],
            replace=False,
        )
        assert sql == "INSERT INTO events (ts, action) VALUES (%s,%s)"

    def test_generate_insert_sql_no_target_fields(self):
        sql = self.hook._generate_insert_sql(
            table="events",
            values=("a", "b", "c"),
            target_fields=None,
            replace=False,
        )
        assert sql == "INSERT INTO events  VALUES (%s,%s,%s)"

    # ------------------------------------------------------------------
    # Lineage — run
    # ------------------------------------------------------------------

    @patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    def test_run_hook_lineage(self, mock_send_lineage):
        self.hook.run("SELECT 1")

        mock_send_lineage.assert_called_once()
        kwargs = mock_send_lineage.call_args.kwargs
        assert kwargs["context"] is self.hook
        assert kwargs["sql"] == "SELECT 1"
        assert kwargs["sql_parameters"] is None
        assert kwargs["cur"] is self.mock_cursor

    @patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    def test_run_hook_lineage_with_params(self, mock_send_lineage):
        self.hook.run("INSERT INTO t VALUES (%s)", parameters=("x",))

        kwargs = mock_send_lineage.call_args.kwargs
        assert kwargs["sql_parameters"] == ("x",)

    # ------------------------------------------------------------------
    # Lineage — insert_rows
    # ------------------------------------------------------------------

    @patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    def test_insert_rows_hook_lineage(self, mock_send_lineage):
        rows = [("hello",), ("world",)]
        self.hook.insert_rows("my_table", rows)

        mock_send_lineage.assert_called()
        kwargs = mock_send_lineage.call_args.kwargs
        assert kwargs["context"] is self.hook
        assert "my_table" in kwargs["sql"]
        assert kwargs["row_count"] == 2

    # ------------------------------------------------------------------
    # Lineage — get_df
    # ------------------------------------------------------------------

    @patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    @patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df")
    def test_get_df_hook_lineage(self, mock_get_pandas_df, mock_send_lineage):
        self.hook.get_df("SELECT 1", df_type="pandas")

        mock_send_lineage.assert_called_once()
        kwargs = mock_send_lineage.call_args.kwargs
        assert kwargs["context"] is self.hook
        assert kwargs["sql"] == "SELECT 1"

    @patch("airflow.providers.common.sql.hooks.sql.send_sql_hook_lineage")
    @patch("airflow.providers.common.sql.hooks.sql.DbApiHook._get_pandas_df_by_chunks")
    def test_get_df_by_chunks_hook_lineage(self, mock_get_pandas_df_by_chunks, mock_send_lineage):
        params = ("x",)
        self.hook.get_df_by_chunks("SELECT 1", parameters=params, chunksize=1)

        mock_send_lineage.assert_called_once()
        kwargs = mock_send_lineage.call_args.kwargs
        assert kwargs["context"] is self.hook
        assert kwargs["sql"] == "SELECT 1"
        assert kwargs["sql_parameters"] == params

    # ------------------------------------------------------------------
    # get_df (pandas / polars) — shape and column names
    # ------------------------------------------------------------------

    def test_get_df_pandas(self):
        pandas = pytest.importorskip("pandas")
        column = "user_id"
        rows = [(1,), (2,), (3,)]
        self.mock_cursor.description = [(column, None, None, None, None, None, None)]
        self.mock_cursor.fetchall.return_value = rows

        df = self.hook.get_df("SELECT user_id FROM t", df_type="pandas")

        assert isinstance(df, pandas.DataFrame)
        assert list(df.columns) == [column]
        assert df.values.tolist() == [[r[0]] for r in rows]

    def test_get_df_polars(self):
        polars = pytest.importorskip("polars")
        column = "event"
        rows = [("click",), ("view",)]
        mock_exec_result = MagicMock()
        mock_exec_result.description = [(column, None, None, None, None, None, None)]
        mock_exec_result.fetchall.return_value = rows
        self.mock_cursor.execute.return_value = mock_exec_result

        df = self.hook.get_df("SELECT event FROM t", df_type="polars")

        assert isinstance(df, polars.DataFrame)
        assert df.columns == [column]
        assert df.to_series(0).to_list() == [r[0] for r in rows]

    # ------------------------------------------------------------------
    # get_first / get_records — empty-result edge cases
    # ------------------------------------------------------------------

    def test_get_first_returns_none_for_empty_result(self):
        self.mock_cursor.description = [("col", None, None, None, None, None, None)]
        self.mock_cursor.fetchone.return_value = None
        assert self.hook.get_first("SELECT 1 WHERE 1=0") is None

    def test_get_records_returns_empty_list(self):
        self.mock_cursor.description = [("col", None, None, None, None, None, None)]
        self.mock_cursor.fetchall.return_value = []
        assert self.hook.get_records("SELECT 1 WHERE 1=0") == []

    # ------------------------------------------------------------------
    # insert_rows — SQL correctness
    # ------------------------------------------------------------------

    def test_insert_rows_executes_correct_sql(self):
        rows = [("val1",), ("val2",)]
        self.hook.insert_rows("my_table", rows, target_fields=["col1"])

        calls = self.mock_cursor.execute.call_args_list
        assert len(calls) == 2
        for call in calls:
            sql = call[0][0]
            assert "my_table" in sql
            assert "col1" in sql

    def test_insert_rows_executemany_executes_once(self):
        rows = [("a",), ("b",), ("c",)]
        self.hook.insert_rows("events", rows, target_fields=["name"], executemany=True)

        self.mock_cursor.executemany.assert_called_once()
        sql, values = self.mock_cursor.executemany.call_args[0]
        assert "events" in sql
        assert "name" in sql
        assert len(values) == 3


# ---------------------------------------------------------------------------
# Tests: client_name / User-Agent identity
# ---------------------------------------------------------------------------


class TestClickHouseHookClientName:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_name_always_present(self, mock_get_connection):
        """client_name must be set even when not configured in extra."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        assert "client_name" in hook._get_client_kwargs()

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_name_contains_airflow_token(self, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        client_name = hook._get_client_kwargs()["client_name"]
        assert "apache-airflow/" in client_name

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_name_contains_provider_token(self, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        client_name = hook._get_client_kwargs()["client_name"]
        assert "apache-airflow-providers-clickhousedb/" in client_name

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_client_name_version_format(self, mock_get_connection):
        """Both version tokens must follow the X.Y(.Z) pattern."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        client_name = hook._get_client_kwargs()["client_name"]
        assert re.search(r"apache-airflow/\d+\.\d+", client_name)
        assert re.search(r"apache-airflow-providers-clickhousedb/\d+\.\d+", client_name)

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_custom_label_appended_as_comment(self, mock_get_connection):
        """A client_name value in extra must appear as '(<label>)' after the version tokens."""
        conn = Connection(
            conn_id="ch_custom",
            conn_type="clickhouse",
            host="host",
            extra=json.dumps({"client_name": "my-etl-pipeline"}),
        )
        mock_get_connection.return_value = conn
        hook = ClickHouseHook(clickhouse_conn_id="ch_custom")
        client_name = hook._get_client_kwargs()["client_name"]
        assert "apache-airflow/" in client_name
        assert "(my-etl-pipeline)" in client_name

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_no_custom_label_no_parentheses(self, mock_get_connection):
        """Without a user label the client_name must not end with empty parentheses."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        client_name = hook._get_client_kwargs()["client_name"]
        assert "()" not in client_name

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_client_name_forwarded_to_get_client(self, mock_get_client, mock_get_connection):
        """The built client_name must reach clickhouse_connect.get_client()."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.get_conn()
        call_kwargs = mock_get_client.call_args.kwargs
        assert "client_name" in call_kwargs
        assert "apache-airflow/" in call_kwargs["client_name"]

    @patch("airflow.providers.clickhousedb.hooks.clickhouse._build_client_name")
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_get_client_kwargs_delegates_to_build_client_name(self, mock_get_connection, mock_build):
        """_get_client_kwargs must call _build_client_name with the extra label (or None)."""
        conn = Connection(
            conn_id="ch_test",
            conn_type="clickhouse",
            host="host",
            extra=json.dumps({"client_name": "my-app"}),
        )
        mock_get_connection.return_value = conn
        mock_build.return_value = "sentinel-name"
        hook = ClickHouseHook(clickhouse_conn_id="ch_test")
        kwargs = hook._get_client_kwargs()
        mock_build.assert_called_once_with("my-app")
        assert kwargs["client_name"] == "sentinel-name"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse._build_client_name")
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    def test_build_client_name_receives_none_when_no_label(self, mock_get_connection, mock_build):
        """When extra has no client_name, _build_client_name must receive None."""
        mock_get_connection.return_value = BASE_CONN
        mock_build.return_value = "sentinel-name"
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook._get_client_kwargs()
        mock_build.assert_called_once_with(None)


# ---------------------------------------------------------------------------
# Tests: test_connection()
# ---------------------------------------------------------------------------


class TestClickHouseHookTestConnection:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_test_connection_success(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.column_names = ["1"]
        mock_result.result_set = [(1,)]
        mock_client.query.return_value = mock_result
        mock_get_client.return_value = mock_client

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        status, message = hook.test_connection()

        assert status is True
        assert message == "Connection successfully tested"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_test_connection_failure(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        mock_get_client.side_effect = Exception("Connection refused")

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        status, message = hook.test_connection()

        assert status is False
        assert "Connection refused" in message

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_test_connection_uses_select_1(self, mock_get_client, mock_get_connection):
        """Verifies that _test_connection_sql is actually executed."""
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_result = MagicMock()
        mock_result.column_names = ["1"]
        mock_result.result_set = [(1,)]
        mock_client.query.return_value = mock_result
        mock_get_client.return_value = mock_client

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.test_connection()

        # Assert on the statement and the bound parameters only. Anything the
        # driver appends beyond those (e.g. a settings kwarg) is its own concern
        # and varies between releases.
        mock_client.query.assert_called_once()
        sql, parameters = mock_client.query.call_args.args[:2]
        assert sql == "SELECT 1"
        assert not parameters


# ---------------------------------------------------------------------------
# Tests: bulk_dump / bulk_load (not implemented in ClickHouse)
# ---------------------------------------------------------------------------


class TestClickHouseHookBulkOperations:
    def test_bulk_dump_raises_not_implemented(self):
        hook = ClickHouseHook.__new__(ClickHouseHook)
        with pytest.raises(NotImplementedError):
            hook.bulk_dump("my_table", "/tmp/dump.tsv")

    def test_bulk_load_raises_not_implemented(self):
        hook = ClickHouseHook.__new__(ClickHouseHook)
        with pytest.raises(NotImplementedError):
            hook.bulk_load("my_table", "/tmp/dump.tsv")


# ---------------------------------------------------------------------------
# Tests: get_client raw accessor
# ---------------------------------------------------------------------------


class TestClickHouseHookGetClient:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_get_client_returns_raw_client(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        result = hook.get_client()

        assert result is mock_client

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_get_client_passes_correct_kwargs(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.get_client()

        call_kwargs = mock_get_client.call_args.kwargs
        assert call_kwargs["host"] == "clickhouse-host"
        assert call_kwargs["port"] == 8123
        assert call_kwargs["username"] == "user"
        assert call_kwargs["password"] == "secret"
        assert call_kwargs["database"] == "analytics"

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_get_client_and_get_conn_use_same_kwargs(self, mock_get_client, mock_get_connection):
        """get_client() and get_conn() should build kwargs from the same source."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")

        hook.get_conn()
        kwargs_from_conn = mock_get_client.call_args.kwargs

        hook.get_client()
        kwargs_from_client = mock_get_client.call_args.kwargs

        assert kwargs_from_conn == kwargs_from_client


# ---------------------------------------------------------------------------
# Tests: bulk_insert_rows
# ---------------------------------------------------------------------------


class TestClickHouseHookBulkInsert:
    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_empty_rows_skips_insert(self, mock_get_client, mock_get_connection):
        """No insert call should be made when rows is empty."""
        mock_get_connection.return_value = BASE_CONN
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.bulk_insert_rows("t", [], column_names=["id"])
        mock_get_client.assert_not_called()

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_single_batch_insert(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        rows = [(1, "a"), (2, "b")]
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.bulk_insert_rows("events", rows, column_names=["id", "name"])

        mock_client.insert.assert_called_once()

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_multiple_batches(self, mock_get_client, mock_get_connection):
        """10 rows with batch_size=3 → context created once, 4 insert calls."""
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        rows = [(i,) for i in range(10)]
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.bulk_insert_rows("t", rows, column_names=["id"], batch_size=3)

        mock_client.create_insert_context.assert_called_once_with("t", column_names=["id"])
        assert mock_client.insert.call_count == 4

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_batch_size_larger_than_rows(self, mock_get_client, mock_get_connection):
        """batch_size > len(rows) → context created once, exactly one insert call."""
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        rows = [(i,) for i in range(5)]
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.bulk_insert_rows("t", rows, column_names=["id"], batch_size=10000)

        mock_client.create_insert_context.assert_called_once_with("t", column_names=["id"])
        mock_client.insert.assert_called_once()

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_exactly_batch_size_rows(self, mock_get_client, mock_get_connection):
        """Rows == batch_size → context created once, exactly one insert call."""
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        rows = [(i,) for i in range(5)]
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.bulk_insert_rows("t", rows, column_names=["id"], batch_size=5)

        mock_client.create_insert_context.assert_called_once_with("t", column_names=["id"])
        mock_client.insert.assert_called_once()

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_client_closed_after_success(self, mock_get_client, mock_get_connection):
        """Client must be closed after successful insert."""
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.bulk_insert_rows("t", [(1,)], column_names=["id"])

        mock_client.close.assert_called_once()

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_client_closed_on_insert_error(self, mock_get_client, mock_get_connection):
        """Client must be closed even when insert raises."""
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_client.insert.side_effect = RuntimeError("insert failed")
        mock_get_client.return_value = mock_client

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        with pytest.raises(RuntimeError, match="insert failed"):
            hook.bulk_insert_rows("t", [(1,)], column_names=["id"])

        mock_client.close.assert_called_once()

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_passes_correct_table_and_columns(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        rows = [("val", 42)]
        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.bulk_insert_rows("target_table", rows, column_names=["name", "count"])

        call_args = mock_client.insert.call_args
        assert call_args.args[0] == "target_table"
        assert call_args.args[1] == rows
        assert call_args.kwargs["column_names"] == ["name", "count"]

    @patch("airflow.providers.clickhousedb.hooks.clickhouse.ClickHouseHook.get_connection")
    @patch("clickhouse_connect.get_client")
    def test_single_row(self, mock_get_client, mock_get_connection):
        mock_get_connection.return_value = BASE_CONN
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        hook = ClickHouseHook(clickhouse_conn_id="clickhouse_test")
        hook.bulk_insert_rows("t", [("a",)], column_names=["name"])

        mock_client.insert.assert_called_once()
