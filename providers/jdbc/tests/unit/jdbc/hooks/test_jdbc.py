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
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import current_thread
from time import sleep
from unittest import mock
from unittest.mock import MagicMock, Mock, patch

import jaydebeapi
import pytest

from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.jdbc.hooks.jdbc import JdbcHook, suppress_and_warn

from tests_common.test_utils.version_compat import SQLALCHEMY_V_1_4

jdbc_conn_mock = Mock(name="jdbc_conn")
logger = logging.getLogger(__name__)


def get_hook(
    hook_params=None,
    conn_params=None,
    conn_type: str | None = None,
    login: str | None = "login",
    password: str | None = "password",
    host: str | None = "host",
    schema: str | None = "schema",
    port: int | None = 1234,
    uri: str | None = None,
):
    hook_params = hook_params or {}
    conn_params = conn_params or {}
    connection = Connection(
        **{
            **dict(
                conn_type=conn_type,
                login=login,
                password=password,
                host=host,
                schema=schema,
                port=port,
                uri=uri,
            ),
            **conn_params,
        }
    )

    class MockedJdbcHook(JdbcHook):
        @classmethod
        def get_connection(cls, conn_id: str) -> Connection:
            return connection

    hook = MockedJdbcHook(**hook_params)
    return hook


class TestJdbcHook:
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="jdbc_default",
                conn_type="jdbc",
                host="jdbc://localhost/",
                port=443,
                extra=json.dumps(
                    {
                        "driver_path": "/path1/test.jar,/path2/t.jar2",
                        "driver_class": "com.driver.main",
                    }
                ),
            )
        )

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect", autospec=True, return_value=jdbc_conn_mock)
    def test_jdbc_conn_connection(self, jdbc_mock):
        jdbc_hook = JdbcHook()
        jdbc_conn = jdbc_hook.get_conn()
        assert jdbc_mock.called
        assert isinstance(jdbc_conn, Mock)
        assert jdbc_conn.name == jdbc_mock.return_value.name

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect")
    def test_jdbc_conn_set_autocommit(self, _):
        jdbc_hook = JdbcHook()
        jdbc_conn = jdbc_hook.get_conn()
        jdbc_hook.set_autocommit(jdbc_conn, False)
        jdbc_conn.jconn.setAutoCommit.assert_called_once_with(False)

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect")
    def test_jdbc_conn_set_autocommit_when_not_supported(self, _):
        jdbc_hook = JdbcHook()
        jdbc_conn = jdbc_hook.get_conn()
        jdbc_conn.jconn.setAutoCommit.side_effect = jaydebeapi.Error()
        jdbc_hook.set_autocommit(jdbc_conn, False)

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect")
    def test_jdbc_conn_get_autocommit(self, _):
        jdbc_hook = JdbcHook()
        jdbc_conn = jdbc_hook.get_conn()
        jdbc_hook.get_autocommit(jdbc_conn)
        jdbc_conn.jconn.getAutoCommit.assert_called_once_with()

    @patch("airflow.providers.jdbc.hooks.jdbc.jaydebeapi.connect")
    def test_jdbc_conn_get_autocommit_when_not_supported_then_return_false(self, _):
        jdbc_hook = JdbcHook()
        jdbc_conn = jdbc_hook.get_conn()
        jdbc_conn.jconn.getAutoCommit.side_effect = jaydebeapi.Error()
        assert jdbc_hook.get_autocommit(jdbc_conn) is False

    def test_driver_hook_params(self):
        hook = get_hook(hook_params=dict(driver_path="Blah driver path", driver_class="Blah driver class"))
        assert hook.driver_path == "Blah driver path"
        assert hook.driver_class == "Blah driver class"

    def test_driver_in_extra_not_used(self):
        conn_params = dict(
            extra=json.dumps(dict(driver_path="ExtraDriverPath", driver_class="ExtraDriverClass"))
        )
        hook_params = {"driver_path": "ParamDriverPath", "driver_class": "ParamDriverClass"}
        hook = get_hook(conn_params=conn_params, hook_params=hook_params)
        assert hook.driver_path == "ParamDriverPath"
        assert hook.driver_class == "ParamDriverClass"

    def test_driver_extra_raises_warning_by_default(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow.providers.jdbc.hooks.test_jdbc"):
            driver_path = get_hook(conn_params=dict(extra='{"driver_path": "Blah driver path"}')).driver_path
            assert (
                "You have supplied 'driver_path' via connection extra but it will not be used"
            ) in caplog.text
            assert driver_path is None

            driver_class = get_hook(
                conn_params=dict(extra='{"driver_class": "Blah driver class"}')
            ).driver_class
            assert (
                "You have supplied 'driver_class' via connection extra but it will not be used"
            ) in caplog.text
            assert driver_class is None

    @mock.patch.dict("os.environ", {"AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_PATH_IN_EXTRA": "TRUE"})
    @mock.patch.dict("os.environ", {"AIRFLOW__PROVIDERS_JDBC__ALLOW_DRIVER_CLASS_IN_EXTRA": "TRUE"})
    def test_driver_extra_works_when_allow_driver_extra(self):
        hook = get_hook(
            conn_params=dict(extra='{"driver_path": "Blah driver path", "driver_class": "Blah driver class"}')
        )
        assert hook.driver_path == "Blah driver path"
        assert hook.driver_class == "Blah driver class"

    def test_default_driver_set(self):
        with (
            patch.object(JdbcHook, "default_driver_path", "Blah driver path") as _,
            patch.object(JdbcHook, "default_driver_class", "Blah driver class") as _,
        ):
            hook = get_hook()
            assert hook.driver_path == "Blah driver path"
            assert hook.driver_class == "Blah driver class"

    def test_driver_none_by_default(self):
        hook = get_hook()
        assert hook.driver_path is None
        assert hook.driver_class is None

    def test_driver_extra_raises_warning_and_returns_default_driver_by_default(self, caplog):
        with patch.object(JdbcHook, "default_driver_path", "Blah driver path"):
            with caplog.at_level(logging.WARNING, logger="airflow.providers.jdbc.hooks.test_jdbc"):
                driver_path = get_hook(
                    conn_params=dict(extra='{"driver_path": "Blah driver path2"}')
                ).driver_path
                assert (
                    "have supplied 'driver_path' via connection extra but it will not be used"
                ) in caplog.text
                assert driver_path == "Blah driver path"

        with patch.object(JdbcHook, "default_driver_class", "Blah driver class"):
            with caplog.at_level(logging.WARNING, logger="airflow.providers.jdbc.hooks.test_jdbc"):
                driver_class = get_hook(
                    conn_params=dict(extra='{"driver_class": "Blah driver class2"}')
                ).driver_class
                assert (
                    "have supplied 'driver_class' via connection extra but it will not be used"
                ) in caplog.text
                assert driver_class == "Blah driver class"

    def test_suppress_and_warn_when_raised_exception_is_suppressed(self):
        with pytest.warns(UserWarning, match="Exception suppressed: Foo Bar"):
            with suppress_and_warn(RuntimeError):
                raise RuntimeError("Foo Bar")

    def test_suppress_and_warn_when_raised_exception_is_not_suppressed(self):
        with pytest.raises(RuntimeError, match="Spam Egg"):
            with suppress_and_warn(KeyError):
                raise RuntimeError("Spam Egg")

    def test_sqlalchemy_url_without_sqlalchemy_scheme(self):
        hook_params = {"driver_path": "ParamDriverPath", "driver_class": "ParamDriverClass"}
        hook = get_hook(hook_params=hook_params)

        with pytest.raises(AirflowException):
            hook.sqlalchemy_url

    def test_sqlalchemy_url_with_sqlalchemy_scheme(self):
        conn_params = dict(extra=json.dumps(dict(sqlalchemy_scheme="mssql")))
        hook_params = {"driver_path": "ParamDriverPath", "driver_class": "ParamDriverClass"}
        hook = get_hook(conn_params=conn_params, hook_params=hook_params)

        expected = "mssql://login:password@host:1234/schema"
        if SQLALCHEMY_V_1_4:
            assert str(hook.sqlalchemy_url) == expected
        else:
            assert hook.sqlalchemy_url.render_as_string(hide_password=False) == expected

    def test_sqlalchemy_url_with_sqlalchemy_scheme_and_query(self):
        conn_params = dict(
            extra=json.dumps(dict(sqlalchemy_scheme="mssql", sqlalchemy_query={"servicename": "test"}))
        )
        hook_params = {"driver_path": "ParamDriverPath", "driver_class": "ParamDriverClass"}
        hook = get_hook(conn_params=conn_params, hook_params=hook_params)

        expected = "mssql://login:password@host:1234/schema?servicename=test"
        if SQLALCHEMY_V_1_4:
            assert str(hook.sqlalchemy_url) == expected
        else:
            assert hook.sqlalchemy_url.render_as_string(hide_password=False) == expected

    def test_sqlalchemy_url_with_sqlalchemy_scheme_and_wrong_query_value(self):
        conn_params = dict(extra=json.dumps(dict(sqlalchemy_scheme="mssql", sqlalchemy_query="wrong type")))
        hook_params = {"driver_path": "ParamDriverPath", "driver_class": "ParamDriverClass"}
        hook = get_hook(conn_params=conn_params, hook_params=hook_params)

        with pytest.raises(AirflowException):
            hook.sqlalchemy_url

    def test_get_sqlalchemy_engine_verify_creator_is_being_used(self):
        jdbc_hook = get_hook(
            conn_params=dict(extra={"sqlalchemy_scheme": "sqlite"}),
            login=None,
            password=None,
            host=None,
            schema=":memory:",
            port=None,
        )

        with sqlite3.connect(":memory:") as connection:
            jdbc_hook.get_conn = lambda: connection
            engine = jdbc_hook.get_sqlalchemy_engine()
            assert engine.connect().connection.connection == connection

    def test_dialect_name(self):
        jdbc_hook = get_hook(
            conn_params=dict(extra={"sqlalchemy_scheme": "hana"}),
            conn_type="jdbc",
            login=None,
            password=None,
            host="localhost",
            schema="sap",
            port=30215,
        )

        assert jdbc_hook.dialect_name == "hana"

    def test_dialect_name_when_host_is_jdbc_url(self):
        jdbc_hook = get_hook(
            conn_params=dict(
                extra={
                    "driver_class": "com.sap.db.jdbc.Driver",
                    "driver_path": "/usr/local/lib/java/ngdbc.jar",
                    "placeholder": "?",
                    "sqlalchemy_scheme": "hana",
                    "replace_statement_format": "UPSERT {} {} VALUES ({}) WITH PRIMARY KEY",
                }
            ),
            conn_type="jdbc",
            login=None,
            password=None,
            host="jdbc:sap://localhost:30015",
        )

        assert jdbc_hook.dialect_name == "hana"

    def test_get_conn_thread_safety(self):
        mock_conn = MagicMock()
        open_connections = 0

        def connect_side_effect(*args, **kwargs):
            nonlocal open_connections
            open_connections += 1
            logger.debug("Thread %s has %s open connections", current_thread().name, open_connections)

            try:
                if open_connections > 1:
                    raise OSError("JVM is already started")
            finally:
                sleep(0.1)  # wait a bit before releasing the connection again
                open_connections -= 1

            return mock_conn

        with patch.object(jaydebeapi, "connect", side_effect=connect_side_effect) as mock_connect:
            jdbc_hook = get_hook()

            def call_get_conn():
                conn = jdbc_hook.get_conn()
                assert conn is mock_conn

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []

                for _ in range(0, 10):
                    futures.append(executor.submit(call_get_conn))

                for future in as_completed(futures):
                    future.result()  # This will raise OSError if get_conn isn't threadsafe

            assert mock_connect.call_count == 10

    @pytest.mark.parametrize(
        ("params", "expected_uri"),
        [
            # JDBC URL fallback cases
            pytest.param(
                {"host": "jdbc:mysql://localhost:3306/test"},
                "jdbc:mysql://localhost:3306/test",
                id="jdbc-mysql",
            ),
            pytest.param(
                {"host": "jdbc:postgresql://localhost:5432/test?user=user&password=pass%40word"},
                "jdbc:postgresql://localhost:5432/test?user=user&password=pass%40word",
                id="jdbc-postgresql",
            ),
            pytest.param(
                {"host": "jdbc:oracle:thin:@localhost:1521:xe"},
                "jdbc:oracle:thin:@localhost:1521:xe",
                id="jdbc-oracle",
            ),
            pytest.param(
                {"host": "jdbc:sqlserver://localhost:1433;databaseName=test;trustServerCertificate=true"},
                "jdbc:sqlserver://localhost:1433;databaseName=test;trustServerCertificate=true",
                id="jdbc-sqlserver",
            ),
            # SQLAlchemy URI cases
            pytest.param(
                {
                    "conn_params": {
                        "extra": json.dumps(
                            {"sqlalchemy_scheme": "mssql", "sqlalchemy_query": {"servicename": "test"}}
                        )
                    }
                },
                "mssql://login:password@host:1234/schema?servicename=test",
                id="sqlalchemy-scheme-with-query",
            ),
            pytest.param(
                {
                    "conn_params": {
                        "extra": json.dumps(
                            {"sqlalchemy_scheme": "postgresql", "sqlalchemy_driver": "psycopg2"}
                        )
                    }
                },
                "postgresql+psycopg2://login:password@host:1234/schema",
                id="sqlalchemy-scheme-with-driver",
            ),
            pytest.param(
                {
                    "conn_params": {
                        "extra": json.dumps(
                            {"sqlalchemy_scheme": "postgresql", "sqlalchemy_driver": "psycopg"}
                        )
                    }
                },
                "postgresql+psycopg://login:password@host:1234/schema",
                id="sqlalchemy-scheme-with-driver-ppg3",
            ),
            pytest.param(
                {
                    "login": "user@domain",
                    "password": "pass/word",
                    "schema": "my/db",
                    "conn_params": {"extra": json.dumps({"sqlalchemy_scheme": "mysql"})},
                },
                "mysql://user%40domain:pass%2Fword@host:1234/my%2Fdb",
                id="sqlalchemy-with-encoding",
            ),
        ],
    )
    def test_get_uri(self, params, expected_uri):
        """Test get_uri with different configurations including JDBC URLs and SQLAlchemy URIs."""
        valid_keys = {"host", "login", "password", "schema", "conn_params"}
        hook_params = {key: params[key] for key in valid_keys & params.keys()}

        jdbc_hook = get_hook(**hook_params)
        assert jdbc_hook.get_uri() == expected_uri
