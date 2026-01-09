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
from unittest import mock
from unittest.mock import patch

import pytest
from trino.transaction import IsolationLevel

from airflow.models import Connection
from airflow.providers.common.compat.openlineage.facet import (
    Dataset,
    SchemaDatasetFacet,
    SchemaDatasetFacetFields,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.trino.hooks.trino import TrinoHook

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

HOOK_GET_CONNECTION = "airflow.providers.trino.hooks.trino.TrinoHook.get_connection"
BASIC_AUTHENTICATION = "airflow.providers.trino.hooks.trino.trino.auth.BasicAuthentication"
KERBEROS_AUTHENTICATION = "airflow.providers.trino.hooks.trino.trino.auth.KerberosAuthentication"
TRINO_DBAPI_CONNECT = "airflow.providers.trino.hooks.trino.trino.dbapi.connect"
JWT_AUTHENTICATION = "airflow.providers.trino.hooks.trino.trino.auth.JWTAuthentication"
CERT_AUTHENTICATION = "airflow.providers.trino.hooks.trino.trino.auth.CertificateAuthentication"
TRINO_CONN_ID = "test_trino"
TRINO_DEFAULT = "trino_default"


@pytest.fixture
def jwt_token_file(tmp_path):
    jwt_file = tmp_path / "jwt.json"
    jwt_file.write_text('{"phony":"jwt"}')
    return jwt_file.__fspath__()


class TestTrinoHookConn:
    @patch(BASIC_AUTHENTICATION)
    @patch(TRINO_DBAPI_CONNECT)
    @patch(HOOK_GET_CONNECTION)
    def test_get_conn_basic_auth(self, mock_get_connection, mock_connect, mock_basic_auth):
        self.set_get_connection_return_value(mock_get_connection, password="password")
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, auth=mock_basic_auth)
        mock_basic_auth.assert_called_once_with("login", "password")

    @patch("airflow.providers.trino.hooks.trino.generate_trino_client_info")
    @patch(BASIC_AUTHENTICATION)
    @patch(TRINO_DBAPI_CONNECT)
    @patch(HOOK_GET_CONNECTION)
    def test_http_headers(
        self,
        mock_get_connection,
        mock_connect,
        mock_basic_auth,
        mocked_generate_airflow_trino_client_info_header,
    ):
        mock_get_connection.return_value = Connection(
            login="login", password="password", host="host", schema="hive"
        )
        date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
        client = json.dumps(
            {
                "dag_id": "dag-id",
                date_key: "2022-01-01T00:00:00",
                "task_id": "task-id",
                "try_number": "1",
                "dag_run_id": "dag-run-id",
                "dag_owner": "dag-owner",
            },
            sort_keys=True,
        )
        http_headers = {"X-Trino-Client-Info": client}

        mocked_generate_airflow_trino_client_info_header.return_value = http_headers["X-Trino-Client-Info"]

        conn = TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, auth=mock_basic_auth, http_headers=http_headers)

        mock_basic_auth.assert_called_once_with("login", "password")
        assert mock_connect.return_value == conn

    @patch(HOOK_GET_CONNECTION)
    def test_get_conn_invalid_auth(self, mock_get_connection):
        extras = {"auth": "kerberos"}
        self.set_get_connection_return_value(
            mock_get_connection,
            password="password",
            extra=json.dumps(extras),
        )
        with pytest.raises(
            AirflowException,
            match=re.escape(
                "Multiple authentication methods specified: password, kerberos. Only one is allowed."
            ),
        ):
            TrinoHook().get_conn()

    @patch(JWT_AUTHENTICATION)
    @patch(TRINO_DBAPI_CONNECT)
    @patch(HOOK_GET_CONNECTION)
    def test_get_conn_jwt_auth(self, mock_get_connection, mock_connect, mock_jwt_auth):
        extras = {
            "auth": "jwt",
            "jwt__token": "TEST_JWT_TOKEN",
        }
        self.set_get_connection_return_value(
            mock_get_connection,
            extra=json.dumps(extras),
        )
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, auth=mock_jwt_auth)

    @patch(JWT_AUTHENTICATION)
    @patch(TRINO_DBAPI_CONNECT)
    @patch(HOOK_GET_CONNECTION)
    def test_get_conn_jwt_file(self, mock_get_connection, mock_connect, mock_jwt_auth, jwt_token_file):
        extras = {
            "auth": "jwt",
            "jwt__file": jwt_token_file,
        }
        self.set_get_connection_return_value(
            mock_get_connection,
            extra=json.dumps(extras),
        )
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, auth=mock_jwt_auth)

    @pytest.mark.parametrize(
        ("jwt_file", "jwt_token", "error_suffix"),
        [
            pytest.param(True, True, "provided both", id="provided-both-params"),
            pytest.param(False, False, "none of them provided", id="no-jwt-provided"),
        ],
    )
    @patch(HOOK_GET_CONNECTION)
    def test_exactly_one_jwt_token(
        self, mock_get_connection, jwt_file, jwt_token, error_suffix, jwt_token_file
    ):
        error_match = f"When auth set to 'jwt'.*{error_suffix}"
        extras = {"auth": "jwt"}
        if jwt_file:
            extras["jwt__file"] = jwt_token_file
        if jwt_token:
            extras["jwt__token"] = "TEST_JWT_TOKEN"

        self.set_get_connection_return_value(mock_get_connection, extra=json.dumps(extras))
        with pytest.raises(ValueError, match=error_match):
            TrinoHook().get_conn()

    @patch(CERT_AUTHENTICATION)
    @patch(TRINO_DBAPI_CONNECT)
    @patch(HOOK_GET_CONNECTION)
    def test_get_conn_cert_auth(self, mock_get_connection, mock_connect, mock_cert_auth):
        extras = {
            "auth": "certs",
            "certs__client_cert_path": "/path/to/client.pem",
            "certs__client_key_path": "/path/to/client.key",
        }
        self.set_get_connection_return_value(
            mock_get_connection,
            extra=json.dumps(extras),
        )
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, auth=mock_cert_auth)
        mock_cert_auth.assert_called_once_with("/path/to/client.pem", "/path/to/client.key")

    @patch(KERBEROS_AUTHENTICATION)
    @patch(TRINO_DBAPI_CONNECT)
    @patch(HOOK_GET_CONNECTION)
    def test_get_conn_kerberos_auth(self, mock_get_connection, mock_connect, mock_auth):
        extras = {
            "auth": "kerberos",
            "kerberos__config": "TEST_KERBEROS_CONFIG",
            "kerberos__service_name": "TEST_SERVICE_NAME",
            "kerberos__mutual_authentication": "TEST_MUTUAL_AUTHENTICATION",
            "kerberos__force_preemptive": True,
            "kerberos__hostname_override": "TEST_HOSTNAME_OVERRIDE",
            "kerberos__sanitize_mutual_error_response": True,
            "kerberos__principal": "TEST_PRINCIPAL",
            "kerberos__delegate": "TEST_DELEGATE",
            "kerberos__ca_bundle": "TEST_CA_BUNDLE",
            "verify": "true",
        }
        self.set_get_connection_return_value(
            mock_get_connection,
            extra=json.dumps(extras),
        )
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, auth=mock_auth)

    @patch(HOOK_GET_CONNECTION)
    @patch(TRINO_DBAPI_CONNECT)
    def test_get_conn_session_properties(self, mock_connect, mock_get_connection):
        extras = {
            "session_properties": {
                "scale_writers": "true",
                "task_writer_count": "1",
                "writer_min_size": "100MB",
            },
        }

        self.set_get_connection_return_value(mock_get_connection, extra=extras)
        TrinoHook().get_conn()

        self.assert_connection_called_with(mock_connect, session_properties=extras["session_properties"])

    @patch(HOOK_GET_CONNECTION)
    @patch(TRINO_DBAPI_CONNECT)
    def test_get_conn_client_tags(self, mock_connect, mock_get_connection):
        extras = {"client_tags": ["abc", "xyz"]}

        self.set_get_connection_return_value(mock_get_connection, extra=extras)
        TrinoHook().get_conn()

        self.assert_connection_called_with(mock_connect, client_tags=extras["client_tags"])

    @pytest.mark.parametrize(
        ("current_verify", "expected_verify"),
        [
            ("False", False),
            ("false", False),
            ("true", True),
            ("True", True),
            ("/tmp/cert.crt", "/tmp/cert.crt"),
        ],
    )
    @patch(HOOK_GET_CONNECTION)
    @patch(TRINO_DBAPI_CONNECT)
    def test_get_conn_verify(self, mock_connect, mock_get_connection, current_verify, expected_verify):
        extras = {"verify": current_verify}
        self.set_get_connection_return_value(mock_get_connection, extra=json.dumps(extras))
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, verify=expected_verify)

    @patch(HOOK_GET_CONNECTION)
    @patch(TRINO_DBAPI_CONNECT)
    def test_get_conn_timezone(self, mock_connect, mock_get_connection):
        extras = {"timezone": "Asia/Jerusalem"}
        self.set_get_connection_return_value(mock_get_connection, extra=json.dumps(extras))
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, timezone="Asia/Jerusalem")

    @patch(HOOK_GET_CONNECTION)
    @patch(TRINO_DBAPI_CONNECT)
    def test_get_conn_extra_credential(self, mock_connect, mock_get_connection):
        extras = {"extra_credential": [["a.username", "bar"], ["a.password", "foo"]]}
        self.set_get_connection_return_value(mock_get_connection, extra=json.dumps(extras))
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, extra_credential=extras["extra_credential"])

    @patch(HOOK_GET_CONNECTION)
    @patch(TRINO_DBAPI_CONNECT)
    def test_get_conn_roles(self, mock_connect, mock_get_connection):
        extras = {"roles": {"catalog1": "trinoRoleA", "catalog2": "trinoRoleB"}}
        self.set_get_connection_return_value(mock_get_connection, extra=json.dumps(extras))
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, roles=extras["roles"])

    @staticmethod
    def set_get_connection_return_value(mock_get_connection, extra=None, password=None):
        mocked_connection = Connection(
            login="login", password=password, host="host", schema="hive", extra=extra or "{}"
        )
        mock_get_connection.return_value = mocked_connection

    @staticmethod
    def assert_connection_called_with(
        mock_connect,
        http_headers=mock.ANY,
        auth=None,
        verify=True,
        session_properties=None,
        client_tags=None,
        timezone=None,
        extra_credential=None,
        roles=None,
    ):
        mock_connect.assert_called_once_with(
            catalog="hive",
            host="host",
            port=None,
            http_scheme="http",
            http_headers=http_headers,
            schema="hive",
            source="airflow",
            user="login",
            isolation_level=IsolationLevel.AUTOCOMMIT,
            auth=None if not auth else auth.return_value,
            verify=verify,
            session_properties=session_properties,
            client_tags=client_tags,
            timezone=timezone,
            extra_credential=extra_credential,
            roles=roles,
        )


class TestTrinoHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestTrinoHook(TrinoHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

            def get_isolation_level(self):
                return IsolationLevel.READ_COMMITTED

        self.db_hook = UnitTestTrinoHook()

    @patch("airflow.providers.common.sql.hooks.sql.DbApiHook.insert_rows")
    def test_insert_rows(self, mock_insert_rows):
        table = "table"
        rows = [("hello",), ("world",)]
        target_fields = None
        commit_every = 10
        replace = True
        self.db_hook.insert_rows(table, rows, target_fields, commit_every, replace)
        mock_insert_rows.assert_called_once_with(table, rows, None, 10, True)

    def test_get_first_record(self):
        statement = "SQL"
        result_sets = [("row1",), ("row2",)]
        self.cur.fetchone.return_value = result_sets[0]

        assert result_sets[0] == self.db_hook.get_first(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    def test_get_records(self):
        statement = "SQL"
        result_sets = [("row1",), ("row2",)]
        self.cur.fetchall.return_value = result_sets

        assert result_sets == self.db_hook.get_records(statement)
        self.conn.close.assert_called_once_with()
        self.cur.close.assert_called_once_with()
        self.cur.execute.assert_called_once_with(statement)

    @pytest.mark.parametrize("df_type", ["pandas", "polars"])
    def test_df(self, df_type):
        statement = "SQL"
        column = "col"
        result_sets = [("row1",), ("row2",)]
        self.cur.description = [(column, None, None, None, None, None, None)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook.get_df(statement, df_type=df_type)
        assert column == df.columns[0]
        if df_type == "pandas":
            assert result_sets[0][0] == df.values.tolist()[0][0]
            assert result_sets[1][0] == df.values.tolist()[1][0]
        else:
            assert result_sets[0][0] == df.row(0)[0]
            assert result_sets[1][0] == df.row(1)[0]

        self.cur.execute.assert_called_once_with(statement, None)

    def test_split_sql_string(self):
        statement = "SELECT 1; SELECT 2"
        result_sets = ["SELECT 1", "SELECT 2"]
        self.cur.fetchall.return_value = result_sets

        assert result_sets == self.db_hook.split_sql_string(
            sql=statement,
            strip_semicolon=self.db_hook.strip_semicolon,
        )

    @patch("airflow.providers.trino.hooks.trino.TrinoHook.run")
    def test_run(self, mock_run):
        sql = "SELECT 1"
        autocommit = False
        parameters = ("hello", "world")
        handler = list
        self.db_hook.run(sql, autocommit, parameters, list)
        mock_run.assert_called_once_with(sql, autocommit, parameters, handler)

    @patch("airflow.providers.common.sql.hooks.sql.DbApiHook.run")
    def test_run_defaults_no_handler(self, super_run):
        super_run.return_value = None
        sql = "SELECT 1"
        result = self.db_hook.run(sql)
        assert result is None
        super_run.assert_called_once_with(sql, False, None, None, True, True)

    @patch("airflow.providers.common.sql.hooks.sql.DbApiHook.run")
    def test_run_with_handler_and_params(self, super_run):
        super_run.return_value = [("ok",)]
        sql = "SELECT 1"
        autocommit = True
        parameters = ("hello", "world")
        handler = list
        res = self.db_hook.run(
            sql,
            autocommit=autocommit,
            parameters=parameters,
            handler=handler,
            split_statements=False,
            return_last=False,
        )
        assert res == [("ok",)]
        super_run.assert_called_once_with(sql, True, parameters, handler, False, False)

    @patch("airflow.providers.common.sql.hooks.sql.DbApiHook.run")
    def test_run_multistatement_defaults_to_split(self, super_run):
        sql = "SELECT 1; SELECT 2"
        self.db_hook.run(sql)
        super_run.assert_called_once_with(sql, False, None, None, True, True)

    def test_connection_success(self):
        status, msg = self.db_hook.test_connection()
        assert status is True
        assert msg == "Connection successfully tested"

    @patch("airflow.providers.trino.hooks.trino.TrinoHook.get_conn")
    def test_connection_failure(self, mock_conn):
        mock_conn.side_effect = Exception("Test")
        self.db_hook.get_conn = mock_conn
        status, msg = self.db_hook.test_connection()
        assert status is False
        assert msg == "Test"

    def test_serialize_cell(self):
        assert self.db_hook._serialize_cell("foo", None) == "foo"
        assert self.db_hook._serialize_cell(1, None) == 1


def test_execute_openlineage_events():
    DB_NAME = "tpch"
    DB_SCHEMA_NAME = "sf1"

    class TrinoHookForTests(TrinoHook):
        get_conn = mock.MagicMock(name="conn")
        get_connection = mock.MagicMock()

        def get_first(self, *_):
            return [f"{DB_NAME}.{DB_SCHEMA_NAME}"]

    dbapi_hook = TrinoHookForTests()

    sql = "SELECT name FROM tpch.sf1.customer LIMIT 3"
    op = SQLExecuteQueryOperator(task_id="trino_test", sql=sql, conn_id=TRINO_DEFAULT)
    op._hook = dbapi_hook
    rows = [
        (DB_SCHEMA_NAME, "customer", "custkey", 1, "bigint", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "name", 2, "varchar(25)", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "address", 3, "varchar(40)", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "nationkey", 4, "bigint", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "phone", 5, "varchar(15)", DB_NAME),
        (DB_SCHEMA_NAME, "customer", "acctbal", 6, "double", DB_NAME),
    ]
    dbapi_hook.get_connection.return_value = Connection(
        conn_id=TRINO_DEFAULT,
        conn_type="trino",
        host="trino",
        port=8080,
    )
    dbapi_hook.get_conn.return_value.cursor.return_value.fetchall.side_effect = [rows, []]

    lineage = op.get_openlineage_facets_on_start()
    assert lineage.inputs == [
        Dataset(
            namespace="trino://trino:8080",
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.customer",
            facets={
                "schema": SchemaDatasetFacet(
                    fields=[
                        SchemaDatasetFacetFields(name="custkey", type="bigint"),
                        SchemaDatasetFacetFields(name="name", type="varchar(25)"),
                        SchemaDatasetFacetFields(name="address", type="varchar(40)"),
                        SchemaDatasetFacetFields(name="nationkey", type="bigint"),
                        SchemaDatasetFacetFields(name="phone", type="varchar(15)"),
                        SchemaDatasetFacetFields(name="acctbal", type="double"),
                    ]
                )
            },
        )
    ]


@pytest.mark.parametrize(
    ("conn_params", "expected_uri"),
    [
        (
            {"login": "user", "password": "pass", "host": "localhost", "port": 8080, "schema": "hive"},
            "trino://user:pass@localhost:8080/hive",
        ),
        (
            {
                "login": "user",
                "password": "pass",
                "host": "localhost",
                "port": 8080,
                "schema": "hive",
                "extra": json.dumps({"schema": "sales"}),
            },
            "trino://user:pass@localhost:8080/hive/sales",
        ),
        (
            {"login": "user@example.com", "password": "p@ss:word", "host": "localhost", "schema": "hive"},
            "trino://user%40example.com:p%40ss%3Aword@localhost/hive",
        ),
        (
            {"host": "localhost", "port": 8080, "schema": "hive"},
            "trino://localhost:8080/hive",
        ),
        (
            {
                "login": "user",
                "host": "host.example.com",
                "schema": "hive",
                "extra": json.dumps({"param1": "value1", "param2": "value2"}),
            },
            "trino://user@host.example.com/hive?param1=value1&param2=value2",
        ),
    ],
    ids=[
        "basic-connection",
        "with-extra-schema",
        "special-chars",
        "no-credentials",
        "extra-params",
    ],
)
def test_get_uri(conn_params, expected_uri):
    """Test TrinoHook.get_uri properly formats connection URIs."""
    with patch(HOOK_GET_CONNECTION) as mock_get_connection:
        mock_get_connection.return_value = Connection(**conn_params)
        hook = TrinoHook()
        assert hook.get_uri() == expected_uri
