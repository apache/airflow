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
from prestodb.transaction import IsolationLevel

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.presto.hooks.presto import PrestoHook, generate_presto_client_info

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


def test_generate_airflow_presto_client_info_header():
    date_key = "logical_date" if AIRFLOW_V_3_0_PLUS else "execution_date"
    environ_date_key = "AIRFLOW_CTX_LOGICAL_DATE" if AIRFLOW_V_3_0_PLUS else "AIRFLOW_CTX_EXECUTION_DATE"
    env_vars = {
        "AIRFLOW_CTX_DAG_ID": "dag_id",
        environ_date_key: "2022-01-01T00:00:00",
        "AIRFLOW_CTX_TASK_ID": "task_id",
        "AIRFLOW_CTX_TRY_NUMBER": "1",
        "AIRFLOW_CTX_DAG_RUN_ID": "dag_run_id",
        "AIRFLOW_CTX_DAG_OWNER": "dag_owner",
    }
    expected = json.dumps(
        {
            "dag_id": "dag_id",
            date_key: "2022-01-01T00:00:00",
            "task_id": "task_id",
            "try_number": "1",
            "dag_run_id": "dag_run_id",
            "dag_owner": "dag_owner",
        },
        sort_keys=True,
    )
    with patch.dict("os.environ", env_vars):
        assert generate_presto_client_info() == expected


class TestPrestoHookConn:
    @patch("airflow.providers.presto.hooks.presto.prestodb.auth.BasicAuthentication")
    @patch("airflow.providers.presto.hooks.presto.prestodb.dbapi.connect")
    @patch("airflow.providers.presto.hooks.presto.PrestoHook.get_connection")
    def test_get_conn_basic_auth(self, mock_get_connection, mock_connect, mock_basic_auth):
        mock_get_connection.return_value = Connection(
            login="login", password="password", host="host", schema="hive"
        )

        conn = PrestoHook().get_conn()
        mock_connect.assert_called_once_with(
            catalog="hive",
            host="host",
            port=None,
            http_headers=mock.ANY,
            http_scheme="http",
            schema="hive",
            source="airflow",
            user="login",
            isolation_level=0,
            auth=mock_basic_auth.return_value,
        )
        mock_basic_auth.assert_called_once_with("login", "password")
        assert mock_connect.return_value == conn

    @patch("airflow.providers.presto.hooks.presto.PrestoHook.get_connection")
    def test_get_conn_invalid_auth(self, mock_get_connection):
        mock_get_connection.return_value = Connection(
            login="login",
            password="password",
            host="host",
            schema="hive",
            extra=json.dumps({"auth": "kerberos"}),
        )
        with pytest.raises(
            AirflowException, match=re.escape("Kerberos authorization doesn't support password.")
        ):
            PrestoHook().get_conn()

    @patch("airflow.providers.presto.hooks.presto.prestodb.auth.KerberosAuthentication")
    @patch("airflow.providers.presto.hooks.presto.prestodb.dbapi.connect")
    @patch("airflow.providers.presto.hooks.presto.PrestoHook.get_connection")
    def test_get_conn_kerberos_auth(self, mock_get_connection, mock_connect, mock_auth):
        mock_get_connection.return_value = Connection(
            login="login",
            host="host",
            schema="hive",
            extra=json.dumps(
                {
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
                }
            ),
        )

        conn = PrestoHook().get_conn()
        mock_connect.assert_called_once_with(
            catalog="hive",
            host="host",
            port=None,
            http_headers=mock.ANY,
            http_scheme="http",
            schema="hive",
            source="airflow",
            user="login",
            isolation_level=0,
            auth=mock_auth.return_value,
        )
        mock_auth.assert_called_once_with(
            ca_bundle="TEST_CA_BUNDLE",
            config="TEST_KERBEROS_CONFIG",
            delegate="TEST_DELEGATE",
            force_preemptive=True,
            hostname_override="TEST_HOSTNAME_OVERRIDE",
            mutual_authentication="TEST_MUTUAL_AUTHENTICATION",
            principal="TEST_PRINCIPAL",
            sanitize_mutual_error_response=True,
            service_name="TEST_SERVICE_NAME",
        )
        assert mock_connect.return_value == conn

    @patch("airflow.providers.presto.hooks.presto.generate_presto_client_info")
    @patch("airflow.providers.presto.hooks.presto.prestodb.auth.BasicAuthentication")
    @patch("airflow.providers.presto.hooks.presto.prestodb.dbapi.connect")
    @patch("airflow.providers.presto.hooks.presto.PrestoHook.get_connection")
    def test_http_headers(
        self,
        mock_get_connection,
        mock_connect,
        mock_basic_auth,
        mocked_generate_airflow_presto_client_info_header,
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
        http_headers = {"X-Presto-Client-Info": client}

        mocked_generate_airflow_presto_client_info_header.return_value = http_headers["X-Presto-Client-Info"]

        conn = PrestoHook().get_conn()
        mock_connect.assert_called_once_with(
            catalog="hive",
            host="host",
            port=None,
            http_headers=http_headers,
            http_scheme="http",
            schema="hive",
            source="airflow",
            user="login",
            isolation_level=0,
            auth=mock_basic_auth.return_value,
        )
        mock_basic_auth.assert_called_once_with("login", "password")
        assert mock_connect.return_value == conn

    @pytest.mark.parametrize(
        ("current_verify", "expected_verify"),
        [
            ("False", False),
            ("false", False),
            ("True", True),
            ("true", True),
            ("/tmp/cert.crt", "/tmp/cert.crt"),
        ],
    )
    def test_get_conn_verify(self, current_verify, expected_verify):
        patcher_connect = patch("airflow.providers.presto.hooks.presto.prestodb.dbapi.connect")
        patcher_get_connections = patch("airflow.providers.presto.hooks.presto.PrestoHook.get_connection")

        with patcher_connect as mock_connect, patcher_get_connections as mock_get_connection:
            mock_get_connection.return_value = Connection(
                login="login", host="host", schema="hive", extra=json.dumps({"verify": current_verify})
            )
            mock_verify = mock.PropertyMock()
            type(mock_connect.return_value._http_session).verify = mock_verify

            conn = PrestoHook().get_conn()
            mock_verify.assert_called_once_with(expected_verify)
            assert mock_connect.return_value == conn


class TestPrestoHook:
    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur
        conn = self.conn

        class UnitTestPrestoHook(PrestoHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

            def get_isolation_level(self):
                return IsolationLevel.READ_COMMITTED

        self.db_hook = UnitTestPrestoHook()

    @patch("airflow.providers.common.sql.hooks.sql.DbApiHook.insert_rows")
    def test_insert_rows(self, mock_insert_rows):
        table = "table"
        rows = [("hello",), ("world",)]
        target_fields = None
        commit_every = 10
        self.db_hook.insert_rows(table, rows, target_fields, commit_every)
        mock_insert_rows.assert_called_once_with(table, rows, None, 10)

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

    def test_serialize_cell(self):
        assert self.db_hook._serialize_cell("foo", None) == "foo"
        assert self.db_hook._serialize_cell(1, None) == 1
