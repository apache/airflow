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
import unittest
from unittest import mock
from unittest.mock import patch

import pytest
from parameterized import parameterized
from trino.transaction import IsolationLevel

from airflow import AirflowException
from airflow.models import Connection
from airflow.providers.trino.hooks.trino import TrinoHook

HOOK_GET_CONNECTION = "airflow.providers.trino.hooks.trino.TrinoHook.get_connection"
BASIC_AUTHENTICATION = "airflow.providers.trino.hooks.trino.trino.auth.BasicAuthentication"
KERBEROS_AUTHENTICATION = "airflow.providers.trino.hooks.trino.trino.auth.KerberosAuthentication"
TRINO_DBAPI_CONNECT = "airflow.providers.trino.hooks.trino.trino.dbapi.connect"
JWT_AUTHENTICATION = "airflow.providers.trino.hooks.trino.trino.auth.JWTAuthentication"
CERT_AUTHENTICATION = "airflow.providers.trino.hooks.trino.trino.auth.CertificateAuthentication"


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
        client = json.dumps(
            {
                "dag_id": "dag-id",
                "execution_date": "2022-01-01T00:00:00",
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
            AirflowException, match=re.escape("The 'kerberos' authorization type doesn't support password.")
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

    @parameterized.expand(
        [
            ("False", False),
            ("false", False),
            ("true", True),
            ("true", True),
            ("/tmp/cert.crt", "/tmp/cert.crt"),
        ]
    )
    @patch(HOOK_GET_CONNECTION)
    @patch(TRINO_DBAPI_CONNECT)
    def test_get_conn_verify(self, current_verify, expected_verify, mock_connect, mock_get_connection):
        extras = {"verify": current_verify}
        self.set_get_connection_return_value(mock_get_connection, extra=json.dumps(extras))
        TrinoHook().get_conn()
        self.assert_connection_called_with(mock_connect, verify=expected_verify)

    @staticmethod
    def set_get_connection_return_value(mock_get_connection, extra=None, password=None):
        mocked_connection = Connection(
            login="login", password=password, host="host", schema="hive", extra=extra or "{}"
        )
        mock_get_connection.return_value = mocked_connection

    @staticmethod
    def assert_connection_called_with(
        mock_connect, http_headers=mock.ANY, auth=None, verify=True, session_properties=None
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
        )


class TestTrinoHook(unittest.TestCase):
    def setUp(self):
        super().setUp()

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

    def test_get_pandas_df(self):
        statement = "SQL"
        column = "col"
        result_sets = [("row1",), ("row2",)]
        self.cur.description = [(column,)]
        self.cur.fetchall.return_value = result_sets
        df = self.db_hook.get_pandas_df(statement)

        assert column == df.columns[0]

        assert result_sets[0][0] == df.values.tolist()[0][0]
        assert result_sets[1][0] == df.values.tolist()[1][0]

        self.cur.execute.assert_called_once_with(statement, None)

    @patch("airflow.providers.trino.hooks.trino.TrinoHook.run")
    def test_run(self, mock_run):
        sql = "SELECT 1"
        autocommit = False
        parameters = ("hello", "world")
        handler = list
        self.db_hook.run(sql, autocommit, parameters, list)
        mock_run.assert_called_once_with(sql, autocommit, parameters, handler)

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


class TestTrinoHookIntegration(unittest.TestCase):
    @pytest.mark.integration("trino")
    @mock.patch.dict("os.environ", AIRFLOW_CONN_TRINO_DEFAULT="trino://airflow@trino:8080/")
    def test_should_record_records(self):
        hook = TrinoHook()
        sql = "SELECT name FROM tpch.sf1.customer ORDER BY custkey ASC LIMIT 3"
        records = hook.get_records(sql)
        assert [["Customer#000000001"], ["Customer#000000002"], ["Customer#000000003"]] == records

    @pytest.mark.integration("trino")
    @pytest.mark.integration("kerberos")
    def test_should_record_records_with_kerberos_auth(self):
        conn_url = (
            "trino://airflow@trino.example.com:7778/?"
            "auth=kerberos&kerberos__service_name=HTTP&"
            "verify=False&"
            "protocol=https"
        )
        with mock.patch.dict("os.environ", AIRFLOW_CONN_TRINO_DEFAULT=conn_url):
            hook = TrinoHook()
            sql = "SELECT name FROM tpch.sf1.customer ORDER BY custkey ASC LIMIT 3"
            records = hook.get_records(sql)
            assert [["Customer#000000001"], ["Customer#000000002"], ["Customer#000000003"]] == records
