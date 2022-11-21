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
import unittest
from tempfile import NamedTemporaryFile
from unittest import mock

import psycopg2.extras
import pytest

from airflow.models import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.types import NOTSET


class TestPostgresHookConn:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.connection = Connection(login="login", password="password", host="host", schema="database")

        class UnitTestPostgresHook(PostgresHook):
            conn_name_attr = "test_conn_id"

        self.db_hook = UnitTestPostgresHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    def test_get_conn_non_default_id(self, mock_connect):
        self.db_hook.test_conn_id = "non_default"
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login", password="password", host="host", dbname="database", port=None
        )
        self.db_hook.get_connection.assert_called_once_with("non_default")

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login", password="password", host="host", dbname="database", port=None
        )

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    def test_get_uri(self, mock_connect):
        self.connection.extra = json.dumps({"client_encoding": "utf-8"})
        self.connection.conn_type = "postgres"
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert self.db_hook.get_uri() == "postgresql://login:password@host/database?client_encoding=utf-8"

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    def test_get_conn_cursor(self, mock_connect):
        self.connection.extra = '{"cursor": "dictcursor"}'
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            cursor_factory=psycopg2.extras.DictCursor,
            user="login",
            password="password",
            host="host",
            dbname="database",
            port=None,
        )

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    def test_get_conn_with_invalid_cursor(self, mock_connect):
        self.connection.extra = '{"cursor": "mycursor"}'
        with pytest.raises(ValueError):
            self.db_hook.get_conn()

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    def test_get_conn_from_connection(self, mock_connect):
        conn = Connection(login="login-conn", password="password-conn", host="host", schema="database")
        hook = PostgresHook(connection=conn)
        hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login-conn", password="password-conn", host="host", dbname="database", port=None
        )

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    def test_get_conn_from_connection_with_database(self, mock_connect):
        conn = Connection(login="login-conn", password="password-conn", host="host", schema="database")
        hook = PostgresHook(connection=conn, database="database-override")
        hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login-conn", password="password-conn", host="host", dbname="database-override", port=None
        )

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook")
    @pytest.mark.parametrize("aws_conn_id", [NOTSET, None, "mock_aws_conn"])
    @pytest.mark.parametrize("port", [65432, 5432, None])
    def test_get_conn_rds_iam_postgres(self, mock_aws_hook_class, mock_connect, aws_conn_id, port):
        mock_conn_extra = {"iam": True}
        if aws_conn_id is not NOTSET:
            mock_conn_extra["aws_conn_id"] = aws_conn_id
        self.connection.extra = json.dumps(mock_conn_extra)
        self.connection.port = port
        mock_db_token = "aws_token"

        # Mock AWS Connection
        mock_aws_hook_instance = mock_aws_hook_class.return_value
        mock_client = mock.MagicMock()
        mock_client.generate_db_auth_token.return_value = mock_db_token
        type(mock_aws_hook_instance).conn = mock.PropertyMock(return_value=mock_client)

        self.db_hook.get_conn()
        # Check AwsHook initialization
        mock_aws_hook_class.assert_called_once_with(
            # If aws_conn_id not set than fallback to aws_default
            aws_conn_id=aws_conn_id if aws_conn_id is not NOTSET else "aws_default",
            client_type="rds",
        )
        # Check boto3 'rds' client method `generate_db_auth_token` call args
        mock_client.generate_db_auth_token.assert_called_once_with(
            self.connection.host, (port or 5432), self.connection.login
        )
        # Check expected psycopg2 connection call args
        mock_connect.assert_called_once_with(
            user=self.connection.login,
            password=mock_db_token,
            host=self.connection.host,
            dbname=self.connection.schema,
            port=(port or 5432),
        )

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    def test_get_conn_extra(self, mock_connect):
        self.connection.extra = '{"connect_timeout": 3}'
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login", password="password", host="host", dbname="database", port=None, connect_timeout=3
        )

    @mock.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")
    @mock.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook")
    @pytest.mark.parametrize("aws_conn_id", [NOTSET, None, "mock_aws_conn"])
    @pytest.mark.parametrize("port", [5432, 5439, None])
    @pytest.mark.parametrize(
        "host,conn_cluster_identifier,expected_cluster_identifier",
        [
            (
                "cluster-identifier.ccdfre4hpd39h.us-east-1.redshift.amazonaws.com",
                NOTSET,
                "cluster-identifier",
            ),
            (
                "cluster-identifier.ccdfre4hpd39h.us-east-1.redshift.amazonaws.com",
                "different-identifier",
                "different-identifier",
            ),
        ],
    )
    def test_get_conn_rds_iam_redshift(
        self,
        mock_aws_hook_class,
        mock_connect,
        aws_conn_id,
        port,
        host,
        conn_cluster_identifier,
        expected_cluster_identifier,
    ):
        mock_conn_extra = {
            "iam": True,
            "redshift": True,
        }
        if aws_conn_id is not NOTSET:
            mock_conn_extra["aws_conn_id"] = aws_conn_id
        if conn_cluster_identifier is not NOTSET:
            mock_conn_extra["cluster-identifier"] = conn_cluster_identifier

        self.connection.extra = json.dumps(mock_conn_extra)
        self.connection.host = host
        self.connection.port = port
        mock_db_user = f"IAM:{self.connection.login}"
        mock_db_pass = "aws_token"

        # Mock AWS Connection
        mock_aws_hook_instance = mock_aws_hook_class.return_value
        mock_client = mock.MagicMock()
        mock_client.get_cluster_credentials.return_value = {
            "DbPassword": mock_db_pass,
            "DbUser": mock_db_user,
        }
        type(mock_aws_hook_instance).conn = mock.PropertyMock(return_value=mock_client)

        self.db_hook.get_conn()
        # Check AwsHook initialization
        mock_aws_hook_class.assert_called_once_with(
            # If aws_conn_id not set than fallback to aws_default
            aws_conn_id=aws_conn_id if aws_conn_id is not NOTSET else "aws_default",
            client_type="redshift",
        )
        # Check boto3 'redshift' client method `get_cluster_credentials` call args
        mock_client.get_cluster_credentials.assert_called_once_with(
            DbUser=self.connection.login,
            DbName=self.connection.schema,
            ClusterIdentifier=expected_cluster_identifier,
            AutoCreate=False,
        )
        # Check expected psycopg2 connection call args
        mock_connect.assert_called_once_with(
            user=mock_db_user,
            password=mock_db_pass,
            host=host,
            dbname=self.connection.schema,
            port=(port or 5439),
        )

    def test_get_uri_from_connection_without_database_override(self):
        self.db_hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="postgres",
                host="host",
                login="login",
                password="password",
                schema="database",
                port=1,
            )
        )
        assert "postgresql://login:password@host:1/database" == self.db_hook.get_uri()

    def test_get_uri_from_connection_with_database_override(self):
        hook = PostgresHook(database="database-override")
        hook.get_connection = mock.MagicMock(
            return_value=Connection(
                conn_type="postgres",
                host="host",
                login="login",
                password="password",
                schema="database",
                port=1,
            )
        )
        assert "postgresql://login:password@host:1/database-override" == hook.get_uri()

    def test_schema_kwarg_database_kwarg_compatibility(self):
        database = "database-override"
        hook = PostgresHook(schema=database)
        assert hook.database == database


class TestPostgresHook(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = "test_postgres_hook_table"

    def setUp(self):
        super().setUp()

        self.cur = mock.MagicMock(rowcount=0)
        self.conn = conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur

        class UnitTestPostgresHook(PostgresHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestPostgresHook()

    def tearDown(self):
        super().tearDown()

        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.table}")

    @pytest.mark.backend("postgres")
    def test_copy_expert(self):
        open_mock = mock.mock_open(read_data='{"some": "json"}')
        with mock.patch("airflow.providers.postgres.hooks.postgres.open", open_mock):
            statement = "SQL"
            filename = "filename"

            self.cur.fetchall.return_value = None

            assert self.db_hook.copy_expert(statement, filename) is None

            assert self.conn.close.call_count == 1
            assert self.cur.close.call_count == 1
            assert self.conn.commit.call_count == 1
            self.cur.copy_expert.assert_called_once_with(statement, open_mock.return_value)
            assert open_mock.call_args[0] == (filename, "r+")

    @pytest.mark.backend("postgres")
    def test_bulk_load(self):
        hook = PostgresHook()
        input_data = ["foo", "bar", "baz"]

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE TABLE {self.table} (c VARCHAR)")
                conn.commit()

                with NamedTemporaryFile() as f:
                    f.write("\n".join(input_data).encode("utf-8"))
                    f.flush()
                    hook.bulk_load(self.table, f.name)

                cur.execute(f"SELECT * FROM {self.table}")
                results = [row[0] for row in cur.fetchall()]

        assert sorted(input_data) == sorted(results)

    @pytest.mark.backend("postgres")
    def test_bulk_dump(self):
        hook = PostgresHook()
        input_data = ["foo", "bar", "baz"]

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE TABLE {self.table} (c VARCHAR)")
                values = ",".join(f"('{data}')" for data in input_data)
                cur.execute(f"INSERT INTO {self.table} VALUES {values}")
                conn.commit()

                with NamedTemporaryFile() as f:
                    hook.bulk_dump(self.table, f.name)
                    f.seek(0)
                    results = [line.rstrip().decode("utf-8") for line in f.readlines()]

        assert sorted(input_data) == sorted(results)

    @pytest.mark.backend("postgres")
    def test_insert_rows(self):
        table = "table"
        rows = [("hello",), ("world",)]

        self.db_hook.insert_rows(table, rows)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == self.conn.commit.call_count

        sql = f"INSERT INTO {table}  VALUES (%s)"
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    @pytest.mark.backend("postgres")
    def test_insert_rows_replace(self):
        table = "table"
        rows = [
            (
                1,
                "hello",
            ),
            (
                2,
                "world",
            ),
        ]
        fields = ("id", "value")

        self.db_hook.insert_rows(table, rows, fields, replace=True, replace_index=fields[0])

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == self.conn.commit.call_count

        sql = (
            "INSERT INTO {0} ({1}, {2}) VALUES (%s,%s) "
            "ON CONFLICT ({1}) DO UPDATE SET {2} = excluded.{2}".format(table, fields[0], fields[1])
        )
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    @pytest.mark.backend("postgres")
    def test_insert_rows_replace_missing_target_field_arg(self):
        table = "table"
        rows = [
            (
                1,
                "hello",
            ),
            (
                2,
                "world",
            ),
        ]
        fields = ("id", "value")
        with pytest.raises(ValueError) as ctx:
            self.db_hook.insert_rows(table, rows, replace=True, replace_index=fields[0])

        assert str(ctx.value) == "PostgreSQL ON CONFLICT upsert syntax requires column names"

    @pytest.mark.backend("postgres")
    def test_insert_rows_replace_missing_replace_index_arg(self):
        table = "table"
        rows = [
            (
                1,
                "hello",
            ),
            (
                2,
                "world",
            ),
        ]
        fields = ("id", "value")
        with pytest.raises(ValueError) as ctx:
            self.db_hook.insert_rows(table, rows, fields, replace=True)

        assert str(ctx.value) == "PostgreSQL ON CONFLICT upsert syntax requires an unique index"

    @pytest.mark.backend("postgres")
    def test_insert_rows_replace_all_index(self):
        table = "table"
        rows = [
            (
                1,
                "hello",
            ),
            (
                2,
                "world",
            ),
        ]
        fields = ("id", "value")

        self.db_hook.insert_rows(table, rows, fields, replace=True, replace_index=fields)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == self.conn.commit.call_count

        sql = (
            f"INSERT INTO {table} ({', '.join(fields)}) VALUES (%s,%s) "
            f"ON CONFLICT ({', '.join(fields)}) DO NOTHING"
        )
        for row in rows:
            self.cur.execute.assert_any_call(sql, row)

    @pytest.mark.backend("postgres")
    def test_rowcount(self):
        hook = PostgresHook()
        input_data = ["foo", "bar", "baz"]

        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE TABLE {self.table} (c VARCHAR)")
                values = ",".join(f"('{data}')" for data in input_data)
                cur.execute(f"INSERT INTO {self.table} VALUES {values}")
                conn.commit()
                assert cur.rowcount == len(input_data)
