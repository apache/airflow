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
import os
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import polars as pl
import pytest
import sqlalchemy

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.postgres.dialects.postgres import PostgresDialect
from airflow.providers.postgres.hooks.postgres import CompatConnection, PostgresHook

from tests_common.test_utils.common_sql import mock_db_hook
from tests_common.test_utils.version_compat import NOTSET, SQLALCHEMY_V_1_4

INSERT_SQL_STATEMENT = "INSERT INTO connection (id, conn_id, conn_type, description, host, {}, login, password, port, is_encrypted, is_extra_encrypted, extra) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"


USE_PSYCOPG3: bool
try:
    from importlib.util import find_spec

    import sqlalchemy
    from packaging.version import Version

    is_psycopg3 = find_spec("psycopg") is not None
    sqlalchemy_version = Version(sqlalchemy.__version__)
    is_sqla2 = (sqlalchemy_version.major, sqlalchemy_version.minor, sqlalchemy_version.micro) >= (2, 0, 0)

    USE_PSYCOPG3 = is_psycopg3 and is_sqla2
except (ImportError, ModuleNotFoundError):
    USE_PSYCOPG3 = False

if USE_PSYCOPG3:
    import psycopg.rows
else:
    import psycopg2.extras


@pytest.fixture
def postgres_hook_setup():
    """Set up mock PostgresHook for testing."""
    table = "test_postgres_hook_table"
    cur = mock.MagicMock(rowcount=0)
    conn = mock.MagicMock(spec=CompatConnection)
    conn.cursor.return_value = cur

    class UnitTestPostgresHook(PostgresHook):
        conn_name_attr = "test_conn_id"

        def get_conn(self):
            return conn

    db_hook = UnitTestPostgresHook()

    # Return a namespace with all the objects
    setup = SimpleNamespace(table=table, cur=cur, conn=conn, db_hook=db_hook)

    yield setup

    # Teardown - only for real database tests
    try:
        with PostgresHook().get_conn() as real_conn:
            with real_conn.cursor() as real_cur:
                real_cur.execute(f"DROP TABLE IF EXISTS {table}")
    except Exception:
        pass  # Ignore cleanup errors for unit tests


@pytest.fixture
def mock_connect(mocker):
    """Mock the connection object according to the correct psycopg version."""
    if USE_PSYCOPG3:
        return mocker.patch("airflow.providers.postgres.hooks.postgres.psycopg.connection.Connection.connect")
    return mocker.patch("airflow.providers.postgres.hooks.postgres.psycopg2.connect")


class TestPostgresHookConn:
    """PostgresHookConn tests that are common to psycopg2 and psycopg3."""

    def setup_method(self):
        self.connection = Connection(login="login", password="password", host="host", schema="database")

        class UnitTestPostgresHook(PostgresHook):
            conn_name_attr = "test_conn_id"

        self.db_hook = UnitTestPostgresHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    def test_sqlalchemy_url_with_wrong_sqlalchemy_query_value(self):
        conn = Connection(
            login="login-conn",
            password="password-conn",
            host="host",
            schema="database",
            extra=dict(sqlalchemy_query="wrong type"),
        )
        hook = PostgresHook(connection=conn)

        with pytest.raises(AirflowException):
            hook.sqlalchemy_url

    @pytest.mark.parametrize("aws_conn_id", [NOTSET, None, "mock_aws_conn"])
    @pytest.mark.parametrize("port", [5432, 5439, None])
    @pytest.mark.parametrize(
        ("host", "conn_cluster_identifier", "expected_host"),
        [
            (
                "cluster-identifier.ccdfre4hpd39h.us-east-1.redshift.amazonaws.com",
                NOTSET,
                "cluster-identifier.us-east-1",
            ),
            (
                "cluster-identifier.ccdfre4hpd39h.us-east-1.redshift.amazonaws.com",
                "different-identifier",
                "different-identifier.us-east-1",
            ),
        ],
    )
    def test_openlineage_methods_with_redshift(
        self,
        mocker,
        aws_conn_id,
        port,
        host,
        conn_cluster_identifier,
        expected_host,
    ):
        mock_aws_hook_class = mocker.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook")

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

        # Mock AWS Connection
        mock_aws_hook_instance = mock_aws_hook_class.return_value
        mock_aws_hook_instance.region_name = "us-east-1"

        assert (
            self.db_hook._get_openlineage_redshift_authority_part(self.connection)
            == f"{expected_host}:{port or 5439}"
        )

    def test_get_conn_non_default_id(self, mock_connect):
        self.db_hook.test_conn_id = "non_default"
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login",
            password="password",
            host="host",
            dbname="database",
            port=None,
        )
        self.db_hook.get_connection.assert_called_once_with("non_default")

    def test_get_conn(self, mock_connect):
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login",
            password="password",
            host="host",
            dbname="database",
            port=None,
        )

    def test_get_uri(self, mock_connect):
        self.connection.conn_type = "postgres"
        self.connection.port = 5432
        self.db_hook.get_conn()
        assert mock_connect.call_count == 1
        assert (
            self.db_hook.get_uri()
            == f"postgresql{'+psycopg' if USE_PSYCOPG3 else ''}://login:password@host:5432/database"
        )

    @pytest.mark.usefixtures("mock_connect")
    def test_get_conn_with_invalid_cursor(self):
        self.connection.extra = '{"cursor": "mycursor"}'
        with pytest.raises(ValueError, match="Invalid cursor passed mycursor."):
            self.db_hook.get_conn()

    def test_get_conn_from_connection(self, mock_connect):
        conn = Connection(login="login-conn", password="password-conn", host="host", schema="database")
        hook = PostgresHook(connection=conn)
        hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login-conn",
            password="password-conn",
            host="host",
            dbname="database",
            port=None,
        )

    def test_get_conn_from_connection_with_database(self, mock_connect):
        conn = Connection(login="login-conn", password="password-conn", host="host", schema="database")
        hook = PostgresHook(connection=conn, database="database-override")
        hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login-conn",
            password="password-conn",
            host="host",
            dbname="database-override",
            port=None,
        )

    def test_get_conn_from_connection_with_options(self, mock_connect):
        conn = Connection(login="login-conn", password="password-conn", host="host", schema="database")
        hook = PostgresHook(connection=conn, options="-c statement_timeout=3000ms")
        hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login-conn",
            password="password-conn",
            host="host",
            dbname="database",
            port=None,
            options="-c statement_timeout=3000ms",
        )

    @pytest.mark.parametrize("aws_conn_id", [NOTSET, None, "mock_aws_conn"])
    @pytest.mark.parametrize("port", [65432, 5432, None])
    def test_get_conn_rds_iam_postgres(self, mocker, mock_connect, aws_conn_id, port):
        mock_aws_hook_class = mocker.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook")

        mock_conn_extra = {"iam": True}
        if aws_conn_id is not NOTSET:
            mock_conn_extra["aws_conn_id"] = aws_conn_id
        self.connection.extra = json.dumps(mock_conn_extra)
        self.connection.port = port
        mock_db_token = "aws_token"

        # Mock AWS Connection
        mock_aws_hook_instance = mock_aws_hook_class.return_value
        mock_client = mocker.MagicMock()
        mock_client.generate_db_auth_token.return_value = mock_db_token
        type(mock_aws_hook_instance).conn = mocker.PropertyMock(return_value=mock_client)

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

    def test_get_conn_extra(self, mock_connect):
        self.connection.extra = '{"connect_timeout": 3}'
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            user="login", password="password", host="host", dbname="database", port=None, connect_timeout=3
        )

    @pytest.mark.parametrize("aws_conn_id", [NOTSET, None, "mock_aws_conn"])
    @pytest.mark.parametrize("port", [5432, 5439, None])
    @pytest.mark.parametrize(
        ("host", "conn_cluster_identifier", "expected_cluster_identifier"),
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
        mocker,
        mock_connect,
        aws_conn_id,
        port,
        host,
        conn_cluster_identifier,
        expected_cluster_identifier,
    ):
        mock_aws_hook_class = mocker.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook")

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
        mock_client = mocker.MagicMock()
        mock_client.get_cluster_credentials.return_value = {
            "DbPassword": mock_db_pass,
            "DbUser": mock_db_user,
        }
        type(mock_aws_hook_instance).conn = mocker.PropertyMock(return_value=mock_client)

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

    @pytest.mark.parametrize("aws_conn_id", [NOTSET, None, "mock_aws_conn"])
    @pytest.mark.parametrize("port", [5432, 5439, None])
    @pytest.mark.parametrize(
        ("host", "conn_workgroup_name", "expected_workgroup_name"),
        [
            (
                "serverless-workgroup.ccdfre4hpd39h.us-east-1.redshift.amazonaws.com",
                NOTSET,
                "serverless-workgroup",
            ),
            (
                "cluster-identifier.ccdfre4hpd39h.us-east-1.redshift.amazonaws.com",
                "different-workgroup",
                "different-workgroup",
            ),
        ],
    )
    def test_get_conn_rds_iam_redshift_serverless(
        self,
        mocker,
        mock_connect,
        aws_conn_id,
        port,
        host,
        conn_workgroup_name,
        expected_workgroup_name,
    ):
        mock_aws_hook_class = mocker.patch("airflow.providers.amazon.aws.hooks.base_aws.AwsBaseHook")

        mock_conn_extra = {
            "iam": True,
            "redshift-serverless": True,
        }
        if aws_conn_id is not NOTSET:
            mock_conn_extra["aws_conn_id"] = aws_conn_id
        if conn_workgroup_name is not NOTSET:
            mock_conn_extra["workgroup-name"] = conn_workgroup_name

        self.connection.extra = json.dumps(mock_conn_extra)
        self.connection.host = host
        self.connection.port = port
        mock_db_user = f"IAM:{self.connection.login}"
        mock_db_pass = "aws_token"

        # Mock AWS Connection
        mock_aws_hook_instance = mock_aws_hook_class.return_value
        mock_client = mocker.MagicMock()
        mock_client.get_credentials.return_value = {
            "dbPassword": mock_db_pass,
            "dbUser": mock_db_user,
        }
        type(mock_aws_hook_instance).conn = mocker.PropertyMock(return_value=mock_client)

        self.db_hook.get_conn()
        # Check AwsHook initialization
        mock_aws_hook_class.assert_called_once_with(
            # If aws_conn_id not set than fallback to aws_default
            aws_conn_id=aws_conn_id if aws_conn_id is not NOTSET else "aws_default",
            client_type="redshift-serverless",
        )
        # Check boto3 'redshift' client method `get_cluster_credentials` call args
        mock_client.get_credentials.assert_called_once_with(
            dbName=self.connection.schema,
            workgroupName=expected_workgroup_name,
        )
        # Check expected psycopg2 connection call args
        mock_connect.assert_called_once_with(
            user=mock_db_user,
            password=mock_db_pass,
            host=host,
            dbname=self.connection.schema,
            port=(port or 5439),
        )

    def test_get_conn_azure_iam(self, mocker, mock_connect):
        mock_azure_conn_id = "azure_conn1"
        mock_db_token = "azure_token1"
        mock_conn_extra = {"iam": True, "azure_conn_id": mock_azure_conn_id}
        self.connection.extra = json.dumps(mock_conn_extra)

        mock_connection_class = mocker.patch("airflow.providers.postgres.hooks.postgres.Connection")
        mock_azure_base_hook = mock_connection_class.get.return_value.get_hook.return_value
        mock_azure_base_hook.get_token.return_value.token = mock_db_token

        self.db_hook.get_conn()

        # Check AzureBaseHook initialization and get_token call args
        mock_connection_class.get.assert_called_once_with(mock_azure_conn_id)
        mock_azure_base_hook.get_token.assert_called_once_with(PostgresHook.default_azure_oauth_scope)

        # Check expected psycopg2 connection call args
        mock_connect.assert_called_once_with(
            user=self.connection.login,
            password=mock_db_token,
            host=self.connection.host,
            dbname=self.connection.schema,
            port=(self.connection.port or 5432),
        )

        assert mock_db_token in self.db_hook.sqlalchemy_url

    def test_get_azure_iam_token_expect_failure_on_older_azure_provider_package(self, mocker):
        class MockAzureBaseHookOldVersion:
            """Simulate an old version of AzureBaseHook where sdk_client is required."""

            def __init__(self, sdk_client, conn_id="azure_default"):
                pass

        azure_conn_id = "azure_test_conn"
        mock_connection_class = mocker.patch("airflow.providers.postgres.hooks.postgres.Connection")
        mock_connection_class.get.return_value.get_hook = MockAzureBaseHookOldVersion

        self.connection.extra = json.dumps({"iam": True, "azure_conn_id": azure_conn_id})
        with pytest.raises(
            AirflowOptionalProviderFeatureException,
            match=(
                "Getting azure token is not supported.*"
                "Please upgrade apache-airflow-providers-microsoft-azure>="
            ),
        ):
            self.db_hook.get_azure_iam_token(self.connection)

        # Check AzureBaseHook initialization
        mock_connection_class.get.assert_called_once_with(azure_conn_id)

    def test_get_uri_from_connection_without_database_override(self, mocker):
        expected: str = f"postgresql{'+psycopg' if USE_PSYCOPG3 else ''}://login:password@host:1/database"
        self.db_hook.get_connection = mocker.MagicMock(
            return_value=Connection(
                conn_type="postgres",
                host="host",
                login="login",
                password="password",
                schema="database",
                port=1,
            )
        )
        assert self.db_hook.get_uri() == expected

    def test_get_uri_from_connection_with_database_override(self, mocker):
        expected: str = (
            f"postgresql{'+psycopg' if USE_PSYCOPG3 else ''}://login:password@host:1/database-override"
        )
        hook = PostgresHook(database="database-override")
        hook.get_connection = mocker.MagicMock(
            return_value=Connection(
                conn_type="postgres",
                host="host",
                login="login",
                password="password",
                schema="database",
                port=1,
            )
        )
        assert hook.get_uri() == expected


@pytest.mark.skipif(USE_PSYCOPG3, reason="psycopg v3 is available")
class TestPostgresHookConnPPG2:
    """PostgresHookConn tests that are specific to psycopg2."""

    def setup_method(self):
        self.connection = Connection(login="login", password="password", host="host", schema="database")

        class UnitTestPostgresHook(PostgresHook):
            conn_name_attr = "test_conn_id"

        self.db_hook = UnitTestPostgresHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    def test_sqlalchemy_url(self):
        conn = Connection(login="login-conn", password="password-conn", host="host", schema="database")
        hook = PostgresHook(connection=conn)
        expected = "postgresql://login-conn:password-conn@host/database"
        if SQLALCHEMY_V_1_4:
            assert str(hook.sqlalchemy_url) == expected
        else:
            assert hook.sqlalchemy_url.render_as_string(hide_password=False) == expected

    def test_sqlalchemy_url_with_sqlalchemy_query(self):
        conn = Connection(
            login="login-conn",
            password="password-conn",
            host="host",
            schema="database",
            extra=dict(sqlalchemy_query={"gssencmode": "disable"}),
        )
        hook = PostgresHook(connection=conn)

        expected = "postgresql://login-conn:password-conn@host/database?gssencmode=disable"
        if SQLALCHEMY_V_1_4:
            assert str(hook.sqlalchemy_url) == expected
        else:
            assert hook.sqlalchemy_url.render_as_string(hide_password=False) == expected

    def test_get_conn_cursor(self, mock_connect):
        self.connection.extra = '{"cursor": "dictcursor", "sqlalchemy_query": {"gssencmode": "disable"}}'
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            cursor_factory=psycopg2.extras.DictCursor,
            user="login",
            password="password",
            host="host",
            dbname="database",
            port=None,
        )


@pytest.mark.skipif(not USE_PSYCOPG3, reason="psycopg v3 or sqlalchemy v2 not available")
class TestPostgresHookConnPPG3:
    """PostgresHookConn tests that are specific to psycopg3."""

    def setup_method(self):
        self.connection = Connection(login="login", password="password", host="host", schema="database")

        class UnitTestPostgresHook(PostgresHook):
            conn_name_attr = "test_conn_id"

        self.db_hook = UnitTestPostgresHook()
        self.db_hook.get_connection = mock.Mock()
        self.db_hook.get_connection.return_value = self.connection

    def test_sqlalchemy_url(self):
        conn = Connection(login="login-conn", password="password-conn", host="host", schema="database")
        hook = PostgresHook(connection=conn)
        expected = "postgresql+psycopg://login-conn:password-conn@host/database"
        if SQLALCHEMY_V_1_4:
            assert str(hook.sqlalchemy_url) == expected
        else:
            assert hook.sqlalchemy_url.render_as_string(hide_password=False) == expected

    def test_sqlalchemy_url_with_sqlalchemy_query(self):
        conn = Connection(
            login="login-conn",
            password="password-conn",
            host="host",
            schema="database",
            extra=dict(sqlalchemy_query={"gssencmode": "disable"}),
        )
        hook = PostgresHook(connection=conn)

        expected = "postgresql+psycopg://login-conn:password-conn@host/database?gssencmode=disable"
        if SQLALCHEMY_V_1_4:
            assert str(hook.sqlalchemy_url) == expected
        else:
            assert hook.sqlalchemy_url.render_as_string(hide_password=False) == expected

    def test_get_conn_cursor(self, mocker):
        mock_connect = mocker.patch("psycopg.connection.Connection.connect")
        self.connection.extra = '{"cursor": "dictcursor", "sqlalchemy_query": {"gssencmode": "disable"}}'
        self.db_hook.get_conn()
        mock_connect.assert_called_once_with(
            row_factory=psycopg.rows.dict_row,
            user="login",
            password="password",
            host="host",
            dbname="database",
            port=None,
        )


@pytest.mark.backend("postgres")
class TestPostgresHook:
    """Tests that are identical between psycopg2 and psycopg3."""

    table = "test_postgres_hook_table"

    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur

        class UnitTestPostgresHook(PostgresHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestPostgresHook()

    def teardown_method(self):
        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.table}")

    def test_bulk_load(self, tmp_path):
        hook = PostgresHook()
        input_data = ["foo", "bar", "baz"]

        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {self.table} (c VARCHAR)")
            conn.commit()

            path = tmp_path / "testfile"
            path.write_text("\n".join(input_data))
            hook.bulk_load(self.table, os.fspath(path))

            cur.execute(f"SELECT * FROM {self.table}")
            results = [row[0] for row in cur.fetchall()]

        assert sorted(input_data) == sorted(results)

    def test_bulk_dump(self, tmp_path):
        hook = PostgresHook()
        input_data = ["foo", "bar", "baz"]

        with hook.get_conn() as conn, conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {self.table} (c VARCHAR)")
            values = ",".join(f"('{data}')" for data in input_data)
            cur.execute(f"INSERT INTO {self.table} VALUES {values}")
            conn.commit()

        path = tmp_path / "testfile"
        hook.bulk_dump(self.table, os.fspath(path))
        results = [line.rstrip() for line in path.read_text().splitlines()]

        assert sorted(input_data) == sorted(results)

    @pytest.mark.parametrize(
        ("df_type", "expected_type"),
        [
            ("pandas", pd.DataFrame),
            ("polars", pl.DataFrame),
        ],
    )
    def test_get_df_with_df_type(self, mocker, df_type, expected_type):
        mock_polars_df = mocker.patch("airflow.providers.postgres.hooks.postgres.PostgresHook._get_polars_df")
        mock_read_sql = mocker.patch("pandas.io.sql.read_sql")
        mock_get_engine = mocker.patch(
            "airflow.providers.postgres.hooks.postgres.PostgresHook.get_sqlalchemy_engine"
        )

        hook = mock_db_hook(PostgresHook)
        mock_read_sql.return_value = pd.DataFrame()
        mock_polars_df.return_value = pl.DataFrame()
        sql = "SELECT * FROM table"
        if df_type == "pandas":
            mock_conn = mocker.MagicMock()
            mock_engine = mocker.MagicMock()
            mock_engine.connect.return_value.__enter__.return_value = mock_conn
            mock_get_engine.return_value = mock_engine
            df = hook.get_df(sql, df_type="pandas")
            mock_read_sql.assert_called_once_with(sql, con=mock_conn, params=None)
            assert isinstance(df, expected_type)
        elif df_type == "polars":
            df = hook.get_df(sql, df_type="polars")
            mock_polars_df.assert_called_once_with(sql, None)
            assert isinstance(df, expected_type)

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

    def test_reserved_words(self):
        hook = PostgresHook()
        assert hook.reserved_words == sqlalchemy.dialects.postgresql.base.RESERVED_WORDS

    def test_generate_insert_sql_without_already_escaped_column_name(self):
        values = [
            "1",
            "mssql_conn",
            "mssql",
            "MSSQL connection",
            "localhost",
            "airflow",
            "admin",
            "admin",
            1433,
            False,
            False,
            {},
        ]
        target_fields = [
            "id",
            "conn_id",
            "conn_type",
            "description",
            "host",
            "schema",
            "login",
            "password",
            "port",
            "is_encrypted",
            "is_extra_encrypted",
            "extra",
        ]
        hook = PostgresHook()
        assert hook._generate_insert_sql(
            table="connection", values=values, target_fields=target_fields
        ) == INSERT_SQL_STATEMENT.format("schema")

    def test_generate_insert_sql_with_already_escaped_column_name(self):
        values = [
            "1",
            "mssql_conn",
            "mssql",
            "MSSQL connection",
            "localhost",
            "airflow",
            "admin",
            "admin",
            1433,
            False,
            False,
            {},
        ]
        target_fields = [
            "id",
            "conn_id",
            "conn_type",
            "description",
            "host",
            '"schema"',
            "login",
            "password",
            "port",
            "is_encrypted",
            "is_extra_encrypted",
            "extra",
        ]
        hook = PostgresHook()
        assert hook._generate_insert_sql(
            table="connection", values=values, target_fields=target_fields
        ) == INSERT_SQL_STATEMENT.format('"schema"')


@pytest.mark.backend("postgres")
@pytest.mark.skipif(USE_PSYCOPG3, reason="psycopg v3 is available")
class TestPostgresHookPPG2:
    """PostgresHook tests that are specific to psycopg2."""

    table = "test_postgres_hook_table"

    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur

        class UnitTestPostgresHook(PostgresHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestPostgresHook()

    def teardown_method(self):
        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.table}")

    def test_copy_expert(self, mocker):
        open_mock = mocker.mock_open(read_data='{"some": "json"}')
        mocker.patch("airflow.providers.postgres.hooks.postgres.open", open_mock)

        statement = "SQL"
        filename = "filename"

        self.cur.fetchall.return_value = None

        assert self.db_hook.copy_expert(statement, filename) is None

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1
        assert self.conn.commit.call_count == 1
        self.cur.copy_expert.assert_called_once_with(statement, open_mock.return_value)
        assert open_mock.call_args.args == (filename, "r+")

    def test_insert_rows(self, postgres_hook_setup):
        setup = postgres_hook_setup
        table = "table"
        rows = [("hello",), ("world",)]

        setup.db_hook.insert_rows(table, rows)

        assert setup.conn.close.call_count == 1
        assert setup.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == setup.conn.commit.call_count

        sql = f"INSERT INTO {table}  VALUES (%s)"
        setup.cur.executemany.assert_any_call(sql, rows)

    @mock.patch("airflow.providers.postgres.hooks.postgres.execute_batch")
    def test_insert_rows_fast_executemany(self, mock_execute_batch, postgres_hook_setup):
        setup = postgres_hook_setup
        table = "table"
        rows = [("hello",), ("world",)]

        setup.db_hook.insert_rows(table, rows, fast_executemany=True)

        assert setup.conn.close.call_count == 1
        assert setup.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert setup.conn.commit.call_count == commit_count

        mock_execute_batch.assert_called_once_with(
            setup.cur,
            f"INSERT INTO {table}  VALUES (%s)",  # expected SQL
            [("hello",), ("world",)],  # expected values
            page_size=1000,
        )

        # executemany should NOT be called in this mode
        setup.cur.executemany.assert_not_called()

    def test_insert_rows_replace(self, postgres_hook_setup):
        setup = postgres_hook_setup
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

        setup.db_hook.insert_rows(table, rows, fields, replace=True, replace_index=fields[0])

        assert setup.conn.close.call_count == 1
        assert setup.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == setup.conn.commit.call_count

        sql = (
            f"INSERT INTO {table} ({fields[0]}, {fields[1]}) VALUES (%s,%s) "
            f"ON CONFLICT ({fields[0]}) DO UPDATE SET {fields[1]} = excluded.{fields[1]}"
        )
        setup.cur.executemany.assert_any_call(sql, rows)

    def test_insert_rows_replace_missing_target_field_arg(self, postgres_hook_setup):
        setup = postgres_hook_setup
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
        with pytest.raises(ValueError, match="PostgreSQL ON CONFLICT upsert syntax requires column names"):
            setup.db_hook.insert_rows(table, rows, replace=True, replace_index=fields[0])

    def test_insert_rows_replace_missing_replace_index_arg(self, postgres_hook_setup):
        setup = postgres_hook_setup
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
        with pytest.raises(ValueError, match="PostgreSQL ON CONFLICT upsert syntax requires an unique index"):
            setup.db_hook.insert_rows(table, rows, fields, replace=True)

    def test_insert_rows_replace_all_index(self, postgres_hook_setup):
        setup = postgres_hook_setup
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

        setup.db_hook.insert_rows(table, rows, fields, replace=True, replace_index=fields)

        assert setup.conn.close.call_count == 1
        assert setup.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == setup.conn.commit.call_count

        sql = (
            f"INSERT INTO {table} ({', '.join(fields)}) VALUES (%s,%s) "
            f"ON CONFLICT ({', '.join(fields)}) DO NOTHING"
        )
        setup.cur.executemany.assert_any_call(sql, rows)

    @pytest.mark.usefixtures("reset_logging_config")
    def test_get_all_db_log_messages(self, mocker):
        messages = ["a", "b", "c"]

        class FakeLogger:
            notices = messages

        # Mock the logger
        mock_logger = mocker.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.log")

        hook = PostgresHook(enable_log_db_messages=True)
        hook.get_db_log_messages(FakeLogger)

        # Verify that info was called for each message
        for msg in messages:
            assert mocker.call(msg) in mock_logger.info.mock_calls

    @pytest.mark.usefixtures("reset_logging_config")
    def test_log_db_messages_by_db_proc(self, mocker):
        proc_name = "raise_notice"
        notice_proc = f"""
        CREATE PROCEDURE {proc_name} (s text) LANGUAGE PLPGSQL AS
        $$
        BEGIN
            raise notice 'Message from db: %', s;
        END;
        $$;
        """

        # Mock the logger
        mock_logger = mocker.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.log")

        hook = PostgresHook(enable_log_db_messages=True)
        try:
            hook.run(sql=notice_proc)
            hook.run(sql=f"call {proc_name}('42')")

            # Check if the notice message was logged
            assert mocker.call("NOTICE:  Message from db: 42\n") in mock_logger.info.mock_calls
        finally:
            hook.run(sql=f"DROP PROCEDURE {proc_name} (s text)")

    def test_dialect_name(self, postgres_hook_setup):
        setup = postgres_hook_setup
        assert setup.db_hook.dialect_name == "postgresql"

    def test_dialect(self, postgres_hook_setup):
        setup = postgres_hook_setup
        assert isinstance(setup.db_hook.dialect, PostgresDialect)


@pytest.mark.backend("postgres")
@pytest.mark.skipif(not USE_PSYCOPG3, reason="psycopg v3 or sqlalchemy v2 are not available")
class TestPostgresHookPPG3:
    """PostgresHook tests that are specific to psycopg3."""

    table = "test_postgres_hook_table"

    def setup_method(self):
        self.cur = mock.MagicMock(rowcount=0)
        self.conn = conn = mock.MagicMock()
        self.conn.cursor.return_value = self.cur

        class UnitTestPostgresHook(PostgresHook):
            conn_name_attr = "test_conn_id"

            def get_conn(self):
                return conn

        self.db_hook = UnitTestPostgresHook()

    def teardown_method(self):
        with PostgresHook().get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {self.table}")

    def test_copy_expert_from(self, mocker):
        """Tests copy_expert with a 'COPY FROM STDIN' operation."""
        statement = "COPY test_table FROM STDIN"
        filename = "filename"

        m_open = mocker.mock_open()
        mocker.patch("airflow.providers.postgres.hooks.postgres.open", m_open)
        mocker.patch("os.path.isfile", return_value=True)  # Mock file exists

        # Configure the file handle for reading
        m_open.return_value.read.side_effect = [b'{"some": "json"}', b""]

        # Set up the context manager chain properly
        # self.conn needs to support context manager
        self.conn.__enter__ = mocker.Mock(return_value=self.conn)
        self.conn.__exit__ = mocker.Mock(return_value=None)

        # cursor() returns something that also supports context manager
        mock_cursor = mocker.MagicMock()
        mock_cursor.__enter__ = mocker.Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = mocker.Mock(return_value=None)
        self.conn.cursor.return_value = mock_cursor

        # cursor.copy() returns a context manager
        mock_copy = mocker.MagicMock()
        mock_copy.__enter__ = mocker.Mock(return_value=mock_copy)
        mock_copy.__exit__ = mocker.Mock(return_value=None)
        mock_cursor.copy.return_value = mock_copy

        # Call the method under test
        self.db_hook.copy_expert(statement, filename)

        # Assert that the file was opened for reading in binary mode
        m_open.assert_any_call(filename, "rb")

        # Assert write was called with the data
        mock_copy.write.assert_called_once_with(b'{"some": "json"}')
        self.conn.commit.assert_called_once()

    def test_copy_expert_to(self, mocker):
        """Tests copy_expert with a 'COPY TO STDOUT' operation."""
        statement = "COPY test_table TO STDOUT"
        filename = "filename"

        m_open = mocker.mock_open()
        mocker.patch("airflow.providers.postgres.hooks.postgres.open", m_open)

        # Set up the context manager chain properly
        # self.conn needs to support context manager
        self.conn.__enter__ = mocker.Mock(return_value=self.conn)
        self.conn.__exit__ = mocker.Mock(return_value=None)

        # cursor() returns something that also supports context manager
        mock_cursor = mocker.MagicMock()
        mock_cursor.__enter__ = mocker.Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = mocker.Mock(return_value=None)
        self.conn.cursor.return_value = mock_cursor

        # cursor.copy() returns a context manager that is iterable
        mock_copy = mocker.MagicMock()
        mock_copy.__enter__ = mocker.Mock(return_value=mock_copy)
        mock_copy.__exit__ = mocker.Mock(return_value=None)
        mock_copy.__iter__ = mocker.Mock(return_value=iter([b"db_data_1", b"db_data_2"]))
        mock_cursor.copy.return_value = mock_copy

        # Call the method under test
        self.db_hook.copy_expert(statement, filename)

        # Assert that the file was opened for writing in binary mode
        m_open.assert_any_call(filename, "wb")

        # Assert that the data from the DB was written to the file
        handle = m_open.return_value
        handle.write.assert_has_calls(
            [
                mocker.call(b"db_data_1"),
                mocker.call(b"db_data_2"),
            ]
        )
        self.conn.commit.assert_called_once()

    def test_insert_rows(self):
        table = "table"
        rows = [("hello",), ("world",)]

        self.db_hook.insert_rows(table, rows)

        assert self.conn.close.call_count == 1
        assert self.cur.close.call_count == 1

        commit_count = 2  # The first and last commit
        assert commit_count == self.conn.commit.call_count

        sql = f"INSERT INTO {table}  VALUES (%s)"
        self.cur.executemany.assert_any_call(sql, rows)

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
            f"INSERT INTO {table} ({fields[0]}, {fields[1]}) VALUES (%s,%s) "
            f"ON CONFLICT ({fields[0]}) DO UPDATE SET {fields[1]} = excluded.{fields[1]}"
        )
        self.cur.executemany.assert_any_call(sql, rows)

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
        with pytest.raises(ValueError, match="PostgreSQL ON CONFLICT upsert syntax requires column names"):
            self.db_hook.insert_rows(table, rows, replace=True, replace_index=fields[0])

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
        with pytest.raises(ValueError, match="PostgreSQL ON CONFLICT upsert syntax requires an unique index"):
            self.db_hook.insert_rows(table, rows, fields, replace=True)

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
        self.cur.executemany.assert_any_call(sql, rows)

    @pytest.mark.skip(reason="Notice handling is callback-based in psycopg3 and cannot be tested this way.")
    def test_get_all_db_log_messages(self, mocker):
        pass

    @pytest.mark.usefixtures("reset_logging_config")
    def test_log_db_messages_by_db_proc(self, mocker):
        proc_name = "raise_notice"
        notice_proc = f"""
        CREATE PROCEDURE {proc_name} (s text) LANGUAGE PLPGSQL AS
        $$
        BEGIN
            raise notice 'Message from db: %', s;
        END;
        $$;
        """

        # Mock the logger
        mock_logger = mocker.patch("airflow.providers.postgres.hooks.postgres.PostgresHook.log")

        hook = PostgresHook(enable_log_db_messages=True)
        try:
            hook.run(sql=notice_proc)
            hook.run(sql=f"call {proc_name}('42')")

            # Check if the notice message was logged
            mock_logger.info.assert_any_call("Message from db: 42")
        finally:
            hook.run(sql=f"DROP PROCEDURE {proc_name} (s text)")

    def test_dialect_name(self):
        assert self.db_hook.dialect_name == "postgresql"

    def test_dialect(self):
        assert isinstance(self.db_hook.dialect, PostgresDialect)
