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
"""
Unit tests for external connection utilities.
"""

from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from airflow.providers.greatexpectations.common.external_connections import (
    SnowflakeKeyConnection,
    build_aws_connection_string,
    build_gcpbigquery_connection_string,
    build_mssql_connection_string,
    build_mysql_connection_string,
    build_postgres_connection_string,
    build_redshift_connection_string,
    build_snowflake_connection_string,
    build_snowflake_key_connection,
    build_sqlite_connection_string,
)


class TestRedshiftConnectionString:
    """Test class for Redshift connection string."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_redshift_connection_string_success(self, mock_get_connection):
        """Test successful Redshift connection string creation."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "redshift-cluster.amazonaws.com"
        mock_conn.port = 5439
        mock_conn.schema = "public"
        mock_get_connection.return_value = mock_conn

        result = build_redshift_connection_string("test_conn")

        expected = "redshift+psycopg2://user:pass@redshift-cluster.amazonaws.com:5439/public"
        assert result == expected
        mock_get_connection.assert_called_once_with("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_redshift_connection_string_with_schema_override(self, mock_get_connection):
        """Test Redshift connection string with schema override."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "redshift-cluster.amazonaws.com"
        mock_conn.port = 5439
        mock_conn.schema = "public"
        mock_get_connection.return_value = mock_conn

        result = build_redshift_connection_string("test_conn", schema="analytics")

        expected = "redshift+psycopg2://user:pass@redshift-cluster.amazonaws.com:5439/analytics"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_redshift_connection_string_no_connection(self, mock_get_connection):
        """Test Redshift connection string when connection doesn't exist."""
        mock_get_connection.return_value = None

        with pytest.raises(
            ValueError,
            match="Failed to retrieve Airflow connection with conn_id: test_conn",
        ):
            build_redshift_connection_string("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_redshift_connection_string_no_schema(self, mock_get_connection):
        """Test Redshift connection string when no schema is provided."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "redshift-cluster.amazonaws.com"
        mock_conn.port = 5439
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        with pytest.raises(
            ValueError,
            match="Schema/database name is required for Redshift connection: test_conn",
        ):
            build_redshift_connection_string("test_conn")


class TestMySQLConnectionString:
    """Test class for MySQL connection string."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_mysql_connection_string_success(self, mock_get_connection):
        """Test successful MySQL connection string creation."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mysql-server.com"
        mock_conn.port = 3306
        mock_conn.schema = "mydb"
        mock_get_connection.return_value = mock_conn

        result = build_mysql_connection_string("test_conn")

        expected = "mysql://user:pass@mysql-server.com:3306/mydb"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_mysql_connection_string_no_schema(self, mock_get_connection):
        """Test MySQL connection string when no schema is provided."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mysql-server.com"
        mock_conn.port = 3306
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        with pytest.raises(
            ValueError,
            match="Schema/database name is required for MySQL connection: test_conn",
        ):
            build_mysql_connection_string("test_conn")


class TestMSSQLConnectionString:
    """Test class for Microsoft SQL Server connection string."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_mssql_connection_string_success(self, mock_get_connection):
        """Test successful MSSQL connection string creation."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mssql-server.com"
        mock_conn.port = 1433
        mock_conn.schema = "mydb"
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        result = build_mssql_connection_string("test_conn")

        expected = "mssql+pyodbc://user:pass@mssql-server.com:1433/mydb?driver=ODBC Driver 17 for SQL Server"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_mssql_connection_string_custom_driver(self, mock_get_connection):
        """Test MSSQL connection string with custom driver."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mssql-server.com"
        mock_conn.port = 1433
        mock_conn.schema = "mydb"
        mock_conn.extra_dejson = {"driver": "ODBC Driver 18 for SQL Server"}
        mock_get_connection.return_value = mock_conn

        result = build_mssql_connection_string("test_conn")

        expected = "mssql+pyodbc://user:pass@mssql-server.com:1433/mydb?driver=ODBC Driver 18 for SQL Server"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_mssql_connection_string_no_schema_defaults_to_master(self, mock_get_connection):
        """Test MSSQL connection string defaults to master when no schema."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "mssql-server.com"
        mock_conn.port = 1433
        mock_conn.schema = None
        mock_conn.extra_dejson = {}
        mock_get_connection.return_value = mock_conn

        result = build_mssql_connection_string("test_conn")

        expected = (
            "mssql+pyodbc://user:pass@mssql-server.com:1433/master?driver=ODBC Driver 17 for SQL Server"
        )
        assert result == expected


class TestPostgresConnectionString:
    """Test class for PostgreSQL connection string."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_postgres_connection_string_success(self, mock_get_connection):
        """Test successful PostgreSQL connection string creation."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "postgres-server.com"
        mock_conn.port = 5432
        mock_conn.schema = "mydb"
        mock_get_connection.return_value = mock_conn

        result = build_postgres_connection_string("test_conn")

        expected = "postgresql+psycopg2://user:pass@postgres-server.com:5432/mydb"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_postgres_connection_string_no_schema(self, mock_get_connection):
        """Test PostgreSQL connection string when no schema is provided."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.host = "postgres-server.com"
        mock_conn.port = 5432
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        with pytest.raises(ValueError, match="Specify the name of the database in the schema parameter"):
            build_postgres_connection_string("test_conn")


class TestSnowflakeConnectionString:
    """Test class for Snowflake connection string."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_connection_string_from_hook"
    )
    def test_build_snowflake_connection_string_hook_success(self, mock_hook_build, mock_get_connection):
        """Test successful Snowflake connection string using hook."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.return_value = "snowflake://user:pass@account/db/schema"

        result = build_snowflake_connection_string("test_conn")

        assert result == "snowflake://user:pass@account/db/schema"
        mock_hook_build.assert_called_once_with("test_conn", None)

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_connection_string_from_hook"
    )
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_connection_string_manual"
    )
    def test_build_snowflake_connection_string_fallback_to_manual(
        self, mock_manual_build, mock_hook_build, mock_get_connection
    ):
        """Test Snowflake connection string falls back to manual when hook fails."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ImportError("Snowflake provider not found")
        mock_manual_build.return_value = "snowflake://user:pass@account/db/schema"

        result = build_snowflake_connection_string("test_conn")

        assert result == "snowflake://user:pass@account/db/schema"
        mock_manual_build.assert_called_once_with(mock_conn, None)

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_snowflake_connection_string_no_connection(self, mock_get_connection):
        """Test Snowflake connection string when connection doesn't exist."""
        mock_get_connection.return_value = None

        with pytest.raises(
            ValueError,
            match="Failed to retrieve Airflow connection with conn_id: test_conn",
        ):
            build_snowflake_connection_string("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_connection_string_from_hook"
    )
    def test_build_snowflake_connection_string_with_schema_override(
        self, mock_hook_build, mock_get_connection
    ):
        """Test Snowflake connection string with schema override."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.return_value = "snowflake://user:pass@account/db/custom_schema"

        result = build_snowflake_connection_string("test_conn", schema="custom_schema")

        assert result == "snowflake://user:pass@account/db/custom_schema"
        mock_hook_build.assert_called_once_with("test_conn", "custom_schema")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_connection_string_from_hook"
    )
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_connection_string_manual"
    )
    def test_build_snowflake_connection_string_manual_success(
        self, mock_manual_build, mock_hook_build, mock_get_connection
    ):
        """Test successful Snowflake connection string with manual construction."""
        mock_conn = Mock()
        mock_conn.login = "user"
        mock_conn.password = "pass"
        mock_conn.schema = "test_schema"
        mock_conn.extra_dejson = {
            "account": "test_account",
            "database": "test_db",
            "warehouse": "test_wh",
        }
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ImportError("Snowflake provider not found")
        mock_manual_build.return_value = (
            "snowflake://user:pass@test_account/test_db/test_schema?warehouse=test_wh"
        )

        result = build_snowflake_connection_string("test_conn")

        assert result == "snowflake://user:pass@test_account/test_db/test_schema?warehouse=test_wh"
        mock_manual_build.assert_called_once_with(mock_conn, None)


class TestSnowflakeKeyConnection:
    """Test class for Snowflake key connection."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_key_connection_from_hook"
    )
    def test_build_snowflake_key_connection_success(self, mock_hook_build, mock_get_connection):
        """Test successful Snowflake key connection."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.return_value = SnowflakeKeyConnection(
            account="test_account",
            user="test_user",
            role="test_role",
            warehouse="test_warehouse",
            database="test_database",
            schema="test_schema",
            private_key=b"private_key_bytes",
        )

        result = build_snowflake_key_connection("test_conn")

        assert isinstance(result, SnowflakeKeyConnection)
        assert result.type == "key"
        assert result.account == "test_account"
        assert result.user == "test_user"
        assert result.role == "test_role"
        assert result.warehouse == "test_warehouse"
        assert result.database == "test_database"
        assert result.schema_name == "test_schema"
        assert result.private_key == b"private_key_bytes"
        mock_hook_build.assert_called_once_with("test_conn", None)

    @patch("airflow.providers.snowflake.hooks.snowflake.SnowflakeHook")
    def test_build_snowflake_key_connection_no_connection(self, mock_hook_class):
        """Test Snowflake key connection when connection doesn't exist."""
        mock_hook_instance = Mock()
        mock_hook_class.return_value = mock_hook_instance
        mock_hook_instance.get_connection.return_value = None

        with pytest.raises(
            ValueError,
            match="Failed to retrieve Airflow connection with conn_id: test_conn",
        ):
            build_snowflake_key_connection("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_key_connection_from_hook"
    )
    def test_build_snowflake_key_connection_with_schema_override(self, mock_hook_build, mock_get_connection):
        """Test Snowflake key connection with schema override."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.return_value = SnowflakeKeyConnection(
            account="test_account",
            user="test_user",
            role="test_role",
            warehouse="test_warehouse",
            database="test_database",
            schema="custom_schema",
            private_key=b"private_key_bytes",
        )

        result = build_snowflake_key_connection("test_conn", schema="custom_schema")

        assert isinstance(result, SnowflakeKeyConnection)
        assert result.schema_name == "custom_schema"
        mock_hook_build.assert_called_once_with("test_conn", "custom_schema")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_key_connection_from_hook"
    )
    def test_build_snowflake_key_connection_missing_private_key(self, mock_hook_build, mock_get_connection):
        """Test Snowflake key connection when private key is missing."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ValueError(
            "Private key file is required for key-based authentication: test_conn"
        )

        with pytest.raises(
            ValueError,
            match="Private key file is required for key-based authentication: test_conn",
        ):
            build_snowflake_key_connection("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_key_connection_from_hook"
    )
    def test_build_snowflake_key_connection_missing_account(self, mock_hook_build, mock_get_connection):
        """Test Snowflake key connection when account is missing."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ValueError(
            "Snowflake account is required in connection extras for conn_id: test_conn"
        )

        with pytest.raises(
            ValueError,
            match="Snowflake account is required in connection extras for conn_id: test_conn",
        ):
            build_snowflake_key_connection("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_key_connection_from_hook"
    )
    def test_build_snowflake_key_connection_missing_database(self, mock_hook_build, mock_get_connection):
        """Test Snowflake key connection when database is missing."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ValueError(
            "Snowflake database is required in connection extras for conn_id: test_conn"
        )

        with pytest.raises(
            ValueError,
            match="Snowflake database is required in connection extras for conn_id: test_conn",
        ):
            build_snowflake_key_connection("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_key_connection_from_hook"
    )
    def test_build_snowflake_key_connection_missing_warehouse(self, mock_hook_build, mock_get_connection):
        """Test Snowflake key connection when warehouse is missing."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ValueError(
            "Snowflake warehouse is required in connection extras for conn_id: test_conn"
        )

        with pytest.raises(
            ValueError,
            match="Snowflake warehouse is required in connection extras for conn_id: test_conn",
        ):
            build_snowflake_key_connection("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_key_connection_from_hook"
    )
    def test_build_snowflake_key_connection_missing_schema(self, mock_hook_build, mock_get_connection):
        """Test Snowflake key connection when schema is missing."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ValueError("Schema is required for Snowflake connection: test_conn")

        with pytest.raises(
            ValueError,
            match="Schema is required for Snowflake connection: test_conn",
        ):
            build_snowflake_key_connection("test_conn")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    @patch(
        "airflow.providers.greatexpectations.common.external_connections._build_snowflake_key_connection_from_hook"
    )
    def test_build_snowflake_key_connection_import_error(self, mock_hook_build, mock_get_connection):
        """Test Snowflake key connection when SnowflakeHook is not available."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn
        mock_hook_build.side_effect = ImportError("Snowflake provider not found")

        with pytest.raises(
            ImportError,
            match="Snowflake provider not found",
        ):
            build_snowflake_key_connection("test_conn")


class TestGCPBigQueryConnectionString:
    """Test class for Google Cloud BigQuery connection string."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_gcpbigquery_connection_string_success(self, mock_get_connection):
        """Test successful BigQuery connection string creation."""
        mock_conn = Mock()
        mock_conn.host = "bigquery://project/"
        mock_conn.schema = "dataset"
        mock_get_connection.return_value = mock_conn

        result = build_gcpbigquery_connection_string("test_conn")

        expected = "bigquery://project/dataset"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_gcpbigquery_connection_string_no_schema(self, mock_get_connection):
        """Test BigQuery connection string with no schema."""
        mock_conn = Mock()
        mock_conn.host = "bigquery://project/"
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        result = build_gcpbigquery_connection_string("test_conn")

        expected = "bigquery://project/"
        assert result == expected


class TestSQLiteConnectionString:
    """Test class for SQLite connection string."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_sqlite_connection_string_success(self, mock_get_connection):
        """Test successful SQLite connection string creation."""
        mock_conn = Mock()
        mock_conn.host = "/path/to/database.db"
        mock_get_connection.return_value = mock_conn

        result = build_sqlite_connection_string("test_conn")

        expected = "sqlite:////path/to/database.db"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_sqlite_connection_string_no_connection(self, mock_get_connection):
        """Test SQLite connection string when connection doesn't exist."""
        mock_get_connection.return_value = None

        with pytest.raises(
            ValueError,
            match="Failed to retrieve Airflow connection with conn_id: test_conn",
        ):
            build_sqlite_connection_string("test_conn")


class TestAWSConnectionString:
    """Test class for AWS connection string."""

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_aws_connection_string_success_with_database(self, mock_get_connection):
        """Test successful AWS connection string creation with database."""
        mock_conn = Mock()
        mock_conn.schema = "default"
        mock_get_connection.return_value = mock_conn

        result = build_aws_connection_string(
            "test_conn",
            database="test_db",
            s3_path="s3://bucket/path/",
            region="us-east-1",
        )

        expected = "awsathena+rest://@athena.us-east-1.amazonaws.com/test_db?s3_staging_dir=s3://bucket/path/"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_aws_connection_string_success_without_database(self, mock_get_connection):
        """Test successful AWS connection string creation without database."""
        mock_conn = Mock()
        mock_conn.schema = None
        mock_get_connection.return_value = mock_conn

        result = build_aws_connection_string("test_conn", s3_path="s3://bucket/path/", region="us-east-1")

        expected = "awsathena+rest://@athena.us-east-1.amazonaws.com/?s3_staging_dir=s3://bucket/path/"
        assert result == expected

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_aws_connection_string_missing_s3_path(self, mock_get_connection):
        """Test AWS connection string with missing s3_path."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn

        with pytest.raises(ValueError, match="s3_path parameter is required for AWS connections"):
            build_aws_connection_string("test_conn", region="us-east-1")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_aws_connection_string_missing_region(self, mock_get_connection):
        """Test AWS connection string with missing region."""
        mock_conn = Mock()
        mock_get_connection.return_value = mock_conn

        with pytest.raises(ValueError, match="region parameter is required for AWS connections"):
            build_aws_connection_string("test_conn", s3_path="s3://bucket/path/")

    @patch("airflow.providers.greatexpectations.common.external_connections.BaseHook.get_connection")
    def test_build_aws_connection_string_schema_fallback(self, mock_get_connection):
        """Test AWS connection string uses schema as database fallback."""
        mock_conn = Mock()
        mock_conn.schema = "schema_db"
        mock_get_connection.return_value = mock_conn

        result = build_aws_connection_string(
            "test_conn",
            schema="schema_override",
            s3_path="s3://bucket/path/",
            region="us-east-1",
        )

        expected = "awsathena+rest://@athena.us-east-1.amazonaws.com/schema_override?s3_staging_dir=s3://bucket/path/"
        assert result == expected
