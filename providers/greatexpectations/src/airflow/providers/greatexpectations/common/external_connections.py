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
External connection utilities for building database connection configurations
from Airflow connections.
"""

import logging
from pathlib import Path
from typing import Literal, Optional, Union

try:  # airflow 3
    from airflow.sdk import BaseHook
    from airflow.sdk.definitions.connection import Connection
except ImportError:  # airflow 2
    from airflow.hooks.base import BaseHook  # type: ignore[attr-defined,no-redef]
    from airflow.models import (  # type: ignore[attr-defined,no-redef,assignment]
        Connection,
    )

from pydantic import BaseModel, Field
logger = logging.getLogger(__name__)


class SnowflakeUriConnection(BaseModel):
    """Pydantic model for URI-based Snowflake connection."""

    type: Literal["uri"] = "uri"
    connection_string: str


class SnowflakeKeyConnection(BaseModel):
    """Pydantic model for key-based Snowflake connection."""

    model_config = {"extra": "forbid"}

    type: Literal["key"] = "key"
    account: str
    user: str
    role: Optional[str] = None
    warehouse: str
    database: str
    schema_name: str = Field(..., alias="schema")
    private_key: bytes


def get_connection_by_id(conn_id: str) -> Connection:
    conn = BaseHook.get_connection(conn_id)
    if not conn:
        raise ValueError(
            f"Failed to retrieve Airflow connection with conn_id: {conn_id}"
        )
    return conn


def build_redshift_connection_string(conn_id: str, schema: Optional[str] = None) -> str:
    """
    Build connection string for Redshift connections.

    Args:
        conn_id: Airflow connection ID
        schema: Optional schema override

    Returns:
        Connection string for Redshift

    Raises:
        ValueError: If connection doesn't exist or is invalid
    """
    conn = get_connection_by_id(conn_id=conn_id)

    database_name = schema or conn.schema
    if not database_name:
        raise ValueError(
            f"Schema/database name is required for Redshift connection: {conn_id}"
        )

    return (
        f"redshift+psycopg2://{conn.login}:{conn.password}@"
        f"{conn.host}:{conn.port}/{database_name}"
    )


def build_mysql_connection_string(conn_id: str, schema: Optional[str] = None) -> str:
    """
    Build connection string for MySQL connections.

    Args:
        conn_id: Airflow connection ID
        schema: Optional schema override

    Returns:
        Connection string for MySQL

    Raises:
        ValueError: If connection doesn't exist or is invalid
    """
    conn = get_connection_by_id(conn_id=conn_id)

    database_name = schema or conn.schema
    if not database_name:
        raise ValueError(
            f"Schema/database name is required for MySQL connection: {conn_id}"
        )

    return (
        f"mysql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{database_name}"
    )


def build_mssql_connection_string(conn_id: str, schema: Optional[str] = None) -> str:
    """
    Build connection string for Microsoft SQL Server connections.

    Args:
        conn_id: Airflow connection ID
        schema: Optional schema override

    Returns:
        Connection string for MSSQL

    Raises:
        ValueError: If connection doesn't exist or is invalid
    """
    conn = get_connection_by_id(conn_id=conn_id)

    database_name = schema or conn.schema or "master"

    # Get driver from connection extras, default to ODBC Driver 17
    ms_driver = conn.extra_dejson.get("driver", "ODBC Driver 17 for SQL Server")
    driver_param = f"?driver={ms_driver}"

    return (
        f"mssql+pyodbc://{conn.login}:{conn.password}@"
        f"{conn.host}:{conn.port}/{database_name}{driver_param}"
    )


def build_postgres_connection_string(conn_id: str, schema: Optional[str] = None) -> str:
    """
    Build connection string for PostgreSQL connections.

    Args:
        conn_id: Airflow connection ID
        schema: Optional schema override (used as database name)

    Returns:
        Connection string for PostgreSQL

    Raises:
        ValueError: If connection doesn't exist or database name is missing
    """
    conn = get_connection_by_id(conn_id=conn_id)

    postgres_database = schema or conn.schema
    if not postgres_database:
        raise ValueError(
            "Specify the name of the database in the schema parameter of the Postgres connection. "
            "See: https://airflow.apache.org/docs/apache-airflow-providers-postgres/stable/connections/postgres.html"
        )

    return (
        f"postgresql+psycopg2://{conn.login}:{conn.password}@"
        f"{conn.host}:{conn.port}/{postgres_database}"
    )


def build_snowflake_connection_string(
    conn_id: str, schema: Optional[str] = None
) -> str:
    """
    Build connection string for Snowflake connections.
    Attempts to use SnowflakeHook first, falls back to manual construction.

    Args:
        conn_id: Airflow connection ID
        schema: Optional schema override

    Returns:
        Connection string for Snowflake

    Raises:
        ValueError: If connection doesn't exist or required parameters are missing
    """
    conn = get_connection_by_id(conn_id=conn_id)

    # Try to use SnowflakeHook first
    try:
        return _build_snowflake_connection_string_from_hook(conn_id, schema)
    except ImportError:
        logger.warning(
            "Snowflake provider package could not be imported, "
            "attempting to build connection uri from connection %s. "
            "Snowflake provider package is required for key-based auth, "
            "see: https://airflow.apache.org/docs/apache-airflow-providers-snowflake/stable/index.html",
            conn_id,
        )

    # Fallback to manual construction
    return _build_snowflake_connection_string_manual(conn, schema)


def build_snowflake_key_connection(
    conn_id: str, schema: Optional[str] = None
) -> SnowflakeKeyConnection:
    """
    Build key-based connection configuration for Snowflake connections.
    Requires SnowflakeHook and private key authentication.

    Args:
        conn_id: Airflow connection ID
        schema: Optional schema override

    Returns:
        SnowflakeKeyConnection containing key-based connection configuration

    Raises:
        ValueError: If connection doesn't exist, required parameters are missing, or private key is not configured
        ImportError: If SnowflakeHook is not available
    """
    conn = get_connection_by_id(conn_id=conn_id)

    # Key-based authentication requires SnowflakeHook
    return _build_snowflake_key_connection_from_hook(conn_id, schema)


def _build_snowflake_connection_string_from_hook(
    conn_id: str, schema: Optional[str] = None
) -> str:
    """Build Snowflake connection string using SnowflakeHook."""
    from airflow.providers.snowflake.hooks.snowflake import (  # type: ignore[import-not-found]
        SnowflakeHook,
    )

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    hook.schema = schema or hook.schema

    engine = hook.get_sqlalchemy_engine()
    url = engine.url.render_as_string(hide_password=False)
    return url


def _build_snowflake_key_connection_from_hook(
    conn_id: str, schema: Optional[str] = None
) -> SnowflakeKeyConnection:
    """Build Snowflake key-based connection using SnowflakeHook."""
    from airflow.providers.snowflake.hooks.snowflake import (  # type: ignore[import-not-found]
        SnowflakeHook,
    )
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    hook.schema = schema or hook.schema

    conn = hook.get_connection(conn_id)

    # Check for private key authentication
    private_key_file = conn.extra_dejson.get(
        "extra__snowflake__private_key_file"
    ) or conn.extra_dejson.get("private_key_file")

    if not private_key_file:
        raise ValueError(
            f"Private key file is required for key-based authentication: {conn_id}"
        )

    private_key_pem = Path(private_key_file).read_bytes()

    passphrase = None
    if conn.password:
        passphrase = conn.password.strip().encode()

    p_key = serialization.load_pem_private_key(
        private_key_pem, password=passphrase, backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    # Extract individual connection fields
    snowflake_account = conn.extra_dejson.get("account") or conn.extra_dejson.get(
        "extra__snowflake__account"
    )
    snowflake_database = conn.extra_dejson.get("database") or conn.extra_dejson.get(
        "extra__snowflake__database"
    )
    snowflake_warehouse = conn.extra_dejson.get("warehouse") or conn.extra_dejson.get(
        "extra__snowflake__warehouse"
    )
    snowflake_role = conn.extra_dejson.get("role") or conn.extra_dejson.get(
        "extra__snowflake__role"
    )

    if not snowflake_account:
        raise ValueError(
            f"Snowflake account is required in connection extras for conn_id: {conn_id}"
        )
    if not snowflake_database:
        raise ValueError(
            f"Snowflake database is required in connection extras for conn_id: {conn_id}"
        )
    if not snowflake_warehouse:
        raise ValueError(
            f"Snowflake warehouse is required in connection extras for conn_id: {conn_id}"
        )

    effective_schema = schema or hook.schema
    if not effective_schema:
        raise ValueError(f"Schema is required for Snowflake connection: {conn_id}")

    return SnowflakeKeyConnection(
        account=snowflake_account,
        user=conn.login,
        role=snowflake_role,
        warehouse=snowflake_warehouse,
        database=snowflake_database,
        schema=effective_schema,
        private_key=pkb,
    )


def _build_snowflake_connection_string_manual(
    conn: Connection, schema: Optional[str] = None
) -> str:
    """Build Snowflake connection string manually from connection parameters."""
    snowflake_account = conn.extra_dejson.get("account") or conn.extra_dejson.get(
        "extra__snowflake__account"
    )
    snowflake_region = conn.extra_dejson.get("region") or conn.extra_dejson.get(
        "extra__snowflake__region"
    )
    snowflake_database = conn.extra_dejson.get("database") or conn.extra_dejson.get(
        "extra__snowflake__database"
    )
    snowflake_warehouse = conn.extra_dejson.get("warehouse") or conn.extra_dejson.get(
        "extra__snowflake__warehouse"
    )
    snowflake_role = conn.extra_dejson.get("role") or conn.extra_dejson.get(
        "extra__snowflake__role"
    )

    if not snowflake_account:
        raise ValueError(f"Snowflake account is required in connection extras")
    if not snowflake_database:
        raise ValueError(f"Snowflake database is required in connection extras")
    if not snowflake_warehouse:
        raise ValueError(f"Snowflake warehouse is required in connection extras")

    effective_schema = schema or conn.schema
    if not effective_schema:
        raise ValueError(f"Schema is required for Snowflake connection")

    if snowflake_region:
        uri_string = (
            f"snowflake://{conn.login}:{conn.password}@{snowflake_account}.{snowflake_region}/"
            f"{snowflake_database}/{effective_schema}?warehouse={snowflake_warehouse}"
        )
    else:
        uri_string = (
            f"snowflake://{conn.login}:{conn.password}@{snowflake_account}/"
            f"{snowflake_database}/{effective_schema}?warehouse={snowflake_warehouse}"
        )

    if snowflake_role:
        uri_string += f"&role={snowflake_role}"

    return uri_string


def build_gcpbigquery_connection_string(
    conn_id: str, schema: Optional[str] = None
) -> str:
    """
    Build connection string for Google Cloud BigQuery connections.

    Args:
        conn_id: Airflow connection ID
        schema: Optional schema override

    Returns:
        Connection string for BigQuery

    Raises:
        ValueError: If connection doesn't exist
    """
    conn = get_connection_by_id(conn_id=conn_id)

    effective_schema = schema or conn.schema or ""
    return f"{conn.host}{effective_schema}"


def build_sqlite_connection_string(conn_id: str) -> str:
    """
    Build connection string for SQLite connections.

    Args:
        conn_id: Airflow connection ID

    Returns:
        Connection string for SQLite

    Raises:
        ValueError: If connection doesn't exist
    """
    conn = get_connection_by_id(conn_id=conn_id)

    return f"sqlite:///{conn.host}"


def build_aws_connection_string(
    conn_id: str,
    schema: Optional[str] = None,
    database: Optional[str] = None,
    s3_path: Optional[str] = None,
    region: Optional[str] = None,
) -> str:
    """
    Build connection string for AWS connections (currently supports Athena).

    Args:
        conn_id: Airflow connection ID
        schema: Optional schema override
        database: Database name for Athena
        s3_path: S3 staging directory path
        region: AWS region

    Returns:
        Connection string for AWS Athena

    Raises:
        ValueError: If connection doesn't exist or required parameters are missing
    """
    conn = get_connection_by_id(conn_id=conn_id)

    if not s3_path:
        raise ValueError("s3_path parameter is required for AWS connections")
    if not region:
        raise ValueError("region parameter is required for AWS connections")

    athena_db = database or schema or conn.schema

    if athena_db:
        return f"awsathena+rest://@athena.{region}.amazonaws.com/{athena_db}?s3_staging_dir={s3_path}"
    else:
        return (
            f"awsathena+rest://@athena.{region}.amazonaws.com/?s3_staging_dir={s3_path}"
        )
