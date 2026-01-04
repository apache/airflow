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

from functools import cached_property
from typing import TYPE_CHECKING

import redshift_connector
import tenacity
from redshift_connector import Connection as RedshiftConnection, InterfaceError, OperationalError

try:
    from sqlalchemy import create_engine
    from sqlalchemy.engine.url import URL
except ImportError:
    URL = create_engine = None

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection
    from airflow.providers.openlineage.sqlparser import DatabaseInfo


class RedshiftSQLHook(DbApiHook):
    """
    Execute statements against Amazon Redshift.

    This hook requires the redshift_conn_id connection.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the
    "aws_default" connection to get the temporary token unless you override
    with aws_conn_id when initializing the hook.
    The cluster-identifier is extracted from the beginning of
    the host field, so is optional. It can however be overridden in the extra field.
    extras example: ``{"iam":true}``

    :param redshift_conn_id: reference to
        :ref:`Amazon Redshift connection id<howto/connection:redshift>`

    .. note::
        get_sqlalchemy_engine() and get_uri() depend on sqlalchemy-amazon-redshift.
    """

    conn_name_attr = "redshift_conn_id"
    default_conn_name = "redshift_default"
    conn_type = "redshift"
    hook_name = "Amazon Redshift"
    supports_autocommit = True

    def __init__(self, *args, aws_conn_id: str | None = "aws_default", **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id

    @classmethod
    def get_ui_field_behaviour(cls) -> dict:
        """Get custom field behavior."""
        return {
            "hidden_fields": [],
            "relabeling": {"login": "User", "schema": "Database"},
        }

    @cached_property
    def conn(self):
        return self.get_connection(self.get_conn_id())

    def _get_conn_params(self) -> dict[str, str | int]:
        """Retrieve connection parameters."""
        conn = self.conn

        conn_params: dict[str, str | int] = {}

        if conn.extra_dejson.get("iam", False):
            conn.login, conn.password, conn.port = self.get_iam_token(conn)

        if conn.login:
            conn_params["user"] = conn.login
        if conn.password:
            conn_params["password"] = conn.password
        if conn.host:
            conn_params["host"] = conn.host
        if conn.port:
            conn_params["port"] = conn.port
        if conn.schema:
            conn_params["database"] = conn.schema

        return conn_params

    def get_iam_token(self, conn: Connection) -> tuple[str, str, int]:
        """
        Retrieve a temporary password to connect to Redshift.

        Port is required. If none is provided, default is used for each service.
        """
        port = conn.port or 5439
        is_serverless = conn.extra_dejson.get("is_serverless", False)
        if is_serverless:
            serverless_work_group = conn.extra_dejson.get("serverless_work_group")
            if not serverless_work_group:
                raise AirflowException(
                    "Please set serverless_work_group in redshift connection to use IAM with"
                    " Redshift Serverless."
                )
            serverless_token_duration_seconds = conn.extra_dejson.get(
                "serverless_token_duration_seconds", 3600
            )
            redshift_serverless_client = AwsBaseHook(
                aws_conn_id=self.aws_conn_id, client_type="redshift-serverless"
            ).conn
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-serverless/client/get_credentials.html#get-credentials
            cluster_creds = redshift_serverless_client.get_credentials(
                dbName=conn.schema,
                workgroupName=serverless_work_group,
                durationSeconds=serverless_token_duration_seconds,
            )
            token = cluster_creds["dbPassword"]
            login = cluster_creds["dbUser"]
        else:
            # Pull the custer-identifier from the beginning of the Redshift URL
            # ex. my-cluster.ccdre4hpd39h.us-east-1.redshift.amazonaws.com returns my-cluster
            cluster_identifier = conn.extra_dejson.get("cluster_identifier")
            if not cluster_identifier:
                if conn.host:
                    cluster_identifier = conn.host.split(".", 1)[0]
                else:
                    raise AirflowException("Please set cluster_identifier or host in redshift connection.")
            redshift_client = AwsBaseHook(aws_conn_id=self.aws_conn_id, client_type="redshift").conn
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.get_cluster_credentials
            cluster_creds = redshift_client.get_cluster_credentials(
                DbUser=conn.login,
                DbName=conn.schema,
                ClusterIdentifier=cluster_identifier,
                AutoCreate=False,
            )
            token = cluster_creds["DbPassword"]
            login = cluster_creds["DbUser"]
        return login, token, port

    def get_uri(self) -> str:
        """Overridden to use the Redshift dialect as driver name."""
        if URL is None:
            raise AirflowOptionalProviderFeatureException(
                "sqlalchemy is required to generate the connection URI. "
                "Install it with: pip install 'apache-airflow-providers-amazon[sqlalchemy]'"
            )
        conn_params = self._get_conn_params()

        if "user" in conn_params:
            conn_params["username"] = conn_params.pop("user")

        # Use URL.create for SQLAlchemy 2 compatibility
        username = conn_params.get("username")
        password = conn_params.get("password")
        host = conn_params.get("host")
        port = conn_params.get("port")
        database = conn_params.get("database")

        return URL.create(
            drivername="postgresql",
            username=str(username) if username is not None else None,
            password=str(password) if password is not None else None,
            host=str(host) if host is not None else None,
            port=int(port) if port is not None else None,
            database=str(database) if database is not None else None,
        ).render_as_string(hide_password=False)

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """Overridden to pass Redshift-specific arguments."""
        if create_engine is None:
            raise AirflowOptionalProviderFeatureException(
                "sqlalchemy is required for creating the engine. Install it with"
                ": pip install 'apache-airflow-providers-amazon[sqlalchemy]'"
            )
        conn_kwargs = self.conn.extra_dejson
        if engine_kwargs is None:
            engine_kwargs = {}

        if "connect_args" in engine_kwargs:
            engine_kwargs["connect_args"] = {**conn_kwargs, **engine_kwargs["connect_args"]}
        else:
            engine_kwargs["connect_args"] = conn_kwargs

        return create_engine(self.get_uri(), **engine_kwargs)

    def get_table_primary_key(self, table: str, schema: str | None = "public") -> list[str] | None:
        """
        Get the table's primary key.

        :param table: Name of the target table
        :param schema: Name of the target schema, public by default
        :return: Primary key columns list
        """
        sql = """
            select kcu.column_name
            from information_schema.table_constraints tco
                    join information_schema.key_column_usage kcu
                        on kcu.constraint_name = tco.constraint_name
                            and kcu.constraint_schema = tco.constraint_schema
                            and kcu.constraint_name = tco.constraint_name
            where tco.constraint_type = 'PRIMARY KEY'
            and kcu.table_schema = %s
            and kcu.table_name = %s
        """
        pk_columns = [row[0] for row in self.get_records(sql, (schema, table))]
        return pk_columns or None

    @tenacity.retry(
        stop=tenacity.stop_after_attempt(5),
        wait=tenacity.wait_exponential(max=20),
        # OperationalError is thrown when the connection times out
        # InterfaceError is thrown when the connection is refused
        retry=tenacity.retry_if_exception_type((OperationalError, InterfaceError)),
        reraise=True,
    )
    def get_conn(self) -> RedshiftConnection:
        """Get a ``redshift_connector.Connection`` object."""
        conn_params = self._get_conn_params()
        conn_kwargs_dejson = self.conn.extra_dejson
        conn_kwargs: dict = {**conn_params, **conn_kwargs_dejson}
        return redshift_connector.connect(**conn_kwargs)

    def get_openlineage_database_info(self, connection: Connection) -> DatabaseInfo:
        """Return Redshift specific information for OpenLineage."""
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        authority = self._get_openlineage_redshift_authority_part(connection)

        return DatabaseInfo(
            scheme="redshift",
            authority=authority,
            database=connection.schema,
            information_schema_table_name="SVV_REDSHIFT_COLUMNS",
            information_schema_columns=[
                "schema_name",
                "table_name",
                "column_name",
                "ordinal_position",
                "data_type",
                "database_name",
            ],
            is_information_schema_cross_db=True,
            use_flat_cross_db_query=True,
        )

    def _get_openlineage_redshift_authority_part(self, connection: Connection) -> str:
        from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook

        port = connection.port or 5439

        cluster_identifier = None

        if connection.extra_dejson.get("iam", False):
            cluster_identifier = connection.extra_dejson.get("cluster_identifier")
            region_name = AwsBaseHook(aws_conn_id=self.aws_conn_id).region_name
            identifier = f"{cluster_identifier}.{region_name}"
        if not cluster_identifier:
            if connection.host:
                identifier = self._get_identifier_from_hostname(connection.host)
            else:
                raise AirflowException("Host is required when cluster_identifier is not provided.")
        return f"{identifier}:{port}"

    def _get_identifier_from_hostname(self, hostname: str) -> str:
        parts = hostname.split(".")
        if hostname.endswith("amazonaws.com") and len(parts) == 6:
            return f"{parts[0]}.{parts[2]}"
        self.log.debug(
            """Could not parse identifier from hostname '%s'.
            You are probably using IP to connect to Redshift cluster.
            Expected format: 'cluster_identifier.id.region_name.redshift.amazonaws.com'
            Falling back to whole hostname.""",
            hostname,
        )
        return hostname

    def get_openlineage_database_dialect(self, connection: Connection) -> str:
        """Return redshift dialect."""
        return "redshift"

    def get_openlineage_default_schema(self) -> str | None:
        """Return current schema. This is usually changed with ``SEARCH_PATH`` parameter."""
        return self.get_first("SELECT CURRENT_SCHEMA();")[0]
