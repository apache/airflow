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

import os
import warnings
from contextlib import closing
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Union

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from deprecated import deprecated
from psycopg2.extras import DictCursor, NamedTupleCursor, RealDictCursor
from sqlalchemy.engine import URL

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from psycopg2.extensions import connection

    from airflow.models.connection import Connection
    from airflow.providers.openlineage.sqlparser import DatabaseInfo

CursorType = Union[DictCursor, RealDictCursor, NamedTupleCursor]


class PostgresHook(DbApiHook):
    """
    Interact with Postgres.

    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
    Also you can choose cursor as ``{"cursor": "dictcursor"}``. Refer to the
    psycopg2.extras for more details.

    Note: For Redshift, use keepalives_idle in the extra connection parameters
    and set it to less than 300 seconds.

    Note: For AWS IAM authentication, use iam in the extra connection parameters
    and set it to true. Leave the password field empty. This will use the
    "aws_default" connection to get the temporary token unless you override
    in extras.
    extras example: ``{"iam":true, "aws_conn_id":"my_aws_conn"}``
    For Redshift, also use redshift in the extra connection parameters and
    set it to true. The cluster-identifier is extracted from the beginning of
    the host field, so is optional. It can however be overridden in the extra field.
    extras example: ``{"iam":true, "redshift":true, "cluster-identifier": "my_cluster_id"}``

    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :param options: Optional. Specifies command-line options to send to the server
        at connection start. For example, setting this to ``-c search_path=myschema``
        sets the session's value of the ``search_path`` to ``myschema``.
    :param enable_log_db_messages: Optional. If enabled logs database messages sent to the client
        during the session. To avoid a memory leak psycopg2 only saves the last 50 messages.
        For details, see: `PostgreSQL logging configuration parameters
        <https://www.postgresql.org/docs/current/runtime-config-logging.html>`__
    """

    conn_name_attr = "postgres_conn_id"
    default_conn_name = "postgres_default"
    conn_type = "postgres"
    hook_name = "Postgres"
    supports_autocommit = True
    supports_executemany = True

    def __init__(
        self, *args, options: str | None = None, enable_log_db_messages: bool = False, **kwargs
    ) -> None:
        if "schema" in kwargs:
            warnings.warn(
                'The "schema" arg has been renamed to "database" as it contained the database name.'
                'Please use "database" to set the database name.',
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            kwargs["database"] = kwargs["schema"]
        super().__init__(*args, **kwargs)
        self.conn: connection = None
        self.database: str | None = kwargs.pop("database", None)
        self.options = options
        self.enable_log_db_messages = enable_log_db_messages

    @property
    @deprecated(
        reason=(
            'The "schema" variable has been renamed to "database" as it contained the database name.'
            'Please use "database" to get the database name.'
        ),
        category=AirflowProviderDeprecationWarning,
    )
    def schema(self):
        return self.database

    @schema.setter
    @deprecated(
        reason=(
            'The "schema" variable has been renamed to "database" as it contained the database name.'
            'Please use "database" to set the database name.'
        ),
        category=AirflowProviderDeprecationWarning,
    )
    def schema(self, value):
        self.database = value

    @property
    def sqlalchemy_url(self) -> URL:
        conn = self.get_connection(self.get_conn_id())
        return URL.create(
            drivername="postgresql",
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,
            database=self.database or conn.schema,
        )

    def _get_cursor(self, raw_cursor: str) -> CursorType:
        _cursor = raw_cursor.lower()
        cursor_types = {
            "dictcursor": psycopg2.extras.DictCursor,
            "realdictcursor": psycopg2.extras.RealDictCursor,
            "namedtuplecursor": psycopg2.extras.NamedTupleCursor,
        }
        if _cursor in cursor_types:
            return cursor_types[_cursor]
        else:
            valid_cursors = ", ".join(cursor_types.keys())
            raise ValueError(f"Invalid cursor passed {_cursor}. Valid options are: {valid_cursors}")

    def get_conn(self) -> connection:
        """Establish a connection to a postgres database."""
        conn = deepcopy(self.connection)

        # check for authentication via AWS IAM
        if conn.extra_dejson.get("iam", False):
            conn.login, conn.password, conn.port = self.get_iam_token(conn)

        conn_args = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "dbname": self.database or conn.schema,
            "port": conn.port,
        }
        raw_cursor = conn.extra_dejson.get("cursor", False)
        if raw_cursor:
            conn_args["cursor_factory"] = self._get_cursor(raw_cursor)

        if self.options:
            conn_args["options"] = self.options

        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name not in [
                "iam",
                "redshift",
                "cursor",
                "cluster-identifier",
                "aws_conn_id",
            ]:
                conn_args[arg_name] = arg_val

        self.conn = psycopg2.connect(**conn_args)
        return self.conn

    def copy_expert(self, sql: str, filename: str) -> None:
        """
        Execute SQL using psycopg2's ``copy_expert`` method.

        Necessary to execute COPY command without access to a superuser.

        Note: if this method is called with a "COPY FROM" statement and
        the specified input file does not exist, it creates an empty
        file and no data is loaded, but the operation succeeds.
        So if users want to be aware when the input file does not exist,
        they have to check its existence by themselves.
        """
        self.log.info("Running copy expert: %s, filename: %s", sql, filename)
        if not os.path.isfile(filename):
            with open(filename, "w"):
                pass

        with open(filename, "r+") as file, closing(self.get_conn()) as conn, closing(conn.cursor()) as cur:
            cur.copy_expert(sql, file)
            file.truncate(file.tell())
            conn.commit()

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.

        :return: the extracted URI in Sqlalchemy URI format.
        """
        return self.sqlalchemy_url.render_as_string(hide_password=False)

    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Load a tab-delimited file into a database table."""
        self.copy_expert(f"COPY {table} FROM STDIN", tmp_file)

    def bulk_dump(self, table: str, tmp_file: str) -> None:
        """Dump a database table into a tab-delimited file."""
        self.copy_expert(f"COPY {table} TO STDOUT", tmp_file)

    @staticmethod
    def _serialize_cell(cell: object, conn: connection | None = None) -> Any:
        """
        Serialize a cell.

        PostgreSQL adapts all arguments to the ``execute()`` method internally,
        hence we return the cell without any conversion.

        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The cell
        """
        return cell

    def get_iam_token(self, conn: Connection) -> tuple[str, str, int]:
        """
        Get the IAM token.

        This uses AWSHook to retrieve a temporary password to connect to
        Postgres or Redshift. Port is required. If none is provided, the default
        5432 is used.
        """
        try:
            from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        except ImportError:
            from airflow.exceptions import AirflowException

            raise AirflowException(
                "apache-airflow-providers-amazon not installed, run: "
                "pip install 'apache-airflow-providers-postgres[amazon]'."
            )

        aws_conn_id = conn.extra_dejson.get("aws_conn_id", "aws_default")
        login = conn.login
        if conn.extra_dejson.get("redshift", False):
            port = conn.port or 5439
            # Pull the custer-identifier from the beginning of the Redshift URL
            # ex. my-cluster.ccdre4hpd39h.us-east-1.redshift.amazonaws.com returns my-cluster
            cluster_identifier = conn.extra_dejson.get("cluster-identifier", conn.host.split(".")[0])
            redshift_client = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="redshift").conn
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift.html#Redshift.Client.get_cluster_credentials
            cluster_creds = redshift_client.get_cluster_credentials(
                DbUser=login,
                DbName=self.database or conn.schema,
                ClusterIdentifier=cluster_identifier,
                AutoCreate=False,
            )
            token = cluster_creds["DbPassword"]
            login = cluster_creds["DbUser"]
        else:
            port = conn.port or 5432
            rds_client = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="rds").conn
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.generate_db_auth_token
            token = rds_client.generate_db_auth_token(conn.host, port, conn.login)
        return login, token, port

    def get_table_primary_key(self, table: str, schema: str | None = "public") -> list[str] | None:
        """
        Get the table's primary key.

        :param table: Name of the target table
        :param schema: Name of the target schema, public by default
        :return: Primary key columns list
        """
        return self.dialect.get_primary_keys(table=table, schema=schema)

    def get_openlineage_database_info(self, connection) -> DatabaseInfo:
        """Return Postgres/Redshift specific information for OpenLineage."""
        from airflow.providers.openlineage.sqlparser import DatabaseInfo

        is_redshift = connection.extra_dejson.get("redshift", False)

        if is_redshift:
            authority = self._get_openlineage_redshift_authority_part(connection)
        else:
            authority = DbApiHook.get_openlineage_authority_part(  # type: ignore[attr-defined]
                connection, default_port=5432
            )

        return DatabaseInfo(
            scheme="postgres" if not is_redshift else "redshift",
            authority=authority,
            database=self.database or connection.schema,
        )

    def _get_openlineage_redshift_authority_part(self, connection) -> str:
        try:
            from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        except ImportError:
            from airflow.exceptions import AirflowException

            raise AirflowException(
                "apache-airflow-providers-amazon not installed, run: "
                "pip install 'apache-airflow-providers-postgres[amazon]'."
            )
        aws_conn_id = connection.extra_dejson.get("aws_conn_id", "aws_default")

        port = connection.port or 5439
        cluster_identifier = connection.extra_dejson.get("cluster-identifier", connection.host.split(".")[0])
        region_name = AwsBaseHook(aws_conn_id=aws_conn_id).region_name

        return f"{cluster_identifier}.{region_name}:{port}"

    def get_openlineage_database_dialect(self, connection) -> str:
        """Return postgres/redshift dialect."""
        return "redshift" if connection.extra_dejson.get("redshift", False) else "postgres"

    def get_openlineage_default_schema(self) -> str | None:
        """Return current schema. This is usually changed with ``SEARCH_PATH`` parameter."""
        return self.get_first("SELECT CURRENT_SCHEMA;")[0]

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": [],
            "relabeling": {
                "schema": "Database",
            },
        }

    def get_db_log_messages(self, conn) -> None:
        """
        Log all database messages sent to the client during the session.

        :param conn: Connection object
        """
        if self.enable_log_db_messages:
            for output in conn.notices:
                self.log.info(output)
