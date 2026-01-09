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
from collections.abc import Mapping
from contextlib import closing
from copy import deepcopy
from typing import TYPE_CHECKING, Any, Literal, Protocol, TypeAlias, cast, overload

import psycopg2
import psycopg2.extras
from more_itertools import chunked
from psycopg2.extras import DictCursor, NamedTupleCursor, RealDictCursor, execute_batch

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.common.compat.sdk import AirflowException, Connection, conf
from airflow.providers.common.sql.hooks.sql import DbApiHook
from airflow.providers.postgres.dialects.postgres import PostgresDialect

USE_PSYCOPG3: bool
try:
    import psycopg as psycopg  # needed for patching in unit tests
    import sqlalchemy
    from packaging.version import Version

    sqlalchemy_version = Version(sqlalchemy.__version__)
    is_sqla2 = (sqlalchemy_version.major, sqlalchemy_version.minor, sqlalchemy_version.micro) >= (2, 0, 0)

    USE_PSYCOPG3 = is_sqla2  # implicitly includes `and bool(psycopg)` since the import above succeeded
except (ImportError, ModuleNotFoundError):
    USE_PSYCOPG3 = False

if USE_PSYCOPG3:
    from psycopg.rows import dict_row, namedtuple_row
    from psycopg.types.json import register_default_adapters

if TYPE_CHECKING:
    from pandas import DataFrame as PandasDataFrame
    from polars import DataFrame as PolarsDataFrame
    from sqlalchemy.engine import URL

    from airflow.providers.common.sql.dialects.dialect import Dialect
    from airflow.providers.openlineage.sqlparser import DatabaseInfo

    if USE_PSYCOPG3:
        from psycopg.errors import Diagnostic

CursorType: TypeAlias = DictCursor | RealDictCursor | NamedTupleCursor
CursorRow: TypeAlias = dict[str, Any] | tuple[Any, ...]


class CompatConnection(Protocol):
    """Protocol for type hinting psycopg2 and psycopg3 connection objects."""

    def cursor(self, *args, **kwargs) -> Any: ...
    def commit(self) -> None: ...
    def close(self) -> None: ...

    # Context manager support
    def __enter__(self) -> CompatConnection: ...
    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None: ...

    # Common properties
    @property
    def notices(self) -> list[Any]: ...

    # psycopg3 specific (optional)
    @property
    def adapters(self) -> Any: ...

    @property
    def row_factory(self) -> Any: ...

    # Optional method for psycopg3
    def add_notice_handler(self, handler: Any) -> None: ...


class PostgresHook(DbApiHook):
    """
    Interact with Postgres.

    You can specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.
    Also, you can choose cursor as ``{"cursor": "dictcursor"}``. Refer to the
    psycopg2.extras or psycopg.rows for more details.

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

    For Redshift Serverless, use redshift-serverless in the extra connection parameters and
    set it to true. The workgroup-name is extracted from the beginning of
    the host field, so is optional. It can however be overridden in the extra field.
    extras example: ``{"iam":true, "redshift-serverless":true, "workgroup-name": "my_serverless_workgroup"}``

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
    default_client_log_level = "warning"
    default_connector_version: int = 2
    conn_type = "postgres"
    hook_name = "Postgres"
    supports_autocommit = True
    supports_executemany = True
    ignored_extra_options = {
        "iam",
        "redshift",
        "redshift-serverless",
        "cursor",
        "cluster-identifier",
        "workgroup-name",
        "aws_conn_id",
        "sqlalchemy_scheme",
        "sqlalchemy_query",
        "azure_conn_id",
    }
    default_azure_oauth_scope = "https://ossrdbms-aad.database.windows.net/.default"

    def __init__(
        self, *args, options: str | None = None, enable_log_db_messages: bool = False, **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.conn: CompatConnection | None = None
        self.database: str | None = kwargs.pop("database", None)
        self.options = options
        self.enable_log_db_messages = enable_log_db_messages

    @staticmethod
    def __cast_nullable(value, dst_type: type) -> Any:
        return dst_type(value) if value is not None else None

    @property
    def sqlalchemy_url(self) -> URL:
        try:
            from sqlalchemy.engine import URL
        except (ImportError, ModuleNotFoundError) as err:
            raise AirflowOptionalProviderFeatureException(
                "SQLAlchemy is not installed. Please install it with "
                "`pip install apache-airflow-providers-postgres[sqlalchemy]`."
            ) from err
        conn = self.connection
        query = conn.extra_dejson.get("sqlalchemy_query", {})
        if not isinstance(query, dict):
            raise AirflowException("The parameter 'sqlalchemy_query' must be of type dict!")
        if conn.extra_dejson.get("iam", False):
            conn.login, conn.password, conn.port = self.get_iam_token(conn)
        return URL.create(
            drivername="postgresql+psycopg" if USE_PSYCOPG3 else "postgresql",
            username=self.__cast_nullable(conn.login, str),
            password=self.__cast_nullable(conn.password, str),
            host=self.__cast_nullable(conn.host, str),
            port=self.__cast_nullable(conn.port, int),
            database=self.__cast_nullable(self.database, str) or self.__cast_nullable(conn.schema, str),
            query=query,
        )

    @property
    def dialect_name(self) -> str:
        return "postgresql"

    @property
    def dialect(self) -> Dialect:
        return PostgresDialect(self)

    def _notice_handler(self, notice: Diagnostic):
        """Handle notices from the database and log them."""
        self.log.info(str(notice.message_primary).strip())

    def _get_cursor(self, raw_cursor: str) -> CursorType:
        _cursor = raw_cursor.lower()
        if USE_PSYCOPG3:
            if _cursor == "dictcursor":
                return dict_row
            if _cursor == "namedtuplecursor":
                return namedtuple_row
            if _cursor == "realdictcursor":
                raise AirflowException(
                    "realdictcursor is not supported with psycopg3. Use dictcursor instead."
                )
            valid_cursors = "dictcursor, namedtuplecursor"
            raise ValueError(f"Invalid cursor passed {_cursor}. Valid options are: {valid_cursors}")

        cursor_types = {
            "dictcursor": psycopg2.extras.DictCursor,
            "realdictcursor": psycopg2.extras.RealDictCursor,
            "namedtuplecursor": psycopg2.extras.NamedTupleCursor,
        }
        if _cursor in cursor_types:
            return cursor_types[_cursor]
        valid_cursors = ", ".join(cursor_types.keys())
        raise ValueError(f"Invalid cursor passed {_cursor}. Valid options are: {valid_cursors}")

    def _generate_cursor_name(self):
        """Generate a unique name for server-side cursor."""
        import uuid

        return f"airflow_cursor_{uuid.uuid4().hex}"

    def get_conn(self) -> CompatConnection:
        """Establish a connection to a postgres database."""
        conn = deepcopy(self.connection)

        if conn.extra_dejson.get("iam", False):
            login, password, port = self.get_iam_token(conn)
            conn.login = cast("Any", login)
            conn.password = cast("Any", password)
            conn.port = cast("Any", port)

        conn_args: dict[str, Any] = {
            "host": conn.host,
            "user": conn.login,
            "password": conn.password,
            "dbname": self.database or conn.schema,
            "port": conn.port,
        }

        if self.options:
            conn_args["options"] = self.options

        # Add extra connection arguments
        for arg_name, arg_val in conn.extra_dejson.items():
            if arg_name not in self.ignored_extra_options:
                conn_args[arg_name] = arg_val

        if USE_PSYCOPG3:
            from psycopg.connection import Connection as pgConnection

            raw_cursor = conn.extra_dejson.get("cursor")
            if raw_cursor:
                conn_args["row_factory"] = self._get_cursor(raw_cursor)

            # Use Any type for the connection args to avoid type conflicts
            connection = pgConnection.connect(**cast("Any", conn_args))
            self.conn = cast("CompatConnection", connection)

            # Register JSON handlers for both json and jsonb types
            # This ensures JSON data is properly decoded from bytes to Python objects
            register_default_adapters(connection)

            # Add the notice handler AFTER the connection is established
            if self.enable_log_db_messages and hasattr(self.conn, "add_notice_handler"):
                self.conn.add_notice_handler(self._notice_handler)
        else:  # psycopg2
            raw_cursor = conn.extra_dejson.get("cursor", False)
            if raw_cursor:
                conn_args["cursor_factory"] = self._get_cursor(raw_cursor)

            self.conn = cast("CompatConnection", psycopg2.connect(**conn_args))

        return self.conn

    @overload
    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["pandas"] = "pandas",
        **kwargs: Any,
    ) -> PandasDataFrame: ...

    @overload
    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["polars"] = ...,
        **kwargs: Any,
    ) -> PolarsDataFrame: ...

    def get_df(
        self,
        sql: str | list[str],
        parameters: list | tuple | Mapping[str, Any] | None = None,
        *,
        df_type: Literal["pandas", "polars"] = "pandas",
        **kwargs: Any,
    ) -> PandasDataFrame | PolarsDataFrame:
        """
        Execute the sql and returns a dataframe.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        :param df_type: Type of dataframe to return, either "pandas" or "polars"
        :param kwargs: (optional) passed into `pandas.io.sql.read_sql` or `polars.read_database` method
        :return: A pandas or polars DataFrame containing the query results.
        """
        if df_type == "pandas":
            try:
                from pandas.io import sql as psql
            except ImportError:
                raise AirflowOptionalProviderFeatureException(
                    "pandas library not installed, run: pip install "
                    "'apache-airflow-providers-common-sql[pandas]'."
                )

            engine = self.get_sqlalchemy_engine()
            with engine.connect() as conn:
                if isinstance(sql, list):
                    sql = "; ".join(sql)  # Or handle multiple queries differently
                return cast("PandasDataFrame", psql.read_sql(sql, con=conn, params=parameters, **kwargs))

        elif df_type == "polars":
            return self._get_polars_df(sql, parameters, **kwargs)

        else:
            raise ValueError(f"Unsupported df_type: {df_type}")

    def copy_expert(self, sql: str, filename: str) -> None:
        """
        Execute SQL using psycopg's ``copy_expert`` method.

        Necessary to execute COPY command without access to a superuser.

        Note: if this method is called with a "COPY FROM" statement and
        the specified input file does not exist, it creates an empty
        file and no data is loaded, but the operation succeeds.
        So if users want to be aware when the input file does not exist,
        they have to check its existence by themselves.
        """
        self.log.info("Running copy expert: %s, filename: %s", sql, filename)
        if USE_PSYCOPG3:
            if " from stdin" in sql.lower():
                # Handle COPY FROM STDIN: read from the file and write to the database.
                if not os.path.isfile(filename):
                    with open(filename, "w"):
                        pass  # Create an empty file to prevent errors.

                with open(filename, "rb") as file, self.get_conn() as conn, conn.cursor() as cur:
                    with cur.copy(sql) as copy:
                        while data := file.read(8192):
                            copy.write(data)
                    conn.commit()
            else:
                # Handle COPY TO STDOUT: read from the database and write to the file.
                with open(filename, "wb") as file, self.get_conn() as conn, conn.cursor() as cur:
                    with cur.copy(sql) as copy:
                        for data in copy:
                            file.write(data)
                    conn.commit()
        else:
            if not os.path.isfile(filename):
                with open(filename, "w"):
                    pass

            with (
                open(filename, "r+") as file,
                closing(self.get_conn()) as conn,
                closing(conn.cursor()) as cur,
            ):
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
    def _serialize_cell_ppg2(cell: object, conn: CompatConnection | None = None) -> Any:
        """
        Serialize a cell using psycopg2.

        Psycopg2 adapts all arguments to the ``execute()`` method internally,
        hence we return the cell without any conversion.

        See https://www.psycopg.org/docs/extensions.html#sql-adaptation-protocol-objects
        for more information.

        To perform custom type adaptation please use register_adapter function
        https://www.psycopg.org/docs/extensions.html#psycopg2.extensions.register_adapter.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The cell
        """
        return cell

    @staticmethod
    def _serialize_cell_ppg3(cell: object, conn: CompatConnection | None = None) -> Any:
        """Serialize a cell using psycopg3."""
        if isinstance(cell, (dict, list)):
            try:
                from psycopg.types.json import Json

                return Json(cell)
            except ImportError:
                return cell
        return cell

    @staticmethod
    def _serialize_cell(cell: object, conn: Any | None = None) -> Any:
        if USE_PSYCOPG3:
            return PostgresHook._serialize_cell_ppg3(cell, conn)
        return PostgresHook._serialize_cell_ppg2(cell, conn)

    def get_iam_token(self, conn: Connection) -> tuple[str, str, int]:
        """Get the IAM token from different identity providers."""
        if conn.extra_dejson.get("azure_conn_id"):
            return self.get_azure_iam_token(conn)
        return self.get_aws_iam_token(conn)

    def get_aws_iam_token(self, conn: Connection) -> tuple[str, str, int]:
        """
        Get the AWS IAM token.

        This uses AWSHook to retrieve a temporary password to connect to
        Postgres or Redshift. Port is required. If none is provided, the default
        5432 is used.
        """
        try:
            from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        except ImportError:
            from airflow.exceptions import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
                "apache-airflow-providers-amazon not installed, run: "
                "pip install 'apache-airflow-providers-postgres[amazon]'."
            )

        aws_conn_id = conn.extra_dejson.get("aws_conn_id", "aws_default")
        login = conn.login
        if conn.extra_dejson.get("redshift", False):
            port = conn.port or 5439
            # Pull the custer-identifier from the beginning of the Redshift URL
            # ex. my-cluster.ccdre4hpd39h.us-east-1.redshift.amazonaws.com returns my-cluster
            cluster_identifier = conn.extra_dejson.get(
                "cluster-identifier", cast("str", conn.host).split(".")[0]
            )
            redshift_client = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="redshift").conn
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift/client/get_cluster_credentials.html#Redshift.Client.get_cluster_credentials
            cluster_creds = redshift_client.get_cluster_credentials(
                DbUser=login,
                DbName=self.database or conn.schema,
                ClusterIdentifier=cluster_identifier,
                AutoCreate=False,
            )
            token = cluster_creds["DbPassword"]
            login = cluster_creds["DbUser"]
        elif conn.extra_dejson.get("redshift-serverless", False):
            port = conn.port or 5439
            # Pull the workgroup-name from the query params/extras, if not there then pull it from the
            # beginning of the Redshift URL
            # ex. workgroup-name.ccdre4hpd39h.us-east-1.redshift.amazonaws.com returns workgroup-name
            workgroup_name = conn.extra_dejson.get("workgroup-name", cast("str", conn.host).split(".")[0])
            redshift_serverless_client = AwsBaseHook(
                aws_conn_id=aws_conn_id, client_type="redshift-serverless"
            ).conn
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/redshift-serverless/client/get_credentials.html#RedshiftServerless.Client.get_credentials
            cluster_creds = redshift_serverless_client.get_credentials(
                dbName=self.database or conn.schema,
                workgroupName=workgroup_name,
            )
            token = cluster_creds["dbPassword"]
            login = cluster_creds["dbUser"]
        else:
            port = conn.port or 5432
            rds_client = AwsBaseHook(aws_conn_id=aws_conn_id, client_type="rds").conn
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds/client/generate_db_auth_token.html#RDS.Client.generate_db_auth_token
            token = rds_client.generate_db_auth_token(conn.host, port, conn.login)
        return cast("str", login), cast("str", token), port

    def get_azure_iam_token(self, conn: Connection) -> tuple[str, str, int]:
        """
        Get the Azure IAM token.

        This uses AzureBaseHook to retrieve an OAUTH token to connect to Postgres.
        Scope for the OAuth token can be set in the config option ``azure_oauth_scope`` under the section ``[postgres]``.
        """
        if TYPE_CHECKING:
            from airflow.providers.microsoft.azure.hooks.base_azure import AzureBaseHook

        azure_conn_id = conn.extra_dejson.get("azure_conn_id", "azure_default")
        try:
            azure_conn = Connection.get(azure_conn_id)
        except AttributeError:
            azure_conn = Connection.get_connection_from_secrets(azure_conn_id)  # type: ignore[attr-defined]
        try:
            azure_base_hook: AzureBaseHook = azure_conn.get_hook()
        except TypeError as e:
            if "required positional argument: 'sdk_client'" in str(e):
                raise AirflowOptionalProviderFeatureException(
                    "Getting azure token is not supported by current version of 'AzureBaseHook'. "
                    "Please upgrade apache-airflow-providers-microsoft-azure>=12.8.0"
                ) from e
            raise
        scope = conf.get("postgres", "azure_oauth_scope", fallback=self.default_azure_oauth_scope)
        token = azure_base_hook.get_token(scope).token
        return cast("str", conn.login or azure_conn.login), token, conn.port or 5432

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
            authority = DbApiHook.get_openlineage_authority_part(connection, default_port=5432)

        return DatabaseInfo(
            scheme="postgres" if not is_redshift else "redshift",
            authority=authority,
            database=self.database or connection.schema,
        )

    def _get_openlineage_redshift_authority_part(self, connection) -> str:
        try:
            from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
        except ImportError:
            from airflow.exceptions import AirflowOptionalProviderFeatureException

            raise AirflowOptionalProviderFeatureException(
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
        """Log database messages."""
        if not self.enable_log_db_messages:
            return

        if USE_PSYCOPG3:
            self.log.debug(
                "With psycopg3, database notices are logged upon creation (via self._notice_handler)."
            )
            return

        for output in conn.notices:
            self.log.info(output)

    def insert_rows(
        self,
        table,
        rows,
        target_fields=None,
        commit_every=1000,
        replace=False,
        *,
        executemany=False,
        fast_executemany=False,
        autocommit=False,
        **kwargs,
    ):
        """
        Insert a collection of tuples into a table.

        Rows are inserted in chunks, each chunk (of size ``commit_every``) is
        done in a new transaction.

        :param table: Name of the target table
        :param rows: The rows to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert
        :param executemany: If True, all rows are inserted at once in
            chunks defined by the commit_every parameter. This only works if all rows
            have same number of column names, but leads to better performance.
        :param fast_executemany: If True, rows will be inserted using an optimized
            bulk execution strategy (``psycopg2.extras.execute_batch``). This can
            significantly improve performance for large inserts. If set to False,
            the method falls back to the default implementation from
            ``DbApiHook.insert_rows``.
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        """
        # if fast_executemany is disabled, defer to default implementation of insert_rows in DbApiHook
        if not fast_executemany:
            return super().insert_rows(
                table,
                rows,
                target_fields=target_fields,
                commit_every=commit_every,
                replace=replace,
                executemany=executemany,
                autocommit=autocommit,
                **kwargs,
            )

        # if fast_executemany is enabled, use optimized execute_batch from psycopg
        nb_rows = 0
        with self._create_autocommit_connection(autocommit) as conn:
            conn.commit()
            with closing(conn.cursor()) as cur:
                for chunked_rows in chunked(rows, commit_every):
                    values = list(
                        map(
                            lambda row: self._serialize_cells(row, conn),
                            chunked_rows,
                        )
                    )
                    sql = self._generate_insert_sql(table, values[0], target_fields, replace, **kwargs)
                    self.log.debug("Generated sql: %s", sql)

                    try:
                        execute_batch(cur, sql, values, page_size=commit_every)
                    except Exception as e:
                        self.log.error("Generated sql: %s", sql)
                        self.log.error("Parameters: %s", values)
                        raise e

                    conn.commit()
                    nb_rows += len(chunked_rows)
                    self.log.info("Loaded %s rows into %s so far", nb_rows, table)
        self.log.info("Done loading. Loaded a total of %s rows into %s", nb_rows, table)
