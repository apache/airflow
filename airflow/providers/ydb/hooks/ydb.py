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
from typing import TYPE_CHECKING, Any, Iterable, Union

from deprecated import deprecated
from sqlalchemy.engine import URL
#import ydb_sqlalchemy.dbapi.connection as YDBConnection
from airflow.providers.ydb.hooks.dbapi.connection import Connection
from airflow.providers.ydb.hooks.dbapi.cursor import YdbQuery
import ydb
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection

class YDBCursor:
    def __init__(self, delegatee, is_ddl):
        self.delegatee = delegatee
        self.is_ddl = is_ddl

    def execute(self, sql: str, parameters: Optional[Mapping[str, Any]] = None):
        q = YdbQuery(yql_text=sql, is_ddl=self.is_ddl)
        return self.delegatee.execute(q, parameters)

    def executemany(self, sql: str, seq_of_parameters: Optional[Sequence[Mapping[str, Any]]]):
        for parameters in seq_of_parameters:
            self.execute(sql, parameters)

    def executescript(self, script):
        return self.execute(script)

    def fetchone(self):
        return self.delegatee.fetchone()

    def fetchmany(self, size=None):
        return self.delegatee.fetchmany(size=size)

    def fetchall(self):
        return self.delegatee.fetchall()

    def nextset(self):
        return self.delegatee.nextset()

    def setinputsizes(self, sizes):
        return self.delegatee.setinputsizes()

    def setoutputsize(self, column=None):
        return self.delegatee.setoutputsize()

    def close(self):
        return self.delegatee.close()

    @property
    def rowcount(self):
        return self.delegatee.rowcount

    @property
    def description(self):
        return self.delegatee.description


class YDBConnection:
    def __init__(self, is_ddl):
        self.is_ddl = is_ddl
        endpoint = "grpcs://ydb.serverless.yandexcloud.net:2135"
        database = "/ru-central1/b1gtl2kg13him37quoo6/etndqstq7ne4v68n6c9b"
        iam_token = "t1.9..."
        credentials = ydb.AccessTokenCredentials(iam_token)
        driver_config = ydb.DriverConfig(
            endpoint=endpoint,
            database=database,
            table_client_settings=self._get_table_client_settings(),
            credentials=credentials,
        )
        driver = ydb.Driver(driver_config)
        # wait until driver become initialized
        driver.wait(fail_fast=True, timeout=10)
        ydb_session_pool = ydb.SessionPool(driver, size=5)

        self.delegatee = Connection(ydb_session_pool=ydb_session_pool)

    def cursor(self):
        return YDBCursor(self.delegatee.cursor(), is_ddl=self.is_ddl)

    def begin(self):
        self.delegatee.begin()

    def commit(self):
        self.delegatee.commit()

    def rollback(self):
        self.delegatee.rollback()

    def close(self):
        self.delegatee.close()

    def _get_table_client_settings(self) -> ydb.TableClientSettings:
        return (
            ydb.TableClientSettings()
            .with_native_date_in_result_sets(True)
            .with_native_datetime_in_result_sets(True)
            .with_native_timestamp_in_result_sets(True)
            .with_native_interval_in_result_sets(True)
            .with_native_json_in_result_sets(False)
        )


class YDBHook(DbApiHook):
    """Interact with YDB.

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
    """

    conn_name_attr = "ydb_conn_id"
    default_conn_name = "ydb_default"
    conn_type = "ydb"
    hook_name = "YDB"
    supports_autocommit = True
    supports_executemany = True

    def __init__(self, *args, is_ddl: bool = False, options: str | None = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.is_ddl = is_ddl
        self.connection: Connection | None = kwargs.pop("connection", None)
        self.conn: connection = None
        self.database: str | None = kwargs.pop("database", None)
        self.options = options

    @property
    def sqlalchemy_url(self) -> URL:
        conn = self.get_connection(getattr(self, self.conn_name_attr))
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
        """Establish a connection to a YDB database."""
        self.conn = YDBConnection(is_ddl=self.is_ddl)
        return self.conn

    def copy_expert(self, sql: str, filename: str) -> None:
        """Execute SQL using psycopg2's ``copy_expert`` method.

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
        """Extract the URI from the connection.

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
        """Serialize a cell.

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
        """Get the IAM token.

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
        """Get the table's primary key.

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

    def _generate_insert_sql(
        self, table: str, values: tuple[str, ...], target_fields: Iterable[str], replace: bool, **kwargs
    ) -> str:
        """Generate the INSERT SQL statement.

        The REPLACE variant is specific to the PostgreSQL syntax.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param replace: Whether to replace instead of insert
        :param replace_index: the column or list of column names to act as
            index for the ON CONFLICT clause
        :return: The generated INSERT or REPLACE SQL statement
        """
        placeholders = [
            self.placeholder,
        ] * len(values)
        replace_index = kwargs.get("replace_index")

        if target_fields:
            target_fields_fragment = ", ".join(target_fields)
            target_fields_fragment = f"({target_fields_fragment})"
        else:
            target_fields_fragment = ""

        sql = f"INSERT INTO {table} {target_fields_fragment} VALUES ({','.join(placeholders)})"

        if replace:
            if not target_fields:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names")
            if not replace_index:
                raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires an unique index")
            if isinstance(replace_index, str):
                replace_index = [replace_index]

            on_conflict_str = f" ON CONFLICT ({', '.join(replace_index)})"
            replace_target = [f for f in target_fields if f not in replace_index]

            if replace_target:
                replace_target_str = ", ".join(f"{col} = excluded.{col}" for col in replace_target)
                sql += f"{on_conflict_str} DO UPDATE SET {replace_target_str}"
            else:
                sql += f"{on_conflict_str} DO NOTHING"

        return sql

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": [],
            "relabeling": {
                "schema": "Database",
            },
        }
