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
from typing import Any, Iterable, Union

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2.extensions import connection
from psycopg2.extras import DictCursor, NamedTupleCursor, RealDictCursor

from airflow.models.connection import Connection
from airflow.providers.common.sql.hooks.sql import DbApiHook

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
    """

    conn_name_attr = "postgres_conn_id"
    default_conn_name = "postgres_default"
    conn_type = "postgres"
    hook_name = "Postgres"
    supports_autocommit = True

    def __init__(self, *args, **kwargs) -> None:
        if "schema" in kwargs:
            warnings.warn(
                'The "schema" arg has been renamed to "database" as it contained the database name.'
                'Please use "database" to set the database name.',
                DeprecationWarning,
                stacklevel=2,
            )
            kwargs["database"] = kwargs["schema"]
        super().__init__(*args, **kwargs)
        self.connection: Connection | None = kwargs.pop("connection", None)
        self.conn: connection = None
        self.database: str | None = kwargs.pop("database", None)

    @property
    def schema(self):
        warnings.warn(
            'The "schema" variable has been renamed to "database" as it contained the database name.'
            'Please use "database" to get the database name.',
            DeprecationWarning,
            stacklevel=2,
        )
        return self.database

    @schema.setter
    def schema(self, value):
        warnings.warn(
            'The "schema" variable has been renamed to "database" as it contained the database name.'
            'Please use "database" to set the database name.',
            DeprecationWarning,
            stacklevel=2,
        )
        self.database = value

    def _get_cursor(self, raw_cursor: str) -> CursorType:
        _cursor = raw_cursor.lower()
        if _cursor == "dictcursor":
            return psycopg2.extras.DictCursor
        if _cursor == "realdictcursor":
            return psycopg2.extras.RealDictCursor
        if _cursor == "namedtuplecursor":
            return psycopg2.extras.NamedTupleCursor
        raise ValueError(f"Invalid cursor passed {_cursor}")

    def get_conn(self) -> connection:
        """Establishes a connection to a postgres database."""
        conn_id = getattr(self, self.conn_name_attr)
        conn = deepcopy(self.connection or self.get_connection(conn_id))

        # check for authentication via AWS IAM
        if conn.extra_dejson.get("iam", False):
            conn.login, conn.password, conn.port = self.get_iam_token(conn)

        conn_args = dict(
            host=conn.host,
            user=conn.login,
            password=conn.password,
            dbname=self.database or conn.schema,
            port=conn.port,
        )
        raw_cursor = conn.extra_dejson.get("cursor", False)
        if raw_cursor:
            conn_args["cursor_factory"] = self._get_cursor(raw_cursor)

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
        Executes SQL using psycopg2 copy_expert method.
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

        with open(filename, "r+") as file:
            with closing(self.get_conn()) as conn:
                with closing(conn.cursor()) as cur:
                    cur.copy_expert(sql, file)
                    file.truncate(file.tell())
                    conn.commit()

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.
        :return: the extracted uri.
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        conn.schema = self.database or conn.schema
        uri = conn.get_uri().replace("postgres://", "postgresql://")
        return uri

    def bulk_load(self, table: str, tmp_file: str) -> None:
        """Loads a tab-delimited file into a database table"""
        self.copy_expert(f"COPY {table} FROM STDIN", tmp_file)

    def bulk_dump(self, table: str, tmp_file: str) -> None:
        """Dumps a database table into a tab-delimited file"""
        self.copy_expert(f"COPY {table} TO STDOUT", tmp_file)

    @staticmethod
    def _serialize_cell(cell: object, conn: connection | None = None) -> Any:
        """
        Postgresql will adapt all arguments to the execute() method internally,
        hence we return cell without any conversion.

        See http://initd.org/psycopg/docs/advanced.html#adapting-new-types for
        more information.

        :param cell: The cell to insert into the table
        :param conn: The database connection
        :return: The cell
        """
        return cell

    def get_iam_token(self, conn: Connection) -> tuple[str, str, int]:
        """
        Uses AWSHook to retrieve a temporary password to connect to Postgres
        or Redshift. Port is required. If none is provided, default is used for
        each service
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
        Helper method that returns the table primary key

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

    @classmethod
    def _generate_insert_sql(
        cls, table: str, values: tuple[str, ...], target_fields: Iterable[str], replace: bool, **kwargs
    ) -> str:
        """
        Static helper method that generates the INSERT SQL statement.
        The REPLACE variant is specific to PostgreSQL syntax.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param replace: Whether to replace instead of insert
        :param replace_index: the column or list of column names to act as
            index for the ON CONFLICT clause
        :return: The generated INSERT or REPLACE SQL statement
        """
        placeholders = [
            cls.placeholder,
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
