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
from redshift_connector import Connection as RedshiftConnection
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection


class RedshiftSQLHook(DbApiHook):
    """Execute statements against Amazon Redshift.

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
        get_sqlalchemy_engine() and get_uri() depend on sqlalchemy-amazon-redshift
    """

    conn_name_attr = "redshift_conn_id"
    default_conn_name = "redshift_default"
    conn_type = "redshift"
    hook_name = "Amazon Redshift"
    supports_autocommit = True

    def __init__(self, *args, aws_conn_id: str = "aws_default", **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Get custom field behavior."""
        return {
            "hidden_fields": [],
            "relabeling": {"login": "User", "schema": "Database"},
        }

    @cached_property
    def conn(self):
        return self.get_connection(self.redshift_conn_id)  # type: ignore[attr-defined]

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
        """Retrieve a temporary password to connect to Redshift.

        Port is required. If none is provided, default is used for each service.
        """
        port = conn.port or 5439
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
        conn_params = self._get_conn_params()

        if "user" in conn_params:
            conn_params["username"] = conn_params.pop("user")

        # Compatibility: The 'create' factory method was added in SQLAlchemy 1.4
        # to replace calling the default URL constructor directly.
        create_url = getattr(URL, "create", URL)
        return str(create_url(drivername="redshift+redshift_connector", **conn_params))

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """Overridden to pass Redshift-specific arguments."""
        conn_kwargs = self.conn.extra_dejson
        if engine_kwargs is None:
            engine_kwargs = {}

        if "connect_args" in engine_kwargs:
            engine_kwargs["connect_args"] = {**conn_kwargs, **engine_kwargs["connect_args"]}
        else:
            engine_kwargs["connect_args"] = conn_kwargs

        return create_engine(self.get_uri(), **engine_kwargs)

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

    def get_conn(self) -> RedshiftConnection:
        """Get a ``redshift_connector.Connection`` object."""
        conn_params = self._get_conn_params()
        conn_kwargs_dejson = self.conn.extra_dejson
        conn_kwargs: dict = {**conn_params, **conn_kwargs_dejson}
        return redshift_connector.connect(**conn_kwargs)
