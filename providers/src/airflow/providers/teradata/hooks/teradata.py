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
"""An Airflow Hook for interacting with Teradata SQL Server."""

from __future__ import annotations

import re
import warnings
from typing import TYPE_CHECKING, Any

import sqlalchemy
import teradatasql
from teradatasql import TeradataConnection

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.common.sql.hooks.sql import DbApiHook

if TYPE_CHECKING:
    from airflow.models.connection import Connection

PARAM_TYPES = {bool, float, int, str}


def _map_param(value):
    if value in PARAM_TYPES:
        # In this branch, value is a Python type; calling it produces
        # an instance of the type which is understood by the Teradata driver
        # in the out parameter mapping mechanism.
        value = value()
    return value


def _handle_user_query_band_text(query_band_text) -> str:
    """Validate given query_band and append if required values missed in query_band."""
    # Ensures 'appname=airflow' and 'org=teradata-internal-telem' are in query_band_text.
    if query_band_text is not None:
        # checking org doesn't exist in query_band, appending 'org=teradata-internal-telem'
        #  If it exists, user might have set some value of their own, so doing nothing in that case
        pattern = r"org\s*=\s*([^;]*)"
        match = re.search(pattern, query_band_text)
        if not match:
            if not query_band_text.endswith(";"):
                query_band_text += ";"
            query_band_text += "org=teradata-internal-telem;"
        # Making sure appname in query_band contains 'airflow'
        pattern = r"appname\s*=\s*([^;]*)"
        # Search for the pattern in the query_band_text
        match = re.search(pattern, query_band_text)
        if match:
            appname_value = match.group(1).strip()
            # if appname exists and airflow not exists in appname then appending 'airflow' to existing
            # appname value
            if "airflow" not in appname_value.lower():
                new_appname_value = appname_value + "_airflow"
                # Optionally, you can replace the original value in the query_band_text
                updated_query_band_text = re.sub(
                    pattern, f"appname={new_appname_value}", query_band_text
                )
                query_band_text = updated_query_band_text
        else:
            # if appname doesn't exist in query_band, adding 'appname=airflow'
            if len(query_band_text.strip()) > 0 and not query_band_text.endswith(";"):
                query_band_text += ";"
            query_band_text += "appname=airflow;"
    else:
        query_band_text = "org=teradata-internal-telem;appname=airflow;"

    return query_band_text


class TeradataHook(DbApiHook):
    """
    General hook for interacting with Teradata SQL Database.

    This module contains basic APIs to connect to and interact with Teradata SQL Database. It uses teradatasql
    client internally as a database driver for connecting to Teradata database. The config parameters like
    Teradata DB Server URL, username, password and database name are fetched from the predefined connection
    config connection_id. It raises an airflow error if the given connection id doesn't exist.

    You can also specify ssl parameters in the extra field of your connection
    as ``{"sslmode": "require", "sslcert": "/path/to/cert.pem", etc}``.

    .. seealso::
        - :ref:`Teradata API connection <howto/connection:teradata>`

    :param args: passed to DbApiHook
    :param database: The Teradata database to connect to.
    :param kwargs: passed to DbApiHook
    """

    # Override to provide the connection name.
    conn_name_attr = "teradata_conn_id"

    # Override to have a default connection id for a particular dbHook
    default_conn_name = "teradata_default"

    # Override if this db supports autocommit.
    supports_autocommit = True

    # Override if this db supports executemany.
    supports_executemany = True

    # Override this for hook to have a custom name in the UI selection
    conn_type = "teradata"

    # Override hook name to give descriptive name for hook
    hook_name = "Teradata"

    # Override with the Teradata specific placeholder parameter string used for insert queries
    placeholder: str = "?"

    # Override SQL query to be used for testing database connection
    _test_connection_sql = "select 1"

    def __init__(
        self,
        *args,
        database: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, schema=database, **kwargs)

    def get_conn(self) -> TeradataConnection:
        """
        Create and return a Teradata Connection object using teradatasql client.

        Establishes connection to a Teradata SQL database using config corresponding to teradata_conn_id.

        :return: a Teradata connection object
        """
        teradata_conn_config: dict = self._get_conn_config_teradatasql()
        query_band_text = None
        if "query_band" in teradata_conn_config:
            query_band_text = teradata_conn_config.pop("query_band")
        teradata_conn = teradatasql.connect(**teradata_conn_config)
        # setting query band
        self.set_query_band(query_band_text, teradata_conn)
        return teradata_conn

    def set_query_band(self, query_band_text, teradata_conn):
        """Set SESSION Query Band for each connection session."""
        try:
            query_band_text = _handle_user_query_band_text(query_band_text)
            set_query_band_sql = f"SET QUERY_BAND='{query_band_text}' FOR SESSION"
            with teradata_conn.cursor() as cur:
                cur.execute(set_query_band_sql)
        except Exception as ex:
            self.log.error(
                "Error occurred while setting session query band: %s ", str(ex)
            )

    def bulk_insert_rows(
        self,
        table: str,
        rows: list[tuple],
        target_fields: list[str] | None = None,
        commit_every: int = 5000,
    ):
        """
        Use :func:`insert_rows` instead, this is deprecated.

        Insert bulk of records into Teradata SQL Database.

        This uses prepared statements via `executemany()`. For best performance,
        pass in `rows` as an iterator.

        :param table: target Teradata database table, use dot notation to target a
            specific database
        :param rows: the rows to insert into the table
        :param target_fields: the names of the columns to fill in the table, default None.
            If None, each row should have some order as table columns name
        :param commit_every: the maximum number of rows to insert in one transaction
            Default 5000. Set greater than 0. Set 1 to insert each row in each transaction
        """
        warnings.warn(
            "bulk_insert_rows is deprecated. Please use the insert_rows method instead.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )

        if not rows:
            raise ValueError("parameter rows could not be None or empty iterable")

        self.insert_rows(
            table=table, rows=rows, target_fields=target_fields, commit_every=commit_every
        )

    def _get_conn_config_teradatasql(self) -> dict[str, Any]:
        """Return set of config params required for connecting to Teradata DB using teradatasql client."""
        conn: Connection = self.get_connection(self.get_conn_id())
        conn_config = {
            "host": conn.host or "localhost",
            "dbs_port": conn.port or "1025",
            "database": conn.schema or "",
            "user": conn.login or "dbc",
            "password": conn.password or "dbc",
        }

        if conn.extra_dejson.get("tmode", False):
            conn_config["tmode"] = conn.extra_dejson["tmode"]

        # Handling SSL connection parameters

        if conn.extra_dejson.get("sslmode", False):
            conn_config["sslmode"] = conn.extra_dejson["sslmode"]
            if "verify" in conn_config["sslmode"]:
                if conn.extra_dejson.get("sslca", False):
                    conn_config["sslca"] = conn.extra_dejson["sslca"]
                if conn.extra_dejson.get("sslcapath", False):
                    conn_config["sslcapath"] = conn.extra_dejson["sslcapath"]
        if conn.extra_dejson.get("sslcipher", False):
            conn_config["sslcipher"] = conn.extra_dejson["sslcipher"]
        if conn.extra_dejson.get("sslcrc", False):
            conn_config["sslcrc"] = conn.extra_dejson["sslcrc"]
        if conn.extra_dejson.get("sslprotocol", False):
            conn_config["sslprotocol"] = conn.extra_dejson["sslprotocol"]
        if conn.extra_dejson.get("query_band", False):
            conn_config["query_band"] = conn.extra_dejson["query_band"]

        return conn_config

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """Return a connection object using sqlalchemy."""
        conn: Connection = self.get_connection(self.get_conn_id())
        link = f"teradatasql://{conn.login}:{conn.password}@{conn.host}"
        connection = sqlalchemy.create_engine(link)
        return connection

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Return custom field behaviour."""
        import json

        return {
            "hidden_fields": ["port"],
            "relabeling": {
                "host": "Database Server URL",
                "schema": "Database Name",
                "login": "Username",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "tmode": "TERA",
                        "sslmode": "verify-ca",
                        "sslca": "/tmp/server-ca.pem",
                    },
                    indent=4,
                ),
                "login": "dbc",
                "password": "dbc",
            },
        }

    def callproc(
        self,
        identifier: str,
        autocommit: bool = False,
        parameters: list | dict | None = None,
    ) -> list | dict | tuple | None:
        """
        Call the stored procedure identified by the provided string.

        Any OUT parameters must be provided with a value of either the
        expected Python type (e.g., `int`) or an instance of that type.

        :param identifier: stored procedure name
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The `IN`, `OUT` and `INOUT` parameters for Teradata
            stored procedure

        The return value is a list or mapping that includes parameters in
        both directions; the actual return type depends on the type of the
        provided `parameters` argument.

        """
        if parameters is None:
            parameters = []

        args = ",".join("?" for name in parameters)

        sql = f"{{CALL {identifier}({(args)})}}"

        def handler(cursor):
            records = cursor.fetchall()

            if records is None:
                return
            if isinstance(records, list):
                return [row for row in records]

            if isinstance(records, dict):
                return {n: v for (n, v) in records.items()}
            raise TypeError(f"Unexpected results: {records}")

        result = self.run(
            sql,
            autocommit=autocommit,
            parameters=(
                [_map_param(value) for (name, value) in parameters.items()]
                if isinstance(parameters, dict)
                else [_map_param(value) for value in parameters]
            ),
            handler=handler,
        )

        return result
