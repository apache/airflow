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
"""A Airflow Hook for interacting with Teradata SQL Server."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeVar

import sqlalchemy
import teradatasql
from teradatasql import TeradataConnection

from airflow.providers.common.sql.hooks.sql import DbApiHook

T = TypeVar("T")
if TYPE_CHECKING:
    from airflow.models.connection import Connection


class TeradataHook(DbApiHook):
    """General hook for interacting with Teradata SQL Database.

    This module contains basic APIs to connect to and interact with Teradata SQL Database. It uses teradatasql
    client internally as a database driver for connecting to Teradata database. The config parameters like
    Teradata DB Server URL, username, password and database name are fetched from the predefined connection
    config connection_id. It raises an airflow error if the given connection id doesn't exist.

    See :doc:` docs/apache-airflow-providers-teradata/connections/teradata.rst` for full documentation.

    :param args: passed to DbApiHook
    :param database: The Teradata database to connect to.
    :param kwargs: passed to DbApiHook


    Usage Help:

    >>> tdh = TeradataHook()
    >>> sql = "SELECT top 1 _airbyte_ab_id from airbyte_td._airbyte_raw_Sales;"
    >>> tdh.get_records(sql)
    [[61ad1d63-3efd-4da4-9904-a4489cc3a520]]

    """

    # Override to provide the connection name.
    conn_name_attr = "teradata_conn_id"

    # Override to have a default connection id for a particular dbHook
    default_conn_name = "teradata_default"

    # Override if this db supports autocommit.
    supports_autocommit = True

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
        """Creates and returns a Teradata Connection object using teradatasql client.

        Establishes connection to a Teradata SQL database using config corresponding to teradata_conn_id.

        .. note:: By default it connects to the database via the teradatasql library.
            But you can also choose the mysql-connector-python library which lets you connect through ssl
            without any further ssl parameters required.

        :return: a mysql connection object
        """
        teradata_conn_config: dict = self._get_conn_config_teradatasql()
        teradata_conn = teradatasql.connect(**teradata_conn_config)
        return teradata_conn

    def bulk_insert_rows(
        self,
        table: str,
        rows: list[tuple],
        target_fields: list[str] | None = None,
        commit_every: int = 5000,
    ):
        """A bulk insert of records for Teradata SQL Database.

        This uses prepared statements via `executemany()`. For best performance,
        pass in `rows` as an iterator.

        :param table: target Teradata database table, use dot notation to target a
            specific database
        :param rows: the rows to insert into the table
        :param target_fields: the names of the columns to fill in the table, default None.
            If None, each rows should have some order as table columns name
        :param commit_every: the maximum number of rows to insert in one transaction
            Default 5000. Set greater than 0. Set 1 to insert each row in each transaction
        """
        if not rows:
            raise ValueError("parameter rows could not be None or empty iterable")
        conn = self.get_conn()
        if self.supports_autocommit:
            self.set_autocommit(conn, False)
        cursor = conn.cursor()
        cursor.fast_executemany = True
        values_base = target_fields if target_fields else rows[0]
        prepared_stm = "INSERT INTO {tablename} {columns} VALUES ({values})".format(
            tablename=table,
            columns="({})".format(", ".join(target_fields)) if target_fields else "",
            values=", ".join("?" for i in range(1, len(values_base) + 1)),
        )
        row_count = 0
        # Chunk the rows
        row_chunk = []
        for row in rows:
            row_chunk.append(row)
            row_count += 1
            if row_count % commit_every == 0:
                cursor.executemany(prepared_stm, row_chunk)
                conn.commit()  # type: ignore[attr-defined]
                # Empty chunk
                row_chunk = []
        # Commit the leftover chunk
        if len(row_chunk) > 0:
            cursor.executemany(prepared_stm, row_chunk)
            conn.commit()  # type: ignore[attr-defined]
        self.log.info("[%s] inserted %s rows", table, row_count)
        cursor.close()
        conn.close()  # type: ignore[attr-defined]

    def _get_conn_config_teradatasql(self) -> dict[str, Any]:
        """Returns set of config params required for connecting to Teradata DB using teradatasql client."""
        conn: Connection = self.get_connection(getattr(self, self.conn_name_attr))
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

        return conn_config

    def get_sqlalchemy_engine(self, engine_kwargs=None):
        """Returns a connection object using sqlalchemy."""
        conn: Connection = self.get_connection(getattr(self, self.conn_name_attr))
        link = f"teradatasql://{conn.login}:{conn.password}@{conn.host}"
        connection = sqlalchemy.create_engine(link)
        return connection

    @staticmethod
    def get_ui_field_behaviour() -> dict:
        """Returns custom field behaviour."""
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
                    {"tmode": "TERA", "sslmode": "verify-ca", "sslca": "/tmp/server-ca.pem"}, indent=4
                ),
                "login": "dbc",
                "password": "dbc",
            },
        }
