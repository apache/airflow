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

import sqlite3
from urllib.parse import unquote

from airflow.providers.common.sql.hooks.sql import DbApiHook


class SqliteHook(DbApiHook):
    """Interact with SQLite."""

    conn_name_attr = "sqlite_conn_id"
    default_conn_name = "sqlite_default"
    conn_type = "sqlite"
    hook_name = "Sqlite"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._placeholder: str = "?"

    def get_conn(self) -> sqlite3.dbapi2.Connection:
        """Return SQLite connection object."""
        sqlalchemy_uri = self.get_uri()
        # The sqlite3 connection does not use the sqlite scheme.
        # See https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#uri-connections for details.
        sqlite_uri = sqlalchemy_uri.replace("sqlite:///", "file:")
        conn = sqlite3.connect(sqlite_uri, uri=True)
        return conn

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()."""
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)
        if airflow_conn.conn_type is None:
            airflow_conn.conn_type = self.conn_type
        airflow_uri = unquote(airflow_conn.get_uri())
        # For sqlite, there is no schema in the connection URI. So we need to drop the trailing slash.
        airflow_sqlite_uri = airflow_uri.replace("/?", "?")
        # The sqlite connection has one more slash for path specification.
        # See https://docs.sqlalchemy.org/en/14/dialects/sqlite.html#connect-strings for details.
        sqlalchemy_uri = airflow_sqlite_uri.replace("sqlite://", "sqlite:///")
        return sqlalchemy_uri
