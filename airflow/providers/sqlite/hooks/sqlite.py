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

from airflow.providers.common.sql.hooks.sql import DbApiHook


class SqliteHook(DbApiHook):
    """Interact with SQLite."""

    conn_name_attr = "sqlite_conn_id"
    default_conn_name = "sqlite_default"
    conn_type = "sqlite"
    hook_name = "Sqlite"
    placeholder = "?"

    def get_conn(self) -> sqlite3.dbapi2.Connection:
        """Returns a sqlite connection object"""
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)
        conn = sqlite3.connect(airflow_conn.host)
        return conn

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)
        return f"sqlite:///{airflow_conn.host}"
