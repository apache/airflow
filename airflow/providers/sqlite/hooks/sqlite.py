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

import sqlite3

from airflow.hooks.dbapi import DbApiHook


class SqliteHook(DbApiHook):
    """Interact with SQLite."""

    conn_name_attr = 'sqlite_conn_id'
    default_conn_name = 'sqlite_default'
    conn_type = 'sqlite'
    hook_name = 'Sqlite'

    def get_conn(self) -> sqlite3.dbapi2.Connection:
        """Returns a sqlite connection object"""
        conn_id = getattr(self, self.conn_name_attr)
        airflow_conn = self.get_connection(conn_id)
        conn = sqlite3.connect(airflow_conn.host)
        return conn

    @staticmethod
    def _generate_insert_sql(table, values, target_fields, replace, **kwargs):
        """
        Static helper method that generates the INSERT SQL statement.
        The REPLACE variant is specific to MySQL syntax.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param replace: Whether to replace instead of insert
        :return: The generated INSERT or REPLACE SQL statement
        :rtype: str
        """
        placeholders = [
            "?",
        ] * len(values)

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = f"({target_fields})"
        else:
            target_fields = ''

        if not replace:
            sql = "INSERT INTO "
        else:
            sql = "REPLACE INTO "
        sql += f"{table} {target_fields} VALUES ({','.join(placeholders)})"
        return sql
