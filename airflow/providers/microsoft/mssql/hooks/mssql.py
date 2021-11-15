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

"""Microsoft SQLServer hook module"""

from typing import Iterable, Sized

import pymssql

from airflow.hooks.dbapi import DbApiHook


class MsSqlHook(DbApiHook):
    """Interact with Microsoft SQL Server."""

    conn_name_attr = 'mssql_conn_id'
    default_conn_name = 'mssql_default'
    conn_type = 'mssql'
    hook_name = 'Microsoft SQL Server'
    supports_autocommit = True

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)

    def get_conn(
        self,
    ) -> pymssql.connect:
        """Returns a mssql connection object"""
        conn = self.get_connection(self.mssql_conn_id)  # type: ignore[attr-defined]

        conn = pymssql.connect(
            server=conn.host,
            user=conn.login,
            password=conn.password,
            database=self.schema or conn.schema,
            port=conn.port,
        )
        return conn

    def set_autocommit(
        self,
        conn: pymssql.connect,
        autocommit: bool,
    ) -> None:
        conn.autocommit(autocommit)

    def get_autocommit(self, conn: pymssql.connect):
        return conn.autocommit_state

    @staticmethod
    def _generate_insert_sql(
        table: str, values: Sized, target_fields: Iterable[str], replace: bool, **kwargs
    ) -> str:
        """
        Static helper method that generates the INSERT SQL statement.

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param replace: Whether to replace instead of insert (replace currently not supported)
        :return: The generated INSERT SQL statement
        """
        if replace:
            raise NotImplementedError("replace is not supported currently")

        placeholders = ["?"] * len(values)

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = f"({target_fields})"
        else:
            target_fields = ""

        sql = f"INSERT INTO {table} {target_fields} VALUES ({','.join(placeholders)})"
        return sql
