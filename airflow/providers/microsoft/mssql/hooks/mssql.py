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
"""Microsoft SQLServer hook module."""

from __future__ import annotations

from typing import Any

import pymssql
from methodtools import lru_cache
from pymssql import Connection as PymssqlConnection

from airflow.providers.common.sql.hooks.sql import DbApiHook, fetch_all_handler


class MsSqlHook(DbApiHook):
    """
    Interact with Microsoft SQL Server.

    :param args: passed to DBApiHook
    :param sqlalchemy_scheme: Scheme sqlalchemy connection.  Default is ``mssql+pymssql`` Only used for
      ``get_sqlalchemy_engine`` and ``get_sqlalchemy_connection`` methods.
    :param kwargs: passed to DbApiHook
    """

    conn_name_attr = "mssql_conn_id"
    default_conn_name = "mssql_default"
    conn_type = "mssql"
    hook_name = "Microsoft SQL Server"
    supports_autocommit = True
    DEFAULT_SQLALCHEMY_SCHEME = "mssql+pymssql"

    def __init__(
        self,
        *args,
        sqlalchemy_scheme: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.schema = kwargs.pop("schema", None)
        self._sqlalchemy_scheme = sqlalchemy_scheme

    @property
    def sqlalchemy_scheme(self) -> str:
        """Sqlalchemy scheme either from constructor, connection extras or default."""
        extra_scheme = self.connection_extra_lower.get("sqlalchemy_scheme")
        if not self._sqlalchemy_scheme and extra_scheme and (":" in extra_scheme or "/" in extra_scheme):
            raise RuntimeError("sqlalchemy_scheme in connection extra should not contain : or / characters")
        return self._sqlalchemy_scheme or extra_scheme or self.DEFAULT_SQLALCHEMY_SCHEME

    def get_uri(self) -> str:
        from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

        r = list(urlsplit(super().get_uri()))
        # change pymssql driver:
        r[0] = self.sqlalchemy_scheme
        # remove query string 'sqlalchemy_scheme' like parameters:
        qs = parse_qs(r[3], keep_blank_values=True)
        for k in list(qs.keys()):
            if k.lower() == "sqlalchemy_scheme":
                qs.pop(k, None)
        r[3] = urlencode(qs, doseq=True)
        return urlunsplit(r)

    def get_sqlalchemy_connection(
        self, connect_kwargs: dict | None = None, engine_kwargs: dict | None = None
    ) -> Any:
        """Sqlalchemy connection object."""
        engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        return engine.connect(**(connect_kwargs or {}))

    @lru_cache(maxsize=None)
    def get_primary_keys(self, table: str) -> list[str]:
        primary_keys = self.run(
            f"""
            SELECT c.name
            FROM sys.columns c
            WHERE c.object_id =  OBJECT_ID('{table}')
                AND EXISTS (SELECT 1 FROM sys.index_columns ic
                    INNER JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                    WHERE i.is_primary_key = 1
                    AND ic.object_id = c.object_id
                    AND ic.column_id = c.column_id);
            """,
            handler=fetch_all_handler,
        )
        return [pk[0] for pk in primary_keys]  # type: ignore

    def _generate_insert_sql(self, table, values, target_fields, replace, **kwargs) -> str:
        """
        Generate the INSERT SQL statement.

        The MERGE INTO variant is specific to MSSQL syntax

        :param table: Name of the target table
        :param values: The row to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param replace: Whether to replace/merge into instead of insert
        :return: The generated INSERT or MERGE INTO SQL statement
        """
        if not replace:
            return super()._generate_insert_sql(table, values, target_fields, replace, **kwargs)  # type: ignore

        primary_keys = self.get_primary_keys(table)
        columns = [
            target_field
            for target_field in target_fields
            if target_field in set(target_fields).difference(set(primary_keys))
        ]

        self.log.debug("primary_keys: %s", primary_keys)
        self.log.info("columns: %s", columns)

        return f"""MERGE INTO {table} AS target
        USING (SELECT {', '.join(map(lambda column: f'{self.placeholder} AS {column}', target_fields))}) AS source
        ON {' AND '.join(map(lambda column: f'target.{column} = source.{column}', primary_keys))}
        WHEN MATCHED THEN
            UPDATE SET {', '.join(map(lambda column: f'target.{column} = source.{column}', columns))}
        WHEN NOT MATCHED THEN
            INSERT ({', '.join(target_fields)}) VALUES ({', '.join(map(lambda column: f'source.{column}', target_fields))});"""

    def get_conn(self) -> PymssqlConnection:
        """Return ``pymssql`` connection object."""
        conn = self.connection
        return pymssql.connect(
            server=conn.host,
            user=conn.login,
            password=conn.password,
            database=self.schema or conn.schema,
            port=str(conn.port),
        )

    def set_autocommit(
        self,
        conn: PymssqlConnection,
        autocommit: bool,
    ) -> None:
        conn.autocommit(autocommit)

    def get_autocommit(self, conn: PymssqlConnection):
        return conn.autocommit_state
