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

import contextlib
import functools
import re
from collections.abc import Iterable, Mapping
from contextlib import closing
from functools import cached_property
from typing import Any
from urllib.parse import quote

from adbc_driver_manager.dbapi import Connection, connect
from more_itertools import chunked
from pyarrow import RecordBatch, Schema, array, schema

from airflow.providers.common.sql.dialects.dialect import Dialect
from airflow.providers.common.sql.hooks.sql import DbApiHook


def fetch_all_handler(cursor) -> list[tuple] | None:
    """Return results for DbApiHook.run()."""
    if not hasattr(cursor, "description"):
        raise RuntimeError(
            "The database we interact with does not support DBAPI 2.0. Use operator and "
            "handlers that are specifically designed for your database."
        )
    if cursor.description is not None:
        return list(zip(*cursor.fetch_arrow_table().to_pydict().values()))
    return None


def replace_placeholders(sql: str, placeholder: str) -> str:
    # Replace each placeholder with $1, $2, $3 ... in order
    def replacer(match, counter=[1]):
        replacement = f"${counter[0]}"
        counter[0] += 1
        return replacement

    return re.sub(placeholder, replacer, sql)


# https://arrow.apache.org/adbc/current/python/api/adbc_driver_manager.html
# https://arrow.apache.org/docs/python/
class AdbcHook(DbApiHook):
    """
    General hook for ADBC access.
    """

    conn_name_attr = "adbc_conn_id"
    default_conn_name = "adbc_default"
    conn_type = "adbc"
    hook_name = "ADBC Connection"
    supports_autocommit = True

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Get custom field behaviour."""
        return {
            "hidden_fields": ["port", "schema"],
            "relabeling": {"host": "Connection URL"},
        }

    @functools.lru_cache
    def _driver_path(self) -> str:
        import pathlib
        import sys

        import importlib_resources

        # Wheels bundle the shared library
        root = importlib_resources.files(self.driver)
        # The filename is always the same regardless of platform
        entrypoint = root.joinpath(f"lib{self.driver}.so")
        if entrypoint.is_file():
            return str(entrypoint)

        # Search sys.prefix + '/lib' (Unix, Conda on Unix)
        root = pathlib.Path(sys.prefix)
        for filename in (f"lib{self.driver}.so", f"lib{self.driver}.dylib"):
            entrypoint = root.joinpath("lib", filename)
            if entrypoint.is_file():
                return str(entrypoint)

        # Conda on Windows
        entrypoint = root.joinpath("bin", f"{self.driver}.dll")
        if entrypoint.is_file():
            return str(entrypoint)

        # Let the driver manager fall back to (DY)LD_LIBRARY_PATH/PATH
        # (It will insert 'lib', 'so', etc. as needed)
        return self.driver

    @cached_property
    def uri(self) -> str:
        uri = f"{self.dialect_name.lower().replace('_', '-')}://"
        host = self.connection.host

        if host and "://" in host:
            protocol, host = host.split("://", 1)
        else:
            protocol, host = None, host

        if protocol:
            uri += f"{protocol}://"

        authority_block = ""
        if self.connection.login is not None:
            authority_block += quote(self.connection.login, safe="")

        if self.connection.password is not None:
            authority_block += ":" + quote(self.connection.password, safe="")

        if authority_block > "":
            authority_block += "@"

            uri += authority_block

        host_block = ""
        if host:
            host_block += quote(host, safe="")

        if self.connection.port:
            if host_block == "" and authority_block == "":
                host_block += f"@:{self.connection.port}"
            else:
                host_block += f":{self.connection.port}"

        if self.connection.schema:
            host_block += f"/{quote(self.connection.schema, safe='')}"

        uri += host_block
        return uri

    @cached_property
    def driver(self) -> str:
        return self.connection_extra_lower.get("driver") or f"adbc_driver_{self.dialect_name}"

    @cached_property
    def entrypoint(self) -> str | None:
        return self.connection_extra_lower.get("entrypoint")

    @cached_property
    def db_kwargs(self) -> dict:
        return {**{"uri": self.uri}, **self.connection_extra_lower.get("db_kwargs", {})}

    @cached_property
    def conn_kwargs(self) -> dict:
        return self.connection_extra_lower.get("conn_kwargs", {})

    @cached_property
    def dialect_name(self) -> str:
        return self.connection_extra_lower.get("dialect", "default")

    def get_conn(self) -> Connection:
        return connect(
            driver=self._driver_path(),
            entrypoint=self.entrypoint,
            db_kwargs=self.db_kwargs,
            conn_kwargs=self.conn_kwargs,
            autocommit=False,
        )

    def get_records(
        self,
        sql: str | list[str],
        parameters: Iterable | Mapping[str, Any] | None = None,
    ) -> Any:
        """
        Execute the sql and return a set of records.

        :param sql: the sql statement to be executed (str) or a list of sql statements to execute
        :param parameters: The parameters to render the SQL query with.
        """
        return self.run(sql=sql, parameters=parameters, handler=fetch_all_handler)

    def _run_command(self, cur, sql_statement, parameters):
        """Run a statement using an already open cursor."""
        if parameters:
            sql_statement = replace_placeholders(sql_statement, re.escape(self.dialect.placeholder))

        super()._run_command(cur, sql_statement, parameters)

    def _generate_insert_sql(self, table, values, target_fields=None, replace: bool = False, **kwargs) -> str:
        sql_statement = super()._generate_insert_sql(
            table, values, target_fields=target_fields, replace=replace, **kwargs
        )
        sql_statement = replace_placeholders(sql_statement, re.escape(self.dialect.placeholder))

        if self.log_sql:
            self.log.info("Running statement: %s", sql_statement)

        return sql_statement

    @classmethod
    def _to_record_batch(cls, rows, schema: Schema) -> RecordBatch:
        return RecordBatch.from_arrays(
            [array([row[index] for row in rows], type=field.type) for index, field in enumerate(schema)],
            schema=schema,
        )

    def insert_rows(
        self,
        table,
        rows,
        target_fields=None,
        commit_every=1000,
        replace=False,
        *,
        executemany=False,
        fast_executemany=False,
        autocommit=False,
        **kwargs,
    ):
        """
        Insert a collection of tuples into a table.

        Rows are inserted in chunks, each chunk (of size ``commit_every``) is
        done in a new transaction.

        :param table: Name of the target table
        :param rows: The rows to insert into the table
        :param target_fields: The names of the columns to fill in the table
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :param replace: Whether to replace instead of insert
        :param executemany: If True, all rows are inserted at once in
            chunks defined by the commit_every parameter. This only works if all rows
            have same number of column names, but leads to better performance.
        :param fast_executemany: If True, the `fast_executemany` parameter will be set on the
            cursor used by `executemany` which leads to better performance, if supported by driver.
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        """
        nb_rows = 0

        with self._create_autocommit_connection(autocommit) as conn:
            table_name, schema_name = Dialect.extract_schema_from_table(table)

            table_schema = conn.adbc_get_table_schema(
                table_name=table_name,
                db_schema_filter=schema_name,
            )

            if not target_fields:
                target_fields = table_schema.names
            else:
                table_schema = schema([field for field in table_schema if field.name in target_fields])

            self.log.info("target fields: %s", target_fields)
            self.log.info("table_schema: %s", table_schema)

            sql = self._generate_insert_sql(
                table,
                target_fields,  # values not needed — parameters will come from RecordBatch
                target_fields,
                replace,
                **kwargs,
            )

            with closing(conn.cursor()) as cur:
                use_native_bind = hasattr(cur, "bind")

                if use_native_bind:
                    self.log.info("Native Arrow bind supported!")
                elif self.supports_executemany or executemany:
                    if fast_executemany:
                        with contextlib.suppress(AttributeError):
                            # Try to set the fast_executemany attribute
                            cur.fast_executemany = True
                            self.log.info(
                                "Fast_executemany is enabled for conn_id '%s'!",
                                self.get_conn_id(),
                            )

                for chunked_rows in chunked(rows, commit_every):
                    batch = self._to_record_batch(rows=chunked_rows, schema=table_schema)

                    # Prefer native Arrow bind if supported
                    if use_native_bind:
                        cur.bind(batch)
                        cur.execute(sql)
                    else:
                        cur.executemany(sql, batch)

                    conn.commit()

                    nb_rows += batch.num_rows
                    self.log.info("Loaded %s rows into %s so far", nb_rows, table)

        self.log.info("Done loading. Loaded a total of %s rows into %s", nb_rows, table)
