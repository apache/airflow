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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.oracle.hooks.oracle import OracleHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class OracleToOracleOperator(BaseOperator):
    """
    Moves data from Oracle to Oracle.


    :param oracle_destination_conn_id: destination Oracle connection.
    :param destination_table: destination table to insert rows.
    :param oracle_source_conn_id: :ref:`Source Oracle connection <howto/connection:oracle>`.
    :param source_sql: SQL query to execute against the source Oracle
        database. (templated)
    :param source_sql_params: Parameters to use in sql query. (templated)
    :param rows_chunk: number of rows per chunk to commit.
    """

    template_fields: Sequence[str] = ("source_sql", "source_sql_params")
    template_fields_renderers = {"source_sql": "sql", "source_sql_params": "py"}
    ui_color = "#e08c8c"

    def __init__(
        self,
        *,
        oracle_destination_conn_id: str,
        destination_table: str,
        oracle_source_conn_id: str,
        source_sql: str,
        source_sql_params: dict | None = None,
        rows_chunk: int = 5000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if source_sql_params is None:
            source_sql_params = {}
        self.oracle_destination_conn_id = oracle_destination_conn_id
        self.destination_table = destination_table
        self.oracle_source_conn_id = oracle_source_conn_id
        self.source_sql = source_sql
        self.source_sql_params = source_sql_params
        self.rows_chunk = rows_chunk

    def _execute(self, src_hook, dest_hook, context) -> None:
        with src_hook.get_conn() as src_conn:
            cursor = src_conn.cursor()
            self.log.info("Querying data from source: %s", self.oracle_source_conn_id)
            cursor.execute(self.source_sql, self.source_sql_params)
            target_fields = list(map(lambda field: field[0], cursor.description))

            rows_total = 0
            rows = cursor.fetchmany(self.rows_chunk)
            while len(rows) > 0:
                rows_total += len(rows)
                dest_hook.bulk_insert_rows(
                    self.destination_table, rows, target_fields=target_fields, commit_every=self.rows_chunk
                )
                rows = cursor.fetchmany(self.rows_chunk)
                self.log.info("Total inserted: %s rows", rows_total)

            self.log.info("Finished data transfer.")
            cursor.close()

    def execute(self, context: Context) -> None:
        src_hook = OracleHook(oracle_conn_id=self.oracle_source_conn_id)
        dest_hook = OracleHook(oracle_conn_id=self.oracle_destination_conn_id)
        self._execute(src_hook, dest_hook, context)
