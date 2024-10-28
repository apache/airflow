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

from functools import cached_property
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.teradata.hooks.teradata import TeradataHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class TeradataToTeradataOperator(BaseOperator):
    """
    Moves data from Teradata source database to Teradata destination database.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TeradataToTeradataOperator`

    :param dest_teradata_conn_id: destination Teradata connection.
    :param destination_table: destination table to insert rows.
    :param source_teradata_conn_id: :ref:`Source Teradata connection <howto/connection:Teradata>`.
    :param sql: SQL query to execute against the source Teradata database
    :param sql_params: Parameters to use in sql query.
    :param rows_chunk: number of rows per chunk to commit.
    """

    template_fields: Sequence[str] = (
        "sql",
        "sql_params",
    )
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql", "sql_params": "py"}
    ui_color = "#e07c24"

    def __init__(
        self,
        *,
        dest_teradata_conn_id: str,
        destination_table: str,
        source_teradata_conn_id: str,
        sql: str,
        sql_params: dict | None = None,
        rows_chunk: int = 5000,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if sql_params is None:
            sql_params = {}
        self.dest_teradata_conn_id = dest_teradata_conn_id
        self.destination_table = destination_table
        self.source_teradata_conn_id = source_teradata_conn_id
        self.sql = sql
        self.sql_params = sql_params
        self.rows_chunk = rows_chunk

    @cached_property
    def src_hook(self) -> TeradataHook:
        return TeradataHook(teradata_conn_id=self.source_teradata_conn_id)

    @cached_property
    def dest_hook(self) -> TeradataHook:
        return TeradataHook(teradata_conn_id=self.dest_teradata_conn_id)

    def execute(self, context: Context) -> None:
        src_hook = self.src_hook
        dest_hook = self.dest_hook
        with src_hook.get_conn() as src_conn:
            cursor = src_conn.cursor()
            cursor.execute(self.sql, self.sql_params)
            target_fields = [field[0] for field in cursor.description]
            rows_total = 0
            if len(target_fields) != 0:
                for rows in iter(lambda: cursor.fetchmany(self.rows_chunk), []):
                    dest_hook.bulk_insert_rows(
                        self.destination_table,
                        rows,
                        target_fields=target_fields,
                        commit_every=self.rows_chunk,
                    )
                    rows_total += len(rows)
            self.log.info(
                "Finished data transfer. Total number of rows transferred - %s",
                rows_total,
            )
            cursor.close()
