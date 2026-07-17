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
"""This module contains Google BigQuery to PostgreSQL operator."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING

from psycopg2.extensions import register_adapter
from psycopg2.extras import Json

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.bigquery_to_sql import BigQueryToSqlBaseOperator
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data
from airflow.providers.postgres.hooks.postgres import PostgresHook

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class BigQueryToPostgresOperator(BigQueryToSqlBaseOperator):
    """
    Fetch data from a BigQuery table (alternatively fetch selected columns) and insert into PostgreSQL table.

    Due to constraints of the PostgreSQL's ON CONFLICT clause both `selected_fields` and `replace_index`
    parameters need to be specified when using the operator with parameter `replace=True`.
    In effect this means that in order to run this operator with `replace=True` your target table MUST
    already have a unique index column / columns, otherwise the INSERT command will fail with an error.
    See more at https://www.postgresql.org/docs/current/sql-insert.html.

    Please note that currently most of the clauses that can be used with PostgreSQL's INSERT
    command, such as ON CONSTRAINT, WHERE, DEFAULT, etc.,  are not supported by this operator.
    If you need the clauses for your queries, `SQLExecuteQueryOperator` will be a more suitable option.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryToPostgresOperator`

    :param target_table_name: target Postgres table (templated)
    :param postgres_conn_id: Reference to :ref:`postgres connection id <howto/connection:postgres>`.
    :param replace: Whether to replace instead of insert
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned. Must be specified if `replace` is True
    :param replace_index: the column or list of column names to act as
        index for the ON CONFLICT clause. Must be specified if `replace` is True
    """

    def __init__(
        self,
        *,
        target_table_name: str,
        postgres_conn_id: str = "postgres_default",
        replace: bool = False,
        selected_fields: list[str] | str | None = None,
        replace_index: list[str] | str | None = None,
        **kwargs,
    ) -> None:
        if replace and not (selected_fields and replace_index):
            raise ValueError("PostgreSQL ON CONFLICT upsert syntax requires column names and a unique index.")
        super().__init__(
            target_table_name=target_table_name, replace=replace, selected_fields=selected_fields, **kwargs
        )
        self.postgres_conn_id = postgres_conn_id
        self.replace_index = replace_index

    @cached_property
    def postgres_hook(self) -> PostgresHook:
        register_adapter(list, Json)
        register_adapter(dict, Json)
        return PostgresHook(database=self.database, postgres_conn_id=self.postgres_conn_id)

    def get_sql_hook(self) -> PostgresHook:
        return self.postgres_hook

    def execute(self, context: Context) -> None:
        if not self.bigquery_hook:
            self.bigquery_hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )
        # Set source_project_dataset_table here, after hooks are initialized and project_id is available
        project_id = self.bigquery_hook.project_id
        self.source_project_dataset_table = f"{project_id}.{self.dataset_id}.{self.table_id}"

        self.persist_links(context)
        for rows in bigquery_get_data(
            self.log,
            self.dataset_id,
            self.table_id,
            self.bigquery_hook,
            self.batch_size,
            self.selected_fields,
        ):
            self.postgres_hook.insert_rows(
                table=self.target_table_name,
                rows=rows,
                target_fields=self.selected_fields,
                replace=self.replace,
                commit_every=self.batch_size,
                replace_index=self.replace_index,
            )
