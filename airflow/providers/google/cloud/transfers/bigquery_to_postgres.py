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

from typing import Sequence

from airflow.providers.google.cloud.transfers.bigquery_to_sql import BigQueryToSqlBaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class BigQueryToPostgresOperator(BigQueryToSqlBaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and insert that data into a PostgreSQL table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryToPostgresOperator`

    :param target_table_name: target Postgres table (templated)
    :param postgres_conn_id: Reference to :ref:`postgres connection id <howto/connection:postgres>`.
    """

    template_fields: Sequence[str] = tuple(BigQueryToSqlBaseOperator.template_fields) + (
        "dataset_id",
        "table_id",
    )

    def __init__(
        self,
        *,
        target_table_name: str,
        postgres_conn_id: str = "postgres_default",
        **kwargs,
    ) -> None:
        super().__init__(target_table_name=target_table_name, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_sql_hook(self) -> PostgresHook:
        return PostgresHook(schema=self.database, postgres_conn_id=self.postgres_conn_id)
