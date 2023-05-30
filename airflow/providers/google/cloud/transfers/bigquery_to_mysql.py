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
"""This module contains Google BigQuery to MySQL operator."""
from __future__ import annotations

import warnings
from typing import Sequence

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.transfers.bigquery_to_sql import BigQueryToSqlBaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook


class BigQueryToMySqlOperator(BigQueryToSqlBaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and insert that data into a MySQL table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryToMySqlOperator`

    :param mysql_table: target MySQL table, use dot notation to target a
        specific database. It is deprecated: use target_table_name instead. (templated)
    :param target_table_name: target MySQL table. It takes precedence over mysql_table. (templated)
    :param mysql_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.
    """

    template_fields: Sequence[str] = tuple(BigQueryToSqlBaseOperator.template_fields) + (
        "dataset_id",
        "table_id",
    )

    def __init__(
        self,
        *,
        mysql_table: str | None = None,
        target_table_name: str | None = None,
        mysql_conn_id: str = "mysql_default",
        **kwargs,
    ) -> None:
        if mysql_table is not None:
            warnings.warn(
                "The `mysql_table` parameter has been deprecated. Use `target_table_name` instead.",
                AirflowProviderDeprecationWarning,
            )

            if target_table_name is not None:
                raise ValueError(
                    f"Cannot set both arguments: mysql_table={mysql_table!r} and "
                    f"target_table_name={target_table_name!r}."
                )

            target_table_name = mysql_table

        super().__init__(target_table_name=target_table_name, **kwargs)
        self.mysql_conn_id = mysql_conn_id

    def get_sql_hook(self) -> MySqlHook:
        return MySqlHook(schema=self.database, mysql_conn_id=self.mysql_conn_id)
