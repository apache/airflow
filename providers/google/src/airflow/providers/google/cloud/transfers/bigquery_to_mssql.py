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
"""This module contains Google BigQuery to MSSQL operator."""

from __future__ import annotations

import warnings
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.links.bigquery import BigQueryTableLink
from airflow.providers.google.cloud.transfers.bigquery_to_sql import BigQueryToSqlBaseOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BigQueryToMsSqlOperator(BigQueryToSqlBaseOperator):
    """
    Fetch data from a BigQuery table (alternatively fetch selected columns) and insert it into a MSSQL table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryToMsSqlOperator`

    :param source_project_dataset_table: A dotted ``<project>.<dataset>.<table>``:
        the big query table of origin
    :param mssql_table: target MsSQL table. It is deprecated: use target_table_name instead. (templated)
    :param target_table_name: target MsSQL table. It takes precedence over mssql_table. (templated)
    :param mssql_conn_id: reference to a specific mssql hook

    .. warning::
        The `mssql_table` parameter has been deprecated. Use `target_table_name` instead.
    """

    template_fields: Sequence[str] = (
        *BigQueryToSqlBaseOperator.template_fields,
        "source_project_dataset_table",
    )
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        source_project_dataset_table: str,
        mssql_table: str | None = None,
        target_table_name: str | None = None,
        mssql_conn_id: str = "mssql_default",
        **kwargs,
    ) -> None:
        if mssql_table is not None:
            warnings.warn(
                "The `mssql_table` parameter has been deprecated. Use `target_table_name` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )

            if target_table_name is not None:
                raise ValueError(
                    f"Cannot set both arguments: mssql_table={mssql_table!r} and "
                    f"target_table_name={target_table_name!r}."
                )

            target_table_name = mssql_table

        try:
            _, dataset_id, table_id = source_project_dataset_table.split(".")
        except ValueError:
            raise ValueError(
                f"Could not parse {source_project_dataset_table} as <project>.<dataset>.<table>"
            ) from None
        super().__init__(
            target_table_name=target_table_name,
            dataset_table=f"{dataset_id}.{table_id}",
            **kwargs,
        )
        self.mssql_conn_id = mssql_conn_id
        self.source_project_dataset_table = source_project_dataset_table

    @cached_property
    def mssql_hook(self) -> MsSqlHook:
        return MsSqlHook(schema=self.database, mssql_conn_id=self.mssql_conn_id)

    def get_sql_hook(self) -> MsSqlHook:
        return self.mssql_hook

    def persist_links(self, context: Context) -> None:
        project_id, dataset_id, table_id = self.source_project_dataset_table.split(".")
        BigQueryTableLink.persist(
            context=context,
            dataset_id=dataset_id,
            project_id=project_id,
            table_id=table_id,
        )
