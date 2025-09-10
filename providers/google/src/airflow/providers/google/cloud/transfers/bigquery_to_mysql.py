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
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.transfers.bigquery_to_sql import BigQueryToSqlBaseOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

if TYPE_CHECKING:
    from airflow.providers.openlineage.extractors import OperatorLineage


class BigQueryToMySqlOperator(BigQueryToSqlBaseOperator):
    """
    Fetch data from a BigQuery table (alternatively fetch selected columns) and insert it into a MySQL table.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:BigQueryToMySqlOperator`

    :param mysql_table: target MySQL table, use dot notation to target a
        specific database. It is deprecated: use target_table_name instead. (templated)
    :param target_table_name: target MySQL table. It takes precedence over mysql_table. (templated)
    :param mysql_conn_id: Reference to :ref:`mysql connection id <howto/connection:mysql>`.

    .. warning::
        The `mysql_table` parameter has been deprecated. Use `target_table_name` instead.
    """

    template_fields: Sequence[str] = (*BigQueryToSqlBaseOperator.template_fields, "dataset_id", "table_id")

    def __init__(
        self,
        *,
        mysql_table: str | None = None,
        target_table_name: str | None = None,
        mysql_conn_id: str = "mysql_default",
        dataset_id: str | None = None,
        table_id: str | None = None,
        **kwargs,
    ) -> None:
        if mysql_table is not None:
            warnings.warn(
                "The `mysql_table` parameter has been deprecated. Use `target_table_name` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )

            if target_table_name is not None:
                raise ValueError(
                    f"Cannot set both arguments: mysql_table={mysql_table!r} and "
                    f"target_table_name={target_table_name!r}."
                )

            target_table_name = mysql_table

        super().__init__(
            target_table_name=target_table_name, dataset_id=dataset_id, table_id=table_id, **kwargs
        )
        self.mysql_conn_id = mysql_conn_id

    @cached_property
    def mysql_hook(self) -> MySqlHook:
        return MySqlHook(schema=self.database, mysql_conn_id=self.mysql_conn_id)

    def get_sql_hook(self) -> MySqlHook:
        return self.mysql_hook

    def execute(self, context):
        # Set source_project_dataset_table here, after hooks are initialized and project_id is available
        project_id = self.bigquery_hook.project_id
        self.source_project_dataset_table = f"{project_id}.{self.dataset_id}.{self.table_id}"
        return super().execute(context)

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage | None:
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.google.cloud.openlineage.utils import (
            BIGQUERY_NAMESPACE,
            get_facets_from_bq_table_for_given_fields,
            get_identity_column_lineage_facet,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        if not self.bigquery_hook:
            self.bigquery_hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )

        try:
            table_obj = self.bigquery_hook.get_client().get_table(self.source_project_dataset_table)
        except Exception:
            self.log.debug(
                "OpenLineage: could not fetch BigQuery table %s",
                self.source_project_dataset_table,
                exc_info=True,
            )
            return OperatorLineage()

        if self.selected_fields:
            if isinstance(self.selected_fields, str):
                bigquery_field_names = list(self.selected_fields)
            else:
                bigquery_field_names = self.selected_fields
        else:
            bigquery_field_names = [f.name for f in getattr(table_obj, "schema", [])]

        input_dataset = Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=self.source_project_dataset_table,
            facets=get_facets_from_bq_table_for_given_fields(table_obj, bigquery_field_names),
        )

        db_info = self.mysql_hook.get_openlineage_database_info(self.mysql_hook.get_conn())
        namespace = f"{db_info.scheme}://{db_info.authority}"

        output_name = f"{self.database}.{self.target_table_name}"

        column_lineage_facet = get_identity_column_lineage_facet(
            bigquery_field_names, input_datasets=[input_dataset]
        )

        output_facets = column_lineage_facet or {}
        output_dataset = Dataset(namespace=namespace, name=output_name, facets=output_facets)

        return OperatorLineage(inputs=[input_dataset], outputs=[output_dataset])
