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
"""Base operator for BigQuery to SQL operators."""

from __future__ import annotations

import abc
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data
from airflow.providers.google.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.utils.context import Context


class BigQueryToSqlOpenLineageMixin:
    """
    Mixin to provide shared OpenLineage facet extraction for BigQueryToSql operators.
    """

    def get_openlineage_facets_on_complete(self, task_instance):
        from airflow.providers.common.compat.openlineage.facet import Dataset
        from airflow.providers.google.cloud.openlineage.utils import (
            BIGQUERY_NAMESPACE,
            get_facets_from_bq_table_for_given_fields,
            get_identity_column_lineage_facet,
        )
        from airflow.providers.openlineage.extractors import OperatorLineage

        # Ensure bigquery_hook is initialized
        if not getattr(self, "bigquery_hook", None):
            self.bigquery_hook = BigQueryHook(
                gcp_conn_id=self.gcp_conn_id,
                location=self.location,
                impersonation_chain=self.impersonation_chain,
            )

        # Determine source_project_dataset_table
        source_project_dataset_table = getattr(self, "source_project_dataset_table", None)
        if not source_project_dataset_table:
            # Try to construct from project_id, dataset_id, table_id
            project_id = getattr(self.bigquery_hook, "project_id", None)
            dataset_id = getattr(self, "dataset_id", None)
            table_id = getattr(self, "table_id", None)
            if project_id and dataset_id and table_id:
                source_project_dataset_table = f"{project_id}.{dataset_id}.{table_id}"
            else:
                return OperatorLineage()

        try:
            table_obj = self.bigquery_hook.get_client().get_table(source_project_dataset_table)
        except Exception:
            self.log.debug(
                "OpenLineage: could not fetch BigQuery table %s",
                source_project_dataset_table,
                exc_info=True,
            )
            return OperatorLineage()

        selected_fields = getattr(self, "selected_fields", None)
        if selected_fields:
            if isinstance(selected_fields, str):
                bigquery_field_names = list(selected_fields)
            else:
                bigquery_field_names = selected_fields
        else:
            bigquery_field_names = [f.name for f in getattr(table_obj, "schema", [])]

        input_dataset = Dataset(
            namespace=BIGQUERY_NAMESPACE,
            name=source_project_dataset_table,
            facets=get_facets_from_bq_table_for_given_fields(table_obj, bigquery_field_names),
        )

        # Get SQL hook and output table info
        sql_hook = self.get_sql_hook()
        db_info = sql_hook.get_openlineage_database_info(sql_hook.get_conn())
        namespace = f"{db_info.scheme}://{db_info.authority}"

        # Output table name logic
        target_table_name = getattr(self, "target_table_name", None)
        database = getattr(self, "database", None)
        output_name = None

        # Try to get schema if available
        get_schema = getattr(sql_hook, "get_openlineage_default_schema", None)
        schema_name = get_schema() if callable(get_schema) else None

        # Special handling for MySQL: ignore schema_name in output_name
        if db_info.scheme == "mysql":
            if target_table_name and database:
                output_name = f"{database}.{target_table_name}"
            elif target_table_name:
                output_name = target_table_name
            else:
                output_name = ""
        else:
            # Compose output_name based on available attributes for other DBs
            if target_table_name and "." in target_table_name and schema_name:
                # If schema is available and target_table_name is schema.table
                _, table_name = target_table_name.split(".", 1)
                if database:
                    output_name = f"{database}.{schema_name}.{table_name}"
                else:
                    output_name = f"{schema_name}.{table_name}"
            elif target_table_name and schema_name and database:
                output_name = f"{database}.{schema_name}.{target_table_name}"
            elif target_table_name and database:
                output_name = f"{database}.{target_table_name}"
            elif target_table_name:
                output_name = target_table_name
            else:
                output_name = ""

        column_lineage_facet = get_identity_column_lineage_facet(
            bigquery_field_names, input_datasets=[input_dataset]
        )
        output_facets = column_lineage_facet or {}
        output_dataset = Dataset(namespace=namespace, name=output_name, facets=output_facets)

        return OperatorLineage(inputs=[input_dataset], outputs=[output_dataset])


class BigQueryToSqlBaseOperator(BaseOperator):
    """
    Fetch data from a BigQuery table (alternatively fetch selected columns) and insert it into an SQL table.

    This is a BaseOperator; an abstract class. Refer to children classes
    which are related to specific SQL databases (MySQL, MsSQL, Postgres...).

    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'`` and passed through this form
        to the SQL database.

    :param dataset_table: A dotted ``<dataset>.<table>``: the big query table of origin
    :param target_table_name: target SQL table
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :param gcp_conn_id: reference to a specific Google Cloud hook.
    :param database: name of database which overwrite defined one in connection
    :param replace: Whether to replace instead of insert
    :param batch_size: The number of rows to take in each batch
    :param location: The location used for the operation.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "target_table_name",
        "impersonation_chain",
        "dataset_id",
        "table_id",
    )

    def __init__(
        self,
        *,
        dataset_table: str,
        target_table_name: str | None,
        selected_fields: list[str] | str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        database: str | None = None,
        replace: bool = False,
        batch_size: int = 1000,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        dataset_id: str | None = None,
        table_id: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.database = database
        self.target_table_name = target_table_name
        self.replace = replace
        self.batch_size = batch_size
        self.location = location
        self.impersonation_chain = impersonation_chain
        self.dataset_id = dataset_id
        self.table_id = table_id
        try:
            self.dataset_id, self.table_id = dataset_table.split(".")
        except ValueError:
            raise ValueError(f"Could not parse {dataset_table} as <dataset>.<table>") from None

    @abc.abstractmethod
    def get_sql_hook(self) -> DbApiHook:
        """Return a concrete SQL Hook (a PostgresHook for instance)."""

    def persist_links(self, context: Context) -> None:
        """Persist the connection to the SQL provider."""

    @cached_property
    def bigquery_hook(self) -> BigQueryHook:
        return BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )

    def execute(self, context: Context) -> None:
        self.persist_links(context)
        sql_hook = self.get_sql_hook()
        for rows in bigquery_get_data(
            self.log,
            self.dataset_id,
            self.table_id,
            self.bigquery_hook,
            self.batch_size,
            self.selected_fields,
        ):
            sql_hook.insert_rows(
                table=self.target_table_name,
                rows=rows,
                target_fields=self.selected_fields,
                replace=self.replace,
                commit_every=self.batch_size,
            )
