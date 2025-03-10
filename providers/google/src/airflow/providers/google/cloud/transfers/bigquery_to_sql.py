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
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data

if TYPE_CHECKING:
    from airflow.providers.common.sql.hooks.sql import DbApiHook
    from airflow.utils.context import Context


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

    def execute(self, context: Context) -> None:
        big_query_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        self.persist_links(context)
        sql_hook = self.get_sql_hook()
        for rows in bigquery_get_data(
            self.log,
            self.dataset_id,
            self.table_id,
            big_query_hook,
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
