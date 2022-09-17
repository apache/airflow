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

from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.links.bigquery import BigQueryTableLink
from airflow.providers.google.cloud.utils.bigquery_get_data import bigquery_get_data
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BigQueryToMsSqlOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and insert that data into a MSSQL table.


    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'`` and passed through this form
        to MSSQL

    **Example**: ::

       transfer_data = BigQueryToMsSqlOperator(
            task_id='task_id',
            source_project_dataset_table='my-project.mydataset.mytable',
            mssql_table='dest_table_name',
            replace=True,
        )

    :param source_project_dataset_table: A dotted ``<project>.<dataset>.<table>``:
        the big query table of origin
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :param gcp_conn_id: reference to a specific Google Cloud hook.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param mssql_conn_id: reference to a specific mssql hook
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

    template_fields: Sequence[str] = ('source_project_dataset_table', 'mssql_table', 'impersonation_chain')
    operator_extra_links = (BigQueryTableLink(),)

    def __init__(
        self,
        *,
        source_project_dataset_table: str,
        mssql_table: str,
        selected_fields: list[str] | str | None = None,
        gcp_conn_id: str = 'google_cloud_default',
        mssql_conn_id: str = 'mssql_default',
        database: str | None = None,
        delegate_to: str | None = None,
        replace: bool = False,
        batch_size: int = 1000,
        location: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.mssql_conn_id = mssql_conn_id
        self.database = database
        self.mssql_table = mssql_table
        self.replace = replace
        self.delegate_to = delegate_to
        self.batch_size = batch_size
        self.location = location
        self.impersonation_chain = impersonation_chain
        try:
            _, self.dataset_id, self.table_id = source_project_dataset_table.split('.')
        except ValueError:
            raise ValueError(
                f'Could not parse {source_project_dataset_table} as <project>.<dataset>.<table>'
            ) from None
        self.source_project_dataset_table = source_project_dataset_table

    def execute(self, context: Context) -> None:
        big_query_hook = BigQueryHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        project_id, dataset_id, table_id = self.source_project_dataset_table.split('.')
        BigQueryTableLink.persist(
            context=context,
            task_instance=self,
            dataset_id=dataset_id,
            project_id=project_id,
            table_id=table_id,
        )
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database)
        for rows in bigquery_get_data(
            self.log,
            self.dataset_id,
            self.table_id,
            big_query_hook,
            self.batch_size,
            self.selected_fields,
        ):
            mssql_hook.insert_rows(
                table=self.mssql_table,
                rows=rows,
                target_fields=self.selected_fields,
                replace=self.replace,
            )
