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
from typing import Optional, Sequence, Union

from google.cloud.bigquery.table import TableReference

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


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
            dataset_table='origin_bq_table',
            mssql_table='dest_table_name',
            replace=True,
        )

    :param dataset_table: A dotted ``<dataset>.<table>``: the big query table of origin
    :type dataset_table: str
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :type selected_fields: str
    :param gcp_conn_id: reference to a specific Google Cloud hook.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param mssql_conn_id: reference to a specific mssql hook
    :type mssql_conn_id: str
    :param database: name of database which overwrite defined one in connection
    :type database: str
    :param replace: Whether to replace instead of insert
    :type replace: bool
    :param batch_size: The number of rows to take in each batch
    :type batch_size: int
    :param location: The location used for the operation.
    :type location: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = (
        'dataset_id',
        'table_id',
        'mssql_table',
        'impersonation_chain',
    )

    def __init__(
        self,
        *,
        source_project_dataset_table: str,
        mssql_table: str,
        selected_fields: Optional[str] = None,
        gcp_conn_id: str = 'google_cloud_default',
        mssql_conn_id: str = 'mssql_default',
        database: Optional[str] = None,
        delegate_to: Optional[str] = None,
        replace: bool = False,
        batch_size: int = 1000,
        location: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
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
        self.source_project_dataset_table = source_project_dataset_table

    def _bq_get_data(self):

        hook = BigQueryHook(
            bigquery_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            location=self.location,
            impersonation_chain=self.impersonation_chain,
        )
        table_ref = TableReference.from_string(self.source_project_dataset_table)
        self.log.info('Fetching Data from:')
        self.log.info('Dataset: %s, Table: %s', table_ref.dataset_id, table_ref.table_id)

        conn = hook.get_conn()
        cursor = conn.cursor()
        i = 0
        while True:
            response = cursor.get_tabledata(
                dataset_id=table_ref.dataset_id,
                table_id=table_ref.table_id,
                max_results=self.batch_size,
                selected_fields=self.selected_fields,
                start_index=i * self.batch_size,
            )

            if 'rows' not in response:
                self.log.info('Job Finished')
                return

            rows = response['rows']

            self.log.info('Total Extracted rows: %s', len(rows) + i * self.batch_size)

            table_data = []
            table_data = [[fields['v'] for fields in dict_row['f']] for dict_row in rows]

            yield table_data
            i += 1

    def execute(self, context):
        mssql_hook = MsSqlHook(mssql_conn_id=self.mssql_conn_id, schema=self.database)
        for rows in self._bq_get_data():
            mssql_hook.insert_rows(self.mssql_table, rows, replace=self.replace)
