# -*- coding: utf-8 -*-
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
"""
This module contains Google BigQuery to MySQL operator.
"""
from typing import Optional

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.gcp.hooks.bigquery import BigQueryHook
from airflow.hooks.mysql_hook import MySqlHook


class BigQueryToMySqlOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and insert that data into a MySQL table.


    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'`` and passed through this form
        to MySQL

    **Example**: ::

       transfer_data = BigQueryToMySqlOperator(
            task_id='task_id',
            dataset_table='origin_bq_table',
            mysql_table='dest_table_name',
            replace=True,
        )

    :param dataset_table: A dotted ``<dataset>.<table>``: the big query table of origin
    :type dataset_table: str
    :param max_results: The maximum number of records (rows) to be fetched
        from the table. (templated)
    :type max_results: str
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :type selected_fields: str
    :param gcp_conn_id: reference to a specific GCP hook.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param mysql_conn_id: reference to a specific mysql hook
    :type mysql_conn_id: str
    :param database: name of database which overwrite defined one in connection
    :type database: str
    :param replace: Whether to replace instead of insert
    :type replace: bool
    :param batch_size: The number of rows to take in each batch
    :type batch_size: int
    :param location: The location used for the operation.
    :type location: str
    """
    template_fields = ('dataset_id', 'table_id', 'mysql_table')

    @apply_defaults
    def __init__(self,  # pylint:disable=too-many-arguments
                 dataset_table: str,
                 mysql_table: str,
                 selected_fields: Optional[str] = None,
                 gcp_conn_id: str = 'google_cloud_default',
                 mysql_conn_id: str = 'mysql_default',
                 database: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 replace: bool = False,
                 batch_size: int = 1000,
                 location: str = None,
                 *args,
                 **kwargs) -> None:
        super(BigQueryToMySqlOperator, self).__init__(*args, **kwargs)
        self.selected_fields = selected_fields
        self.gcp_conn_id = gcp_conn_id
        self.mysql_conn_id = mysql_conn_id
        self.database = database
        self.mysql_table = mysql_table
        self.replace = replace
        self.delegate_to = delegate_to
        self.batch_size = batch_size
        self.location = location
        try:
            self.dataset_id, self.table_id = dataset_table.split('.')
        except ValueError:
            raise ValueError('Could not parse {} as <dataset>.<table>'
                             .format(dataset_table))

    def _bq_get_data(self):
        self.log.info('Fetching Data from:')
        self.log.info('Dataset: %s ; Table: %s',
                      self.dataset_id, self.table_id)

        hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            location=self.location)

        conn = hook.get_conn()
        cursor = conn.cursor()
        i = 0
        while True:
            response = cursor.get_tabledata(dataset_id=self.dataset_id,
                                            table_id=self.table_id,
                                            max_results=self.batch_size,
                                            selected_fields=self.selected_fields,
                                            start_index=i * self.batch_size)

            if 'rows' in response:
                rows = response['rows']
            else:
                self.log.info('Job Finished')
                return

            self.log.info('Total Extracted rows: %s', len(rows) + i * self.batch_size)

            table_data = []
            for dict_row in rows:
                single_row = []
                for fields in dict_row['f']:
                    single_row.append(fields['v'])
                table_data.append(single_row)

            yield table_data
            i += 1

    def execute(self, context):
        mysql_hook = MySqlHook(schema=self.database, mysql_conn_id=self.mysql_conn_id)
        for rows in self._bq_get_data():
            mysql_hook.insert_rows(self.mysql_table, rows, replace=self.replace)
