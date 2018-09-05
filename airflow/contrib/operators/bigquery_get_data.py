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

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryGetDataOperator(BaseOperator):
    """
    Fetches the data from a BigQuery table (alternatively fetch data for selected columns)
    and returns data in a python list. The number of elements in the returned list will
    be equal to the number of rows fetched. Each element in the list will again be a list
    where element would represent the columns values for that row.

    **Example Result**: ``[['Tony', '10'], ['Mike', '20'], ['Steve', '15']]``

    .. note::
        If you pass fields to ``selected_fields`` which are in different order than the
        order of columns already in
        BQ table, the data will still be in the order of BQ table.
        For example if the BQ table has 3 columns as
        ``[A,B,C]`` and you pass 'B,A' in the ``selected_fields``
        the data would still be of the form ``'A,B'``.

    **Example**: ::

        get_data = BigQueryGetDataOperator(
            task_id='get_data_from_bq',
            dataset_id='test_dataset',
            table_id='Transaction_partitions',
            max_results='100',
            selected_fields='DATE',
            bigquery_conn_id='airflow-service-account'
        )

    :param dataset_id: The dataset ID of the requested table. (templated)
    :type dataset_id: string
    :param table_id: The table ID of the requested table. (templated)
    :type table_id: string
    :param max_results: The maximum number of records (rows) to be fetched
        from the table. (templated)
    :type max_results: string
    :param selected_fields: List of fields to return (comma-separated). If
        unspecified, all fields are returned.
    :type selected_fields: string
    :param bigquery_conn_id: reference to a specific BigQuery hook.
    :type bigquery_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    """
    template_fields = ('dataset_id', 'table_id', 'max_results')
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self,
                 dataset_id,
                 table_id,
                 max_results='100',
                 selected_fields=None,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(BigQueryGetDataOperator, self).__init__(*args, **kwargs)
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.max_results = max_results
        self.selected_fields = selected_fields
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        self.log.info('Fetching Data from:')
        self.log.info('Dataset: %s ; Table: %s ; Max Results: %s',
                      self.dataset_id, self.table_id, self.max_results)

        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)

        conn = hook.get_conn()
        cursor = conn.cursor()
        response = cursor.get_tabledata(dataset_id=self.dataset_id,
                                        table_id=self.table_id,
                                        max_results=self.max_results,
                                        selected_fields=self.selected_fields)

        self.log.info('Total Extracted rows: %s', response['totalRows'])
        rows = response['rows']

        table_data = []
        for dict_row in rows:
            single_row = []
            for fields in dict_row['f']:
                single_row.append(fields['v'])
            table_data.append(single_row)

        return table_data
