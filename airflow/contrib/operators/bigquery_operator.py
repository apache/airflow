# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryOperator(BaseOperator):
    """
    Executes BigQuery SQL queries in a specific BigQuery database
    """
    template_fields = ('bql', 'destination_dataset_table')
    template_ext = ('.sql',)
    ui_color = '#e4f0e8'

    @apply_defaults
    def __init__(self,
                 bql,
                 destination_dataset_table = False,
                 write_disposition='WRITE_EMPTY',
                 allow_large_results=False,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 udf_config=False,
                 use_legacy_sql=True,
                 *args,
                 **kwargs):
        """
        Create a new BigQueryOperator.

        :param bql: the sql code to be executed
        :type bql: Can receive a str representing a sql statement,
            a list of str (sql statements), or reference to a template file.
            Template reference are recognized by str ending in '.sql'
        :param destination_dataset_table: A dotted
            (<project>.|<project>:)<dataset>.<table> that, if set, will store the results
            of the query.
        :type destination_dataset_table: string
        :param bigquery_conn_id: reference to a specific BigQuery hook.
        :type bigquery_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: string
        :param udf_config: The User Defined Function configuration for the query.
            See https://cloud.google.com/bigquery/user-defined-functions for details.
        :type udf_config: list
        :param use_legacy_sql: Whether to use legacy SQL (true) or standard SQL (false).
        :type use_legacy_sql: boolean
        """
        super(BigQueryOperator, self).__init__(*args, **kwargs)
        self.bql = bql
        self.destination_dataset_table = destination_dataset_table
        self.write_disposition = write_disposition
        self.allow_large_results = allow_large_results
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.udf_config = udf_config
        self.use_legacy_sql = use_legacy_sql

    def execute(self, context):
        logging.info('Executing: %s', str(self.bql))
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_query(self.bql, self.destination_dataset_table, self.write_disposition,
                         self.allow_large_results, self.udf_config, self.use_legacy_sql)
