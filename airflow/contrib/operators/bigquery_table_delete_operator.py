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
This module contains Google BigQuery table delete operator.
"""
import warnings
from typing import Optional

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryTableDeleteOperator(BaseOperator):
    """
    Deletes BigQuery tables

    :param deletion_dataset_table: A dotted
        ``(<project>.|<project>:)<dataset>.<table>`` that indicates which table
        will be deleted. (templated)
    :type deletion_dataset_table: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param ignore_if_missing: if True, then return success even if the
        requested table does not exist.
    :type ignore_if_missing: bool
    """
    template_fields = ('deletion_dataset_table',)
    ui_color = '#ffd1dc'

    @apply_defaults
    def __init__(self,
                 deletion_dataset_table: str,
                 gcp_conn_id: str = 'google_cloud_default',
                 bigquery_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 ignore_if_missing: bool = False,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.deletion_dataset_table = deletion_dataset_table
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.ignore_if_missing = ignore_if_missing

    def execute(self, context):
        self.log.info('Deleting: %s', self.deletion_dataset_table)
        hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_table_delete(
            deletion_dataset_table=self.deletion_dataset_table,
            ignore_if_missing=self.ignore_if_missing)
