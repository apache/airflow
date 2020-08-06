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
This module contains a Google Bigquery sensor.
"""
from typing import Optional

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class BigQueryTableExistenceSensor(BaseSensorOperator):
    """
    Checks for the existence of a table in Google Bigquery.

    :param project_id: The Google cloud project in which to look for the table.
        The connection supplied to the hook must provide
        access to the specified project.
    :type project_id: str
    :param dataset_id: The name of the dataset in which to look for the table.
        storage bucket.
    :type dataset_id: str
    :param table_id: The name of the table to check the existence of.
    :type table_id: str
    :param bigquery_conn_id: The connection ID to use when connecting to
        Google BigQuery.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must
        have domain-wide delegation enabled.
    :type delegate_to: str
    """
    template_fields = ('project_id', 'dataset_id', 'table_id',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self, *,
                 project_id: str,
                 dataset_id: str,
                 table_id: str,
                 bigquery_conn_id: str = 'google_cloud_default',
                 delegate_to: Optional[str] = None,
                 **kwargs) -> None:

        super().__init__(**kwargs)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        table_uri = '{0}:{1}.{2}'.format(self.project_id, self.dataset_id, self.table_id)
        self.log.info('Sensor checks existence of table: %s', table_uri)
        hook = BigQueryHook(
            bigquery_conn_id=self.bigquery_conn_id,
            delegate_to=self.delegate_to)
        return hook.table_exists(
            project_id=self.project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id)
