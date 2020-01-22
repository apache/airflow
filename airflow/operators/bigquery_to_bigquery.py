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
This module contains Google BigQuery to BigQuery operator.
"""
import warnings
from typing import Dict, List, Optional, Union

from airflow.gcp.hooks.bigquery import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class BigQueryToBigQueryOperator(BaseOperator):
    """
    Copies data from one BigQuery table to another.

    .. seealso::
        For more details about these parameters:
        https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.copy

    :param source_project_dataset_tables: One or more
        dotted ``(project:|project.)<dataset>.<table>`` BigQuery tables to use as the
        source data. If ``<project>`` is not included, project will be the
        project defined in the connection json. Use a list if there are multiple
        source tables. (templated)
    :type source_project_dataset_tables: list|string
    :param destination_project_dataset_table: The destination BigQuery
        table. Format is: ``(project:|project.)<dataset>.<table>`` (templated)
    :type destination_project_dataset_table: str
    :param write_disposition: The write disposition if the table already exists.
    :type write_disposition: str
    :param create_disposition: The create disposition if the table doesn't exist.
    :type create_disposition: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param bigquery_conn_id: (Deprecated) The connection ID used to connect to Google Cloud Platform.
        This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type bigquery_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param labels: a dictionary containing labels for the job/query,
        passed to BigQuery
    :type labels: dict
    :param encryption_configuration: [Optional] Custom encryption configuration (e.g., Cloud KMS keys).
        **Example**: ::

            encryption_configuration = {
                "kmsKeyName": "projects/testp/locations/us/keyRings/test-kr/cryptoKeys/test-key"
            }
    :type encryption_configuration: dict
    :param location: The location used for the operation.
    :type location: str
    """
    template_fields = ('source_project_dataset_tables',
                       'destination_project_dataset_table', 'labels')
    template_ext = ('.sql',)
    ui_color = '#e6f0e4'

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 source_project_dataset_tables: Union[List[str], str],
                 destination_project_dataset_table: str,
                 write_disposition: str = 'WRITE_EMPTY',
                 create_disposition: str = 'CREATE_IF_NEEDED',
                 gcp_conn_id: str = 'google_cloud_default',
                 bigquery_conn_id: Optional[str] = None,
                 delegate_to: Optional[str] = None,
                 labels: Optional[Dict] = None,
                 encryption_configuration: Optional[Dict] = None,
                 location: Optional[str] = None,
                 *args,
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)

        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = bigquery_conn_id

        self.source_project_dataset_tables = source_project_dataset_tables
        self.destination_project_dataset_table = destination_project_dataset_table
        self.write_disposition = write_disposition
        self.create_disposition = create_disposition
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.labels = labels
        self.encryption_configuration = encryption_configuration
        self.location = location

    def execute(self, context):
        self.log.info(
            'Executing copy of %s into: %s',
            self.source_project_dataset_tables, self.destination_project_dataset_table
        )
        hook = BigQueryHook(bigquery_conn_id=self.gcp_conn_id,
                            delegate_to=self.delegate_to,
                            location=self.location)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_copy(
            source_project_dataset_tables=self.source_project_dataset_tables,
            destination_project_dataset_table=self.destination_project_dataset_table,
            write_disposition=self.write_disposition,
            create_disposition=self.create_disposition,
            labels=self.labels,
            encryption_configuration=self.encryption_configuration)
