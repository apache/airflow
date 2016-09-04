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


class BigQueryToCloudStorageOperator(BaseOperator):
    """
    Transfers a BigQuery table to a Google Cloud Storage bucket.
    """
    template_fields = ('source_project_dataset_table', 'destination_cloud_storage_uris')
    template_ext = ('.sql',)
    ui_color = '#e4e6f0'

    @apply_defaults
    def __init__(
        self,
        source_project_dataset_table,
        destination_cloud_storage_uris,
        compression='NONE',
        export_format='CSV',
        field_delimiter=',',
        print_header=True,
        bigquery_conn_id='bigquery_default',
        delegate_to=None,
        *args,
        **kwargs):
        """
        Create a new BigQueryToCloudStorage to move data from BigQuery to
        Google Cloud Storage.  See here:

        https://cloud.google.com/bigquery/docs/reference/v2/jobs

        For more details about these parameters.

        :param source_project_dataset_table: The dotted
            (<project>.|<project>:)<dataset>.<table> BigQuery table to use as the source
            data. If <project> is not included, project will be the project defined in
            the connection json.
        :type source_project_dataset_table: string
        :param destination_cloud_storage_uris: The destination Google Cloud
            Storage URI (e.g. gs://some-bucket/some-file.txt). Follows
            convention defined here:
            https://cloud.google.com/bigquery/exporting-data-from-bigquery#exportingmultiple
        :type destination_cloud_storage_uris: list
        :param compression: Type of compression to use.
        :type compression: string
        :param export_format: File format to export.
        :type field_delimiter: string
        :param field_delimiter: The delimiter to use when extracting to a CSV.
        :type field_delimiter: string
        :param print_header: Whether to print a header for a CSV file extract.
        :type print_header: boolean
        :param bigquery_conn_id: reference to a specific BigQuery hook.
        :type bigquery_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: string
        """
        super(BigQueryToCloudStorageOperator, self).__init__(*args, **kwargs)
        self.source_project_dataset_table = source_project_dataset_table
        self.destination_cloud_storage_uris = destination_cloud_storage_uris
        self.compression = compression
        self.export_format = export_format
        self.field_delimiter = field_delimiter
        self.print_header = print_header
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        logging.info('Executing extract of %s into: %s',
                     self.source_project_dataset_table,
                     self.destination_cloud_storage_uris)
        hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                            delegate_to=self.delegate_to)
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.run_extract(
            self.source_project_dataset_table,
            self.destination_cloud_storage_uris,
            self.compression,
            self.export_format,
            self.field_delimiter,
            self.print_header)
