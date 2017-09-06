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

import json
import logging

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageToBigQueryOperator(BaseOperator):
    """
    Loads files from Google cloud storage into BigQuery.
    """
    template_fields = ('bucket', 'source_objects',
                       'schema_object', 'destination_project_dataset_table')
    template_ext = ('.sql',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
        self,
        bucket,
        source_objects,
        destination_project_dataset_table,
        schema_fields=None,
        schema_object=None,
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        skip_leading_rows=0,
        write_disposition='WRITE_EMPTY',
        field_delimiter=',',
        max_bad_records=0,
        quote_character=None,
        allow_quoted_newlines=False,
        max_id_key=None,
        bigquery_conn_id='bigquery_default',
        google_cloud_storage_conn_id='google_cloud_storage_default',
        delegate_to=None,
        schema_update_options=(),
        src_fmt_configs={},
        *args,
        **kwargs):
        """
        The schema to be used for the BigQuery table may be specified in one of
        two ways. You may either directly pass the schema fields in, or you may
        point the operator to a Google cloud storage object name. The object in
        Google cloud storage must be a JSON file with the schema fields in it.

        :param bucket: The bucket to load from.
        :type bucket: string
        :param source_objects: List of Google cloud storage URIs to load from.
            If source_format is 'DATASTORE_BACKUP', the list must only contain a single URI.
        :type object: list
        :param destination_project_dataset_table: The dotted (<project>.)<dataset>.<table>
            BigQuery table to load data into. If <project> is not included, project will
            be the project defined in the connection json.
        :type destination_project_dataset_table: string
        :param schema_fields: If set, the schema field list as defined here:
            https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load
            Should not be set when source_format is 'DATASTORE_BACKUP'.
        :type schema_fields: list
        :param schema_object: If set, a GCS object path pointing to a .json file that
            contains the schema for the table.
        :param schema_object: string
        :param source_format: File format to export.
        :type source_format: string
        :param create_disposition: The create disposition if the table doesn't exist.
        :type create_disposition: string
        :param skip_leading_rows: Number of rows to skip when loading from a CSV.
        :type skip_leading_rows: int
        :param write_disposition: The write disposition if the table already exists.
        :type write_disposition: string
        :param field_delimiter: The delimiter to use when loading from a CSV.
        :type field_delimiter: string
        :param max_bad_records: The maximum number of bad records that BigQuery can
            ignore when running the job.
        :type max_bad_records: int
        :param quote_character: The value that is used to quote data sections in a CSV file.
        :type quote_character: string
        :param allow_quoted_newlines: Whether to allow quoted newlines (true) or not (false).
        :type allow_quoted_newlines: boolean
        :param max_id_key: If set, the name of a column in the BigQuery table
            that's to be loaded. Thsi will be used to select the MAX value from
            BigQuery after the load occurs. The results will be returned by the
            execute() command, which in turn gets stored in XCom for future
            operators to use. This can be helpful with incremental loads--during
            future executions, you can pick up from the max ID.
        :type max_id_key: string
        :param bigquery_conn_id: Reference to a specific BigQuery hook.
        :type bigquery_conn_id: string
        :param google_cloud_storage_conn_id: Reference to a specific Google
            cloud storage hook.
        :type google_cloud_storage_conn_id: string
        :param delegate_to: The account to impersonate, if any. For this to
            work, the service account making the request must have domain-wide
            delegation enabled.
        :type delegate_to: string
        :param schema_update_options: Allows the schema of the desitination 
            table to be updated as a side effect of the load job.
        :type schema_update_options: list
        :param src_fmt_configs: configure optional fields specific to the source format
        :type src_fmt_configs: dict
        """
        super(GoogleCloudStorageToBigQueryOperator, self).__init__(*args, **kwargs)

        # GCS config
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        # BQ config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.create_disposition = create_disposition
        self.skip_leading_rows = skip_leading_rows
        self.write_disposition = write_disposition
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.allow_quoted_newlines = allow_quoted_newlines

        self.max_id_key = max_id_key
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.schema_update_options = schema_update_options
        self.src_fmt_configs = src_fmt_configs

    def execute(self, context):
        bq_hook = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                               delegate_to=self.delegate_to)

        if not self.schema_fields and self.schema_object \
                                  and self.source_format != 'DATASTORE_BACKUP':
            gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to)
            schema_fields = json.loads(gcs_hook.download(
                self.bucket,
                self.schema_object).decode("utf-8"))
        else:
            schema_fields = self.schema_fields

        source_uris = ['gs://{}/{}'.format(self.bucket, source_object)
                       for source_object in self.source_objects]
        conn = bq_hook.get_conn()
        cursor = conn.cursor()
        cursor.run_load(
            destination_project_dataset_table=self.destination_project_dataset_table,
            schema_fields=schema_fields,
            source_uris=source_uris,
            source_format=self.source_format,
            create_disposition=self.create_disposition,
            skip_leading_rows=self.skip_leading_rows,
            write_disposition=self.write_disposition,
            field_delimiter=self.field_delimiter,
            max_bad_records=self.max_bad_records,
            quote_character=self.quote_character,
            allow_quoted_newlines=self.allow_quoted_newlines,
            schema_update_options=self.schema_update_options,
            src_fmt_configs=self.src_fmt_configs)

        if self.max_id_key:
            cursor.execute('SELECT MAX({}) FROM {}'.format(
                self.max_id_key,
                self.destination_project_dataset_table))
            row = cursor.fetchone()
            max_id = row[0] if row[0] else 0
            logging.info('Loaded BQ data with max {}.{}={}'.format(
                self.destination_project_dataset_table,
                self.max_id_key, max_id))
            return max_id
