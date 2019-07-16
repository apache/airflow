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

import json
import decimal

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.mssql_hook import MsSqlHook
from tempfile import NamedTemporaryFile


class MsSqlToGoogleCloudStorageOperator(BaseOperator):
    """
    Copy data from Microsoft SQL Server to Google Cloud Storage
    in JSON format.

    :param sql: The SQL to execute on the MSSQL table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size, e.g. filename='data/customers/export_{}.json'.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MSSQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files.
    :type approx_max_file_size_bytes: long
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param mssql_conn_id: Reference to a specific MSSQL hook.
    :type mssql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str

    **Example**:
        The following operator will export data from the Customers table
        within the given MSSQL Database and then upload it to the
        'mssql-export' GCS bucket (along with a schema file). ::

            export_customers = MsSqlToGoogleCloudStorageOperator(
                task_id='export_customers',
                sql='SELECT * FROM dbo.Customers;',
                bucket='mssql-export',
                filename='data/customers/export.json',
                schema_filename='schemas/export.json',
                mssql_conn_id='mssql_default',
                google_cloud_storage_conn_id='google_cloud_default',
                dag=dag
            )
    """

    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#e0a98c'

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 filename,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000,
                 gzip=False,
                 mssql_conn_id='mssql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.gzip = gzip
        self.mssql_conn_id = mssql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        cursor = self._query_mssql()
        files_to_upload = self._write_local_data_files(cursor)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            files_to_upload.update(self._write_local_schema_file(cursor))

        # Flush all files before uploading
        for file_handle in files_to_upload.values():
            file_handle.flush()

        self._upload_to_gcs(files_to_upload)

        # Close all temp file handles
        for file_handle in files_to_upload.values():
            file_handle.close()

    def _query_mssql(self):
        """
        Queries MSSQL and returns a cursor of results.

        :return: mssql cursor
        """
        mssql = MsSqlHook(mssql_conn_id=self.mssql_conn_id)
        conn = mssql.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql)
        return cursor

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        schema = list(map(lambda schema_tuple: schema_tuple[0].replace(' ', '_'), cursor.description))
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handles = {self.filename.format(file_no): tmp_file_handle}

        for row in cursor:
            # Convert if needed
            row = map(self.convert_types, row)
            row_dict = dict(zip(schema, row))

            s = json.dumps(row_dict, sort_keys=True)
            s = s.encode('utf-8')
            tmp_file_handle.write(s)

            # Append newline to make dumps BQ compatible
            tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                tmp_file_handles[self.filename.format(file_no)] = tmp_file_handle

        return tmp_file_handles

    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system.

        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema = []
        for field in cursor.description:
            # See PEP 249 for details about the description tuple.
            field_name = field[0].replace(' ', '_')  # Clean spaces
            field_type = self.type_map(field[1])
            field_mode = 'NULLABLE'  # pymssql doesn't support field_mode

            schema.append({
                'name': field_name,
                'type': field_type,
                'mode': field_mode,
            })

        self.log.info('Using schema for %s: %s', self.schema_filename, schema)
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        s = json.dumps(schema, sort_keys=True)
        s = s.encode('utf-8')
        tmp_schema_file_handle.write(s)
        return {self.schema_filename: tmp_schema_file_handle}

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for object_name, tmp_file_handle in files_to_upload.items():
            hook.upload(self.bucket, object_name, tmp_file_handle.name, 'application/json',
                        (self.gzip if object_name != self.schema_filename else False))

    @classmethod
    def convert_types(cls, value):
        """
        Takes a value from MSSQL, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery.
        """
        if isinstance(value, decimal.Decimal):
            return float(value)
        else:
            return value

    @classmethod
    def type_map(cls, mssql_type):
        """
        Helper function that maps from MSSQL fields to BigQuery fields. Used
        when a schema_filename is set.
        """
        d = {
            3: 'INTEGER',
            4: 'TIMESTAMP',
            5: 'NUMERIC'
        }
        return d[mssql_type] if mssql_type in d else 'STRING'
