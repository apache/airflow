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
Base operator for SQL to GCS operators.
"""

import abc
import json
import warnings
from tempfile import NamedTemporaryFile

import unicodecsv as csv

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.decorators import apply_defaults


class BaseSQLToGCSOperator(BaseOperator, metaclass=abc.ABCMeta):
    """
    :param sql: The SQL to execute.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google Cloud Storage. A ``{}`` should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from the database.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files (see notes in the
        filename param docs above). This param allows developers to specify the
        file size of the splits. Check https://cloud.google.com/storage/quotas
        to see the maximum allowed file size for a single object.
    :type approx_max_file_size_bytes: long
    :param export_format: Desired format of files to be exported.
    :type export_format: str
    :param field_delimiter: The delimiter to be used for CSV files.
    :type field_delimiter: str
    :param gzip: Option to compress file for upload (does not apply to schemas).
    :type gzip: bool
    :param schema: The schema to use, if any. Should be a list of dict or
        a str. Pass a string if using Jinja template, otherwise, pass a list of
        dict. Examples could be seen: https://cloud.google.com/bigquery/docs
        /schemas#specifying_a_json_schema_file
    :type schema: str or list
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :param parameters: a parameters dict that is substituted at query runtime.
    :type parameters: dict
    """
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename', 'schema', 'parameters')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,  # pylint: disable=too-many-arguments
                 sql,
                 bucket,
                 filename,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000,
                 export_format='json',
                 field_delimiter=',',
                 gzip=False,
                 schema=None,
                 parameters=None,
                 gcp_conn_id='google_cloud_default',
                 google_cloud_storage_conn_id=None,
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.export_format = export_format.lower()
        self.field_delimiter = field_delimiter
        self.gzip = gzip
        self.schema = schema
        self.parameters = parameters
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.parameters = parameters

    def execute(self, context):
        self.log.info("Executing query")
        cursor = self.query()

        self.log.info("Writing local data files")
        files_to_upload = self._write_local_data_files(cursor)
        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            self.log.info("Writing local schema file")
            files_to_upload.append(self._write_local_schema_file(cursor))

        # Flush all files before uploading
        for tmp_file in files_to_upload:
            tmp_file['file_handle'].flush()

        self.log.info("Uploading %d files to GCS.", len(files_to_upload))
        self._upload_to_gcs(files_to_upload)

        self.log.info("Removing local files")
        # Close all temp file handles.
        for tmp_file in files_to_upload:
            tmp_file['file_handle'].close()

    def convert_types(self, schema, col_type_dict, row):
        """Convert values from DBAPI to output-friendly formats."""
        return [
            self.convert_type(value, col_type_dict.get(name))
            for name, value in zip(schema, row)
        ]

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        schema = list(map(lambda schema_tuple: schema_tuple[0], cursor.description))
        col_type_dict = self._get_col_type_dict()
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        if self.export_format == 'csv':
            file_mime_type = 'text/csv'
        else:
            file_mime_type = 'application/json'
        files_to_upload = [{
            'file_name': self.filename.format(file_no),
            'file_handle': tmp_file_handle,
            'file_mime_type': file_mime_type
        }]
        self.log.info("Current file count: %d", len(files_to_upload))

        if self.export_format == 'csv':
            csv_writer = self._configure_csv_file(tmp_file_handle, schema)

        for row in cursor:
            # Convert datetime objects to utc seconds, and decimals to floats.
            # Convert binary type object to string encoded with base64.
            row = self.convert_types(schema, col_type_dict, row)

            if self.export_format == 'csv':
                csv_writer.writerow(row)
            else:
                row_dict = dict(zip(schema, row))

                tmp_file_handle.write(
                    json.dumps(row_dict, sort_keys=True, ensure_ascii=False).encode("utf-8")
                )

                # Append newline to make dumps BigQuery compatible.
                tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit.
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                files_to_upload.append({
                    'file_name': self.filename.format(file_no),
                    'file_handle': tmp_file_handle,
                    'file_mime_type': file_mime_type
                })
                self.log.info("Current file count: %d", len(files_to_upload))
                if self.export_format == 'csv':
                    csv_writer = self._configure_csv_file(tmp_file_handle, schema)

        return files_to_upload

    def _configure_csv_file(self, file_handle, schema):
        """Configure a csv writer with the file_handle and write schema
        as headers for the new file.
        """
        csv_writer = csv.writer(file_handle, encoding='utf-8',
                                delimiter=self.field_delimiter)
        csv_writer.writerow(schema)
        return csv_writer

    @abc.abstractmethod
    def query(self):
        """Execute DBAPI query."""

    @abc.abstractmethod
    def field_to_bigquery(self, field):
        """Convert a DBAPI field to BigQuery schema format."""

    @abc.abstractmethod
    def convert_type(self, value, schema_type):
        """Convert a value from DBAPI to output-friendly formats."""

    def _get_col_type_dict(self):
        """
        Return a dict of column name and column type based on self.schema if not None.
        """
        schema = []
        if isinstance(self.schema, str):
            schema = json.loads(self.schema)
        elif isinstance(self.schema, list):
            schema = self.schema
        elif self.schema is not None:
            self.log.warning('Using default schema due to unexpected type.'
                             'Should be a string or list.')

        col_type_dict = {}
        try:
            col_type_dict = {col['name']: col['type'] for col in schema}
        except KeyError:
            self.log.warning('Using default schema due to missing name or type. Please '
                             'refer to: https://cloud.google.com/bigquery/docs/schemas'
                             '#specifying_a_json_schema_file')
        return col_type_dict

    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema for the results to a
        local file system.

        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema = [self.field_to_bigquery(field) for field in cursor.description]

        self.log.info('Using schema for %s', self.schema_filename)
        self.log.debug("Current schema: %s", schema)
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        tmp_schema_file_handle.write(json.dumps(schema, sort_keys=True).encode('utf-8'))
        schema_file_to_upload = {
            'file_name': self.schema_filename,
            'file_handle': tmp_schema_file_handle,
            'file_mime_type': 'application/json',
        }
        return schema_file_to_upload

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google Cloud Storage.
        """
        hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)
        for tmp_file in files_to_upload:
            hook.upload(self.bucket, tmp_file.get('file_name'),
                        tmp_file.get('file_handle').name,
                        mime_type=tmp_file.get('file_mime_type'),
                        gzip=self.gzip if tmp_file.get('file_name') == self.schema_filename else False)
