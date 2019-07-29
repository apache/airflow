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

import base64
import calendar
import json

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import date, datetime, timedelta
from decimal import Decimal
from MySQLdb.constants import FIELD_TYPE
from tempfile import NamedTemporaryFile
import unicodecsv as csv


class MySqlToGoogleCloudStorageOperator(BaseOperator):
    """Copy data from MySQL to Google cloud storage in JSON or CSV format.

    The JSON data files generated are newline-delimited to enable them to be
    loaded into BigQuery.
    Reference: https://cloud.google.com/bigquery/docs/
    loading-data-cloud-storage-json#limitations

    :param sql: The SQL to execute on the MySQL table.
    :type sql: str
    :param bucket: The bucket to upload to.
    :type bucket: str
    :param filename: The filename to use as the object name when uploading
        to Google cloud storage. A {} should be specified in the filename
        to allow the operator to inject file numbers in cases where the
        file is split due to size.
    :type filename: str
    :param schema_filename: If set, the filename to use as the object name
        when uploading a .json file containing the BigQuery schema fields
        for the table that was dumped from MySQL.
    :type schema_filename: str
    :param approx_max_file_size_bytes: This operator supports the ability
        to split large table dumps into multiple files (see notes in the
        filename param docs above). This param allows developers to specify the
        file size of the splits. Check https://cloud.google.com/storage/quotas
        to see the maximum allowed file size for a single object.
    :type approx_max_file_size_bytes: long
    :param mysql_conn_id: Reference to a specific MySQL hook.
    :type mysql_conn_id: str
    :param google_cloud_storage_conn_id: Reference to a specific Google
        cloud storage hook.
    :type google_cloud_storage_conn_id: str
    :param schema: The schema to use, if any. Should be a list of dict or
        a str. Pass a string if using Jinja template, otherwise, pass a list of
        dict. Examples could be seen: https://cloud.google.com/bigquery/docs
        /schemas#specifying_a_json_schema_file
    :type schema: str or list
    :param delegate_to: The account to impersonate, if any. For this to
        work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: str
    :param export_format: Desired format of files to be exported.
    :type export_format: str
    :param field_delimiter: The delimiter to be used for CSV files.
    :type field_delimiter: str
    :param ensure_utc: Ensure TIMESTAMP columns exported as UTC. If set to
        `False`, TIMESTAMP columns will be exported using the MySQL server's
        default timezone.
    :type ensure_utc: bool
    """
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename', 'schema')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    type_map = {
        FIELD_TYPE.BIT: 'INTEGER',
        FIELD_TYPE.DATETIME: 'TIMESTAMP',
        FIELD_TYPE.DATE: 'TIMESTAMP',
        FIELD_TYPE.DECIMAL: 'FLOAT',
        FIELD_TYPE.NEWDECIMAL: 'FLOAT',
        FIELD_TYPE.DOUBLE: 'FLOAT',
        FIELD_TYPE.FLOAT: 'FLOAT',
        FIELD_TYPE.INT24: 'INTEGER',
        FIELD_TYPE.LONG: 'INTEGER',
        FIELD_TYPE.LONGLONG: 'INTEGER',
        FIELD_TYPE.SHORT: 'INTEGER',
        FIELD_TYPE.TIME: 'TIME',
        FIELD_TYPE.TIMESTAMP: 'TIMESTAMP',
        FIELD_TYPE.TINY: 'INTEGER',
        FIELD_TYPE.YEAR: 'INTEGER',
    }

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 filename,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000,
                 mysql_conn_id='mysql_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 schema=None,
                 delegate_to=None,
                 export_format='json',
                 field_delimiter=',',
                 ensure_utc=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.mysql_conn_id = mysql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.schema = schema
        self.delegate_to = delegate_to
        self.export_format = export_format.lower()
        self.field_delimiter = field_delimiter
        self.ensure_utc = ensure_utc

    def execute(self, context):
        cursor = self._query_mysql()
        files_to_upload = self._write_local_data_files(cursor)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            files_to_upload.append(self._write_local_schema_file(cursor))

        # Flush all files before uploading.
        for tmp_file in files_to_upload:
            tmp_file_handle = tmp_file.get('file_handle')
            tmp_file_handle.flush()

        self._upload_to_gcs(files_to_upload)

        # Close all temp file handles.
        for tmp_file in files_to_upload:
            tmp_file_handle = tmp_file.get('file_handle')
            tmp_file_handle.close()

    def _query_mysql(self):
        """
        Queries mysql and returns a cursor to the results.
        """
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = mysql.get_conn()
        cursor = conn.cursor()
        if self.ensure_utc:
            # Ensure TIMESTAMP results are in UTC
            tz_query = "SET time_zone = '+00:00'"
            self.log.info('Executing: %s', tz_query)
            cursor.execute(tz_query)
        self.log.info('Executing: %s', self.sql)
        cursor.execute(self.sql)
        return cursor

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

        if self.export_format == 'csv':
            csv_writer = self._configure_csv_file(tmp_file_handle, schema)

        for row in cursor:
            # Convert datetime objects to utc seconds, and decimals to floats.
            # Convert binary type object to string encoded with base64.
            row = self._convert_types(schema, col_type_dict, row)

            if self.export_format == 'csv':
                csv_writer.writerow(row)
            else:
                row_dict = dict(zip(schema, row))

                # TODO validate that row isn't > 2MB. BQ enforces a hard row size of 2MB.
                s = json.dumps(row_dict, sort_keys=True).encode('utf-8')
                tmp_file_handle.write(s)

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

    def _write_local_schema_file(self, cursor):
        """
        Takes a cursor, and writes the BigQuery schema in .json format for the
        results to a local file system.

        :return: A dictionary where key is a filename to be used as an object
            name in GCS, and values are file handles to local files that
            contains the BigQuery schema fields in .json format.
        """
        schema_str = None
        schema_file_mime_type = 'application/json'
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        if self.schema is not None and isinstance(self.schema, str):
            schema_str = self.schema.encode('utf-8')
        elif self.schema is not None and isinstance(self.schema, list):
            schema_str = json.dumps(self.schema).encode('utf-8')
        else:
            schema = []
            for field in cursor.description:
                # See PEP 249 for details about the description tuple.
                field_name = field[0]
                field_type = self.type_map.get(field[1], "STRING")
                # Always allow TIMESTAMP to be nullable. MySQLdb returns None types
                # for required fields because some MySQL timestamps can't be
                # represented by Python's datetime (e.g. 0000-00-00 00:00:00).
                if field[6] or field_type == 'TIMESTAMP':
                    field_mode = 'NULLABLE'
                else:
                    field_mode = 'REQUIRED'
                schema.append({
                    'name': field_name,
                    'type': field_type,
                    'mode': field_mode,
                })
            schema_str = json.dumps(schema, sort_keys=True).encode('utf-8')
        tmp_schema_file_handle.write(schema_str)

        self.log.info('Using schema for %s: %s', self.schema_filename, schema_str)
        schema_file_to_upload = {
            'file_name': self.schema_filename,
            'file_handle': tmp_schema_file_handle,
            'file_mime_type': schema_file_mime_type
        }
        return schema_file_to_upload

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for tmp_file in files_to_upload:
            hook.upload(self.bucket, tmp_file.get('file_name'),
                        tmp_file.get('file_handle').name,
                        mime_type=tmp_file.get('file_mime_type'))

    @classmethod
    def _convert_types(cls, schema, col_type_dict, row):
        return [
            cls._convert_type(value, col_type_dict.get(name))
            for name, value in zip(schema, row)
        ]

    @classmethod
    def _convert_type(cls, value, schema_type):
        """
        Takes a value from MySQLdb, and converts it to a value that's safe for
        JSON/Google cloud storage/BigQuery. Dates are converted to UTC seconds.
        Decimals are converted to floats. Binary type fields are encoded with base64,
        as imported BYTES data must be base64-encoded according to Bigquery SQL
        date type documentation: https://cloud.google.com/bigquery/data-types

        :param value: MySQLdb column value
        :type value: Any
        :param schema_type: BigQuery data type
        :type schema_type: str
        """
        if isinstance(value, (datetime, date)):
            return calendar.timegm(value.timetuple())
        if isinstance(value, timedelta):
            return value.total_seconds()
        if isinstance(value, Decimal):
            return float(value)
        if schema_type == "BYTES":
            return base64.standard_b64encode(value).decode('ascii')
        return value

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
            self.log.warn('Using default schema due to unexpected type.'
                          'Should be a string or list.')

        col_type_dict = {}
        try:
            col_type_dict = {col['name']: col['type'] for col in schema}
        except KeyError:
            self.log.warn('Using default schema due to missing name or type. Please '
                          'refer to: https://cloud.google.com/bigquery/docs/schemas'
                          '#specifying_a_json_schema_file')
        return col_type_dict
