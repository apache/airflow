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

import sys
import json
import time
import base64

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import date, datetime
from decimal import Decimal
from MySQLdb.constants import FIELD_TYPE
from tempfile import NamedTemporaryFile
from six import string_types

PY3 = sys.version_info[0] == 3


class MySqlToGoogleCloudStorageOperator(BaseOperator):
    """
    Copy data from MySQL to Google cloud storage in JSON format.

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
        filenamed param docs above). Google cloud storage allows for files
        to be a maximum of 4GB. This param allows developers to specify the
        file size of the splits.
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
    """
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename', 'schema')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

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
                 *args,
                 **kwargs):
        super(MySqlToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.mysql_conn_id = mysql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.schema = schema
        self.delegate_to = delegate_to

    def execute(self, context):
        cursor = self._query_mysql()
        files_to_upload = self._write_local_data_files(cursor)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            files_to_upload.update(self._write_local_schema_file(cursor))

        # Flush all files before uploading.
        for file_handle in files_to_upload.values():
            file_handle.flush()

        self._upload_to_gcs(files_to_upload)

        # Close all temp file handles.
        for file_handle in files_to_upload.values():
            file_handle.close()

    def _query_mysql(self):
        """
        Queries mysql and returns a cursor to the results.
        """
        mysql = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = mysql.get_conn()
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
        schema = list(map(lambda schema_tuple: schema_tuple[0], cursor.description))
        col_type_dict = self._get_col_type_dict()
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handles = {self.filename.format(file_no): tmp_file_handle}

        for row in cursor:
            # Convert datetime objects to utc seconds, and decimals to floats.
            # Convert binary type object to string encoded with base64.
            row = self._convert_types(schema, col_type_dict, row)
            row_dict = dict(zip(schema, row))

            # TODO validate that row isn't > 2MB. BQ enforces a hard row size of 2MB.
            s = json.dumps(row_dict)
            if PY3:
                s = s.encode('utf-8')
            tmp_file_handle.write(s)

            # Append newline to make dumps BigQuery compatible.
            tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit.
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
        schema_str = None
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        if self.schema is not None and isinstance(self.schema, string_types):
            schema_str = self.schema
        elif self.schema is not None and isinstance(self.schema, list):
            schema_str = json.dumps(self.schema)
        else:
            schema = []
            for field in cursor.description:
                # See PEP 249 for details about the description tuple.
                field_name = field[0]
                field_type = self.type_map(field[1])
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
            schema_str = json.dumps(schema)
        if PY3:
            schema_str = schema_str.encode('utf-8')
        tmp_schema_file_handle.write(schema_str)

        self.log.info('Using schema for %s: %s', self.schema_filename, schema_str)
        return {self.schema_filename: tmp_schema_file_handle}

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for object, tmp_file_handle in files_to_upload.items():
            hook.upload(self.bucket, object, tmp_file_handle.name, 'application/json')

    @staticmethod
    def _convert_types(schema, col_type_dict, row):
        """
        Takes a value from MySQLdb, and converts it to a value that's safe for
        JSON/Google cloud storage/BigQuery. Dates are converted to UTC seconds.
        Decimals are converted to floats. Binary type fields are encoded with base64,
        as imported BYTES data must be base64-encoded according to Bigquery SQL
        date type documentation: https://cloud.google.com/bigquery/data-types
        """
        converted_row = []
        for col_name, col_val in zip(schema, row):
            if type(col_val) in (datetime, date):
                col_val = time.mktime(col_val.timetuple())
            elif isinstance(col_val, Decimal):
                col_val = float(col_val)
            elif col_type_dict.get(col_name) == "BYTES":
                col_val = base64.standard_b64encode(col_val)
                if PY3:
                    col_val = col_val.decode('ascii')
            else:
                col_val = col_val
            converted_row.append(col_val)
        return converted_row

    def _get_col_type_dict(self):
        """
        Return a dict of column name and column type based on self.schema if not None.
        """
        schema = []
        if isinstance(self.schema, string_types):
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

    @classmethod
    def type_map(cls, mysql_type):
        """
        Helper function that maps from MySQL fields to BigQuery fields. Used
        when a schema_filename is set.
        """
        d = {
            FIELD_TYPE.INT24: 'INTEGER',
            FIELD_TYPE.TINY: 'INTEGER',
            FIELD_TYPE.BIT: 'INTEGER',
            FIELD_TYPE.DATETIME: 'TIMESTAMP',
            FIELD_TYPE.DATE: 'TIMESTAMP',
            FIELD_TYPE.DECIMAL: 'FLOAT',
            FIELD_TYPE.NEWDECIMAL: 'FLOAT',
            FIELD_TYPE.DOUBLE: 'FLOAT',
            FIELD_TYPE.FLOAT: 'FLOAT',
            FIELD_TYPE.LONG: 'INTEGER',
            FIELD_TYPE.LONGLONG: 'INTEGER',
            FIELD_TYPE.SHORT: 'INTEGER',
            FIELD_TYPE.TIMESTAMP: 'TIMESTAMP',
            FIELD_TYPE.YEAR: 'INTEGER',
        }
        return d[mysql_type] if mysql_type in d else 'STRING'
