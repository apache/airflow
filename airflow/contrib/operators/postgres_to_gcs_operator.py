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
import datetime

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from decimal import Decimal
from tempfile import NamedTemporaryFile

PY3 = sys.version_info[0] == 3


class PostgresToGoogleCloudStorageOperator(BaseOperator):
    """
    Copy data from Postgres to Google Cloud Storage in JSON format.
    """
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename',
                       'parameters')
    template_ext = ('.sql', )
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 filename,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000,
                 postgres_conn_id='postgres_default',
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None,
                 parameters=None,
                 *args,
                 **kwargs):
        """
        :param sql: The SQL to execute on the Postgres table.
        :type sql: str
        :param bucket: The bucket to upload to.
        :type bucket: str
        :param filename: The filename to use as the object name when uploading
            to Google Cloud Storage. A {} should be specified in the filename
            to allow the operator to inject file numbers in cases where the
            file is split due to size.
        :type filename: str
        :param schema_filename: If set, the filename to use as the object name
            when uploading a .json file containing the BigQuery schema fields
            for the table that was dumped from Postgres.
        :type schema_filename: str
        :param approx_max_file_size_bytes: This operator supports the ability
            to split large table dumps into multiple files (see notes in the
            filenamed param docs above). Google Cloud Storage allows for files
            to be a maximum of 4GB. This param allows developers to specify the
            file size of the splits.
        :type approx_max_file_size_bytes: long
        :param postgres_conn_id: Reference to a specific Postgres hook.
        :type postgres_conn_id: str
        :param google_cloud_storage_conn_id: Reference to a specific Google
            cloud storage hook.
        :type google_cloud_storage_conn_id: str
        :param delegate_to: The account to impersonate, if any. For this to
            work, the service account making the request must have domain-wide
            delegation enabled.
        :param parameters: a parameters dict that is substituted at query runtime.
        :type parameters: dict
        """
        super(PostgresToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.postgres_conn_id = postgres_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to
        self.parameters = parameters

    def execute(self, context):
        cursor = self._query_postgres()
        files_to_upload = self._write_local_data_files(cursor)

        # If a schema is set, create a BQ schema JSON file.
        if self.schema_filename:
            files_to_upload.update(self._write_local_schema_file(cursor))

        # Flush all files before uploading
        for file_handle in files_to_upload.values():
            file_handle.flush()

        self._upload_to_gcs(files_to_upload)

        # Close all temp file handles.
        for file_handle in files_to_upload.values():
            file_handle.close()

    def _query_postgres(self):
        """
        Queries Postgres and returns a cursor to the results.
        """
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        conn = postgres.get_conn()
        cursor = conn.cursor()
        cursor.execute(self.sql, self.parameters)
        return cursor

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        schema = list(map(lambda schema_tuple: schema_tuple[0], cursor.description))
        tmp_file_handles = {}
        row_no = 0

        def _create_new_file():
            handle = NamedTemporaryFile(delete=True)
            filename = self.filename.format(len(tmp_file_handles))
            tmp_file_handles[filename] = handle
            return handle

        # Don't create a file if there is nothing to write
        if cursor.rowcount > 0:
            tmp_file_handle = _create_new_file()

            for row in cursor:
                # Convert datetime objects to utc seconds, and decimals to floats
                row = map(self.convert_types, row)
                row_dict = dict(zip(schema, row))

                s = json.dumps(row_dict, sort_keys=True)
                if PY3:
                    s = s.encode('utf-8')
                tmp_file_handle.write(s)

                # Append newline to make dumps BigQuery compatible.
                tmp_file_handle.write(b'\n')

                # Stop if the file exceeds the file size limit.
                if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                    tmp_file_handle = _create_new_file()
                row_no += 1

        self.log.info('Received %s rows over %s files', row_no, len(tmp_file_handles))

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
            field_name = field[0]
            field_type = self.type_map(field[1])
            field_mode = 'REPEATED' if field[1] in (1009, 1005, 1007,
                                                    1016) else 'NULLABLE'
            schema.append({
                'name': field_name,
                'type': field_type,
                'mode': field_mode,
            })

        self.log.info('Using schema for %s: %s', self.schema_filename, schema)
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        s = json.dumps(schema, sort_keys=True)
        if PY3:
            s = s.encode('utf-8')
        tmp_schema_file_handle.write(s)
        return {self.schema_filename: tmp_schema_file_handle}

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google Cloud Storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to)
        for object, tmp_file_handle in files_to_upload.items():
            hook.upload(self.bucket, object, tmp_file_handle.name,
                        'application/json')

    @classmethod
    def convert_types(cls, value):
        """
        Takes a value from Postgres, and converts it to a value that's safe for
        JSON/Google Cloud Storage/BigQuery. Dates are converted to UTC seconds.
        Decimals are converted to floats. Times are converted to seconds.
        """
        if type(value) in (datetime.datetime, datetime.date):
            return time.mktime(value.timetuple())
        elif type(value) == datetime.time:
            formated_time = time.strptime(str(value), "%H:%M:%S")
            return datetime.timedelta(
                hours=formated_time.tm_hour,
                minutes=formated_time.tm_min,
                seconds=formated_time.tm_sec).seconds
        elif isinstance(value, Decimal):
            return float(value)
        else:
            return value

    @classmethod
    def type_map(cls, postgres_type):
        """
        Helper function that maps from Postgres fields to BigQuery fields. Used
        when a schema_filename is set.
        """
        d = {
            1114: 'TIMESTAMP',
            1184: 'TIMESTAMP',
            1082: 'TIMESTAMP',
            1083: 'TIMESTAMP',
            1005: 'INTEGER',
            1007: 'INTEGER',
            1016: 'INTEGER',
            20: 'INTEGER',
            21: 'INTEGER',
            23: 'INTEGER',
            16: 'BOOLEAN',
            700: 'FLOAT',
            701: 'FLOAT',
            1700: 'FLOAT'
        }

        return d[postgres_type] if postgres_type in d else 'STRING'
