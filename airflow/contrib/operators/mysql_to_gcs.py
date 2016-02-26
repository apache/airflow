import json
import logging

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks import MySqlHook
from airflow.models import BaseOperator
from airflow.utils import apply_defaults
from collections import OrderedDict
from MySQLdb.constants import FIELD_TYPE
from tempfile import NamedTemporaryFile

class MySqlToGoogleCloudStorageOperator(BaseOperator):
    """
    Copy data from MySQL to Google cloud storage in JSON format.
    """
    template_fields = ('sql', 'bucket', 'filename', 'schema_filename')
    template_ext = ('.sql',)
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 sql,
                 bucket,
                 filename,
                 schema_filename=None,
                 approx_max_file_size_bytes=1900000000L,
                 mysql_conn_id='mysql_default',
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        """
        :param sql: The SQL to execute on the MySQL table.
        :type sql: string
        :param bucket: The bucket to upload to.
        :type bucket: string
        :param filename: The filename to use as the object name when uploading
            to Google cloud storage. A {} should be specified in the filename
            to allow the operator to inject file numbers in cases where the
            file is split due to size.
        :type filename: string
        :param schema_filename: If set, the filename to use as the object name
            when uploading a .json file containing the BigQuery schema fields
            for the table that was dumped from MySQL.
        :type schema_filename: string
        :param approx_max_file_size_bytes: This operator supports the ability
            to split large table dumps into multiple files (see notes in the
            filenamed param docs above). Google cloud storage allows for files
            to be a maximum of 4GB. This param allows developers to specify the
            file size of the splits.
        :type approx_max_file_size_bytes: long
        :param mysql_conn_id: Reference to a specific MySQL hook.
        :type mysql_conn_id: string
        :param google_cloud_storage_conn_id: Reference to a specific Google
            cloud storage hook.
        :type google_cloud_storage_conn_id: string
        :param delegate_to: The account to impersonate, if any. For this to
            work, the service account making the request must have domain-wide
            delegation enabled.
        """
        super(MySqlToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.sql = sql;
        self.bucket = bucket
        self.filename = filename
        self.schema_filename = schema_filename
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.mysql_conn_id = mysql_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        cursor = self._query_mysql()
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
        schema = map(lambda schema_tuple: schema_tuple[0], cursor.description)
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        tmp_file_handles = { self.filename.format(file_no): tmp_file_handle }

        for row in cursor:
            row_dict = dict(zip(schema, row))
            # TODO validate that row isn't > 2MB. BQ enforces a hard row size of 2MB.
            json.dump(row_dict, tmp_file_handle)

            # Append newline to make dumps BigQuery compatible.
            tmp_file_handle.write('\n')

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
        schema = []
        for field in cursor.description:
            # See PEP 249 for details about the description tuple.
            field_name = field[0]
            field_type = self.type_map(field[1])
            field_mode = 'NULLABLE' if field[6] else 'REQUIRED'
            schema.append({
                'name': field_name,
                'type': field_type,
                'mode': field_mode,
            })

        logging.info('Using schema for %s: %s', self.schema_filename, schema)
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        json.dump(schema, tmp_schema_file_handle)
        return {self.schema_filename: tmp_schema_file_handle}

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                                      delegate_to=self.delegate_to)
        for object, tmp_file_handle in files_to_upload.items():
            hook.upload(self.bucket, object, tmp_file_handle.name, 'application/json')

    @classmethod
    def type_map(cls, mysql_type):
        """
        Helper function that maps from MySQL fields to BigQuery fields. Used
        when a schema_filename is set.
        """
        d = {
            FIELD_TYPE.BIT: 'INTEGER',
            FIELD_TYPE.DATETIME: 'TIMESTAMP',
            FIELD_TYPE.DECIMAL: 'FLOAT',
            FIELD_TYPE.DOUBLE: 'FLOAT',
            FIELD_TYPE.FLOAT: 'FLOAT',
            FIELD_TYPE.INT24: 'INTEGER',
            FIELD_TYPE.LONG: 'INTEGER',
            FIELD_TYPE.LONGLONG: 'INTEGER',
            FIELD_TYPE.SHORT: 'INTEGER',
            FIELD_TYPE.TIMESTAMP: 'TIMESTAMP',
            FIELD_TYPE.YEAR: 'INTEGER',
        }
        return d[mysql_type] if mysql_type in d else 'STRING'
