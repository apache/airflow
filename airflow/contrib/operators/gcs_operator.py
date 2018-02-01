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
import sys

from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageCopyOperator(BaseOperator):
    """
    Copies objects (optionally from a directory) filtered by 'delimiter'
    (file extension for e.g .json) from a bucket to another bucket in a different
    directory, if required.

    :param source_bucket: The source Google cloud storage bucket where the object is.
    :type source_bucket: string
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket.
    :type source_object: string
    :param source_files_delimiter: The delimiter by which you want to filter the
        files to copy.
        For e.g to copy the CSV files from in a directory in GCS you would use
            source_files_delimiter='.csv'.
    :type source_files_delimiter: string
    :param destination_bucket: The destination Google cloud storage bucket where the
        object should be.
    :type destination_bucket: string
    :param destination_directory: The destination name of the directory in the destination
        Google cloud storage bucket.
    :type destination_directory: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string

    Example: The following Operator would move all the CSV files from `sales/sales-2017`
    folder in `data` bucket to `sales` folder in `archive` bucket.

    move_file = GoogleCloudStorageCopyOperator(
        task_id='move_file',
        source_bucket='data',
        source_object='sales/sales-2017/',
        source_files_delimiter='.csv'
        destination_bucket='archive',
        destination_directory='sales',
        google_cloud_storage_conn_id='airflow-service-account'
    )
    """
    template_fields = ('source_bucket', 'source_object', 'source_files_delimiter',
                       'destination_bucket', 'destination_directory')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 source_bucket,
                 source_object,
                 source_files_delimiter=None,
                 destination_bucket=None,
                 destination_directory='',
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleCloudStorageCopyOperator, self).__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.source_files_delimiter = source_files_delimiter
        self.files_to_copy = list()
        self.destination_bucket = destination_bucket
        self.destination_directory = destination_directory
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):

        self.log.info('Executing copy - Source_Bucket: %s, Source_directory: %s, '
                      'Destination_bucket: %s, Destination_directory: %s',
                      self.source_bucket, self.source_object,
                      self.destination_bucket or self.source_bucket,
                      self.destination_directory or self.source_object)

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )

        self.log.info(
            'Getting list of the files to copy. Source Bucket: %s; Source Object: %s',
            self.source_bucket, self.source_object
        )

        # Create a list of objects to copy from Source bucket. The function uses prefix
        # keyword to pass the name of the object to copy.
        self.files_to_copy = hook.list(
            bucket=self.source_bucket, prefix=self.source_object,
            delimiter=self.source_files_delimiter
        )

        # Log the names of all objects to be copied
        self.log.info('Files to copy: %s', self.files_to_copy)

        if self.files_to_copy is not None:
            for file_to_copy in self.files_to_copy:
                self.log.info('Source_Bucket: %s, Source_Object: %s, '
                              'Destination_bucket: %s, Destination_Directory: %s',
                              self.source_bucket, file_to_copy,
                              self.destination_bucket or self.source_bucket,
                              self.destination_directory + file_to_copy)
                hook.copy(
                    self.source_bucket, file_to_copy,
                    self.destination_bucket, self.destination_directory + file_to_copy
                )
        else:
            self.log.info('No Files to copy.')


class GoogleCloudStorageDownloadOperator(BaseOperator):
    """
    Downloads a file from Google Cloud Storage.

    :param bucket: The Google cloud storage bucket where the object is.
    :type bucket: string
    :param object: The name of the object to download in the Google cloud
        storage bucket.
    :type object: string
    :param filename: The file path on the local file system (where the
        operator is being executed) that the file should be downloaded to.
        If no filename passed, the downloaded data will not be stored on the local file
        system.
    :type filename: string
    :param store_to_xcom_key: If this param is set, the operator will push
        the contents of the downloaded file to XCom with the key set in this
        parameter. If not set, the downloaded data will not be pushed to XCom.
    :type store_to_xcom_key: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    """
    template_fields = ('bucket', 'object', 'filename', 'store_to_xcom_key',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 object,
                 filename=None,
                 store_to_xcom_key=None,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleCloudStorageDownloadOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.filename = filename
        self.store_to_xcom_key = store_to_xcom_key
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        self.log.info(
            'Executing download: %s, %s, %s', self.bucket, self.object, self.filename
        )
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        file_bytes = hook.download(self.bucket, self.object, self.filename)
        if self.store_to_xcom_key:
            if sys.getsizeof(file_bytes) < 48000:
                context['ti'].xcom_push(key=self.store_to_xcom_key, value=file_bytes)
            else:
                raise RuntimeError(
                    'The size of the downloaded file is too large to push to XCom!'
                )
        self.log.debug(file_bytes)


class GoogleCloudStorageListOperator(BaseOperator):
    """
    List all objects from the bucket with the give string prefix and delimiter in name.

    This operator returns a python list with the name of objects which can be used by
     `xcom` in the downstream task.

    :param bucket: The Google cloud storage bucket to find the objects.
    :type bucket: string
    :param prefix: Prefix string which filters objects whose name begin with this prefix
    :type prefix: string
    :param delimiter: The delimiter by which you want to filter the objects.
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :type delimiter: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: string

    Example: The following Operator would list all the Avro files from `sales/sales-2017`
        folder in `data` bucket.

    GCS_Files = GoogleCloudStorageListOperator(
        task_id='GCS_Files',
        bucket='data',
        prefix='sales/sales-2017/',
        delimiter='.avro',
        google_cloud_storage_conn_id=google_cloud_conn_id
    )
    """
    template_fields = ('bucket', 'prefix', 'delimiter')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 prefix=None,
                 delimiter=None,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleCloudStorageListOperator, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.prefix = prefix
        self.delimiter = delimiter
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):

        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )

        self.log.info('Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s',
                      self.bucket, self.delimiter, self.prefix)

        return hook.list(bucket=self.bucket,
                         prefix=self.prefix,
                         delimiter=self.delimiter)


class GoogleCloudStorageToBigQueryOperator(BaseOperator):
    """
    Loads files from Google cloud storage into BigQuery.

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
    :param compression: [Optional] The compression type of the data source.
            Possible values include GZIP and NONE.
            The default value is NONE.
            This setting is ignored for Google Cloud Bigtable,
                Google Cloud Datastore backups and Avro formats.
    :type compression: string
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
    :param allow_jagged_rows: Accept rows that are missing trailing optional columns.
        The missing values are treated as nulls. If false, records with missing trailing
        columns are treated as bad records, and if there are too many bad records, an
        invalid error is returned in the job result. Only applicable to CSV, ignored
        for other formats.
    :type allow_jagged_rows: bool
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
    :param schema_update_options: Allows the schema of the destination
        table to be updated as a side effect of the load job.
    :type schema_update_options: list
    :param src_fmt_configs: configure optional fields specific to the source format
    :type src_fmt_configs: dict
    :param external_table: Flag to specify if the destination table should be
        a BigQuery external table. Default Value is False.
    :type external_table: bool
    :param time_partitioning: configure optional time partitioning fields i.e.
        partition by field, type and  expiration as per API specifications.
        Note that 'field' is not available in concurrency with
        dataset.table$partition.
    :type time_partitioning: dict
    """
    template_fields = ('bucket', 'source_objects',
                       'schema_object', 'destination_project_dataset_table')
    template_ext = ('.sql',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 bucket,
                 source_objects,
                 destination_project_dataset_table,
                 schema_fields=None,
                 schema_object=None,
                 source_format='CSV',
                 compression='NONE',
                 create_disposition='CREATE_IF_NEEDED',
                 skip_leading_rows=0,
                 write_disposition='WRITE_EMPTY',
                 field_delimiter=',',
                 max_bad_records=0,
                 quote_character=None,
                 allow_quoted_newlines=False,
                 allow_jagged_rows=False,
                 max_id_key=None,
                 bigquery_conn_id='bigquery_default',
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 schema_update_options=(),
                 src_fmt_configs={},
                 external_table=False,
                 time_partitioning={},
                 *args, **kwargs):

        super(GoogleCloudStorageToBigQueryOperator, self).__init__(*args, **kwargs)

        # GCS config
        self.bucket = bucket
        self.source_objects = source_objects
        self.schema_object = schema_object

        # BQ config
        self.destination_project_dataset_table = destination_project_dataset_table
        self.schema_fields = schema_fields
        self.source_format = source_format
        self.compression = compression
        self.create_disposition = create_disposition
        self.skip_leading_rows = skip_leading_rows
        self.write_disposition = write_disposition
        self.field_delimiter = field_delimiter
        self.max_bad_records = max_bad_records
        self.quote_character = quote_character
        self.allow_quoted_newlines = allow_quoted_newlines
        self.allow_jagged_rows = allow_jagged_rows
        self.external_table = external_table

        self.max_id_key = max_id_key
        self.bigquery_conn_id = bigquery_conn_id
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

        self.schema_update_options = schema_update_options
        self.src_fmt_configs = src_fmt_configs
        self.time_partitioning = time_partitioning

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

        if self.external_table:
            cursor.create_external_table(
                external_project_dataset_table=self.destination_project_dataset_table,
                schema_fields=schema_fields,
                source_uris=source_uris,
                source_format=self.source_format,
                compression=self.compression,
                skip_leading_rows=self.skip_leading_rows,
                field_delimiter=self.field_delimiter,
                max_bad_records=self.max_bad_records,
                quote_character=self.quote_character,
                allow_quoted_newlines=self.allow_quoted_newlines,
                allow_jagged_rows=self.allow_jagged_rows,
                src_fmt_configs=self.src_fmt_configs
            )
        else:
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
                allow_jagged_rows=self.allow_jagged_rows,
                schema_update_options=self.schema_update_options,
                src_fmt_configs=self.src_fmt_configs,
                time_partitioning=self.time_partitioning)

        if self.max_id_key:
            cursor.execute('SELECT MAX({}) FROM {}'.format(
                self.max_id_key,
                self.destination_project_dataset_table))
            row = cursor.fetchone()
            max_id = row[0] if row[0] else 0
            self.log.info(
                'Loaded BQ data with max %s.%s=%s',
                self.destination_project_dataset_table, self.max_id_key, max_id
            )
            return max_id


class GoogleCloudStorageToGoogleCloudStorageOperator(BaseOperator):
    """
    Copies an object from a bucket to another, with renaming if requested.

    :param source_bucket: The source Google cloud storage bucket where the object is.
    :type source_bucket: string
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket.
    :type source_object: string
    :param destination_bucket: The destination Google cloud storage bucket where the
        object should be.
    :type destination_bucket: string
    :param destination_object: The destination name of the object in the destination
        Google cloud storage bucket.
    :type destination_object: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide
        delegation enabled.
    :type delegate_to: string
    """
    template_fields = ('source_bucket', 'source_object',
                       'destination_bucket', 'destination_object',)
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(self,
                 source_bucket,
                 source_object,
                 destination_bucket=None,
                 destination_object=None,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(GoogleCloudStorageToGoogleCloudStorageOperator,
              self).__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_bucket = destination_bucket
        self.destination_object = destination_object
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        self.log.info('Executing copy: %s, %s, %s, %s',
                      self.source_bucket, self.source_object,
                      self.destination_bucket or self.source_bucket,
                      self.destination_object or self.source_object)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        hook.copy(self.source_bucket, self.source_object,
                  self.destination_bucket, self.destination_object)
