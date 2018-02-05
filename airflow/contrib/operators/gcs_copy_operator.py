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

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageCopyOperator(BaseOperator):
    """
    Copies objects (optionally from a directory) filtered by 'delimiter' (file extension for e.g .json) from a bucket
    to another bucket in a different directory, if required.

    :param source_bucket: The source Google cloud storage bucket where the object is.
    :type source_bucket: string
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket.
    :type source_object: string
    :param source_files_delimiter: The delimiter by which you want to filter the files to copy.
        For e.g to copy the CSV files from in a directory in GCS you would use source_files_delimiter='.csv'.
    :type source_files_delimiter: string
    :param destination_bucket: The destination Google cloud storage bucket where the object should be.
    :type destination_bucket: string
    :param destination_directory: The destination name of the directory in the destination Google cloud
        storage bucket.
    :type destination_directory: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: string

    **Example**:
    The following Operator would move all the CSV files from `sales/sales-2017` folder in
    `data` bucket to `sales` folder in `archive` bucket. ::

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

        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                                      delegate_to=self.delegate_to)

        self.log.info('Getting list of the files to copy. Source Bucket: %s; Source Object: %s',
                      self.source_bucket, self.source_object)

        # Create a list of objects to copy from Source bucket. The function uses prefix keyword to pass the name of
        # the object to copy.
        self.files_to_copy = hook.list(bucket=self.source_bucket, prefix=self.source_object,
                                       delimiter=self.source_files_delimiter)

        # Log the names of all objects to be copied
        self.log.info('Files to copy: %s', self.files_to_copy)

        if self.files_to_copy is not None:
            for file_to_copy in self.files_to_copy:
                self.log.info('Source_Bucket: %s, Source_Object: %s, '
                              'Destination_bucket: %s, Destination_Directory: %s',
                              self.source_bucket, file_to_copy,
                              self.destination_bucket or self.source_bucket,
                              self.destination_directory + file_to_copy)
                hook.copy(self.source_bucket, file_to_copy,
                          self.destination_bucket, self.destination_directory + file_to_copy)
        else:
            self.log.info('No Files to copy.')
