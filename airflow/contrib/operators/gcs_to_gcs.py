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


class GoogleCloudStorageToGoogleCloudStorageOperator(BaseOperator):
    """
    Copies an object from a bucket to another, with renaming if requested.

    :param source_bucket: The source Google cloud storage bucket where the object is.
    :type source_bucket: string
    :param source_object: The source name of the object to copy in the Google cloud
        storage bucket.
    :type source_object: string
    :param destination_bucket: The destination Google cloud storage bucket where the object should be.
    :type destination_bucket: string
    :param destination_object: The destination name of the object in the destination Google cloud
        storage bucket.
    :type destination_object: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have domain-wide delegation enabled.
    :type delegate_to: string
    """
    template_fields = ('source_bucket', 'source_object', 'destination_bucket', 'destination_object',)
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
        super(GoogleCloudStorageOperatorToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.source_bucket = source_bucket
        self.source_object = source_object
        self.destination_bucket = destination_bucket
        self.destination_object = destination_object
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.delegate_to = delegate_to

    def execute(self, context):
        self.log.info('Executing copy: %s, %s, %s, %s', self.source_bucket, self.source_object,
                      self.destination_bucket or self.source_bucket,
                      self.destination_object or self.source_object)
        hook = GoogleCloudStorageHook(google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                                      delegate_to=self.delegate_to)
        hook.copy(self.source_bucket, self.source_object, self.destination_bucket, self.destination_object)
