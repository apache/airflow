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
#

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class FileToGoogleCloudStorageOperator(BaseOperator):
    """
    Uploads a file to Google Cloud Storage

    :param src: Path to the local file
    :type src: string
    :param dst: Destination path within the specified bucket
    :type dst: string
    :param bucket: The bucket to upload to
    :type bucket: string
    :param google_cloud_storage_conn_id: The Airflow connection ID to upload with
    :type google_cloud_storage_conn_id: string
    :param mime_type: The mime-type string
    :type mime_type: string
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: string
    """

    @apply_defaults
    def __init__(self,
                 src,
                 dst,
                 bucket,
                 google_cloud_storage_conn_id='google_cloud_storage_default',
                 mime_type='application/octet-stream',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super(FileToGoogleCloudStorageOperator, self).__init__(*args, **kwargs)
        self.src = src
        self.dst = dst
        self.bucket = bucket
        self.google_cloud_storage_conn_id = google_cloud_storage_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to

    def execute(self, context):
        """
        Uploads the file to Google cloud storage
        """
        hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
                delegate_to=self.delegate_to)

        hook.upload(
            bucket=self.bucket,
            object=self.dst,
            mime_type=self.mime_type,
            filename=self.src)
