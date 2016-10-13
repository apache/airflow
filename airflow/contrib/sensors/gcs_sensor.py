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

import logging

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.operators.sensors import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageObjectSensor(BaseSensorOperator):
    """
    Checks for the existence of a file in Google Cloud Storage.
    """
    template_fields = ('bucket', 'object')
    ui_color = '#f0eee4'

    @apply_defaults
    def __init__(
            self,
            bucket,
            object,
            google_cloud_conn_id='google_cloud_storage_default',
            delegate_to=None,
            *args,
            **kwargs):
        """
        Create a new GoogleCloudStorageDownloadOperator.

        :param bucket: The Google cloud storage bucket where the object is.
        :type bucket: string
        :param object: The name of the object to check in the Google cloud
            storage bucket.
        :type object: string
        :param google_cloud_storage_conn_id: The connection ID to use when
            connecting to Google cloud storage.
        :type google_cloud_storage_conn_id: string
        :param delegate_to: The account to impersonate, if any.
            For this to work, the service account making the request must have domain-wide delegation enabled.
        :type delegate_to: string
        """
        super(GoogleCloudStorageObjectSensor, self).__init__(*args, **kwargs)
        self.bucket = bucket
        self.object = object
        self.google_cloud_conn_id = google_cloud_conn_id
        self.delegate_to = delegate_to

    def poke(self, context):
        logging.info('Sensor checks existence of : %s, %s', self.bucket, self.object)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_conn_id,
            delegate_to=self.delegate_to)
        return hook.exists(self.bucket, self.object)
