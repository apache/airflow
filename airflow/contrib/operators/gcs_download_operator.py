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

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageDownloadOperator(BaseOperator):
    """
    Downloads a file from Google Cloud Storage.

    :param bucket: The Google cloud storage bucket where the object is. (templated)
    :type bucket: string
    :param object: The name of the object to download in the Google cloud
        storage bucket. (templated)
    :type object: string
    :param filename: The file path on the local file system (where the
        operator is being executed) that the file should be downloaded to. (templated)
        If no filename passed, the downloaded data will not be stored on the local file
        system.
    :type filename: string
    :param store_to_xcom_key: If this param is set, the operator will push
        the contents of the downloaded file to XCom with the key set in this
        parameter. If not set, the downloaded data will not be pushed to XCom. (templated)
    :type store_to_xcom_key: string
    :param google_cloud_storage_conn_id: The connection ID to use when
        connecting to Google cloud storage.
    :type google_cloud_storage_conn_id: string
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
                 google_cloud_storage_conn_id='google_cloud_default',
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
        self.log.info('Executing download: %s, %s, %s', self.bucket,
                      self.object, self.filename)
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.google_cloud_storage_conn_id,
            delegate_to=self.delegate_to
        )
        file_bytes = hook.download(bucket=self.bucket,
                                   object=self.object,
                                   filename=self.filename)
        if self.store_to_xcom_key:
            if sys.getsizeof(file_bytes) < 48000:
                context['ti'].xcom_push(key=self.store_to_xcom_key, value=file_bytes)
            else:
                raise RuntimeError(
                    'The size of the downloaded file is too large to push to XCom!'
                )
        self.log.debug(file_bytes)
