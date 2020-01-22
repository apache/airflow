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
"""
This module contains operator for uploading local file to GCS.
"""
import warnings

from airflow.gcp.hooks.gcs import GCSHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LocalFilesystemToGCSOperator(BaseOperator):
    """
    Uploads a file to Google Cloud Storage.
    Optionally can compress the file for upload.

    :param src: Path to the local file. (templated)
    :type src: str
    :param dst: Destination path within the specified bucket, it must be the full file path
        to destination object on GCS, including GCS object (ex. `path/to/file.txt`) (templated)
    :type dst: str
    :param bucket: The bucket to upload to. (templated)
    :type bucket: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param google_cloud_storage_conn_id: (Deprecated) The connection ID used to connect to Google Cloud
        Platform. This parameter has been deprecated. You should pass the gcp_conn_id parameter instead.
    :type google_cloud_storage_conn_id: str
    :param mime_type: The mime-type string
    :type mime_type: str
    :param delegate_to: The account to impersonate, if any
    :type delegate_to: str
    :param gzip: Allows for file to be compressed and uploaded as gzip
    :type gzip: bool
    """
    template_fields = ('src', 'dst', 'bucket')

    @apply_defaults
    def __init__(self,
                 src,
                 dst,
                 bucket,
                 gcp_conn_id='google_cloud_default',
                 google_cloud_storage_conn_id=None,
                 mime_type='application/octet-stream',
                 delegate_to=None,
                 gzip=False,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        if google_cloud_storage_conn_id:
            warnings.warn(
                "The google_cloud_storage_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.", DeprecationWarning, stacklevel=3)
            gcp_conn_id = google_cloud_storage_conn_id

        self.src = src
        self.dst = dst
        self.bucket = bucket
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip

    def execute(self, context):
        """
        Uploads the file to Google Cloud Storage
        """
        hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)

        hook.upload(
            bucket_name=self.bucket,
            object_name=self.dst,
            mime_type=self.mime_type,
            filename=self.src,
            gzip=self.gzip,
        )
