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
This module contains SFTP to Google Cloud Storage operator.
"""
from tempfile import NamedTemporaryFile
from typing import Optional

from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class SFTPToGoogleCloudStorageOperator(BaseOperator):
    """
    Transfer files to Google Cloud Storage from SFTP server.

    :param sftp_path: The sftp remote path. This is the specified file path
        for downloading the file from the SFTP server.
    :type sftp_path: str
    :param destination_path: Destination path within the specified bucket, it must be the full file path
        to destination object on GCS, including GCS object (ex. `path/to/file.txt`) (templated)
    :type destination_path: str
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
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    """

    template_fields = ("sftp_path", "dst", "bucket")

    @apply_defaults
    def __init__(
        self,
        sftp_path: str,
        destination_path: str,
        bucket: str,
        gcp_conn_id: str = "google_cloud_default",
        mime_type: str = "application/octet-stream",
        delegate_to: Optional[str] = None,
        gzip: bool = False,
        sftp_conn_id: str = "ssh_default",
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)

        self.sftp_path = sftp_path
        self.destination_path = destination_path
        self.bucket = bucket
        self.gcp_conn_id = gcp_conn_id
        self.mime_type = mime_type
        self.delegate_to = delegate_to
        self.gzip = gzip
        self.sftp_conn_id = sftp_conn_id

    def execute(self, context):
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )

        ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
        sftp_client = ssh_hook.get_conn().open_sftp()

        with NamedTemporaryFile("w") as f:
            sftp_client.get(self.sftp_path, f.name)

            gcs_hook.upload(
                bucket_name=self.bucket,
                object_name=self.destination_path,
                mime_type=self.mime_type,
                filename=f.name,
                gzip=self.gzip,
            )
