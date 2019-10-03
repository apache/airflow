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
This module contains Google Cloud Storage to SFTP operator.
"""
from tempfile import NamedTemporaryFile
from typing import Optional

from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.gcp.operators.gcs import GoogleCloudStorageListOperator
from airflow.utils.decorators import apply_defaults


class GoogleCloudStorageToSFTPOperator(GoogleCloudStorageListOperator):
    """
    Transfer files from a Google Cloud Storage bucket to SFTP server.

    :param bucket: The Google Cloud Storage bucket to find the objects. (templated)
    :type bucket: str
    :param sftp_path: The sftp remote path. This is the specified file path for
        uploading file to the SFTP server.
    :type sftp_path: str
    :param prefix: Prefix string which filters objects whose name begin with
        this prefix. (templated)
    :type prefix: str
    :param delimiter: The delimiter by which you want to filter the objects. (templated)
        For e.g to lists the CSV files from in a directory in GCS you would use
        delimiter='.csv'.
    :type delimiter: str
    :param gcp_conn_id: (Optional) The connection ID used to connect to Google Cloud Platform.
    :type gcp_conn_id: str
    :param sftp_conn_id: The sftp connection id. The name or identifier for
        establishing a connection to the SFTP server.
    :type sftp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("bucket", "prefix", "delimiter", "sftp_path")
    ui_color = "#f0eee4"

    # pylint: disable=too-many-arguments
    @apply_defaults
    def __init__(
        self,
        bucket: str,
        sftp_path: str,
        prefix: Optional[str] = None,
        delimiter: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        sftp_conn_id: str = "ssh_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:

        super().__init__(
            bucket=bucket,
            prefix=prefix,
            delimiter=delimiter,
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            *args,
            **kwargs
        )

        self.sftp_path = sftp_path
        self.sftp_conn_id = sftp_conn_id

    def execute(self, context):
        # use the super to list all files in an Google Cloud Storage bucket
        files = super().execute(context) or []

        if files:

            gcs_hook = GoogleCloudStorageHook(
                google_cloud_storage_conn_id=self.gcp_conn_id,
                delegate_to=self.delegate_to,
            )

            ssh_hook = SSHHook(ssh_conn_id=self.sftp_conn_id)
            sftp_client = ssh_hook.get_conn().open_sftp()

            for file in files:
                with NamedTemporaryFile("w") as tmp:
                    gcs_hook.download(self.bucket, file, tmp.name)
                    sftp_client.put(tmp.name, self.sftp_path)

            self.log.info("All done, uploaded %d files to STPS", len(files))
        else:
            self.log.info("No files in bucket.")

        return files
