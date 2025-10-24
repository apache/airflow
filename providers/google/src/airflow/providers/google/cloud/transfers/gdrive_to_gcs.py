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
from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.providers.google.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class GoogleDriveToGCSOperator(BaseOperator):
    """
    Writes a Google Drive file into Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDriveToGCSOperator`

    :param bucket_name: The destination Google cloud storage bucket where the
        file should be written to
    :param object_name: The Google Cloud Storage object name for the object created by the operator.
        For example: ``path/to/my/file/file.txt``.
    :param folder_id: The folder id of the folder in which the Google Drive file resides
    :param file_name: The name of the file residing in Google Drive
    :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
    :param gcp_conn_id: The GCP connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "bucket_name",
        "object_name",
        "folder_id",
        "file_name",
        "drive_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        bucket_name: str,
        object_name: str | None = None,
        file_name: str,
        folder_id: str,
        drive_id: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.folder_id = folder_id
        self.drive_id = drive_id
        self.file_name = file_name
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        gdrive_hook = GoogleDriveHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        file_metadata = gdrive_hook.get_file_id(
            folder_id=self.folder_id, file_name=self.file_name, drive_id=self.drive_id
        )
        with gcs_hook.provide_file_and_upload(
            bucket_name=self.bucket_name, object_name=self.object_name
        ) as file:
            gdrive_hook.download_file(file_id=file_metadata["id"], file_handle=file)

    def dry_run(self):
        """Perform a dry run of the operator."""
        return None
