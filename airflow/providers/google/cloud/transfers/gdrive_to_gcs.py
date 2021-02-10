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

from io import BytesIO
from typing import Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from airflow.utils.decorators import apply_defaults


class GoogleDriveToGCSOperator(BaseOperator):
    """
    Writes a Google Drive file into Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDriveToGCSOperator`

    :param destination_bucket: The destination Google cloud storage bucket where the
        file should be written to
    :type destination_bucket: str
    :param destination_object: The Google Cloud Storage object name for the object created by the operator.
        For example: ``path/to/my/file/file.txt``.
    :type destination_object: str
    :param folder_id: The folder id of the folder in which the Google Drive file resides
    :type folder_id: str
    :param file_name: The name of the file residing in Google Drive
    :type file_name: str
    :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
    :type drive_id: str
    :param gcp_conn_id: The GCP connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    template_fields = [
        "destination_bucket",
        "destination_object",
        "folder_id",
        "file_name",
        "drive_id",
        "impersonation_chain",
    ]

    @apply_defaults
    def __init__(
        self,
        *,
        destination_bucket: str,
        destination_object: str,
        file_name: str,
        folder_id: str,
        drive_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.destination_bucket = destination_bucket
        self.destination_object = destination_object
        self.folder_id = folder_id
        self.drive_id = drive_id
        self.file_name = file_name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.file_metadata = None

    def _set_file_metadata(self, gdrive_hook):
        if not self.file_metadata:
            self.file_metadata = gdrive_hook.get_file_id(
                folder_id=self.folder_id, file_name=self.file_name, drive_id=self.drive_id
            )
        return self.file_metadata

    def _upload_data(self, gcs_hook: GCSHook, gdrive_hook: GoogleDriveHook) -> str:
        file_handle = BytesIO()
        self._set_file_metadata(gdrive_hook=gdrive_hook)
        file_id = self.file_metadata["id"]
        mime_type = self.file_metadata["mime_type"]
        request = gdrive_hook.get_media_request(file_id=file_id)
        gdrive_hook.download_content_from_request(
            file_handle=file_handle, request=request, chunk_size=104857600
        )
        gcs_hook.upload(
            bucket_name=self.destination_bucket,
            object_name=self.destination_object,
            data=file_handle.getvalue(),
            mime_type=mime_type,
        )

    def execute(self, context):
        gdrive_hook = GoogleDriveHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        self._upload_data(gdrive_hook=gdrive_hook, gcs_hook=gcs_hook)
