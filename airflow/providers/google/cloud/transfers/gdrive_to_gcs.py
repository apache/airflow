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

import warnings
from typing import TYPE_CHECKING, Optional, Sequence, Union

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


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
    :param destination_bucket: Same as bucket_name, but for backward compatibly
    :param destination_object: Same as object_name, but for backward compatibly
    :param folder_id: The folder id of the folder in which the Google Drive file resides
    :param file_name: The name of the file residing in Google Drive
    :param drive_id: Optional. The id of the shared Google Drive in which the file resides.
    :param gcp_conn_id: The GCP connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
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
        bucket_name: Optional[str] = None,
        object_name: Optional[str] = None,
        destination_bucket: Optional[str] = None,  # deprecated
        destination_object: Optional[str] = None,  # deprecated
        file_name: str,
        folder_id: str,
        drive_id: Optional[str] = None,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        if destination_bucket:
            warnings.warn(
                "`destination_bucket` is deprecated please use `bucket_name`",
                DeprecationWarning,
                stacklevel=2,
            )
        actual_bucket = destination_bucket or bucket_name
        if actual_bucket is None:
            raise RuntimeError("One of the destination_bucket or bucket_name must be set")
        self.bucket_name: str = actual_bucket
        self.object_name = destination_object or object_name
        if destination_object:
            warnings.warn(
                "`destination_object` is deprecated please use `object_name`",
                DeprecationWarning,
                stacklevel=2,
            )
        self.folder_id = folder_id
        self.drive_id = drive_id
        self.file_name = file_name
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: 'Context'):
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
        file_metadata = gdrive_hook.get_file_id(
            folder_id=self.folder_id, file_name=self.file_name, drive_id=self.drive_id
        )
        with gcs_hook.provide_file_and_upload(
            bucket_name=self.bucket_name, object_name=self.object_name
        ) as file:
            gdrive_hook.download_file(file_id=file_metadata["id"], file_handle=file)
