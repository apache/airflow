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
from __future__ import annotations

import tempfile
from typing import TYPE_CHECKING, Sequence

from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AzureBlobStorageToGCSOperator(BaseOperator):
    """
    Operator transfers data from Azure Blob Storage to specified bucket in Google Cloud Storage

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AzureBlobStorageToGCSOperator`

    :param wasb_conn_id: Reference to the wasb connection.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param blob_name: Name of the blob
    :param file_path: Path to the file to download
    :param container_name: Name of the container
    :param bucket_name: The bucket to upload to
    :param object_name: The object name to set when uploading the file
    :param filename: The local file path to the file to be uploaded
    :param gzip: Option to compress local file or file data for upload
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
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        *,
        wasb_conn_id='wasb_default',
        gcp_conn_id: str = "google_cloud_default",
        blob_name: str,
        file_path: str,
        container_name: str,
        bucket_name: str,
        object_name: str,
        filename: str,
        gzip: bool,
        delegate_to: str | None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.wasb_conn_id = wasb_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.blob_name = blob_name
        self.file_path = file_path
        self.container_name = container_name
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.filename = filename
        self.gzip = gzip
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    template_fields: Sequence[str] = (
        "blob_name",
        "file_path",
        "container_name",
        "bucket_name",
        "object_name",
        "filename",
    )

    def execute(self, context: Context) -> str:
        azure_hook = WasbHook(wasb_conn_id=self.wasb_conn_id)
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        with tempfile.NamedTemporaryFile() as temp_file:
            self.log.info("Downloading data from blob: %s", self.blob_name)
            azure_hook.get_file(
                file_path=temp_file.name,
                container_name=self.container_name,
                blob_name=self.blob_name,
            )
            self.log.info(
                "Uploading data from blob's: %s into GCP bucket: %s", self.object_name, self.bucket_name
            )
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=temp_file.name,
                gzip=self.gzip,
            )
            self.log.info(
                "Resources have been uploaded from blob: %s to GCS bucket:%s",
                self.blob_name,
                self.bucket_name,
            )
        return f"gs://{self.bucket_name}/{self.object_name}"
