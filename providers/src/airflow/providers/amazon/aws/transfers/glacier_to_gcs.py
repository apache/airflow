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
from airflow.providers.amazon.aws.hooks.glacier import GlacierHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GlacierToGCSOperator(BaseOperator):
    """
    Transfers data from Amazon Glacier to Google Cloud Storage.

    .. note::
        Please be warn that GlacierToGCSOperator may depends on memory usage.
        Transferring big files may not working well.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GlacierToGCSOperator`

    :param aws_conn_id: The reference to the AWS connection details
    :param gcp_conn_id: The reference to the GCP connection details
    :param vault_name: the Glacier vault on which job is executed
    :param bucket_name: the Google Cloud Storage bucket where the data will be transferred
    :param object_name: the name of the object to check in the Google cloud
        storage bucket.
    :param gzip: option to compress local file or file data for upload
    :param chunk_size: size of chunk in bytes the that will be downloaded from Glacier vault
    :param google_impersonation_chain: Optional Google service account to impersonate using
        short-term credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = ("vault_name", "bucket_name", "object_name")

    def __init__(
        self,
        *,
        aws_conn_id: str | None = "aws_default",
        gcp_conn_id: str = "google_cloud_default",
        vault_name: str,
        bucket_name: str,
        object_name: str,
        gzip: bool,
        chunk_size: int = 1024,
        google_impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.gcp_conn_id = gcp_conn_id
        self.vault_name = vault_name
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gzip = gzip
        self.chunk_size = chunk_size
        self.impersonation_chain = google_impersonation_chain

    def execute(self, context: Context) -> str:
        glacier_hook = GlacierHook(aws_conn_id=self.aws_conn_id)
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        job_id = glacier_hook.retrieve_inventory(vault_name=self.vault_name)

        with tempfile.NamedTemporaryFile() as temp_file:
            glacier_data = glacier_hook.retrieve_inventory_results(
                vault_name=self.vault_name, job_id=job_id["jobId"]
            )
            # Read the file content in chunks using StreamingBody
            # https://botocore.amazonaws.com/v1/documentation/api/latest/reference/response.html
            stream = glacier_data["body"]
            for chunk in stream.iter_chunks(chunk_size=self.chunk_size):
                temp_file.write(chunk)
            temp_file.flush()
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=temp_file.name,
                gzip=self.gzip,
            )
        return f"gs://{self.bucket_name}/{self.object_name}"
