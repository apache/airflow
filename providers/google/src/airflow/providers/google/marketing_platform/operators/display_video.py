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
"""This module contains Google DisplayVideo operators."""

from __future__ import annotations

import os
import tempfile
import zipfile
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.providers.google.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class GoogleDisplayVideo360CreateSDFDownloadTaskOperator(BaseOperator):
    """
    Creates an SDF operation task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360CreateSDFDownloadTaskOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/display-video/api/reference/rest`

    :param version: The SDF version of the downloaded file.
    :param partner_id: The ID of the partner to download SDF for.
    :param advertiser_id: The ID of the advertiser to download SDF for.
    :param parent_entity_filter: Filters on selected file types.
    :param id_filter: Filters on entities by their entity IDs.
    :param inventory_source_filter: Filters on Inventory Sources by their IDs.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "body_request",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        body_request: dict[str, Any],
        api_version: str = "v4",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.body_request = body_request
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Creating operation for SDF download task...")
        operation = hook.create_sdf_download_operation(body_request=self.body_request)

        name = operation["name"]
        context["task_instance"].xcom_push(key="name", value=name)
        self.log.info("Created SDF operation with name: %s", name)

        return operation


class GoogleDisplayVideo360SDFtoGCSOperator(BaseOperator):
    """
    Download SDF media and save it in the Google Cloud Storage.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360SDFtoGCSOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/display-video/api/reference/rest`

    :param version: The SDF version of the downloaded file.
    :param partner_id: The ID of the partner to download SDF for.
    :param advertiser_id: The ID of the advertiser to download SDF for.
    :param parent_entity_filter: Filters on selected file types.
    :param id_filter: Filters on entities by their entity IDs.
    :param inventory_source_filter: Filters on Inventory Sources by their IDs.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "operation_name",
        "bucket_name",
        "object_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        operation_name: str,
        bucket_name: str,
        object_name: str,
        gzip: bool = False,
        api_version: str = "v4",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.operation_name = operation_name
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gzip = gzip
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Retrieving operation...")
        operation_state = hook.get_sdf_download_operation(operation_name=self.operation_name)

        self.log.info("Creating file for upload...")
        media = hook.download_media(resource_name=operation_state["response"]["resourceName"])

        self.log.info("Sending file to the Google Cloud Storage...")
        with tempfile.TemporaryDirectory() as tmp_dir:
            zip_path = os.path.join(tmp_dir, "sdf.zip")

            # Download the ZIP
            with open(zip_path, "wb") as f:
                hook.download_content_from_request(f, media, chunk_size=1024 * 1024)

            # Extract CSV
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(tmp_dir)

            # Upload CSV file
            for fname in os.listdir(tmp_dir):
                if fname.endswith(".csv"):
                    gcs_hook.upload(
                        bucket_name=self.bucket_name,
                        object_name=self.object_name,
                        filename=os.path.join(tmp_dir, fname),
                        gzip=False,
                    )
        return f"{self.bucket_name}/{self.object_name}"
