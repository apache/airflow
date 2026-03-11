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
"""This module contains operators for Bid Manager API part of the Google Display & Video 360."""

from __future__ import annotations

import json
import shutil
import tempfile
import urllib.request
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.marketing_platform.hooks.bid_manager import GoogleBidManagerHook
from airflow.providers.google.version_compat import BaseOperator

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class GoogleBidManagerCreateQueryOperator(BaseOperator):
    """
    Creates a query.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleBidManagerCreateQueryOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v2/queries/create`

    :param body: Report object passed to the request's body as described here:
        https://developers.google.com/bid-manager/v2/queries#Query
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with the first
        account from the list granting this role to the originating account (templated).
    """

    template_fields: Sequence[str] = (
        "body",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".json",)

    def __init__(
        self,
        *,
        body: dict[str, Any],
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.body = body
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def prepare_template(self) -> None:
        # If .json is passed then we have to read the file
        if isinstance(self.body, str) and self.body.endswith(".json"):
            with open(self.body) as file:
                self.body = json.load(file)

    def execute(self, context: Context) -> dict:
        hook = GoogleBidManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Bid Manager API query.")
        response = hook.create_query(query=self.body)
        query_id = response["queryId"]
        context["task_instance"].xcom_push(key="query_id", value=query_id)
        self.log.info("Created query with ID: %s", query_id)
        return response


class GoogleBidManagerRunQueryOperator(BaseOperator):
    """
    Runs a stored query to generate a report.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleBidManagerRunQueryOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v2/queries/run`

    :param query_id: Query ID to run.
    :param parameters: Parameters for running a report as described here:
        https://developers.google.com/bid-manager/v2/queries/run
    :param api_version: The version of the api that will be requested for example 'v3'.
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
        "query_id",
        "parameters",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        query_id: str,
        parameters: dict[str, Any] | None = None,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query_id = query_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.parameters = parameters
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict:
        hook = GoogleBidManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info(
            "Running query %s with the following parameters:\n %s",
            self.query_id,
            self.parameters,
        )
        response = hook.run_query(query_id=self.query_id, params=self.parameters)
        context["task_instance"].xcom_push(key="query_id", value=response["key"]["queryId"])
        context["task_instance"].xcom_push(key="report_id", value=response["key"]["reportId"])
        return response


class GoogleBidManagerDeleteQueryOperator(BaseOperator):
    """
    Deletes a stored query as well as the associated stored reports.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleBidManagerDeleteQueryOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v2/queries/delete`

    :param query_id: Query ID to delete.
    :param api_version: The version of the api that will be requested for example 'v3'.
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
        "query_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        query_id: str,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.query_id = query_id

    def execute(self, context: Context) -> None:
        hook = GoogleBidManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Deleting query with id: %s and all connected reports", self.query_id)
        hook.delete_query(query_id=self.query_id)
        self.log.info("Report deleted.")


class GoogleBidManagerDownloadReportOperator(BaseOperator):
    """
    Retrieves a stored query.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleBidManagerDownloadReportOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v2/queries/get`

    :param report_id: Report ID to retrieve.
    :param query_id: Query ID for which report was generated..
    :param bucket_name: The bucket to upload to.
    :param report_name: The report name to set when uploading the local file.
    :param chunk_size: File will be downloaded in chunks of this many bytes.
    :param gzip: Option to compress local file or file data for upload
    :param api_version: The version of the api that will be requested for example 'v3'.
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
        "query_id",
        "report_id",
        "bucket_name",
        "report_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        query_id: str,
        report_id: str,
        bucket_name: str,
        report_name: str | None = None,
        gzip: bool = True,
        chunk_size: int = 10 * 1024 * 1024,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.query_id = query_id
        self.report_id = report_id
        self.chunk_size = chunk_size
        self.gzip = gzip
        self.bucket_name = bucket_name
        self.report_name = report_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def _resolve_file_name(self, name: str) -> str:
        new_name = name if name.endswith(".csv") else f"{name}.csv"
        new_name = f"{new_name}.gz" if self.gzip else new_name
        return new_name

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")

    def execute(self, context: Context):
        hook = GoogleBidManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        resource = hook.get_report(query_id=self.query_id, report_id=self.report_id)
        status = resource.get("metadata", {}).get("status", {}).get("state")
        if resource and status not in ["DONE", "FAILED"]:
            raise AirflowException(f"Report {self.report_id} for query {self.query_id} is still running")

        # If no custom report_name provided, use Bid Manager name
        file_url = resource["metadata"]["googleCloudStoragePath"]
        if urllib.parse.urlparse(file_url).scheme == "file":
            raise AirflowException("Accessing local file is not allowed in this operator")
        report_name = self.report_name or urlsplit(file_url).path.split("/")[-1]
        report_name = self._resolve_file_name(report_name)

        # Download the report
        self.log.info("Starting downloading report %s", self.report_id)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            with urllib.request.urlopen(file_url) as response:  # nosec
                shutil.copyfileobj(response, temp_file, length=self.chunk_size)

            temp_file.flush()
            # Upload the local file to bucket
            bucket_name = self._set_bucket_name(self.bucket_name)
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=report_name,
                gzip=self.gzip,
                filename=temp_file.name,
                mime_type="text/csv",
            )
        self.log.info(
            "Report %s was saved in bucket %s as %s.",
            self.report_id,
            self.bucket_name,
            report_name,
        )
        context["task_instance"].xcom_push(key="report_name", value=report_name)
