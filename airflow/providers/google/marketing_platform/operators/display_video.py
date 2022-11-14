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

import csv
import json
import shutil
import tempfile
import urllib.request
from typing import TYPE_CHECKING, Any, Sequence
from urllib.parse import urlsplit

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GoogleDisplayVideo360CreateReportOperator(BaseOperator):
    """
    Creates a query.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360CreateReportOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v1/queries/createquery`

    :param body: Report object passed to the request's body as described here:
        https://developers.google.com/bid-manager/v1/queries#resource
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "body",
        "impersonation_chain",
    )
    template_ext: Sequence[str] = (".json",)

    def __init__(
        self,
        *,
        body: dict[str, Any],
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.body = body
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def prepare_template(self) -> None:
        # If .json is passed then we have to read the file
        if isinstance(self.body, str) and self.body.endswith(".json"):
            with open(self.body) as file:
                self.body = json.load(file)

    def execute(self, context: Context) -> dict:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info("Creating Display & Video 360 report.")
        response = hook.create_query(query=self.body)
        report_id = response["queryId"]
        self.xcom_push(context, key="report_id", value=report_id)
        self.log.info("Created report with ID: %s", report_id)
        return response


class GoogleDisplayVideo360DeleteReportOperator(BaseOperator):
    """
    Deletes a stored query as well as the associated stored reports.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360DeleteReportOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v1/queries/deletequery`

    :param report_id: Report ID to delete.
    :param report_name: Name of the report to delete.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "report_id",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        report_id: str | None = None,
        report_name: str | None = None,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.report_id = report_id
        self.report_name = report_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

        if report_name and report_id:
            raise AirflowException("Use only one value - `report_name` or `report_id`.")

        if not (report_name or report_id):
            raise AirflowException("Provide one of the values: `report_name` or `report_id`.")

    def execute(self, context: Context) -> None:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        if self.report_id:
            reports_ids_to_delete = [self.report_id]
        else:
            reports = hook.list_queries()
            reports_ids_to_delete = [
                report["queryId"] for report in reports if report["metadata"]["title"] == self.report_name
            ]

        for report_id in reports_ids_to_delete:
            self.log.info("Deleting report with id: %s", report_id)
            hook.delete_query(query_id=report_id)
            self.log.info("Report deleted.")


class GoogleDisplayVideo360DownloadReportOperator(BaseOperator):
    """
    Retrieves a stored query.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360DownloadReportOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v1/queries/getquery`

    :param report_id: Report ID to retrieve.
    :param bucket_name: The bucket to upload to.
    :param report_name: The report name to set when uploading the local file.
    :param chunk_size: File will be downloaded in chunks of this many bytes.
    :param gzip: Option to compress local file or file data for upload
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "report_id",
        "bucket_name",
        "report_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        report_id: str,
        bucket_name: str,
        report_name: str | None = None,
        gzip: bool = True,
        chunk_size: int = 10 * 1024 * 1024,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.report_id = report_id
        self.chunk_size = chunk_size
        self.gzip = gzip
        self.bucket_name = bucket_name
        self.report_name = report_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
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
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        resource = hook.get_query(query_id=self.report_id)
        # Check if report is ready
        if resource["metadata"]["running"]:
            raise AirflowException(f"Report {self.report_id} is still running")

        # If no custom report_name provided, use DV360 name
        file_url = resource["metadata"]["googleCloudStoragePathForLatestReport"]
        report_name = self.report_name or urlsplit(file_url).path.split("/")[-1]
        report_name = self._resolve_file_name(report_name)

        # Download the report
        self.log.info("Starting downloading report %s", self.report_id)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            with urllib.request.urlopen(file_url) as response:
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
        self.xcom_push(context, key="report_name", value=report_name)


class GoogleDisplayVideo360RunReportOperator(BaseOperator):
    """
    Runs a stored query to generate a report.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360RunReportOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v1/queries/runquery`

    :param report_id: Report ID to run.
    :param parameters: Parameters for running a report as described here:
        https://developers.google.com/bid-manager/v1/queries/runquery
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
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
        "report_id",
        "parameters",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        report_id: str,
        parameters: dict[str, Any] | None = None,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.report_id = report_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.parameters = parameters
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        self.log.info(
            "Running report %s with the following parameters:\n %s",
            self.report_id,
            self.parameters,
        )
        hook.run_query(query_id=self.report_id, params=self.parameters)


class GoogleDisplayVideo360DownloadLineItemsOperator(BaseOperator):
    """
    Retrieves line items in CSV format.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360DownloadLineItemsOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v1.1/lineitems/downloadlineitems`

    :param request_body: dictionary with parameters that should be passed into.
            More information about it can be found here:
            https://developers.google.com/bid-manager/v1.1/lineitems/downloadlineitems
    """

    template_fields: Sequence[str] = (
        "request_body",
        "bucket_name",
        "object_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        request_body: dict[str, Any],
        bucket_name: str,
        object_name: str,
        gzip: bool = False,
        api_version: str = "v1.1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.request_body = request_body
        self.object_name = object_name
        self.bucket_name = bucket_name
        self.gzip = gzip
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Retrieving report...")
        content: list[str] = hook.download_line_items(request_body=self.request_body)

        with tempfile.NamedTemporaryFile("w+") as temp_file:
            writer = csv.writer(temp_file)
            writer.writerows(content)
            temp_file.flush()
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=temp_file.name,
                mime_type="text/csv",
                gzip=self.gzip,
            )
        return f"{self.bucket_name}/{self.object_name}"


class GoogleDisplayVideo360UploadLineItemsOperator(BaseOperator):
    """
    Uploads line items in CSV format.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleDisplayVideo360UploadLineItemsOperator`

    .. seealso::
        Check also the official API docs:
        `https://developers.google.com/bid-manager/v1.1/lineitems/uploadlineitems`

    :param request_body: request to upload line items.
    :param bucket_name: The bucket form data is downloaded.
    :param object_name: The object to fetch.
    :param filename: The filename to fetch.
    :param dry_run: Upload status without actually persisting the line items.
    """

    template_fields: Sequence[str] = (
        "bucket_name",
        "object_name",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        bucket_name: str,
        object_name: str,
        api_version: str = "v1.1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> None:
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Uploading file %s...")
        # Saving file in the temporary directory,
        # downloaded file from the GCS could be a 1GB size or even more
        with tempfile.NamedTemporaryFile("w+") as f:
            line_items = gcs_hook.download(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=f.name,
            )
            f.flush()
            hook.upload_line_items(line_items=line_items)


class GoogleDisplayVideo360CreateSDFDownloadTaskOperator(BaseOperator):
    """
    Creates SDF operation task.

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
        "body_request",
        "impersonation_chain",
    )

    def __init__(
        self,
        *,
        body_request: dict[str, Any],
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.body_request = body_request
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> dict[str, Any]:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Creating operation for SDF download task...")
        operation = hook.create_sdf_download_operation(body_request=self.body_request)

        name = operation["name"]
        self.xcom_push(context, key="name", value=name)
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
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
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
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context) -> str:
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
            impersonation_chain=self.impersonation_chain,
        )
        gcs_hook = GCSHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Retrieving operation...")
        operation_state = hook.get_sdf_download_operation(operation_name=self.operation_name)

        self.log.info("Creating file for upload...")
        media = hook.download_media(resource_name=operation_state["response"]["resourceName"])

        self.log.info("Sending file to the Google Cloud Storage...")
        with tempfile.NamedTemporaryFile() as temp_file:
            hook.download_content_from_request(temp_file, media, chunk_size=1024 * 1024)
            temp_file.flush()
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=temp_file.name,
                gzip=self.gzip,
            )

        return f"{self.bucket_name}/{self.object_name}"
