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
This module contains Google DisplayVideo operators.
"""
import csv
import shutil
import tempfile
import urllib.request
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.utils.decorators import apply_defaults


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
    :type body: Dict[str, Any]
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("body",)
    template_ext = (".json",)

    @apply_defaults
    def __init__(
        self,
        body: Dict[str, Any],
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.body = body
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
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
    :type report_id: str
    :param report_name: Name of the report to delete.
    :type report_name: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("report_id",)

    @apply_defaults
    def __init__(
        self,
        report_id: Optional[str] = None,
        report_name: Optional[str] = None,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.report_id = report_id
        self.report_name = report_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

        if report_name and report_id:
            raise AirflowException("Use only one value - `report_name` or `report_id`.")

        if not (report_name or report_id):
            raise AirflowException(
                "Provide one of the values: `report_name` or `report_id`."
            )

    def execute(self, context: Dict):
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        if self.report_id:
            reports_ids_to_delete = [self.report_id]
        else:
            reports = hook.list_queries()
            reports_ids_to_delete = [
                report["queryId"]
                for report in reports
                if report["metadata"]["title"] == self.report_name
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
    :type report_id: str
    :param bucket_name: The bucket to upload to.
    :type bucket_name: str
    :param report_name: The report name to set when uploading the local file.
    :type report_name: str
    :param chunk_size: File will be downloaded in chunks of this many bytes.
    :type chunk_size: int
    :param gzip: Option to compress local file or file data for upload
    :type gzip: bool
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("report_id", "bucket_name", "report_name")

    @apply_defaults
    def __init__(
        self,
        report_id: str,
        bucket_name: str,
        report_name: Optional[str] = None,
        gzip: bool = True,
        chunk_size: int = 10 * 1024 * 1024,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.report_id = report_id
        self.chunk_size = chunk_size
        self.gzip = gzip
        self.bucket_name = self._set_bucket_name(bucket_name)
        self.report_name = report_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def _resolve_file_name(self, name: str) -> str:
        new_name = name if name.endswith(".csv") else f"{name}.csv"
        new_name = f"{new_name}.gz" if self.gzip else new_name
        return new_name

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")

    def execute(self, context: Dict):
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )

        resource = hook.get_query(query_id=self.report_id)
        # Check if report is ready
        if resource["metadata"]["running"]:
            raise AirflowException(f"Report {self.report_id} is still running")

        # If no custom report_name provided, use DV360 name
        file_url = resource["metadata"]["googleCloudStoragePathForLatestReport"]
        report_name = self.report_name or urlparse(file_url).path.split("/")[-1]
        report_name = self._resolve_file_name(report_name)

        # Download the report
        self.log.info("Starting downloading report %s", self.report_id)
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            with urllib.request.urlopen(file_url) as response:
                shutil.copyfileobj(response, temp_file, length=self.chunk_size)

            temp_file.flush()
            # Upload the local file to bucket
            gcs_hook.upload(
                bucket_name=self.bucket_name,
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
    :type report_id: str
    :param params: Parameters for running a report as described here:
        https://developers.google.com/bid-manager/v1/queries/runquery
    :type params: Dict[str, Any]
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service account making the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("report_id", "params")

    @apply_defaults
    def __init__(
        self,
        report_id: str,
        params: Dict[str, Any],
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.report_id = report_id
        self.params = params
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info(
            "Running report %s with the following params:\n %s",
            self.report_id,
            self.params,
        )
        hook.run_query(query_id=self.report_id, params=self.params)


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
    :type request_body: Dict[str, Any],
    """

    template_fields = ("request_body", "bucket_name", "object_name")

    @apply_defaults
    def __init__(
        self,
        request_body: Dict[str, Any],
        bucket_name: str,
        object_name: str,
        gzip: bool = False,
        api_version: str = "v1.1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.request_body = request_body
        self.object_name = object_name
        self.bucket_name = bucket_name
        self.gzip = gzip
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict) -> str:
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            api_version=self.api_version,
            delegate_to=self.delegate_to,
        )

        self.log.info("Retrieving report...")
        content: List[str] = hook.download_line_items(request_body=self.request_body)

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
    :type request_body: Dict[str, Any]
    :param bucket_name: The bucket form data is downloaded.
    :type bucket_name: str
    :param object_name: The object to fetch.
    :type object_name: str,
    :param filename: The filename to fetch.
    :type filename: str,
    :param dry_run: Upload status without actually persisting the line items.
    :type filename: str,
    """

    template_fields = (
        "bucket_name",
        "object_name",
    )

    @apply_defaults
    def __init__(
        self,
        bucket_name: str,
        object_name: str,
        api_version: str = "v1.1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
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

    :param version: The SDF version of the downloaded file..
    :type version: str
    :param partner_id: The ID of the partner to download SDF for.
    :type partner_id: str
    :param advertiser_id: The ID of the advertiser to download SDF for.
    :type advertiser_id: str
    :param parent_entity_filter: Filters on selected file types.
    :type parent_entity_filter: Dict[str, Any]
    :param id_filter: Filters on entities by their entity IDs.
    :type id_filter: Dict[str, Any]
    :param inventory_source_filter: Filters on Inventory Sources by their IDs.
    :type inventory_source_filter: Dict[str, Any]
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service account making the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("body_request", )

    @apply_defaults
    def __init__(
        self,
        body_request: Dict[str, Any],
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.body_request = body_request
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )

        self.log.info("Creating operation for SDF download task...")
        operation = hook.create_sdf_download_operation(
            body_request=self.body_request
        )

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

    :param version: The SDF version of the downloaded file..
    :type version: str
    :param partner_id: The ID of the partner to download SDF for.
    :type partner_id: str
    :param advertiser_id: The ID of the advertiser to download SDF for.
    :type advertiser_id: str
    :param parent_entity_filter: Filters on selected file types.
    :type parent_entity_filter: Dict[str, Any]
    :param id_filter: Filters on entities by their entity IDs.
    :type id_filter: Dict[str, Any]
    :param inventory_source_filter: Filters on Inventory Sources by their IDs.
    :type inventory_source_filter: Dict[str, Any]
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service account making the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("operation_name", "bucket_name", "object_name")

    @apply_defaults
    def __init__(
        self,
        operation_name: str,
        bucket_name: str,
        object_name: str,
        gzip: bool = False,
        api_version: str = "v1",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.operation_name = operation_name
        self.bucket_name = bucket_name
        self.object_name = object_name
        self.gzip = gzip
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleDisplayVideo360Hook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        gcs_hook = GCSHook(gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to)

        self.log.info("Retrieving operation...")
        operation = hook.get_sdf_download_operation(operation_name=self.operation_name)

        self.log.info("Creating file for upload...")
        media = hook.download_media(resource_name=operation)

        self.log.info("Sending file to the Google Cloud Storage...")
        with tempfile.NamedTemporaryFile() as temp_file:
            hook.download_content_from_request(
                temp_file, media, chunk_size=1024 * 1024
            )
            temp_file.flush()
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=self.object_name,
                filename=temp_file.name,
                gzip=self.gzip,
            )

        return f"{self.bucket_name}/{self.object_name}"
