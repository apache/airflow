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
This module contains Google DisplayVideo operators.
"""
import shutil
import tempfile
import urllib.request
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from airflow import AirflowException
from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.models import BaseOperator
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
        **kwargs
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
        **kwargs
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
        **kwargs
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
        csv = ".csv"
        gzip = ".gz"
        if not name.endswith(csv):
            name += csv
        if self.gzip:
            name += gzip
        return name

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
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )

        resource = hook.get_query(query_id=self.report_id)
        # Check if report is ready
        if resource["metadata"]["running"]:
            raise AirflowException('Report {} is still running'.format(self.report_id))

        # If no custom report_name provided, use DV360 name
        file_url = resource["metadata"]["googleCloudStoragePathForLatestReport"]
        report_name = self.report_name or urlparse(file_url).path.split('/')[2]
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
        self.xcom_push(context, key='report_name', value=report_name)


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
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
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
        **kwargs
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
