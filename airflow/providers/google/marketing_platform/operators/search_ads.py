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
This module contains Google Search Ads operators.
"""
from tempfile import NamedTemporaryFile
from typing import Any, Dict, Optional

from airflow import AirflowException
from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.models.baseoperator import BaseOperator
from airflow.providers.google.marketing_platform.hooks.search_ads import GoogleSearchAdsHook
from airflow.utils.decorators import apply_defaults


class GoogleSearchAdsInsertReportOperator(BaseOperator):
    """
    Inserts a report request into the reporting system.

    .. seealso:
        For API documentation check:
        https://developers.google.com/search-ads/v2/reference/reports/request

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSearchAdsInsertReportOperator`

    :param report: Report to be generated
    :type report: Dict[str, Any]
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = ("report",)
    template_ext = (".json",)

    @apply_defaults
    def __init__(
        self,
        report: Dict[str, Any],
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.report = report
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleSearchAdsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Generating Search Ads report")
        response = hook.insert_report(report=self.report)
        report_id = response.get("id")
        self.xcom_push(context, key="report_id", value=report_id)
        self.log.info("Report generated, id: %s", report_id)
        return response


class GoogleSearchAdsDownloadReportOperator(BaseOperator):
    """
    Downloads a report to GCS bucket.

    .. seealso:
        For API documentation check:
        https://developers.google.com/search-ads/v2/reference/reports/getFile

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleSearchAdsGetfileReportOperator`

    :param report_id: ID of the report.
    :type report_id: str
    :param bucket_name: The bucket to upload to.
    :type bucket_name: str
    :param report_name: The report name to set when uploading the local file. If not provided then
        report_id is used.
    :type report_name: str
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

    template_fields = ("report_name", "report_id", "bucket_name")

    @apply_defaults
    def __init__(
        self,
        report_id: str,
        bucket_name: str,
        report_name: Optional[str] = None,
        gzip: bool = True,
        chunk_size: int = 10 * 1024 * 1024,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.report_id = report_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.report_id = report_id
        self.chunk_size = chunk_size
        self.gzip = gzip
        self.bucket_name = self._set_bucket_name(bucket_name)
        self.report_name = report_name

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

    @staticmethod
    def _handle_report_fragment(fragment: bytes) -> bytes:
        fragment_records = fragment.split(b"\n", 1)
        if len(fragment_records) > 1:
            return fragment_records[1]
        return b""

    def execute(self, context: Dict):
        hook = GoogleSearchAdsHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )

        gcs_hook = GoogleCloudStorageHook(
            gcp_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )

        # Resolve file name of the report
        report_name = self.report_name or self.report_id
        report_name = self._resolve_file_name(report_name)

        response = hook.get(report_id=self.report_id)
        if not response['isReportReady']:
            raise AirflowException('Report {} is not ready yet'.format(self.report_id))

        # Resolve report fragments
        fragments_count = len(response["files"])

        # Download chunks of report's data
        self.log.info("Downloading Search Ads report %s", self.report_id)
        with NamedTemporaryFile() as temp_file:
            for i in range(fragments_count):
                byte_content = hook.get_file(
                    report_fragment=i, report_id=self.report_id
                )
                fragment = (
                    byte_content
                    if i == 0
                    else self._handle_report_fragment(byte_content)
                )
                temp_file.write(fragment)

            temp_file.flush()

            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=report_name,
                gzip=self.gzip,
                filename=temp_file.name,
            )
        self.xcom_push(context, key="file_name", value=report_name)
