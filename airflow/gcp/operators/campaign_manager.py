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
This module contains Google CampaignManager operators.
"""
import tempfile
from typing import Any, Dict, Optional, Union

from googleapiclient import http

from airflow.gcp.hooks.gcs import GoogleCloudStorageHook
from airflow.gcp.hooks.campaign_manager import GoogleCampaignManagerHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class GoogleCampaignManagerDeleteReportOperator(BaseOperator):
    """
    Deletes a report by its ID.

    .. seealso::
        Check official API docs:
        https://developers.google.com/doubleclick-advertisers/v3.3/reports/delete

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCampaignManagerDeleteReportOperator`

    :param profile_id: The DFA user profile ID.
    :type profile_id: str
    :param report_id: The ID of the report.
    :type report_id: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = (
        "profile_id",
        "report_id",
        "api_version",
        "gcp_conn_id",
        "delegate_to",
    )

    @apply_defaults
    def __init__(
        self,
        profile_id: str,
        report_id: str,
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.profile_id = profile_id
        self.report_id = report_id
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleCampaignManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Deleting Campaign Manager report: %s", self.report_id)
        hook.delete(profile_id=self.profile_id, report_id=self.report_id)
        self.log.info("Report deleted.")


class GoogleCampaignManagerDownloadReportOperator(BaseOperator):
    """
    Retrieves a report and uploads it to GCS bucket.

    .. seealso::
        Check official API docs:
        https://developers.google.com/doubleclick-advertisers/v3.3/reports/files/get

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCampaignManagerDownloadReportOperator`

    :param profile_id: The DFA user profile ID.
    :type profile_id: str
    :param report_id: The ID of the report.
    :type report_id: str
    :param file_id: The ID of the report file.
    :type file_id: str
    :param bucket_name: The bucket to upload to.
    :type bucket_name: str
    :param report_name: The report name to set when uploading the local file.
    :type report_name: str
    :param chunk_size: File will be downloaded in chunks of this many bytes.
    :type chunk_size: int
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = (
        "profile_id",
        "report_id",
        "file_id",
        "bucket_name",
        "report_name",
        "chunk_size",
        "api_version",
        "gcp_conn_id",
        "delegate_to",
    )

    @apply_defaults
    def __init__(
        self,
        profile_id: str,
        report_id: str,
        file_id: str,
        bucket_name: str,
        report_name: str,
        chunk_size: int = 5 * 1024 * 1024,
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.profile_id = profile_id
        self.report_id = report_id
        self.file_id = file_id
        self.api_version = api_version
        self.chunk_size = chunk_size
        self.bucket_name = self._set_bucket_name(bucket_name)
        self.report_name = self._set_report_name(report_name)
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    @staticmethod
    def _set_report_name(name: str) -> str:
        return name if name.endswith(".csv") else name + ".csv"

    @staticmethod
    def _set_bucket_name(name: str) -> str:
        bucket = name if not name.startswith("gs://") else name[5:]
        return bucket.strip("/")

    def execute(self, context: Dict):
        hook = GoogleCampaignManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        gcs_hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )
        self.log.info("Starting downloading report %s", self.report_id)
        request = hook.get_report_file(
            profile_id=self.profile_id, report_id=self.report_id, file_id=self.file_id
        )

        # Download the report
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            downloader = http.MediaIoBaseDownload(
                fd=temp_file, request=request, chunksize=self.chunk_size
            )
            download_finished = False
            while not download_finished:
                _, download_finished = downloader.next_chunk()

            # Upload the local file to bucket
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=self.report_name,
                gzip=True,
                filename=temp_file.name,
                mime_type="text/csv",
            )


class GoogleCampaignManagerInsertReportOperator(BaseOperator):
    """
    Creates a report.

    .. seealso::
        Check official API docs:
        https://developers.google.com/doubleclick-advertisers/v3.3/reports/insert

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCampaignManagerInsertReportOperator`

    :param profile_id: The DFA user profile ID.
    :type profile_id: str
    :param report: Report to be created.
    :type report: Dict[str, Any]
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = (
        "profile_id",
        "report",
        "api_version",
        "gcp_conn_id",
        "delegate_to",
    )
    template_ext = (".json",)

    @apply_defaults
    def __init__(
        self,
        profile_id: str,
        report: Union[Dict[str, Any], str],
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.profile_id = profile_id
        self.report = report
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleCampaignManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Inserting Campaign Manager report.")
        response = hook.insert(profile_id=self.profile_id, report=self.report)  # type: ignore
        self.log.info("Report successfully inserted. Report id: %s", response["id"])
        return response


class GoogleCampaignManagerRunReportOperator(BaseOperator):
    """
    Runs a report.

    .. seealso::
        Check official API docs:
        https://developers.google.com/doubleclick-advertisers/v3.3/reports/run

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCampaignManagerRunReportOperator`

    :param profile_id: The DFA profile ID.
    :type profile_id: str
    :param report_id: The ID of the report.
    :type report_id: str
    :param synchronous: If set and true, tries to run the report synchronously.
    :type synchronous: Optional[bool]
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: str
    """

    template_fields = (
        "profile_id",
        "report_id",
        "synchronous",
        "api_version",
        "gcp_conn_id",
        "delegate_to",
    )
    template_ext = (".json",)

    @apply_defaults
    def __init__(
        self,
        profile_id: str,
        report_id: str,
        synchronous: Optional[bool] = None,
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.profile_id = profile_id
        self.report_id = report_id
        self.synchronous = synchronous
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleCampaignManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        self.log.info("Running report %s", self.report_id)
        response = hook.run_report(
            profile_id=self.profile_id,
            report_id=self.report_id,
            synchronous=self.synchronous,
        )
        return response
