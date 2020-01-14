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
import uuid
from typing import Any, Dict, List, Optional

from googleapiclient import http

from airflow import AirflowException
from airflow.models import BaseOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.marketing_platform.hooks.campaign_manager import GoogleCampaignManagerHook
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
    :param report_name: The name of the report to delete.
    :type report_name: str
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
        "report_name",
        "api_version",
        "gcp_conn_id",
        "delegate_to",
    )

    @apply_defaults
    def __init__(
        self,
        profile_id: str,
        report_name: Optional[str] = None,
        report_id: Optional[str] = None,
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        if not (report_name or report_id):
            raise AirflowException("Please provide `report_name` or `report_id`.")
        if report_name and report_id:
            raise AirflowException(
                "Please provide only one parameter `report_name` or `report_id`."
            )

        self.profile_id = profile_id
        self.report_name = report_name
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
        if self.report_name:
            reports = hook.list_reports(profile_id=self.profile_id)
            reports_with_name = [r for r in reports if r["name"] == self.report_name]
            for report in reports_with_name:
                report_id = report["id"]
                self.log.info("Deleting Campaign Manager report: %s", report_id)
                hook.delete_report(profile_id=self.profile_id, report_id=report_id)
                self.log.info("Report deleted.")
        elif self.report_id:
            self.log.info("Deleting Campaign Manager report: %s", self.report_id)
            hook.delete_report(profile_id=self.profile_id, report_id=self.report_id)
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
    :param gzip: Option to compress local file or file data for upload
    :type gzip: bool
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
    def __init__(  # pylint: disable=too-many-arguments
        self,
        profile_id: str,
        report_id: str,
        file_id: str,
        bucket_name: str,
        report_name: Optional[str] = None,
        gzip: bool = True,
        chunk_size: int = 10 * 1024 * 1024,
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
        self.gzip = gzip
        self.bucket_name = self._set_bucket_name(bucket_name)
        self.report_name = report_name
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
        hook = GoogleCampaignManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        gcs_hook = GCSHook(
            google_cloud_storage_conn_id=self.gcp_conn_id, delegate_to=self.delegate_to
        )
        # Get name of the report
        report = hook.get_report(
            file_id=self.file_id, profile_id=self.profile_id, report_id=self.report_id
        )
        report_name = self.report_name or report.get("fileName", str(uuid.uuid4()))
        report_name = self._resolve_file_name(report_name)

        # Download the report
        self.log.info("Starting downloading report %s", self.report_id)
        request = hook.get_report_file(
            profile_id=self.profile_id, report_id=self.report_id, file_id=self.file_id
        )
        with tempfile.NamedTemporaryFile() as temp_file:
            downloader = http.MediaIoBaseDownload(
                fd=temp_file, request=request, chunksize=self.chunk_size
            )
            download_finished = False
            while not download_finished:
                _, download_finished = downloader.next_chunk()

            temp_file.flush()
            # Upload the local file to bucket
            gcs_hook.upload(
                bucket_name=self.bucket_name,
                object_name=report_name,
                gzip=self.gzip,
                filename=temp_file.name,
                mime_type="text/csv",
            )

        self.xcom_push(context, key="report_name", value=report_name)


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
        report: Dict[str, Any],
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
        response = hook.insert_report(
            profile_id=self.profile_id, report=self.report
        )  # type: ignore
        report_id = response.get("id")
        self.xcom_push(context, key="report_id", value=report_id)
        self.log.info("Report successfully inserted. Report id: %s", report_id)
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
    :type synchronous: bool
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
        synchronous: bool = False,
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
        file_id = response.get("id")
        self.xcom_push(context, key="file_id", value=file_id)
        self.log.info("Report file id: %s", file_id)
        return response


class GoogleCampaignManagerBatchInsertConversionsOperator(BaseOperator):
    """
    Inserts conversions.

    .. seealso::
        Check official API docs:
        https://developers.google.com/doubleclick-advertisers/v3.3/conversions/batchinsert

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCampaignManagerBatchInsertConversionsOperator`

    :param profile_id: User profile ID associated with this request.
    :type profile_id: str
    :param conversions: Conversations to insert, should by type of Conversation:
        https://developers.google.com/doubleclick-advertisers/v3.3/conversions#resource
    :type conversions: List[Dict[str, Any]]
    :param encryption_entity_type: The encryption entity type. This should match the encryption
        configuration for ad serving or Data Transfer.
    :type encryption_entity_type: str
    :param encryption_entity_id: The encryption entity ID. This should match the encryption
        configuration for ad serving or Data Transfer.
    :type encryption_entity_id: int
    :param encryption_source: Describes whether the encrypted cookie was received from ad serving
        (the %m macro) or from Data Transfer.
    :type encryption_source: str
    :param max_failed_inserts: The maximum number of conversions that failed to be inserted
    :type max_failed_inserts: int
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: Optional[str]
    """

    template_fields = (
        "profile_id",
        "conversions",
        "encryption_entity_type",
        "encryption_entity_id",
        "encryption_source",
    )

    @apply_defaults
    def __init__(
        self,
        profile_id: str,
        conversions: List[Dict[str, Any]],
        encryption_entity_type: str,
        encryption_entity_id: int,
        encryption_source: str,
        max_failed_inserts: int = 0,
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.profile_id = profile_id
        self.conversions = conversions
        self.encryption_entity_type = encryption_entity_type
        self.encryption_entity_id = encryption_entity_id
        self.encryption_source = encryption_source
        self.max_failed_inserts = max_failed_inserts
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleCampaignManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        response = hook.conversions_batch_insert(
            profile_id=self.profile_id,
            conversions=self.conversions,
            encryption_entity_type=self.encryption_entity_type,
            encryption_entity_id=self.encryption_entity_id,
            encryption_source=self.encryption_source,
            max_failed_inserts=self.max_failed_inserts
        )
        return response


class GoogleCampaignManagerBatchUpdateConversionsOperator(BaseOperator):
    """
    Updates existing conversions.

    .. seealso::
        Check official API docs:
        https://developers.google.com/doubleclick-advertisers/v3.3/conversions/batchupdate

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GoogleCampaignManagerBatchUpdateConversionsOperator`

    :param profile_id: User profile ID associated with this request.
    :type profile_id: str
    :param conversions: Conversations to update, should by type of Conversation:
        https://developers.google.com/doubleclick-advertisers/v3.3/conversions#resource
    :type conversions: List[Dict[str, Any]]
    :param encryption_entity_type: The encryption entity type. This should match the encryption
        configuration for ad serving or Data Transfer.
    :type encryption_entity_type: str
    :param encryption_entity_id: The encryption entity ID. This should match the encryption
        configuration for ad serving or Data Transfer.
    :type encryption_entity_id: int
    :param encryption_source: Describes whether the encrypted cookie was received from ad serving
        (the %m macro) or from Data Transfer.
    :type encryption_source: str
    :param max_failed_updates: The maximum number of conversions that failed to be updateed
    :type max_failed_updates: int
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any. For this to work, the service accountmaking the
        request must have  domain-wide delegation enabled.
    :type delegate_to: Optional[str]
    """

    template_fields = (
        "profile_id",
        "conversions",
        "encryption_entity_type",
        "encryption_entity_id",
        "encryption_source",
    )

    @apply_defaults
    def __init__(
        self,
        profile_id: str,
        conversions: List[Dict[str, Any]],
        encryption_entity_type: str,
        encryption_entity_id: int,
        encryption_source: str,
        max_failed_updates: int = 0,
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        *args,
        **kwargs
    ) -> None:
        super().__init__(*args, **kwargs)
        self.profile_id = profile_id
        self.conversions = conversions
        self.encryption_entity_type = encryption_entity_type
        self.encryption_entity_id = encryption_entity_id
        self.encryption_source = encryption_source
        self.max_failed_updates = max_failed_updates
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to

    def execute(self, context: Dict):
        hook = GoogleCampaignManagerHook(
            gcp_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to,
            api_version=self.api_version,
        )
        response = hook.conversions_batch_update(
            profile_id=self.profile_id,
            conversions=self.conversions,
            encryption_entity_type=self.encryption_entity_type,
            encryption_entity_id=self.encryption_entity_id,
            encryption_source=self.encryption_source,
            max_failed_updates=self.max_failed_updates
        )
        return response
