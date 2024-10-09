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
"""This module contains Google Campaign Manager hook."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Sequence

from googleapiclient.discovery import Resource, build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from googleapiclient import http


class GoogleCampaignManagerHook(GoogleBaseHook):
    """Hook for Google Campaign Manager."""

    _conn: Resource | None = None

    def __init__(
        self,
        api_version: str = "v4",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str | None = None,
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """Retrieve connection to Campaign Manager."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "dfareporting",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def delete_report(self, profile_id: str, report_id: str) -> Any:
        """
        Delete a report by its ID.

        :param profile_id: The DFA user profile ID.
        :param report_id: The ID of the report.
        """
        response = (
            self.get_conn()
            .reports()
            .delete(profileId=profile_id, reportId=report_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def insert_report(self, profile_id: str, report: dict[str, Any]) -> Any:
        """
        Create  a report.

        :param profile_id: The DFA user profile ID.
        :param report: The report resource to be inserted.
        """
        response = (
            self.get_conn()
            .reports()
            .insert(profileId=profile_id, body=report)
            .execute(num_retries=self.num_retries)
        )
        return response

    def list_reports(
        self,
        profile_id: str,
        max_results: int | None = None,
        scope: str | None = None,
        sort_field: str | None = None,
        sort_order: str | None = None,
    ) -> list[dict]:
        """
        Retrieve  list of reports.

        :param profile_id: The DFA user profile ID.
        :param max_results: Maximum number of results to return.
        :param scope: The scope that defines which results are returned.
        :param sort_field: The field by which to sort the list.
        :param sort_order: Order of sorted results.
        """
        reports: list[dict] = []
        conn = self.get_conn()
        request = conn.reports().list(
            profileId=profile_id,
            maxResults=max_results,
            scope=scope,
            sortField=sort_field,
            sortOrder=sort_order,
        )
        while request is not None:
            response = request.execute(num_retries=self.num_retries)
            reports.extend(response.get("items", []))
            request = conn.reports().list_next(previous_request=request, previous_response=response)

        return reports

    def patch_report(self, profile_id: str, report_id: str, update_mask: dict) -> Any:
        """
        Update  a report. This method supports patch semantics.

        :param profile_id: The DFA user profile ID.
        :param report_id: The ID of the report.
        :param update_mask: The relevant portions of a report resource,
            according to the rules of patch semantics.
        """
        response = (
            self.get_conn()
            .reports()
            .patch(profileId=profile_id, reportId=report_id, body=update_mask)
            .execute(num_retries=self.num_retries)
        )
        return response

    def run_report(self, profile_id: str, report_id: str, synchronous: bool | None = None) -> Any:
        """
        Run a report.

        :param profile_id: The DFA profile ID.
        :param report_id: The ID of the report.
        :param synchronous: If set and true, tries to run the report synchronously.
        """
        response = (
            self.get_conn()
            .reports()
            .run(profileId=profile_id, reportId=report_id, synchronous=synchronous)
            .execute(num_retries=self.num_retries)
        )
        return response

    def update_report(self, profile_id: str, report_id: str) -> Any:
        """
        Update a report.

        :param profile_id: The DFA user profile ID.
        :param report_id: The ID of the report.
        """
        response = (
            self.get_conn()
            .reports()
            .update(profileId=profile_id, reportId=report_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def get_report(self, file_id: str, profile_id: str, report_id: str) -> Any:
        """
        Retrieve a report file.

        :param profile_id: The DFA user profile ID.
        :param report_id: The ID of the report.
        :param file_id: The ID of the report file.
        """
        response = (
            self.get_conn()
            .reports()
            .files()
            .get(fileId=file_id, profileId=profile_id, reportId=report_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def get_report_file(self, file_id: str, profile_id: str, report_id: str) -> http.HttpRequest:
        """
        Retrieve a media part of report file.

        :param profile_id: The DFA user profile ID.
        :param report_id: The ID of the report.
        :param file_id: The ID of the report file.
        :return: googleapiclient.http.HttpRequest
        """
        request = (
            self.get_conn()
            .reports()
            .files()
            .get_media(fileId=file_id, profileId=profile_id, reportId=report_id)
        )
        return request

    @staticmethod
    def _conversions_batch_request(
        conversions: list[dict[str, Any]],
        encryption_entity_type: str,
        encryption_entity_id: int,
        encryption_source: str,
        kind: str,
    ) -> dict[str, Any]:
        return {
            "kind": kind,
            "conversions": conversions,
            "encryptionInfo": {
                "kind": "dfareporting#encryptionInfo",
                "encryptionEntityType": encryption_entity_type,
                "encryptionEntityId": encryption_entity_id,
                "encryptionSource": encryption_source,
            },
        }

    def conversions_batch_insert(
        self,
        profile_id: str,
        conversions: list[dict[str, Any]],
        encryption_entity_type: str,
        encryption_entity_id: int,
        encryption_source: str,
        max_failed_inserts: int = 0,
    ) -> Any:
        """
        Insert conversions.

        :param profile_id: User profile ID associated with this request.
        :param conversions: Conversations to insert, should by type of Conversation:
            https://developers.google.com/doubleclick-advertisers/rest/v4/conversions/batchinsert
        :param encryption_entity_type: The encryption entity type. This should match the encryption
            configuration for ad serving or Data Transfer.
        :param encryption_entity_id: The encryption entity ID. This should match the encryption
            configuration for ad serving or Data Transfer.
        :param encryption_source: Describes whether the encrypted cookie was received from ad serving
            (the %m macro) or from Data Transfer.
        :param max_failed_inserts: The maximum number of conversions that failed to be inserted
        """
        response = (
            self.get_conn()
            .conversions()
            .batchinsert(
                profileId=profile_id,
                body=self._conversions_batch_request(
                    conversions=conversions,
                    encryption_entity_type=encryption_entity_type,
                    encryption_entity_id=encryption_entity_id,
                    encryption_source=encryption_source,
                    kind="dfareporting#conversionsBatchInsertRequest",
                ),
            )
            .execute(num_retries=self.num_retries)
        )
        if response.get("hasFailures", False):
            errored_conversions = [stat["errors"] for stat in response["status"] if "errors" in stat]
            if len(errored_conversions) > max_failed_inserts:
                raise AirflowException(errored_conversions)
        return response

    def conversions_batch_update(
        self,
        profile_id: str,
        conversions: list[dict[str, Any]],
        encryption_entity_type: str,
        encryption_entity_id: int,
        encryption_source: str,
        max_failed_updates: int = 0,
    ) -> Any:
        """
        Update existing conversions.

        :param profile_id: User profile ID associated with this request.
        :param conversions: Conversations to update, should by type of Conversation:
            https://developers.google.com/doubleclick-advertisers/rest/v4/conversions/batchupdate
        :param encryption_entity_type: The encryption entity type. This should match the encryption
            configuration for ad serving or Data Transfer.
        :param encryption_entity_id: The encryption entity ID. This should match the encryption
            configuration for ad serving or Data Transfer.
        :param encryption_source: Describes whether the encrypted cookie was received from ad serving
            (the %m macro) or from Data Transfer.
        :param max_failed_updates: The maximum number of conversions that failed to be updated
        """
        response = (
            self.get_conn()
            .conversions()
            .batchupdate(
                profileId=profile_id,
                body=self._conversions_batch_request(
                    conversions=conversions,
                    encryption_entity_type=encryption_entity_type,
                    encryption_entity_id=encryption_entity_id,
                    encryption_source=encryption_source,
                    kind="dfareporting#conversionsBatchUpdateRequest",
                ),
            )
            .execute(num_retries=self.num_retries)
        )
        if response.get("hasFailures", False):
            errored_conversions = [stat["errors"] for stat in response["status"] if "errors" in stat]
            if len(errored_conversions) > max_failed_updates:
                raise AirflowException(errored_conversions)
        return response
