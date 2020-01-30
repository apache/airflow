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
This module contains Google Campaign Manager hook.
"""
from typing import Any, Dict, List, Optional

from googleapiclient import http
from googleapiclient.discovery import Resource, build

from airflow.providers.google.cloud.hooks.base import CloudBaseHook


class GoogleCampaignManagerHook(CloudBaseHook):
    """
    Hook for Google Campaign Manager.
    """

    _conn = None  # type: Optional[Resource]

    def __init__(
        self,
        api_version: str = "v3.3",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
    ) -> None:
        super().__init__(gcp_conn_id, delegate_to)
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """
        Retrieves connection to Campaign Manager.
        """
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
        Deletes a report by its ID.

        :param profile_id: The DFA user profile ID.
        :type profile_id: str
        :param report_id: The ID of the report.
        :type report_id: str
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .delete(profileId=profile_id, reportId=report_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def insert_report(self, profile_id: str, report: Dict[str, Any]) -> Any:
        """
        Creates a report.

        :param profile_id: The DFA user profile ID.
        :type profile_id: str
        :param report: The report resource to be inserted.
        :type report: Dict[str, Any]
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .insert(profileId=profile_id, body=report)
            .execute(num_retries=self.num_retries)
        )
        return response

    def list_reports(
        self,
        profile_id: str,
        max_results: Optional[int] = None,
        scope: Optional[str] = None,
        sort_field: Optional[str] = None,
        sort_order: Optional[str] = None,
    ) -> List[Dict]:
        """
        Retrieves list of reports.

        :param profile_id: The DFA user profile ID.
        :type profile_id: str
        :param max_results: Maximum number of results to return.
        :type max_results: Optional[int]
        :param scope: The scope that defines which results are returned.
        :type scope: Optional[str]
        :param sort_field: The field by which to sort the list.
        :type sort_field: Optional[str]
        :param sort_order: Order of sorted results.
        :type sort_order: Optional[str]
        """
        reports = []  # type: List[Dict]
        conn = self.get_conn()
        request = conn.reports().list(  # pylint: disable=no-member
            profileId=profile_id,
            maxResults=max_results,
            scope=scope,
            sortField=sort_field,
            sortOrder=sort_order,
        )
        while request is not None:
            response = request.execute(num_retries=self.num_retries)
            reports.extend(response.get("items", []))
            request = conn.reports().list_next(  # pylint: disable=no-member
                previous_request=request, previous_response=response
            )

        return reports

    def patch_report(self, profile_id: str, report_id: str, update_mask: Dict) -> Any:
        """
        Updates a report. This method supports patch semantics.

        :param profile_id: The DFA user profile ID.
        :type profile_id: str
        :param report_id: The ID of the report.
        :type report_id: str
        :param update_mask: The relevant portions of a report resource,
            according to the rules of patch semantics.
        :type update_mask: Dict
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .patch(profileId=profile_id, reportId=report_id, body=update_mask)
            .execute(num_retries=self.num_retries)
        )
        return response

    def run_report(
        self, profile_id: str, report_id: str, synchronous: Optional[bool] = None
    ) -> Any:
        """
        Runs a report.

        :param profile_id: The DFA profile ID.
        :type profile_id: str
        :param report_id: The ID of the report.
        :type report_id: str
        :param synchronous: If set and true, tries to run the report synchronously.
        :type synchronous: Optional[bool]
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .run(profileId=profile_id, reportId=report_id, synchronous=synchronous)
            .execute(num_retries=self.num_retries)
        )
        return response

    def update_report(self, profile_id: str, report_id: str) -> Any:
        """
        Updates a report.

        :param profile_id: The DFA user profile ID.
        :type profile_id: str
        :param report_id: The ID of the report.
        :type report_id: str
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .update(profileId=profile_id, reportId=report_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def get_report(self, file_id: str, profile_id: str, report_id: str) -> Any:
        """
        Retrieves a report file.

        :param profile_id: The DFA user profile ID.
        :type profile_id: str
        :param report_id: The ID of the report.
        :type report_id: str
        :param file_id: The ID of the report file.
        :type file_id: str
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .files()
            .get(fileId=file_id, profileId=profile_id, reportId=report_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def get_report_file(
        self, file_id: str, profile_id: str, report_id: str
    ) -> http.HttpRequest:
        """
        Retrieves a media part of report file.

        :param profile_id: The DFA user profile ID.
        :type profile_id: str
        :param report_id: The ID of the report.
        :type report_id: str
        :param file_id: The ID of the report file.
        :type file_id: str
        :return: googleapiclient.http.HttpRequest
        """
        request = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .files()
            .get_media(fileId=file_id, profileId=profile_id, reportId=report_id)
        )
        return request
