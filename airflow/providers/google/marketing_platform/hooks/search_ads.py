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
"""This module contains Google Search Ads 360 hook."""
from typing import Any, Dict, Optional, Sequence, Union

from googleapiclient.discovery import build

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleSearchAdsHook(GoogleBaseHook):
    """Hook for Google Search Ads 360."""

    _conn = None  # type: Optional[Any]

    def __init__(
        self,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.api_version = api_version

    def get_conn(self):
        """Retrieves connection to Google SearchAds."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "doubleclicksearch",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def insert_report(self, report: Dict[str, Any]) -> Any:
        """
        Inserts a report request into the reporting system.

        :param report: Report to be generated.
        :type report: Dict[str, Any]
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .request(body=report)
            .execute(num_retries=self.num_retries)
        )
        return response

    def get(self, report_id: str) -> Any:
        """
        Polls for the status of a report request.

        :param report_id: ID of the report request being polled.
        :type report_id: str
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .get(reportId=report_id)
            .execute(num_retries=self.num_retries)
        )
        return response

    def get_file(self, report_fragment: int, report_id: str) -> Any:
        """
        Downloads a report file encoded in UTF-8.

        :param report_fragment: The index of the report fragment to download.
        :type report_fragment: int
        :param report_id: ID of the report.
        :type report_id: str
        """
        response = (
            self.get_conn()  # pylint: disable=no-member
            .reports()
            .getFile(reportFragment=report_fragment, reportId=report_id)
            .execute(num_retries=self.num_retries)
        )
        return response
