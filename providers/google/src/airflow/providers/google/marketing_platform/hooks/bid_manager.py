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
"""This module contains Google Bid Manager API hook."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from googleapiclient.discovery import Resource, build

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleBidManagerHook(GoogleBaseHook):
    """Hook for Google Bid Manager API."""

    _conn: Resource | None = None

    def __init__(
        self,
        api_version: str = "v2",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
            **kwargs,
        )
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """Retrieve connection to Bid Manager API."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "doubleclickbidmanager",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
                client_options=self.get_client_options(),
            )
        return self._conn

    def create_query(self, query: dict[str, Any]) -> dict:
        """
        Create a query.

        :param query: Query object to be passed to request body.
        """
        response = self.get_conn().queries().create(body=query).execute(num_retries=self.num_retries)
        return response

    def delete_query(self, query_id: str) -> None:
        """
        Delete a stored query as well as the associated stored reports.

        :param query_id: Query ID to delete.
        """
        self.get_conn().queries().delete(queryId=query_id).execute(num_retries=self.num_retries)

    def get_query(self, query_id: str) -> dict:
        """
        Retrieve a stored query.

        :param query_id: Query ID to retrieve.
        """
        response = self.get_conn().queries().get(queryId=query_id).execute(num_retries=self.num_retries)
        return response

    def list_queries(self) -> list[dict]:
        """Retrieve stored queries."""
        response = self.get_conn().queries().list().execute(num_retries=self.num_retries)
        return response.get("queries", [])

    def run_query(self, query_id: str, params: dict[str, Any] | None) -> dict:
        """
        Run a stored query to generate a report.

        :param query_id: Query ID to run.
        :param params: Parameters for the report.
        """
        return (
            self.get_conn().queries().run(queryId=query_id, body=params).execute(num_retries=self.num_retries)
        )

    def get_report(self, query_id: str, report_id: str) -> dict:
        """
        Retrieve a report.

        :param query_id: Query ID for which report was generated.
        :param report_id: Report ID to retrieve.
        """
        return (
            self.get_conn()
            .queries()
            .reports()
            .get(queryId=query_id, reportId=report_id)
            .execute(num_retries=self.num_retries)
        )

    def list_reports(self, query_id: str) -> dict:
        """
        Retrieve a list of reports.

        :param query_id: Query ID for which report was generated.
        """
        return (
            self.get_conn().queries().reports().list(queryId=query_id).execute(num_retries=self.num_retries)
        )
