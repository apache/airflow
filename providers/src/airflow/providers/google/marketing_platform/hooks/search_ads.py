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

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any, Sequence

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from airflow.exceptions import AirflowException
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

if TYPE_CHECKING:
    from googleapiclient.discovery import Resource


class GoogleSearchAdsReportingHook(GoogleBaseHook):
    """Hook for the Google Search Ads 360 Reporting API."""

    _conn: build | None = None
    default_api_version: str = "v0"

    def __init__(
        self,
        api_version: str | None = None,
        gcp_conn_id: str = "google_search_ads_default",
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
        )
        self.api_version = api_version or self.default_api_version

    def _get_config(self) -> None:
        """
        Set up Google Search Ads config from Connection.

        This pulls the connections from db, and uses it to set up
        ``google_search_ads_client``.
        """
        conn = self.get_connection(self.gcp_conn_id)
        if "google_search_ads_client" not in conn.extra_dejson:
            raise AirflowException("google_search_ads_client not found in extra field")

        self.google_search_ads_config = conn.extra_dejson["google_search_ads_client"]

    def get_credentials(self) -> Credentials:
        """Return the credential instance for search ads."""
        self._get_config()
        self.logger().info(f"Credential configuration: {self.google_search_ads_config}")
        return Credentials(**self.google_search_ads_config)

    def get_conn(self) -> Resource:
        if not self._conn:
            creds = self.get_credentials()

            self._conn = build(
                "searchads360",
                self.api_version,
                credentials=creds,
                cache_discovery=False,
            )
        return self._conn

    @cached_property
    def customer_service(self):
        return self.get_conn().customers()

    @cached_property
    def fields_service(self):
        return self.get_conn().searchAds360Fields()

    def search(
        self,
        customer_id: str,
        query: str,
        page_token: str | None = None,
        page_size: int = 10000,
        return_total_results_count: bool = False,
        summary_row_setting: str | None = None,
        validate_only: bool = False,
    ):
        """
        Search and download the report. Use pagination to download entire report.

        :param customer_id: The ID of the customer being queried.
        :param query: The query to execute.
        :param page_token: Token of the page to retrieve. If not specified, the first page of results will be
            returned. Use the value obtained from `next_page_token` in the previous response
            in order to request the next page of results.
        :param page_size: Number of elements to retrieve in a single page. When too large a page is requested,
            the server may decide to further limit the number of returned resources.
            Default is 10000.
        :param return_total_results_count: If true, the total number of results that match the query ignoring
            the LIMIT clause will be included in the response. Default is false.
        :param summary_row_setting: Determines whether a summary row will be returned. By default,
            summary row is not returned. If requested, the summary row will be sent
            in a response by itself after all others query results are returned.
        :param validate_only: If true, the request is validated but not executed. Default is false.
        """
        params: dict[str, Any] = {
            "query": query,
            "pageSize": page_size,
            "returnTotalResultsCount": return_total_results_count,
            "validateOnly": validate_only,
        }
        if page_token is not None:
            params.update({"pageToken": page_token})
        if summary_row_setting is not None:
            params.update({"summaryRowSetting": summary_row_setting})

        response = (
            self.customer_service.searchAds360()
            .search(customerId=customer_id, body=params)
            .execute(num_retries=self.num_retries)
        )
        self.log.info("Search response: %s", response)
        return response

    def get_custom_column(self, customer_id: str, custom_column_id: str):
        """
        Retrieve the requested custom column in full detail.

        :param customer_id: The customer id
        :param custom_column_id: The custom column id
        """
        resource_name = f"customers/{customer_id}/customColumns/{custom_column_id}"
        response = (
            self.customer_service.customColumns()
            .get(resourceName=resource_name)
            .execute(num_retries=self.num_retries)
        )
        self.log.info("Retrieved custom column: %s", response)
        return response

    def list_custom_columns(self, customer_id: str):
        """
        Retrieve all the custom columns associated with the customer in full detail.

        :param customer_id: The customer id
        """
        response = (
            self.customer_service.customColumns()
            .list(customerId=customer_id)
            .execute(num_retries=self.num_retries)
        )
        self.log.info("Listing the custom columns: %s", response)
        return response

    def get_field(self, field_name: str):
        """
        Retrieve the requested field details.

        :param field_name: The name of the field.
        """
        resource_name = f"searchAds360Fields/{field_name}"
        response = self.fields_service.get(resourceName=resource_name).execute(num_retries=self.num_retries)
        self.log.info("Retrieved field: %s", response)
        return response

    def search_fields(self, query: str, page_token: str | None = None, page_size: int | None = 10000):
        """
        Retrieve all the fields that match with the given search.

        :param query: The query string to execute.
        :param page_token: Token of the page to retrieve. If not specified, the first page of results will be
            returned. Use the value obtained from `next_page_token` in the previous response
            in order to request the next page of results.
        :param page_size: Number of elements to retrieve in a single page. When too large a page is requested,
            the server may decide to further limit the number of returned resources.
            Default 10000.
        """
        params: dict[str, Any] = {
            "query": query,
            "pageSize": page_size,
        }
        if page_token is not None:
            params.update({"pageToken": page_token})
        response = self.fields_service.search(body=params).execute(num_retries=self.num_retries)
        self.log.info("Retrieved fields: %s", response)
        return response


class GoogleSearchAdsHook(GoogleBaseHook):
    """Hook for Google Search Ads 360."""

    _conn: build | None = None

    def __init__(
        self,
        api_version: str = "v2",
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

    def get_conn(self):
        """Retrieve connection to Google SearchAds."""
        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                "doubleclicksearch",
                self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn
