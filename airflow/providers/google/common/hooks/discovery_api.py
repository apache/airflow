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
"""This module allows you to connect to the Google Discovery API Service and query it."""
from __future__ import annotations

from typing import Sequence

from googleapiclient.discovery import Resource, build

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GoogleDiscoveryApiHook(GoogleBaseHook):
    """
    A hook to use the Google API Discovery Service.

    :param api_service_name: The name of the api service that is needed to get the data
        for example 'youtube'.
    :param api_version: The version of the api that will be requested for example 'v3'.
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    _conn: Resource | None = None

    def __init__(
        self,
        api_service_name: str,
        api_version: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            impersonation_chain=impersonation_chain,
        )
        self.api_service_name = api_service_name
        self.api_version = api_version

    def get_conn(self) -> Resource:
        """
        Creates an authenticated api client for the given api service name and credentials.

        :return: the authenticated api service.
        """
        self.log.info("Authenticating Google API Client")

        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                serviceName=self.api_service_name,
                version=self.api_version,
                http=http_authorized,
                cache_discovery=False,
            )
        return self._conn

    def query(self, endpoint: str, data: dict, paginate: bool = False, num_retries: int = 0) -> dict:
        """
        Creates a dynamic API call to any Google API registered in Google's API Client Library
        and queries it.

        :param endpoint: The client libraries path to the api call's executing method.
            For example: 'analyticsreporting.reports.batchGet'

            .. seealso:: https://developers.google.com/apis-explorer
                for more information on what methods are available.
        :param data: The data (endpoint params) needed for the specific request to given endpoint.
        :param paginate: If set to True, it will collect all pages of data.
        :param num_retries: Define the number of retries for the requests being made if it fails.
        :return: the API response from the passed endpoint.
        """
        google_api_conn_client = self.get_conn()

        api_response = self._call_api_request(google_api_conn_client, endpoint, data, paginate, num_retries)
        return api_response

    def _call_api_request(self, google_api_conn_client, endpoint, data, paginate, num_retries):
        api_endpoint_parts = endpoint.split(".")

        google_api_endpoint_instance = self._build_api_request(
            google_api_conn_client, api_sub_functions=api_endpoint_parts[1:], api_endpoint_params=data
        )

        if paginate:
            return self._paginate_api(
                google_api_endpoint_instance, google_api_conn_client, api_endpoint_parts, num_retries
            )

        return google_api_endpoint_instance.execute(num_retries=num_retries)

    def _build_api_request(self, google_api_conn_client, api_sub_functions, api_endpoint_params):
        for sub_function in api_sub_functions:
            google_api_conn_client = getattr(google_api_conn_client, sub_function)
            if sub_function != api_sub_functions[-1]:
                google_api_conn_client = google_api_conn_client()
            else:
                google_api_conn_client = google_api_conn_client(**api_endpoint_params)

        return google_api_conn_client

    def _paginate_api(
        self, google_api_endpoint_instance, google_api_conn_client, api_endpoint_parts, num_retries
    ):
        api_responses = []

        while google_api_endpoint_instance:
            api_response = google_api_endpoint_instance.execute(num_retries=num_retries)
            api_responses.append(api_response)

            google_api_endpoint_instance = self._build_next_api_request(
                google_api_conn_client, api_endpoint_parts[1:], google_api_endpoint_instance, api_response
            )

        return api_responses

    def _build_next_api_request(
        self, google_api_conn_client, api_sub_functions, api_endpoint_instance, api_response
    ):
        for sub_function in api_sub_functions:
            if sub_function != api_sub_functions[-1]:
                google_api_conn_client = getattr(google_api_conn_client, sub_function)
                google_api_conn_client = google_api_conn_client()
            else:
                google_api_conn_client = getattr(google_api_conn_client, sub_function + "_next")
                google_api_conn_client = google_api_conn_client(api_endpoint_instance, api_response)

        return google_api_conn_client
