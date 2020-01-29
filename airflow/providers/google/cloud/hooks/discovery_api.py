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
#
"""
This module allows you to connect to the Google Discovery API Service and query it.
"""
from typing import Dict, Optional

from googleapiclient.discovery import Resource, build

from airflow.gcp.hooks.base import CloudBaseHook


class GoogleDiscoveryApiHook(CloudBaseHook):
    """
    A hook to use the Google API Discovery Service.

    :param api_service_name: The name of the api service that is needed to get the data
        for example 'youtube'.
    :type api_service_name: str
    :param api_version: The version of the api that will be requested for example 'v3'.
    :type api_version: str
    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    """
    _conn = None  # type: Optional[Resource]

    def __init__(
        self,
        api_service_name: str,
        api_version: str,
        gcp_conn_id='google_cloud_default',
        delegate_to: Optional[str] = None
    ) -> None:
        super().__init__(gcp_conn_id=gcp_conn_id, delegate_to=delegate_to)
        self.api_service_name = api_service_name
        self.api_version = api_version

    def get_conn(self):
        """
        Creates an authenticated api client for the given api service name and credentials.

        :return: the authenticated api service.
        :rtype: Resource
        """
        self.log.info("Authenticating Google API Client")

        if not self._conn:
            http_authorized = self._authorize()
            self._conn = build(
                serviceName=self.api_service_name,
                version=self.api_version,
                http=http_authorized,
                cache_discovery=False
            )
        return self._conn

    def query(self, endpoint: str, data: Dict, paginate: bool = False, num_retries: int = 0) -> Dict:
        """
        Creates a dynamic API call to any Google API registered in Google's API Client Library
        and queries it.

        :param endpoint: The client libraries path to the api call's executing method.
            For example: 'analyticsreporting.reports.batchGet'

            .. seealso:: https://developers.google.com/apis-explorer
                for more information on what methods are available.
        :type endpoint: str
        :param data: The data (endpoint params) needed for the specific request to given endpoint.
        :type data: dict
        :param paginate: If set to True, it will collect all pages of data.
        :type paginate: bool
        :param num_retries: Define the number of retries for the requests being made if it fails.
        :type num_retries: int
        :return: the API response from the passed endpoint.
        :rtype: dict
        """
        google_api_conn_client = self.get_conn()

        api_response = self._call_api_request(google_api_conn_client, endpoint, data, paginate, num_retries)
        return api_response

    def _call_api_request(self, google_api_conn_client, endpoint, data, paginate, num_retries):
        api_endpoint_parts = endpoint.split('.')

        google_api_endpoint_instance = self._build_api_request(
            google_api_conn_client,
            api_sub_functions=api_endpoint_parts[1:],
            api_endpoint_params=data
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
                google_api_conn_client = getattr(google_api_conn_client, sub_function + '_next')
                google_api_conn_client = google_api_conn_client(api_endpoint_instance, api_response)

        return google_api_conn_client
