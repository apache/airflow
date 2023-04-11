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
"""This module contains Google Ad hook."""
from __future__ import annotations

from tempfile import NamedTemporaryFile
from typing import IO, Any

from google.auth.exceptions import GoogleAuthError

from airflow import AirflowException
from airflow.compat.functools import cached_property
from airflow.hooks.base import BaseHook
from airflow.providers.google.common.hooks.base_google import get_field
from airflow.providers.google_vendor.googleads.client import GoogleAdsClient
from airflow.providers.google_vendor.googleads.errors import GoogleAdsException
from airflow.providers.google_vendor.googleads.v12.services.services.customer_service import (
    CustomerServiceClient,
)
from airflow.providers.google_vendor.googleads.v12.services.services.google_ads_service import (
    GoogleAdsServiceClient,
)
from airflow.providers.google_vendor.googleads.v12.services.services.google_ads_service.pagers import (
    SearchPager,
)
from airflow.providers.google_vendor.googleads.v12.services.types.google_ads_service import (
    GoogleAdsRow,
)


class GoogleAdsHook(BaseHook):
    """
    Hook for the Google Ads API.

    This hook requires two connections:

        - gcp_conn_id - provides service account details (like any other GCP connection)
        - google_ads_conn_id - which contains information from Google Ads config.yaml file
          in the ``extras``. Example of the ``extras``:

        .. code-block:: json

            {
                "google_ads_client": {
                    "developer_token": "{{ INSERT_TOKEN }}",
                    "json_key_file_path": null,
                    "impersonated_email": "{{ INSERT_IMPERSONATED_EMAIL }}"
                }
            }

        The ``json_key_file_path`` is resolved by the hook using credentials from gcp_conn_id.
        https://developers.google.com/google-ads/api/docs/client-libs/python/oauth-service

    .. seealso::
        For more information on how Google Ads authentication flow works take a look at:
        https://developers.google.com/google-ads/api/docs/client-libs/python/oauth-service

    .. seealso::
        For more information on the Google Ads API, take a look at the API docs:
        https://developers.google.com/google-ads/api/docs/start

    :param gcp_conn_id: The connection ID with the service account details.
    :param google_ads_conn_id: The connection ID with the details of Google Ads config.yaml file.
    :param api_version: The Google Ads API version to use.

    :return: list of Google Ads Row object(s)
    """

    default_api_version = "v12"

    def __init__(
        self,
        api_version: str | None,
        gcp_conn_id: str = "google_cloud_default",
        google_ads_conn_id: str = "google_ads_default",
    ) -> None:
        super().__init__()
        self.api_version = api_version or self.default_api_version
        self.gcp_conn_id = gcp_conn_id
        self.google_ads_conn_id = google_ads_conn_id
        self.google_ads_config: dict[str, Any] = {}

    def search(
        self, client_ids: list[str], query: str, page_size: int = 10000, **kwargs
    ) -> list[GoogleAdsRow]:
        """
        Pulls data from the Google Ads API and returns it as native protobuf
        message instances (those seen in versions prior to 10.0.0 of the
        google-ads library).

        This method is for backwards compatibility with older versions of the
        google_ads_hook.

        Check out the search_proto_plus method to get API results in the new
        default format of the google-ads library since v10.0.0 that behave
        more like conventional python object (using proto-plus-python).

        :param client_ids: Google Ads client ID(s) to query the API for.
        :param query: Google Ads Query Language query.
        :param page_size: Number of results to return per page. Max 10000.
        :return: Google Ads API response, converted to Google Ads Row objects
        """
        data_proto_plus = self._search(client_ids, query, page_size, **kwargs)
        data_native_pb = [row._pb for row in data_proto_plus]

        return data_native_pb

    def search_proto_plus(
        self, client_ids: list[str], query: str, page_size: int = 10000, **kwargs
    ) -> list[GoogleAdsRow]:
        """
        Pulls data from the Google Ads API and returns it as proto-plus-python
        message instances that behave more like conventional python objects.

        :param client_ids: Google Ads client ID(s) to query the API for.
        :param query: Google Ads Query Language query.
        :param page_size: Number of results to return per page. Max 10000.
        :return: Google Ads API response, converted to Google Ads Row objects
        """
        return self._search(client_ids, query, page_size, **kwargs)

    def list_accessible_customers(self) -> list[str]:
        """
        Returns resource names of customers directly accessible by the user authenticating the call.
        The resulting list of customers is based on your OAuth credentials. The request returns a list
        of all accounts that you are able to act upon directly given your current credentials. This will
        not necessarily include all accounts within the account hierarchy; rather, it will only include
        accounts where your authenticated user has been added with admin or other rights in the account.

        ..seealso::
            https://developers.google.com/google-ads/api/reference/rpc

        :return: List of names of customers
        """
        try:
            accessible_customers = self._get_customer_service.list_accessible_customers()
            return accessible_customers.resource_names
        except GoogleAdsException as ex:
            for error in ex.failure.errors:
                self.log.error('\tError with message "%s".', error.message)
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        self.log.error("\t\tOn field: %s", field_path_element.field_name)
            raise

    @cached_property
    def _get_service(self) -> GoogleAdsServiceClient:
        """Connects and authenticates with the Google Ads API using a service account"""
        client = self._get_client
        return client.get_service("GoogleAdsService", version=self.api_version)

    @cached_property
    def _get_client(self) -> GoogleAdsClient:
        with NamedTemporaryFile("w", suffix=".json") as secrets_temp:
            self._get_config()
            self._update_config_with_secret(secrets_temp)
            try:
                client = GoogleAdsClient.load_from_dict(self.google_ads_config)
                return client
            except GoogleAuthError as e:
                self.log.error("Google Auth Error: %s", e)
                raise

    @cached_property
    def _get_customer_service(self) -> CustomerServiceClient:
        """Connects and authenticates with the Google Ads API using a service account"""
        with NamedTemporaryFile("w", suffix=".json") as secrets_temp:
            self._get_config()
            self._update_config_with_secret(secrets_temp)
            try:
                client = GoogleAdsClient.load_from_dict(self.google_ads_config)
                return client.get_service("CustomerService", version=self.api_version)
            except GoogleAuthError as e:
                self.log.error("Google Auth Error: %s", e)
                raise

    def _get_config(self) -> None:
        """
        Gets google ads connection from meta db and sets google_ads_config attribute with returned config
        file
        """
        conn = self.get_connection(self.google_ads_conn_id)
        if "google_ads_client" not in conn.extra_dejson:
            raise AirflowException("google_ads_client not found in extra field")

        self.google_ads_config = conn.extra_dejson["google_ads_client"]

    def _update_config_with_secret(self, secrets_temp: IO[str]) -> None:
        """
        Gets Google Cloud secret from connection and saves the contents to the temp file
        Updates google ads config with file path of the temp file containing the secret
        Note, the secret must be passed as a file path for Google Ads API
        """
        extras = self.get_connection(self.gcp_conn_id).extra_dejson
        secret = get_field(extras, "keyfile_dict")
        if not secret:
            raise KeyError("secret_conn.extra_dejson does not contain keyfile_dict")
        secrets_temp.write(secret)
        secrets_temp.flush()

        self.google_ads_config["json_key_file_path"] = secrets_temp.name

    def _search(
        self, client_ids: list[str], query: str, page_size: int = 10000, **kwargs
    ) -> list[GoogleAdsRow]:
        """
        Pulls data from the Google Ads API

        :param client_ids: Google Ads client ID(s) to query the API for.
        :param query: Google Ads Query Language query.
        :param page_size: Number of results to return per page. Max 10000.

        :return: Google Ads API response, converted to Google Ads Row objects
        """
        service = self._get_service

        iterators = []
        for client_id in client_ids:
            iterator = service.search(request=dict(customer_id=client_id, query=query, page_size=page_size))
            iterators.append(iterator)

        self.log.info("Fetched Google Ads Iterators")

        return self._extract_rows(iterators)

    def _extract_rows(self, iterators: list[SearchPager]) -> list[GoogleAdsRow]:
        """
        Convert Google Page Iterator (GRPCIterator) objects to Google Ads Rows

        :param iterators: List of Google Page Iterator (GRPCIterator) objects

        :return: API response for all clients in the form of Google Ads Row object(s)
        """
        try:
            self.log.info("Extracting data from returned Google Ads Iterators")
            return [row for iterator in iterators for row in iterator]
        except GoogleAdsException as e:
            self.log.error(
                "Request ID %s failed with status %s and includes the following errors:",
                e.request_id,
                e.error.code().name,
            )
            for error in e.failure.errors:
                self.log.error("\tError with message: %s.", error.message)
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        self.log.error("\t\tOn field: %s", field_path_element.field_name)
            raise
