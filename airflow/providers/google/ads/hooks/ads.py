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

from functools import cached_property
from tempfile import NamedTemporaryFile
from typing import IO, TYPE_CHECKING, Any, Literal

from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.auth.exceptions import GoogleAuthError

from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.providers.google.common.hooks.base_google import get_field

if TYPE_CHECKING:
    from google.ads.googleads.v17.services.services.customer_service import CustomerServiceClient
    from google.ads.googleads.v17.services.services.google_ads_service import GoogleAdsServiceClient
    from google.ads.googleads.v17.services.types.google_ads_service import GoogleAdsRow
    from google.api_core.page_iterator import GRPCIterator


class GoogleAdsHook(BaseHook):
    """
    Interact with Google Ads API.

    This hook offers two flows of authentication.

    1. OAuth Service Account Flow (requires two connections)

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

    2. Developer token from API center flow (only requires google_ads_conn_id)

        - google_ads_conn_id - which contains developer token, refresh token, client_id and client_secret
            in the ``extras``. Example of the ``extras``:

            .. code-block:: json

                {
                    "google_ads_client": {
                        "developer_token": "{{ INSERT_DEVELOPER_TOKEN }}",
                        "refresh_token": "{{ INSERT_REFRESH_TOKEN }}",
                        "client_id": "{{ INSERT_CLIENT_ID }}",
                        "client_secret": "{{ INSERT_CLIENT_SECRET }}",
                        "use_proto_plus": "{{ True or False }}",
                    }
                }

        .. seealso::
            For more information on how to obtain a developer token look at:
            https://developers.google.com/google-ads/api/docs/get-started/dev-token

        .. seealso::
            For more information about use_proto_plus option see the Protobuf Messages guide:
            https://developers.google.com/google-ads/api/docs/client-libs/python/protobuf-messages

    :param gcp_conn_id: The connection ID with the service account details.
    :param google_ads_conn_id: The connection ID with the details of Google Ads config.yaml file.
    :param api_version: The Google Ads API version to use.
    """

    default_api_version = "v17"

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
        self.authentication_method: Literal["service_account", "developer_token"] = "service_account"

    def search(
        self, client_ids: list[str], query: str, page_size: int = 10000, **kwargs
    ) -> list[GoogleAdsRow]:
        """
        Pull data from the Google Ads API.

        Native protobuf message instances are returned (those seen in versions
        prior to 10.0.0 of the google-ads library).

        This method is for backwards compatibility with older versions of the
        google_ads_hook.

        Check out the search_proto_plus method to get API results in the new
        default format of the google-ads library since v10.0.0 that behave
        more like conventional python object (using proto-plus-python).

        :param client_ids: Google Ads client ID(s) to query the API for.
        :param query: Google Ads Query Language query.
        :param page_size: Number of results to return per page. Max 10000.
        :return: Google Ads API response, converted to Google Ads Row objects.
        """
        data_proto_plus = self._search(client_ids, query, page_size, **kwargs)
        data_native_pb = [row._pb for row in data_proto_plus]

        return data_native_pb

    def search_proto_plus(
        self, client_ids: list[str], query: str, page_size: int = 10000, **kwargs
    ) -> list[GoogleAdsRow]:
        """
        Pull data from the Google Ads API.

        Instances of proto-plus-python message are returned, which behave more
        like conventional Python objects.

        :param client_ids: Google Ads client ID(s) to query the API for.
        :param query: Google Ads Query Language query.
        :param page_size: Number of results to return per page. Max 10000.
        :return: Google Ads API response, converted to Google Ads Row objects
        """
        return self._search(client_ids, query, page_size, **kwargs)

    def list_accessible_customers(self) -> list[str]:
        """
        List resource names of customers.

        The resulting list of customers is based on your OAuth credentials. The
        request returns a list of all accounts that you are able to act upon
        directly given your current credentials. This will not necessarily
        include all accounts within the account hierarchy; rather, it will only
        include accounts where your authenticated user has been added with admin
        or other rights in the account.

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
        """Connect and authenticate with the Google Ads API using a service account."""
        client = self._get_client
        return client.get_service("GoogleAdsService", version=self.api_version)

    @cached_property
    def _get_client(self) -> GoogleAdsClient:
        with NamedTemporaryFile("w", suffix=".json") as secrets_temp:
            self._get_config()
            self._determine_authentication_method()
            self._update_config_with_secret(
                secrets_temp
            ) if self.authentication_method == "service_account" else None
            try:
                client = GoogleAdsClient.load_from_dict(self.google_ads_config)
                return client
            except GoogleAuthError as e:
                self.log.error("Google Auth Error: %s", e)
                raise

    @cached_property
    def _get_customer_service(self) -> CustomerServiceClient:
        """Connect and authenticate with the Google Ads API using a service account."""
        with NamedTemporaryFile("w", suffix=".json") as secrets_temp:
            self._get_config()
            self._determine_authentication_method()
            if self.authentication_method == "service_account":
                self._update_config_with_secret(secrets_temp)
            try:
                client = GoogleAdsClient.load_from_dict(self.google_ads_config)
                return client.get_service("CustomerService", version=self.api_version)
            except GoogleAuthError as e:
                self.log.error("Google Auth Error: %s", e)
                raise

    def _get_config(self) -> None:
        """
        Set up Google Ads config from Connection.

        This pulls the connections from db, and uses it to set up
        ``google_ads_config``.
        """
        conn = self.get_connection(self.google_ads_conn_id)
        if "google_ads_client" not in conn.extra_dejson:
            raise AirflowException("google_ads_client not found in extra field")

        self.google_ads_config = conn.extra_dejson["google_ads_client"]

    def _determine_authentication_method(self) -> None:
        """Determine authentication method based on google_ads_config."""
        if self.google_ads_config.get("json_key_file_path") and self.google_ads_config.get(
            "impersonated_email"
        ):
            self.authentication_method = "service_account"
        elif (
            self.google_ads_config.get("refresh_token")
            and self.google_ads_config.get("client_id")
            and self.google_ads_config.get("client_secret")
            and self.google_ads_config.get("use_proto_plus")
        ):
            self.authentication_method = "developer_token"
        else:
            raise AirflowException("Authentication method could not be determined")

    def _update_config_with_secret(self, secrets_temp: IO[str]) -> None:
        """
        Set up Google Cloud config secret from Connection.

        This pulls the connection, saves the contents to a temp file, and point
        the config to the path containing the secret. Note that the secret must
        be passed as a file path for Google Ads API.
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
        Pull data from the Google Ads API.

        :param client_ids: Google Ads client ID(s) to query the API for.
        :param query: Google Ads Query Language query.
        :param page_size: Number of results to return per page. Max 10000.

        :return: Google Ads API response, converted to Google Ads Row objects
        """
        service = self._get_service

        iterators = []
        for client_id in client_ids:
            iterator = service.search(
                request={"customer_id": client_id, "query": query, "page_size": page_size}
            )
            iterators.append(iterator)

        self.log.info("Fetched Google Ads Iterators")

        return self._extract_rows(iterators)

    def _extract_rows(self, iterators: list[GRPCIterator]) -> list[GoogleAdsRow]:
        """
        Convert Google Page Iterator (GRPCIterator) objects to Google Ads Rows.

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
