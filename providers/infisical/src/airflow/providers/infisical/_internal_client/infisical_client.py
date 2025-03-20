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
from __future__ import annotations

from functools import cached_property

from infisical_sdk import InfisicalSDKClient

from airflow.exceptions import AirflowException
from airflow.utils.log.logging_mixin import LoggingMixin

VALID_AUTH_TYPES: list[str] = [
    "universal-auth",
]


class _InfisicalClient(LoggingMixin):
    """
    Retrieves Authenticated client from Infisical.

    This class is used only for internal usage. It's main purpose is authentication and secret interactions.

    :param url: Base URL for the Infisical instance being addressed.
    :param auth_type: Authentication Type for Infisical. Default is ``universal-auth``. Available values are in
    :param universal_auth_client_id: Universal Auth Client ID for Infisical.
    :param universal_auth_client_secret: Universal Auth Client Secret for Infisical.
    """

    def __init__(
        self,
        url: str | None = None,
        auth_type: str = "universal-auth",
        universal_auth_client_id: str | None = None,
        universal_auth_client_secret: str | None = None,
        **kwargs,
    ):
        super().__init__()
        if auth_type not in VALID_AUTH_TYPES:
            raise AirflowException(
                f"The auth_type is not supported: {auth_type}. It should be one of {VALID_AUTH_TYPES}"
            )
        if (
            auth_type == "universal_auth"
            and not universal_auth_client_id
            and not universal_auth_client_secret
        ):
            raise AirflowException(
                "The 'universal_auth' authentication type requires 'universal_auth_client_id' and 'universal_auth_client_secret'"
            )

        self.url = url
        self.auth_type = auth_type
        self.kwargs = kwargs
        self.universal_auth_client_id = universal_auth_client_id
        self.universal_auth_client_secret = universal_auth_client_secret

    @property
    def client(self):
        """
        Checks that it is still authenticated to Infisical and invalidates the cache if this is not the case.

        :return: Infisical SDK Client
        """
        # TODO: Add authentication check and if not authenticated, re-authenticate.
        # if not self._client.is_authenticated():

        return self._client

    @cached_property
    def _client(self) -> InfisicalSDKClient:
        """
        Return an authenticated Infisical client.

        :return: Infisical Client

        """
        _client = InfisicalSDKClient(host=self.url)
        if self.auth_type == "universal-auth":
            self._auth_universal_auth(_client)
        else:
            raise AirflowException(f"Authentication type '{self.auth_type}' not supported")

        return _client

    def _auth_universal_auth(self, _client: InfisicalSDKClient) -> None:
        _client.auth.universal_auth.login(
            client_id=self.universal_auth_client_id, client_secret=self.universal_auth_client_secret
        )

    def get_secret_value(
        self, secret_path: str, project_id: str, environment_slug: str, secret_name: str
    ) -> str | None:
        try:
            secret = self.client.secrets.get_secret_by_name(
                secret_name=secret_name,
                environment_slug=environment_slug,
                project_id=project_id,
                secret_path=secret_path,
                expand_secret_references=True,
                view_secret_value=True,
            )
            return secret.secretValue
        except Exception:
            return None
