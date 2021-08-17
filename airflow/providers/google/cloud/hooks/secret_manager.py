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
"""Hook for Secrets Manager service"""
from typing import Optional, Sequence, Union

from airflow.providers.google.cloud._internal_client.secret_manager_client import _SecretManagerClient
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class SecretsManagerHook(GoogleBaseHook):
    """
    Hook for the Google Secret Manager API.

    See https://cloud.google.com/secret-manager

    All the methods in the hook where project_id is used must be called with
    keyword arguments rather than positional.

    :param gcp_conn_id: The connection ID to use when fetching connection info.
    :type gcp_conn_id: str
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    :type impersonation_chain: Union[str, Sequence[str]]
    """

    def __init__(
        self,
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
    ) -> None:
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.client = _SecretManagerClient(credentials=self._get_credentials())

    def get_conn(self) -> _SecretManagerClient:
        """
        Retrieves the connection to Secret Manager.

        :return: Secret Manager client.
        :rtype: airflow.providers.google.cloud._internal_client.secret_manager_client._SecretManagerClient
        """
        return self.client

    @GoogleBaseHook.fallback_to_default_project_id
    def get_secret(
        self, secret_id: str, secret_version: str = 'latest', project_id: Optional[str] = None
    ) -> Optional[str]:
        """
        Get secret value from the Secret Manager.

        :param secret_id: Secret Key
        :type secret_id: str
        :param secret_version: version of the secret (default is 'latest')
        :type secret_version: str
        :param project_id: Project id (if you want to override the project_id from credentials)
        :type project_id: str
        """
        return self.get_conn().get_secret(
            secret_id=secret_id, secret_version=secret_version, project_id=project_id  # type: ignore
        )
