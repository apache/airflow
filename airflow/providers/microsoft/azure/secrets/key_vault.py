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

import logging
import os
import re
import warnings
from functools import cached_property

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.version import version as airflow_version


def _parse_version(val):
    match = re.search(r"(\d+)\.(\d+)\.(\d+)", val)
    return tuple(int(x) for x in match.groups())


class AzureKeyVaultBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Airflow Connections or Variables from Azure Key Vault secrets.

    The Azure Key Vault can be configured as a secrets backend in the ``airflow.cfg``:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.microsoft.azure.secrets.key_vault.AzureKeyVaultBackend
        backend_kwargs = {"connections_prefix": "airflow-connections", "vault_url": "<azure_key_vault_uri>"}

    For example, if the secrets prefix is ``airflow-connections-smtp-default``, this would be accessible
    if you provide ``{"connections_prefix": "airflow-connections"}`` and request conn_id ``smtp-default``.
    And if variables prefix is ``airflow-variables-hello``, this would be accessible
    if you provide ``{"variables_prefix": "airflow-variables"}`` and request variable key ``hello``.

    For client authentication, the ``DefaultAzureCredential`` from the Azure Python SDK is used as
    credential provider, which supports service principal, managed identity and user credentials

    For example, to specify a service principal with secret you can set the environment variables
    ``AZURE_TENANT_ID``, ``AZURE_CLIENT_ID`` and ``AZURE_CLIENT_SECRET``.

    .. seealso::
        For more details on client authentication refer to the ``DefaultAzureCredential`` Class reference:
        https://docs.microsoft.com/en-us/python/api/azure-identity/azure.identity.defaultazurecredential?view=azure-python

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections
        If set to None (null), requests for connections will not be sent to Azure Key Vault
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables
        If set to None (null), requests for variables will not be sent to Azure Key Vault
    :param config_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null), requests for configurations will not be sent to Azure Key Vault
    :param vault_url: The URL of an Azure Key Vault to use
    :param sep: separator used to concatenate secret_prefix and secret_id. Default: "-"
    :param tenant_id: The tenant id of an Azure Key Vault to use.
        If not given, it falls back to ``DefaultAzureCredential``
    :param client_id: The client id of an Azure Key Vault to use.
        If not given, it falls back to ``DefaultAzureCredential``
    :param client_secret: The client secret of an Azure Key Vault to use.
        If not given, it falls back to ``DefaultAzureCredential``
    """

    def __init__(
        self,
        connections_prefix: str = "airflow-connections",
        variables_prefix: str = "airflow-variables",
        config_prefix: str = "airflow-config",
        vault_url: str = "",
        sep: str = "-",
        *,
        tenant_id: str = "",
        client_id: str = "",
        client_secret: str = "",
        **kwargs,
    ) -> None:
        super().__init__()
        self.vault_url = vault_url
        if connections_prefix is not None:
            self.connections_prefix = connections_prefix.rstrip(sep)
        else:
            self.connections_prefix = connections_prefix
        if variables_prefix is not None:
            self.variables_prefix = variables_prefix.rstrip(sep)
        else:
            self.variables_prefix = variables_prefix
        if config_prefix is not None:
            self.config_prefix = config_prefix.rstrip(sep)
        else:
            self.config_prefix = config_prefix

        logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
        try:
            logger.setLevel(os.environ.get("AZURE_HTTP_LOGGING_LEVEL", logging.WARNING))
        except ValueError:
            logger.setLevel(logging.WARNING)

        self.sep = sep
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.kwargs = kwargs

    @cached_property
    def client(self) -> SecretClient:
        """Create a Azure Key Vault client."""
        credential: ClientSecretCredential | DefaultAzureCredential
        if all([self.tenant_id, self.client_id, self.client_secret]):
            credential = ClientSecretCredential(self.tenant_id, self.client_id, self.client_secret)
        else:
            credential = DefaultAzureCredential()
        client = SecretClient(vault_url=self.vault_url, credential=credential, **self.kwargs)
        return client

    def get_conn_value(self, conn_id: str) -> str | None:
        """
        Get a serialized representation of Airflow Connection from an Azure Key Vault secret.

        :param conn_id: The Airflow connection id to retrieve
        """
        if self.connections_prefix is None:
            return None

        return self._get_secret(self.connections_prefix, conn_id)

    def get_conn_uri(self, conn_id: str) -> str | None:
        """
        Return URI representation of Connection conn_id.

        As of Airflow version 2.3.0 this method is deprecated.

        :param conn_id: the connection id
        :return: deserialized Connection
        """
        if _parse_version(airflow_version) >= (2, 3):
            warnings.warn(
                f"Method `{self.__class__.__name__}.get_conn_uri` is deprecated and will be removed "
                "in a future release.  Please use method `get_conn_value` instead.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
        return self.get_conn_value(conn_id)

    def get_variable(self, key: str) -> str | None:
        """
        Get an Airflow Variable from an Azure Key Vault secret.

        :param key: Variable Key
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None

        return self._get_secret(self.variables_prefix, key)

    def get_config(self, key: str) -> str | None:
        """
        Get Airflow Configuration.

        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        if self.config_prefix is None:
            return None

        return self._get_secret(self.config_prefix, key)

    @staticmethod
    def build_path(path_prefix: str, secret_id: str, sep: str = "-") -> str:
        """
        Given a path_prefix and secret_id, build a valid secret name for the Azure Key Vault Backend.

        Also replaces underscore in the path with dashes to support easy switching between
        environment variables, so ``connection_default`` becomes ``connection-default``.

        :param path_prefix: The path prefix of the secret to retrieve
        :param secret_id: Name of the secret
        :param sep: Separator used to concatenate path_prefix and secret_id
        """
        # When an empty prefix is given, do not add a separator to the secret name
        if path_prefix == "":
            path = f"{secret_id}"
        else:
            path = f"{path_prefix}{sep}{secret_id}"
        return path.replace("_", sep)

    def _get_secret(self, path_prefix: str, secret_id: str) -> str | None:
        """
        Get an Azure Key Vault secret value.

        :param path_prefix: Prefix for the Path to get Secret
        :param secret_id: Secret Key
        """
        name = self.build_path(path_prefix, secret_id, self.sep)
        try:
            secret = self.client.get_secret(name=name)
            return secret.value
        except ResourceNotFoundError as ex:
            self.log.debug("Secret %s not found: %s", name, ex)
            return None
