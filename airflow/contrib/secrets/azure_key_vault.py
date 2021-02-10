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
from typing import Optional

from azure.core.exceptions import ResourceNotFoundError
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from cached_property import cached_property

from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin


class AzureKeyVaultBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Airflow Connections or Variables from Azure Key Vault secrets.

    The Azure Key Vault can be configured as a secrets backend in the ``airflow.cfg``:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.microsoft.azure.secrets.azure_key_vault.AzureKeyVaultBackend
        backend_kwargs = {"connections_prefix": "airflow-connections", "vault_url": "<azure_key_vault_uri>"}

    For example, if the secrets prefix is ``airflow-connections-smtp-default``, this would be accessible
    if you provide ``{"connections_prefix": "airflow-connections"}`` and request conn_id ``smtp-default``.
    And if variables prefix is ``airflow-variables-hello``, this would be accessible
    if you provide ``{"variables_prefix": "airflow-variables"}`` and request variable key ``hello``.

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections
    :type connections_prefix: str
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables
    :type variables_prefix: str
    :param config_prefix: Specifies the prefix of the secret to read to get Variables.
    :type config_prefix: str
    :param vault_url: The URL of an Azure Key Vault to use
    :type vault_url: str
    :param sep: separator used to concatenate secret_prefix and secret_id. Default: "-"
    :type sep: str
    """

    def __init__(
        self,
        connections_prefix='airflow-connections',  # type: str
        variables_prefix='airflow-variables',  # type: str
        config_prefix='airflow-config',  # type: str
        vault_url='',  # type: str
        sep='-',  # type: str
        **kwargs
    ):
        super(AzureKeyVaultBackend, self).__init__()
        self.vault_url = vault_url
        self.connections_prefix = connections_prefix.rstrip(sep)
        self.variables_prefix = variables_prefix.rstrip(sep)
        self.config_prefix = config_prefix.rstrip(sep)
        self.sep = sep
        self.kwargs = kwargs

    @cached_property
    def client(self):
        """
        Create a Azure Key Vault client.
        """
        credential = DefaultAzureCredential()
        client = SecretClient(vault_url=self.vault_url, credential=credential, **self.kwargs)
        return client

    def get_conn_uri(self, conn_id):
        # type: (str) -> Optional[str]
        """
        Get an Airflow Connection URI from an Azure Key Vault secret

        :param conn_id: The Airflow connection id to retrieve
        :type conn_id: str
        """
        return self._get_secret(self.connections_prefix, conn_id)

    def get_variable(self, key):
        # type: (str) -> Optional[str]
        """
        Get an Airflow Variable from an Azure Key Vault secret.

        :param key: Variable Key
        :type key: str
        :return: Variable Value
        """
        return self._get_secret(self.variables_prefix, key)

    def get_config(self, key):
        # type: (str) -> Optional[str]
        """
        Get Airflow Configuration

        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        return self._get_secret(self.config_prefix, key)

    @staticmethod
    def build_path(path_prefix, secret_id, sep='-'):
        # type: (str, str, str) -> str
        """
        Given a path_prefix and secret_id, build a valid secret name for the Azure Key Vault Backend.
        Also replaces underscore in the path with dashes to support easy switching between
        environment variables, so ``connection_default`` becomes ``connection-default``.

        :param path_prefix: The path prefix of the secret to retrieve
        :type path_prefix: str
        :param secret_id: Name of the secret
        :type secret_id: str
        :param sep: Separator used to concatenate path_prefix and secret_id
        :type sep: str
        """
        path = '{}{}{}'.format(path_prefix, sep, secret_id)
        return path.replace('_', sep)

    def _get_secret(self, path_prefix, secret_id):
        # type: (str, str) -> Optional[str]
        """
        Get an Azure Key Vault secret value

        :param path_prefix: Prefix for the Path to get Secret
        :type path_prefix: str
        :param secret_id: Secret Key
        :type secret_id: str
        """
        name = self.build_path(path_prefix, secret_id, self.sep)
        try:
            secret = self.client.get_secret(name=name)
            return secret.value
        except ResourceNotFoundError as ex:
            self.log.debug('Secret %s not found: %s', name, ex)
            return None
