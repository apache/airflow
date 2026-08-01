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
"""Objects relating to sourcing connections from Google Cloud Secrets Manager."""

from __future__ import annotations

import logging
from collections.abc import Sequence

from google.auth.exceptions import DefaultCredentialsError

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud._internal_client.secret_manager_client import _SecretManagerClient
from airflow.providers.google.cloud.utils.credentials_provider import (
    _get_target_principal_and_delegates,
    get_credentials_and_project_id,
)
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.secrets import BaseSecretsBackend
from airflow.utils.log.logging_mixin import LoggingMixin

log = logging.getLogger(__name__)

SECRET_ID_PATTERN = r"^[a-zA-Z0-9-_]*$"

# Separator between the team name and the secret id in a team scoped secret name.
# Matches the convention the other secrets backends use.
TEAM_SEP = "--"


class CloudSecretManagerBackend(BaseSecretsBackend, LoggingMixin):
    """
    Retrieves Connection object from Google Cloud Secrets Manager.

    Configurable via ``airflow.cfg`` as follows:

    .. code-block:: ini

        [secrets]
        backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
        backend_kwargs = {"connections_prefix": "airflow-connections", "sep": "-"}

    For example, if the Secrets Manager secret id is ``airflow-connections-smtp_default``, this would be
    accessible if you provide ``{"connections_prefix": "airflow-connections", "sep": "-"}`` and request
    conn_id ``smtp_default``.

    If the Secrets Manager secret id is ``airflow-variables-hello``, this would be
    accessible if you provide ``{"variables_prefix": "airflow-variables", "sep": "-"}`` and request
    Variable Key ``hello``.

    The full secret id should follow the pattern "[a-zA-Z0-9-_]".

    :param connections_prefix: Specifies the prefix of the secret to read to get Connections.
        If set to None (null), requests for connections will not be sent to GCP Secrets Manager
    :param variables_prefix: Specifies the prefix of the secret to read to get Variables.
        If set to None (null), requests for variables will not be sent to GCP Secrets Manager
    :param config_prefix: Specifies the prefix of the secret to read to get Airflow Configurations
        containing secrets.
        If set to None (null), requests for configurations will not be sent to GCP Secrets Manager
    :param gcp_key_path: Path to Google Cloud Service Account key file (JSON). Mutually exclusive with
        gcp_keyfile_dict. use default credentials in the current environment if not provided.
    :param gcp_keyfile_dict: Dictionary of keyfile parameters. Mutually exclusive with gcp_key_path.
    :param gcp_credential_config_file: File path to or content of a GCP credential configuration file.
    :param gcp_scopes: Comma-separated string containing OAuth2 scopes
    :param project_id: Project ID to read the secrets from. If not passed, the project ID from credentials
        will be used.
    :param sep: Separator used to concatenate connections_prefix and conn_id. Default: "-"
    :param impersonation_chain: Optional service account to impersonate using
        short-term credentials, or chained list of accounts required to get the
        access token of the last account in the list, which will be impersonated
        in the request. If set as a string, the account must grant the
        originating account the Service Account Token Creator IAM role. If set
        as a sequence, the identities from the list must grant Service Account
        Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account.
    """

    def __init__(
        self,
        connections_prefix: str = "airflow-connections",
        variables_prefix: str = "airflow-variables",
        config_prefix: str = "airflow-config",
        gcp_keyfile_dict: dict | None = None,
        gcp_key_path: str | None = None,
        gcp_credential_config_file: dict[str, str] | str | None = None,
        gcp_scopes: str | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
        sep: str = "-",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.connections_prefix = connections_prefix
        self.variables_prefix = variables_prefix
        self.config_prefix = config_prefix
        self.sep = sep
        if connections_prefix is not None:
            if not self._is_valid_prefix_and_sep():
                raise AirflowException(
                    "`connections_prefix`, `variables_prefix` and `sep` should "
                    f"follows that pattern {SECRET_ID_PATTERN}"
                )
        try:
            if impersonation_chain:
                target_principal, delegates = _get_target_principal_and_delegates(impersonation_chain)
            else:
                target_principal = None
                delegates = None

            self.credentials, self.project_id = get_credentials_and_project_id(
                keyfile_dict=gcp_keyfile_dict,
                key_path=gcp_key_path,
                credential_config_file=gcp_credential_config_file,
                scopes=gcp_scopes,
                target_principal=target_principal,
                delegates=delegates,
            )
        except (DefaultCredentialsError, FileNotFoundError):
            log.exception(
                "Unable to load credentials for GCP Secret Manager. "
                "Make sure that the keyfile path or dictionary, credential configuration file, "
                "or GOOGLE_APPLICATION_CREDENTIALS environment variable is correct and properly configured."
            )

        # In case project id provided
        if project_id:
            self.project_id = project_id

        if not self.project_id:
            raise ValueError(
                "Project ID could not be determined. "
                "Please provide 'project_id' in backend configuration or ensure "
                "your credentials include a default project."
            )

    @property
    def client(self) -> _SecretManagerClient:
        """
        Property returning secret client.

        :return: Secrets client
        """
        return _SecretManagerClient(credentials=self.credentials)

    def _is_valid_prefix_and_sep(self) -> bool:
        prefix = self.connections_prefix + self.sep
        return _SecretManagerClient.is_valid_secret_name(prefix)

    def get_conn_value(self, conn_id: str, team_name: str | None = None) -> str | None:
        """
        Get serialized representation of Connection.

        :param conn_id: connection id
        :param team_name: Team name associated to the task trying to access the connection (if any)
        """
        if self.connections_prefix is None:
            return None

        if self._names_a_team_namespace(conn_id):
            self._log_refusal("connection", conn_id)
            return None

        return self._get_secret(self.connections_prefix, conn_id, team_name)

    def get_variable(self, key: str, team_name: str | None = None) -> str | None:
        """
        Get Airflow Variable from Environment Variable.

        :param key: Variable Key
        :param team_name: Team name associated to the task trying to access the variable (if any)
        :return: Variable Value
        """
        if self.variables_prefix is None:
            return None

        if self._names_a_team_namespace(key):
            self._log_refusal("variable", key)
            return None

        return self._get_secret(self.variables_prefix, key, team_name)

    def get_config(self, key: str) -> str | None:
        """
        Get Airflow Configuration.

        :param key: Configuration Option Key
        :return: Configuration Option Value
        """
        if self.config_prefix is None:
            return None

        return self._get_secret(self.config_prefix, key)

    def _log_refusal(self, kind: str, secret_id: str) -> None:
        self.log.warning(
            "%s id %r contains %r, which separates the team name from the secret id in a team "
            "scoped secret name. Such an id is ambiguous and is not looked up. Returning None.",
            kind.capitalize(),
            secret_id,
            TEAM_SEP,
        )

    def _build_team_secret_name(self, path_prefix: str, team_name: str, secret_id: str) -> str:
        """
        Build a team scoped secret name using a dedicated separator before the secret id.

        The secret id is used verbatim. Normalizing it (``_`` -> ``sep``, as the Azure Key
        Vault backend does) would let a plain id manufacture ``TEAM_SEP``: team ``a`` asking
        for ``b__c`` would build the same name as team ``a--b`` asking for ``c``. It would
        also contradict this backend's naming, which keeps underscores everywhere else --
        unlike Azure, this backend does not override :meth:`build_path`.
        """
        team_prefix = self.build_path(path_prefix, team_name, self.sep)
        return f"{team_prefix}{TEAM_SEP}{secret_id}"

    def _names_a_team_namespace(self, secret_id: str) -> bool:
        """
        Whether ``secret_id`` spells out a team scoped secret name.

        A team scoped secret is named ``<prefix><sep><team><TEAM_SEP><secret id>``, so an id
        that itself contains the team separator makes the built name ambiguous: team ``a``
        with id ``b--c`` and team ``a--b`` with id ``c`` produce the same string. Such an id
        is refused for *every* lookup -- team scoped as well as team agnostic -- because the
        ambiguity exists in both directions and the caller's own namespace is not a safe
        harbor for it.

        The id is never parsed to work out *which* team it names, because it cannot be:
        nothing in the string distinguishes the two readings above. Comparing the id against
        the prefix the caller's own team builds looks equivalent and is not -- a caller in
        team ``a`` would match ``a--b``'s namespace on the prefix and read its secrets. Only
        the caller's own namespace is ever constructed, never parsed.

        The raw id is matched. Routing it through :meth:`build_path` first, as the Azure
        backend does, is wrong here: the inherited implementation prepends a separator to an
        empty prefix (``'' -> '-smtp_default'``) and normalizes nothing, so the guard would
        both mis-anchor and miss ids whose separator only appears after normalization.
        """
        return TEAM_SEP in secret_id

    def _get_secret(self, path_prefix: str, secret_id: str, team_name: str | None = None) -> str | None:
        """
        Get secret value from the SecretManager based on prefix.

        :param path_prefix: Prefix for the Path to get Secret
        :param secret_id: Secret Key
        :param team_name: Team the lookup is scoped to (if any)
        """
        # ``self.client`` builds a new ``_SecretManagerClient`` -- and with it a real gRPC
        # client -- on every access, so it is bound once for both lookups below.
        client = self.client

        # The team scoped name is tried first and is safe by construction: it can only ever
        # build the caller's own namespace. Ids that would make that name ambiguous are
        # refused by the callers before reaching here.
        if team_name:
            team_secret = client.get_secret(
                secret_id=self._build_team_secret_name(path_prefix, team_name, secret_id),
                project_id=self.project_id,
            )
            if team_secret is not None:
                return team_secret

        return client.get_secret(
            secret_id=self.build_path(path_prefix, secret_id, self.sep), project_id=self.project_id
        )
