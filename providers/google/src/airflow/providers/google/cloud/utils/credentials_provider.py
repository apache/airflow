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
"""This module contains a mechanism for providing temporary Google Cloud authentication."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from collections.abc import Collection, Generator, Sequence
from contextlib import ExitStack, contextmanager
from urllib.parse import urlencode

import google.auth
import google.oauth2.service_account
from google.auth import impersonated_credentials
from google.auth.credentials import AnonymousCredentials, Credentials
from google.auth.environment_vars import CREDENTIALS, LEGACY_PROJECT, PROJECT

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.google.cloud._internal_client.secret_manager_client import _SecretManagerClient
from airflow.providers.google.cloud.utils.external_token_supplier import (
    ClientCredentialsGrantFlowTokenSupplier,
)
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.process_utils import patch_environ

log = logging.getLogger(__name__)

AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT = "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"
_DEFAULT_SCOPES: Sequence[str] = ("https://www.googleapis.com/auth/cloud-platform",)


def build_gcp_conn(
    key_file_path: str | None = None,
    scopes: Sequence[str] | None = None,
    project_id: str | None = None,
) -> str:
    """
    Build a uri that can be used as :envvar:`AIRFLOW_CONN_{CONN_ID}` with provided values.

    :param key_file_path: Path to service key.
    :param scopes: Required OAuth scopes.
    :param project_id: The Google Cloud project id to be used for the connection.
    :return: String representing Airflow connection.
    """
    conn = "google-cloud-platform://?{}"
    query_params = {}
    if key_file_path:
        query_params["key_path"] = key_file_path
    if scopes:
        scopes_string = ",".join(scopes)
        query_params["scope"] = scopes_string
    if project_id:
        query_params["projects"] = project_id

    query = urlencode(query_params)
    return conn.format(query)


@contextmanager
def provide_gcp_credentials(
    key_file_path: str | None = None,
    key_file_dict: dict | None = None,
) -> Generator[None, None, None]:
    """
    Context manager that provides Google Cloud credentials for Application Default Credentials (ADC).

    .. seealso::
        `Application Default Credentials (ADC) strategy`__.

    It can be used to provide credentials for external programs (e.g. gcloud) that expect authorization
    file in ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.

    :param key_file_path: Path to file with Google Cloud Service Account .json file.
    :param key_file_dict: Dictionary with credentials.

    __ https://cloud.google.com/docs/authentication/production
    """
    if not key_file_path and not key_file_dict:
        raise ValueError("Please provide `key_file_path` or `key_file_dict`.")

    if key_file_path and key_file_path.endswith(".p12"):
        raise AirflowException("Legacy P12 key file are not supported, use a JSON key file.")

    with tempfile.NamedTemporaryFile(mode="w+t") as conf_file:
        if not key_file_path and key_file_dict:
            conf_file.write(json.dumps(key_file_dict))
            conf_file.flush()
            key_file_path = conf_file.name
        if key_file_path:
            with patch_environ({CREDENTIALS: key_file_path}):
                yield
        else:
            # We will use the default service account credentials.
            yield


@contextmanager
def provide_gcp_connection(
    key_file_path: str | None = None,
    scopes: Sequence | None = None,
    project_id: str | None = None,
) -> Generator[None, None, None]:
    """
    Context manager that provides a temporary value of :envvar:`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` connection.

    It builds a new connection that includes path to provided service json, required scopes and project id.

    :param key_file_path: Path to file with Google Cloud Service Account .json file.
    :param scopes: OAuth scopes for the connection
    :param project_id: The id of Google Cloud project for the connection.
    """
    if key_file_path and key_file_path.endswith(".p12"):
        raise AirflowException("Legacy P12 key file are not supported, use a JSON key file.")

    conn = build_gcp_conn(scopes=scopes, key_file_path=key_file_path, project_id=project_id)

    with patch_environ({AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: conn}):
        yield


@contextmanager
def provide_gcp_conn_and_credentials(
    key_file_path: str | None = None,
    scopes: Sequence | None = None,
    project_id: str | None = None,
) -> Generator[None, None, None]:
    """
    Context manager that provides GPC connection and credentials.

     It provides both:

    - Google Cloud credentials for application supporting `Application Default Credentials (ADC)
      strategy`__.
    - temporary value of :envvar:`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` connection

    :param key_file_path: Path to file with Google Cloud Service Account .json file.
    :param scopes: OAuth scopes for the connection
    :param project_id: The id of Google Cloud project for the connection.

    __ https://cloud.google.com/docs/authentication/production
    """
    with ExitStack() as stack:
        if key_file_path:
            stack.enter_context(provide_gcp_credentials(key_file_path))  # type; ignore
        if project_id:
            stack.enter_context(  # type; ignore
                patch_environ({PROJECT: project_id, LEGACY_PROJECT: project_id})
            )

        stack.enter_context(provide_gcp_connection(key_file_path, scopes, project_id))  # type; ignore
        yield


class _CredentialProvider(LoggingMixin):
    """
    Prepare the Credentials object for Google API and the associated project_id.

    Only either `key_path` or `keyfile_dict` should be provided, or an exception will
    occur. If neither of them are provided, return default credentials for the current environment

    :param key_path: Path to Google Cloud Service Account key file (JSON).
    :param keyfile_dict: A dict representing Cloud Service Account as in the Credential JSON file
    :param key_secret_name: Keyfile Secret Name in GCP Secret Manager.
    :param key_secret_project_id: Project ID to read the secrets from. If not passed, the project ID from
        default credentials will be used.
    :param credential_config_file: File path to or content of a GCP credential configuration file.
    :param scopes:  OAuth scopes for the connection
    :param delegate_to: The account to impersonate using domain-wide delegation of authority,
        if any. For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :param disable_logging: If true, disable all log messages, which allows you to use this
        class to configure Logger.
    :param target_principal: The service account to directly impersonate using short-term
        credentials, if any. For this to work, the target_principal account must grant
        the originating account the Service Account Token Creator IAM role.
    :param delegates: optional chained list of accounts required to get the access_token of
        target_principal. If set, the sequence of identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account and target_principal
        granting the role to the last account from the list.
    :param is_anonymous: Provides an anonymous set of credentials,
        which is useful for APIs which do not require authentication.
    """

    def __init__(
        self,
        key_path: str | None = None,
        keyfile_dict: dict[str, str] | None = None,
        credential_config_file: dict[str, str] | str | None = None,
        key_secret_name: str | None = None,
        key_secret_project_id: str | None = None,
        scopes: Collection[str] | None = None,
        delegate_to: str | None = None,
        disable_logging: bool = False,
        target_principal: str | None = None,
        delegates: Sequence[str] | None = None,
        is_anonymous: bool | None = None,
        idp_issuer_url: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        idp_extra_params_dict: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        key_options_map = {
            "key_path": key_path,
            "keyfile_dict": keyfile_dict,
            "credential_config_file": credential_config_file,
            "key_secret_name": key_secret_name,
            "is_anonymous": is_anonymous,
        }
        key_options_label_provided = [label for label, credential in key_options_map.items() if credential]
        if len(key_options_label_provided) > 1:
            raise AirflowException(
                f"The `keyfile_dict`, `key_path`, `credential_config_file`, `is_anonymous` and"
                f" `key_secret_name` fields are all mutually exclusive. "
                f"Received options: {key_options_label_provided}. Please provide only one value."
            )
        self.key_path = key_path
        self.keyfile_dict = keyfile_dict
        self.credential_config_file = credential_config_file
        self.key_secret_name = key_secret_name
        self.key_secret_project_id = key_secret_project_id
        self.scopes = scopes
        self.delegate_to = delegate_to
        self.disable_logging = disable_logging
        self.target_principal = target_principal
        self.delegates = delegates
        self.is_anonymous = is_anonymous
        self.idp_issuer_url = idp_issuer_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.idp_extra_params_dict = idp_extra_params_dict

    def get_credentials_and_project(self) -> tuple[Credentials, str]:
        """
        Get current credentials and project ID.

        Project ID is an empty string when using anonymous credentials.

        :return: Google Auth Credentials
        """
        if self.is_anonymous:
            credentials: Credentials = AnonymousCredentials()
            project_id = ""
        else:
            if self.key_path:
                credentials, project_id = self._get_credentials_using_key_path()
            elif self.key_secret_name:
                credentials, project_id = self._get_credentials_using_key_secret_name()
            elif self.keyfile_dict:
                credentials, project_id = self._get_credentials_using_keyfile_dict()
            elif self.idp_issuer_url:
                credentials, project_id = (
                    self._get_credentials_using_credential_config_file_and_token_supplier()
                )
            elif self.credential_config_file:
                credentials, project_id = self._get_credentials_using_credential_config_file()
            else:
                credentials, project_id = self._get_credentials_using_adc()
            if self.delegate_to:
                if hasattr(credentials, "with_subject"):
                    credentials = credentials.with_subject(self.delegate_to)
                else:
                    raise AirflowException(
                        "The `delegate_to` parameter cannot be used here as the current "
                        "authentication method does not support account impersonate. "
                        "Please use service-account for authorization."
                    )

            if self.target_principal:
                credentials = impersonated_credentials.Credentials(
                    source_credentials=credentials,
                    target_principal=self.target_principal,
                    delegates=self.delegates,
                    target_scopes=self.scopes,
                )

                project_id = _get_project_id_from_service_account_email(self.target_principal)

        return credentials, project_id

    def _get_credentials_using_keyfile_dict(self) -> tuple[Credentials, str]:
        self._log_debug("Getting connection using JSON Dict")
        # Depending on how the JSON was formatted, it may contain
        # escaped newlines. Convert those to actual newlines.
        if self.keyfile_dict is None:
            raise ValueError("The keyfile_dict field is None, and we need it for keyfile_dict auth.")
        self.keyfile_dict["private_key"] = self.keyfile_dict["private_key"].replace("\\n", "\n")
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            self.keyfile_dict, scopes=self.scopes
        )
        project_id = credentials.project_id
        return credentials, project_id

    def _get_credentials_using_key_path(self) -> tuple[Credentials, str]:
        if self.key_path is None:
            raise ValueError("The ky_path field is None, and we need it for keyfile_dict auth.")
        if self.key_path.endswith(".p12"):
            raise AirflowException("Legacy P12 key file are not supported, use a JSON key file.")

        if not self.key_path.endswith(".json"):
            raise AirflowException("Unrecognised extension for key file.")

        self._log_debug("Getting connection using JSON key file %s", self.key_path)
        credentials = google.oauth2.service_account.Credentials.from_service_account_file(
            self.key_path, scopes=self.scopes
        )
        project_id = credentials.project_id
        return credentials, project_id

    def _get_credentials_using_key_secret_name(self) -> tuple[Credentials, str]:
        self._log_debug("Getting connection using JSON key data from GCP secret: %s", self.key_secret_name)

        # Use ADC to access GCP Secret Manager.
        scopes = list(self.scopes) if self.scopes else None
        adc_credentials, adc_project_id = google.auth.default(scopes=scopes)
        secret_manager_client = _SecretManagerClient(credentials=adc_credentials)

        if self.key_secret_name is None:
            raise ValueError("The key_secret_name field is None, and we need it for keyfile_dict auth.")
        if not secret_manager_client.is_valid_secret_name(self.key_secret_name):
            raise AirflowException("Invalid secret name specified for fetching JSON key data.")

        project_id = self.key_secret_project_id if self.key_secret_project_id else adc_project_id
        if not project_id:
            raise AirflowException(
                "Project ID could not be determined from default credentials. "
                "Please provide `key_secret_project_id` parameter."
            )
        secret_value = secret_manager_client.get_secret(
            secret_id=self.key_secret_name,
            project_id=project_id,
        )
        if secret_value is None:
            raise AirflowException(f"Failed getting value of secret {self.key_secret_name}.")

        try:
            keyfile_dict = json.loads(secret_value)
        except json.decoder.JSONDecodeError:
            raise AirflowException("Key data read from GCP Secret Manager is not valid JSON.")

        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            keyfile_dict, scopes=self.scopes
        )
        project_id = credentials.project_id
        return credentials, project_id

    def _get_credentials_using_credential_config_file(self) -> tuple[Credentials, str]:
        if isinstance(self.credential_config_file, str) and os.path.exists(self.credential_config_file):
            self._log_info(
                f"Getting connection using credential configuration file: `{self.credential_config_file}`"
            )
            credentials, project_id = google.auth.load_credentials_from_file(
                self.credential_config_file, scopes=self.scopes
            )
        else:
            with tempfile.NamedTemporaryFile(mode="w+t") as temp_credentials_fd:
                if isinstance(self.credential_config_file, dict):
                    self._log_info("Getting connection using credential configuration dict.")
                    temp_credentials_fd.write(json.dumps(self.credential_config_file))
                elif isinstance(self.credential_config_file, str):
                    self._log_info("Getting connection using credential configuration string.")
                    temp_credentials_fd.write(self.credential_config_file)

                temp_credentials_fd.flush()
                credentials, project_id = google.auth.load_credentials_from_file(
                    temp_credentials_fd.name, scopes=self.scopes
                )

        return credentials, project_id

    def _get_credentials_using_credential_config_file_and_token_supplier(self):
        self._log_info(
            "Getting connection using credential configuration file and external Identity Provider."
        )

        if not self.credential_config_file:
            raise AirflowException(
                "Credential Configuration File is needed to use authentication by External Identity Provider."
            )

        info = _get_info_from_credential_configuration_file(self.credential_config_file)
        info["subject_token_supplier"] = ClientCredentialsGrantFlowTokenSupplier(
            oidc_issuer_url=self.idp_issuer_url, client_id=self.client_id, client_secret=self.client_secret
        )

        scopes = list(self.scopes) if self.scopes else None
        credentials, project_id = google.auth.load_credentials_from_dict(info=info, scopes=scopes)
        if not project_id:
            raise AirflowException(
                "Project ID could not be determined from default credentials. "
                "Please provide `key_secret_project_id` parameter."
            )
        return credentials, project_id

    def _get_credentials_using_adc(self) -> tuple[Credentials, str]:
        self._log_info(
            "Getting connection using `google.auth.default()` since no explicit credentials are provided."
        )
        scopes = list(self.scopes) if self.scopes else None
        credentials, project_id = google.auth.default(scopes=scopes)
        if not project_id:
            raise AirflowException(
                "Project ID could not be determined from default credentials. "
                "Please provide `key_secret_project_id` parameter."
            )
        return credentials, project_id

    def _log_info(self, *args, **kwargs) -> None:
        if not self.disable_logging:
            self.log.info(*args, **kwargs)

    def _log_debug(self, *args, **kwargs) -> None:
        if not self.disable_logging:
            self.log.debug(*args, **kwargs)


def get_credentials_and_project_id(*args, **kwargs) -> tuple[Credentials, str]:
    """Return the Credentials object for Google API and the associated project_id."""
    return _CredentialProvider(*args, **kwargs).get_credentials_and_project()


def _get_scopes(scopes: str | None = None) -> Sequence[str]:
    """
    Parse a comma-separated string containing OAuth2 scopes if `scopes` is provided; otherwise return default.

    :param scopes: A comma-separated string containing OAuth2 scopes
    :return: Returns the scope defined in the connection configuration, or the default scope
    """
    return [s.strip() for s in scopes.split(",")] if scopes else _DEFAULT_SCOPES


def _get_target_principal_and_delegates(
    impersonation_chain: str | Sequence[str] | None = None,
) -> tuple[str | None, Sequence[str] | None]:
    """
    Get the target_principal and optional list of delegates from impersonation_chain.

    Analyze contents of impersonation_chain and return target_principal (the service account
    to directly impersonate using short-term credentials, if any) and optional list of delegates
    required to get the access_token of target_principal.

    :param impersonation_chain: the service account to impersonate or a chained list leading to this
        account

    :return: Returns the tuple of target_principal and delegates
    """
    if not impersonation_chain:
        return None, None

    if isinstance(impersonation_chain, str):
        return impersonation_chain, None

    return impersonation_chain[-1], impersonation_chain[:-1]


def _get_project_id_from_service_account_email(service_account_email: str) -> str:
    """
    Extract project_id from service account's email address.

    :param service_account_email: email of the service account.

    :return: Returns the project_id of the provided service account.
    """
    try:
        return service_account_email.split("@")[1].split(".")[0]
    except IndexError:
        raise AirflowException(
            f"Could not extract project_id from service account's email: {service_account_email}."
        )


def _get_info_from_credential_configuration_file(
    credential_configuration_file: str | dict[str, str],
) -> dict[str, str]:
    """
    Extract the Credential Configuration File information, either from a json file, json string or dictionary.

    :param credential_configuration_file: File path or content (as json string or dictionary) of a GCP credential configuration file.

    :return: Returns a dictionary containing the Credential Configuration File information.
    """
    # if it's already a dict, just return it
    if isinstance(credential_configuration_file, dict):
        return credential_configuration_file

    if not isinstance(credential_configuration_file, str):
        raise AirflowException(
            f"Invalid argument type, expected str or dict, got {type(credential_configuration_file)}."
        )

    if os.path.exists(credential_configuration_file):  # attempts to load from json file
        with open(credential_configuration_file) as file_obj:
            try:
                return json.load(file_obj)
            except ValueError:
                raise AirflowException(
                    f"Credential Configuration File '{credential_configuration_file}' is not a valid json file."
                )

    # if not a file, attempt to load it from a json string
    try:
        return json.loads(credential_configuration_file)
    except ValueError:
        raise AirflowException("Credential Configuration File is not a valid json string.")
