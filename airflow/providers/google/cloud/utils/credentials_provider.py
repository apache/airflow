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
"""
This module contains a mechanism for providing temporary
Google Cloud authentication.
"""
from __future__ import annotations

import json
import logging
import tempfile
from contextlib import ExitStack, contextmanager
from typing import Collection, Generator, Sequence
from urllib.parse import urlencode

import google.auth
import google.auth.credentials
import google.oauth2.service_account
from google.auth import impersonated_credentials
from google.auth.environment_vars import CREDENTIALS, LEGACY_PROJECT, PROJECT

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud._internal_client.secret_manager_client import _SecretManagerClient
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
    Builds a uri that can be used as :envvar:`AIRFLOW_CONN_{CONN_ID}` with provided service key,
    scopes and project id.

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
    Context manager that provides a Google Cloud credentials for application supporting
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
    Context manager that provides a temporary value of :envvar:`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`
    connection. It build a new connection that includes path to provided service json,
    required scopes and project id.

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
    Context manager that provides both:

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
    Prepare the Credentials object for Google API and the associated project_id

    Only either `key_path` or `keyfile_dict` should be provided, or an exception will
    occur. If neither of them are provided, return default credentials for the current environment

    :param key_path: Path to Google Cloud Service Account key file (JSON).
    :param keyfile_dict: A dict representing Cloud Service Account as in the Credential JSON file
    :param key_secret_name: Keyfile Secret Name in GCP Secret Manager.
    :param key_secret_project_id: Project ID to read the secrets from. If not passed, the project ID from
        default credentials will be used.
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
    """

    def __init__(
        self,
        key_path: str | None = None,
        keyfile_dict: dict[str, str] | None = None,
        key_secret_name: str | None = None,
        key_secret_project_id: str | None = None,
        scopes: Collection[str] | None = None,
        delegate_to: str | None = None,
        disable_logging: bool = False,
        target_principal: str | None = None,
        delegates: Sequence[str] | None = None,
    ) -> None:
        super().__init__()
        key_options = [key_path, key_secret_name, keyfile_dict]
        if len([x for x in key_options if x]) > 1:
            raise AirflowException(
                "The `keyfile_dict`, `key_path`, and `key_secret_name` fields "
                "are all mutually exclusive. Please provide only one value."
            )
        self.key_path = key_path
        self.keyfile_dict = keyfile_dict
        self.key_secret_name = key_secret_name
        self.key_secret_project_id = key_secret_project_id
        self.scopes = scopes
        self.delegate_to = delegate_to
        self.disable_logging = disable_logging
        self.target_principal = target_principal
        self.delegates = delegates

    def get_credentials_and_project(self) -> tuple[google.auth.credentials.Credentials, str]:
        """
        Get current credentials and project ID.

        :return: Google Auth Credentials
        """
        if self.key_path:
            credentials, project_id = self._get_credentials_using_key_path()
        elif self.key_secret_name:
            credentials, project_id = self._get_credentials_using_key_secret_name()
        elif self.keyfile_dict:
            credentials, project_id = self._get_credentials_using_keyfile_dict()
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

    def _get_credentials_using_keyfile_dict(self):
        self._log_debug("Getting connection using JSON Dict")
        # Depending on how the JSON was formatted, it may contain
        # escaped newlines. Convert those to actual newlines.
        self.keyfile_dict["private_key"] = self.keyfile_dict["private_key"].replace("\\n", "\n")
        credentials = google.oauth2.service_account.Credentials.from_service_account_info(
            self.keyfile_dict, scopes=self.scopes
        )
        project_id = credentials.project_id
        return credentials, project_id

    def _get_credentials_using_key_path(self):
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

    def _get_credentials_using_key_secret_name(self):
        self._log_debug("Getting connection using JSON key data from GCP secret: %s", self.key_secret_name)

        # Use ADC to access GCP Secret Manager.
        adc_credentials, adc_project_id = google.auth.default(scopes=self.scopes)
        secret_manager_client = _SecretManagerClient(credentials=adc_credentials)

        if not secret_manager_client.is_valid_secret_name(self.key_secret_name):
            raise AirflowException("Invalid secret name specified for fetching JSON key data.")

        secret_value = secret_manager_client.get_secret(
            secret_id=self.key_secret_name,
            project_id=self.key_secret_project_id if self.key_secret_project_id else adc_project_id,
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

    def _get_credentials_using_adc(self):
        self._log_info(
            "Getting connection using `google.auth.default()` since no key file is defined for hook."
        )
        credentials, project_id = google.auth.default(scopes=self.scopes)
        return credentials, project_id

    def _log_info(self, *args, **kwargs) -> None:
        if not self.disable_logging:
            self.log.info(*args, **kwargs)

    def _log_debug(self, *args, **kwargs) -> None:
        if not self.disable_logging:
            self.log.debug(*args, **kwargs)


def get_credentials_and_project_id(*args, **kwargs) -> tuple[google.auth.credentials.Credentials, str]:
    """Returns the Credentials object for Google API and the associated project_id."""
    return _CredentialProvider(*args, **kwargs).get_credentials_and_project()


def _get_scopes(scopes: str | None = None) -> Sequence[str]:
    """
    Parse a comma-separated string containing OAuth2 scopes if `scopes` is provided.
    Otherwise, default scope will be returned.

    :param scopes: A comma-separated string containing OAuth2 scopes
    :return: Returns the scope defined in the connection configuration, or the default scope
    """
    return [s.strip() for s in scopes.split(",")] if scopes else _DEFAULT_SCOPES


def _get_target_principal_and_delegates(
    impersonation_chain: str | Sequence[str] | None = None,
) -> tuple[str | None, Sequence[str] | None]:
    """
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
    Extracts project_id from service account's email address.

    :param service_account_email: email of the service account.

    :return: Returns the project_id of the provided service account.
    """
    try:
        return service_account_email.split("@")[1].split(".")[0]
    except IndexError:
        raise AirflowException(
            f"Could not extract project_id from service account's email: {service_account_email}."
        )
