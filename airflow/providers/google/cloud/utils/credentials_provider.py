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
Google Cloud Platform authentication.
"""
import json
import logging
import tempfile
from contextlib import ExitStack, contextmanager
from typing import Dict, Optional, Sequence, Tuple
from urllib.parse import urlencode

import google.auth
import google.oauth2.service_account
from google.auth.environment_vars import CREDENTIALS, LEGACY_PROJECT, PROJECT

from airflow.exceptions import AirflowException
from airflow.utils.process_utils import patch_environ

log = logging.getLogger(__name__)

AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT = "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"
_DEFAULT_SCOPES: Sequence[str] = ('https://www.googleapis.com/auth/cloud-platform',)


def build_gcp_conn(
    key_file_path: Optional[str] = None,
    scopes: Optional[Sequence[str]] = None,
    project_id: Optional[str] = None,
) -> str:
    """
    Builds a variable that can be used as ``AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`` with provided service key,
    scopes and project id.

    :param key_file_path: Path to service key.
    :type key_file_path: Optional[str]
    :param scopes: Required OAuth scopes.
    :type scopes: Optional[List[str]]
    :param project_id: The GCP project id to be used for the connection.
    :type project_id: Optional[str]
    :return: String representing Airflow connection.
    """
    conn = "google-cloud-platform://?{}"
    extras = "extra__google_cloud_platform"

    query_params = dict()
    if key_file_path:
        query_params["{}__key_path".format(extras)] = key_file_path
    if scopes:
        scopes_string = ",".join(scopes)
        query_params["{}__scope".format(extras)] = scopes_string
    if project_id:
        query_params["{}__projects".format(extras)] = project_id

    query = urlencode(query_params)
    return conn.format(query)


@contextmanager
def provide_gcp_credentials(
    key_file_path: Optional[str] = None, key_file_dict: Optional[Dict] = None
):
    """
    Context manager that provides a GCP credentials for application supporting `Application
    Default Credentials (ADC) strategy <https://cloud.google.com/docs/authentication/production>`__.

    It can be used to provide credentials for external programs (e.g. gcloud) that expect authorization
    file in ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.

    :param key_file_path: Path to file with GCP credentials .json file.
    :type key_file_path: str
    :param key_file_dict: Dictionary with credentials.
    :type key_file_dict: Dict
    """
    if not key_file_path and not key_file_dict:
        raise ValueError("Please provide `key_file_path` or `key_file_dict`.")

    if key_file_path and key_file_path.endswith(".p12"):
        raise AirflowException(
            "Legacy P12 key file are not supported, use a JSON key file."
        )

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
    key_file_path: Optional[str] = None,
    scopes: Optional[Sequence] = None,
    project_id: Optional[str] = None,
):
    """
    Context manager that provides a temporary value of AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT
    connection. It build a new connection that includes path to provided service json,
    required scopes and project id.

    :param key_file_path: Path to file with GCP credentials .json file.
    :type key_file_path: str
    :param scopes: OAuth scopes for the connection
    :type scopes: Sequence
    :param project_id: The id of GCP project for the connection.
    :type project_id: str
    """
    if key_file_path and key_file_path.endswith(".p12"):
        raise AirflowException(
            "Legacy P12 key file are not supported, use a JSON key file."
        )

    conn = build_gcp_conn(
        scopes=scopes, key_file_path=key_file_path, project_id=project_id
    )

    with patch_environ({AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT: conn}):
        yield


@contextmanager
def provide_gcp_conn_and_credentials(
    key_file_path: Optional[str] = None,
    scopes: Optional[Sequence] = None,
    project_id: Optional[str] = None,
):
    """
    Context manager that provides both:

    - GCP credentials for application supporting `Application Default Credentials (ADC)
    strategy <https://cloud.google.com/docs/authentication/production>`__.
    - temporary value of ``AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`` connection

    :param key_file_path: Path to file with GCP credentials .json file.
    :type key_file_path: str
    :param scopes: OAuth scopes for the connection
    :type scopes: Sequence
    :param project_id: The id of GCP project for the connection.
    :type project_id: str
    """
    with ExitStack() as stack:
        if key_file_path:
            stack.enter_context(  # type; ignore  # pylint: disable=no-member
                provide_gcp_credentials(key_file_path)
            )
        if project_id:
            stack.enter_context(  # type; ignore  # pylint: disable=no-member
                patch_environ({PROJECT: project_id, LEGACY_PROJECT: project_id})
            )

        stack.enter_context(  # type; ignore  # pylint: disable=no-member
            provide_gcp_connection(key_file_path, scopes, project_id)
        )
        yield


def get_credentials_and_project_id(
    key_path: Optional[str] = None,
    keyfile_dict: Optional[Dict[str, str]] = None,
    scopes: Optional[Sequence[str]] = None,
    delegate_to: Optional[str] = None
) -> Tuple[google.auth.credentials.Credentials, str]:
    """
    Returns the Credentials object for Google API and the associated project_id

    Only either `key_path` or `keyfile_dict` should be provided, or an exception will
    occur. If neither of them are provided, return default credentials for the current environment

    :param key_path: Path to GCP Credential JSON file
    :type key_path: str
    :param keyfile_dict: A dict representing GCP Credential as in the Credential JSON file
    :type keyfile_dict: Dict[str, str]
    :param scopes:  OAuth scopes for the connection
    :type scopes: Sequence[str]
    :param delegate_to: The account to impersonate, if any.
        For this to work, the service account making the request must have
        domain-wide delegation enabled.
    :type delegate_to: str
    :return: Google Auth Credentials
    :type: google.auth.credentials.Credentials
    """
    if key_path and keyfile_dict:
        raise AirflowException(
            "The `keyfile_dict` and `key_path` fields are mutually exclusive. "
            "Please provide only one value."
        )
    if not key_path and not keyfile_dict:
        log.info(
            'Getting connection using `google.auth.default()` since no key file is defined for hook.'
        )
        credentials, project_id = google.auth.default(scopes=scopes)
    elif key_path:
        # Get credentials from a JSON file.
        if key_path.endswith('.json'):
            log.debug('Getting connection using JSON key file %s', key_path)
            credentials = (
                google.oauth2.service_account.Credentials.from_service_account_file(
                    key_path, scopes=scopes)
            )
            project_id = credentials.project_id
        elif key_path.endswith('.p12'):
            raise AirflowException(
                'Legacy P12 key file are not supported, use a JSON key file.'
            )
        else:
            raise AirflowException('Unrecognised extension for key file.')
    else:
        if not keyfile_dict:
            raise ValueError("The keyfile_dict should be set")
        # Depending on how the JSON was formatted, it may contain
        # escaped newlines. Convert those to actual newlines.
        keyfile_dict['private_key'] = keyfile_dict['private_key'].replace(
            '\\n', '\n')

        credentials = (
            google.oauth2.service_account.Credentials.from_service_account_info(
                keyfile_dict, scopes=scopes)
        )
        project_id = credentials.project_id

    if delegate_to:
        if hasattr(credentials, 'with_subject'):
            credentials = credentials.with_subject(delegate_to)
        else:
            raise AirflowException(
                "The `delegate_to` parameter cannot be used here as the current "
                "authentication method does not support account impersonate. "
                "Please use service-account for authorization."
            )

    return credentials, project_id


def _get_scopes(scopes: Optional[str] = None) -> Sequence[str]:
    """
    Parse a comma-separated string containing GCP scopes if `scopes` is provided.
    Otherwise, default scope will be returned.

    :param scopes: A comma-separated string containing GCP scopes
    :type scopes: Optional[str]
    :return: Returns the scope defined in the connection configuration, or the default scope
    :rtype: Sequence[str]
    """
    return [s.strip() for s in scopes.split(',')] \
        if scopes else _DEFAULT_SCOPES
