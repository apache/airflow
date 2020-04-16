# -*- coding: utf-8 -*-
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
#

import logging
from typing import Dict, Optional, Sequence

import google.auth
import google.oauth2.service_account
from airflow.exceptions import AirflowException

log = logging.getLogger(__name__)

AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT = "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"
_DEFAULT_SCOPES = ('https://www.googleapis.com/auth/cloud-platform',)  # type: Sequence[str]


def get_credentials_and_project_id(
    key_path=None,  # type: Optional[str]
    keyfile_dict=None,  # type: Optional[Dict[str, str]]
    scopes=None,  # type: Optional[Sequence[str]]
    delegate_to=None  # type: Optional[str]
):
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
        credentials = credentials.with_subject(delegate_to)

    return credentials, project_id


def _get_scopes(scopes=None):
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
