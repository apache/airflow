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
"""
This module contains a mechanism for providing temporary
Google Cloud Platform authentication.
"""
import json
import os
import tempfile
from contextlib import contextmanager
from typing import Dict, Optional, Sequence
from urllib.parse import urlencode

from google.auth.environment_vars import CREDENTIALS

from airflow.exceptions import AirflowException

_AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT = "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"


def assert_not_legacy_key(key_path: str) -> None:
    """
    Validates if provided key is supported.

    :param key_path: path to the key
    :type key_path: str
    """
    if key_path.endswith(".p12"):
        raise AirflowException(
            "Legacy P12 key file are not supported, use a JSON key file."
        )


def restore_env_variable(value: Optional[str], env_var_name: str = CREDENTIALS) -> None:
    """
    Restores environment variable to provided value.

    :param value: The value to be restored. If None the variable will be unset.
    :type value: Optional[str]
    :param env_var_name: The name of the environment variable to restore
    :type env_var_name: str
    """
    if value is None and env_var_name in os.environ:
        del os.environ[env_var_name]
    elif value:
        os.environ[env_var_name] = value


def build_gcp_conn(
    key_file_path: Optional[str] = None,
    scopes: Optional[Sequence[str]] = None,
    project_id: Optional[str] = None,
) -> str:
    """
    Builds a variable that can be used as `AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` with provided service key,
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


def resolve_full_gcp_key_path(key: str) -> str:
    """
    Returns path full path to provided GCP key.

    :param key: Name of the GCP key, for example "my_service.json"
    :type key: str
    :returns: Full path to the key
    """
    path = os.environ.get("GCP_CONFIG_DIR", "/config")
    key_path = os.path.join(path, "keys", key)
    return key_path


@contextmanager
def provide_gcp_credentials(
    key_file_path: Optional[str] = None,
    key_file_dict: Optional[Dict] = None,
    scopes: Optional[Sequence] = None,
    project_id: Optional[str] = None,
):
    """
    Context manager that provides a GCP credentials for application supporting `Application
    Default Credentials (ADC) strategy <https://cloud.google.com/docs/authentication/production>`__.

    It can be used to provide credentials for external programs (e.g. gcloud) that expect authorization
    file in ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable.

    :param key_file_path: Path to service key.
    :type key_file_path: Optional[str]
    :param key_file_dict: Dictionary used to create temporary key file
    :type key_file_dict: Optional[Dict]
    :param scopes: Required OAuth scopes.
    :type scopes: Optional[List[str]]
    :param project_id: The GCP project id to be used for the connection.
    :type project_id: Optional[str]
    """
    # Store initial state
    initial_gcp_credentials = os.environ.get(CREDENTIALS)
    initial_gcp_conn = os.environ.get(_AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT)

    try:
        if key_file_path:
            assert_not_legacy_key(key_file_path)
            if "/" not in key_file_path:
                # Here we handle case when user passes only "my_service.json"
                key_file_path = resolve_full_gcp_key_path(key_file_path)

        with tempfile.NamedTemporaryFile(mode="w+t") as conf_file:
            if not key_file_path and key_file_dict:
                conf_file.write(json.dumps(key_file_dict))
                conf_file.flush()
                key_file_path = conf_file.name

            if key_file_path:
                # Set new temporary values
                os.environ[CREDENTIALS] = key_file_path
                os.environ[_AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT] = build_gcp_conn(
                    scopes=scopes, key_file_path=key_file_path, project_id=project_id
                )
            else:
                # We will use the default service account credentials.
                pass
            yield
    finally:
        # Restore initial values
        restore_env_variable(initial_gcp_credentials, CREDENTIALS)
        restore_env_variable(initial_gcp_conn, _AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT)
