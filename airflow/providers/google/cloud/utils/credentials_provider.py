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

AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT = "AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT"


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
def temporary_environment_variable(variable_name: str, value: str):
    """
    Context manager that set up temporary value for a given environment
    variable and the restore initial state.

    :param variable_name: Name of the environment variable
    :type variable_name: str
    :param value: The temporary value
    :type value: str
    """
    # Save initial value
    init_value = os.environ.get(variable_name)
    try:
        # set temporary value
        os.environ[variable_name] = value
        yield
    finally:
        # Restore initial state (remove or restore)
        if variable_name in os.environ:
            del os.environ[variable_name]
        if init_value:
            os.environ[variable_name] = init_value


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
            with temporary_environment_variable(CREDENTIALS, key_file_path):
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

    with temporary_environment_variable(AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT, conn):
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
    with provide_gcp_credentials(key_file_path), provide_gcp_connection(
        key_file_path, scopes, project_id
    ):
        yield
