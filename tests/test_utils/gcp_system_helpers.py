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
import os
from typing import Optional, Sequence

from airflow.providers.google.cloud.utils.credentials_provider import provide_gcp_conn_and_credentials
from tests.test_utils import AIRFLOW_MAIN_FOLDER

CLOUD_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "google", "cloud", "example_dags"
)
MARKETING_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "google", "marketing_platform", "example_dags"
)
POSTGRES_LOCAL_EXECUTOR = os.path.join(
    AIRFLOW_MAIN_FOLDER, "tests", "test_utils", "postgres_local_executor.cfg"
)


def resolve_full_gcp_key_path(key: str) -> str:
    """
    Returns path full path to provided GCP key.

    :param key: Name of the GCP key, for example ``my_service.json``
    :type key: str
    :returns: Full path to the key
    """
    path = os.environ.get("CREDENTIALS_DIR", "/files/airflow-breeze-config/keys")
    key = os.path.join(path, key)
    return key


def provide_gcp_context(
    key_file_path: Optional[str] = None,
    scopes: Optional[Sequence] = None,
    project_id: Optional[str] = None,
):
    """
    Context manager that provides both:

    - GCP credentials for application supporting `Application Default Credentials (ADC)
    strategy <https://cloud.google.com/docs/authentication/production>`__.
    - temporary value of ``AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT`` connection

    Moreover it resolves full path to service keys so user can pass ``myservice.json``
    as ``key_file_path``.

    :param key_file_path: Path to file with GCP credentials .json file.
    :type key_file_path: str
    :param scopes: OAuth scopes for the connection
    :type scopes: Sequence
    :param project_id: The id of GCP project for the connection.
    :type project_id: str
    """
    key_file_path = resolve_full_gcp_key_path(key_file_path)  # type: ignore
    return provide_gcp_conn_and_credentials(
        key_file_path=key_file_path, scopes=scopes, project_id=project_id
    )
