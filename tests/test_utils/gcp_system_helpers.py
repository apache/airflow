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
import os
from tempfile import TemporaryDirectory
from typing import List, Optional, Sequence

import pytest
from google.auth.environment_vars import CREDENTIALS

from airflow.gcp.utils.credentials_provider import (
    provide_gcp_conn_and_credentials, temporary_environment_variable,
)
from tests.gcp.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils.system_tests_class import SystemTest

AIRFLOW_MAIN_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)
GCP_DAG_FOLDER = os.path.join(AIRFLOW_MAIN_FOLDER, "airflow", "gcp", "example_dags")
CLOUD_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "google", "cloud", "example_dags"
)
POSTGRES_LOCAL_EXECUTOR = os.path.join(
    AIRFLOW_MAIN_FOLDER, "tests", "test_utils", "postgres_local_executor.cfg"
)


SKIP_TEST_WARNING = """
The test requires the following GCP service key: {key}{long_test}.
You can enable the test in one of two ways:

* Run this test within Breeze environment and place your {key} under
  /files/gcp/keys directory.
* Set GCP_CONFIG_DIR environment variable to point to the GCP configuration
  directory which keeps the {key} key.

"""

LONG_TEST_INFO = """
and environment variable GCP_ENABLE_LONG_TESTS=True
"""


def resolve_full_gcp_key_path(key: str) -> str:
    """
    Returns path full path to provided GCP key.

    :param key: Name of the GCP key, for example ``my_service.json``
    :type key: str
    :returns: Full path to the key
    """
    path = os.environ.get("GCP_CONFIG_DIR", "/files/gcp/keys")
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
    with temporary_environment_variable("AIRFLOW__CORE__EXECUTOR", "LocalExecutor"):
        return provide_gcp_conn_and_credentials(
            key_file_path=key_file_path, scopes=scopes, project_id=project_id
        )


class GcpSystemTest(SystemTest):
    @staticmethod
    def _project_id():
        return os.environ.get("GCP_PROJECT_ID")

    @staticmethod
    def _service_key():
        return os.environ.get(CREDENTIALS)

    @classmethod
    def authenticate(cls):
        """
        Authenticate with service account specified via key name.

        Required only when we use gcloud / gsutil.
        """
        cls.executor.execute_cmd(
            [
                "gcloud",
                "auth",
                "activate-service-account",
                f"--key-file={cls._service_key()}",
                f"--project={cls._project_id()}",
            ]
        )

    @classmethod
    def revoke_authentication(cls):
        """
        Change default authentication to none - which is not existing one.
        """
        cls.executor.execute_cmd(
            [
                "gcloud",
                "config",
                "set",
                "account",
                "none",
                f"--project={cls._project_id()}",
            ]
        )

    @classmethod
    def execute_with_ctx(cls, cmd: List[str], key: str = GCP_GCS_KEY):
        """
        Executes command with context created by provide_gcp_context and activated
        service key.
        """
        with provide_gcp_context(key):
            cls.authenticate()
            env = os.environ.copy()
            cls.executor.execute_cmd(cmd=cmd, env=env)
            cls.revoke_authentication()

    @classmethod
    def create_gcs_bucket(cls, name: str, location: Optional[str] = None) -> None:
        cmd = ["gsutil", "mb"]
        if location:
            cmd += ["-c", "regional", "-l", location]
        cmd += [f"gs://{name}"]
        cls.execute_with_ctx(cmd, key=GCP_GCS_KEY)

    @classmethod
    def delete_gcs_bucket(cls, name: str):
        cmd = ["gsutil", "-m", "rm", "-r", f"gs://{name}"]
        cls.execute_with_ctx(cmd, key=GCP_GCS_KEY)

    @classmethod
    def upload_to_gcs(cls, bucket_uri: str, filename: str):
        cls.execute_with_ctx(
            ["gsutil", "cp", f"{filename}", f"{bucket_uri}"], key=GCP_GCS_KEY
        )

    @classmethod
    def upload_content_to_gcs(cls, lines: List[str], bucket_uri: str, filename: str):
        with TemporaryDirectory(prefix="airflow-gcp") as tmp_dir:
            tmp_path = os.path.join(tmp_dir, filename)
            with open(tmp_path, "w") as file:
                file.writelines(lines)
                file.flush()
            os.chmod(tmp_path, 555)
            cls.upload_to_gcs(bucket_uri, tmp_path)

    @staticmethod
    def skip(service_key: str, long_lasting: bool = False):  # type: ignore
        """
        Decorator for skipping GCP system tests.

        :param service_key: name of the service key that will be used to provide credentials
        :type service_key: str
        :param long_lasting: set True if a test take relatively long time
        :type long_lasting: bool
        """
        try:
            full_key_path = resolve_full_gcp_key_path(service_key)
            with open(full_key_path):
                pass
        except FileNotFoundError:
            return pytest.skip(SKIP_TEST_WARNING.format(key=service_key, long_test=""))

        if long_lasting and os.environ.get("GCP_ENABLE_LONG_TESTS") == "True":
            return pytest.skip(
                SKIP_TEST_WARNING.format(key=service_key, long_test=LONG_TEST_INFO)
            )

        return lambda cls: cls
