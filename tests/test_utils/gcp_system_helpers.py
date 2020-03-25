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
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from typing import List, Optional, Sequence

import pytest
from google.auth.environment_vars import CREDENTIALS

from airflow.providers.google.cloud.utils.credentials_provider import provide_gcp_conn_and_credentials
from tests.providers.google.cloud.utils.gcp_authenticator import GCP_GCS_KEY
from tests.test_utils import AIRFLOW_MAIN_FOLDER
from tests.test_utils.system_tests_class import SystemTest
from tests.utils.logging_command_executor import get_executor

CLOUD_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "google", "cloud", "example_dags"
)
MARKETING_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "google", "marketing_platform", "example_dags"
)
FIREBASE_DAG_FOLDER = os.path.join(
    AIRFLOW_MAIN_FOLDER, "airflow", "providers", "google", "firebase", "example_dags"
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


@pytest.mark.system("google")
class GoogleSystemTest(SystemTest):
    @staticmethod
    def _project_id():
        return os.environ.get("GCP_PROJECT_ID")

    @staticmethod
    def _service_key():
        return os.environ.get(CREDENTIALS)

    @staticmethod
    @contextmanager
    def authentication():
        GoogleSystemTest._authenticate()
        try:
            yield
        finally:
            GoogleSystemTest._revoke_authentication()

    @staticmethod
    def _authenticate():
        """
        Authenticate with service account specified via key name.
        Required only when we use gcloud / gsutil.
        """
        executor = get_executor()
        executor.execute_cmd(
            [
                "gcloud",
                "auth",
                "activate-service-account",
                f"--key-file={GoogleSystemTest._service_key()}",
                f"--project={GoogleSystemTest._project_id()}",
            ]
        )

    @staticmethod
    def _revoke_authentication():
        """
        Change default authentication to none - which is not existing one.
        """
        executor = get_executor()
        executor.execute_cmd(
            [
                "gcloud",
                "config",
                "set",
                "account",
                "none",
                f"--project={GoogleSystemTest._project_id()}",
            ]
        )

    @staticmethod
    def execute_with_ctx(cmd: List[str], key: str = GCP_GCS_KEY):
        """
        Executes command with context created by provide_gcp_context and activated
        service key.
        """
        executor = get_executor()
        with provide_gcp_context(key), GoogleSystemTest.authentication():
            env = os.environ.copy()
            executor.execute_cmd(cmd=cmd, env=env)

    @staticmethod
    def create_gcs_bucket(name: str, location: Optional[str] = None) -> None:
        bucket_name = f"gs://{name}" if not name.startswith("gs://") else name
        cmd = ["gsutil", "mb"]
        if location:
            cmd += ["-c", "regional", "-l", location]
        cmd += [bucket_name]
        GoogleSystemTest.execute_with_ctx(cmd, key=GCP_GCS_KEY)

    @staticmethod
    def delete_gcs_bucket(name: str):
        bucket_name = f"gs://{name}" if not name.startswith("gs://") else name
        cmd = ["gsutil", "-m", "rm", "-r", bucket_name]
        GoogleSystemTest.execute_with_ctx(cmd, key=GCP_GCS_KEY)

    @staticmethod
    def upload_to_gcs(source_uri: str, target_uri: str):
        GoogleSystemTest.execute_with_ctx(
            ["gsutil", "cp", f"{target_uri}", f"{source_uri}"], key=GCP_GCS_KEY
        )

    @staticmethod
    def upload_content_to_gcs(lines: str, bucket_uri: str, filename: str):
        with TemporaryDirectory(prefix="airflow-gcp") as tmp_dir:
            tmp_path = os.path.join(tmp_dir, filename)
            with open(tmp_path, "w") as file:
                file.writelines(lines)
                file.flush()
            os.chmod(tmp_path, 555)
            GoogleSystemTest.upload_to_gcs(bucket_uri, tmp_path)

    @staticmethod
    def get_project_number(project_id: str) -> str:
        with GoogleSystemTest.authentication():
            cmd = ['gcloud', 'projects', 'describe', project_id, '--format', 'value(projectNumber)']
            return GoogleSystemTest.check_output(cmd).decode("utf-8").strip()

    @staticmethod
    def grant_bucket_access(bucket: str, account_email: str):
        bucket_name = f"gs://{bucket}" if not bucket.startswith("gs://") else bucket
        with GoogleSystemTest.authentication():
            GoogleSystemTest.execute_cmd(
                [
                    "gsutil",
                    "iam",
                    "ch",
                    "serviceAccount:%s:admin" % account_email,
                    bucket_name,
                ]
            )
