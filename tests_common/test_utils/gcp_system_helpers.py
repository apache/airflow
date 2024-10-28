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
from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Sequence
from unittest import mock

import pytest
from google.auth.environment_vars import CLOUD_SDK_CONFIG_DIR, CREDENTIALS

import airflow.providers.google
from airflow.providers.google.cloud.utils.credentials_provider import (
    provide_gcp_conn_and_credentials,
)

from providers.tests.google.cloud.utils.gcp_authenticator import (
    GCP_GCS_KEY,
    GCP_SECRET_MANAGER_KEY,
)
from tests_common.test_utils import AIRFLOW_MAIN_FOLDER
from tests_common.test_utils.logging_command_executor import CommandExecutor
from tests_common.test_utils.system_tests_class import SystemTest

GCP_DIR = Path(airflow.providers.google.__file__).parent
CLOUD_DAG_FOLDER = GCP_DIR.joinpath("cloud", "example_dags")
MARKETING_DAG_FOLDER = GCP_DIR.joinpath("marketing_platform", "example_dags")
GSUITE_DAG_FOLDER = GCP_DIR.joinpath("suite", "example_dags")
FIREBASE_DAG_FOLDER = GCP_DIR.joinpath("firebase", "example_dags")
LEVELDB_DAG_FOLDER = GCP_DIR.joinpath("leveldb", "example_dags")
POSTGRES_LOCAL_EXECUTOR = os.path.join(
    AIRFLOW_MAIN_FOLDER, "tests", "test_utils", "postgres_local_executor.cfg"
)


def resolve_full_gcp_key_path(key: str) -> str:
    """
    Return path full path to provided GCP key.

    :param key: Name of the GCP key, for example ``my_service.json``
    :returns: Full path to the key
    """
    path = os.environ.get("CREDENTIALS_DIR", "/files/airflow-breeze-config/keys")
    key = os.path.join(path, key)
    return key


@contextmanager
def provide_gcp_context(
    key_file_path: str | None = None,
    scopes: Sequence | None = None,
    project_id: str | None = None,
):
    """
    Provide context manager for GCP.

    - GCP credentials for application supporting `Application Default Credentials (ADC)
    strategy <https://cloud.google.com/docs/authentication/production>`__.
    - temporary value of :envvar:`AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT` variable
    - the ``gcloud`` config directory isolated from user configuration

    Moreover it resolves full path to service keys so user can pass ``myservice.json``
    as ``key_file_path``.

    :param key_file_path: Path to file with GCP credentials .json file.
    :param scopes: OAuth scopes for the connection
    :param project_id: The id of GCP project for the connection.
        Default: ``os.environ["GCP_PROJECT_ID"]`` or None
    """
    key_file_path = resolve_full_gcp_key_path(key_file_path)  # type: ignore
    if project_id is None:
        project_id = os.environ.get("GCP_PROJECT_ID")
    with provide_gcp_conn_and_credentials(
        key_file_path, scopes, project_id
    ), tempfile.TemporaryDirectory() as gcloud_config_tmp, mock.patch.dict(
        "os.environ", {CLOUD_SDK_CONFIG_DIR: gcloud_config_tmp}
    ):
        executor = CommandExecutor()

        if key_file_path:
            executor.execute_cmd(
                [
                    "gcloud",
                    "auth",
                    "activate-service-account",
                    f"--key-file={key_file_path}",
                ]
            )
        if project_id:
            executor.execute_cmd(["gcloud", "config", "set", "core/project", project_id])
        yield


@contextmanager
@provide_gcp_context(GCP_GCS_KEY)
def provide_gcs_bucket(bucket_name: str):
    GoogleSystemTest.create_gcs_bucket(bucket_name)
    yield
    GoogleSystemTest.delete_gcs_bucket(bucket_name)


@pytest.mark.system("google")
class GoogleSystemTest(SystemTest):
    """Base class for Google system tests."""

    @staticmethod
    def execute_cmd(*args, **kwargs):
        executor = CommandExecutor()
        return executor.execute_cmd(*args, **kwargs)

    @staticmethod
    def _project_id():
        return os.environ.get("GCP_PROJECT_ID")

    @staticmethod
    def _service_key():
        return os.environ.get(CREDENTIALS)

    @classmethod
    def execute_with_ctx(
        cls,
        cmd: list[str],
        key: str = GCP_GCS_KEY,
        project_id=None,
        scopes=None,
        silent: bool = False,
    ):
        """Execute command with context created by provide_gcp_context and activated service key."""
        current_project_id = project_id or cls._project_id()
        with provide_gcp_context(key, project_id=current_project_id, scopes=scopes):
            cls.execute_cmd(cmd=cmd, silent=silent)

    @classmethod
    def create_gcs_bucket(cls, name: str, location: str | None = None) -> None:
        bucket_name = f"gs://{name}" if not name.startswith("gs://") else name
        cmd = ["gsutil", "mb"]
        if location:
            cmd += ["-c", "regional", "-l", location]
        cmd += [bucket_name]
        cls.execute_with_ctx(cmd, key=GCP_GCS_KEY)

    @classmethod
    def delete_gcs_bucket(cls, name: str):
        bucket_name = f"gs://{name}" if not name.startswith("gs://") else name
        cmd = ["gsutil", "-m", "rm", "-r", bucket_name]
        cls.execute_with_ctx(cmd, key=GCP_GCS_KEY)

    @classmethod
    def upload_to_gcs(cls, source_uri: str, target_uri: str):
        cls.execute_with_ctx(["gsutil", "cp", source_uri, target_uri], key=GCP_GCS_KEY)

    @classmethod
    def upload_content_to_gcs(cls, lines: str, bucket: str, filename: str):
        bucket_name = f"gs://{bucket}" if not bucket.startswith("gs://") else bucket
        with TemporaryDirectory(prefix="airflow-gcp") as tmp_dir:
            tmp_path = os.path.join(tmp_dir, filename)
            tmp_dir_path = os.path.dirname(tmp_path)
            if tmp_dir_path:
                os.makedirs(tmp_dir_path, exist_ok=True)
            with open(tmp_path, "w") as file:
                file.writelines(lines)
                file.flush()
            os.chmod(tmp_path, 777)
            cls.upload_to_gcs(tmp_path, bucket_name)

    @classmethod
    def get_project_number(cls, project_id: str) -> str:
        cmd = [
            "gcloud",
            "projects",
            "describe",
            project_id,
            "--format",
            "value(projectNumber)",
        ]
        return cls.check_output(cmd).decode("utf-8").strip()

    @classmethod
    def grant_bucket_access(cls, bucket: str, account_email: str):
        bucket_name = f"gs://{bucket}" if not bucket.startswith("gs://") else bucket
        cls.execute_cmd(
            [
                "gsutil",
                "iam",
                "ch",
                f"serviceAccount:{account_email}:admin",
                bucket_name,
            ]
        )

    @classmethod
    def delete_secret(cls, name: str, silent: bool = False):
        cmd = [
            "gcloud",
            "secrets",
            "delete",
            name,
            "--project",
            GoogleSystemTest._project_id(),
            "--quiet",
        ]
        cls.execute_with_ctx(cmd, key=GCP_SECRET_MANAGER_KEY, silent=silent)

    @classmethod
    def create_secret(cls, name: str, value: str):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(value.encode("UTF-8"))
            tmp.flush()
            cmd = [
                "gcloud",
                "secrets",
                "create",
                name,
                "--replication-policy",
                "automatic",
                "--project",
                GoogleSystemTest._project_id(),
                "--data-file",
                tmp.name,
            ]
            cls.execute_with_ctx(cmd, key=GCP_SECRET_MANAGER_KEY)

    @classmethod
    def update_secret(cls, name: str, value: str):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(value.encode("UTF-8"))
            tmp.flush()
            cmd = [
                "gcloud",
                "secrets",
                "versions",
                "add",
                name,
                "--project",
                GoogleSystemTest._project_id(),
                "--data-file",
                tmp.name,
            ]
            cls.execute_with_ctx(cmd, key=GCP_SECRET_MANAGER_KEY)
