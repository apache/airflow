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

import json
import os
import subprocess
from importlib.util import find_spec
from pathlib import Path

import pytest

from docker_tests.command_utils import run_command
from docker_tests.constants import SOURCE_ROOT
from docker_tests.docker_tests_utils import (
    display_dependency_conflict_message,
    docker_image,
    run_bash_in_docker,
    run_python_in_docker,
)

DEV_DIR_PATH = SOURCE_ROOT / "dev"
AIRFLOW_PRE_INSTALLED_PROVIDERS_FILE_PATH = DEV_DIR_PATH / "airflow_pre_installed_providers.txt"
PROD_IMAGE_PROVIDERS_FILE_PATH = DEV_DIR_PATH / "prod_image_installed_providers.txt"
AIRFLOW_ROOT_PATH = Path(__file__).parents[2].resolve()
SLIM_IMAGE_PROVIDERS = [
    f"apache-airflow-providers-{provider_id.replace('.','-')}"
    for provider_id in AIRFLOW_PRE_INSTALLED_PROVIDERS_FILE_PATH.read_text().splitlines()
    if not provider_id.startswith("#")
]
REGULAR_IMAGE_PROVIDERS = [
    f"apache-airflow-providers-{provider_id.replace('.','-')}"
    for provider_id in PROD_IMAGE_PROVIDERS_FILE_PATH.read_text().splitlines()
    if not provider_id.startswith("#")
]


class TestCommands:
    def test_without_command(self):
        """Checking the image without a command. It should return non-zero exit code."""
        with pytest.raises(subprocess.CalledProcessError) as ctx:
            run_command(["docker", "run", "--rm", "-e", "COLUMNS=180", docker_image])
        assert 2 == ctx.value.returncode

    def test_airflow_command(self):
        """Checking 'airflow' command  It should return non-zero exit code."""
        with pytest.raises(subprocess.CalledProcessError) as ctx:
            run_command(["docker", "run", "--rm", "-e", "COLUMNS=180", docker_image, "airflow"])
        assert 2 == ctx.value.returncode

    def test_airflow_version(self):
        """Checking 'airflow version' command  It should return zero exit code."""
        output = run_command(
            ["docker", "run", "--rm", "-e", "COLUMNS=180", docker_image, "airflow", "version"],
            return_output=True,
        )
        assert "2." in output

    def test_python_version(self):
        """Checking 'python --version' command  It should return zero exit code."""
        output = run_command(
            ["docker", "run", "--rm", "-e", "COLUMNS=180", docker_image, "python", "--version"],
            return_output=True,
        )
        assert "Python 3." in output

    def test_bash_version(self):
        """Checking 'bash --version' command  It should return zero exit code."""
        output = run_command(
            ["docker", "run", "--rm", "-e", "COLUMNS=180", docker_image, "bash", "--version"],
            return_output=True,
        )
        assert "GNU bash," in output


class TestPythonPackages:
    def test_required_providers_are_installed(self):
        if os.environ.get("TEST_SLIM_IMAGE"):
            packages_to_install = set(SLIM_IMAGE_PROVIDERS)
            package_file = AIRFLOW_PRE_INSTALLED_PROVIDERS_FILE_PATH
        else:
            packages_to_install = set(REGULAR_IMAGE_PROVIDERS)
            package_file = PROD_IMAGE_PROVIDERS_FILE_PATH
        assert len(packages_to_install) != 0
        output = run_bash_in_docker(
            "airflow providers list --output json", stderr=subprocess.DEVNULL, return_output=True
        )
        providers = json.loads(output)
        packages_installed = set(d["package_name"] for d in providers)
        assert len(packages_installed) != 0

        assert (
            packages_to_install == packages_installed
        ), f"List of expected installed packages and image content mismatch. Check {package_file} file."

    def test_pip_dependencies_conflict(self):
        try:
            run_bash_in_docker("pip check")
        except subprocess.CalledProcessError as ex:
            display_dependency_conflict_message()
            raise ex

    PACKAGE_IMPORTS = {
        "amazon": ["boto3", "botocore", "watchtower"],
        "async": ["gevent", "eventlet", "greenlet"],
        "azure": [
            "azure.batch",
            "azure.cosmos",
            "azure.datalake.store",
            "azure.identity",
            "azure.keyvault.secrets",
            "azure.kusto.data",
            "azure.mgmt.containerinstance",
            "azure.mgmt.datalake.store",
            "azure.mgmt.resource",
            "azure.storage",
        ],
        "celery": ["celery", "flower", "vine"],
        "cncf.kubernetes": ["kubernetes", "cryptography"],
        "docker": ["docker"],
        "elasticsearch": ["elasticsearch"],
        "google": [
            "OpenSSL",
            # "google.ads", Remove google ads as it is vendored in google provider now
            "googleapiclient",
            "google.auth",
            "google_auth_httplib2",
            "google.cloud.automl",
            "google.cloud.bigquery_datatransfer",
            "google.cloud.bigtable",
            "google.cloud.container",
            "google.cloud.datacatalog",
            "google.cloud.dataproc",
            "google.cloud.dlp",
            "google.cloud.kms",
            "google.cloud.language",
            "google.cloud.logging",
            "google.cloud.memcache",
            "google.cloud.monitoring",
            "google.cloud.oslogin",
            "google.cloud.pubsub",
            "google.cloud.redis",
            "google.cloud.secretmanager",
            "google.cloud.spanner",
            "google.cloud.speech",
            "google.cloud.storage",
            "google.cloud.tasks",
            "google.cloud.texttospeech",
            "google.cloud.translate",
            "google.cloud.videointelligence",
            "google.cloud.vision",
        ],
        "grpc": ["grpc", "google.auth", "google_auth_httplib2"],
        "hashicorp": ["hvac"],
        "ldap": ["ldap"],
        "mysql": ["MySQLdb", *(["mysql"] if bool(find_spec("mysql")) else [])],
        "postgres": ["psycopg2"],
        "pyodbc": ["pyodbc"],
        "redis": ["redis"],
        "sendgrid": ["sendgrid"],
        "sftp/ssh": ["paramiko", "sshtunnel"],
        "slack": ["slack_sdk"],
        "statsd": ["statsd"],
        "virtualenv": ["virtualenv"],
    }

    @pytest.mark.skipif(os.environ.get("TEST_SLIM_IMAGE") == "true", reason="Skipped with slim image")
    @pytest.mark.parametrize("package_name,import_names", PACKAGE_IMPORTS.items())
    def test_check_dependencies_imports(self, package_name, import_names):
        run_python_in_docker(f"import {','.join(import_names)}")


class TestExecuteAsRoot:
    def test_execute_airflow_as_root(self):
        run_command(
            [
                "docker",
                "run",
                "--rm",
                "--user",
                "0",
                "-e",
                "PYTHONDONTWRITEBYTECODE=true",
                docker_image,
                "airflow",
                "info",
            ]
        )

    def test_run_custom_python_packages_as_root(self, tmp_path):
        (tmp_path / "__init__.py").write_text("")
        (tmp_path / "awesome.py").write_text('print("Awesome")')

        run_command(
            [
                "docker",
                "run",
                "--rm",
                "-e",
                f"PYTHONPATH={tmp_path}",
                "-e",
                "PYTHONDONTWRITEBYTECODE=true",
                "-v",
                f"{tmp_path}:{tmp_path}",
                "--user",
                "0",
                docker_image,
                "python",
                "-c",
                "import awesome",
            ]
        )
