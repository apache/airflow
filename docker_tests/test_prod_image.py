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
import tempfile
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
from setup import PREINSTALLED_PROVIDERS

INSTALLED_PROVIDER_PATH = SOURCE_ROOT / "scripts" / "ci" / "installed_providers.txt"


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
            lines = PREINSTALLED_PROVIDERS
        else:
            lines = (d.strip() for d in INSTALLED_PROVIDER_PATH.read_text().splitlines())
            lines = (d for d in lines)
        packages_to_install = {f"apache-airflow-providers-{d.replace('.', '-')}" for d in lines}
        assert len(packages_to_install) != 0

        output = run_bash_in_docker(
            "airflow providers list --output json", stderr=subprocess.DEVNULL, return_output=True
        )
        providers = json.loads(output)
        packages_installed = {d["package_name"] for d in providers}
        assert len(packages_installed) != 0

        assert packages_to_install == packages_installed, (
            f"List of expected installed packages and image content mismatch. "
            f"Check {INSTALLED_PROVIDER_PATH} file."
        )

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
        "dask": ["cloudpickle", "distributed"],
        "docker": ["docker"],
        "elasticsearch": ["elasticsearch", "es.elastic", "elasticsearch_dsl"],
        "google": [
            "OpenSSL",
            "google.ads",
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
        "mysql": ["mysql"],
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

    def test_run_custom_python_packages_as_root(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            (Path(tmp_dir) / "__init__.py").write_text("")
            (Path(tmp_dir) / "awesome.py").write_text('print("Awesome")')

            run_command(
                [
                    "docker",
                    "run",
                    "--rm",
                    "-e",
                    f"PYTHONPATH={tmp_dir}",
                    "-e",
                    "PYTHONDONTWRITEBYTECODE=true",
                    "-v",
                    f"{tmp_dir}:{tmp_dir}",
                    "--user",
                    "0",
                    docker_image,
                    "python",
                    "-c",
                    "import awesome",
                ]
            )
