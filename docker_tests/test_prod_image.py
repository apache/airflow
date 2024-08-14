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
import sys
from importlib.util import find_spec
from pathlib import Path

import pytest
from python_on_whales import DockerException

from docker_tests.constants import SOURCE_ROOT
from docker_tests.docker_utils import (
    display_dependency_conflict_message,
    run_airflow_cmd_in_docker,
    run_bash_in_docker,
    run_cmd_in_docker,
    run_python_in_docker,
)

PROD_IMAGE_PROVIDERS_FILE_PATH = SOURCE_ROOT / "prod_image_installed_providers.txt"
AIRFLOW_ROOT_PATH = Path(__file__).parents[2].resolve()

sys.path.insert(0, AIRFLOW_ROOT_PATH.as_posix())  # make sure airflow root is imported
from hatch_build import PRE_INSTALLED_PROVIDERS  # noqa: E402

SLIM_IMAGE_PROVIDERS = [
    f"apache-airflow-providers-{provider_id.split('>=')[0].replace('.','-')}"
    for provider_id in PRE_INSTALLED_PROVIDERS
    if not provider_id.startswith("#")
]
REGULAR_IMAGE_PROVIDERS = [
    f"apache-airflow-providers-{provider_id.split('>=')[0].replace('.','-')}"
    for provider_id in PROD_IMAGE_PROVIDERS_FILE_PATH.read_text().splitlines()
    if not provider_id.startswith("#")
]


class TestCommands:
    def test_without_command(self, default_docker_image):
        """Checking the image without a command. It should return non-zero exit code."""
        with pytest.raises(DockerException) as ctx:
            run_cmd_in_docker(image=default_docker_image)
        assert 2 == ctx.value.return_code

    def test_airflow_command(self, default_docker_image):
        """Checking 'airflow' command. It should return non-zero exit code."""
        with pytest.raises(DockerException) as ctx:
            run_airflow_cmd_in_docker(image=default_docker_image)
        assert 2 == ctx.value.return_code

    def test_airflow_version(self, default_docker_image):
        """Checking 'airflow version' command. It should return zero exit code."""
        output = run_airflow_cmd_in_docker(["version"], image=default_docker_image)
        assert "3." in output

    def test_python_version(self, default_docker_image):
        """Checking 'python --version' command. It should return zero exit code."""
        output = run_cmd_in_docker(cmd=["python", "--version"], image=default_docker_image)
        assert "Python 3." in output

    def test_bash_version(self, default_docker_image):
        """Checking 'bash --version' command  It should return zero exit code."""
        output = run_cmd_in_docker(cmd=["bash", "--version"], image=default_docker_image)
        assert "GNU bash," in output


class TestPythonPackages:
    def test_required_providers_are_installed(self, default_docker_image):
        if os.environ.get("TEST_SLIM_IMAGE"):
            packages_to_install = set(SLIM_IMAGE_PROVIDERS)
            package_file = "hatch_build.py"
        else:
            packages_to_install = set(REGULAR_IMAGE_PROVIDERS)
            package_file = PROD_IMAGE_PROVIDERS_FILE_PATH
        assert len(packages_to_install) != 0
        output = run_bash_in_docker("airflow providers list --output json", image=default_docker_image)
        providers = json.loads(output)
        packages_installed = set(d["package_name"] for d in providers)
        assert len(packages_installed) != 0

        assert (
            packages_to_install == packages_installed
        ), f"List of expected installed packages and image content mismatch. Check {package_file} file."

    def test_pip_dependencies_conflict(self, default_docker_image):
        try:
            run_bash_in_docker("pip check", image=default_docker_image)
        except DockerException:
            display_dependency_conflict_message()
            raise

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
    def test_check_dependencies_imports(self, package_name, import_names, default_docker_image):
        run_python_in_docker(f"import {','.join(import_names)}", image=default_docker_image)

    def test_there_is_no_opt_airflow_airflow_folder(self, default_docker_image):
        output = run_bash_in_docker(
            "find /opt/airflow/airflow/ 2>/dev/null | wc -l", image=default_docker_image
        )
        assert output == "0"


class TestExecuteAsRoot:
    def test_execute_airflow_as_root(self, default_docker_image):
        run_cmd_in_docker(
            cmd=["airflow", "info"],
            user=0,
            envs={"PYTHONDONTWRITEBYTECODE": "true"},
            image=default_docker_image,
        )

    def test_run_custom_python_packages_as_root(self, tmp_path, default_docker_image):
        (tmp_path / "__init__.py").write_text("")
        (tmp_path / "awesome.py").write_text('print("Awesome")')

        output = run_cmd_in_docker(
            envs={"PYTHONPATH": "/custom/mount", "PYTHONDONTWRITEBYTECODE": "true"},
            volumes=[(tmp_path.as_posix(), "/custom/mount")],
            user=0,
            cmd=["python", "-c", "import awesome"],
            image=default_docker_image,
        )
        assert output.strip() == "Awesome"
