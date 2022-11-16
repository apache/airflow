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
from pathlib import Path

"""
Global constants that are used by all other Breeze components.
"""
import platform
from enum import Enum
from functools import lru_cache

from airflow_breeze.utils.host_info_utils import Architecture
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT

RUNS_ON_PUBLIC_RUNNER = "ubuntu-20.04"
RUNS_ON_SELF_HOSTED_RUNNER = "self-hosted"

ANSWER = ""

APACHE_AIRFLOW_GITHUB_REPOSITORY = "apache/airflow"

# Checked before putting in build cache
ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS = ["3.7", "3.8", "3.9", "3.10"]
DEFAULT_PYTHON_MAJOR_MINOR_VERSION = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
ALLOWED_ARCHITECTURES = [Architecture.X86_64, Architecture.ARM]
ALLOWED_BACKENDS = ["sqlite", "mysql", "postgres", "mssql"]
ALLOWED_PROD_BACKENDS = ["mysql", "postgres", "mssql"]
DEFAULT_BACKEND = ALLOWED_BACKENDS[0]
ALL_INTEGRATIONS = [
    "cassandra",
    "kerberos",
    "mongo",
    "openldap",
    "pinot",
    "rabbitmq",
    "redis",
    "statsd",
    "trino",
]
ALLOWED_INTEGRATIONS = [
    *ALL_INTEGRATIONS,
    "all",
]
ALLOWED_KUBERNETES_VERSIONS = ["v1.25.3", "v1.24.7", "v1.23.13", "v1.22.15", "v1.21.14"]
ALLOWED_EXECUTORS = ["KubernetesExecutor", "CeleryExecutor", "LocalExecutor", "CeleryKubernetesExecutor"]
ALLOWED_KIND_OPERATIONS = ["start", "stop", "restart", "status", "deploy", "test", "shell", "k9s"]
ALLOWED_CONSTRAINTS_MODES_CI = ["constraints-source-providers", "constraints", "constraints-no-providers"]
ALLOWED_CONSTRAINTS_MODES_PROD = ["constraints", "constraints-no-providers", "constraints-source-providers"]

MOUNT_SELECTED = "selected"
MOUNT_ALL = "all"
MOUNT_SKIP = "skip"
MOUNT_REMOVE = "remove"

ALLOWED_MOUNT_OPTIONS = [MOUNT_SELECTED, MOUNT_ALL, MOUNT_SKIP, MOUNT_REMOVE]
ALLOWED_POSTGRES_VERSIONS = ["11", "12", "13", "14"]
ALLOWED_MYSQL_VERSIONS = ["5.7", "8"]
ALLOWED_MSSQL_VERSIONS = ["2017-latest", "2019-latest"]

PIP_VERSION = "22.3.1"


@lru_cache(maxsize=None)
def all_selective_test_types() -> tuple[str, ...]:
    return tuple(sorted(e.value for e in SelectiveUnitTestTypes))


class SelectiveUnitTestTypes(Enum):
    ALWAYS = "Always"
    API = "API"
    CLI = "CLI"
    CORE = "Core"
    OTHER = "Other"
    INTEGRATION = "Integration"
    PROVIDERS = "Providers"
    WWW = "WWW"


ALLOWED_TEST_TYPE_CHOICES = [
    "All",
    *all_selective_test_types(),
    "Helm",
    "Postgres",
    "MySQL",
    "Quarantine",
]

ALLOWED_PACKAGE_FORMATS = ["wheel", "sdist", "both"]
ALLOWED_INSTALLATION_PACKAGE_FORMATS = ["wheel", "sdist"]
ALLOWED_INSTALLATION_METHODS = [".", "apache-airflow"]
ALLOWED_BUILD_CACHE = ["registry", "local", "disabled"]
MULTI_PLATFORM = "linux/amd64,linux/arm64"
SINGLE_PLATFORMS = ["linux/amd64", "linux/arm64"]
ALLOWED_PLATFORMS = [*SINGLE_PLATFORMS, MULTI_PLATFORM]
ALLOWED_USE_AIRFLOW_VERSIONS = ["none", "wheel", "sdist"]

PROVIDER_PACKAGE_JSON_FILE = AIRFLOW_SOURCES_ROOT / "generated" / "provider_dependencies.json"


def get_available_documentation_packages(short_version=False) -> list[str]:
    provider_names: list[str] = list(json.loads(PROVIDER_PACKAGE_JSON_FILE.read_text()).keys())
    doc_provider_names = [provider_name.replace(".", "-") for provider_name in provider_names]
    available_packages = [f"apache-airflow-providers-{doc_provider}" for doc_provider in doc_provider_names]
    available_packages.extend(["apache-airflow", "docker-stack", "helm-chart"])
    available_packages.sort()
    if short_version:
        prefix_len = len("apache-airflow-providers-")
        available_packages = [
            package[prefix_len:].replace("-", ".")
            for package in available_packages
            if len(package) > prefix_len
        ]
    return available_packages


def get_default_platform_machine() -> str:
    machine = platform.uname().machine
    # Some additional conversion for various platforms...
    machine = {"AMD64": "x86_64"}.get(machine, machine)
    return machine


# Initialise base variables
DOCKER_DEFAULT_PLATFORM = f"linux/{get_default_platform_machine()}"
DOCKER_BUILDKIT = 1

SSH_PORT = "12322"
WEBSERVER_HOST_PORT = "28080"
POSTGRES_HOST_PORT = "25433"
MYSQL_HOST_PORT = "23306"
MSSQL_HOST_PORT = "21433"
FLOWER_HOST_PORT = "25555"
REDIS_HOST_PORT = "26379"

SQLITE_URL = "sqlite:////root/airflow/airflow.db"
PYTHONDONTWRITEBYTECODE = True

PRODUCTION_IMAGE = False
ALL_PYTHON_MAJOR_MINOR_VERSIONS = ["3.7", "3.8", "3.9", "3.10"]
CURRENT_PYTHON_MAJOR_MINOR_VERSIONS = ALL_PYTHON_MAJOR_MINOR_VERSIONS
CURRENT_POSTGRES_VERSIONS = ["11", "12", "13", "14"]
DEFAULT_POSTGRES_VERSION = CURRENT_POSTGRES_VERSIONS[0]
CURRENT_MYSQL_VERSIONS = ["5.7", "8"]
DEFAULT_MYSQL_VERSION = CURRENT_MYSQL_VERSIONS[0]
CURRENT_MSSQL_VERSIONS = ["2017-latest", "2019-latest"]
DEFAULT_MSSQL_VERSION = CURRENT_MSSQL_VERSIONS[0]

DB_RESET = False
START_AIRFLOW = "false"
LOAD_EXAMPLES = False
LOAD_DEFAULT_CONNECTIONS = False
PRESERVE_VOLUMES = False
CLEANUP_CONTEXT = False
INIT_SCRIPT_FILE = ""
BREEZE_INIT_COMMAND = ""
DRY_RUN_DOCKER = False
INSTALL_AIRFLOW_VERSION = ""
SQLITE_URL = "sqlite:////root/airflow/airflow.db"


def get_airflow_version():
    airflow_setup_file = AIRFLOW_SOURCES_ROOT / "setup.py"
    with open(airflow_setup_file) as setup_file:
        for line in setup_file.readlines():
            if "version =" in line:
                return line.split()[2][1:-1]


def get_airflow_extras():
    airflow_dockerfile = AIRFLOW_SOURCES_ROOT / "Dockerfile"
    with open(airflow_dockerfile) as dockerfile:
        for line in dockerfile.readlines():
            if "ARG AIRFLOW_EXTRAS=" in line:
                line = line.split("=")[1].strip()
                return line.replace('"', "")


# Initialize integrations
AVAILABLE_INTEGRATIONS = [
    "cassandra",
    "kerberos",
    "mongo",
    "openldap",
    "pinot",
    "rabbitmq",
    "redis",
    "statsd",
    "trino",
]
ENABLED_INTEGRATIONS = ""
ALL_PROVIDER_YAML_FILES = Path(AIRFLOW_SOURCES_ROOT).glob("airflow/providers/**/provider.yaml")
# Initialize files for rebuild check
FILES_FOR_REBUILD_CHECK = [
    "setup.py",
    "setup.cfg",
    "Dockerfile.ci",
    ".dockerignore",
    "scripts/docker/common.sh",
    "scripts/docker/install_additional_dependencies.sh",
    "scripts/docker/install_airflow.sh",
    "scripts/docker/install_airflow_dependencies_from_branch_tip.sh",
    "scripts/docker/install_from_docker_context_files.sh",
    "scripts/docker/install_mysql.sh",
    *ALL_PROVIDER_YAML_FILES,
]

ENABLED_SYSTEMS = ""

CURRENT_KUBERNETES_VERSIONS = ALLOWED_KUBERNETES_VERSIONS
CURRENT_EXECUTORS = ["KubernetesExecutor"]

DEFAULT_KUBERNETES_VERSION = CURRENT_KUBERNETES_VERSIONS[0]
DEFAULT_EXECUTOR = CURRENT_EXECUTORS[0]

KIND_VERSION = "v0.17.0"
HELM_VERSION = "v3.9.4"

# Initialize image build variables - Have to check if this has to go to ci dataclass
USE_AIRFLOW_VERSION = None
GITHUB_ACTIONS = ""

ISSUE_ID = ""
NUM_RUNS = ""

MIN_DOCKER_VERSION = "20.10.0"
MIN_DOCKER_COMPOSE_VERSION = "1.29.0"

AIRFLOW_SOURCES_FROM = "."
AIRFLOW_SOURCES_TO = "/opt/airflow"

DEFAULT_EXTRAS = [
    # BEGINNING OF EXTRAS LIST UPDATED BY PRE COMMIT
    "amazon",
    "async",
    "celery",
    "cncf.kubernetes",
    "dask",
    "docker",
    "elasticsearch",
    "ftp",
    "google",
    "google_auth",
    "grpc",
    "hashicorp",
    "http",
    "ldap",
    "microsoft.azure",
    "mysql",
    "odbc",
    "pandas",
    "postgres",
    "redis",
    "sendgrid",
    "sftp",
    "slack",
    "ssh",
    "statsd",
    "virtualenv",
    # END OF EXTRAS LIST UPDATED BY PRE COMMIT
]


class GithubEvents(Enum):
    PULL_REQUEST = "pull_request"
    PULL_REQUEST_REVIEW = "pull_request_review"
    PULL_REQUEST_TARGET = "pull_request_target"
    PULL_REQUEST_WORKFLOW = "pull_request_workflow"
    PUSH = "push"
    SCHEDULE = "schedule"
    WORKFLOW_DISPATCH = "workflow_dispatch"
    WORKFLOW_RUN = "workflow_run"


@lru_cache(maxsize=None)
def github_events() -> list[str]:
    return [e.value for e in GithubEvents]
