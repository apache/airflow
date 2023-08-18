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
Global constants that are used by all other Breeze components.
"""
from __future__ import annotations

import json
import platform
from enum import Enum
from functools import lru_cache
from pathlib import Path

from airflow_breeze.utils.host_info_utils import Architecture
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, PROVIDER_DEPENDENCIES_JSON_FILE_PATH

RUNS_ON_PUBLIC_RUNNER = "ubuntu-22.04"
RUNS_ON_SELF_HOSTED_RUNNER = "self-hosted"
SELF_HOSTED_RUNNERS_CPU_COUNT = 8

ANSWER = ""

APACHE_AIRFLOW_GITHUB_REPOSITORY = "apache/airflow"

# Checked before putting in build cache
ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS = ["3.8", "3.9", "3.10", "3.11"]
DEFAULT_PYTHON_MAJOR_MINOR_VERSION = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
ALLOWED_ARCHITECTURES = [Architecture.X86_64, Architecture.ARM]
ALLOWED_BACKENDS = ["sqlite", "mysql", "postgres", "mssql"]
ALLOWED_PROD_BACKENDS = ["mysql", "postgres", "mssql"]
DEFAULT_BACKEND = ALLOWED_BACKENDS[0]
TESTABLE_INTEGRATIONS = ["cassandra", "celery", "kerberos", "mongo", "pinot", "trino", "kafka"]
OTHER_INTEGRATIONS = ["statsd"]
ALL_INTEGRATIONS = sorted(
    [
        *TESTABLE_INTEGRATIONS,
        *OTHER_INTEGRATIONS,
    ]
)
AUTOCOMPLETE_INTEGRATIONS = sorted(
    [
        "all-testable",
        "all",
        "otel",
        "statsd",
        *ALL_INTEGRATIONS,
    ]
)

# Unlike everything else, k8s versions are supported as long as 2 major cloud providers support them.
# See:
#   - https://endoflife.date/amazon-eks
#   - https://endoflife.date/azure-kubernetes-service
#   - https://endoflife.date/google-kubernetes-engine
ALLOWED_KUBERNETES_VERSIONS = ["v1.24.15", "v1.25.11", "v1.26.6", "v1.27.3"]
ALLOWED_EXECUTORS = ["KubernetesExecutor", "CeleryExecutor", "LocalExecutor", "CeleryKubernetesExecutor"]
START_AIRFLOW_ALLOWED_EXECUTORS = ["CeleryExecutor", "LocalExecutor"]
START_AIRFLOW_DEFAULT_ALLOWED_EXECUTORS = START_AIRFLOW_ALLOWED_EXECUTORS[1]
ALLOWED_KIND_OPERATIONS = ["start", "stop", "restart", "status", "deploy", "test", "shell", "k9s"]
ALLOWED_CONSTRAINTS_MODES_CI = ["constraints-source-providers", "constraints", "constraints-no-providers"]
ALLOWED_CONSTRAINTS_MODES_PROD = ["constraints", "constraints-no-providers", "constraints-source-providers"]

ALLOWED_CELERY_BROKERS = ["rabbitmq", "redis"]
DEFAULT_CELERY_BROKER = ALLOWED_CELERY_BROKERS[1]

MOUNT_SELECTED = "selected"
MOUNT_ALL = "all"
MOUNT_SKIP = "skip"
MOUNT_REMOVE = "remove"

ALLOWED_MOUNT_OPTIONS = [MOUNT_SELECTED, MOUNT_ALL, MOUNT_SKIP, MOUNT_REMOVE]
ALLOWED_POSTGRES_VERSIONS = ["11", "12", "13", "14", "15"]
ALLOWED_MYSQL_VERSIONS = ["5.7", "8"]
ALLOWED_MSSQL_VERSIONS = ["2017-latest", "2019-latest"]

PIP_VERSION = "23.2.1"


@lru_cache(maxsize=None)
def all_selective_test_types() -> tuple[str, ...]:
    return tuple(sorted(e.value for e in SelectiveUnitTestTypes))


class SelectiveUnitTestTypes(Enum):
    ALWAYS = "Always"
    API = "API"
    CLI = "CLI"
    CORE = "Core"
    OTHER = "Other"
    PROVIDERS = "Providers"
    WWW = "WWW"


ALLOWED_TEST_TYPE_CHOICES = [
    "All",
    *all_selective_test_types(),
    "PlainAsserts",
    "Postgres",
    "MySQL",
    "Quarantine",
]


@lru_cache(maxsize=None)
def all_helm_test_packages() -> list[str]:
    return sorted(
        [
            candidate.name
            for candidate in (AIRFLOW_SOURCES_ROOT / "helm_tests").iterdir()
            if candidate.is_dir() and candidate.name != "__pycache__"
        ]
    )


ALLOWED_HELM_TEST_PACKAGES = [
    "all",
    *all_helm_test_packages(),
]

ALLOWED_PACKAGE_FORMATS = ["wheel", "sdist", "both"]
ALLOWED_INSTALLATION_PACKAGE_FORMATS = ["wheel", "sdist"]
ALLOWED_INSTALLATION_METHODS = [".", "apache-airflow"]
ALLOWED_BUILD_CACHE = ["registry", "local", "disabled"]
MULTI_PLATFORM = "linux/amd64,linux/arm64"
SINGLE_PLATFORMS = ["linux/amd64", "linux/arm64"]
ALLOWED_PLATFORMS = [*SINGLE_PLATFORMS, MULTI_PLATFORM]

ALLOWED_USE_AIRFLOW_VERSIONS = ["none", "wheel", "sdist"]


ALL_HISTORICAL_PYTHON_VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11"]


def get_available_documentation_packages(short_version=False, only_providers: bool = False) -> list[str]:
    provider_names: list[str] = list(json.loads(PROVIDER_DEPENDENCIES_JSON_FILE_PATH.read_text()).keys())
    doc_provider_names = [provider_name.replace(".", "-") for provider_name in provider_names]
    available_packages = []
    if not only_providers:
        available_packages.extend(["apache-airflow", "docker-stack", "helm-chart"])
    all_providers = [f"apache-airflow-providers-{doc_provider}" for doc_provider in doc_provider_names]
    all_providers.sort()
    available_packages.extend(all_providers)
    if short_version:
        prefix_len = len("apache-airflow-providers-")
        available_packages = [
            package[prefix_len:].replace("-", ".") if len(package) > prefix_len else package
            for package in available_packages
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

SQLITE_URL = "sqlite:////root/airflow/sqlite/airflow.db"
PYTHONDONTWRITEBYTECODE = True

PRODUCTION_IMAGE = False
ALL_PYTHON_MAJOR_MINOR_VERSIONS = ["3.8", "3.9", "3.10", "3.11"]
CURRENT_PYTHON_MAJOR_MINOR_VERSIONS = ALL_PYTHON_MAJOR_MINOR_VERSIONS
CURRENT_POSTGRES_VERSIONS = ["11", "12", "13", "14", "15"]
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

COMMITTERS = [
    "BasPH",
    "Fokko",
    "KevinYang21",
    "Taragolis",
    "XD-DENG",
    "aijamalnk",
    "alexvanboxel",
    "aoen",
    "artwr",
    "ashb",
    "bbovenzi",
    "bolkedebruin",
    "criccomini",
    "dimberman",
    "dstandish",
    "eladkal",
    "ephraimbuddy",
    "feluelle",
    "feng-tao",
    "ferruzzi",
    "houqp",
    "hussein-awala",
    "jedcunningham",
    "jgao54",
    "jghoman",
    "jhtimmins",
    "jmcarp",
    "josh-fell",
    "kaxil",
    "leahecole",
    "malthe",
    "mik-laj",
    "milton0825",
    "mistercrunch",
    "mobuchowski",
    "msumit",
    "o-nikolas",
    "pankajastro",
    "phanikumv",
    "pierrejeambrun",
    "pingzh",
    "potiuk",
    "r39132",
    "ryanahamilton",
    "ryw",
    "saguziel",
    "sekikn",
    "turbaszek",
    "uranusjr",
    "vikramkoka",
    "vincbeck",
    "xinbinhuang",
    "yuqian90",
    "zhongjiajie",
]


def get_airflow_version():
    airflow_init_py_file = AIRFLOW_SOURCES_ROOT / "airflow" / "__init__.py"
    airflow_version = "unknown"
    with open(airflow_init_py_file) as init_file:
        while line := init_file.readline():
            if "__version__ = " in line:
                airflow_version = line.split()[2][1:-1]
                break
    if airflow_version == "unknown":
        raise Exception("Unable to determine Airflow version")
    return airflow_version


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
    "pinot",
    "celery",
    "statsd",
    "trino",
]
ALL_PROVIDER_YAML_FILES = Path(AIRFLOW_SOURCES_ROOT).glob("airflow/providers/**/provider.yaml")

with Path(AIRFLOW_SOURCES_ROOT, "generated", "provider_dependencies.json").open() as f:
    PROVIDER_DEPENDENCIES = json.load(f)

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

KIND_VERSION = "v0.20.0"
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
    "aiobotocore",
    "amazon",
    "async",
    "celery",
    "cncf.kubernetes",
    "daskexecutor",
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
    "openlineage",
    "pandas",
    "postgres",
    "redis",
    "sendgrid",
    "sftp",
    "slack",
    "snowflake",
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
