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
import sys
from enum import Enum

from airflow_breeze.utils.console import get_console

try:
    from functools import cache
except ImportError:
    get_console().print(
        "\n[error]Breeze doesn't support Python version <=3.8\n\n"
        "[warning]Use Python 3.9 and force reinstall breeze with pipx\n\n"
        "     pipx install --force -e ./dev/breeze\n"
        "\nTo find out more, visit [info]https://github.com/apache/airflow/"
        "blob/main/dev/breeze/doc/01_installation.rst#the-pipx-tool[/]\n"
    )
    sys.exit(1)
from pathlib import Path

from airflow_breeze.utils.host_info_utils import Architecture
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT

RUNS_ON_PUBLIC_RUNNER = '["ubuntu-22.04"]'
# we should get more sophisticated logic here in the future, but for now we just check if
# we use self airflow, vm-based, amd hosted runner as a default
# TODO: temporarily we need to switch to public runners to avoid issues with self-hosted runners
RUNS_ON_SELF_HOSTED_RUNNER = '["ubuntu-22.04"]'
RUNS_ON_SELF_HOSTED_ASF_RUNNER = '["self-hosted", "asf-runner"]'
# TODO: when we have it properly set-up with labels we should change it to
# RUNS_ON_SELF_HOSTED_RUNNER = '["self-hosted", "airflow-runner", "vm-runner", "X64"]'
# RUNS_ON_SELF_HOSTED_RUNNER = '["self-hosted", "Linux", "X64"]'
SELF_HOSTED_RUNNERS_CPU_COUNT = 8

ANSWER = ""

APACHE_AIRFLOW_GITHUB_REPOSITORY = "apache/airflow"

# Checked before putting in build cache
ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS = ["3.9", "3.10", "3.11", "3.12"]
DEFAULT_PYTHON_MAJOR_MINOR_VERSION = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
ALLOWED_ARCHITECTURES = [Architecture.X86_64, Architecture.ARM]
# Database Backends used when starting Breeze. The "none" value means that invalid configuration
# Is set and no database started - access to a database will fail.
ALLOWED_BACKENDS = ["sqlite", "mysql", "postgres", "none"]
ALLOWED_PROD_BACKENDS = ["mysql", "postgres"]
DEFAULT_BACKEND = ALLOWED_BACKENDS[0]
CELERY_INTEGRATION = "celery"
TESTABLE_INTEGRATIONS = [
    "cassandra",
    CELERY_INTEGRATION,
    "drill",
    "kafka",
    "kerberos",
    "mongo",
    "mssql",
    "pinot",
    "qdrant",
    "redis",
    "trino",
    "ydb",
]
DISABLE_TESTABLE_INTEGRATIONS_FROM_CI = [
    "mssql",
]
OTHER_INTEGRATIONS = ["statsd", "otel", "openlineage"]
ALLOWED_DEBIAN_VERSIONS = ["bookworm"]
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
        *ALL_INTEGRATIONS,
    ]
)
ALLOWED_TTY = ["auto", "enabled", "disabled"]
ALLOWED_DOCKER_COMPOSE_PROJECTS = ["breeze", "pre-commit", "docker-compose"]

# Unlike everything else, k8s versions are supported as long as 2 major cloud providers support them.
# See:
#   - https://endoflife.date/amazon-eks
#   - https://endoflife.date/azure-kubernetes-service
#   - https://endoflife.date/google-kubernetes-engine
ALLOWED_KUBERNETES_VERSIONS = ["v1.28.13", "v1.29.8", "v1.30.4", "v1.31.0"]

LOCAL_EXECUTOR = "LocalExecutor"
KUBERNETES_EXECUTOR = "KubernetesExecutor"
CELERY_EXECUTOR = "CeleryExecutor"
CELERY_K8S_EXECUTOR = "CeleryKubernetesExecutor"
EDGE_EXECUTOR = "EdgeExecutor"
SEQUENTIAL_EXECUTOR = "SequentialExecutor"
ALLOWED_EXECUTORS = [
    LOCAL_EXECUTOR,
    KUBERNETES_EXECUTOR,
    CELERY_EXECUTOR,
    CELERY_K8S_EXECUTOR,
    EDGE_EXECUTOR,
    SEQUENTIAL_EXECUTOR,
]

DEFAULT_ALLOWED_EXECUTOR = ALLOWED_EXECUTORS[0]
START_AIRFLOW_ALLOWED_EXECUTORS = [LOCAL_EXECUTOR, CELERY_EXECUTOR, EDGE_EXECUTOR, SEQUENTIAL_EXECUTOR]
START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR = START_AIRFLOW_ALLOWED_EXECUTORS[0]
ALLOWED_CELERY_EXECUTORS = [CELERY_EXECUTOR, CELERY_K8S_EXECUTOR]

ALLOWED_KIND_OPERATIONS = ["start", "stop", "restart", "status", "deploy", "test", "shell", "k9s"]
ALLOWED_CONSTRAINTS_MODES_CI = ["constraints-source-providers", "constraints", "constraints-no-providers"]
ALLOWED_CONSTRAINTS_MODES_PROD = ["constraints", "constraints-no-providers", "constraints-source-providers"]

ALLOWED_CELERY_BROKERS = ["rabbitmq", "redis"]
DEFAULT_CELERY_BROKER = ALLOWED_CELERY_BROKERS[1]

MOUNT_SELECTED = "selected"
MOUNT_ALL = "all"
MOUNT_SKIP = "skip"
MOUNT_REMOVE = "remove"
MOUNT_TESTS = "tests"
MOUNT_PROVIDERS_AND_TESTS = "providers-and-tests"

ALLOWED_MOUNT_OPTIONS = [
    MOUNT_SELECTED,
    MOUNT_ALL,
    MOUNT_SKIP,
    MOUNT_REMOVE,
    MOUNT_TESTS,
    MOUNT_PROVIDERS_AND_TESTS,
]

USE_AIRFLOW_MOUNT_SOURCES = [MOUNT_REMOVE, MOUNT_TESTS, MOUNT_PROVIDERS_AND_TESTS]
ALLOWED_POSTGRES_VERSIONS = ["12", "13", "14", "15", "16", "17"]
# Oracle introduced new release model for MySQL
# - LTS: Long Time Support releases, new release approx every 2 year,
#  with 5 year premier and 3 year extended support, no new features/removals during current LTS release.
#  the first LTS release should be in summer/fall 2024.
# - Innovations: Shot living releases with short support cycle - only until next Innovation/LTS release.
# See: https://dev.mysql.com/blog-archive/introducing-mysql-innovation-and-long-term-support-lts-versions/
MYSQL_LTS_RELEASES: list[str] = ["8.4"]
MYSQL_OLD_RELEASES = ["8.0"]
MYSQL_INNOVATION_RELEASE: str | None = None
ALLOWED_MYSQL_VERSIONS = [*MYSQL_OLD_RELEASES, *MYSQL_LTS_RELEASES]
if MYSQL_INNOVATION_RELEASE:
    ALLOWED_MYSQL_VERSIONS.append(MYSQL_INNOVATION_RELEASE)

ALLOWED_INSTALL_MYSQL_CLIENT_TYPES = ["mariadb", "mysql"]

PIP_VERSION = "24.2"

DEFAULT_UV_HTTP_TIMEOUT = 300
DEFAULT_WSL2_HTTP_TIMEOUT = 900

# packages that  providers docs
REGULAR_DOC_PACKAGES = [
    "apache-airflow",
    "docker-stack",
    "helm-chart",
    "apache-airflow-providers",
]


@cache
def all_selective_test_types() -> tuple[str, ...]:
    return tuple(sorted(e.value for e in SelectiveUnitTestTypes))


@cache
def all_selective_test_types_except_providers() -> tuple[str, ...]:
    return tuple(sorted(e.value for e in SelectiveUnitTestTypes if e != SelectiveUnitTestTypes.PROVIDERS))


class SelectiveUnitTestTypes(Enum):
    ALWAYS = "Always"
    API = "API"
    BRANCH_PYTHON_VENV = "BranchPythonVenv"
    EXTERNAL_PYTHON = "ExternalPython"
    EXTERNAL_BRANCH_PYTHON = "BranchExternalPython"
    CLI = "CLI"
    CORE = "Core"
    SERIALIZATION = "Serialization"
    OTHER = "Other"
    OPERATORS = "Operators"
    PLAIN_ASSERTS = "PlainAsserts"
    PROVIDERS = "Providers"
    PYTHON_VENV = "PythonVenv"
    WWW = "WWW"


ALLOWED_TEST_TYPE_CHOICES = [
    "All",
    "Default",
    *all_selective_test_types(),
    "All-Postgres",
    "All-MySQL",
    "All-Quarantined",
]

ALLOWED_PARALLEL_TEST_TYPE_CHOICES = [
    *all_selective_test_types(),
]


@cache
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


@cache
def all_task_sdk_test_packages() -> list[str]:
    return sorted(
        [
            candidate.name
            for candidate in (AIRFLOW_SOURCES_ROOT / "task_sdk" / "tests").iterdir()
            if candidate.is_dir() and candidate.name != "__pycache__"
        ]
    )


ALLOWED_TASK_SDK_TEST_PACKAGES = [
    "all",
    *all_task_sdk_test_packages(),
]

ALLOWED_PACKAGE_FORMATS = ["wheel", "sdist", "both"]
ALLOWED_INSTALLATION_PACKAGE_FORMATS = ["wheel", "sdist"]
ALLOWED_INSTALLATION_METHODS = [".", "apache-airflow"]
ALLOWED_BUILD_CACHE = ["registry", "local", "disabled"]
ALLOWED_BUILD_PROGRESS = ["auto", "plain", "tty"]
MULTI_PLATFORM = "linux/amd64,linux/arm64"
SINGLE_PLATFORMS = ["linux/amd64", "linux/arm64"]
ALLOWED_PLATFORMS = [*SINGLE_PLATFORMS, MULTI_PLATFORM]

ALLOWED_USE_AIRFLOW_VERSIONS = ["none", "wheel", "sdist"]

ALL_HISTORICAL_PYTHON_VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12"]


def get_default_platform_machine() -> str:
    machine = platform.uname().machine.lower()
    # Some additional conversion for various platforms...
    machine = {"x86_64": "amd64"}.get(machine, machine)
    return machine


# Initialise base variables
DOCKER_DEFAULT_PLATFORM = f"linux/{get_default_platform_machine()}"
DOCKER_BUILDKIT = 1

DRILL_HOST_PORT = "28047"
FLOWER_HOST_PORT = "25555"
MSSQL_HOST_PORT = "21433"
MYSQL_HOST_PORT = "23306"
POSTGRES_HOST_PORT = "25433"
RABBITMQ_HOST_PORT = "25672"
REDIS_HOST_PORT = "26379"
SSH_PORT = "12322"
WEBSERVER_HOST_PORT = "28080"
VITE_DEV_PORT = "5173"
FASTAPI_API_HOST_PORT = "29091"

CELERY_BROKER_URLS_MAP = {"rabbitmq": "amqp://guest:guest@rabbitmq:5672", "redis": "redis://redis:6379/0"}
SQLITE_URL = "sqlite:////root/airflow/sqlite/airflow.db"
PYTHONDONTWRITEBYTECODE = True

PRODUCTION_IMAGE = False
# All python versions include all past python versions available in previous branches
# Even if we remove them from the main version. This is needed to make sure we can cherry-pick
# changes from main to the previous branch.
ALL_PYTHON_MAJOR_MINOR_VERSIONS = ["3.9", "3.10", "3.11", "3.12"]
CURRENT_PYTHON_MAJOR_MINOR_VERSIONS = ALL_PYTHON_MAJOR_MINOR_VERSIONS
CURRENT_POSTGRES_VERSIONS = ["12", "13", "14", "15", "16", "17"]
DEFAULT_POSTGRES_VERSION = CURRENT_POSTGRES_VERSIONS[0]
USE_MYSQL_INNOVATION_RELEASE = True
if USE_MYSQL_INNOVATION_RELEASE:
    CURRENT_MYSQL_VERSIONS = ALLOWED_MYSQL_VERSIONS.copy()
else:
    CURRENT_MYSQL_VERSIONS = [*MYSQL_OLD_RELEASES, *MYSQL_LTS_RELEASES]
DEFAULT_MYSQL_VERSION = CURRENT_MYSQL_VERSIONS[0]


AIRFLOW_PYTHON_COMPATIBILITY_MATRIX = {
    "2.0.0": ["3.6", "3.7", "3.8"],
    "2.0.1": ["3.6", "3.7", "3.8"],
    "2.0.2": ["3.6", "3.7", "3.8"],
    "2.1.0": ["3.6", "3.7", "3.8"],
    "2.1.1": ["3.6", "3.7", "3.8"],
    "2.1.2": ["3.6", "3.7", "3.8", "3.9"],
    "2.1.3": ["3.6", "3.7", "3.8", "3.9"],
    "2.1.4": ["3.6", "3.7", "3.8", "3.9"],
    "2.2.0": ["3.6", "3.7", "3.8", "3.9"],
    "2.2.1": ["3.6", "3.7", "3.8", "3.9"],
    "2.2.2": ["3.6", "3.7", "3.8", "3.9"],
    "2.2.3": ["3.6", "3.7", "3.8", "3.9"],
    "2.2.4": ["3.6", "3.7", "3.8", "3.9"],
    "2.2.5": ["3.6", "3.7", "3.8", "3.9"],
    "2.3.0": ["3.7", "3.8", "3.9", "3.10"],
    "2.3.1": ["3.7", "3.8", "3.9", "3.10"],
    "2.3.2": ["3.7", "3.8", "3.9", "3.10"],
    "2.3.3": ["3.7", "3.8", "3.9", "3.10"],
    "2.3.4": ["3.7", "3.8", "3.9", "3.10"],
    "2.4.0": ["3.7", "3.8", "3.9", "3.10"],
    "2.4.1": ["3.7", "3.8", "3.9", "3.10"],
    "2.4.2": ["3.7", "3.8", "3.9", "3.10"],
    "2.4.3": ["3.7", "3.8", "3.9", "3.10"],
    "2.5.0": ["3.7", "3.8", "3.9", "3.10"],
    "2.5.1": ["3.7", "3.8", "3.9", "3.10"],
    "2.5.2": ["3.7", "3.8", "3.9", "3.10"],
    "2.5.3": ["3.7", "3.8", "3.9", "3.10"],
    "2.6.0": ["3.7", "3.8", "3.9", "3.10"],
    "2.6.1": ["3.7", "3.8", "3.9", "3.10"],
    "2.6.2": ["3.7", "3.8", "3.9", "3.10", "3.11"],
    "2.6.3": ["3.7", "3.8", "3.9", "3.10", "3.11"],
    "2.7.0": ["3.8", "3.9", "3.10", "3.11"],
    "2.7.1": ["3.8", "3.9", "3.10", "3.11"],
    "2.7.2": ["3.8", "3.9", "3.10", "3.11"],
    "2.7.3": ["3.8", "3.9", "3.10", "3.11"],
    "2.8.0": ["3.8", "3.9", "3.10", "3.11"],
    "2.8.1": ["3.8", "3.9", "3.10", "3.11"],
    "2.8.2": ["3.8", "3.9", "3.10", "3.11"],
    "2.8.3": ["3.8", "3.9", "3.10", "3.11"],
    "2.9.0": ["3.8", "3.9", "3.10", "3.11", "3.12"],
    "2.9.1": ["3.8", "3.9", "3.10", "3.11", "3.12"],
    "2.9.2": ["3.8", "3.9", "3.10", "3.11", "3.12"],
    "2.9.3": ["3.8", "3.9", "3.10", "3.11", "3.12"],
    "2.10.0": ["3.8", "3.9", "3.10", "3.11", "3.12"],
    "2.10.1": ["3.8", "3.9", "3.10", "3.11", "3.12"],
    "2.10.2": ["3.8", "3.9", "3.10", "3.11", "3.12"],
}

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
    "Lee-W",
    "RNHTTR",
    "Taragolis",
    "XD-DENG",
    "aijamalnk",
    "alexvanboxel",
    "amoghrajesh",
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
    "jscheffl",
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
    "pankajkoti",
    "phanikumv",
    "pierrejeambrun",
    "pingzh",
    "potiuk",
    "r39132",
    "romsharon98",
    "ryanahamilton",
    "ryw",
    "saguziel",
    "sekikn",
    "shahar1",
    "turbaszek",
    "uranusjr",
    "utkarsharma2",
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
        raise RuntimeError("Unable to determine Airflow version")
    return airflow_version


@cache
def get_airflow_extras():
    airflow_dockerfile = AIRFLOW_SOURCES_ROOT / "Dockerfile"
    with open(airflow_dockerfile) as dockerfile:
        for line in dockerfile.readlines():
            if "ARG AIRFLOW_EXTRAS=" in line:
                line = line.split("=")[1].strip()
                return line.replace('"', "")


# Initialize integrations
ALL_PROVIDER_YAML_FILES = Path(AIRFLOW_SOURCES_ROOT, "airflow", "providers").rglob("provider.yaml")
PROVIDER_RUNTIME_DATA_SCHEMA_PATH = AIRFLOW_SOURCES_ROOT / "airflow" / "provider_info.schema.json"

with Path(AIRFLOW_SOURCES_ROOT, "generated", "provider_dependencies.json").open() as f:
    PROVIDER_DEPENDENCIES = json.load(f)

DEVEL_DEPS_PATH = AIRFLOW_SOURCES_ROOT / "generated" / "devel_deps.txt"

# Initialize files for rebuild check
FILES_FOR_REBUILD_CHECK = [
    "pyproject.toml",
    "Dockerfile.ci",
    ".dockerignore",
    "generated/provider_dependencies.json",
    "scripts/docker/common.sh",
    "scripts/docker/install_additional_dependencies.sh",
    "scripts/docker/install_airflow.sh",
    "scripts/docker/install_airflow_dependencies_from_branch_tip.sh",
    "scripts/docker/install_from_docker_context_files.sh",
    "scripts/docker/install_mysql.sh",
]

ENABLED_SYSTEMS = ""

CURRENT_KUBERNETES_VERSIONS = ALLOWED_KUBERNETES_VERSIONS
CURRENT_EXECUTORS = [KUBERNETES_EXECUTOR]

DEFAULT_KUBERNETES_VERSION = CURRENT_KUBERNETES_VERSIONS[0]
DEFAULT_EXECUTOR = CURRENT_EXECUTORS[0]

KIND_VERSION = "v0.24.0"
HELM_VERSION = "v3.15.3"

# Initialize image build variables - Have to check if this has to go to ci dataclass
USE_AIRFLOW_VERSION = None
GITHUB_ACTIONS = ""

ISSUE_ID = ""
NUM_RUNS = ""

MIN_DOCKER_VERSION = "24.0.0"
MIN_DOCKER_COMPOSE_VERSION = "2.20.2"

AIRFLOW_SOURCES_FROM = "."
AIRFLOW_SOURCES_TO = "/opt/airflow"

DEFAULT_EXTRAS = [
    # BEGINNING OF EXTRAS LIST UPDATED BY PRE COMMIT
    "aiobotocore",
    "amazon",
    "async",
    "celery",
    "cncf-kubernetes",
    "common-io",
    "docker",
    "elasticsearch",
    "fab",
    "ftp",
    "google",
    "google-auth",
    "graphviz",
    "grpc",
    "hashicorp",
    "http",
    "ldap",
    "microsoft-azure",
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
    "uv",
    "virtualenv",
    # END OF EXTRAS LIST UPDATED BY PRE COMMIT
]

CHICKEN_EGG_PROVIDERS = " ".join(["standard amazon"])


BASE_PROVIDERS_COMPATIBILITY_CHECKS: list[dict[str, str | list[str]]] = [
    {
        "python-version": "3.9",
        "airflow-version": "2.8.4",
        "remove-providers": "cloudant fab edge standard",
        "run-tests": "true",
    },
    {
        "python-version": "3.9",
        "airflow-version": "2.9.3",
        "remove-providers": "cloudant edge standard",
        "run-tests": "true",
    },
    {
        "python-version": "3.9",
        "airflow-version": "2.10.1",
        "remove-providers": "cloudant",
        "run-tests": "true",
    },
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


@cache
def github_events() -> list[str]:
    return [e.value for e in GithubEvents]
