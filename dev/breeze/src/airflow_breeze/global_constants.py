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

import platform
from enum import Enum
from pathlib import Path

from airflow_breeze.utils.functools_cache import clearable_cache
from airflow_breeze.utils.host_info_utils import Architecture
from airflow_breeze.utils.path_utils import (
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_CTL_SOURCES_PATH,
    AIRFLOW_ROOT_PATH,
    AIRFLOW_TASK_SDK_SOURCES_PATH,
)

PUBLIC_AMD_RUNNERS = '["ubuntu-22.04"]'
PUBLIC_ARM_RUNNERS = '["ubuntu-22.04-arm"]'

# The runner type cross-mapping is intentional — if the previous scheduled build used AMD, the current scheduled build should run with ARM.
RUNNERS_TYPE_CROSS_MAPPING = {
    "ubuntu-22.04": '["ubuntu-22.04-arm"]',
    "ubuntu-22.04-arm": '["ubuntu-22.04"]',
}

ANSWER = ""

APACHE_AIRFLOW_GITHUB_REPOSITORY = "apache/airflow"

# Checked before putting in build cache
ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS = ["3.10", "3.11", "3.12", "3.13"]
DEFAULT_PYTHON_MAJOR_MINOR_VERSION = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
# We set 3.12 as default image version until FAB supports Python 3.13
DEFAULT_PYTHON_MAJOR_MINOR_VERSION_FOR_IMAGES = "3.12"


# Maps each supported Python version to the minimum Airflow version that supports it.
# Used to filter Airflow versions incompatible with a given Python runtime.
PYTHON_TO_MIN_AIRFLOW_MAPPING = {"3.10": "v3.10.18"}

ALLOWED_ARCHITECTURES = [Architecture.X86_64, Architecture.ARM]
# Database Backends used when starting Breeze. The "none" value means that the configuration is invalid.
# No database will be started - access to a database will fail.
SQLITE_BACKEND = "sqlite"
MYSQL_BACKEND = "mysql"
POSTGRES_BACKEND = "postgres"
NONE_BACKEND = "none"
ALLOWED_BACKENDS = [SQLITE_BACKEND, MYSQL_BACKEND, POSTGRES_BACKEND, NONE_BACKEND]
ALLOWED_PROD_BACKENDS = [MYSQL_BACKEND, POSTGRES_BACKEND]
DEFAULT_BACKEND = ALLOWED_BACKENDS[0]
TESTABLE_CORE_INTEGRATIONS = ["kerberos", "redis"]
TESTABLE_PROVIDERS_INTEGRATIONS = [
    "celery",
    "cassandra",
    "drill",
    "tinkerpop",
    "kafka",
    "localstack",
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
    "localstack",  # just for local integration testing for now
]
DISABLE_TESTABLE_INTEGRATIONS_FROM_ARM = [
    "kerberos",
    "drill",
    "tinkerpop",
    "pinot",
    "trino",
    "ydb",
]
KEYCLOAK_INTEGRATION = "keycloak"
STATSD_INTEGRATION = "statsd"
OTEL_INTEGRATION = "otel"
OPENLINEAGE_INTEGRATION = "openlineage"
OTHER_CORE_INTEGRATIONS = [STATSD_INTEGRATION, OTEL_INTEGRATION, KEYCLOAK_INTEGRATION]
OTHER_PROVIDERS_INTEGRATIONS = [OPENLINEAGE_INTEGRATION]
ALLOWED_DEBIAN_VERSIONS = ["bookworm"]
ALL_CORE_INTEGRATIONS = sorted(
    [
        *TESTABLE_CORE_INTEGRATIONS,
        *OTHER_CORE_INTEGRATIONS,
    ]
)
ALL_PROVIDERS_INTEGRATIONS = sorted(
    [
        *TESTABLE_PROVIDERS_INTEGRATIONS,
        *OTHER_PROVIDERS_INTEGRATIONS,
    ]
)
AUTOCOMPLETE_CORE_INTEGRATIONS = sorted(
    [
        "all-testable",
        "all",
        *ALL_CORE_INTEGRATIONS,
    ]
)
AUTOCOMPLETE_PROVIDERS_INTEGRATIONS = sorted(
    [
        "all-testable",
        "all",
        *ALL_PROVIDERS_INTEGRATIONS,
    ]
)
AUTOCOMPLETE_ALL_INTEGRATIONS = sorted(
    [
        "all-testable",
        "all",
        *ALL_CORE_INTEGRATIONS,
        *ALL_PROVIDERS_INTEGRATIONS,
    ]
)
ALLOWED_TTY = ["auto", "enabled", "disabled"]
ALLOWED_DOCKER_COMPOSE_PROJECTS = ["breeze", "prek", "docker-compose"]

# Unlike everything else, k8s versions are supported as long as 2 major cloud providers support them.
# See:
#   - https://endoflife.date/amazon-eks
#   - https://endoflife.date/azure-kubernetes-service
#   - https://endoflife.date/google-kubernetes-engine
ALLOWED_KUBERNETES_VERSIONS = ["v1.30.10", "v1.31.6", "v1.32.3", "v1.33.0"]

LOCAL_EXECUTOR = "LocalExecutor"
KUBERNETES_EXECUTOR = "KubernetesExecutor"
CELERY_EXECUTOR = "CeleryExecutor"
CELERY_K8S_EXECUTOR = "CeleryKubernetesExecutor"
EDGE_EXECUTOR = "EdgeExecutor"
ALLOWED_EXECUTORS = [
    LOCAL_EXECUTOR,
    KUBERNETES_EXECUTOR,
    CELERY_EXECUTOR,
    CELERY_K8S_EXECUTOR,
    EDGE_EXECUTOR,
]

SIMPLE_AUTH_MANAGER = "SimpleAuthManager"
FAB_AUTH_MANAGER = "FabAuthManager"

DEFAULT_ALLOWED_EXECUTOR = ALLOWED_EXECUTORS[0]
ALLOWED_AUTH_MANAGERS = [SIMPLE_AUTH_MANAGER, FAB_AUTH_MANAGER]
START_AIRFLOW_ALLOWED_EXECUTORS = [LOCAL_EXECUTOR, CELERY_EXECUTOR, EDGE_EXECUTOR]
START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR = START_AIRFLOW_ALLOWED_EXECUTORS[0]
ALLOWED_CELERY_EXECUTORS = [CELERY_EXECUTOR, CELERY_K8S_EXECUTOR]

CONSTRAINTS_SOURCE_PROVIDERS = "constraints-source-providers"
CONSTRAINTS = "constraints"
CONSTRAINTS_NO_PROVIDERS = "constraints-no-providers"

ALLOWED_KIND_OPERATIONS = ["start", "stop", "restart", "status", "deploy", "test", "shell", "k9s"]
ALLOWED_CONSTRAINTS_MODES_CI = [CONSTRAINTS_SOURCE_PROVIDERS, CONSTRAINTS, CONSTRAINTS_NO_PROVIDERS]
ALLOWED_CONSTRAINTS_MODES_PROD = [CONSTRAINTS, CONSTRAINTS_NO_PROVIDERS, CONSTRAINTS_SOURCE_PROVIDERS]

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
ALLOWED_POSTGRES_VERSIONS = ["13", "14", "15", "16", "17"]
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

PIP_VERSION = "25.3"
UV_VERSION = "0.9.13"

DEFAULT_UV_HTTP_TIMEOUT = 300
DEFAULT_WSL2_HTTP_TIMEOUT = 900

# packages that providers docs
REGULAR_DOC_PACKAGES = [
    "apache-airflow",
    "docker-stack",
    "helm-chart",
    "apache-airflow-providers",
    "task-sdk",
    "apache-airflow-ctl",
]


# Type of the tarball to build
class TarBallType(Enum):
    AIRFLOW = "apache_airflow"
    PROVIDERS = "apache_airflow_providers"
    TASK_SDK = "apache_airflow_task_sdk"
    AIRFLOW_CTL = "apache_airflow_ctl"
    PYTHON_CLIENT = "apache_airflow_python_client"
    HELM_CHART = "helm-chart"


DESTINATION_LOCATIONS = [
    "s3://live-docs-airflow-apache-org/docs/",
    "s3://staging-docs-airflow-apache-org/docs/",
]

PACKAGES_METADATA_EXCLUDE_NAMES = ["docker-stack", "apache-airflow-providers"]


@clearable_cache
def all_selective_core_test_types() -> tuple[str, ...]:
    return tuple(sorted(e.value for e in SelectiveCoreTestType))


@clearable_cache
def providers_test_type() -> tuple[str, ...]:
    return tuple(sorted(e.value for e in SelectiveProvidersTestType))


class SelectiveTestType(Enum):
    pass


class SelectiveCoreTestType(SelectiveTestType):
    ALWAYS = "Always"
    API = "API"
    CLI = "CLI"
    CORE = "Core"
    SERIALIZATION = "Serialization"
    OTHER = "Other"


class SelectiveProvidersTestType(SelectiveTestType):
    PROVIDERS = "Providers"


class SelectiveTaskSdkTestType(SelectiveTestType):
    TASK_SDK = "TaskSdk"


class SelectiveAirflowCtlTestType(SelectiveTestType):
    AIRFLOW_CTL = "AirflowCTL"


class GroupOfTests(Enum):
    CORE = "core"
    PROVIDERS = "providers"
    TASK_SDK = "task-sdk"
    TASK_SDK_INTEGRATION = "task-sdk-integration"
    CTL = "airflow-ctl"
    CTL_INTEGRATION = "airflow-ctl-integration"
    HELM = "helm"
    INTEGRATION_CORE = "integration-core"
    INTEGRATION_PROVIDERS = "integration-providers"
    SYSTEM = "system"
    PYTHON_API_CLIENT = "python-api-client"


ALL_TEST_TYPE = "All"
NONE_TEST_TYPE = "None"

ALL_TEST_SUITES: dict[str, tuple[str, ...]] = {
    "All": (),
    "All-Long": ("-m", "long_running", "--include-long-running"),
    "All-Quarantined": ("-m", "quarantined", "--include-quarantined"),
    "All-Postgres": ("--backend", "postgres"),
    "All-MySQL": ("--backend", "mysql"),
}


@clearable_cache
def all_helm_test_packages() -> list[str]:
    return sorted(
        [
            candidate.name
            for candidate in (AIRFLOW_ROOT_PATH / "helm-tests" / "tests" / "helm_tests").iterdir()
            if candidate.is_dir() and candidate.name != "__pycache__"
        ]
    )


ALLOWED_TEST_TYPE_CHOICES: dict[GroupOfTests, list[str]] = {
    GroupOfTests.CORE: [*ALL_TEST_SUITES.keys(), *all_selective_core_test_types()],
    GroupOfTests.PROVIDERS: [*ALL_TEST_SUITES.keys()],
    GroupOfTests.TASK_SDK: [ALL_TEST_TYPE],
    GroupOfTests.TASK_SDK_INTEGRATION: [ALL_TEST_TYPE],
    GroupOfTests.HELM: [ALL_TEST_TYPE, *all_helm_test_packages()],
    GroupOfTests.CTL: [ALL_TEST_TYPE],
    GroupOfTests.CTL_INTEGRATION: [ALL_TEST_TYPE],
}


@clearable_cache
def all_task_sdk_test_packages() -> list[str]:
    try:
        return sorted(
            [
                candidate.name
                for candidate in (AIRFLOW_ROOT_PATH / "task-sdk" / "tests").iterdir()
                if candidate.is_dir() and candidate.name != "__pycache__"
            ]
        )
    except FileNotFoundError:
        return []


ALLOWED_TASK_SDK_TEST_PACKAGES = [
    "all",
    *all_task_sdk_test_packages(),
]


@clearable_cache
def all_ctl_test_packages() -> list[str]:
    try:
        return sorted(
            [
                candidate.name
                for candidate in (AIRFLOW_ROOT_PATH / "airflow-ctl" / "tests").iterdir()
                if candidate.is_dir() and candidate.name != "__pycache__"
            ]
        )
    except FileNotFoundError:
        return []


ALLOWED_CTL_TEST_PACKAGES = [
    "all",
    *all_ctl_test_packages(),
]

ALLOWED_DISTRIBUTION_FORMATS = ["wheel", "sdist", "both"]
ALLOWED_INSTALLATION_DISTRIBUTION_FORMATS = ["wheel", "sdist"]
ALLOWED_INSTALLATION_METHODS = [".", "apache-airflow"]
ALLOWED_BUILD_CACHE = ["registry", "local", "disabled"]
ALLOWED_BUILD_PROGRESS = ["auto", "plain", "tty"]
MULTI_PLATFORM = "linux/amd64,linux/arm64"
ALTERNATIVE_PLATFORMS = ["linux/x86_64", "linux/aarch64"]
SINGLE_PLATFORMS = ["linux/amd64", "linux/arm64", *ALTERNATIVE_PLATFORMS]
ALLOWED_PLATFORMS = [*SINGLE_PLATFORMS, MULTI_PLATFORM]

ALLOWED_USE_AIRFLOW_VERSIONS = ["none", "wheel", "sdist"]

ALL_HISTORICAL_PYTHON_VERSIONS = ["3.6", "3.7", "3.8", "3.9", "3.10", "3.11", "3.12", "3.13"]

GITHUB_REPO_BRANCH_PATTERN = r"^([^/]+)/([^/:]+):([^:]+)$"
PR_NUMBER_PATTERN = r"^\d+$"


def normalize_platform_machine(platform_machine: str) -> str:
    if "linux/" in platform_machine:
        return {"linux/x86_64": "linux/amd64", "linux/aarch64": "linux/arm64"}.get(
            platform_machine, platform_machine
        )
    return "linux/" + {"x86_64": "amd64", "aarch64": "arm64"}.get(platform_machine, platform_machine)


def get_default_platform_machine() -> str:
    machine = platform.uname().machine.lower()
    return normalize_platform_machine(machine)


# Initialise base variables
DOCKER_DEFAULT_PLATFORM = get_default_platform_machine()
DOCKER_BUILDKIT = 1

DRILL_HOST_PORT = "28047"
FLOWER_HOST_PORT = "25555"
GREMLIN_HOST_PORT = "8182"
MSSQL_HOST_PORT = "21433"
MYSQL_HOST_PORT = "23306"
POSTGRES_HOST_PORT = "25433"
RABBITMQ_HOST_PORT = "25672"
REDIS_HOST_PORT = "26379"
RABBITMQ_HOST_PORT = "25672"
SSH_PORT = "12322"
VITE_DEV_PORT = "5173"
WEB_HOST_PORT = "28080"
BREEZE_DEBUG_SCHEDULER_PORT = "50231"
BREEZE_DEBUG_DAG_PROCESSOR_PORT = "50232"
BREEZE_DEBUG_TRIGGERER_PORT = "50233"
BREEZE_DEBUG_APISERVER_PORT = "50234"
BREEZE_DEBUG_CELERY_WORKER_PORT = "50235"
BREEZE_DEBUG_EDGE_PORT = "50236"
BREEZE_DEBUG_WEBSERVER_PORT = "50237"

CELERY_BROKER_URLS_MAP = {"rabbitmq": "amqp://guest:guest@rabbitmq:5672", "redis": "redis://redis:6379/0"}
SQLITE_URL = "sqlite:////root/airflow/sqlite/airflow.db"
PYTHONDONTWRITEBYTECODE = True

PRODUCTION_IMAGE = False
# All python versions include all past python versions available in previous branches
# Even if we remove them from the main version. This is needed to make sure we can cherry-pick
# changes from main to the previous branch.
ALL_PYTHON_MAJOR_MINOR_VERSIONS = ["3.10", "3.11", "3.12", "3.13"]
CURRENT_PYTHON_MAJOR_MINOR_VERSIONS = ALL_PYTHON_MAJOR_MINOR_VERSIONS
CURRENT_POSTGRES_VERSIONS = ["13", "14", "15", "16", "17"]
DEFAULT_POSTGRES_VERSION = CURRENT_POSTGRES_VERSIONS[0]
USE_MYSQL_INNOVATION_RELEASE = True
if USE_MYSQL_INNOVATION_RELEASE:
    CURRENT_MYSQL_VERSIONS = ALLOWED_MYSQL_VERSIONS.copy()
else:
    CURRENT_MYSQL_VERSIONS = [*MYSQL_OLD_RELEASES, *MYSQL_LTS_RELEASES]
DEFAULT_MYSQL_VERSION = CURRENT_MYSQL_VERSIONS[0]

PYTHON_3_6_TO_3_8 = ["3.6", "3.7", "3.8"]
PYTHON_3_6_TO_3_9 = ["3.6", "3.7", "3.8", "3.9"]
PYTHON_3_6_TO_3_10 = ["3.7", "3.8", "3.9", "3.10"]
PYTHON_3_7_TO_3_11 = ["3.7", "3.8", "3.9", "3.10", "3.11"]
PYTHON_3_8_TO_3_11 = ["3.8", "3.9", "3.10", "3.11"]
PYTHON_3_8_TO_3_12 = ["3.8", "3.9", "3.10", "3.11", "3.12"]
PYTHON_3_9_TO_3_12 = ["3.9", "3.10", "3.11", "3.12"]


AIRFLOW_PYTHON_COMPATIBILITY_MATRIX = {
    "2.0.0": PYTHON_3_6_TO_3_8,
    "2.0.1": PYTHON_3_6_TO_3_8,
    "2.0.2": PYTHON_3_6_TO_3_8,
    "2.1.0": PYTHON_3_6_TO_3_8,
    "2.1.1": PYTHON_3_6_TO_3_8,
    "2.1.2": PYTHON_3_6_TO_3_9,
    "2.1.3": PYTHON_3_6_TO_3_9,
    "2.1.4": PYTHON_3_6_TO_3_9,
    "2.2.0": PYTHON_3_6_TO_3_9,
    "2.2.1": PYTHON_3_6_TO_3_9,
    "2.2.2": PYTHON_3_6_TO_3_9,
    "2.2.3": PYTHON_3_6_TO_3_9,
    "2.2.4": PYTHON_3_6_TO_3_9,
    "2.2.5": PYTHON_3_6_TO_3_9,
    "2.3.0": PYTHON_3_6_TO_3_10,
    "2.3.1": PYTHON_3_6_TO_3_10,
    "2.3.2": PYTHON_3_6_TO_3_10,
    "2.3.3": PYTHON_3_6_TO_3_10,
    "2.3.4": PYTHON_3_6_TO_3_10,
    "2.4.0": PYTHON_3_6_TO_3_10,
    "2.4.1": PYTHON_3_6_TO_3_10,
    "2.4.2": PYTHON_3_6_TO_3_10,
    "2.4.3": PYTHON_3_6_TO_3_10,
    "2.5.0": PYTHON_3_6_TO_3_10,
    "2.5.1": PYTHON_3_6_TO_3_10,
    "2.5.2": PYTHON_3_6_TO_3_10,
    "2.5.3": PYTHON_3_6_TO_3_10,
    "2.6.0": PYTHON_3_6_TO_3_10,
    "2.6.1": PYTHON_3_6_TO_3_10,
    "2.6.2": PYTHON_3_7_TO_3_11,
    "2.6.3": PYTHON_3_7_TO_3_11,
    "2.7.0": PYTHON_3_8_TO_3_11,
    "2.7.1": PYTHON_3_8_TO_3_11,
    "2.7.2": PYTHON_3_8_TO_3_11,
    "2.7.3": PYTHON_3_8_TO_3_11,
    "2.8.0": PYTHON_3_8_TO_3_11,
    "2.8.1": PYTHON_3_8_TO_3_11,
    "2.8.2": PYTHON_3_8_TO_3_11,
    "2.8.3": PYTHON_3_8_TO_3_11,
    "2.9.0": PYTHON_3_8_TO_3_12,
    "2.9.1": PYTHON_3_8_TO_3_12,
    "2.9.2": PYTHON_3_8_TO_3_12,
    "2.9.3": PYTHON_3_8_TO_3_12,
    "2.10.0": PYTHON_3_8_TO_3_12,
    "2.10.1": PYTHON_3_8_TO_3_12,
    "2.10.2": PYTHON_3_8_TO_3_12,
    "2.10.3": PYTHON_3_8_TO_3_12,
    "2.10.4": PYTHON_3_8_TO_3_12,
    "2.10.5": PYTHON_3_8_TO_3_12,
    "2.11.0": PYTHON_3_9_TO_3_12,
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
    "bugraoz93",
    "criccomini",
    "dimberman",
    "dirrao",
    "dstandish",
    "eladkal",
    "ephraimbuddy",
    "feluelle",
    "feng-tao",
    "ferruzzi",
    "gopidesupavan",
    "guan404ming",
    "houqp",
    "hussein-awala",
    "jason810496",
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
    "rawwar",
    "romsharon98",
    "ryanahamilton",
    "ryw",
    "saguziel",
    "sekikn",
    "shahar1",
    "shubhamraj-git",
    "tirkarthi",
    "turbaszek",
    "uranusjr",
    "utkarsharma2",
    "vatsrahul1001",
    "vikramkoka",
    "vincbeck",
    "xinbinhuang",
    "yuqian90",
    "zhongjiajie",
]


def get_airflowctl_version():
    airflowctl_init_py_file = AIRFLOW_CTL_SOURCES_PATH / "airflowctl" / "__init__.py"
    airflowctl_version = "unknown"
    with open(airflowctl_init_py_file) as init_file:
        while line := init_file.readline():
            if "__version__ = " in line:
                airflowctl_version = line.split()[2][1:-1]
                break
    if airflowctl_version == "unknown":
        raise RuntimeError("Unable to determine AirflowCTL version")
    return airflowctl_version


def get_airflow_version():
    airflow_init_py_file = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "__init__.py"
    airflow_version = "unknown"
    with open(airflow_init_py_file) as init_file:
        while line := init_file.readline():
            if "__version__ = " in line:
                airflow_version = line.split()[2][1:-1]
                break
    if airflow_version == "unknown":
        raise RuntimeError("Unable to determine Airflow version")
    return airflow_version


def get_task_sdk_version():
    task_sdk_init_py_file = AIRFLOW_TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "__init__.py"
    task_sdk_version = "unknown"
    with open(task_sdk_init_py_file) as init_file:
        while line := init_file.readline():
            if "__version__ = " in line:
                task_sdk_version = line.split()[2][1:-1]
                break
    if task_sdk_version == "unknown":
        raise RuntimeError("Unable to determine Task SDK version")
    return task_sdk_version


@clearable_cache
def get_airflow_extras():
    airflow_dockerfile = AIRFLOW_ROOT_PATH / "Dockerfile"
    with open(airflow_dockerfile) as dockerfile:
        for line in dockerfile.readlines():
            if "ARG AIRFLOW_EXTRAS=" in line:
                line = line.split("=")[1].strip()
                return line.replace('"', "")


# Initialize integrations
PROVIDER_RUNTIME_DATA_SCHEMA_PATH = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "provider_info.schema.json"

ALL_PYPROJECT_TOML_FILES: list[Path] = []


UPDATE_PROVIDER_DEPENDENCIES_SCRIPT = (
    AIRFLOW_ROOT_PATH / "scripts" / "ci" / "prek" / "update_providers_dependencies.py"
)


DEVEL_DEPS_PATH = AIRFLOW_ROOT_PATH / "generated" / "devel_deps.txt"


# Initialize files for rebuild check
FILES_FOR_REBUILD_CHECK = [
    "Dockerfile.ci",
    ".dockerignore",
    "scripts/docker/common.sh",
    "scripts/docker/install_additional_dependencies.sh",
    "scripts/docker/install_airflow_when_building_images.sh",
    "scripts/docker/install_from_docker_context_files.sh",
    "scripts/docker/install_mysql.sh",
]

CURRENT_KUBERNETES_VERSIONS = ALLOWED_KUBERNETES_VERSIONS
CURRENT_EXECUTORS = [KUBERNETES_EXECUTOR]

DEFAULT_KUBERNETES_VERSION = CURRENT_KUBERNETES_VERSIONS[0]
DEFAULT_EXECUTOR = CURRENT_EXECUTORS[0]

KIND_VERSION = "v0.27.0"
HELM_VERSION = "v3.17.3"

# Initialize image build variables - Have to check if this has to go to ci dataclass
USE_AIRFLOW_VERSION = None
GITHUB_ACTIONS = ""

ISSUE_ID = ""
NUM_RUNS = ""

MIN_DOCKER_VERSION = "25.0.0"
MIN_DOCKER_COMPOSE_VERSION = "2.20.2"
MIN_GH_VERSION = "2.70.0"

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
    "common-messaging",
    "docker",
    "elasticsearch",
    "fab",
    "ftp",
    "git",
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
    # END OF EXTRAS LIST UPDATED BY PRE COMMIT
]

PROVIDERS_COMPATIBILITY_TESTS_MATRIX: list[dict[str, str | list[str]]] = [
    {
        "python-version": "3.10",
        "airflow-version": "2.10.5",
        "remove-providers": "common.messaging fab git keycloak",
        "run-unit-tests": "true",
    },
    {
        "python-version": "3.10",
        "airflow-version": "2.11.0",
        "remove-providers": "common.messaging fab git keycloak",
        "run-unit-tests": "true",
    },
    {
        "python-version": "3.10",
        "airflow-version": "3.0.6",
        "remove-providers": "",
        "run-unit-tests": "true",
    },
]

ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION: dict[str, str] = {
    "3.10": "3.10.19",
    "3.11": "3.11.14",
    "3.12": "3.12.12",
    "3.13": "3.13.9",
}

# Number of slices for low dep tests
NUMBER_OF_LOW_DEP_SLICES = 5


class GithubEvents(Enum):
    PULL_REQUEST = "pull_request"
    PULL_REQUEST_REVIEW = "pull_request_review"
    PULL_REQUEST_TARGET = "pull_request_target"
    PULL_REQUEST_WORKFLOW = "pull_request_workflow"
    PUSH = "push"
    SCHEDULE = "schedule"
    WORKFLOW_DISPATCH = "workflow_dispatch"
    WORKFLOW_RUN = "workflow_run"


@clearable_cache
def github_events() -> list[str]:
    return [e.value for e in GithubEvents]
