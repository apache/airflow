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
from functools import lru_cache

from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT

# Commented this out as we are using buildkit and this vars became irrelevant
# FORCE_PULL_IMAGES = False
# CHECK_IF_BASE_PYTHON_IMAGE_UPDATED = False
FORCE_BUILD_IMAGES = False
ANSWER = ""
SKIP_CHECK_REMOTE_IMAGE = False
# PUSH_PYTHON_BASE_IMAGE = False


# Checked before putting in build cache
ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS = ['3.7', '3.8', '3.9', '3.10']
DEFAULT_PYTHON_MAJOR_MINOR_VERSION = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
ALLOWED_BACKENDS = ['sqlite', 'mysql', 'postgres', 'mssql']
ALLOWED_PROD_BACKENDS = ['mysql', 'postgres', 'mssql']
DEFAULT_BACKEND = ALLOWED_BACKENDS[0]
ALLOWED_INTEGRATIONS = [
    'cassandra',
    'kerberos',
    'mongo',
    'openldap',
    'pinot',
    'rabbitmq',
    'redis',
    'statsd',
    'trino',
    'all',
]
ALLOWED_KUBERNETES_MODES = ['image']
ALLOWED_KUBERNETES_VERSIONS = ['v1.24.0', 'v1.23.6', 'v1.22.9', 'v1.21.12', 'v1.20.15']
ALLOWED_KIND_VERSIONS = ['v0.14.0']
ALLOWED_HELM_VERSIONS = ['v3.6.3']
ALLOWED_EXECUTORS = ['KubernetesExecutor', 'CeleryExecutor', 'LocalExecutor', 'CeleryKubernetesExecutor']
ALLOWED_KIND_OPERATIONS = ['start', 'stop', 'restart', 'status', 'deploy', 'test', 'shell', 'k9s']
ALLOWED_CONSTRAINTS_MODES_CI = ['constraints-source-providers', 'constraints', 'constraints-no-providers']
ALLOWED_CONSTRAINTS_MODES_PROD = ['constraints', 'constraints-no-providers', 'constraints-source-providers']

MOUNT_SELECTED = "selected"
MOUNT_ALL = "all"
MOUNT_SKIP = "skip"
MOUNT_REMOVE = "remove"

ALLOWED_MOUNT_OPTIONS = [MOUNT_SELECTED, MOUNT_ALL, MOUNT_SKIP, MOUNT_REMOVE]
ALLOWED_POSTGRES_VERSIONS = ['10', '11', '12', '13', '14']
ALLOWED_MYSQL_VERSIONS = ['5.7', '8']
ALLOWED_MSSQL_VERSIONS = ['2017-latest', '2019-latest']


@lru_cache(maxsize=None)
def all_selective_test_types() -> tuple[str, ...]:
    return tuple(sorted(e.value for e in SelectiveUnitTestTypes))


class SelectiveUnitTestTypes(Enum):
    ALWAYS = 'Always'
    API = 'API'
    CLI = 'CLI'
    CORE = 'Core'
    OTHER = 'Other'
    INTEGRATION = 'Integration'
    PROVIDERS = 'Providers'
    WWW = 'WWW'


ALLOWED_TEST_TYPE_CHOICES = [
    "All",
    "Always",
    *all_selective_test_types(),
    "Helm",
    "Postgres",
    "MySQL",
    "Integration",
    "Other",
    "Quarantine",
]

ALLOWED_PACKAGE_FORMATS = ['wheel', 'sdist', 'both']
ALLOWED_INSTALLATION_PACKAGE_FORMATS = ['wheel', 'sdist']
ALLOWED_INSTALLATION_METHODS = ['.', 'apache-airflow']
ALLOWED_DEBIAN_VERSIONS = ['bullseye', 'buster']
ALLOWED_BUILD_CACHE = ["registry", "local", "disabled"]
MULTI_PLATFORM = "linux/amd64,linux/arm64"
SINGLE_PLATFORMS = ["linux/amd64", "linux/arm64"]
ALLOWED_PLATFORMS = [*SINGLE_PLATFORMS, MULTI_PLATFORM]
ALLOWED_USE_AIRFLOW_VERSIONS = ['none', 'wheel', 'sdist']

PARAM_NAME_DESCRIPTION = {
    "BACKEND": "backend",
    "MYSQL_VERSION": "Mysql version",
    "KUBERNETES_MODE": "Kubernetes mode",
    "KUBERNETES_VERSION": "Kubernetes version",
    "KIND_VERSION": "KinD version",
    "HELM_VERSION": "Helm version",
    "EXECUTOR": "Executors",
    "POSTGRES_VERSION": "Postgres version",
    "MSSQL_VERSION": "MSSql version",
}

EXCLUDE_DOCS_PACKAGE_FOLDER = [
    'exts',
    'integration-logos',
    'rtd-deprecation',
    '_build',
    '_doctrees',
    '_inventory_cache',
]


def get_available_packages(short_version=False) -> list[str]:
    docs_path_content = (AIRFLOW_SOURCES_ROOT / 'docs').glob('*/')
    available_packages = [x.name for x in docs_path_content if x.is_dir()]
    package_list = list(set(available_packages) - set(EXCLUDE_DOCS_PACKAGE_FOLDER))
    package_list.sort()
    if short_version:
        prefix_len = len("apache-airflow-providers-")
        package_list = [
            package[prefix_len:].replace("-", ".") for package in package_list if len(package) > prefix_len
        ]
    return package_list


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
ALL_PYTHON_MAJOR_MINOR_VERSIONS = ['3.7', '3.8', '3.9', '3.10']
CURRENT_PYTHON_MAJOR_MINOR_VERSIONS = ['3.7', '3.8', '3.9', '3.10']
CURRENT_POSTGRES_VERSIONS = ['10', '11', '12', '13', '14']
DEFAULT_POSTGRES_VERSION = CURRENT_POSTGRES_VERSIONS[0]
CURRENT_MYSQL_VERSIONS = ['5.7', '8']
DEFAULT_MYSQL_VERSION = CURRENT_MYSQL_VERSIONS[0]
CURRENT_MSSQL_VERSIONS = ['2017-latest', '2019-latest']
DEFAULT_MSSQL_VERSION = CURRENT_MSSQL_VERSIONS[0]

DB_RESET = False
START_AIRFLOW = "false"
LOAD_EXAMPLES = False
LOAD_DEFAULT_CONNECTIONS = False
PRESERVE_VOLUMES = False
CLEANUP_CONTEXT = False
INIT_SCRIPT_FILE = ""
DRY_RUN_DOCKER = False
INSTALL_AIRFLOW_VERSION = ""
SQLITE_URL = "sqlite:////root/airflow/airflow.db"


def get_airflow_version():
    airflow_setup_file = AIRFLOW_SOURCES_ROOT / 'setup.py'
    with open(airflow_setup_file) as setup_file:
        for line in setup_file.readlines():
            if "version =" in line:
                return line.split()[2][1:-1]


def get_airflow_extras():
    airflow_dockerfile = AIRFLOW_SOURCES_ROOT / 'Dockerfile'
    with open(airflow_dockerfile) as dockerfile:
        for line in dockerfile.readlines():
            if "ARG AIRFLOW_EXTRAS=" in line:
                line = line.split('=')[1].strip()
                return line.replace('"', '')


# Initialize integrations
AVAILABLE_INTEGRATIONS = [
    'cassandra',
    'kerberos',
    'mongo',
    'openldap',
    'pinot',
    'rabbitmq',
    'redis',
    'statsd',
    'trino',
]
ENABLED_INTEGRATIONS = ""
# Initialize files for rebuild check
FILES_FOR_REBUILD_CHECK = [
    'setup.py',
    'setup.cfg',
    'Dockerfile.ci',
    '.dockerignore',
    'scripts/docker/compile_www_assets.sh',
    'scripts/docker/common.sh',
    'scripts/docker/install_additional_dependencies.sh',
    'scripts/docker/install_airflow.sh',
    'scripts/docker/install_airflow_dependencies_from_branch_tip.sh',
    'scripts/docker/install_from_docker_context_files.sh',
    'scripts/docker/install_mysql.sh',
    'airflow/www/package.json',
    'airflow/www/yarn.lock',
    'airflow/www/webpack.config.js',
    'airflow/ui/package.json',
    'airflow/ui/yarn.lock',
]

ENABLED_SYSTEMS = ""

CURRENT_KUBERNETES_MODES = ['image']
CURRENT_KUBERNETES_VERSIONS = ['v1.24.0', 'v1.23.6', 'v1.22.9', 'v1.21.12', 'v1.20.15']
CURRENT_KIND_VERSIONS = ['v0.14.0']
CURRENT_HELM_VERSIONS = ['v3.6.3']
CURRENT_EXECUTORS = ['KubernetesExecutor']

DEFAULT_KUBERNETES_MODE = CURRENT_KUBERNETES_MODES[0]
DEFAULT_KUBERNETES_VERSION = CURRENT_KUBERNETES_VERSIONS[0]
DEFAULT_KIND_VERSION = CURRENT_KIND_VERSIONS[0]
DEFAULT_HELM_VERSION = CURRENT_HELM_VERSIONS[0]
DEFAULT_EXECUTOR = CURRENT_EXECUTORS[0]

# Initialize image build variables - Have to check if this has to go to ci dataclass
USE_AIRFLOW_VERSION = None
GITHUB_ACTIONS = ""

ISSUE_ID = ""
NUM_RUNS = ""

MIN_DOCKER_VERSION = "20.10.0"
MIN_DOCKER_COMPOSE_VERSION = "1.29.0"

AIRFLOW_SOURCES_FROM = "."
AIRFLOW_SOURCES_TO = "/opt/airflow"
AIRFLOW_SOURCES_WWW_FROM = "./airflow/www"
AIRFLOW_SOURCES_WWW_TO = "/opt/airflow/airflow/www"

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
