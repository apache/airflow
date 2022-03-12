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
from pathlib import Path
from typing import List

from airflow_breeze.utils.path_utils import get_airflow_sources_root

# Commented this out as we are using buildkit and this vars became irrelevant
# FORCE_PULL_IMAGES = False
# CHECK_IF_BASE_PYTHON_IMAGE_UPDATED = False
FORCE_BUILD_IMAGES = False
FORCE_ANSWER_TO_QUESTION = ""
SKIP_CHECK_REMOTE_IMAGE = False
# PUSH_PYTHON_BASE_IMAGE = False

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = '3.7'
DEFAULT_BACKEND = 'sqlite'

# Checked before putting in build cache
ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS = ['3.7', '3.8', '3.9', '3.10']
ALLOWED_BACKENDS = ['sqlite', 'mysql', 'postgres', 'mssql']
ALLOWED_STATIC_CHECKS = [
    "all",
    "airflow-config-yaml",
    "airflow-providers-available",
    "airflow-provider-yaml-files-ok",
    "base-operator",
    "black",
    "blacken-docs",
    "boring-cyborg",
    "build-providers-dependencies",
    "chart-schema-lint",
    "capitalized-breeze",
    "changelog-duplicates",
    "check-apache-license",
    "check-builtin-literals",
    "check-executables-have-shebangs",
    "check-extras-order",
    "check-hooks-apply",
    "check-integrations",
    "check-merge-conflict",
    "check-xml",
    "daysago-import-check",
    "debug-statements",
    "detect-private-key",
    "doctoc",
    "dont-use-safe-filter",
    "end-of-file-fixer",
    "fix-encoding-pragma",
    "flake8",
    "flynt",
    "codespell",
    "forbid-tabs",
    "helm-lint",
    "identity",
    "incorrect-use-of-LoggingMixin",
    "insert-license",
    "isort",
    "json-schema",
    "language-matters",
    "lint-dockerfile",
    "lint-openapi",
    "markdownlint",
    "mermaid",
    "mixed-line-ending",
    "mypy",
    "mypy-helm",
    "no-providers-in-core-examples",
    "no-relative-imports",
    "pre-commit-descriptions",
    "pre-commit-hook-names",
    "pretty-format-json",
    "provide-create-sessions",
    "providers-changelogs",
    "providers-init-file",
    "providers-subpackages-init-file",
    "provider-yamls",
    "pydevd",
    "pydocstyle",
    "python-no-log-warn",
    "pyupgrade",
    "restrict-start_date",
    "rst-backticks",
    "setup-order",
    "setup-extra-packages",
    "shellcheck",
    "sort-in-the-wild",
    "sort-spelling-wordlist",
    "stylelint",
    "trailing-whitespace",
    "ui-lint",
    "update-breeze-file",
    "update-extras",
    "update-local-yml-file",
    "update-setup-cfg-file",
    "update-versions",
    "verify-db-migrations-documented",
    "version-sync",
    "www-lint",
    "yamllint",
    "yesqa",
]
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
ALLOWED_KUBERNETES_VERSIONS = ['v1.21.1', 'v1.20.2']
ALLOWED_KIND_VERSIONS = ['v0.11.1']
ALLOWED_HELM_VERSIONS = ['v3.6.3']
ALLOWED_EXECUTORS = ['KubernetesExecutor', 'CeleryExecutor', 'LocalExecutor', 'CeleryKubernetesExecutor']
ALLOWED_KIND_OPERATIONS = ['start', 'stop', 'restart', 'status', 'deploy', 'test', 'shell', 'k9s']
ALLOWED_INSTALL_AIRFLOW_VERSIONS = ['2.0.2', '2.0.1', '2.0.0', 'wheel', 'sdist']
ALLOWED_GENERATE_CONSTRAINTS_MODES = ['source-providers', 'pypi-providers', 'no-providers']
ALLOWED_POSTGRES_VERSIONS = ['10', '11', '12', '13']
ALLOWED_MYSQL_VERSIONS = ['5.7', '8']
ALLOWED_MSSQL_VERSIONS = ['2017-latest', '2019-latest']
ALLOWED_TEST_TYPES = [
    'All',
    'Always',
    'Core',
    'Providers',
    'API',
    'CLI',
    'Integration',
    'Other',
    'WWW',
    'Postgres',
    'MySQL',
    'Helm',
    'Quarantined',
]
ALLOWED_PACKAGE_FORMATS = ['both', 'sdist', 'wheel']
ALLOWED_USE_AIRFLOW_VERSIONS = ['.', 'apache-airflow']
ALLOWED_DEBIAN_VERSIONS = ['buster', 'bullseye']

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

PARAM_NAME_FLAG = {
    "BACKEND": "--backend",
    "MYSQL_VERSION": "--mysql-version",
    "KUBERNETES_MODE": "--kubernetes-mode",
    "KUBERNETES_VERSION": "--kubernetes-version",
    "KIND_VERSION": "--kind-version",
    "HELM_VERSION": "--helm-version",
    "EXECUTOR": "--executor",
    "POSTGRES_VERSION": "--postgres-version",
    "MSSQL_VERSION": "--mssql-version",
}

EXCLUDE_DOCS_PACKAGE_FOLDER = [
    'exts',
    'integration-logos',
    'rtd-deprecation',
    '_build',
    '_doctrees',
    '_inventory_cache',
]


def get_available_packages() -> List[str]:
    docs_path_content = Path(get_airflow_sources_root(), 'docs').glob('*/')
    available_packages = [x.name for x in docs_path_content if x.is_dir()]
    return list(set(available_packages) - set(EXCLUDE_DOCS_PACKAGE_FOLDER))


# Initialise base variables
DOCKER_DEFAULT_PLATFORM = f"linux/{os.uname().machine}"
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
CURRENT_POSTGRES_VERSIONS = ['10', '11', '12', '13']
CURRENT_MYSQL_VERSIONS = ['5.7', '8']
CURRENT_MSSQL_VERSIONS = ['2017-latest', '2019-latest']
POSTGRES_VERSION = CURRENT_POSTGRES_VERSIONS[0]
MYSQL_VERSION = CURRENT_MYSQL_VERSIONS[0]
MSSQL_VERSION = CURRENT_MSSQL_VERSIONS[0]
DB_RESET = False
START_AIRFLOW = "false"
LOAD_EXAMPLES = False
LOAD_DEFAULT_CONNECTIONS = False
PRESERVE_VOLUMES = False
CLEANUP_DOCKER_CONTEXT_FILES = False
INIT_SCRIPT_FILE = ""
DRY_RUN_DOCKER = False
INSTALL_AIRFLOW_VERSION = ""
SQLITE_URL = "sqlite:////root/airflow/airflow.db"


def get_airflow_version():
    airflow_setup_file = Path(get_airflow_sources_root()) / 'setup.py'
    with open(airflow_setup_file) as setup_file:
        for line in setup_file.readlines():
            if "version =" in line:
                return line.split()[2][1:-1]


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

# Initialize mount variables
MOUNT_SELECTED_LOCAL_SOURCES = True
MOUNT_ALL_LOCAL_SOURCES = False

ENABLED_SYSTEMS = ""


CURRENT_KUBERNETES_MODES = ['image']
CURRENT_KUBERNETES_VERSIONS = ['v1.21.1', 'v1.20.2']
CURRENT_KIND_VERSIONS = ['v0.11.1']
CURRENT_HELM_VERSIONS = ['v3.6.3']
CURRENT_EXECUTORS = ['KubernetesExecutor']

DEFAULT_KUBERNETES_MODES = CURRENT_KUBERNETES_MODES[0]
DEFAULT_KUBERNETES_VERSIONS = CURRENT_KUBERNETES_VERSIONS[0]
DEFAULT_KIND_VERSIONS = CURRENT_KIND_VERSIONS[0]
DEFAULT_HELM_VERSIONS = CURRENT_HELM_VERSIONS[0]
DEFAULT_EXECUTOR = CURRENT_EXECUTORS[0]

# Initialize image build variables - Have to check if this has to go to ci dataclass
SKIP_TWINE_CHECK = ""
USE_AIRFLOW_VERSION = ""
GITHUB_ACTIONS = ""

ISSUE_ID = ""
NUM_RUNS = ""

# Initialize package variables
PACKAGE_FORMAT = "wheel"
VERSION_SUFFIX_FOR_SVN = ""
VERSION_SUFFIX_FOR_PYPI = ""

MIN_DOCKER_VERSION = "20.10.0"
MIN_DOCKER_COMPOSE_VERSION = "1.29.0"
