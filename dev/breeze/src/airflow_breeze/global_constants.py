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


AIRFLOW_SOURCES = ""

FORCE_PULL_IMAGES = False
CHECK_IF_BASE_PYTHON_IMAGE_UPDATED = False
FORCE_BUILD_IMAGES = False
# LAST_FORCE_ANSWER_FILE = f"{BUILD_CACHE_DIR}/last_force_answer.sh"
FORCE_ANSWER_TO_QUESTION = ""
SKIP_CHECK_REMOTE_IMAGE = False
PUSH_PYTHON_BASE_IMAGE = False

DEFAULT_PYTHON_MAJOR_MINOR_VERSION = '3.7'
DEFAULT_BACKEND = 'sqlite'

ALLOWED_PYTHON_MAJOR_MINOR_VERSION = ['3.6', '3.7', '3.8', '3.9']
ALLOWED_BACKENDS = ['sqlite', 'mysql', 'postgres', 'mssql']
ALLOWED_STATIC_CHECKS = [
    "all",
    "airflow-config-yaml",
    "airflow-providers-available",
    "airflow-provider-yaml-files-ok",
    "base-operator",
    "bats-tests",
    "bats-in-container-tests",
    "black",
    "blacken-docs",
    "boring-cyborg",
    "build",
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
ALLOWED_USE_AIRFLOW_VERSION = ['.', 'apache-airflow']

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


SSH_PORT = "12322"
WEBSERVER_HOST_PORT = "28080"
POSTGRES_HOST_PORT = "25433"
MYSQL_HOST_PORT = "23306"
MSSQL_HOST_PORT = "21433"
FLOWER_HOST_PORT = "25555"
REDIS_HOST_PORT = "26379"
