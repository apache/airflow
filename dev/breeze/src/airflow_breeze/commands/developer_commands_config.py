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

DEVELOPER_COMMANDS: dict[str, str | list[str]] = {
    "name": "Developer commands",
    "commands": [
        "start-airflow",
        "build-docs",
        "down",
        "shell",
        "exec",
        "run",
        "cleanup",
        "generate-migration-file",
        "doctor",
    ],
}
DEVELOPER_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze": [
        {
            "name": "Execution mode",
            "options": [
                "--python",
                "--integration",
                "--standalone-dag-processor",
                "--auth-manager",
            ],
        },
        {
            "name": "Docker Compose selection and cleanup",
            "options": [
                "--project-name",
                "--restart",
                "--docker-host",
            ],
        },
        {
            "name": "Database",
            "options": [
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--db-reset",
            ],
        },
        {
            "name": "Build CI image (before entering shell)",
            "options": [
                "--github-repository",
                "--builder",
                "--use-uv",
                "--uv-http-timeout",
            ],
        },
        {
            "name": "Other options",
            "options": [
                "--forward-credentials",
                "--max-time",
            ],
        },
    ],
    "breeze shell": [
        {
            "name": "Execution mode",
            "options": [
                "--python",
                "--integration",
                "--load-example-dags",
                "--load-default-connections",
                "--standalone-dag-processor",
                "--start-api-server-with-examples",
                "--auth-manager",
            ],
        },
        {
            "name": "Docker Compose project management",
            "options": [
                "--project-name",
                "--restart",
                "--docker-host",
            ],
        },
        {
            "name": "Scripts execution",
            "options": [
                "--quiet",
                "--skip-image-upgrade-check",
                "--warn-image-upgrade-needed",
                "--skip-environment-initialization",
                "--tty",
            ],
        },
        {
            "name": "Database",
            "options": [
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--db-reset",
            ],
        },
        {
            "name": "Choose executor",
            "options": [
                "--executor",
                "--celery-broker",
                "--celery-flower",
            ],
        },
        {
            "name": "Build CI image (before entering shell)",
            "options": [
                "--force-build",
                "--platform",
                "--github-repository",
                "--builder",
                "--use-uv",
                "--uv-http-timeout",
            ],
        },
        {
            "name": "Mounting the sources and volumes",
            "options": [
                "--mount-sources",
                "--include-mypy-volume",
            ],
        },
        {
            "name": "Installing packages after entering shell",
            "options": [
                "--airflow-constraints-location",
                "--airflow-constraints-mode",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--clean-airflow-installation",
                "--force-lowest-dependencies",
                "--test-type",
                "--excluded-providers",
                "--install-airflow-with-constraints",
                "--install-selected-providers",
                "--distribution-format",
                "--providers-constraints-location",
                "--providers-constraints-mode",
                "--providers-constraints-reference",
                "--providers-skip-constraints",
                "--use-airflow-version",
                "--mount-ui-dist",
                "--allow-pre-releases",
                "--use-distributions-from-dist",
                "--install-airflow-python-client",
            ],
        },
        {
            "name": "Upgrading/downgrading/removing selected packages",
            "options": [
                "--upgrade-boto",
                "--upgrade-sqlalchemy",
                "--downgrade-sqlalchemy",
                "--downgrade-pendulum",
            ],
        },
        {
            "name": "DB test flags",
            "options": [
                "--run-db-tests-only",
                "--skip-db-tests",
            ],
        },
        {
            "name": "Other options",
            "options": [
                "--forward-credentials",
                "--max-time",
                "--verbose-commands",
                "--keep-env-variables",
                "--no-db-cleanup",
            ],
        },
    ],
    "breeze start-airflow": [
        {
            "name": "Execution mode",
            "options": [
                "--python",
                "--platform",
                "--integration",
                "--standalone-dag-processor",
                "--use-mprocs",
                "--auth-manager",
                "--load-example-dags",
                "--load-default-connections",
            ],
        },
        {
            "name": "Docker Compose selection and cleanup",
            "options": [
                "--project-name",
                "--restart",
                "--docker-host",
            ],
        },
        {
            "name": "Database",
            "options": [
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--db-reset",
            ],
        },
        {
            "name": "Choosing executor",
            "options": [
                "--executor",
                "--celery-broker",
                "--celery-flower",
            ],
        },
        {
            "name": "Asset compilation options",
            "options": [
                "--skip-assets-compilation",
                "--dev-mode",
            ],
        },
        {
            "name": "Build CI image (before entering shell)",
            "options": [
                "--force-build",
                "--github-repository",
                "--builder",
                "--use-uv",
                "--uv-http-timeout",
            ],
        },
        {
            "name": "Mounting the sources and volumes",
            "options": [
                "--mount-sources",
            ],
        },
        {
            "name": "Installing packages after entering shell",
            "options": [
                "--airflow-constraints-location",
                "--airflow-constraints-mode",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--clean-airflow-installation",
                "--install-airflow-with-constraints",
                "--install-selected-providers",
                "--distribution-format",
                "--providers-constraints-location",
                "--providers-constraints-mode",
                "--providers-constraints-reference",
                "--providers-skip-constraints",
                "--use-airflow-version",
                "--mount-ui-dist",
                "--allow-pre-releases",
                "--use-distributions-from-dist",
            ],
        },
        {
            "name": "Other options",
            "options": ["--forward-credentials", "--create-all-roles"],
        },
        {
            "name": "Debugging options",
            "options": ["--debug", "--debugger"],
        },
    ],
    "breeze exec": [
        {"name": "Drops in the interactive shell of active airflow container"},
    ],
    "breeze run": [
        {
            "name": "Command execution",
            "options": [
                "--python",
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--tty",
            ],
        },
        {
            "name": "Build CI image (before running command)",
            "options": [
                "--force-build",
                "--platform",
                "--github-repository",
                "--builder",
                "--use-uv",
                "--uv-http-timeout",
            ],
        },
        {
            "name": "Docker Compose project management",
            "options": [
                "--project-name",
                "--docker-host",
            ],
        },
        {
            "name": "Other options",
            "options": [
                "--forward-credentials",
                "--skip-image-upgrade-check",
            ],
        },
    ],
    "breeze down": [
        {
            "name": "Down flags",
            "options": [
                "--preserve-volumes",
                "--cleanup-mypy-cache",
                "--cleanup-build-cache",
            ],
        },
    ],
    "breeze build-docs": [
        {
            "name": "Build scope (default is to build docs and spellcheck)",
            "options": ["--docs-only", "--spellcheck-only"],
        },
        {
            "name": "Type of build",
            "options": ["--one-pass-only"],
        },
        {
            "name": "Cleaning inventories",
            "options": ["--clean-build", "--refresh-airflow-inventories"],
        },
        {
            "name": "Filtering options",
            "options": [
                "--package-filter",
                "--include-not-ready-providers",
                "--include-removed-providers",
            ],
        },
        {
            "name": "Misc options",
            "options": [
                "--include-commits",
                "--github-repository",
                "--builder",
                "--distributions-list",
            ],
        },
    ],
    "breeze cleanup": [
        {
            "name": "Cleanup flags",
            "options": [
                "--all",
            ],
        },
    ],
    "breeze generate-migration-file": [
        {
            "name": "generate-migration-file flags",
            "options": [
                "--message",
                "--github-repository",
                "--builder",
            ],
        },
    ],
    "breeze doctor": [
        {
            "name": "Auto-healing of breeze",
            "options": [
                "--answer",
            ],
        }
    ],
}
