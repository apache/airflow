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
        "static-checks",
        "build-docs",
        "down",
        "shell",
        "exec",
        "compile-ui-assets",
        "compile-www-assets",
        "cleanup",
        "generate-migration-file",
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
                "--database-isolation",
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
                "--standalone-dag-processor",
                "--database-isolation",
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
                "--image-tag",
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
                "--airflow-skip-constraints",
                "--clean-airflow-installation",
                "--excluded-providers",
                "--force-lowest-dependencies",
                "--install-airflow-with-constraints",
                "--install-selected-providers",
                "--package-format",
                "--providers-constraints-location",
                "--providers-constraints-mode",
                "--providers-constraints-reference",
                "--providers-skip-constraints",
                "--test-type",
                "--use-airflow-version",
                "--use-packages-from-dist",
            ],
        },
        {
            "name": "Upgrading/downgrading/removing selected packages",
            "options": [
                "--upgrade-boto",
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
    "breeze compile-www-assets": [
        {
            "name": "Compile www assets flag",
            "options": [
                "--dev",
                "--force-clean",
            ],
        }
    ],
    "breeze compile-ui-assets": [
        {
            "name": "Compile ui assets flag",
            "options": [
                "--dev",
                "--force-clean",
            ],
        }
    ],
    "breeze start-airflow": [
        {
            "name": "Execution mode",
            "options": [
                "--python",
                "--platform",
                "--integration",
                "--standalone-dag-processor",
                "--database-isolation",
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
                "--image-tag",
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
                "--airflow-skip-constraints",
                "--clean-airflow-installation",
                "--install-selected-providers",
                "--package-format",
                "--providers-constraints-location",
                "--providers-constraints-mode",
                "--providers-constraints-reference",
                "--providers-skip-constraints",
                "--use-airflow-version",
                "--use-packages-from-dist",
            ],
        },
        {
            "name": "Other options",
            "options": [
                "--forward-credentials",
            ],
        },
    ],
    "breeze exec": [
        {"name": "Drops in the interactive shell of active airflow container"},
    ],
    "breeze down": [
        {
            "name": "Down flags",
            "options": [
                "--preserve-volumes",
                "--cleanup-mypy-cache",
                "--project-name",
            ],
        },
    ],
    "breeze build-docs": [
        {
            "name": "Doc flags",
            "options": [
                "--docs-only",
                "--spellcheck-only",
                "--clean-build",
                "--one-pass-only",
                "--package-filter",
                "--include-not-ready-providers",
                "--include-removed-providers",
                "--github-repository",
                "--builder",
                "--package-list",
            ],
        },
    ],
    "breeze static-checks": [
        {
            "name": "Pre-commit flags",
            "options": [
                "--type",
                "--show-diff-on-failure",
                "--initialize-environment",
                "--max-initialization-attempts",
            ],
        },
        {
            "name": "Selecting files to run the checks on",
            "options": [
                "--file",
                "--all-files",
                "--commit-ref",
                "--last-commit",
                "--only-my-changes",
            ],
        },
        {
            "name": "Building image before running checks",
            "options": [
                "--skip-image-upgrade-check",
                "--force-build",
                "--image-tag",
                "--github-repository",
                "--builder",
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
}
