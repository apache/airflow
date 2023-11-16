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
        "compile-www-assets",
        "cleanup",
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
            "name": "Database",
            "options": [
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--db-reset",
            ],
        },
        {
            "name": "Build CI image (before entering shell)",
            "options": [
                "--github-repository",
                "--builder",
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
            "name": "Database",
            "options": [
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
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
                "--install-selected-providers",
                "--use-airflow-version",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
            ],
        },
        {
            "name": "Upgrading/downgrading selected packages",
            "options": [
                "--upgrade-boto",
                "--downgrade-sqlalchemy",
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
            ],
        },
    ],
    "breeze compile-www-assets": [
        {
            "name": "Compile www assets flag",
            "options": [
                "--dev",
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
            "name": "Database",
            "options": [
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
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
                "--skip-asset-compilation",
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
                "--use-airflow-version",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
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
                "--github-repository",
                "--builder",
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
                "--skip-image-check",
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
}
