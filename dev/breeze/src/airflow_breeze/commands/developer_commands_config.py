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
    "name": "Basic developer commands",
    "commands": [
        "start-airflow",
        "static-checks",
        "build-docs",
        "stop",
        "shell",
        "exec",
        "compile-www-assets",
        "cleanup",
    ],
}
DEVELOPER_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze": [
        {
            "name": "Basic flags",
            "options": [
                "--python",
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--integration",
                "--forward-credentials",
                "--db-reset",
            ],
        },
    ],
    "breeze shell": [
        {
            "name": "Basic flags",
            "options": [
                "--python",
                "--backend",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--integration",
                "--forward-credentials",
                "--db-reset",
            ],
        },
        {
            "name": "Advanced flag for running",
            "options": [
                "--use-airflow-version",
                "--airflow-constraints-reference",
                "--platform",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
                "--force-build",
                "--image-tag",
                "--mount-sources",
                "--include-mypy-volume",
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
            "name": "Basic flags",
            "options": [
                "--python",
                "--load-example-dags",
                "--load-default-connections",
                "--backend",
                "--platform",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--integration",
                "--forward-credentials",
                "--db-reset",
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
            "name": "Advanced flag for running",
            "options": [
                "--use-airflow-version",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
                "--force-build",
                "--image-tag",
                "--mount-sources",
            ],
        },
    ],
    "breeze exec": [
        {"name": "Drops in the interactive shell of active airflow container"},
    ],
    "breeze stop": [
        {
            "name": "Stop flags",
            "options": [
                "--preserve-volumes",
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
                "--for-production",
                "--package-filter",
            ],
        },
    ],
    "breeze static-checks": [
        {
            "name": "Pre-commit flags",
            "options": [
                "--type",
                "--file",
                "--all-files",
                "--show-diff-on-failure",
                "--last-commit",
                "--commit-ref",
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
