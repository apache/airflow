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

TESTING_COMMANDS: dict[str, str | list[str]] = {
    "name": "Testing",
    "commands": ["tests", "integration-tests", "helm-tests", "docker-compose-tests"],
}
TESTING_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze testing tests": [
        {
            "name": "Select test types to run (tests can also be selected by command args individually)",
            "options": [
                "--test-type",
                "--parallel-test-types",
                "--excluded-parallel-test-types",
            ],
        },
        {
            "name": "Test options",
            "options": [
                "--test-timeout",
                "--enable-coverage",
                "--collect-only",
                "--db-reset",
                "--skip-provider-tests",
            ],
        },
        {
            "name": "Selectively run DB or non-DB tests",
            "options": [
                "--run-db-tests-only",
                "--skip-db-tests",
            ],
        },
        {
            "name": "Test environment",
            "options": [
                "--integration",
                "--backend",
                "--python",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
            ],
        },
        {
            "name": "Options for parallel test commands",
            "options": [
                "--run-in-parallel",
                "--use-xdist",
                "--parallelism",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
        {
            "name": "Advanced flag for tests command",
            "options": [
                "--image-tag",
                "--github-repository",
                "--use-airflow-version",
                "--mount-sources",
                "--upgrade-boto",
                "--downgrade-sqlalchemy",
                "--remove-arm-packages",
                "--skip-docker-compose-down",
            ],
        },
    ],
    "breeze testing non-db-tests": [
        {
            "name": "Select test types to run",
            "options": [
                "--parallel-test-types",
                "--excluded-parallel-test-types",
            ],
        },
        {
            "name": "Test options",
            "options": [
                "--test-timeout",
                "--enable-coverage",
                "--collect-only",
                "--skip-provider-tests",
            ],
        },
        {
            "name": "Test environment",
            "options": [
                "--python",
            ],
        },
        {
            "name": "Options for parallel test commands",
            "options": [
                "--parallelism",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
        {
            "name": "Advanced flag for tests command",
            "options": [
                "--image-tag",
                "--github-repository",
                "--use-airflow-version",
                "--mount-sources",
                "--upgrade-boto",
                "--downgrade-sqlalchemy",
                "--remove-arm-packages",
                "--skip-docker-compose-down",
            ],
        },
    ],
    "breeze testing db-tests": [
        {
            "name": "Select tests to run",
            "options": [
                "--parallel-test-types",
                "--excluded-parallel-test-types",
            ],
        },
        {
            "name": "Test options",
            "options": [
                "--test-timeout",
                "--enable-coverage",
                "--collect-only",
                "--skip-provider-tests",
            ],
        },
        {
            "name": "Test environment",
            "options": [
                "--backend",
                "--python",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
            ],
        },
        {
            "name": "Options for parallel test commands",
            "options": [
                "--parallelism",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
        {
            "name": "Advanced flag for tests command",
            "options": [
                "--image-tag",
                "--github-repository",
                "--use-airflow-version",
                "--mount-sources",
                "--upgrade-boto",
                "--downgrade-sqlalchemy",
                "--remove-arm-packages",
                "--skip-docker-compose-down",
            ],
        },
    ],
    "breeze testing integration-tests": [
        {
            "name": "Test options",
            "options": [
                "--test-timeout",
                "--enable-coverage",
                "--db-reset",
                "--skip-provider-tests",
            ],
        },
        {
            "name": "Test environment",
            "options": [
                "--integration",
                "--backend",
                "--python",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
            ],
        },
        {
            "name": "Advanced flag for integration tests command",
            "options": [
                "--image-tag",
                "--mount-sources",
                "--github-repository",
            ],
        },
    ],
    "breeze testing helm-tests": [
        {
            "name": "Flags for helms-tests command",
            "options": [
                "--helm-test-package",
                "--test-timeout",
                "--use-xdist",
                "--parallelism",
            ],
        },
        {
            "name": "Advanced flags for helms-tests command",
            "options": [
                "--image-tag",
                "--mount-sources",
                "--github-repository",
            ],
        },
    ],
    "breeze testing docker-compose-tests": [
        {
            "name": "Docker-compose tests flag",
            "options": [
                "--image-name",
                "--image-tag",
                "--python",
                "--skip-docker-compose-deletion",
                "--github-repository",
            ],
        }
    ],
}
