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

TEST_OPTIONS_NON_DB: dict[str, str | list[str]] = {
    "name": "Test options",
    "options": [
        "--test-timeout",
        "--enable-coverage",
        "--collect-only",
    ],
}

TEST_OPTIONS_DB: dict[str, str | list[str]] = {
    "name": "Test options",
    "options": [
        "--test-timeout",
        "--enable-coverage",
        "--collect-only",
        "--db-reset",
    ],
}

TEST_ENVIRONMENT_DB: dict[str, str | list[str]] = {
    "name": "Test environment",
    "options": [
        "--backend",
        "--no-db-cleanup",
        "--python",
        "--postgres-version",
        "--mysql-version",
        "--forward-credentials",
        "--force-sa-warnings",
    ],
}

TEST_PARALLELISM_OPTIONS: dict[str, str | list[str]] = {
    "name": "Options for parallel test commands",
    "options": [
        "--parallelism",
        "--skip-cleanup",
        "--debug-resources",
        "--include-success-outputs",
    ],
}

TEST_PARALLELISM_OPTIONS_XDIST: dict[str, str | list[str]] = {
    "name": "Options for parallel test commands",
    "options": [
        "--run-in-parallel",
        "--use-xdist",
        "--parallelism",
        "--skip-cleanup",
        "--debug-resources",
        "--include-success-outputs",
    ],
}

TEST_UPGRADING_PACKAGES: dict[str, str | list[str]] = {
    "name": "Upgrading/downgrading/removing selected packages",
    "options": [
        "--upgrade-boto",
        "--downgrade-sqlalchemy",
        "--downgrade-pendulum",
        "--remove-arm-packages",
    ],
}

TEST_ADVANCED_FLAGS: dict[str, str | list[str]] = {
    "name": "Advanced flag for tests command",
    "options": [
        "--github-repository",
        "--image-tag",
        "--mount-sources",
        "--skip-docker-compose-down",
        "--keep-env-variables",
    ],
}

TEST_ADVANCED_FLAGS_FOR_INSTALLATION: dict[str, str | list[str]] = {
    "name": "Advanced flag for installing airflow in container",
    "options": [
        "--airflow-constraints-reference",
        "--clean-airflow-installation",
        "--force-lowest-dependencies",
        "--install-airflow-with-constraints",
        "--package-format",
        "--use-airflow-version",
        "--use-packages-from-dist",
    ],
}

TEST_ADVANCED_FLAGS_FOR_PROVIDERS: dict[str, str | list[str]] = {
    "name": "Advanced flag for provider tests command",
    "options": [
        "--excluded-providers",
        "--providers-constraints-location",
        "--providers-skip-constraints",
        "--skip-providers",
    ],
}


TEST_PARAMS: list[dict[str, str | list[str]]] = [
    {
        "name": "Select test types to run (tests can also be selected by command args individually)",
        "options": [
            "--test-type",
            "--parallel-test-types",
            "--excluded-parallel-test-types",
        ],
    },
    TEST_OPTIONS_DB,
    {
        "name": "Selectively run DB or non-DB tests",
        "options": [
            "--run-db-tests-only",
            "--skip-db-tests",
        ],
    },
    TEST_ENVIRONMENT_DB,
    TEST_PARALLELISM_OPTIONS_XDIST,
    TEST_UPGRADING_PACKAGES,
]

TEST_PARAMS_NON_DB: list[dict[str, str | list[str]]] = [
    {
        "name": "Select test types to run",
        "options": [
            "--parallel-test-types",
            "--excluded-parallel-test-types",
        ],
    },
    TEST_OPTIONS_NON_DB,
    {
        "name": "Test environment",
        "options": [
            "--python",
            "--forward-credentials",
            "--force-sa-warnings",
        ],
    },
    TEST_PARALLELISM_OPTIONS,
    TEST_UPGRADING_PACKAGES,
]

TEST_PARAMS_DB: list[dict[str, str | list[str]]] = [
    {
        "name": "Select tests to run",
        "options": [
            "--parallel-test-types",
            "--excluded-parallel-test-types",
        ],
    },
    TEST_OPTIONS_DB,
    TEST_ENVIRONMENT_DB,
    TEST_PARALLELISM_OPTIONS,
    TEST_UPGRADING_PACKAGES,
]

DATABASE_ISOLATION_TESTS: dict[str, str | list[str]] = {
    "name": "DB isolation tests",
    "options": [
        "--database-isolation",
    ],
}

INTEGRATION_TESTS: dict[str, str | list[str]] = {
    "name": "Integration tests",
    "options": [
        "--integration",
    ],
}


TESTING_COMMANDS: list[dict[str, str | list[str]]] = [
    {
        "name": "Core Tests",
        "commands": [
            "core-tests",
            "core-integration-tests",
        ],
    },
    {
        "name": "Providers Tests",
        "commands": ["providers-tests", "providers-integration-tests"],
    },
    {
        "name": "Task SDK Tests",
        "commands": ["task-sdk-tests"],
    },
    {
        "name": "Other Tests",
        "commands": ["system-tests", "helm-tests", "docker-compose-tests"],
    },
]

TESTING_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze testing core-tests": [
        *TEST_PARAMS,
        TEST_ADVANCED_FLAGS,
        TEST_ADVANCED_FLAGS_FOR_INSTALLATION,
    ],
    "breeze testing providers-tests": [
        *TEST_PARAMS,
        DATABASE_ISOLATION_TESTS,
        TEST_ADVANCED_FLAGS,
        TEST_ADVANCED_FLAGS_FOR_INSTALLATION,
        TEST_ADVANCED_FLAGS_FOR_PROVIDERS,
    ],
    "breeze testing task-sdk-tests": [
        TEST_OPTIONS_NON_DB,
        {
            "name": "Test environment",
            "options": [
                "--python",
                "--forward-credentials",
                "--force-sa-warnings",
            ],
        },
        TEST_ADVANCED_FLAGS,
        TEST_PARALLELISM_OPTIONS,
    ],
    "breeze testing core-integration-tests": [
        TEST_OPTIONS_DB,
        TEST_ENVIRONMENT_DB,
        INTEGRATION_TESTS,
        TEST_ADVANCED_FLAGS,
    ],
    "breeze testing providers-integration-tests": [
        TEST_OPTIONS_DB,
        TEST_ENVIRONMENT_DB,
        INTEGRATION_TESTS,
        TEST_ADVANCED_FLAGS,
    ],
    "breeze testing system-tests": [
        TEST_OPTIONS_DB,
        TEST_ENVIRONMENT_DB,
        TEST_ADVANCED_FLAGS,
    ],
    "breeze testing helm-tests": [
        {
            "name": "Flags for helms-tests command",
            "options": [
                "--test-type",
                "--test-timeout",
                "--use-xdist",
                "--parallelism",
            ],
        },
        {
            "name": "Advanced flag for helm-test command",
            "options": [
                "--github-repository",
                "--image-tag",
                "--mount-sources",
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
