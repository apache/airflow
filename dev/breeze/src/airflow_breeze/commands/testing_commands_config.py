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
        "--run-in-parallel",
        "--use-xdist",
        "--parallelism",
        "--skip-cleanup",
        "--debug-resources",
        "--include-success-outputs",
        "--total-test-timeout",
    ],
}

TEST_UPGRADING_PACKAGES: dict[str, str | list[str]] = {
    "name": "Upgrading/downgrading/removing selected packages",
    "options": [
        "--upgrade-boto",
        "--upgrade-sqlalchemy",
        "--downgrade-sqlalchemy",
        "--downgrade-pendulum",
    ],
}

TEST_ADVANCED_FLAGS: dict[str, str | list[str]] = {
    "name": "Advanced flag for tests command",
    "options": [
        "--github-repository",
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
        "--distribution-format",
        "--use-airflow-version",
        "--allow-pre-releases",
        "--use-distributions-from-dist",
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
    TEST_PARALLELISM_OPTIONS,
    TEST_UPGRADING_PACKAGES,
]

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
        "commands": ["task-sdk-tests", "task-sdk-integration-tests"],
    },
    {
        "name": "airflowctl Tests",
        "commands": ["airflow-ctl-tests", "airflow-ctl-integration-tests"],
    },
    {
        "name": "Other Tests",
        "commands": [
            "system-tests",
            "helm-tests",
            "docker-compose-tests",
            "python-api-client-tests",
            "airflow-e2e-tests",
        ],
    },
    {
        "name": "UI Tests",
        "commands": ["ui-e2e-tests"],
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
    ],
    "breeze testing task-sdk-integration-tests": [
        {
            "name": "Docker-compose tests flag",
            "options": [
                "--image-name",
                "--python",
                "--task-sdk-version",
                "--skip-docker-compose-deletion",
                "--skip-mounting-local-volumes",
                "--down",
            ],
        },
        {
            "name": "Common CI options",
            "options": [
                "--include-success-outputs",
                "--github-repository",
            ],
        },
    ],
    "breeze testing airflow-ctl-tests": [
        {
            "name": "Test environment",
            "options": [
                "--python",
                "--parallelism",
            ],
        },
    ],
    "breeze testing airflow-ctl-integration-tests": [
        {
            "name": "Docker-compose tests flag",
            "options": [
                "--image-name",
                "--python",
                "--skip-docker-compose-deletion",
                "--airflow-ctl-version",
            ],
        },
        {
            "name": "Common CI options",
            "options": [
                "--include-success-outputs",
                "--github-repository",
            ],
        },
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
        TEST_ADVANCED_FLAGS_FOR_INSTALLATION,
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
                "--mount-sources",
            ],
        },
    ],
    "breeze testing docker-compose-tests": [
        {
            "name": "Docker-compose tests flag",
            "options": [
                "--image-name",
                "--python",
                "--skip-docker-compose-deletion",
                "--include-success-outputs",
                "--github-repository",
            ],
        }
    ],
    "breeze testing python-api-client-tests": [
        {
            "name": "Advanced flag for tests command",
            "options": [
                "--github-repository",
                "--skip-docker-compose-down",
                "--keep-env-variables",
            ],
        },
        TEST_OPTIONS_DB,
        TEST_ENVIRONMENT_DB,
    ],
    "breeze testing airflow-e2e-tests": [
        {
            "name": "Airflow E2E tests flags",
            "options": [
                "--image-name",
                "--python",
                "--skip-docker-compose-deletion",
                "--include-success-outputs",
                "--github-repository",
                "--e2e-test-mode",
            ],
        }
    ],
    "breeze testing ui-e2e-tests": [
        {
            "name": "UI End-to-End test options",
            "options": [
                "--browser",
                "--headed",
                "--debug-e2e",
                "--ui-mode",
                "--test-pattern",
                "--workers",
                "--timeout",
                "--reporter",
            ],
        },
        {
            "name": "Test environment for UI tests",
            "options": [
                "--airflow-ui-base-url",
                "--test-admin-username",
                "--test-admin-password",
            ],
        },
        {
            "name": "Advanced flags for UI e2e tests",
            "options": [
                "--force-reinstall-deps",
            ],
        },
    ],
}
