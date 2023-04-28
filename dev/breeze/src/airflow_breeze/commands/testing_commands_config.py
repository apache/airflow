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
            "name": "Basic flag for tests command",
            "options": [
                "--test-type",
                "--test-timeout",
                "--collect-only",
                "--db-reset",
                "--backend",
                "--python",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--integration",
                "--github-repository",
            ],
        },
        {
            "name": "Options for parallel test commands",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--parallel-test-types",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
        {
            "name": "Advanced flag for tests command",
            "options": [
                "--image-tag",
                "--mount-sources",
                "--upgrade-boto",
                "--remove-arm-packages",
            ],
        },
    ],
    "breeze testing integration-tests": [
        {
            "name": "Basic flag for integration tests command",
            "options": [
                "--integration",
                "--test-timeout",
                "--db-reset",
                "--backend",
                "--python",
                "--postgres-version",
                "--mysql-version",
                "--mssql-version",
                "--github-repository",
            ],
        },
        {
            "name": "Advanced flag for integration tests command",
            "options": [
                "--image-tag",
                "--mount-sources",
                "--skip-provider-tests",
            ],
        },
    ],
    "breeze testing helm-tests": [
        {
            "name": "Advanced flag for helms-tests command",
            "options": [
                "--image-tag",
                "--mount-sources",
                "--helm-test-package",
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
                "--github-repository",
            ],
        }
    ],
}
