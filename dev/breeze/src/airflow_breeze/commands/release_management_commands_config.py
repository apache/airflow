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

RELEASE_MANAGEMENT_COMMANDS: dict[str, str | list[str]] = {
    "name": "Release management",
    "commands": [
        "verify-provider-packages",
        "prepare-provider-documentation",
        "prepare-provider-packages",
        "prepare-airflow-package",
        "release-prod-images",
        "generate-constraints",
    ],
}

RELEASE_MANAGEMENT_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze release-management prepare-airflow-package": [
        {
            "name": "Package flags",
            "options": [
                "--package-format",
                "--version-suffix-for-pypi",
                "--debug",
                "--github-repository",
            ],
        }
    ],
    "breeze release-management verify-provider-packages": [
        {
            "name": "Provider verification flags",
            "options": [
                "--use-airflow-version",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
                "--skip-constraints",
                "--debug",
                "--github-repository",
            ],
        }
    ],
    "breeze release-management prepare-provider-packages": [
        {
            "name": "Package flags",
            "options": [
                "--package-format",
                "--version-suffix-for-pypi",
                "--package-list-file",
                "--debug",
                "--github-repository",
            ],
        }
    ],
    "breeze release-management prepare-provider-documentation": [
        {
            "name": "Provider documentation preparation flags",
            "options": [
                "--debug",
                "--github-repository",
                "--base-branch",
            ],
        }
    ],
    "breeze release-management generate-constraints": [
        {
            "name": "Generate constraints flags",
            "options": [
                "--image-tag",
                "--python",
                "--airflow-constraints-mode",
                "--debug",
                "--github-repository",
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
                "--skip-cleanup",
                "--debug-resources",
            ],
        },
    ],
    "breeze release-management release-prod-images": [
        {
            "name": "Release PROD IMAGE flags",
            "options": [
                "--airflow-version",
                "--dockerhub-repo",
                "--slim-images",
                "--limit-python",
                "--limit-platform",
                "--skip-latest",
            ],
        }
    ],
    "breeze release-management generate-issue-content": [
        {
            "name": "Generate issue content flags",
            "options": [
                "--github-token",
                "--suffix",
                "--only-available-in-dist",
                "--excluded-pr-list",
                "--disable-progress",
            ],
        }
    ],
}
