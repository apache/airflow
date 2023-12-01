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

RELEASE_AIRFLOW_COMMANDS: dict[str, str | list[str]] = {
    "name": "Airflow release commands",
    "commands": [
        "prepare-airflow-package",
        "create-minor-branch",
        "start-rc-process",
        "start-release",
        "release-prod-images",
    ],
}

RELEASE_PROVIDERS_COMMANDS: dict[str, str | list[str]] = {
    "name": "Providers release commands",
    "commands": [
        "prepare-provider-documentation",
        "prepare-provider-packages",
        "install-provider-packages",
        "verify-provider-packages",
        "generate-providers-metadata",
        "generate-issue-content-providers",
    ],
}

RELEASE_OTHER_COMMANDS: dict[str, str | list[str]] = {
    "name": "Other release commands",
    "commands": [
        "publish-docs",
        "generate-constraints",
        "add-back-references",
    ],
}

RELEASE_MANAGEMENT_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze release-management prepare-airflow-package": [
        {
            "name": "Package flags",
            "options": [
                "--package-format",
                "--use-container-for-assets-compilation",
                "--version-suffix-for-pypi",
            ],
        }
    ],
    "breeze release-management verify-provider-packages": [
        {
            "name": "Provider verification flags",
            "options": [
                "--use-airflow-version",
                "--install-selected-providers",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--use-packages-from-dist",
                "--package-format",
                "--skip-constraints",
                "--debug",
                "--github-repository",
            ],
        },
    ],
    "breeze release-management install-provider-packages": [
        {
            "name": "Provider installation flags",
            "options": [
                "--use-airflow-version",
                "--install-selected-providers",
                "--airflow-constraints-reference",
                "--airflow-extras",
                "--package-format",
                "--skip-constraints",
                "--debug",
                "--github-repository",
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--skip-cleanup",
                "--include-success-outputs",
                "--debug-resources",
            ],
        },
    ],
    "breeze release-management prepare-provider-packages": [
        {
            "name": "Package flags",
            "options": [
                "--package-format",
                "--version-suffix-for-pypi",
                "--clean-dist",
                "--skip-tag-check",
                "--skip-deleting-generated-files",
                "--package-list-file",
                "--github-repository",
            ],
        }
    ],
    "breeze release-management prepare-provider-documentation": [
        {
            "name": "Provider documentation preparation flags",
            "options": [
                "--github-repository",
                "--skip-git-fetch",
                "--base-branch",
                "--only-min-version-update",
                "--reapply-templates-only",
                "--non-interactive",
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
                "--chicken-egg-providers",
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
                "--commit-sha",
            ],
        }
    ],
    "breeze release-management publish-docs": [
        {
            "name": "Publish Docs",
            "options": [
                "--override-versioned",
                "--package-filter",
                "--airflow-site-directory",
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--skip-cleanup",
                "--debug-resources",
                "--include-success-outputs",
            ],
        },
    ],
    "breeze release-management add-back-references": [
        {
            "name": "Add Back References to Docs",
            "options": ["--airflow-site-directory"],
        },
    ],
    "breeze release-management generate-issue-content-providers": [
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
    "breeze release-management generate-providers-metadata": [
        {"name": "Generate providers metadata flags", "options": ["--refresh-constraints", "--python"]}
    ],
    "breeze release-management start-rc-process": [
        {
            "name": "Start RC process flags",
            "options": [
                "--version",
                "--previous-version",
                "--github-token",
            ],
        }
    ],
    "breeze release-management create-minor-branch": [
        {
            "name": "Create minor branch flags",
            "options": [
                "--version-branch",
            ],
        }
    ],
    "breeze release-management start-release": [
        {"name": "Start release flags", "options": ["--release-candidate", "--previous-release"]}
    ],
    "breeze release-management update-constraints": [
        {
            "name": "Update constraints flags",
            "options": [
                "--constraints-repo",
                "--commit-message",
                "--remote-name",
            ],
        },
        {
            "name": "Selection criteria",
            "options": [
                "--airflow-versions",
                "--airflow-constraints-mode",
            ],
        },
        {
            "name": "Action to perform",
            "options": [
                "--updated-constraint",
                "--comment-file",
            ],
        },
    ],
}
