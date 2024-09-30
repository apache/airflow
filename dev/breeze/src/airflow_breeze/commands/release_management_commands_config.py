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
        "create-minor-branch",
        "prepare-airflow-package",
        "prepare-airflow-tarball",
        "start-rc-process",
        "start-release",
        "release-prod-images",
        "generate-issue-content-core",
    ],
}

RELEASE_HELM_COMMANDS: dict[str, str | list[str]] = {
    "name": "Helm release commands",
    "commands": [
        "prepare-helm-chart-tarball",
        "prepare-helm-chart-package",
        "generate-issue-content-helm-chart",
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
        "clean-old-provider-artifacts",
        "tag-providers",
    ],
}


RELEASE_OTHER_COMMANDS: dict[str, str | list[str]] = {
    "name": "Other release commands",
    "commands": [
        "add-back-references",
        "prepare-python-client",
        "publish-docs",
        "generate-constraints",
        "update-constraints",
    ],
}

RELEASE_MANAGEMENT_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze release-management prepare-airflow-package": [
        {
            "name": "Package flags",
            "options": [
                "--package-format",
                "--version-suffix-for-pypi",
                "--use-local-hatch",
            ],
        }
    ],
    "breeze release-management prepare-airflow-tarball": [
        {
            "name": "Package flags",
            "options": [
                "--version",
            ],
        }
    ],
    "breeze release-management prepare-helm-chart-tarball": [
        {
            "name": "Package flags",
            "options": [
                "--version",
                "--version-suffix",
                "--ignore-version-check",
                "--override-tag",
                "--skip-tagging",
                "--skip-tag-signing",
            ],
        }
    ],
    "breeze release-management prepare-helm-chart-package": [
        {
            "name": "Package flags",
            "options": [
                "--sign-email",
            ],
        }
    ],
    "breeze release-management generate-issue-content-helm-chart": [
        {
            "name": "Generate issue flags",
            "options": [
                "--github-token",
                "--previous-release",
                "--current-release",
                "--excluded-pr-list",
                "--limit-pr-count",
                "--latest",
            ],
        }
    ],
    "breeze release-management verify-provider-packages": [
        {
            "name": "Provider verification flags",
            "options": [
                "--python",
                "--mount-sources",
                "--github-repository",
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
                "--install-airflow-with-constraints",
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
    ],
    "breeze release-management install-provider-packages": [
        {
            "name": "Provider installation flags",
            "options": [
                "--python",
                "--mount-sources",
                "--github-repository",
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
                "--clean-dist",
                "--github-repository",
                "--include-not-ready-providers",
                "--include-removed-providers",
                "--package-format",
                "--package-list-file",
                "--skip-deleting-generated-files",
                "--skip-tag-check",
                "--version-suffix-for-pypi",
                "--package-list",
            ],
        }
    ],
    "breeze release-management tag-providers": [
        {
            "name": "Add tags to providers",
            "options": [
                "--clean-local-tags",
            ],
        },
    ],
    "breeze release-management prepare-provider-documentation": [
        {
            "name": "Provider documentation preparation flags",
            "options": [
                "--base-branch",
                "--github-repository",
                "--include-not-ready-providers",
                "--include-removed-providers",
                "--non-interactive",
                "--only-min-version-update",
                "--reapply-templates-only",
                "--skip-git-fetch",
            ],
        }
    ],
    "breeze release-management prepare-python-client": [
        {
            "name": "Python client preparation flags",
            "options": [
                "--package-format",
                "--version-suffix-for-pypi",
                "--use-local-hatch",
                "--python-client-repo",
                "--only-publish-build-scripts",
                "--security-schemes",
            ],
        }
    ],
    "breeze release-management generate-constraints": [
        {
            "name": "Generate constraints flags",
            "options": [
                "--airflow-constraints-mode",
                "--chicken-egg-providers",
                "--github-repository",
                "--image-tag",
                "--python",
                "--use-uv",
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--debug-resources",
                "--parallelism",
                "--python-versions",
                "--run-in-parallel",
                "--skip-cleanup",
            ],
        },
    ],
    "breeze release-management release-prod-images": [
        {
            "name": "Release PROD IMAGE flags",
            "options": [
                "--airflow-version",
                "--chicken-egg-providers",
                "--commit-sha",
                "--dockerhub-repo",
                "--limit-python",
                "--limit-platform",
                "--skip-latest",
                "--slim-images",
            ],
        }
    ],
    "breeze release-management generate-issue-content-core": [
        {
            "name": "Generate issue flags",
            "options": [
                "--github-token",
                "--previous-release",
                "--current-release",
                "--excluded-pr-list",
                "--limit-pr-count",
                "--latest",
            ],
        }
    ],
    "breeze release-management publish-docs": [
        {
            "name": "Publish Docs",
            "options": [
                "--airflow-site-directory",
                "--include-not-ready-providers",
                "--include-removed-providers",
                "--override-versioned",
                "--package-filter",
                "--package-list",
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--debug-resources",
                "--include-success-outputs",
                "--parallelism",
                "--run-in-parallel",
                "--skip-cleanup",
            ],
        },
    ],
    "breeze release-management add-back-references": [
        {
            "name": "Add Back References to Docs",
            "options": [
                "--airflow-site-directory",
                "--include-not-ready-providers",
                "--include-removed-providers",
            ],
        },
    ],
    "breeze release-management generate-issue-content-providers": [
        {
            "name": "Generate issue content flags",
            "options": [
                "--disable-progress",
                "--excluded-pr-list",
                "--github-token",
                "--only-available-in-dist",
            ],
        }
    ],
    "breeze release-management clean-old-provider-artifacts": [
        {
            "name": "Cleans the old provider artifacts",
            "options": ["--directory"],
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
