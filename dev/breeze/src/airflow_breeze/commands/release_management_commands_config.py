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
        "prepare-airflow-distributions",
        "prepare-tarball",
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
        "update-providers-next-version",
        "prepare-provider-distributions",
        "install-provider-distributions",
        "verify-provider-distributions",
        "generate-providers-metadata",
        "generate-issue-content-providers",
        "clean-old-provider-artifacts",
        "tag-providers",
    ],
}

RELEASE_AIRFLOW_TASK_SDK_COMMANDS: dict[str, str | list[str]] = {
    "name": "Airflow Task SDK release commands",
    "commands": [
        "prepare-task-sdk-distributions",
    ],
}

RELEASE_AIRFLOW_CTL_COMMANDS: dict[str, str | list[str]] = {
    "name": "airflowctl release commands",
    "commands": [
        "prepare-airflow-ctl-distributions",
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
        "publish-docs-to-s3",
        "validate-rc-by-pmc",
        "check-release-files",
    ],
}

RELEASE_MANAGEMENT_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze release-management prepare-airflow-distributions": [
        {
            "name": "Package flags",
            "options": [
                "--distribution-format",
                "--version-suffix",
                "--use-local-hatch",
            ],
        }
    ],
    "breeze release-management prepare-tarball": [
        {
            "name": "Tarball flags",
            "options": ["--tarball-type", "--version", "--version-suffix"],
        }
    ],
    "breeze release-management prepare-task-sdk-distributions": [
        {
            "name": "Package flags",
            "options": [
                "--distribution-format",
                "--version-suffix",
                "--use-local-hatch",
            ],
        }
    ],
    "breeze release-management prepare-airflow-ctl-distributions": [
        {
            "name": "Package flags",
            "options": [
                "--distribution-format",
                "--version-suffix",
                "--use-local-hatch",
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
                "--version-suffix",
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
            ],
        }
    ],
    "breeze release-management verify-provider-distributions": [
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
                "--clean-airflow-installation",
                "--install-airflow-with-constraints",
                "--install-selected-providers",
                "--distribution-format",
                "--providers-constraints-location",
                "--providers-constraints-mode",
                "--providers-constraints-reference",
                "--providers-skip-constraints",
                "--use-airflow-version",
                "--allow-pre-releases",
                "--use-distributions-from-dist",
            ],
        },
    ],
    "breeze release-management install-provider-distributions": [
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
                "--clean-airflow-installation",
                "--install-airflow-with-constraints",
                "--install-selected-providers",
                "--distribution-format",
                "--providers-constraints-location",
                "--providers-constraints-mode",
                "--providers-constraints-reference",
                "--providers-skip-constraints",
                "--use-airflow-version",
                "--allow-pre-releases",
                "--use-distributions-from-dist",
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
    "breeze release-management prepare-provider-distributions": [
        {
            "name": "Package flags",
            "options": [
                "--clean-dist",
                "--github-repository",
                "--include-not-ready-providers",
                "--include-removed-providers",
                "--distribution-format",
                "--distributions-list-file",
                "--skip-deleting-generated-files",
                "--skip-tag-check",
                "--version-suffix",
                "--distributions-list",
            ],
        }
    ],
    "breeze release-management tag-providers": [
        {
            "name": "Add tags to providers",
            "options": [
                "--clean-tags",
                "--release-date",
            ],
        },
    ],
    "breeze release-management prepare-provider-documentation": [
        {
            "name": "Documentation generation mode",
            "options": [
                "--release-date",
                "--incremental-update",
                "--only-min-version-update",
                "--reapply-templates-only",
                "--non-interactive",
            ],
        },
        {
            "name": "Select non-regular providers",
            "options": [
                "--include-not-ready-providers",
                "--include-removed-providers",
            ],
        },
        {
            "name": "Skip steps",
            "options": [
                "--skip-git-fetch",
                "--skip-changelog",
                "--skip-readme",
            ],
        },
        {
            "name": "Advanced options",
            "options": [
                "--base-branch",
                "--github-repository",
            ],
        },
    ],
    "breeze release-management update-providers-next-version": [],
    "breeze release-management prepare-python-client": [
        {
            "name": "Python client preparation flags",
            "options": [
                "--distribution-format",
                "--version-suffix",
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
                "--github-repository",
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
            "name": "Select images to release",
            "options": [
                "--airflow-version",
                "--python",
                "--platform",
                "--slim-images",
            ],
        },
        {
            "name": "Prepare digest only images",
            "options": [
                "--metadata-folder",
            ],
        },
        {
            "name": "Additional options for release",
            "options": [
                "--commit-sha",
                "--dockerhub-repo",
                "--include-pre-release",
                "--skip-latest",
            ],
        },
    ],
    "breeze release-management merge-prod-images": [
        {
            "name": "Select images to merge",
            "options": [
                "--airflow-version",
                "--python",
                "--slim-images",
                "--metadata-folder",
            ],
        },
        {
            "name": "Extra options for merge",
            "options": [
                "--skip-latest",
                "--dockerhub-repo",
            ],
        },
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
                "--distributions-list",
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--debug-resources",
                "--parallelism",
                "--run-in-parallel",
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
                "--head-repo",
                "--head-ref",
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
        {
            "name": "Generate providers metadata flags",
            "options": ["--refresh-constraints-and-airflow-releases", "--github-token"],
        },
        {
            "name": "Debug options",
            "options": ["--provider-id", "--provider-version"],
        },
    ],
    "breeze release-management start-rc-process": [
        {
            "name": "Start RC process flags",
            "options": [
                "--version",
                "--previous-version",
                "--task-sdk-version",
                "--github-token",
                "--remote-name",
                "--sync-branch",
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
        {
            "name": "Start release flags",
            "options": ["--version", "--task-sdk-version"],
        }
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
    "breeze release-management publish-docs-to-s3": [
        {
            "name": "Publish docs to S3",
            "options": [
                "--source-dir-path",
                "--destination-location",
                "--exclude-docs",
                "--dry-run",
                "--overwrite",
                "--parallelism",
                "--stable-versions",
                "--publish-all-docs",
                "--skip-write-to-stable-folder",
            ],
        }
    ],
    "breeze release-management constraints-version-check": [
        {
            "name": "Constraints options.",
            "options": [
                "--python",
                "--airflow-constraints-mode",
                "--github-repository",
                "--github-token",
            ],
        },
        {
            "name": "Comparison mode.",
            "options": [
                "--diff-mode",
                "--package",
                "--explain-why",
            ],
        },
        {
            "name": "Build options.",
            "options": [
                "--builder",
            ],
        },
    ],
    "breeze release-management validate-rc-by-pmc": [
        {
            "name": "Validation options",
            "options": [
                "--distribution",
                "--version",
                "--task-sdk-version",
                "--path-to-airflow-svn",
                "--checks",
            ],
        },
    ],
    "breeze release-management check-release-files": [
        {
            "name": "Check release files flags",
            "options": [
                "--path-to-airflow-svn",
                "--version",
                "--release-date",
                "--packages-file",
            ],
        }
    ],
}
