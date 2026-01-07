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

CI_IMAGE_TOOLS_COMMANDS: dict[str, str | list[str]] = {
    "name": "CI Image tools",
    "commands": [
        "build",
        "pull",
        "verify",
        "save",
        "load",
        "export-mount-cache",
        "import-mount-cache",
    ],
}
CI_IMAGE_TOOLS_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze ci-image build": [
        {
            "name": "Basic usage",
            "options": [
                "--python",
                "--upgrade-to-newer-dependencies",
                "--upgrade-on-failure",
                "--docker-cache",
                "--version-suffix",
                "--build-progress",
                "--docker-host",
            ],
        },
        {
            "name": "Building images in parallel",
            "options": [
                "--debug-resources",
                "--include-success-outputs",
                "--parallelism",
                "--python-versions",
                "--run-in-parallel",
                "--skip-cleanup",
            ],
        },
        {
            "name": "Advanced build options (for power users)",
            "options": [
                "--additional-pip-install-flags",
                "--commit-sha",
                "--debian-version",
                "--disable-airflow-repo-cache",
                "--install-mysql-client-type",
                "--python-image",
                "--use-uv",
                "--uv-http-timeout",
            ],
        },
        {
            "name": "Selecting constraint location (for power users)",
            "options": [
                "--airflow-constraints-location",
                "--airflow-constraints-mode",
                "--airflow-constraints-reference",
            ],
        },
        {
            "name": "Choosing dependencies and extras (for power users)",
            "options": [
                "--additional-airflow-extras",
                "--additional-python-deps",
                "--dev-apt-deps",
                "--additional-dev-apt-deps",
                "--dev-apt-command",
                "--additional-dev-apt-command",
                "--additional-dev-apt-env",
            ],
        },
        {
            "name": "Preparing cache and push (for maintainers and CI)",
            "options": [
                "--builder",
                "--platform",
                "--push",
                "--prepare-buildx-cache",
            ],
        },
        {
            "name": "GitHub authentication",
            "options": [
                "--github-repository",
                "--github-token",
            ],
        },
    ],
    "breeze ci-image pull": [
        {
            "name": "Pull image flags",
            "options": [
                "--python",
                "--verify",
                "--wait-for-image",
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
                "--include-success-outputs",
            ],
        },
        {
            "name": "GitHub authentication",
            "options": [
                "--github-repository",
                "--github-token",
            ],
        },
    ],
    "breeze ci-image verify": [
        {
            "name": "Verify image flags",
            "options": [
                "--image-name",
                "--python",
                "--pull",
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
                "--include-success-outputs",
            ],
        },
        {
            "name": "GitHub authentication",
            "options": [
                "--github-repository",
                "--github-token",
            ],
        },
    ],
    "breeze ci-image save": [
        {
            "name": "Save image flags",
            "options": [
                "--python",
                "--platform",
                "--github-repository",
                "--image-file",
                "--image-file-dir",
                "--action-branch",
            ],
        },
    ],
    "breeze ci-image load": [
        {
            "name": "Load image flags",
            "options": [
                "--python",
                "--platform",
                "--image-file",
                "--image-file-dir",
                "--github-repository",
                "--github-token",
                "--from-pr",
                "--from-run",
                "--skip-image-file-deletion",
                "--action-branch",
            ],
        },
    ],
    "breeze ci-image export-mount-cache": [
        {
            "name": "Export flags",
            "options": [
                "--cache-file",
                "--builder",
            ],
        },
    ],
    "breeze ci-image import-mount-cache": [
        {
            "name": "Import flags",
            "options": [
                "--cache-file",
                "--builder",
            ],
        },
    ],
}
