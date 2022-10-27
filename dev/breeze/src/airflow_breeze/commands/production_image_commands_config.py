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

PRODUCTION_IMAGE_TOOLS_COMMANDS: dict[str, str | list[str]] = {
    "name": "Production Image tools",
    "commands": [
        "build",
        "pull",
        "verify",
    ],
}
PRODUCTION_IMAGE_TOOLS_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze prod-image build": [
        {
            "name": "Basic usage",
            "options": [
                "--python",
                "--install-airflow-version",
                "--upgrade-to-newer-dependencies",
                "--upgrade-on-failure",
                "--image-tag",
                "--tag-as-latest",
                "--docker-cache",
                "--github-repository",
            ],
        },
        {
            "name": "Building images in parallel",
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
            "name": "Options for customizing images",
            "options": [
                "--builder",
                "--install-providers-from-sources",
                "--airflow-extras",
                "--airflow-constraints-mode",
                "--airflow-constraints-reference",
                "--python-image",
                "--additional-extras",
                "--additional-pip-install-flags",
                "--additional-python-deps",
                "--additional-runtime-apt-deps",
                "--additional-runtime-apt-env",
                "--additional-runtime-apt-command",
                "--additional-dev-apt-deps",
                "--additional-dev-apt-env",
                "--additional-dev-apt-command",
                "--runtime-apt-deps",
                "--runtime-apt-command",
                "--dev-apt-deps",
                "--dev-apt-command",
            ],
        },
        {
            "name": "Customization options (for specific customization needs)",
            "options": [
                "--install-packages-from-context",
                "--cleanup-context",
                "--disable-mysql-client-installation",
                "--disable-mssql-client-installation",
                "--disable-postgres-client-installation",
                "--disable-airflow-repo-cache",
                "--install-airflow-reference",
                "--installation-method",
            ],
        },
        {
            "name": "Preparing cache and push (for maintainers and CI)",
            "options": [
                "--github-token",
                "--github-username",
                "--platform",
                "--login-to-github-registry",
                "--push",
                "--empty-image",
                "--prepare-buildx-cache",
            ],
        },
    ],
    "breeze prod-image pull": [
        {
            "name": "Pull image flags",
            "options": [
                "--image-tag",
                "--python",
                "--github-token",
                "--verify",
                "--wait-for-image",
                "--tag-as-latest",
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
                "--include-success-outputs",
            ],
        },
    ],
    "breeze prod-image verify": [
        {
            "name": "Verify image flags",
            "options": [
                "--image-name",
                "--python",
                "--slim-image",
                "--image-tag",
                "--pull",
                "--github-repository",
            ],
        }
    ],
}
