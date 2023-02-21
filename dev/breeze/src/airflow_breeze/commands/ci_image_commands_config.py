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
                "--image-tag",
                "--tag-as-latest",
                "--docker-cache",
                "--force-build",
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
            "name": "Advanced options (for power users)",
            "options": [
                "--builder",
                "--install-providers-from-sources",
                "--airflow-constraints-mode",
                "--airflow-constraints-reference",
                "--python-image",
                "--additional-python-deps",
                "--additional-extras",
                "--additional-pip-install-flags",
                "--additional-dev-apt-deps",
                "--additional-dev-apt-env",
                "--additional-dev-apt-command",
                "--dev-apt-deps",
                "--dev-apt-command",
            ],
        },
        {
            "name": "Preparing cache and push (for maintainers and CI)",
            "options": [
                "--github-token",
                "--github-username",
                "--platform",
                "--push",
                "--empty-image",
                "--prepare-buildx-cache",
            ],
        },
    ],
    "breeze ci-image pull": [
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
    "breeze ci-image verify": [
        {
            "name": "Verify image flags",
            "options": [
                "--image-name",
                "--python",
                "--image-tag",
                "--pull",
                "--github-repository",
            ],
        }
    ],
}
