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

SBOM_COMMANDS: dict[str, str | list[str]] = {
    "name": "SBOM commands",
    "commands": [
        "update-sbom-information",
        "build-all-airflow-images",
        "generate-providers-requirements",
        "export-dependency-information",
    ],
}

SBOM_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze sbom update-sbom-information": [
        {
            "name": "Update SBOM information flags",
            "options": [
                "--airflow-site-directory",
                "--airflow-version",
                "--python",
                "--include-provider-dependencies",
                "--include-python",
                "--include-npm",
                "--all-combinations",
                "--package-filter",
                "--force",
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
    "breeze sbom build-all-airflow-images": [
        {
            "name": "Generate all airflow images flags",
            "options": [
                "--python",
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
    "breeze sbom generate-providers-requirements": [
        {
            "name": "Generate provider requirements flags",
            "options": [
                "--python",
                "--provider-id",
                "--provider-version",
                "--force",
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
    "breeze sbom export-dependency-information": [
        {
            "name": "Export dependency information flags",
            "options": [
                "--airflow-version",
                "--python",
                "--include-open-psf-scorecard",
                "--include-github-stats",
                "--include-actions",
            ],
        },
        {
            "name": "Github auth flags",
            "options": [
                "--github-token",
            ],
        },
        {
            "name": "Google spreadsheet flags",
            "options": [
                "--json-credentials-file",
                "--google-spreadsheet-id",
            ],
        },
        {
            "name": "Debugging flags",
            "options": [
                "--limit-output",
                "--project-name",
            ],
        },
    ],
}
