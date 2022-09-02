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
from typing import Dict, List, Union

RELEASE_MANAGEMENT_COMMANDS: Dict[str, Union[str, List[str]]] = {
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

RELEASE_MANAGEMENT_PARAMETERS: Dict[str, List[Dict[str, Union[str, List[str]]]]] = {
    "breeze release-management prepare-airflow-package": [
        {"name": "Package flags", "options": ["--package-format", "--version-suffix-for-pypi"]}
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
            ],
        }
    ],
    "breeze release-management prepare-provider-documentation": [
        {
            "name": "Provider documentation preparation flags",
            "options": [
                "--debug",
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
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--skip-cleanup",
                "--python-versions",
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
}
