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

WORKFLOW_RUN_COMMANDS: dict[str, str | list[str]] = {
    "name": "Airflow github actions workflow commands",
    "commands": ["publish-docs"],
}

WORKFLOW_RUN_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze workflow-run publish-docs": [
        {
            "name": "Select branch or tag to build docs",
            "options": [
                "--ref",
                "--skip-tag-validation",
                "--apply-commits",
                "--workflow-branch",
            ],
        },
        {
            "name": "Optional airflow versions to build.",
            "options": [
                "--airflow-version",
                "--airflow-base-version",
            ],
        },
        {
            "name": "Select docs to exclude and destination",
            "options": [
                "--exclude-docs",
                "--site-env",
                "--skip-write-to-stable-folder",
            ],
        },
    ],
}
