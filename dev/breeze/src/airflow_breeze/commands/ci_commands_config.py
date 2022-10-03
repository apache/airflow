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

CI_COMMANDS: dict[str, str | list[str]] = {
    "name": "CI commands",
    "commands": [
        "fix-ownership",
        "free-space",
        "resource-check",
        "selective-check",
        "find-newer-dependencies",
        "get-workflow-info",
    ],
}
CI_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze ci fix-ownership": [
        {
            "name": "Fix ownership flags",
            "options": [
                "--use-sudo",
            ],
        }
    ],
    "breeze ci selective-check": [
        {
            "name": "Selective check flags",
            "options": [
                "--commit-ref",
                "--pr-labels",
                "--default-branch",
                "--default-constraints-branch",
                "--github-event-name",
            ],
        }
    ],
    "breeze ci find-newer-dependencies": [
        {
            "name": "Find newer dependencies flags",
            "options": [
                "--python",
                "--timezone",
                "--airflow-constraints-reference",
                "--updated-on-or-after",
                "--max-age",
            ],
        }
    ],
    "breeze ci get-workflow-info": [
        {
            "name": "Get workflow info flags",
            "options": [
                "--github-context",
                "--github-context-input",
            ],
        }
    ],
}
