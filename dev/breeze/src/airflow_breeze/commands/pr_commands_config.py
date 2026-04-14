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

PR_COMMANDS: dict[str, str | list[str]] = {
    "name": "PR commands",
    "commands": ["auto-triage", "stats"],
}

PR_PARAMETERS: dict[str, list[dict[str, str | list[str]]]] = {
    "breeze pr auto-triage": [
        {
            "name": "Mode",
            "options": ["--mode", "--tui", "--llm-use"],
        },
        {
            "name": "Select people",
            "options": [
                "--author",
                "--authors",
                "--reviews-for-me",
                "--reviews-for",
            ],
        },
        {
            "name": "Target selection",
            "options": ["--pr"],
        },
        {
            "name": "Filter options",
            "options": [
                "--label",
                "--exclude-label",
                "--created-after",
                "--created-before",
                "--updated-after",
                "--updated-before",
                "--pending-approval-only",
                "--checks-state",
                "--min-commits-behind",
            ],
        },
        {
            "name": "Pagination and sorting",
            "options": ["--batch-size", "--max-num", "--sort"],
        },
        {
            "name": "Assessment options",
            "options": [
                "--llm-model",
                "--llm-concurrency",
                "--clear-cache",
            ],
        },
        {
            "name": "Other",
            "options": [
                "--answer-triage",
                "--github-token",
                "--github-repository",
            ],
        },
    ],
    "breeze pr stats": [
        {
            "name": "Options",
            "options": [
                "--batch-size",
                "--clear-cache",
                "--github-token",
                "--github-repository",
            ],
        },
    ],
}
