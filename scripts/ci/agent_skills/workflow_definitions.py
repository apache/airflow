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

"""DEPRECATED: Initial PoC workflow definitions.

This module was used in the first stage of the PoC where workflows were
hardcoded in Python. It is no longer used after introducing executable
documentation and skill extraction from markdown.

Kept temporarily for reference and comparison.
"""

from __future__ import annotations

from typing import Any

WORKFLOWS: dict[str, dict[str, Any]] = {
    "run_targeted_tests": {
        "id": "run_targeted_tests",
        "title": "Run targeted tests",
        "goal": (
            "Run a specific pytest target using the preferred local workflow "
            "on host, with Breeze fallback when local execution is not suitable."
        ),
        "preferred_context": "host",
        "allowed_contexts": ["host", "breeze"],
        "local_command": ("uv run --project {distribution_folder} pytest {test_path}"),
        "breeze_command": ("breeze exec pytest {test_path} -xvs"),
        "inside_breeze_command": ("pytest {test_path} -xvs"),
        "fallback_when": [
            "missing_system_dependencies",
            "local_environment_mismatch",
            "ci_local_discrepancy",
        ],
        "wrong_context_guidance": {
            "host_only_step_inside_breeze": (
                "You are already inside a Breeze container. "
                "Do not start a new Breeze shell. Run the test command directly."
            ),
            "breeze_required_but_unavailable": (
                "Local execution is not suitable for this case. Use Breeze for reproducible test execution."
            ),
        },
    }
}


def get_workflow_definition(workflow_id: str) -> dict[str, Any]:
    """Return the workflow definition for a known workflow ID.

    :param workflow_id: Identifier of the workflow.
    :return: Workflow definition dictionary.
    :raises ValueError: If the workflow ID is unknown.
    """
    if workflow_id not in WORKFLOWS:
        raise ValueError(f"Unknown workflow_id: {workflow_id}")
    return WORKFLOWS[workflow_id]
