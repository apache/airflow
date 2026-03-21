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

"""Return a workflow-aware response for the given workflow and context.

The response can be either:
- a runnable command
- structured guidance when command execution is not appropriate
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts.ci.agent_skills.breeze_context import get_context


def load_skills() -> dict[str, dict[str, Any]]:
    """Load extracted skills from skills.json and index them by workflow ID."""
    base_dir = Path(__file__).resolve().parent
    skills_path = base_dir / "skills.json"

    data = json.loads(skills_path.read_text(encoding="utf-8"))
    return {skill["id"]: skill for skill in data}


def get_workflow_definition(workflow_id: str) -> dict[str, Any]:
    """Return the extracted workflow definition for a given workflow ID."""
    skills = load_skills()

    if workflow_id not in skills:
        raise ValueError(f"Unknown workflow_id: {workflow_id}")

    return skills[workflow_id]


def get_recommended_command(
    workflow_id: str,
    test_path: str = "",
    project: str = "airflow-core",
    force_breeze_fallback: bool = False,
    context: str | None = None,
) -> dict[str, Any]:
    """Return the recommended command or guidance for a workflow."""
    workflow = get_workflow_definition(workflow_id)
    current_context = context or get_context()

    if current_context not in workflow["allowed_contexts"]:
        return {
            "mode": "guidance",
            "message": (f"Unsupported context '{current_context}' for workflow '{workflow_id}'."),
        }

    if current_context == "breeze":
        inside_command = workflow["inside_breeze_command"]

        if inside_command == "NONE":
            return {
                "mode": "guidance",
                "message": ("You are already inside Breeze. Do not start a new Breeze shell from here."),
                "next_action": "stay_in_current_context",
            }

        return {
            "mode": "command",
            "command": inside_command.format(test_path=test_path),
            "reason": "Already inside Breeze, so run the container command directly.",
        }

    if current_context == "host":
        if force_breeze_fallback:
            return {
                "mode": "command",
                "command": workflow["breeze_command"].format(test_path=test_path),
                "reason": (
                    "Host context requested Breeze fallback due to environment or dependency concerns."
                ),
            }

        return {
            "mode": "command",
            "command": workflow["local_command"].format(
                project=project,
                test_path=test_path,
            ),
            "reason": "Host context prefers local-first execution.",
        }

    return {
        "mode": "guidance",
        "message": "Could not determine a valid command for the workflow.",
    }
