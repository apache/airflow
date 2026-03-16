#
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
"""Apache Airflow Agent Skills — Breeze Context Detection.

Provides runtime host vs. container detection and command routing for
AI coding agents contributing to Apache Airflow.

Detection priority chain:
  1. AIRFLOW_BREEZE_CONTAINER env var (explicit)
  2. /.dockerenv file (Docker marker)
  3. /opt/airflow path (Breeze mount)
  4. Default: host
"""

from __future__ import annotations

import json
import os
import pathlib
from typing import Optional

SKILLS_JSON = pathlib.Path("contributing-docs/agent_skills/skills.json")


def get_context() -> str:
    """
    Return "breeze" if running inside Breeze container, else "host".

    Detection priority:
      1. AIRFLOW_BREEZE_CONTAINER env var
      2. /.dockerenv exists
      3. /opt/airflow exists
      4. default: host
    """
    if os.environ.get("AIRFLOW_BREEZE_CONTAINER"):
        return "breeze"
    if pathlib.Path("/.dockerenv").exists():
        return "breeze"
    if pathlib.Path("/opt/airflow").exists():
        return "breeze"
    return "host"


def load_skills() -> list[dict]:
    """Load skills from the generated skills.json manifest."""
    if not SKILLS_JSON.exists():
        raise FileNotFoundError(
            f"skills.json not found at {SKILLS_JSON}. Run extract_agent_skills.py first.",
        )
    with SKILLS_JSON.open() as f:
        data = json.load(f)
    return data["skills"]


def get_skill(skill_id: str) -> dict:
    """Return a skill definition by id."""
    skills = load_skills()
    for skill in skills:
        if skill.get("id") == skill_id:
            return skill
    raise KeyError(f"Skill '{skill_id}' not found. Available: {[s.get('id') for s in skills]}")


def get_command(skill_id: str, **kwargs: str) -> str:
    """
    Return the correct command for skill_id in the current execution context, with kwargs substituted.

    Example:
        get_command(
            "run-single-test",
            project="providers/apache/kafka",
            test_path="providers/apache/kafka/tests/...",
        )
    Returns:
        "uv run --project providers/apache/kafka pytest providers/apache/kafka/tests/... -xvs"
    """
    skill = get_skill(skill_id)
    ctx = get_context()

    skill_ctx = skill.get("context")
    if skill_ctx == "either":
        cmd = skill.get("command", "")
    elif skill_ctx == ctx:
        cmd = skill.get("command", "")
    else:
        # Wrong context — return guidance message
        return (
            f"Skill '{skill_id}' requires context '{skill_ctx}' but current context is '{ctx}'. "
            f"Expected environment: {skill_ctx}"
        )

    # Substitute kwargs into command template
    try:
        return cmd.format(**kwargs)
    except KeyError as e:
        missing = e.args[0]
        raise ValueError(
            f"Missing parameter {missing!r} for skill '{skill_id}'. Command template: {cmd}",
        ) from e


def list_skills_for_context(category: Optional[str] = None) -> list[dict]:
    """Return all skills valid for current context, optionally filtered by category."""
    ctx = get_context()
    skills = load_skills()
    result = [s for s in skills if s.get("context") in (ctx, "either")]
    if category:
        result = [s for s in result if s.get("category") == category]
    return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        skill_id = sys.argv[1]
        params: dict[str, str] = {}
        for arg in sys.argv[2:]:
            if "=" in arg:
                key, val = arg.split("=", 1)
                params[key] = val
        print(get_command(skill_id, **params))
    else:
        current = get_context()
        print(f"Current context: {current}")
        skills_in_context = list_skills_for_context()
        print(f"Available skills ({len(skills_in_context)}):")
        for skill in skills_in_context:
            print(f"  {skill.get('id')} [{skill.get('category')}]")

