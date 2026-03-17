#!/usr/bin/env python
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
"""Runtime host vs Breeze container detection for AI agent skills.

Provides a callable API that AI agents (and tests) use to determine the
current execution environment and retrieve the correct command for a skill.

Detection priority chain:
  1. AIRFLOW_BREEZE_CONTAINER env var set  -> "breeze"
  2. /.dockerenv file exists               -> "breeze"
  3. /opt/airflow path exists              -> "breeze"
  4. (default)                             -> "host"

Usage:
    from ci.prek.context_detect import get_context, get_command

    ctx = get_context()                    # "host" or "breeze"
    cmd = get_command("run-single-test",   # raises if skill not found
                      project="providers/amazon",
                      test_path="providers/amazon/tests/test_s3.py")
"""

from __future__ import annotations

import json
import os
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
SKILLS_JSON = REPO_ROOT / "contributing-docs" / "agent_skills" / "skills.json"


# ---------------------------------------------------------------------------
# Context detection
# ---------------------------------------------------------------------------


def get_context() -> str:
    """Return 'breeze' if running inside the Breeze container, else 'host'.

    Detection priority:
      1. AIRFLOW_BREEZE_CONTAINER env var
      2. /.dockerenv file (set by Docker runtime)
      3. /opt/airflow directory (Breeze mount point)
      4. default: host
    """
    if os.environ.get("AIRFLOW_BREEZE_CONTAINER"):
        return "breeze"
    if Path("/.dockerenv").exists():
        return "breeze"
    if Path("/opt/airflow").exists():
        return "breeze"
    return "host"


# ---------------------------------------------------------------------------
# Skill loading
# ---------------------------------------------------------------------------


def _load_skills(skills_json: Path = SKILLS_JSON) -> list[dict]:
    """Load skills from skills.json. Raises FileNotFoundError if missing."""
    if not skills_json.exists():
        raise FileNotFoundError(
            f"skills.json not found at {skills_json}. Run scripts/ci/prek/generate_agent_skills.py first."
        )
    return json.loads(skills_json.read_text(encoding="utf-8"))["skills"]


def _find_skill(skill_id: str, skills: list[dict]) -> dict:
    """Return skill dict by id. Raises KeyError if not found."""
    for skill in skills:
        if skill.get("id") == skill_id:
            return skill
    known = [s.get("id") for s in skills]
    raise KeyError(f"Skill '{skill_id}' not found. Available: {known}")


# ---------------------------------------------------------------------------
# Command routing
# ---------------------------------------------------------------------------


def get_command(
    skill_id: str,
    skills_json: Path = SKILLS_JSON,
    **kwargs: str,
) -> str:
    """Return the correct command for skill_id in the current context.

    Applies {placeholder} substitution from kwargs.
    If the skill has a step whose condition matches a kwargs key set to 'false',
    the fallback step is used instead.

    Raises:
        FileNotFoundError: if skills.json has not been generated yet.
        KeyError: if skill_id is not found.
        ValueError: if a required parameter placeholder is missing from kwargs.
    """
    skills = _load_skills(skills_json)
    skill = _find_skill(skill_id, skills)
    ctx = get_context()

    steps = skill.get("steps", [])
    # Pick steps valid for current context
    context_steps = [s for s in steps if s.get("context") in (ctx, "either")]

    if not context_steps:
        available_contexts = {s.get("context") for s in steps}
        return (
            f"Skill '{skill_id}' has no steps for context '{ctx}'. "
            f"This skill requires: {sorted(available_contexts)}"
        )

    # Respect condition/fallback: if condition key is in kwargs and set to "false",
    # skip the conditional step and use the fallback step.
    selected_step = context_steps[0]
    condition = selected_step.get("condition")
    if condition and kwargs.get(condition) == "false":
        fallback_steps = [s for s in context_steps if s.get("fallback_for") == condition]
        if fallback_steps:
            selected_step = fallback_steps[0]

    cmd = selected_step.get("command", "")
    try:
        return cmd.format(**{k: v for k, v in kwargs.items() if k != condition})
    except KeyError as exc:
        missing = exc.args[0]
        params = skill.get("parameters", {})
        hint = params.get(missing, {}).get("description", "no description")
        raise ValueError(
            f"Missing parameter '{missing}' for skill '{skill_id}'. "
            f"Description: {hint}. Command template: {cmd}"
        ) from exc


# ---------------------------------------------------------------------------
# Skill discovery
# ---------------------------------------------------------------------------


def list_skills_for_context(
    category: str | None = None,
    skills_json: Path = SKILLS_JSON,
) -> list[dict]:
    """Return all skills that have at least one step valid for the current context.

    Optionally filter by category.
    """
    ctx = get_context()
    skills = _load_skills(skills_json)

    def _has_step_for_context(skill: dict) -> bool:
        return any(s.get("context") in (ctx, "either") for s in skill.get("steps", []))

    result = [s for s in skills if _has_step_for_context(s)]
    if category:
        result = [s for s in result if s.get("category") == category]
    return result


# ---------------------------------------------------------------------------
# CLI entry point (for agent introspection)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]
    if not args:
        ctx = get_context()
        print(f"Current context: {ctx}")
        try:
            available = list_skills_for_context()
            print(f"Available skills ({len(available)}):")
            for s in available:
                print(f"  {s['id']:<35} [{s.get('category', '')}]")
        except FileNotFoundError as exc:
            print(str(exc), file=sys.stderr)
            sys.exit(1)
    else:
        # get_command <skill_id> [key=value ...]
        skill_id = args[0]
        params: dict[str, str] = {}
        for arg in args[1:]:
            if "=" in arg:
                k, v = arg.split("=", 1)
                params[k] = v
        try:
            print(get_command(skill_id, **params))
        except (FileNotFoundError, KeyError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            sys.exit(1)
