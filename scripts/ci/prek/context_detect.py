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

Reads skill definitions directly from agent_skills.rst — no generated
artifacts required.

Provides an importable API that AI agents (and tests) use to determine the
current execution environment and retrieve the correct command for a skill.

Detection priority chain:
  1. AIRFLOW_BREEZE_CONTAINER env var set  -> "breeze"
  2. /.dockerenv file exists               -> "breeze"
  3. /opt/airflow path exists              -> "breeze"
  4. (default)                             -> "host"

Usage (importable):
    from ci.prek.context_detect import get_context, get_command

    ctx = get_context()                    # "host" or "breeze"
    cmd = get_command("run-single-test",   # raises if skill not found
                      project="providers/amazon",
                      test_path="providers/amazon/tests/test_s3.py")

Usage (CLI):
    python scripts/ci/prek/context_detect.py --list
    python scripts/ci/prek/context_detect.py run-single-test project=providers/vertica test_path=...
    python scripts/ci/prek/context_detect.py run-single-test --context breeze test_path=...
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
AGENT_SKILLS_RST = REPO_ROOT / "contributing-docs" / "workflows" / "agent_skills.rst"

_DIRECTIVE_RE = re.compile(r"^\.\.\s+agent-skill::\s*$")
_OPTION_RE = re.compile(r"^\s+:([^:]+):\s+(.+)$")


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
# RST parsing
# ---------------------------------------------------------------------------


def _collect_options(lines: list[str], start: int) -> tuple[dict[str, str], int]:
    opts: dict[str, str] = {}
    i = start
    while i < len(lines):
        m = _OPTION_RE.match(lines[i])
        if m:
            opts[m.group(1)] = m.group(2).strip()
            i += 1
        elif lines[i].strip() == "" or not lines[i].startswith(" "):
            break
        else:
            i += 1
    return opts, i


def _parse_skills(rst_path: Path) -> list[dict]:
    """Parse all ``.. agent-skill::`` directives from rst_path.

    Returns a list of skill dicts with fields:
      id, category, description, steps, prereqs, params, expected_output

    Each step dict has: context ("host"|"breeze"), command, preferred (bool).
    A skill with both :local: and :fallback: produces two host steps —
    preferred=True for local, preferred=False for fallback.

    Raises:
        FileNotFoundError: if rst_path does not exist.
    """
    if not rst_path.exists():
        raise FileNotFoundError(f"agent_skills.rst not found at {rst_path}")

    lines = rst_path.read_text(encoding="utf-8").splitlines()
    skills: list[dict] = []
    i = 0
    while i < len(lines):
        if _DIRECTIVE_RE.match(lines[i]):
            opts, i = _collect_options(lines, i + 1)
            skill_id = opts.get("id", "").strip()
            if not skill_id:
                continue

            steps: list[dict] = []
            local_cmd = opts.get("local", "").strip()
            fallback_cmd = opts.get("fallback", "").strip()
            breeze_cmd = opts.get("breeze", "").strip()

            if local_cmd:
                steps.append({"context": "host", "command": local_cmd, "preferred": True})
            if fallback_cmd:
                steps.append({"context": "host", "command": fallback_cmd, "preferred": False})
            if breeze_cmd:
                steps.append({"context": "breeze", "command": breeze_cmd, "preferred": True})

            prereqs = [p.strip() for p in opts.get("prereqs", "").split(",") if p.strip()]
            params: dict[str, bool] = {}
            for raw_param in opts.get("params", "").split(","):
                raw = raw_param.strip()
                if ":" in raw:
                    name, req = raw.split(":", 1)
                    params[name.strip()] = req.strip() == "required"

            skills.append(
                {
                    "id": skill_id,
                    "category": opts.get("category", ""),
                    "description": opts.get("description", ""),
                    "steps": steps,
                    "prereqs": prereqs,
                    "params": params,
                    "expected_output": opts.get("expected-output", ""),
                }
            )
        else:
            i += 1

    return skills


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
    rst_path: Path = AGENT_SKILLS_RST,
    **kwargs: str,
) -> str:
    """Return the correct command for skill_id in the current context.

    Applies {placeholder} substitution from kwargs.
    If any kwarg value is 'false' and the skill has a non-preferred (fallback)
    step for the current context, the fallback step is used instead.

    Raises:
        FileNotFoundError: if agent_skills.rst does not exist.
        KeyError: if skill_id is not found.
        ValueError: if a required parameter placeholder is missing from kwargs.
    """
    skills = _parse_skills(rst_path)
    skill = _find_skill(skill_id, skills)
    ctx = get_context()

    context_steps = [s for s in skill["steps"] if s["context"] in (ctx, "either")]

    if not context_steps:
        available_contexts = {s["context"] for s in skill["steps"]}
        return (
            f"Skill '{skill_id}' has no steps for context '{ctx}'. "
            f"This skill requires: {sorted(available_contexts)}"
        )

    preferred_steps = [s for s in context_steps if s.get("preferred", True)]
    fallback_steps = [s for s in context_steps if not s.get("preferred", True)]

    use_fallback = any(v == "false" for v in kwargs.values()) and bool(fallback_steps)
    if use_fallback:
        selected_step = fallback_steps[0]
    elif preferred_steps:
        selected_step = preferred_steps[0]
    else:
        selected_step = context_steps[0]

    cmd = selected_step["command"]
    # Exclude "false" sentinel values from substitution
    sub_kwargs = {k: v for k, v in kwargs.items() if v != "false"}
    try:
        return cmd.format(**sub_kwargs)
    except KeyError as exc:
        missing = exc.args[0]
        raise ValueError(
            f"Missing parameter '{missing}' for skill '{skill_id}'. Command template: {cmd}"
        ) from exc


# ---------------------------------------------------------------------------
# Skill discovery
# ---------------------------------------------------------------------------


def list_skills_for_context(
    category: str | None = None,
    rst_path: Path = AGENT_SKILLS_RST,
) -> list[dict]:
    """Return all skills that have at least one step valid for the current context.

    Optionally filter by category.

    Raises:
        FileNotFoundError: if agent_skills.rst does not exist.
    """
    ctx = get_context()
    skills = _parse_skills(rst_path)

    def _has_step_for_context(skill: dict) -> bool:
        return any(s["context"] in (ctx, "either") for s in skill.get("steps", []))

    result = [s for s in skills if _has_step_for_context(s)]
    if category:
        result = [s for s in result if s.get("category") == category]
    return result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Resolve the exact command for a skill in the current execution context.",
        epilog="Parameters are passed as key=value pairs after the skill ID.",
    )
    parser.add_argument("skill_id", nargs="?", help="Skill ID from agent_skills.rst")
    parser.add_argument(
        "params",
        nargs="*",
        metavar="key=value",
        help="Parameter substitutions (e.g. project=providers/vertica)",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available skill IDs and exit",
    )
    parser.add_argument(
        "--context",
        choices=["host", "breeze"],
        help="Override context detection (default: auto-detect)",
    )
    args = parser.parse_args()

    try:
        skills = _parse_skills(AGENT_SKILLS_RST)
    except FileNotFoundError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.list:
        for skill in skills:
            print(f"{skill['id']:30s}  {skill['description']}")
        return 0

    if not args.skill_id:
        parser.error("skill_id is required unless --list is used")

    skill: dict | None = next((s for s in skills if s["id"] == args.skill_id), None)
    if skill is None:
        known = [s["id"] for s in skills]
        print(f"ERROR: unknown skill '{args.skill_id}'. Known: {', '.join(known)}", file=sys.stderr)
        return 1

    params: dict[str, str] = {}
    for p in args.params:
        if "=" not in p:
            print(f"ERROR: parameter must be key=value, got: '{p}'", file=sys.stderr)
            return 1
        k, v = p.split("=", 1)
        params[k.strip()] = v.strip()

    context = args.context or get_context()
    ctx_steps = [s for s in skill["steps"] if s["context"] in (context, "either")]
    if not ctx_steps:
        available = sorted({s["context"] for s in skill["steps"]})
        print(
            f"ERROR: skill '{args.skill_id}' has no steps for context '{context}'. Requires: {available}",
            file=sys.stderr,
        )
        return 1

    preferred = next((s for s in ctx_steps if s.get("preferred", True)), ctx_steps[0])
    fallback_steps = [s for s in ctx_steps if not s.get("preferred", True)]

    def substitute(cmd: str) -> str:
        for k, v in params.items():
            cmd = cmd.replace(f"{{{k}}}", v)
        return cmd

    command = substitute(preferred["command"])
    print(command)

    if fallback_steps:
        print(
            f"# fallback (if system deps missing): {substitute(fallback_steps[0]['command'])}",
            file=sys.stderr,
        )
    if skill["expected_output"]:
        print(f"# success signal: {skill['expected_output']}", file=sys.stderr)
    if skill["prereqs"]:
        print(f"# prereqs: {', '.join(skill['prereqs'])}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
