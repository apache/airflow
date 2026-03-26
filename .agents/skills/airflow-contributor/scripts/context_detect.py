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
"""
Detect execution context and resolve the exact command for a skill.

Reads skill definitions directly from agent_skills.rst — no generated
artifacts required.

Usage:
    python context_detect.py <skill_id> [key=value ...]

Examples:
    python context_detect.py run-single-test project=providers/vertica test_path=providers/vertica/tests/unit/
    python context_detect.py run-static-checks target_branch=main
    python context_detect.py format-and-lint project=airflow-core file_path=airflow-core/src/airflow/models/dag.py
    python context_detect.py setup-breeze-environment

Output:
    Prints the resolved command to stdout.
    If a fallback exists, prints it to stderr prefixed with "# fallback:".
    Exit code 0 on success, 1 if skill not found or params missing.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[4]
AGENT_SKILLS_RST = REPO_ROOT / "contributing-docs" / "workflows" / "agent_skills.rst"

_DIRECTIVE_RE = re.compile(r"^\.\.\s+agent-skill::\s*$")
_OPTION_RE = re.compile(r"^\s+:([^:]+):\s+(.+)$")


# ---------------------------------------------------------------------------
# Context detection
# ---------------------------------------------------------------------------


def detect_context() -> str:
    """Return 'breeze' if running inside the Breeze container, else 'host'."""
    if Path("/opt/airflow").exists():
        return "breeze"
    if os.environ.get("AIRFLOW_BREEZE") == "true":
        return "breeze"
    return "host"


# ---------------------------------------------------------------------------
# RST parser (minimal — directives only)
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


def parse_skills(rst_path: Path) -> list[dict]:
    """Parse all ``.. agent-skill::`` directives. Return list of skill dicts."""
    if not rst_path.exists():
        print(f"ERROR: RST not found: {rst_path}", file=sys.stderr)
        sys.exit(1)

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

            if local_cmd and fallback_cmd:
                steps.append({"context": "host", "command": local_cmd, "preferred": True})
                steps.append({"context": "host", "command": fallback_cmd, "preferred": False})
            elif local_cmd:
                steps.append({"context": "host", "command": local_cmd, "preferred": True})

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


# ---------------------------------------------------------------------------
# Command resolution
# ---------------------------------------------------------------------------


def resolve_command(skill: dict, context: str, params: dict[str, str]) -> tuple[str, str | None]:
    """Return (preferred_command, fallback_command | None) with params substituted."""
    context_steps = [s for s in skill["steps"] if s["context"] == context]

    if not context_steps:
        print(
            f"ERROR: skill '{skill['id']}' has no steps for context '{context}'",
            file=sys.stderr,
        )
        sys.exit(1)

    preferred = next((s for s in context_steps if s.get("preferred")), context_steps[0])
    fallback_steps = [s for s in context_steps if not s.get("preferred")]
    fallback = fallback_steps[0] if fallback_steps else None

    def substitute(cmd: str) -> str:
        for k, v in params.items():
            cmd = cmd.replace(f"{{{k}}}", v)
        return cmd

    return substitute(preferred["command"]), (substitute(fallback["command"]) if fallback else None)


def check_required_params(skill: dict, provided: dict[str, str]) -> list[str]:
    """Return list of missing required parameter names."""
    return [name for name, required in skill["params"].items() if required and name not in provided]


# ---------------------------------------------------------------------------
# Entry point
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

    skills = parse_skills(AGENT_SKILLS_RST)

    if args.list:
        for skill in skills:
            print(f"{skill['id']:30s}  {skill['description']}")
        return 0

    if not args.skill_id:
        parser.error("skill_id is required unless --list is used")

    skill = next((s for s in skills if s["id"] == args.skill_id), None)
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

    missing = check_required_params(skill, params)
    if missing:
        print(
            f"ERROR: missing required parameter(s) for '{args.skill_id}': {', '.join(missing)}",
            file=sys.stderr,
        )
        return 1

    context = args.context or detect_context()
    command, fallback = resolve_command(skill, context, params)

    print(command)

    if fallback:
        print(f"# fallback (if system deps missing): {fallback}", file=sys.stderr)
    if skill["expected_output"]:
        print(f"# success signal: {skill['expected_output']}", file=sys.stderr)
    if skill["prereqs"]:
        print(f"# prereqs: {', '.join(skill['prereqs'])}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
