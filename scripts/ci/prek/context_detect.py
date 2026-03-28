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

``.. agent-skill::`` directives are embedded directly in contributing-docs RST
files — the source of truth. A generator extracts them into ``skills.json`` so
agents can read a static artifact without parsing RST at every invocation.

Source files scanned (see AGENT_SKILLS_RST_FILES):
  contributing-docs/03a_contributors_quick_start_beginners.rst
  contributing-docs/08_static_code_checks.rst
  contributing-docs/11_documentation_building.rst
  contributing-docs/testing/unit_tests.rst

Generated artifact (see AGENT_SKILLS_JSON):
  .github/skills/airflow-contributor/skills.json

The JSON is regenerated with ``--generate`` and validated against the RST with
``--check`` (run as a prek hook so drift is caught at commit time).

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
    python scripts/ci/prek/context_detect.py --generate        # write skills.json
    python scripts/ci/prek/context_detect.py --check           # drift check (CI)
    python scripts/ci/prek/context_detect.py run-single-test project=providers/vertica test_path=...
    python scripts/ci/prek/context_detect.py run-single-test --context breeze test_path=...
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]

# Contributing-docs files that contain embedded ``.. agent-skill::`` directives.
# Skills live next to the human-readable workflow they describe so updates are atomic.
AGENT_SKILLS_RST_FILES: list[Path] = [
    REPO_ROOT / "contributing-docs" / "03a_contributors_quick_start_beginners.rst",
    REPO_ROOT / "contributing-docs" / "08_static_code_checks.rst",
    REPO_ROOT / "contributing-docs" / "11_documentation_building.rst",
    REPO_ROOT / "contributing-docs" / "testing" / "unit_tests.rst",
]

_DIRECTIVE_RE = re.compile(r"^\.\.\s+agent-skill::\s*$")
_OPTION_RE = re.compile(r"^\s+:([^:]+):\s+(.+)$")

# Committed JSON artifact — agent-facing interface generated from the RST sources.
AGENT_SKILLS_JSON: Path = REPO_ROOT / ".github" / "skills" / "airflow-contributor" / "assets" / "skills.json"


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


def _parse_skills_from_files(rst_paths: list[Path] = AGENT_SKILLS_RST_FILES) -> list[dict]:
    """Parse ``.. agent-skill::`` directives from all contributing-docs source files.

    Merges results across files. Raises ValueError if the same skill id appears
    in more than one file (duplicate IDs are a configuration error).

    Raises:
        FileNotFoundError: if any path in rst_paths does not exist.
        ValueError: if a duplicate skill id is found across files.
    """
    all_skills: list[dict] = []
    seen: dict[str, Path] = {}
    for path in rst_paths:
        for skill in _parse_skills(path):
            sid = skill["id"]
            if sid in seen:
                raise ValueError(
                    f"Duplicate skill id '{sid}' found in {path} (already defined in {seen[sid]})"
                )
            seen[sid] = path
            all_skills.append(skill)
    return all_skills


def _find_skill(skill_id: str, skills: list[dict]) -> dict:
    """Return skill dict by id. Raises KeyError if not found."""
    for skill in skills:
        if skill.get("id") == skill_id:
            return skill
    known = [s.get("id") for s in skills]
    raise KeyError(f"Skill '{skill_id}' not found. Available: {known}")


# ---------------------------------------------------------------------------
# JSON artifact: generate and drift-check
# ---------------------------------------------------------------------------


def _load_skills(
    json_path: Path = AGENT_SKILLS_JSON,
    rst_paths: list[Path] = AGENT_SKILLS_RST_FILES,
) -> list[dict]:
    """Load skills from committed JSON if available, else parse RST.

    JSON is the fast path for agents at runtime. RST is the source of truth
    and is used as fallback when JSON hasn't been generated yet (e.g. during
    development before running ``--generate``).

    The JSON fast path is only used when the caller is using the default RST
    paths. If a caller overrides ``rst_paths`` (e.g. in tests), RST is always
    parsed so the custom paths are respected.
    """
    if rst_paths is AGENT_SKILLS_RST_FILES and json_path.exists():
        return json.loads(json_path.read_text(encoding="utf-8"))
    return _parse_skills_from_files(rst_paths)


def generate_skills_json(
    output_path: Path = AGENT_SKILLS_JSON,
    rst_paths: list[Path] = AGENT_SKILLS_RST_FILES,
) -> None:
    """Parse RST sources and write the canonical skills.json artifact.

    Run this after editing any ``.. agent-skill::`` directive and commit the
    updated skills.json alongside the RST change so the two stay in sync.

    Raises:
        FileNotFoundError: if any RST source file does not exist.
        ValueError: if a duplicate skill id is found.
    """
    skills = _parse_skills_from_files(rst_paths)
    output_path.write_text(json.dumps(skills, indent=2) + "\n", encoding="utf-8")
    print(f"Generated {output_path.relative_to(REPO_ROOT)} ({len(skills)} skill(s))")


def check_skills_json_drift(
    json_path: Path = AGENT_SKILLS_JSON,
    rst_paths: list[Path] = AGENT_SKILLS_RST_FILES,
) -> list[str]:
    """Return error strings if skills.json doesn't match what RST sources produce.

    Used by the prek ``check-agent-skills-drift`` hook. An empty list means the
    JSON is up to date and the commit can proceed.
    """
    errors: list[str] = []

    if not json_path.exists():
        errors.append(
            f"skills.json not found at {json_path.relative_to(REPO_ROOT)}. "
            f"Run: python scripts/ci/prek/context_detect.py --generate"
        )
        return errors

    try:
        current = _parse_skills_from_files(rst_paths)
    except (FileNotFoundError, ValueError) as exc:
        errors.append(str(exc))
        return errors

    stored = json.loads(json_path.read_text(encoding="utf-8"))

    # Normalize both sides to a canonical JSON string for comparison.
    current_canonical = json.dumps(current, indent=2, sort_keys=True)
    stored_canonical = json.dumps(stored, indent=2, sort_keys=True)

    if current_canonical != stored_canonical:
        errors.append(
            "skills.json is out of sync with RST sources.\n"
            "  Run: python scripts/ci/prek/context_detect.py --generate\n"
            f"  Then commit the updated {json_path.relative_to(REPO_ROOT)}"
        )
    return errors


# ---------------------------------------------------------------------------
# Command routing
# ---------------------------------------------------------------------------


def get_command(
    skill_id: str,
    rst_paths: list[Path] = AGENT_SKILLS_RST_FILES,
    **kwargs: str,
) -> str:
    """Return the correct command for skill_id in the current context.

    Applies {placeholder} substitution from kwargs.
    If any kwarg value is 'false' and the skill has a non-preferred (fallback)
    step for the current context, the fallback step is used instead.

    Raises:
        FileNotFoundError: if any source RST file does not exist.
        KeyError: if skill_id is not found.
        ValueError: if a required parameter placeholder is missing from kwargs.
    """
    skills = _load_skills(rst_paths=rst_paths)
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
    rst_paths: list[Path] = AGENT_SKILLS_RST_FILES,
) -> list[dict]:
    """Return all skills that have at least one step valid for the current context.

    Optionally filter by category.

    Raises:
        FileNotFoundError: if any source RST file does not exist.
    """
    ctx = get_context()
    skills = _load_skills(rst_paths=rst_paths)

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
    parser.add_argument("skill_id", nargs="?", help="Skill ID (defined in contributing-docs source files)")
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
        "--generate",
        action="store_true",
        help="Generate skills.json from RST sources and exit",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check skills.json is in sync with RST and exit (used by prek hook)",
    )
    parser.add_argument(
        "--context",
        choices=["host", "breeze"],
        help="Override context detection (default: auto-detect)",
    )
    args = parser.parse_args()

    if args.generate:
        try:
            generate_skills_json()
            return 0
        except (FileNotFoundError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            return 1

    if args.check:
        drift_errors = check_skills_json_drift()
        if drift_errors:
            for err in drift_errors:
                print(f"ERROR: {err}", file=sys.stderr)
            return 1
        total = len(_load_skills())
        print(f"OK: skills.json is up to date ({total} skill(s))")
        return 0

    try:
        skills = _load_skills()
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    if args.list:
        for entry in skills:
            print(f"{entry['id']:30s}  {entry['description']}")
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
