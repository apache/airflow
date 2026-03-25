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
"""Generate Agent Skills manifest from RST executable documentation.

Reads ``.. agent-skill::`` directives from contributing-docs/workflows/agent_skills.rst,
validates each against the schema, then writes:
  - contributing-docs/agent_skills/skills.json   (flat skill list)
  - contributing-docs/agent_skills/skill_graph.json  (dependency graph)

Modes:
  (default)    Generate/update both JSON files.
  --check      Verify committed JSON matches current RST. Exit 1 if drifted.

Mirror of the update-breeze-cmd-output hook pattern.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[3]
AGENT_SKILLS_RST = REPO_ROOT / "contributing-docs" / "workflows" / "agent_skills.rst"
OUTPUT_DIR = REPO_ROOT / "contributing-docs" / "agent_skills"
SKILLS_JSON = OUTPUT_DIR / "skills.json"
GRAPH_JSON = OUTPUT_DIR / "skill_graph.json"

VALID_CONTEXTS = {"host", "breeze", "either"}
VALID_CATEGORIES = {"environment", "testing", "linting", "documentation", "providers", "dags"}
VALID_ID_RE = re.compile(r"^[a-z][a-z0-9-]*$")

_DIRECTIVE_RE = re.compile(r"^\.\.\s+agent-skill::\s*$")
_OPTION_RE = re.compile(r"^\s+:([^:]+):\s+(.+)$")


# ---------------------------------------------------------------------------
# RST parsing
# ---------------------------------------------------------------------------


def _collect_options(lines: list[str], start: int) -> tuple[dict[str, str], int]:
    """Collect all :option: value lines starting at `start`. Return (opts, next_index)."""
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


def _directive_to_skill(opts: dict[str, str], source: Path) -> tuple[dict | None, list[str]]:
    """Convert a parsed options dict into a skill dict. Return (skill, errors)."""
    errors: list[str] = []
    name = source.name

    skill_id = opts.get("id", "").strip()
    if not skill_id:
        errors.append(f"[{name}] .. agent-skill:: missing required option :id:")
        return None, errors

    category = opts.get("category", "").strip()
    description = opts.get("description", "").strip()

    if not category:
        errors.append(f"[{name}:{skill_id}] missing required option :category:")
    if not description:
        errors.append(f"[{name}:{skill_id}] missing required option :description:")
    if errors:
        return None, errors

    # Build steps from :local:, :fallback:, :breeze: options
    steps: list[dict] = []
    local_cmd = opts.get("local", "").strip()
    fallback_cmd = opts.get("fallback", "").strip()
    breeze_cmd = opts.get("breeze", "").strip()

    if local_cmd and fallback_cmd:
        # Two-step host pattern: conditional uv + fallback Breeze
        steps.append(
            {
                "context": "host",
                "command": local_cmd,
                "condition": "system_deps_available",
                "description": "Preferred: run directly with uv (faster, debuggable in IDE)",
            }
        )
        steps.append(
            {
                "context": "host",
                "command": fallback_cmd,
                "fallback_for": "system_deps_available",
                "description": "Fallback: use Breeze when system deps are missing",
            }
        )
    elif local_cmd:
        # Unconditional host command (e.g. run-db-test: always Breeze from host)
        steps.append({"context": "host", "command": local_cmd})
    elif fallback_cmd:
        errors.append(f"[{name}:{skill_id}] :fallback: requires :local: to also be set")
        return None, errors

    if breeze_cmd:
        steps.append({"context": "breeze", "command": breeze_cmd})

    if not steps:
        errors.append(f"[{name}:{skill_id}] at least one of :local: or :breeze: is required")
        return None, errors

    # Parse :prereqs: (comma-separated)
    prereqs: list[str] = []
    if opts.get("prereqs"):
        prereqs = [p.strip() for p in opts["prereqs"].split(",") if p.strip()]

    # Parse :params: (comma-separated name:required/optional pairs)
    parameters: dict[str, dict] = {}
    if opts.get("params"):
        for raw_pair in opts["params"].split(","):
            stripped = raw_pair.strip()
            if ":" in stripped:
                pname, req = stripped.split(":", 1)
                parameters[pname.strip()] = {"required": req.strip() == "required"}

    skill: dict = {
        "id": skill_id,
        "category": category,
        "description": description,
        "prereqs": prereqs,
        "steps": steps,
        "parameters": parameters,
    }
    if opts.get("expected-output"):
        skill["expected_output"] = opts["expected-output"].strip()

    return skill, []


def parse_rst_skills(rst_path: Path) -> tuple[list[dict], list[str]]:
    """Parse all ``.. agent-skill::`` directives from rst_path. Return (skills, errors)."""
    if not rst_path.exists():
        return [], [f"RST file not found: {rst_path}"]

    lines = rst_path.read_text(encoding="utf-8").splitlines()
    skills: list[dict] = []
    all_errors: list[str] = []
    i = 0
    while i < len(lines):
        if _DIRECTIVE_RE.match(lines[i]):
            opts, i = _collect_options(lines, i + 1)
            skill, errs = _directive_to_skill(opts, rst_path)
            if errs:
                all_errors.extend(errs)
            elif skill is not None:
                skills.append(skill)
        else:
            i += 1

    return skills, all_errors


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------


def validate_workflow(data: dict, source: Path) -> list[str]:
    """Return list of error strings. Empty list means valid."""
    errors: list[str] = []
    name = str(source.name)

    for field in ("id", "category", "description", "steps"):
        if not data.get(field):
            errors.append(f"[{name}] missing required field: '{field}'")

    skill_id = data.get("id", "")
    if skill_id and not VALID_ID_RE.match(skill_id):
        errors.append(f"[{name}] 'id' must match ^[a-z][a-z0-9-]*$, got: '{skill_id}'")

    category = data.get("category", "")
    if category and category not in VALID_CATEGORIES:
        errors.append(f"[{name}] invalid category '{category}'. Must be one of: {sorted(VALID_CATEGORIES)}")

    steps = data.get("steps", [])
    if not isinstance(steps, list) or len(steps) == 0:
        errors.append(f"[{name}] 'steps' must be a non-empty list")
    else:
        for i, step in enumerate(steps):
            ctx = step.get("context", "")
            if ctx not in VALID_CONTEXTS:
                errors.append(
                    f"[{name}] step[{i}] invalid context '{ctx}'. Must be one of: {sorted(VALID_CONTEXTS)}"
                )
            if not step.get("command", "").strip():
                errors.append(f"[{name}] step[{i}] missing or empty 'command'")

    prereqs = data.get("prereqs", [])
    if not isinstance(prereqs, list):
        errors.append(f"[{name}] 'prereqs' must be a list")

    return errors


def validate_prereq_references(all_skills: list[dict]) -> list[str]:
    """Check that every prereq id references an existing skill."""
    errors: list[str] = []
    known_ids = {s["id"] for s in all_skills}
    for skill in all_skills:
        for prereq_id in skill.get("prereqs", []):
            if prereq_id not in known_ids:
                errors.append(
                    f"[{skill['id']}] prereq '{prereq_id}' does not match any skill id. "
                    f"Known ids: {sorted(known_ids)}"
                )
    return errors


# ---------------------------------------------------------------------------
# Dependency graph
# ---------------------------------------------------------------------------


def build_graph(skills: list[dict]) -> tuple[list[dict], list[dict]]:
    """Return (nodes, edges) for skill_graph.json."""
    nodes = [
        {"id": s["id"], "category": s.get("category", ""), "context": _primary_context(s)} for s in skills
    ]
    edges = [
        {"from": s["id"], "to": prereq, "type": "requires"} for s in skills for prereq in s.get("prereqs", [])
    ]
    return nodes, edges


def _primary_context(skill: dict) -> str:
    """Return the context of the first step, or 'either' if mixed."""
    steps = skill.get("steps", [])
    contexts = {step.get("context") for step in steps}
    if len(contexts) == 1:
        return contexts.pop()
    return "either"


def detect_cycle(skills: list[dict]) -> list[str] | None:
    """Return cycle as list of ids if one exists, else None."""
    adj: dict[str, list[str]] = {s["id"]: list(s.get("prereqs", [])) for s in skills}
    path: list[str] = []
    in_stack: set[str] = set()
    visited: set[str] = set()

    def dfs(node: str) -> list[str] | None:
        visited.add(node)
        in_stack.add(node)
        path.append(node)
        for neighbour in adj.get(node, []):
            if neighbour not in visited:
                result = dfs(neighbour)
                if result is not None:
                    return result
            elif neighbour in in_stack:
                start = path.index(neighbour)
                return path[start:] + [neighbour]
        path.pop()
        in_stack.discard(node)
        return None

    for skill_id in list(adj):
        if skill_id not in visited:
            result = dfs(skill_id)
            if result is not None:
                return result
    return None


# ---------------------------------------------------------------------------
# Load / generate
# ---------------------------------------------------------------------------


def load_workflows(rst_path: Path) -> tuple[list[dict], list[str]]:
    """Parse RST and validate all skills. Return (skills, errors)."""
    skills, parse_errors = parse_rst_skills(rst_path)
    if parse_errors:
        return [], parse_errors

    all_errors: list[str] = []
    valid_skills: list[dict] = []
    for skill in skills:
        errors = validate_workflow(skill, rst_path)
        if errors:
            all_errors.extend(errors)
        else:
            valid_skills.append(skill)

    return valid_skills, all_errors


def generate(rst_path: Path, output_dir: Path) -> int:
    """Parse RST, validate, write skills.json + skill_graph.json. Return exit code."""
    skills, errors = load_workflows(rst_path)
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    ref_errors = validate_prereq_references(skills)
    if ref_errors:
        for err in ref_errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    cycle = detect_cycle(skills)
    if cycle:
        print(f"ERROR: circular dependency detected: {' -> '.join(cycle)}", file=sys.stderr)
        return 1

    nodes, edges = build_graph(skills)

    output_dir.mkdir(parents=True, exist_ok=True)

    skills_out = {
        "version": "1.0",
        "generated_by": "generate_agent_skills.py",
        "generated_from": "contributing-docs/workflows/agent_skills.rst",
        "skill_count": len(skills),
        "skills": skills,
    }
    (output_dir / "skills.json").write_text(
        json.dumps(skills_out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    graph_out = {"nodes": nodes, "edges": edges}
    (output_dir / "skill_graph.json").write_text(
        json.dumps(graph_out, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )

    print(f"Generated {len(skills)} skills -> contributing-docs/agent_skills/skills.json")
    print(
        f"Generated skill graph ({len(nodes)} nodes, {len(edges)} edges) -> contributing-docs/agent_skills/skill_graph.json"
    )
    return 0


# ---------------------------------------------------------------------------
# --check: drift detection
# ---------------------------------------------------------------------------


def check_drift(rst_path: Path, output_dir: Path) -> int:
    """Compare re-generated output against committed files. Exit 1 if drifted."""
    skills, errors = load_workflows(rst_path)
    if errors:
        for err in errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    ref_errors = validate_prereq_references(skills)
    if ref_errors:
        for err in ref_errors:
            print(f"ERROR: {err}", file=sys.stderr)
        return 1

    cycle = detect_cycle(skills)
    if cycle:
        print(f"ERROR: circular dependency detected: {' -> '.join(cycle)}", file=sys.stderr)
        return 1

    skills_path = output_dir / "skills.json"
    graph_path = output_dir / "skill_graph.json"

    if not skills_path.exists() or not graph_path.exists():
        print("DRIFT: skills.json or skill_graph.json missing. Run generate_agent_skills.py to create them.")
        return 1

    committed = json.loads(skills_path.read_text(encoding="utf-8"))
    committed_skills = {s["id"]: s for s in committed.get("skills", [])}
    current_skills = {s["id"]: s for s in skills}

    added = [sid for sid in current_skills if sid not in committed_skills]
    removed = [sid for sid in committed_skills if sid not in current_skills]
    modified = [
        sid
        for sid in current_skills
        if sid in committed_skills and current_skills[sid] != committed_skills[sid]
    ]

    if not added and not removed and not modified:
        print("OK: skills.json and skill_graph.json are in sync with agent_skills.rst")
        return 0

    if added:
        print(f"DRIFT: skills added in RST but not in skills.json: {added}")
    if removed:
        print(f"DRIFT: skills removed from RST but still in skills.json: {removed}")
    if modified:
        print(f"DRIFT: skills modified in RST but skills.json not regenerated: {modified}")
    print("Run generate_agent_skills.py to regenerate.")
    return 1


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Agent Skills from RST executable documentation")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify committed JSON matches current RST. Exit 1 if drifted.",
    )
    parser.add_argument(
        "--rst-path",
        type=Path,
        default=AGENT_SKILLS_RST,
        help="Path to agent_skills.rst",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Path to output directory for skills.json and skill_graph.json",
    )
    args = parser.parse_args()

    if args.check:
        return check_drift(args.rst_path, args.output_dir)
    return generate(args.rst_path, args.output_dir)


if __name__ == "__main__":
    sys.exit(main())
