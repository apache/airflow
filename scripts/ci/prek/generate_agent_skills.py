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
"""Generate Agent Skills manifest from YAML workflow specs.

Reads *.yaml files from contributing-docs/workflows/, validates each against
the schema, then writes:
  - contributing-docs/agent_skills/skills.json   (flat skill list)
  - contributing-docs/agent_skills/skill_graph.json  (dependency graph)

Modes:
  (default)    Generate/update both JSON files.
  --check      Verify committed JSON matches current YAML. Exit 1 if drifted.
  --coverage   Verify every .. workflow-skill:: marker in RST docs has a
               matching YAML file. Exit 1 if any marker is uncovered.

Mirror of the update-breeze-cmd-output hook pattern.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = REPO_ROOT / "contributing-docs" / "workflows"
OUTPUT_DIR = REPO_ROOT / "contributing-docs" / "agent_skills"
SKILLS_JSON = OUTPUT_DIR / "skills.json"
GRAPH_JSON = OUTPUT_DIR / "skill_graph.json"
CONTRIBUTING_DOCS_DIR = REPO_ROOT / "contributing-docs"

VALID_CONTEXTS = {"host", "breeze", "either"}
VALID_CATEGORIES = {"environment", "testing", "linting", "documentation", "providers", "dags"}
VALID_ID_RE = re.compile(r"^[a-z][a-z0-9-]*$")
MARKER_RE = re.compile(r"\.\.\s+workflow-skill::\s+(\S+)")


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

    if skill_id and source.stem != skill_id:
        errors.append(f"[{name}] filename stem '{source.stem}' must match id '{skill_id}'")

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


def load_workflows(workflows_dir: Path) -> tuple[list[dict], list[str]]:
    """Parse all YAML files. Return (skills, errors)."""
    skills: list[dict] = []
    all_errors: list[str] = []

    for yaml_file in sorted(workflows_dir.glob("*.yaml")):
        try:
            data = yaml.safe_load(yaml_file.read_text(encoding="utf-8"))
        except yaml.YAMLError as exc:
            all_errors.append(f"[{yaml_file.name}] YAML parse error: {exc}")
            continue
        if not isinstance(data, dict):
            all_errors.append(f"[{yaml_file.name}] expected a YAML mapping at top level")
            continue
        errors = validate_workflow(data, yaml_file)
        if errors:
            all_errors.extend(errors)
        else:
            skills.append(data)

    return skills, all_errors


def generate(workflows_dir: Path, output_dir: Path) -> int:
    """Load YAML, validate, write skills.json + skill_graph.json. Return exit code."""
    skills, errors = load_workflows(workflows_dir)
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
        "generated_from": "contributing-docs/workflows/*.yaml",
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


def check_drift(workflows_dir: Path, output_dir: Path) -> int:
    """Compare re-generated output against committed files. Exit 1 if drifted."""
    skills, errors = load_workflows(workflows_dir)
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
        print("OK: skills.json and skill_graph.json are in sync with YAML workflow specs")
        return 0

    if added:
        print(f"DRIFT: skills added in YAML but not in skills.json: {added}")
    if removed:
        print(f"DRIFT: skills removed from YAML but still in skills.json: {removed}")
    if modified:
        print(f"DRIFT: skills modified in YAML but skills.json not regenerated: {modified}")
    print("Run generate_agent_skills.py to regenerate.")
    return 1


# ---------------------------------------------------------------------------
# --coverage: doc marker check
# ---------------------------------------------------------------------------


def check_coverage(workflows_dir: Path, docs_dir: Path) -> int:
    """Check every .. workflow-skill:: <id> marker in RST docs has a matching YAML file."""
    yaml_ids = {f.stem for f in workflows_dir.glob("*.yaml")}

    missing: list[tuple[str, str, int]] = []  # (file, id, line_number)
    for rst_file in sorted(docs_dir.glob("*.rst")):
        for lineno, line in enumerate(rst_file.read_text(encoding="utf-8").splitlines(), start=1):
            match = MARKER_RE.search(line)
            if match:
                skill_id = match.group(1)
                if skill_id not in yaml_ids:
                    missing.append((rst_file.name, skill_id, lineno))

    if missing:
        for fname, sid, lineno in missing:
            print(
                f"COVERAGE: {fname}:{lineno} references '.. workflow-skill:: {sid}' "
                f"but contributing-docs/workflows/{sid}.yaml does not exist"
            )
        return 1

    print(
        f"OK: all workflow-skill markers in contributing-docs/*.rst are covered ({len(yaml_ids)} YAML files)"
    )
    return 0


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Agent Skills from YAML workflow specs")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify committed JSON matches current YAML specs. Exit 1 if drifted.",
    )
    parser.add_argument(
        "--coverage",
        action="store_true",
        help="Verify every .. workflow-skill:: marker in RST docs has a YAML file. Exit 1 if missing.",
    )
    parser.add_argument(
        "--workflows-dir",
        type=Path,
        default=WORKFLOWS_DIR,
        help="Path to directory containing workflow YAML files",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=OUTPUT_DIR,
        help="Path to output directory for skills.json and skill_graph.json",
    )
    parser.add_argument(
        "--docs-dir",
        type=Path,
        default=CONTRIBUTING_DOCS_DIR,
        help="Path to contributing-docs directory (for --coverage)",
    )
    args = parser.parse_args()

    if args.check:
        return check_drift(args.workflows_dir, args.output_dir)
    if args.coverage:
        return check_coverage(args.workflows_dir, args.docs_dir)
    return generate(args.workflows_dir, args.output_dir)


if __name__ == "__main__":
    sys.exit(main())
