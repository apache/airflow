#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use it except in compliance
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
"""Extract and validate .. agent-skill:: blocks from AGENTS.md into skills.json."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

VALID_CONTEXTS = {"host", "breeze", "either"}
VALID_CATEGORIES = {"environment", "testing", "linting", "providers", "dags", "documentation"}
ID_RE = re.compile(r"^[a-z][a-z0-9-]*$")


def parse_skills(text: str) -> list[dict]:
    """Extract agent-skill blocks and return list of skill dicts."""
    blocks = re.split(r"\n\.\. agent-skill::\s*\n", text)
    skills = []
    for block in blocks[1:]:  # skip content before first block
        skill = {}
        # Options: :key: value (value can continue on next line if indented)
        for m in re.finditer(r"^   :(\w+):\s*(.*?)(?=^   :\w+:|^   \.\.|\Z)", block, re.M | re.S):
            key, val = m.group(1), m.group(2)
            skill[key] = re.sub(r"\n\s+", " ", val).strip()
        # code-block content
        cb = re.search(r"\.\. code-block::\s*\w+\s*\n(.*?)(?=\n   \.\.|\Z)", block, re.S)
        skill["command"] = re.sub(r"^      ", "", cb.group(1).strip()).replace("\n      ", "\n") if cb else ""
        # expected output
        eo = re.search(r"\.\. agent-skill-expected-output::\s*\n(.*?)(?=\n\.\. agent-skill|\Z)", block, re.S)
        skill["expected_output"] = re.sub(r"^      ", "", eo.group(1).strip()).replace("\n      ", "\n") if eo else ""
        skills.append(skill)
    return skills


def validate_skill(skill: dict, index: int) -> None:
    """Raise ValueError if skill is invalid."""
    sid = skill.get("id", "")
    if not sid:
        raise ValueError(f"Skill at index {index}: missing required field 'id'")
    if not ID_RE.match(sid):
        raise ValueError(f"Skill '{sid}': id must match ^[a-z][a-z0-9-]*$")
    ctx = skill.get("context", "")
    if not ctx:
        raise ValueError(f"Skill '{sid}': missing required field 'context'")
    if ctx not in VALID_CONTEXTS:
        raise ValueError(f"Skill '{sid}': context must be one of {sorted(VALID_CONTEXTS)}, got '{ctx}'")
    cat = skill.get("category", "")
    if not cat:
        raise ValueError(f"Skill '{sid}': missing required field 'category'")
    if cat not in VALID_CATEGORIES:
        raise ValueError(f"Skill '{sid}': category must be one of {sorted(VALID_CATEGORIES)}, got '{cat}'")
    desc = (skill.get("description") or "").strip()
    if not desc:
        raise ValueError(f"Skill '{sid}': missing or empty required field 'description'")
    cmd = (skill.get("command") or "").strip()
    if not cmd:
        raise ValueError(f"Skill '{sid}': missing or empty required field 'command'")


def _skill_key_set(skill: dict) -> set[str]:
    """Return set of keys that define skill identity and content for comparison."""
    return {"id", "context", "category", "prereqs", "validates", "description", "command", "expected_output"}


def _normalize_skill_for_compare(skill: dict) -> dict:
    """Return a comparable dict with only the fields we care about, sorted keys."""
    keys = _skill_key_set(skill)
    return {k: skill.get(k, "") for k in sorted(keys) if k in skill or k in keys}


def check_drift(docs_path: Path, out_path: Path, docs_file: str) -> int:
    """
    Compare re-extracted skills from AGENTS.md with skills.json on disk.
    Print diff (added/removed/modified) and return 0 if in sync, 1 if drifted.
    """
    text = docs_path.read_text(encoding="utf-8")
    try:
        extracted = parse_skills(text)
    except Exception as e:
        print(f"Failed to parse AGENTS.md: {e}", file=sys.stderr)
        return 1
    for i, skill in enumerate(extracted):
        try:
            validate_skill(skill, i)
        except ValueError as e:
            print(f"Validation error in source: {e}", file=sys.stderr)
            return 1

    if not out_path.exists():
        print("Skills added (in source but not in json):")
        for s in extracted:
            print(f"  - {s.get('id', '?')}")
        print("DRIFT DETECTED: run extract_agent_skills.py to regenerate")
        return 1

    on_disk = json.loads(out_path.read_text(encoding="utf-8"))
    disk_skills = {s["id"]: s for s in on_disk.get("skills", []) if isinstance(s, dict) and s.get("id")}
    extracted_by_id = {s["id"]: _normalize_skill_for_compare(s) for s in extracted}

    added = [sid for sid in extracted_by_id if sid not in disk_skills]
    removed = [sid for sid in disk_skills if sid not in extracted_by_id]
    modified = []
    for sid in extracted_by_id:
        if sid in disk_skills:
            disk_norm = _normalize_skill_for_compare(disk_skills[sid])
            if disk_norm != extracted_by_id[sid]:
                modified.append(sid)

    if not added and not removed and not modified:
        print("OK: skills.json is in sync with AGENTS.md")
        return 0

    if added:
        print("Skills added (in source but not in json):")
        for sid in added:
            print(f"  - {sid}")
    if removed:
        print("Skills removed (in json but not in source):")
        for sid in removed:
            print(f"  - {sid}")
    if modified:
        print("Skills modified (fields changed):")
        for sid in modified:
            print(f"  - {sid}")
    print("DRIFT DETECTED: run extract_agent_skills.py to regenerate")
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract agent-skill blocks into skills.json")
    parser.add_argument("--docs-file", default="AGENTS.md", help="Path to AGENTS.md")
    parser.add_argument("--output", default="contributing-docs/agent_skills/skills.json", help="Output JSON path")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check if skills.json is in sync with source. Exit 1 if drifted.",
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    docs_path = root / args.docs_file
    out_path = root / args.output

    if args.check:
        return check_drift(docs_path, out_path, args.docs_file)

    text = docs_path.read_text(encoding="utf-8")
    skills = parse_skills(text)
    for i, skill in enumerate(skills):
        try:
            validate_skill(skill, i)
        except ValueError as e:
            print(f"Validation error: {e}", file=sys.stderr)
            return 1
    out = {
        "version": "1.0",
        "generated_by": "extract_agent_skills.py",
        "generated_from": args.docs_file,
        "skill_count": len(skills),
        "skills": skills,
    }
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Extracted {len(skills)} skills -> {args.output}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
