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


def main() -> int:
    parser = argparse.ArgumentParser(description="Extract agent-skill blocks into skills.json")
    parser.add_argument("--docs-file", default="AGENTS.md", help="Path to AGENTS.md")
    parser.add_argument("--output", default="contributing-docs/agent_skills/skills.json", help="Output JSON path")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[3]
    docs_path = root / args.docs_file
    out_path = root / args.output
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
