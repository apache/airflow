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
"""Validate the Agent Skills manifest against a JSON-schema-like contract."""

from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys

SKILLS_JSON = pathlib.Path("contributing-docs/agent_skills/skills.json")

VALID_CONTEXTS = {"host", "breeze", "either"}
VALID_CATEGORIES = {"environment", "testing", "linting", "providers", "dags", "documentation"}
ID_PATTERN = re.compile(r"^[a-z][a-z0-9-]*$")


def validate_skill(skill: dict) -> list[str]:
    """
    Validate a single skill. Returns list of errors. Empty list means valid.
    """
    errors: list[str] = []
    sid = skill.get("id", "<unknown>")

    # Required fields
    for field in ["id", "context", "category", "description", "command"]:
        if not skill.get(field):
            errors.append(f"[{sid}] Missing required field: '{field}'")

    # ID format
    skill_id = skill.get("id")
    if skill_id and not ID_PATTERN.match(skill_id):
        errors.append(f"[{sid}] Invalid id format. Must match ^[a-z][a-z0-9-]*$ Got: '{skill_id}'")

    # Context values
    if skill.get("context") not in VALID_CONTEXTS:
        errors.append(
            f"[{sid}] Invalid context: '{skill.get('context')}'. Must be one of: {sorted(VALID_CONTEXTS)}",
        )

    # Category values
    if skill.get("category") not in VALID_CATEGORIES:
        errors.append(
            f"[{sid}] Invalid category: '{skill.get('category')}'. Must be one of: {sorted(VALID_CATEGORIES)}",
        )

    # Command not empty (if present)
    cmd = skill.get("command")
    if cmd is not None and not str(cmd).strip():
        errors.append(f"[{sid}] Command field is empty")

    return errors


def validate_all(skills: list[dict], verbose: bool = False) -> bool:
    """Validate all skills. Returns True if all valid."""
    all_errors: list[str] = []
    all_ids: list[str] = []

    for skill in skills:
        errors = validate_skill(skill)
        all_errors.extend(errors)
        if skill.get("id"):
            all_ids.append(skill["id"])

    # Check for duplicate IDs
    seen: set[str] = set()
    for sid in all_ids:
        if sid in seen:
            all_errors.append(f"Duplicate skill id: '{sid}'")
        seen.add(sid)

    if all_errors:
        print(f"VALIDATION FAILED: {len(all_errors)} error(s)")
        for err in all_errors:
            print(f"  ERROR: {err}")
        return False

    if verbose:
        print(f"OK: {len(skills)} skills valid")
        for skill in skills:
            print(
                f"  {skill['id']} "
                f"[{skill.get('category', '')}] "
                f"[{skill.get('context', '')}]",
            )
    else:
        print(f"OK: {len(skills)} skills valid")

    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--skill", help="Validate a specific skill by id")
    args = parser.parse_args()

    with SKILLS_JSON.open() as f:
        data = json.load(f)

    skills_data = data.get("skills", [])

    if args.skill:
        skill_selected = next((s for s in skills_data if s.get("id") == args.skill), None)
        if not skill_selected:
            print(f"Skill '{args.skill}' not found")
            sys.exit(1)
        validation_errors = validate_skill(skill_selected)
        if validation_errors:
            for err in validation_errors:
                print(f"ERROR: {err}")
            sys.exit(1)
        print(f"OK: skill '{args.skill}' is valid")
        sys.exit(0)

    ok = validate_all(skills_data, verbose=args.verbose)
    sys.exit(0 if ok else 1)

