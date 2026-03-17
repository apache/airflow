#!/usr/bin/env python3
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

"""Validate and sync skills between SKILL.md and skills.json."""

from __future__ import annotations

import json
import sys
from pathlib import Path

# Add the script's directory to sys.path to allow importing extract_breeze_contribution_skills
sys.path.append(str(Path(__file__).parent))

from extract_breeze_contribution_skills import extract_skills_from_markdown


def detect_drift(skill_md_path: str, skills_json_path: str) -> bool:
    """
    Detect if SKILL.md and skills.json are out of sync.

    Args:
        skill_md_path: Path to SKILL.md
        skills_json_path: Path to skills.json

    Returns:
        True if drift detected, False if in sync
    """
    # Read SKILL.md
    skill_md = Path(skill_md_path)
    if not skill_md.exists():
        print(f"Error: {skill_md_path} not found")
        return True

    content = skill_md.read_text()
    extracted_skills = extract_skills_from_markdown(content)

    # Read skills.json
    skills_json = Path(skills_json_path)
    if not skills_json.exists():
        print(f"Drift detected: {skills_json_path} does not exist")
        return True

    with open(skills_json) as f:
        current_skills = json.load(f)

    # Compare
    extracted_ids = {s["id"] for s in extracted_skills}
    current_ids = {s["id"] for s in current_skills.get("skills", [])}

    if extracted_ids != current_ids:
        print("Drift detected: Skill IDs mismatch")
        print(f"  In SKILL.md: {extracted_ids}")
        print(f"  In skills.json: {current_ids}")
        return True

    # Check if content changed
    if extracted_skills != current_skills.get("skills", []):
        print("Drift detected: Skill definitions have changed")
        return True

    return False


def regenerate_skills_json(skill_md_path: str, skills_json_path: str) -> None:
    """
    Regenerate skills.json from SKILL.md.

    Args:
        skill_md_path: Path to SKILL.md
        skills_json_path: Path to output skills.json
    """
    from extract_breeze_contribution_skills import extract_and_generate

    extract_and_generate(skill_md_path, skills_json_path)
    print(f"✓ Regenerated {skills_json_path}")


def main():
    """Main entry point for prek hook."""

    # Default paths (relative to repo root)
    skill_md = ".github/skills/breeze-contribution/SKILL.md"
    skills_json = ".github/skills/breeze-contribution/skills.json"

    # Allow custom paths via CLI
    if len(sys.argv) > 1:
        if sys.argv[1] == "--fix":
            regenerate_skills_json(skill_md, skills_json)
            return 0
        if sys.argv[1] == "--check":
            if detect_drift(skill_md, skills_json):
                print("\n❌ Drift detected! Run: python validate_skills.py --fix")
                return 1
            print("✓ Skills in sync")
            return 0

    # Default: check for drift
    if detect_drift(skill_md, skills_json):
        print("Run with --fix to regenerate skills.json")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
