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

"""Extract agent skills from SKILL.md and generate skills.json."""

from __future__ import annotations

import json
import re
from pathlib import Path


def extract_skills_from_markdown(markdown_content: str) -> list:
    """
    Extract skill definitions from SKILL.md markdown.

    Looks for JSON blocks within markdown skill definitions.

    Args:
        markdown_content: Content of SKILL.md file

    Returns:
        List of skill dictionaries
    """
    skills = []

    # Find all JSON blocks in markdown
    json_pattern = r"```json\s*(.*?)\s*```"
    matches = re.findall(json_pattern, markdown_content, re.DOTALL)

    for match in matches:
        try:
            skill = json.loads(match)
            # Validate required fields
            if "id" not in skill:
                raise ValueError("Skill missing required field: id")
            skills.append(skill)
        except json.JSONDecodeError as e:
            print(f"Warning: Failed to parse skill JSON: {e}")
            continue

    # Check for duplicate IDs
    ids = [s["id"] for s in skills]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate skill IDs found")

    return skills


def validate_skills_json(skills_data: dict) -> bool:
    """
    Validate skills.json structure.

    Args:
        skills_data: Parsed skills.json data

    Returns:
        True if valid

    Raises:
        ValueError: If validation fails
    """
    if "version" not in skills_data:
        raise ValueError("Skills missing required field: version")

    if "skills" not in skills_data:
        raise ValueError("Skills missing required field: skills")

    for skill in skills_data["skills"]:
        if "id" not in skill:
            raise ValueError("Skill missing required field: id")
        if "commands" not in skill:
            raise ValueError(f"Skill {skill['id']} missing required field: commands")

    return True


def extract_and_generate(skill_md_path: str, skills_json_path: str) -> None:
    """
    Extract skills from SKILL.md and generate skills.json.

    Args:
        skill_md_path: Path to SKILL.md
        skills_json_path: Path to output skills.json
    """
    # Read SKILL.md
    skill_md = Path(skill_md_path)
    if not skill_md.exists():
        raise FileNotFoundError(f"SKILL.md not found at {skill_md_path}")

    content = skill_md.read_text()

    # Extract skills
    skills = extract_skills_from_markdown(content)

    # Generate skills.json
    skills_data = {"version": "1.0", "skills": skills}

    # Validate
    validate_skills_json(skills_data)

    # Write to file
    output_path = Path(skills_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(skills_data, f, indent=2)

    print(f"✓ Generated skills.json with {len(skills)} skills")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python extract_breeze_contribution_skills.py <SKILL.md> <skills.json>")
        sys.exit(1)

    skill_md_path = sys.argv[1]
    skills_json_path = sys.argv[2]

    extract_and_generate(skill_md_path, skills_json_path)
