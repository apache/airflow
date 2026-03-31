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
"""Validate Agent Skills adherence to the agentskills.io standard.

This script is used by the project's prek/pre-commit pipeline to ensure that:

- All skills in `.agents/skills` follow the valid directory structure
- SKILL.md files have proper YAML frontmatter with `name` and `description`
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

try:
    import yaml
except ImportError:
    import sys

    print("PyYAML is required for validate_skills.py. Install with `pip install pyyaml`.")
    sys.exit(1)

logger = logging.getLogger(__name__)

SKILLS_DIR = Path(".agents/skills")


def validate_skill(skill_dir: Path) -> list[str]:
    """Validate a single skill directory and return a list of errors."""
    errors = []

    skill_md = skill_dir / "SKILL.md"
    if not skill_md.exists():
        errors.append(f"Missing SKILL.md in {skill_dir}")
        return errors

    content = skill_md.read_text(encoding="utf-8")

    # Strip leading HTML comment blocks (e.g. SPDX license and markdownlint
    # pragmas inserted/maintained by hooks) before checking for YAML frontmatter.
    stripped = content.lstrip()
    while stripped.startswith("<!--"):
        end_comment = stripped.find("-->")
        if end_comment == -1:
            break
        stripped = stripped[end_comment + 3 :].lstrip()

    if not stripped.startswith("---\n"):
        errors.append(f"Missing YAML frontmatter at start of {skill_md}")
        return errors

    parts = stripped.split("---\n")
    if len(parts) < 3:
        errors.append(f"Incomplete YAML frontmatter in {skill_md}")
        return errors

    frontmatter_str = parts[1]
    try:
        frontmatter = yaml.safe_load(frontmatter_str)
    except yaml.YAMLError as e:
        errors.append(f"Invalid YAML in {skill_md}: {e}")
        return errors

    if not isinstance(frontmatter, dict):
        errors.append(f"YAML frontmatter in {skill_md} must be a dictionary")
        return errors

    if "name" not in frontmatter:
        errors.append(f"Missing 'name' in {skill_md} frontmatter")
    elif frontmatter["name"] != skill_dir.name:
        errors.append(f"Skill name '{frontmatter['name']}' does not match directory name '{skill_dir.name}'")

    if "description" not in frontmatter:
        errors.append(f"Missing 'description' in {skill_md} frontmatter")

    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate Agent Skills")
    parser.parse_args(argv)

    if not SKILLS_DIR.exists():
        print(f"Directory {SKILLS_DIR} not found.")
        return 0

    all_errors = []
    for skill_dir in SKILLS_DIR.iterdir():
        if skill_dir.is_dir():
            errors = validate_skill(skill_dir)
            all_errors.extend(errors)

    if all_errors:
        print("❌ Agent Skills Validation Failed:")
        for error in all_errors:
            print(f"  - {error}")
        return 1

    print("✓ Agent Skills conform to standard")
    return 0


if __name__ == "__main__":
    sys.exit(main())
