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
# /// script
# requires-python = ">=3.11"
# dependencies = ["pyyaml", "rich>=13.6.0"]
# ///
"""Validate agent skill files and symlinks.

Ensures that all skills defined in `.agents/skills/`:
1. Contain a valid SKILL.md with proper YAML frontmatter (name, description).
2. Carry the required Apache-2.0 license declaration (SPDX or ASF header).
3. Have valid, functional symlinks in `.claude/skills/` and `.github/skills/`.
"""

from __future__ import annotations

import sys
from pathlib import Path

import yaml
from common_prek_utils import AIRFLOW_ROOT_PATH, console


def validate_skill_file(skill_file: Path) -> list[str]:
    """Validate the structure, frontmatter, and license declaration of a single SKILL.md."""
    errors: list[str] = []

    if not skill_file.is_file():
        return [f"{skill_file}: file does not exist"]

    content = skill_file.read_text(encoding="utf-8")

    if not content.startswith("---"):
        return [f"{skill_file}: missing YAML frontmatter (must start with '---')"]

    parts = content.split("---", 2)
    if len(parts) < 3:
        return [f"{skill_file}: unclosed YAML frontmatter delimiter '---'"]

    raw_frontmatter = parts[1]

    try:
        frontmatter = yaml.safe_load(raw_frontmatter)
    except yaml.YAMLError as exc:
        return [f"{skill_file}: invalid YAML frontmatter: {exc}"]

    if not isinstance(frontmatter, dict):
        return [f"{skill_file}: YAML frontmatter must be a mapping"]

    name = frontmatter.get("name")
    if not name or not isinstance(name, str) or not name.strip():
        errors.append(f"{skill_file}: frontmatter missing non-empty 'name' field")
    elif skill_file.parent.name != name:
        errors.append(
            f"{skill_file}: frontmatter name '{name}' does not match directory '{skill_file.parent.name}'"
        )

    description = frontmatter.get("description")
    if not description or not isinstance(description, str) or not description.strip():
        errors.append(f"{skill_file}: frontmatter missing non-empty 'description' field")

    license_val = frontmatter.get("license")
    if license_val is not None and license_val != "Apache-2.0":
        errors.append(f"{skill_file}: frontmatter license must be 'Apache-2.0' (got {license_val!r})")

    # Check for license declaration in file content
    has_spdx = "SPDX-License-Identifier: Apache-2.0" in content
    has_asf = "Licensed to the Apache Software Foundation" in content
    if not (has_spdx or has_asf):
        errors.append(f"{skill_file}: missing Apache-2.0 license declaration (SPDX comment or ASF header)")

    return errors


def validate_symlinks(root_path: Path) -> list[str]:
    """Validate that symlinks in .claude/skills and .github/skills point to existing skills."""
    errors: list[str] = []
    agents_skills_dir = (root_path / ".agents" / "skills").resolve()

    for dir_name in (".claude/skills", ".github/skills"):
        symlink_dir = root_path / dir_name
        if not symlink_dir.is_dir():
            continue

        for item in sorted(symlink_dir.iterdir()):
            if not item.is_symlink():
                continue

            try:
                target_path = item.resolve(strict=True)
            except (FileNotFoundError, RuntimeError) as exc:
                errors.append(f"{item}: broken symlink ({exc})")
                continue

            if not target_path.exists():
                errors.append(f"{item}: symlink target '{target_path}' does not exist")
                continue

            if not target_path.is_relative_to(agents_skills_dir):
                errors.append(f"{item}: symlink points outside .agents/skills/ ('{target_path}')")

    gitignore_path = root_path / ".gitignore"
    if gitignore_path.is_file():
        gitignore_lines = set(gitignore_path.read_text(encoding="utf-8").splitlines())
        claude_skills_dir = root_path / ".claude" / "skills"
        if claude_skills_dir.is_dir():
            for item in sorted(claude_skills_dir.iterdir()):
                if item.is_symlink():
                    expected_pattern = f"!.claude/skills/{item.name}"
                    if expected_pattern not in gitignore_lines:
                        errors.append(
                            f"{item}: symlink is ignored by .gitignore (missing '{expected_pattern}')"
                        )

    return errors


def validate_all_skills(root_path: Path = AIRFLOW_ROOT_PATH) -> list[str]:
    """Validate all skills under .agents/skills and their symlinks."""
    errors: list[str] = []
    skills_dir = root_path / ".agents" / "skills"

    if not skills_dir.is_dir():
        return [f"Skills directory not found: {skills_dir}"]

    skill_count = 0
    for child in sorted(skills_dir.iterdir()):
        if not child.is_dir():
            continue

        skill_file = child / "SKILL.md"
        if not skill_file.exists():
            errors.append(f"{child}: missing SKILL.md")
            continue

        errors.extend(validate_skill_file(skill_file))
        skill_count += 1

    if skill_count == 0:
        errors.append(f"No skills found in {skills_dir}")

    errors.extend(validate_symlinks(root_path))
    return errors


def main() -> int:
    errors = validate_all_skills()
    if errors:
        if console:
            console.print("[red]Agent skill validation failed:[/red]")
            for err in errors:
                console.print(f"  [red]• {err}[/red]")
        else:
            print("Agent skill validation failed:", file=sys.stderr)
            for err in errors:
                print(f"  • {err}", file=sys.stderr)
        return 1

    if console:
        console.print("[green]All agent skills and symlinks are valid.[/green]")
    else:
        print("All agent skills and symlinks are valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
