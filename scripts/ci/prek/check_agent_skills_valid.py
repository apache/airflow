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
"""Validate agent_skills.rst: no duplicate IDs, no missing required fields, valid contexts.

Runs as a prek (pre-commit) hook. Exits 1 if any violation is found so that
contributors cannot commit a malformed skill definition.

Checked rules:
  - Every ``.. agent-skill::`` directive must have an ``id`` field.
  - Every skill must have at least one of ``local`` or ``breeze`` (a step to execute).
  - Skill IDs must be unique across all source files.
  - ``params`` values, if present, must end with ``:required`` or ``:optional``.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Ensure the scripts/ directory is on sys.path so this script works when
# invoked directly (e.g. as a prek hook) without the package being installed.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ci.prek.context_detect import AGENT_SKILLS_RST_FILES

REPO_ROOT = Path(__file__).resolve().parents[3]

_DIRECTIVE_RE = re.compile(r"^\.\.\s+agent-skill::\s*$")
_OPTION_RE = re.compile(r"^\s+:([^:]+):\s+(.+)$")

REQUIRED_FIELDS = ("id",)
STEP_FIELDS = ("local", "breeze")
VALID_PARAM_SUFFIXES = (":required", ":optional")


def _parse_directives(rst_path: Path) -> list[tuple[int, dict[str, str]]]:
    """Return list of (line_number, options_dict) for every agent-skill directive."""
    lines = rst_path.read_text(encoding="utf-8").splitlines()
    result: list[tuple[int, dict[str, str]]] = []
    i = 0
    while i < len(lines):
        if _DIRECTIVE_RE.match(lines[i]):
            directive_line = i + 1  # 1-indexed for error messages
            opts: dict[str, str] = {}
            i += 1
            while i < len(lines):
                m = _OPTION_RE.match(lines[i])
                if m:
                    opts[m.group(1)] = m.group(2).strip()
                    i += 1
                elif lines[i].strip() == "" or not lines[i].startswith(" "):
                    break
                else:
                    i += 1
            result.append((directive_line, opts))
        else:
            i += 1
    return result


def validate(rst_paths: list[Path] = AGENT_SKILLS_RST_FILES) -> list[str]:
    """Return a list of error strings. Empty list means all files are valid.

    Validates each file individually, then checks for duplicate IDs across files.
    """
    errors: list[str] = []
    seen_ids: dict[str, Path] = {}  # skill_id -> first file that defined it
    total_skills = 0

    for rst_path in rst_paths:
        if not rst_path.exists():
            errors.append(f"source file not found: {rst_path}")
            continue

        directives = _parse_directives(rst_path)
        total_skills += len(directives)

        for line_no, opts in directives:
            skill_id = opts.get("id", "").strip()

            # Rule: id is required
            if not skill_id:
                errors.append(f"{rst_path}:{line_no}: skill is missing required field 'id'")
                continue

            # Rule: duplicate IDs across files
            if skill_id in seen_ids:
                errors.append(
                    f"{rst_path}:{line_no}: duplicate skill id '{skill_id}' "
                    f"(already defined in {seen_ids[skill_id]})"
                )
            else:
                seen_ids[skill_id] = rst_path

            # Rule: at least one step field
            has_step = any(opts.get(f, "").strip() for f in STEP_FIELDS)
            if not has_step:
                errors.append(
                    f"{rst_path}:{line_no}: skill '{skill_id}' has no executable step "
                    f"(needs at least one of: {', '.join(STEP_FIELDS)})"
                )

            # Rule: params values must end with :required or :optional
            raw_params = opts.get("params", "").strip()
            if raw_params:
                for raw_param in raw_params.split(","):
                    raw = raw_param.strip()
                    if raw and not any(raw.endswith(s) for s in VALID_PARAM_SUFFIXES):
                        errors.append(
                            f"{rst_path}:{line_no}: skill '{skill_id}': param '{raw}' must end "
                            f"with ':required' or ':optional'"
                        )

    return errors


def main() -> int:
    errors = validate()
    if errors:
        print(f"ERROR: agent skills validation failed ({len(errors)} issue(s)):")
        for err in errors:
            print(f"  {err}")
        return 1
    total = sum(len(_parse_directives(p)) for p in AGENT_SKILLS_RST_FILES if p.exists())
    print(f"OK: agent skills valid ({total} skill(s) across {len(AGENT_SKILLS_RST_FILES)} source files)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
