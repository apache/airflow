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

"""
Extract agent skills from ``.github/skills/breeze-contribution/SKILL.md`` and
generate ``skills.json``.

Skill authors embed one or more JSON objects inside fenced code blocks:

```json
{ "id": "...", "commands": { "host": "...", "breeze": "..." }, ... }
```

The extractor:
1. Finds all `````json`` blocks in the markdown content.
2. Parses them as JSON (and fails fast on malformed JSON).
3. Validates required fields (currently ``id`` and ``commands``).
4. Writes the normalized ``skills.json`` payload.

Future evolution
-----------------
Long-term we want to support additional sources such as contributor docs
(``contributing-docs/*.rst``). Keep the extraction logic isolated so new
sources can be added without changing validation and output shape.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_skills_from_markdown(markdown_content: str) -> list[dict]:
    """
    Extract skill definitions from ``SKILL.md`` markdown.

    Looks for fenced JSON blocks (`````json``) within markdown content.

    Args:
        markdown_content: Markdown content of ``SKILL.md``.

    Returns:
        List of skill dictionaries.

    Raises:
        ValueError: If JSON parsing fails, required fields are missing, or
            duplicate skill IDs are found.
    """
    if not markdown_content.strip():
        raise ValueError("SKILL.md is empty. Add one or more fenced ```json blocks with skill definitions.")

    # Find all JSON blocks in markdown.
    json_pattern = r"```json\s*(.*?)\s*```"
    matches = re.findall(json_pattern, markdown_content, re.DOTALL)

    if not matches:
        raise ValueError(
            "No ```json blocks found in SKILL.md. Add one or more fenced code blocks that contain a single JSON object per skill."
        )

    skills: list[dict] = []
    for idx, match in enumerate(matches, start=1):
        try:
            skill = json.loads(match)
        except json.JSONDecodeError as e:
            snippet = match.strip().replace("\\n", " ")
            snippet = snippet[:200] + ("..." if len(snippet) > 200 else "")
            raise ValueError(
                f"Malformed JSON in SKILL.md (block #{idx}). {e.msg} at char {e.pos}. "
                f"Fix the JSON inside the ```json ...``` fence. Snippet: {snippet}"
            ) from e

        if not isinstance(skill, dict):
            raise ValueError(f"Skill JSON block #{idx} must decode to an object.")

        missing_fields = [k for k in ("id", "commands") if k not in skill]
        if missing_fields:
            raise ValueError(
                f"Skill JSON block #{idx} is missing required fields: {', '.join(missing_fields)}. "
                "Each skill must define an 'id' and a 'commands' object."
            )

        commands = skill["commands"]
        if not isinstance(commands, dict):
            raise ValueError(f"Skill {skill.get('id')!r} must define 'commands' as an object.")

        # Allow empty 'commands' during extraction so tests can focus on other
        # properties (e.g. duplicate detection). CI-level validation can be
        # extended later to enforce non-empty mappings if desired.

        # Optional parameter schema validation for better DX.
        if "parameters" in skill:
            parameters = skill["parameters"]
            if not isinstance(parameters, dict):
                raise ValueError(f"Skill {skill['id']!r} has 'parameters' but it is not an object.")
            for param_name, param_spec in parameters.items():
                if not isinstance(param_spec, dict):
                    raise ValueError(f"Skill {skill['id']!r} parameter {param_name!r} must be an object.")
                if "description" not in param_spec:
                    raise ValueError(
                        f"Skill {skill['id']!r} parameter {param_name!r} is missing required field 'description'."
                    )
                # 'required' is strongly recommended for agent-facing workflows.
                if "required" not in param_spec:
                    raise ValueError(
                        f"Skill {skill['id']!r} parameter {param_name!r} should include 'required' (true/false)."
                    )

        skills.append(skill)

    # Check for duplicate IDs.
    ids = [s["id"] for s in skills]
    duplicates = sorted({i for i in ids if ids.count(i) > 1})
    if duplicates:
        raise ValueError(f"Duplicate skill IDs found: {', '.join(duplicates)}")

    logger.debug("Extracted %d skills from SKILL.md", len(skills))
    return skills


def validate_skills_json(skills_data: dict) -> bool:
    """
    Validate the structure of the generated ``skills.json`` payload.

    Args:
        skills_data: Parsed skills.json data.

    Returns:
        True if valid.

    Raises:
        ValueError: If validation fails
    """
    if not isinstance(skills_data, dict):
        raise ValueError("skills.json root must be a JSON object")

    if "version" not in skills_data:
        raise ValueError("skills.json missing required field: version")

    if "skills" not in skills_data:
        raise ValueError("skills.json missing required field: skills")

    skills = skills_data["skills"]
    if not isinstance(skills, list):
        raise ValueError("skills.json field 'skills' must be a list")

    for idx, skill in enumerate(skills, start=1):
        if not isinstance(skill, dict):
            raise ValueError(f"skills.json skills[{idx}] must be an object")
        if "id" not in skill:
            raise ValueError("Skill missing required field: id")
        if "commands" not in skill:
            raise ValueError(f"Skill {skill.get('id')!r} missing required field: commands")

    return True


def extract_and_generate(skill_md_path: str, skills_json_path: str) -> None:
    """
    Extract skills from a ``SKILL.md`` file and generate a ``skills.json``.

    Args:
        skill_md_path: Path to ``SKILL.md``.
        skills_json_path: Path to output ``skills.json``.

    Raises:
        ValueError: If extraction/validation fails.
        OSError: If reading/writing files fails.
    """
    skill_md = Path(skill_md_path)
    if not skill_md.exists():
        raise FileNotFoundError(
            f"SKILL.md not found at {skill_md_path}. Fix: ensure the file exists and contains one or more fenced ```json blocks."
        )

    try:
        content = skill_md.read_text(encoding="utf-8")
    except OSError as e:
        raise OSError(f"Failed reading {skill_md_path}: {e}") from e

    logger.info("Extracting skills from %s", skill_md_path)
    skills = extract_skills_from_markdown(content)

    skills_data = {"version": "1.0", "skills": skills}
    validate_skills_json(skills_data)

    output_path = Path(skills_json_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        output_path.write_text(json.dumps(skills_data, indent=2) + "\n", encoding="utf-8")
    except OSError as e:
        raise OSError(f"Failed writing {skills_json_path}: {e}") from e

    print(f"✓ Generated skills.json with {len(skills)} skills")


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python extract_breeze_contribution_skills.py <SKILL.md> <skills.json>")
        sys.exit(1)

    skill_md_path = sys.argv[1]
    skills_json_path = sys.argv[2]

    extract_and_generate(skill_md_path, skills_json_path)
