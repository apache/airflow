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

"""Extract structured agent-skill blocks from a markdown PoC file."""

from __future__ import annotations

import json
from pathlib import Path

START_MARKER = "<!-- agent-skill:start -->"
END_MARKER = "<!-- agent-skill:end -->"


def parse_skill_block(block_text: str) -> dict[str, object]:
    """Parse one agent-skill block into a dictionary."""
    parsed: dict[str, object] = {}

    for raw_line in block_text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue

        if ":" not in line:
            raise ValueError(f"Invalid line in skill block: {line}")

        key, value = line.split(":", 1)
        key = key.strip()
        value = value.strip()

        if key == "allowed_contexts":
            parsed[key] = [item.strip() for item in value.split(",") if item.strip()]
        elif key == "fallback_when":
            parsed[key] = [item.strip() for item in value.split("|") if item.strip()]
        else:
            parsed[key] = value

    required_keys = {
        "id",
        "title",
        "preferred_context",
        "allowed_contexts",
        "local_command",
        "breeze_command",
        "inside_breeze_command",
        "fallback_when",
    }

    missing = required_keys - parsed.keys()
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"Missing required keys: {missing_list}")

    return parsed


def extract_skills_from_text(text: str) -> list[dict[str, object]]:
    """Extract all agent-skill blocks from markdown text."""
    skills: list[dict[str, object]] = []
    cursor = 0

    while True:
        start = text.find(START_MARKER, cursor)
        if start == -1:
            break

        end = text.find(END_MARKER, start)
        if end == -1:
            raise ValueError("Found start marker without matching end marker.")

        block_start = start + len(START_MARKER)
        block_text = text[block_start:end].strip()
        skills.append(parse_skill_block(block_text))

        cursor = end + len(END_MARKER)

    return skills


def extract_skills_from_file(input_path: Path) -> list[dict[str, object]]:
    """Read a markdown file and extract all skill blocks."""
    text = input_path.read_text(encoding="utf-8")
    return extract_skills_from_text(text)


def write_skills_json(skills: list[dict[str, object]], output_path: Path) -> None:
    """Write extracted skills to a JSON file."""
    output_path.write_text(
        json.dumps(skills, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def main() -> None:
    """Extract skills from the PoC markdown file and write skills.json."""
    base_dir = Path(__file__).resolve().parent
    input_path = base_dir / "poc_agent_skills.md"
    output_path = base_dir / "skills.json"

    skills = extract_skills_from_file(input_path)
    write_skills_json(skills, output_path)

    print(f"Wrote {len(skills)} skill(s) to {output_path}")


if __name__ == "__main__":
    main()
