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
from __future__ import annotations

"""
Extract agent skill definitions from SKILL.md marker comments and write skills.json.

This script mirrors the pattern used by update-breeze-cmd-output (line 909 of
.pre-commit-config.yaml): a prek hook reads a source file, generates a derived
artifact, and fails CI if the committed artifact diverges from what would be generated.

Markers in SKILL.md look like:
    <!-- agent-skill-sync: workflow=run-tests host="uv run..." breeze="pytest..." -->

This script:
1. Parses those markers from SKILL.md
2. Builds a structured skills dict
3. Writes .github/skills/breeze-contribution/skills.json
4. Exits 1 if the committed skills.json differs from what was just generated
   (drift detection — same principle as update-breeze-cmd-output)

Usage:
    # Generate/update skills.json:
    python3 scripts/ci/prek/extract_agent_skills.py

    # Check for drift only (CI mode — exits 1 if drift detected):
    python3 scripts/ci/prek/extract_agent_skills.py --check
"""

import argparse
import json
import re
import sys
from pathlib import Path

# Canonical paths — relative to repo root
SKILL_MD = Path(".github/skills/breeze-contribution/SKILL.md")
SKILLS_JSON = Path(".github/skills/breeze-contribution/skills.json")

# Regex to match agent-skill-sync marker lines
# Handles both quoted and unquoted values:
#   workflow=run-tests
#   host="uv run --project {dist} pytest {path} -xvs"
#   fallback=missing_system_deps
MARKER_RE = re.compile(r"<!--\s*agent-skill-sync:\s*(.+?)\s*-->")
FIELD_RE = re.compile(r'(\w+)=(?:"([^"]*?)"|(\S+))')


def parse_marker(line: str) -> dict[str, str] | None:
    """
    Parse a single agent-skill-sync marker line into a dict.

    Returns None if the line does not contain a valid marker.
    """
    match = MARKER_RE.search(line)
    if not match:
        return None

    fields: dict[str, str] = {}
    for field_match in FIELD_RE.finditer(match.group(1)):
        key = field_match.group(1)
        # group(2) = quoted value, group(3) = unquoted value
        value = field_match.group(2) if field_match.group(2) is not None else field_match.group(3)
        fields[key] = value

    if "workflow" not in fields:
        return None

    return fields


RST_SKILL_RE = re.compile(r"[.][.] agent-skill::[ ]*\n(?P<fields>(?:[ ]{3}:[^:]+: .+\n)+)", re.MULTILINE)
RST_FIELD_RE = re.compile(r"[ ]{3}:([^:]+): (.+)")


def extract_skills_from_rst(rst_path: Path) -> list[dict[str, str]]:
    """Extract agent-skill directives from RST contributing docs."""
    if not rst_path.exists():
        return []
    skills = []
    for match in RST_SKILL_RE.finditer(rst_path.read_text(encoding="utf-8")):
        fields = dict(RST_FIELD_RE.findall(match.group("fields")))
        if "id" in fields:
            fields["workflow"] = fields.pop("id")
            skills.append(fields)
    return skills


def extract_skills(skill_md_path: Path) -> list[dict[str, str]]:
    """
    Read SKILL.md and extract all agent-skill-sync markers.

    Returns a list of skill dicts, one per marker found.
    """
    if not skill_md_path.exists():
        print(f"ERROR: {skill_md_path} not found. Run from repo root.", file=sys.stderr)
        sys.exit(1)

    skills = []
    for line in skill_md_path.read_text(encoding="utf-8").splitlines():
        parsed = parse_marker(line)
        if parsed:
            skills.append(parsed)

    return skills


def build_skills_json(skills: list[dict[str, str]]) -> dict:
    """
    Build the full skills.json structure from extracted skill dicts.
    """
    return {
        "$schema": "breeze-agent-skills/v1",
        "source": str(SKILL_MD),
        "description": (
            "Auto-generated from agent-skill-sync markers in SKILL.md. "
            "Do not edit manually — update SKILL.md markers instead."
        ),
        "skills": [
            {
                "workflow": s["workflow"],
                "host": s.get("host", ""),
                "breeze": s.get("breeze", ""),
                "fallback_condition": s.get("fallback", s.get("fallback_condition", "never")),
            }
            for s in skills
        ],
    }


def write_skills_json(data: dict, output_path: Path) -> None:
    """Write skills dict to JSON file with stable formatting."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )


def check_drift(generated: dict, existing_path: Path) -> bool:
    """
    Returns True if drift is detected (committed file differs from generated).
    Returns False if they match.
    """
    if not existing_path.exists():
        print(f"DRIFT: {existing_path} does not exist but should be generated.", file=sys.stderr)
        return True

    committed = json.loads(existing_path.read_text(encoding="utf-8"))

    # Compare only the skills list — ignore metadata fields like description
    if committed.get("skills") != generated.get("skills"):
        print("DRIFT DETECTED: committed skills.json does not match SKILL.md markers.", file=sys.stderr)
        print("Run: python3 scripts/ci/prek/extract_agent_skills.py", file=sys.stderr)
        print("Then commit the updated skills.json.", file=sys.stderr)
        return True

    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract agent skills from SKILL.md and write/validate skills.json"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check mode: exit 1 if committed skills.json differs from SKILL.md markers (for CI)",
    )
    parser.add_argument(
        "--skill-md",
        type=Path,
        default=SKILL_MD,
        help=f"Path to SKILL.md (default: {SKILL_MD})",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=SKILLS_JSON,
        help=f"Path to output skills.json (default: {SKILLS_JSON})",
    )
    args = parser.parse_args()

    skills = extract_skills(args.skill_md)
    rst_path = Path("contributing-docs/03_contributors_quick_start.rst")
    skills += extract_skills_from_rst(rst_path)

    if not skills:
        print(
            f"WARNING: No agent-skill-sync markers found in {args.skill_md}.",
            file=sys.stderr,
        )
        sys.exit(0)

    generated = build_skills_json(skills)

    if args.check:
        drift = check_drift(generated, args.output)
        if drift:
            sys.exit(1)
        print(f"OK: {args.output} is in sync with {args.skill_md}")
        sys.exit(0)

    # Write mode
    write_skills_json(generated, args.output)
    print(f"Written {len(skills)} skill(s) to {args.output}")
    for s in skills:
        print(f"  - {s['workflow']}")


if __name__ == "__main__":
    main()
