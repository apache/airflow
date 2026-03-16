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
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

import yaml

# Add current directory to path to allow common_prek_utils import
sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_ROOT_PATH, console

SKILL_DOC_PATH = AIRFLOW_ROOT_PATH / ".github" / "skills" / "breeze-contribution" / "SKILL.md"
OUTPUT_SKILLS_PATH = AIRFLOW_ROOT_PATH / ".github" / "skills" / "breeze-contribution" / "skills.json"

SKILL_BLOCK_RE = re.compile(r"```agent-skill\\n(.*?)\\n```", re.DOTALL)


def _extract_skills_from_markdown(markdown: str) -> list[dict]:
    matches = SKILL_BLOCK_RE.findall(markdown)
    if not matches:
        return []
    if len(matches) > 1:
        raise ValueError("Expected exactly one ```agent-skill``` block in SKILL.md")
    skills = yaml.safe_load(matches[0])
    if skills is None:
        return []
    if not isinstance(skills, list):
        raise TypeError("agent-skill block must be a YAML list")
    return skills


def extract_skills(check_only: bool = False) -> None:
    if not SKILL_DOC_PATH.exists():
        console.print(f"[red]Error: {SKILL_DOC_PATH} not found.[/]")
        raise SystemExit(1)

    markdown = SKILL_DOC_PATH.read_text()
    try:
        skills = _extract_skills_from_markdown(markdown)
    except (ValueError, TypeError, yaml.YAMLError) as e:
        console.print(f"[red]Error parsing {SKILL_DOC_PATH}: {e}[/]")
        raise SystemExit(1)

    new_content = json.dumps(skills, indent=2, sort_keys=True) + "\n"

    if check_only:
        if not OUTPUT_SKILLS_PATH.exists():
            console.print(f"[red]Drift detected: {OUTPUT_SKILLS_PATH} does not exist.[/]")
            console.print(
                "[yellow]Please run: python scripts/ci/prek/extract_breeze_contribution_skills.py[/]"
            )
            raise SystemExit(1)
        current_content = OUTPUT_SKILLS_PATH.read_text()
        if current_content != new_content:
            console.print(
                f"[red]Drift detected: {OUTPUT_SKILLS_PATH} is out of sync with {SKILL_DOC_PATH}.[/]"
            )
            console.print(
                "[yellow]Please run: python scripts/ci/prek/extract_breeze_contribution_skills.py[/]"
            )
            raise SystemExit(1)
        console.print("[success]Breeze contribution skills are in sync.[/]")
        return

    OUTPUT_SKILLS_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_SKILLS_PATH.write_text(new_content)
    console.print(f"[success]Wrote {OUTPUT_SKILLS_PATH}[/]")


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract Breeze contribution skills from SKILL.md")
    parser.add_argument("--check", action="store_true", help="Check for drift without writing")
    args = parser.parse_args()
    extract_skills(check_only=args.check)


if __name__ == "__main__":
    main()
