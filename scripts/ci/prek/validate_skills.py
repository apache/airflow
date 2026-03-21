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
"""Validate and optionally regenerate Breeze agent skills drift.

This script is used by the project's prek/pre-commit pipeline to ensure that:

- The structured skill source (``SKILL.md``) is kept in sync with
- the machine-readable artifact (``skills.json``)

If drift is detected, the check fails with detailed information and an exact
command to fix the problem.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent))

import extract_breeze_contribution_skills

logger = logging.getLogger(__name__)


DEFAULT_SKILL_MD = Path(".github/skills/breeze-contribution/SKILL.md")
DEFAULT_SKILLS_JSON = Path(".github/skills/breeze-contribution/skills.json")


@dataclass(frozen=True)
class DriftReport:
    drift: bool
    summary: str
    details: dict[str, Any]


def _load_skills_json(skills_json_path: Path) -> dict[str, Any]:
    """Load skills.json and return parsed JSON.

    Raises:
        ValueError: if skills.json cannot be parsed
    """
    try:
        raw = skills_json_path.read_text(encoding="utf-8")
    except OSError as e:
        raise OSError(f"Failed reading {skills_json_path}: {e}") from e

    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Malformed skills.json at {skills_json_path}: {e}") from e


def compute_drift_report(
    skill_md_path: Path, skills_json_path: Path, *, verbose: bool = False
) -> DriftReport:
    """Compute a drift report between SKILL.md and skills.json.

    Args:
        skill_md_path: Path to the structured skill source (SKILL.md).
        skills_json_path: Path to the generated machine artifact (skills.json).
        verbose: If True, include additional details in the report.

    Returns:
        A :class:`DriftReport` with drift boolean and detailed messages.
    """
    if verbose:
        logger.setLevel(logging.DEBUG)

    if not skill_md_path.exists():
        return DriftReport(
            drift=True,
            summary=f"Missing {skill_md_path}",
            details={
                "action": f"Create {skill_md_path} and run with --fix to regenerate skills.json.",
            },
        )

    if not skills_json_path.exists():
        return DriftReport(
            drift=True,
            summary=f"Missing {skills_json_path}",
            details={"action": f"Run with --fix to regenerate {skills_json_path}."},
        )

    # Extract from SKILL.md
    try:
        skill_md_content = skill_md_path.read_text(encoding="utf-8")
    except OSError as e:
        return DriftReport(
            drift=True,
            summary=f"Failed reading {skill_md_path}",
            details={"error": str(e), "action": "Fix file permissions and re-run."},
        )

    try:
        extracted_skills = extract_breeze_contribution_skills.extract_skills_from_markdown(skill_md_content)
    except Exception as e:
        return DriftReport(
            drift=True,
            summary="SKILL.md extraction failed",
            details={"error": str(e), "action": "Fix SKILL.md JSON blocks so extraction succeeds."},
        )

    # Load skills.json
    try:
        current_skills_payload = _load_skills_json(skills_json_path)
    except Exception as e:
        return DriftReport(
            drift=True,
            summary="skills.json loading failed",
            details={"error": str(e), "action": "Run with --fix to regenerate skills.json."},
        )

    current_skills = current_skills_payload.get("skills", [])
    if not isinstance(current_skills, list):
        return DriftReport(
            drift=True,
            summary="skills.json has invalid 'skills' shape",
            details={"action": "Run with --fix to regenerate."},
        )

    extracted_ids = [s.get("id") for s in extracted_skills]
    extracted_id_set = set(extracted_ids)
    current_ids = {s.get("id") for s in current_skills if isinstance(s, dict)}

    missing_in_json = extracted_id_set - current_ids
    extra_in_json = current_ids - extracted_id_set

    if missing_in_json or extra_in_json:
        return DriftReport(
            drift=True,
            summary="Skill ID set mismatch",
            details={
                "missing_in_skills_json": sorted(missing_in_json),
                "extra_in_skills_json": sorted(extra_in_json),
                "action": "Run with --fix to regenerate skills.json from SKILL.md.",
            },
        )

    # Compare definitions. Keep order-insensitive comparison by id.
    extracted_by_id = {s["id"]: s for s in extracted_skills}
    current_by_id = {s["id"]: s for s in current_skills if isinstance(s, dict) and "id" in s}

    differing_ids: list[str] = []
    for skill_id in sorted(extracted_id_set):
        if extracted_by_id.get(skill_id) != current_by_id.get(skill_id):
            differing_ids.append(skill_id)

    if differing_ids:
        return DriftReport(
            drift=True,
            summary="Skill definitions differ",
            details={
                "differing_skill_ids": differing_ids,
                "action": f"Run with --fix to regenerate {skills_json_path}.",
            },
        )

    return DriftReport(
        drift=False,
        summary="SKILL.md and skills.json are in sync",
        details={},
    )


def regenerate_skills_json(skill_md_path: Path, skills_json_path: Path) -> None:
    """Regenerate skills.json from SKILL.md."""
    extract_breeze_contribution_skills.extract_and_generate(str(skill_md_path), str(skills_json_path))


def main(argv: list[str] | None = None) -> int:
    """Main entry point for the prek hook."""
    parser = argparse.ArgumentParser(description="Validate Breeze skills drift")
    parser.add_argument("--check", action="store_true", help="Check drift and fail if out of sync")
    parser.add_argument("--fix", action="store_true", help="Regenerate skills.json from SKILL.md")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--skill-md", type=Path, default=DEFAULT_SKILL_MD, help="Path to SKILL.md")
    parser.add_argument(
        "--skills-json",
        type=Path,
        default=DEFAULT_SKILLS_JSON,
        help="Path to skills.json",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO)

    if args.check and args.fix:
        print("Invalid arguments: use either --check or --fix")
        return 2

    if args.fix:
        try:
            regenerate_skills_json(args.skill_md, args.skills_json)
        except Exception as e:  # pragma: no cover (defensive)
            print(f"❌ Failed to regenerate skills.json: {e}")
            return 1
        print(f"✓ Regenerated {args.skills_json}")
        return 0

    report = compute_drift_report(args.skill_md, args.skills_json, verbose=args.verbose)
    if report.drift:
        print(f"❌ Drift detected: {report.summary}")
        if report.details:
            for k, v in report.details.items():
                print(f"  {k}: {v}")
        print(
            "How to fix: run "
            f"`./scripts/ci/prek/validate_skills.py --fix --skill-md {args.skill_md} --skills-json {args.skills_json}`"
        )
        return 1

    print("✓ Skills in sync")
    return 0


if __name__ == "__main__":
    sys.exit(main())
