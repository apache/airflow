# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import TYPE_CHECKING

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.parser.parser import WorkflowParseError, parse_workflows

if TYPE_CHECKING:
    from agent_skills_poc.model.workflow import Workflow


class SkillsGenerationError(ValueError):
    """Raised when generated skills JSON does not match the expected shape."""


def build_skills_payload(workflows: list[Workflow]) -> dict[str, object]:
    payload: dict[str, object] = {"skills": [workflow.as_skill() for workflow in workflows]}
    validate_payload(payload)
    return payload


def validate_payload(payload: dict[str, object]) -> None:
    if "skills" not in payload:
        raise SkillsGenerationError("payload must contain top-level 'skills' key")
    skills = payload["skills"]
    if not isinstance(skills, list):
        raise SkillsGenerationError("'skills' must be a list")

    for index, skill in enumerate(skills):
        if not isinstance(skill, dict):
            raise SkillsGenerationError(f"skill[{index}] must be an object")
        if "id" not in skill or not isinstance(skill["id"], str) or not skill["id"].strip():
            raise SkillsGenerationError(f"skill[{index}].id must be a non-empty string")
        steps = skill.get("steps")
        if not isinstance(steps, list) or len(steps) != 2:
            raise SkillsGenerationError(f"skill[{index}].steps must contain exactly 2 steps")

        expected_types = ["local", "fallback"]
        for step_index, expected in enumerate(expected_types):
            step = steps[step_index]
            if not isinstance(step, dict):
                raise SkillsGenerationError(f"skill[{index}].steps[{step_index}] must be an object")
            if step.get("type") != expected:
                raise SkillsGenerationError(f"skill[{index}].steps[{step_index}].type must be '{expected}'")
            command = step.get("command")
            if not isinstance(command, str) or not command.strip():
                raise SkillsGenerationError(
                    f"skill[{index}].steps[{step_index}].command must be a non-empty string"
                )


def generate_skills(input_path: str | Path, output_path: str | Path) -> dict[str, object]:
    workflows = parse_workflows(input_path)
    payload = build_skills_payload(workflows)

    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(f"{json.dumps(payload, indent=2)}\n", encoding="utf-8")
    return payload


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate skills.json from RST executable documentation blocks"
    )
    parser.add_argument("input_rst", help="Path to source RST documentation file")
    parser.add_argument("output_json", help="Path to output skills JSON file")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    try:
        generate_skills(args.input_rst, args.output_json)
    except (WorkflowParseError, SkillsGenerationError) as err:
        print(f"ERROR: {err}")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
