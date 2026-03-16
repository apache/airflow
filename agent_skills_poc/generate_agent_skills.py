# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import argparse
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_skills_poc.generator import SkillGenerationError, generate_agent_skills
from agent_skills_poc.parser import WorkflowParseError, parse_workflows


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate Agent Skills folders from executable contributor workflows"
    )
    parser.add_argument("input_rst", help="Path to RST workflow source")
    parser.add_argument(
        "output_dir",
        nargs="?",
        default="skills",
        help="Output directory for generated skills (default: skills)",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    try:
        workflows = parse_workflows(args.input_rst)
        generate_agent_skills(workflows, args.output_dir)
    except (WorkflowParseError, SkillGenerationError) as err:
        print(f"ERROR: {err}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
