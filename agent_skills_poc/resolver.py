# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_skills_poc.context import ExecutionContext, detect_context


class CommandResolutionError(ValueError):
    """Raised when a command cannot be resolved from skills."""


def load_skills(skills_path: str | Path) -> dict[str, Any]:
    """
    Load and parse skills.json file.

    Args:
        skills_path: Path to the skills.json file.

    Returns:
        Parsed skills dictionary.

    Raises:
        CommandResolutionError: If file not found or JSON is invalid.
    """
    path = Path(skills_path)
    if not path.exists():
        raise CommandResolutionError(f"skills file not found: {path}")

    try:
        content = path.read_text(encoding="utf-8")
        return json.loads(content)
    except json.JSONDecodeError as err:
        raise CommandResolutionError(f"invalid JSON in skills file: {err}") from err


def resolve_command(
    skill_id: str,
    skills_path: str | Path | None = None,
    context: ExecutionContext | None = None,
) -> str:
    """
    Resolve the correct command for a skill based on runtime environment.

    Args:
        skill_id: The skill identifier to resolve.
        skills_path: Path to skills.json. If None, searches common locations.
        context: ExecutionContext. If None, auto-detects environment.

    Returns:
        The command string to execute.

    Raises:
        CommandResolutionError: If skill not found or command cannot be resolved.
    """
    if skills_path is None:
        # Search for skills.json in common locations
        candidates = [
            Path(__file__).resolve().parents[1] / "agent_skills_poc" / "output" / "skills.json",
            Path.cwd() / "output" / "skills.json",
            Path.cwd() / "agent_skills_poc" / "output" / "skills.json",
        ]
        skills_path = None
        for candidate in candidates:
            if candidate.exists():
                skills_path = candidate
                break

        if skills_path is None:
            raise CommandResolutionError("skills.json not found in common locations")

    if context is None:
        context = detect_context()

    skills = load_skills(skills_path)

    if "skills" not in skills:
        raise CommandResolutionError("skills.json missing 'skills' key")

    skill_list = skills["skills"]
    if not isinstance(skill_list, list):
        raise CommandResolutionError("'skills' must be a list")

    # Find the skill by ID
    skill = None
    for s in skill_list:
        if s.get("id") == skill_id:
            skill = s
            break

    if skill is None:
        available = [s.get("id") for s in skill_list if isinstance(s, dict)]
        raise CommandResolutionError(f"skill '{skill_id}' not found. Available: {available}")

    # Extract steps
    steps = skill.get("steps")
    if not isinstance(steps, list) or len(steps) < 1:
        raise CommandResolutionError(f"skill '{skill_id}' has no valid steps")

    # Resolve based on environment
    # Strategy: try local first, fallback to fallback step if in host
    if context.is_host():
        # On host, prefer local step, then fallback
        for step in steps:
            if step.get("type") == "local":
                command = step.get("command")
                if command:
                    return command

        for step in steps:
            if step.get("type") == "fallback":
                command = step.get("command")
                if command:
                    return command

    else:  # breeze-container
        # In container, use local step
        for step in steps:
            if step.get("type") == "local":
                command = step.get("command")
                if command:
                    return command

    raise CommandResolutionError(
        f"no valid command found in skill '{skill_id}' for environment '{context.environment}'"
    )
