# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path

from agent_skills_poc.model import Workflow


class SkillGenerationError(ValueError):
    """Raised when generated Agent Skill files are invalid."""


def render_skill_markdown(workflow: Workflow) -> str:
    """Render one workflow into Agent Skills standard SKILL.md content."""
    title = workflow.id.replace("-", " ").title()
    return (
        "---\n"
        f"name: {workflow.id}\n"
        f"description: {workflow.description}\n"
        "---\n\n"
        f"# {title}\n\n"
        "## When to use\n"
        "Use this when validating Airflow changes with this workflow.\n\n"
        "## Instructions\n\n"
        "1. Detect execution context:\n"
        "   - If running inside a Breeze container, use Breeze commands.\n"
        "   - Otherwise, use local environment commands.\n\n"
        "2. Execute:\n\n"
        "### Local\n"
        f"{workflow.local}\n\n"
        "### Breeze\n"
        f"{workflow.fallback}\n\n"
        "3. If local execution fails due to missing dependencies, fallback to Breeze.\n\n"
        "4. Return clear success/failure signals.\n"
    )


def validate_skill_markdown(skill_markdown: str) -> None:
    """Validate required Agent Skills frontmatter and sections."""
    lines = skill_markdown.splitlines()
    if len(lines) < 6 or lines[0] != "---":
        raise SkillGenerationError("SKILL.md missing YAML frontmatter start")

    try:
        closing_index = lines.index("---", 1)
    except ValueError as err:
        raise SkillGenerationError("SKILL.md missing YAML frontmatter end") from err

    frontmatter = lines[1:closing_index]
    frontmatter_map: dict[str, str] = {}
    for entry in frontmatter:
        if ":" not in entry:
            raise SkillGenerationError(f"invalid frontmatter entry: {entry}")
        key, value = entry.split(":", 1)
        frontmatter_map[key.strip()] = value.strip()

    if not frontmatter_map.get("name"):
        raise SkillGenerationError("frontmatter must contain non-empty name")
    if not frontmatter_map.get("description"):
        raise SkillGenerationError("frontmatter must contain non-empty description")

    content = "\n".join(lines[closing_index + 1 :])
    if "## Instructions" not in content:
        raise SkillGenerationError("SKILL.md missing Instructions section")
    if "### Local" not in content or "### Breeze" not in content:
        raise SkillGenerationError("SKILL.md must include Local and Breeze execution sections")


def generate_agent_skills(workflows: list[Workflow], output_dir: str | Path) -> list[Path]:
    """Generate one skill directory per workflow under output_dir."""
    root = Path(output_dir)
    root.mkdir(parents=True, exist_ok=True)

    generated_files: list[Path] = []
    for workflow in workflows:
        skill_dir = root / workflow.id
        skill_dir.mkdir(parents=True, exist_ok=True)
        skill_md = skill_dir / "SKILL.md"

        content = render_skill_markdown(workflow)
        validate_skill_markdown(content)
        skill_md.write_text(content, encoding="utf-8")
        generated_files.append(skill_md)

    return generated_files
