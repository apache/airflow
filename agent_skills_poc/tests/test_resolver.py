# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S101

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.context import ExecutionContext
from agent_skills_poc.resolver import (
    CommandResolutionError,
    load_skills,
    resolve_command,
)


def test_load_skills_reads_valid_json(tmp_path) -> None:
    """Test loading a valid skills.json file."""
    skills_path = tmp_path / "skills.json"
    test_skills = {
        "skills": [
            {
                "id": "test_skill",
                "steps": [
                    {"type": "local", "command": "local_cmd"},
                    {"type": "fallback", "command": "fallback_cmd"},
                ],
            }
        ]
    }
    skills_path.write_text(json.dumps(test_skills) + "\n", encoding="utf-8")

    loaded = load_skills(skills_path)
    assert loaded == test_skills


def test_load_skills_raises_on_missing_file() -> None:
    """Test that load_skills raises when file doesn't exist."""
    with pytest.raises(CommandResolutionError, match="not found"):
        load_skills(Path("/nonexistent/skills.json"))


def test_load_skills_raises_on_invalid_json(tmp_path) -> None:
    """Test that load_skills raises on malformed JSON."""
    skills_path = tmp_path / "skills.json"
    skills_path.write_text("{ invalid json", encoding="utf-8")

    with pytest.raises(CommandResolutionError, match="invalid JSON"):
        load_skills(skills_path)


def test_resolve_command_host_environment(tmp_path) -> None:
    """Test command resolution in host environment."""
    skills_path = tmp_path / "skills.json"
    test_skills = {
        "skills": [
            {
                "id": "run_tests",
                "steps": [
                    {"type": "local", "command": "uv run pytest"},
                    {"type": "fallback", "command": "breeze exec pytest"},
                ],
            }
        ]
    }
    skills_path.write_text(json.dumps(test_skills) + "\n", encoding="utf-8")

    ctx_host = ExecutionContext(environment="host")
    command = resolve_command("run_tests", skills_path=skills_path, context=ctx_host)
    assert command == "uv run pytest"


def test_resolve_command_breeze_environment(tmp_path) -> None:
    """Test command resolution in Breeze container environment."""
    skills_path = tmp_path / "skills.json"
    test_skills = {
        "skills": [
            {
                "id": "run_tests",
                "steps": [
                    {"type": "local", "command": "uv run pytest"},
                    {"type": "fallback", "command": "breeze exec pytest"},
                ],
            }
        ]
    }
    skills_path.write_text(json.dumps(test_skills) + "\n", encoding="utf-8")

    ctx_breeze = ExecutionContext(environment="breeze-container")
    command = resolve_command("run_tests", skills_path=skills_path, context=ctx_breeze)
    assert command == "uv run pytest"


def test_resolve_command_raises_on_missing_skill(tmp_path) -> None:
    """Test that resolve_command raises when skill not found."""
    skills_path = tmp_path / "skills.json"
    test_skills = {"skills": []}
    skills_path.write_text(json.dumps(test_skills) + "\n", encoding="utf-8")

    with pytest.raises(CommandResolutionError, match="not found"):
        resolve_command("nonexistent", skills_path=skills_path)


def test_resolve_command_raises_on_missing_command(tmp_path) -> None:
    """Test resolve_command when skill has no valid command."""
    skills_path = tmp_path / "skills.json"
    test_skills = {
        "skills": [
            {
                "id": "bad_skill",
                "steps": [
                    {"type": "local", "command": ""},  # empty command
                ],
            }
        ]
    }
    skills_path.write_text(json.dumps(test_skills) + "\n", encoding="utf-8")

    ctx = ExecutionContext(environment="host")
    with pytest.raises(CommandResolutionError, match="no valid command"):
        resolve_command("bad_skill", skills_path=skills_path, context=ctx)
