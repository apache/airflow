#
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
"""Tests for validate_skills.py."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SCRIPT_ROOT))

from agent_skills import validate_skills  # noqa: E402


def test_valid_skills_pass_validation(capsys: pytest.CaptureFixture[str]) -> None:
    """Two valid skills should pass validate_all and print OK."""
    skills = [
        {
            "id": "skill-a",
            "context": "host",
            "category": "testing",
            "description": "Skill A",
            "command": "echo a",
        },
        {
            "id": "skill-b",
            "context": "breeze",
            "category": "linting",
            "description": "Skill B",
            "command": "echo b",
        },
    ]
    ok = validate_skills.validate_all(skills, verbose=False)
    captured = capsys.readouterr()
    assert ok is True
    assert "OK: 2 skills valid" in captured.out


def test_duplicate_id_fails(capsys: pytest.CaptureFixture[str]) -> None:
    """Duplicate ids should fail validation."""
    skills = [
        {
            "id": "dup-skill",
            "context": "host",
            "category": "testing",
            "description": "First",
            "command": "echo first",
        },
        {
            "id": "dup-skill",
            "context": "host",
            "category": "testing",
            "description": "Second",
            "command": "echo second",
        },
    ]
    ok = validate_skills.validate_all(skills, verbose=False)
    captured = capsys.readouterr()
    assert ok is False
    assert "Duplicate skill id" in captured.out


def test_invalid_context_fails() -> None:
    """Invalid context should be reported by validate_skill."""
    skill = {
        "id": "bad-context",
        "context": "container",
        "category": "testing",
        "description": "Bad",
        "command": "echo bad",
    }
    errors = validate_skills.validate_skill(skill)
    assert any("Invalid context" in e for e in errors)


def test_missing_required_field_fails() -> None:
    """Missing description should produce an error."""
    skill = {
        "id": "no-desc",
        "context": "host",
        "category": "testing",
        "command": "echo ok",
    }
    errors = validate_skills.validate_skill(skill)
    assert any("Missing required field" in e and "'description'" in e for e in errors)


def test_verbose_output_lists_all_skills(capsys: pytest.CaptureFixture[str]) -> None:
    """Verbose validation should list all skills."""
    skills = [
        {
            "id": "skill-a",
            "context": "host",
            "category": "testing",
            "description": "Skill A",
            "command": "echo a",
        },
        {
            "id": "skill-b",
            "context": "breeze",
            "category": "linting",
            "description": "Skill B",
            "command": "echo b",
        },
    ]
    ok = validate_skills.validate_all(skills, verbose=True)
    captured = capsys.readouterr()
    assert ok is True
    assert "OK: 2 skills valid" in captured.out
    assert "skill-a" in captured.out
    assert "skill-b" in captured.out

