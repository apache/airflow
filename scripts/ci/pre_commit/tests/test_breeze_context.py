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
"""Tests for breeze_context.py."""

from __future__ import annotations

import json
import pathlib
import sys
from pathlib import Path

import pytest

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(SCRIPT_ROOT))

from agent_skills import breeze_context  # noqa: E402


def test_get_context_returns_breeze_when_env_var_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Set AIRFLOW_BREEZE_CONTAINER and assert get_context() == 'breeze'."""
    monkeypatch.setenv("AIRFLOW_BREEZE_CONTAINER", "1")
    assert breeze_context.get_context() == "breeze"


def test_get_context_returns_host_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no env var and no docker markers, context should be host."""
    monkeypatch.delenv("AIRFLOW_BREEZE_CONTAINER", raising=False)
    original_exists = pathlib.Path.exists

    def fake_exists(self: pathlib.Path) -> bool:  # type: ignore[override]
        if self.as_posix() in {"/.dockerenv", "/opt/airflow"}:
            return False
        return original_exists(self)

    monkeypatch.setattr(pathlib.Path, "exists", fake_exists)
    assert breeze_context.get_context() == "host"


def test_get_context_returns_breeze_with_containerenv(monkeypatch: pytest.MonkeyPatch) -> None:
    """If /.containerenv exists (Podman marker), context should be breeze."""
    monkeypatch.delenv("AIRFLOW_BREEZE_CONTAINER", raising=False)
    original_exists = pathlib.Path.exists

    def fake_exists(self: pathlib.Path) -> bool:  # type: ignore[override]
        if self.as_posix() == "/.containerenv":
            return True
        if self.as_posix() in {"/.dockerenv", "/opt/airflow"}:
            return False
        return original_exists(self)

    monkeypatch.setattr(pathlib.Path, "exists", fake_exists)
    assert breeze_context.get_context() == "breeze"


def test_force_context_overrides_detection(monkeypatch: pytest.MonkeyPatch) -> None:
    """With force set, get_context() should bypass filesystem detection entirely."""
    monkeypatch.delenv("AIRFLOW_BREEZE_CONTAINER", raising=False)

    def bomb_exists(self: pathlib.Path) -> bool:  # type: ignore[override]
        raise AssertionError("Filesystem detection should have been skipped when force is set")

    monkeypatch.setattr(pathlib.Path, "exists", bomb_exists)
    assert breeze_context.get_context(force="host") == "host"


def _write_skills(tmp_path: Path, skills: list[dict]) -> Path:
    skills_file = tmp_path / "skills.json"
    skills_file.write_text(
        json.dumps({"version": "1.0", "skills": skills}),
        encoding="utf-8",
    )
    return skills_file


def test_get_command_returns_correct_for_host(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Mock context=host and ensure run-static-checks returns a command containing 'prek'."""
    skills = [
        {
            "id": "run-static-checks",
            "context": "host",
            "category": "linting",
            "prereqs": "setup-breeze-environment",
            "description": "Run fast static checks with prek",
            "command": "prek run --from-ref main --stage pre-commit",
            "expected_output": "All checks passed.",
        }
    ]
    skills_file = _write_skills(tmp_path, skills)
    monkeypatch.setattr(breeze_context, "SKILLS_JSON", skills_file)
    monkeypatch.setattr(breeze_context, "get_context", lambda: "host")

    cmd = breeze_context.get_command("run-static-checks")
    assert "prek" in cmd
    assert "--stage pre-commit" in cmd


def test_get_command_wrong_context_returns_guidance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """When context is breeze for a host-only skill, return guidance message."""
    skills = [
        {
            "id": "run-static-checks",
            "context": "host",
            "category": "linting",
            "prereqs": "setup-breeze-environment",
            "description": "Run fast static checks with prek",
            "command": "prek run --from-ref main --stage pre-commit",
            "expected_output": "All checks passed.",
        }
    ]
    skills_file = _write_skills(tmp_path, skills)
    monkeypatch.setattr(breeze_context, "SKILLS_JSON", skills_file)
    monkeypatch.setattr(breeze_context, "get_context", lambda: "breeze")

    msg = breeze_context.get_command("run-static-checks")
    assert "requires context" in msg
    assert "host" in msg


def test_list_skills_for_context_filters_correctly(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """list_skills_for_context should only return skills for current context or 'either'."""
    skills = [
        {
            "id": "host-only",
            "context": "host",
            "category": "testing",
            "description": "Host skill",
            "command": "echo host",
        },
        {
            "id": "either-skill",
            "context": "either",
            "category": "testing",
            "description": "Either skill",
            "command": "echo either",
        },
        {
            "id": "breeze-only",
            "context": "breeze",
            "category": "testing",
            "description": "Breeze skill",
            "command": "echo breeze",
        },
    ]
    skills_file = _write_skills(tmp_path, skills)
    monkeypatch.setattr(breeze_context, "SKILLS_JSON", skills_file)
    monkeypatch.setattr(breeze_context, "get_context", lambda: "host")

    result = breeze_context.list_skills_for_context()
    assert all(s["context"] in ("host", "either") for s in result)
