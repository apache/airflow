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

import json
from pathlib import Path

import ci.prek.context_detect as cd
import pytest
from ci.prek.context_detect import get_command, get_context, list_skills_for_context

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SKILLS: list[dict] = [
    {
        "id": "run-single-test",
        "category": "testing",
        "description": "Run a targeted test",
        "prereqs": ["setup-breeze-environment"],
        "steps": [
            {
                "context": "host",
                "command": "uv run --project {project} pytest {test_path} -xvs",
                "condition": "system_deps_available",
            },
            {
                "context": "host",
                "command": "breeze run pytest {test_path} -xvs",
                "fallback_for": "system_deps_available",
            },
            {
                "context": "breeze",
                "command": "pytest {test_path} -xvs",
            },
        ],
        "parameters": {
            "project": {"description": "distribution folder", "required": True},
            "test_path": {"description": "path to test", "required": True},
        },
        "expected_output": "passed",
    },
    {
        "id": "run-static-checks",
        "category": "linting",
        "description": "Run fast static checks",
        "prereqs": [],
        "steps": [{"context": "host", "command": "prek run --from-ref {target_branch} --stage pre-commit"}],
        "parameters": {"target_branch": {"description": "target branch", "required": True}},
        "expected_output": "All checks passed.",
    },
    {
        "id": "build-docs",
        "category": "documentation",
        "description": "Build docs",
        "prereqs": [],
        "steps": [{"context": "either", "command": "breeze build-docs"}],
        "parameters": {},
        "expected_output": "Build finished.",
    },
]


def _write_skills(tmp_path: Path, skills: list[dict] | None = None) -> Path:
    skills_file = tmp_path / "skills.json"
    skills_file.write_text(
        json.dumps({"version": "1.0", "skills": skills or SKILLS}, indent=2),
        encoding="utf-8",
    )
    return skills_file


# ---------------------------------------------------------------------------
# get_context
# ---------------------------------------------------------------------------


def test_get_context_returns_breeze_when_env_var_set(monkeypatch):
    monkeypatch.setenv("AIRFLOW_BREEZE_CONTAINER", "1")
    assert get_context() == "breeze"


def test_get_context_returns_host_by_default(monkeypatch, tmp_path):
    monkeypatch.delenv("AIRFLOW_BREEZE_CONTAINER", raising=False)
    # Patch Path.exists to return False for the Breeze markers
    original_exists = Path.exists

    def patched_exists(self):
        if str(self) in {"/.dockerenv", "/opt/airflow"}:
            return False
        return original_exists(self)

    monkeypatch.setattr(Path, "exists", patched_exists)
    assert get_context() == "host"


def test_get_context_env_var_takes_priority_over_markers(monkeypatch):
    monkeypatch.setenv("AIRFLOW_BREEZE_CONTAINER", "1")
    # Even if /.dockerenv doesn't exist, env var wins
    assert get_context() == "breeze"


# ---------------------------------------------------------------------------
# get_command
# ---------------------------------------------------------------------------


def test_get_command_host_context_returns_uv_command(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    monkeypatch.setenv("AIRFLOW_BREEZE_CONTAINER", "")
    monkeypatch.delenv("AIRFLOW_BREEZE_CONTAINER", raising=False)
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    cmd = get_command(
        "run-single-test",
        skills_json=skills_file,
        project="providers/amazon",
        test_path="providers/amazon/tests/test_s3.py",
    )
    assert "uv run" in cmd
    assert "providers/amazon" in cmd
    assert "test_s3.py" in cmd


def test_get_command_uses_fallback_when_condition_false(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    cmd = get_command(
        "run-single-test",
        skills_json=skills_file,
        project="providers/amazon",
        test_path="providers/amazon/tests/test_s3.py",
        system_deps_available="false",
    )
    assert "breeze run pytest" in cmd


def test_get_command_breeze_context_returns_pytest_directly(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    monkeypatch.setattr(cd, "get_context", lambda: "breeze")

    cmd = get_command(
        "run-single-test",
        skills_json=skills_file,
        test_path="providers/amazon/tests/test_s3.py",
    )
    assert cmd.startswith("pytest")
    assert "breeze run" not in cmd


def test_get_command_returns_guidance_when_no_steps_for_context(monkeypatch, tmp_path):
    host_only_skill = {
        "id": "host-only",
        "category": "linting",
        "description": "Host only",
        "prereqs": [],
        "steps": [{"context": "host", "command": "prek run"}],
        "parameters": {},
        "expected_output": "",
    }
    skills_file = _write_skills(tmp_path, [host_only_skill])
    monkeypatch.setattr(cd, "get_context", lambda: "breeze")

    result = get_command("host-only", skills_json=skills_file)
    assert "no steps for context" in result


def test_get_command_raises_on_missing_placeholder(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    with pytest.raises(ValueError, match="Missing parameter"):
        get_command("run-single-test", skills_json=skills_file)  # missing project + test_path


def test_get_command_raises_key_error_on_unknown_skill(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    with pytest.raises(KeyError, match="nonexistent-skill"):
        get_command("nonexistent-skill", skills_json=skills_file)


def test_get_command_either_context_works_from_both(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    for ctx in ("host", "breeze"):
        monkeypatch.setattr(cd, "get_context", lambda c=ctx: c)
        cmd = get_command("build-docs", skills_json=skills_file)
        assert "breeze build-docs" in cmd


def test_get_command_substitutes_parameters(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    cmd = get_command("run-static-checks", skills_json=skills_file, target_branch="v3-1-test")
    assert "v3-1-test" in cmd


# ---------------------------------------------------------------------------
# list_skills_for_context
# ---------------------------------------------------------------------------


def test_list_skills_for_host_context(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    result = list_skills_for_context(skills_json=skills_file)
    ids = [s["id"] for s in result]
    # host skills + either skills should be included
    assert "run-single-test" in ids
    assert "run-static-checks" in ids
    assert "build-docs" in ids  # context: either


def test_list_skills_for_breeze_context(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    monkeypatch.setattr(cd, "get_context", lambda: "breeze")

    result = list_skills_for_context(skills_json=skills_file)
    ids = [s["id"] for s in result]
    assert "run-single-test" in ids  # has a breeze step
    assert "build-docs" in ids  # context: either


def test_list_skills_filtered_by_category(monkeypatch, tmp_path):
    skills_file = _write_skills(tmp_path)
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    result = list_skills_for_context(category="linting", skills_json=skills_file)
    assert all(s["category"] == "linting" for s in result)
    assert any(s["id"] == "run-static-checks" for s in result)


def test_list_skills_raises_if_json_missing(tmp_path):
    missing = tmp_path / "skills.json"
    with pytest.raises(FileNotFoundError):
        list_skills_for_context(skills_json=missing)
