# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S101

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.generator.generate_skills import (
    SkillsGenerationError,
    build_skills_payload,
    generate_skills,
    validate_payload,
)
from agent_skills_poc.model.workflow import Workflow


def test_build_skills_payload_schema() -> None:
    workflows = [
        Workflow(
            id="run_tests",
            description="Run tests",
            local_command="uv run --project airflow-core pytest",
            fallback_command="breeze exec pytest",
        )
    ]

    payload = build_skills_payload(workflows)

    assert payload == {
        "skills": [
            {
                "id": "run_tests",
                "steps": [
                    {"type": "local", "command": "uv run --project airflow-core pytest"},
                    {"type": "fallback", "command": "breeze exec pytest"},
                ],
            }
        ]
    }


def test_validate_payload_rejects_invalid_shape() -> None:
    with pytest.raises(SkillsGenerationError):
        validate_payload({"skills": [{"id": "x", "steps": [{"type": "local", "command": "a"}]}]})


def test_generate_skills_writes_expected_json(tmp_path) -> None:
    rst_path = tmp_path / "CONTRIBUTING_POC.rst"
    out_path = tmp_path / "skills.json"
    rst_path.write_text(
        """
PoC
===

.. agent-skill::
   :id: run_tests
   :description: Run tests
   :local: uv run --project airflow-core pytest
   :fallback: breeze exec pytest
""",
        encoding="utf-8",
    )

    payload = generate_skills(rst_path, out_path)
    on_disk = json.loads(out_path.read_text(encoding="utf-8"))

    assert payload == on_disk
    assert on_disk["skills"][0]["id"] == "run_tests"
