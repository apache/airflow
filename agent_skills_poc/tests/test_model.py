# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S101

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.model import Workflow, WorkflowValidationError


def test_valid_workflow_model() -> None:
    workflow = Workflow(
        id="run-tests",
        description="Run Airflow tests correctly",
        local="uv run --project airflow-core pytest airflow-core/tests/cli/test_cli_parser.py -xvs",
        fallback="breeze run pytest airflow-core/tests/cli/test_cli_parser.py -xvs",
    )
    assert workflow.id == "run-tests"


def test_invalid_id_rejected() -> None:
    with pytest.raises(WorkflowValidationError):
        Workflow(
            id="Run Tests",
            description="Invalid id",
            local="uv run --project airflow-core pytest",
            fallback="breeze run pytest airflow-core/tests -xvs",
        )


def test_missing_fields_rejected() -> None:
    with pytest.raises(WorkflowValidationError):
        Workflow(id="run-tests", description=" ", local="a", fallback="b")
    with pytest.raises(WorkflowValidationError):
        Workflow(id="run-tests", description="desc", local=" ", fallback="b")
    with pytest.raises(WorkflowValidationError):
        Workflow(id="run-tests", description="desc", local="a", fallback=" ")
