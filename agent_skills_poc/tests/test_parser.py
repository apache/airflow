# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S101

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.parser.parser import WorkflowParseError, parse_workflows, parse_workflows_from_text


def test_parse_single_workflow_block() -> None:
    rst = """
Title
=====

.. agent-skill::
   :id: run_tests
   :description: Run tests locally first and fallback to Breeze
   :local: uv run --project distribution_folder pytest
   :fallback: breeze exec pytest
"""

    workflows = parse_workflows_from_text(rst)

    assert len(workflows) == 1
    assert workflows[0].id == "run_tests"
    assert workflows[0].description == "Run tests locally first and fallback to Breeze"
    assert workflows[0].local_command == "uv run --project distribution_folder pytest"
    assert workflows[0].fallback_command == "breeze exec pytest"


def test_parse_multiple_workflow_blocks() -> None:
    rst = """
Title
=====

.. agent-skill::
   :id: run_tests
   :description: Run tests
   :local: uv run --project airflow-core pytest
   :fallback: breeze exec pytest

.. agent-skill::
   :id: run_static
   :description: Run static checks
   :local: uv run --project airflow-core ruff check .
   :fallback: breeze exec prek run --stage pre-commit
"""

    workflows = parse_workflows_from_text(rst)

    assert [workflow.id for workflow in workflows] == ["run_tests", "run_static"]


def test_parse_invalid_workflow_missing_required_option() -> None:
    rst = """
Title
=====

.. agent-skill::
   :id: bad_block
   :description: Missing local option
   :fallback: breeze exec pytest
"""

    with pytest.raises(WorkflowParseError):
        parse_workflows_from_text(rst)


def test_parse_invalid_workflow_bad_id() -> None:
    rst = """
Title
=====

.. agent-skill::
   :id: invalid id
   :description: Bad id format
   :local: uv run --project airflow-core pytest
   :fallback: breeze exec pytest
"""

    with pytest.raises(WorkflowParseError):
        parse_workflows_from_text(rst)


def test_parse_workflows_from_file(tmp_path) -> None:
    rst_path = tmp_path / "CONTRIBUTING_POC.rst"
    rst_path.write_text(
        """
PoC
===

.. agent-skill::
   :id: run_debug
   :description: Debug environment
   :local: uv run --project airflow-core pytest -k debug
   :fallback: breeze exec pytest -k debug
""",
        encoding="utf-8",
    )

    workflows = parse_workflows(rst_path)

    assert len(workflows) == 1
    assert workflows[0].id == "run_debug"
