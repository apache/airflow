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

# SPDX-License-Identifier: Apache-2.0
# ruff: noqa: S101

from __future__ import annotations

import sys
from pathlib import Path
from textwrap import dedent

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.parser import WorkflowParseError, parse_workflows, parse_workflows_from_text


def test_parse_single_workflow_block() -> None:
    rst = dedent(
        """
        Title
        =====

        .. agent-skill::
           :id: run-tests
           :description: Run tests locally first and fallback to Breeze
           :local: uv run --project distribution_folder pytest
           :fallback: breeze run pytest distribution_folder/tests -xvs
        """
    )

    workflows = parse_workflows_from_text(rst)

    assert len(workflows) == 1
    assert workflows[0].id == "run-tests"
    assert workflows[0].description == "Run tests locally first and fallback to Breeze"
    assert workflows[0].local == "uv run --project distribution_folder pytest"
    assert workflows[0].fallback == "breeze run pytest distribution_folder/tests -xvs"


def test_parse_multiple_workflow_blocks() -> None:
    rst = dedent(
        """
        Title
        =====

        .. agent-skill::
           :id: run-tests
           :description: Run tests
           :local: uv run --project airflow-core pytest
           :fallback: breeze run pytest airflow-core/tests -xvs

        .. agent-skill::
           :id: run-static
           :description: Run static checks
           :local: uv run --project airflow-core ruff check .
           :fallback: breeze run prek run --stage pre-commit
        """
    )

    workflows = parse_workflows_from_text(rst)

    assert [workflow.id for workflow in workflows] == ["run-tests", "run-static"]


def test_parse_invalid_workflow_missing_required_option() -> None:
    rst = dedent(
        """
        Title
        =====

        .. agent-skill::
           :id: bad-block
           :description: Missing local option
           :fallback: breeze run pytest airflow-core/tests -xvs
        """
    )

    with pytest.raises(WorkflowParseError):
        parse_workflows_from_text(rst)


def test_parse_invalid_workflow_bad_id() -> None:
    rst = dedent(
        """
        Title
        =====

        .. agent-skill::
           :id: invalid id
           :description: Bad id format
           :local: uv run --project airflow-core pytest
           :fallback: breeze run pytest airflow-core/tests -xvs
        """
    )

    with pytest.raises(WorkflowParseError):
        parse_workflows_from_text(rst)


def test_parse_workflows_from_file(tmp_path) -> None:
    rst_path = tmp_path / "CONTRIBUTING_POC.rst"
    rst_path.write_text(
        dedent(
            """
            PoC
            ===

            .. agent-skill::
               :id: run-debug
               :description: Debug environment
               :local: uv run --project airflow-core pytest -k debug
               :fallback: breeze run pytest airflow-core/tests -k debug -xvs
            """
        ),
        encoding="utf-8",
    )

    workflows = parse_workflows(rst_path)

    assert len(workflows) == 1
    assert workflows[0].id == "run-debug"
