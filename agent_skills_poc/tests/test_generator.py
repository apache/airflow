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

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from agent_skills_poc.generator import SkillGenerationError, generate_agent_skills, validate_skill_markdown
from agent_skills_poc.model import Workflow


def test_generate_skill_markdown_file(tmp_path) -> None:
    workflow = Workflow(
        id="run-tests",
        description="Run Airflow tests using correct environment host or Breeze",
        local="uv run --project airflow-core pytest airflow-core/tests/cli/test_cli_parser.py -xvs",
        fallback="breeze run pytest airflow-core/tests/cli/test_cli_parser.py -xvs",
    )

    generated = generate_agent_skills([workflow], output_dir=tmp_path / "skills")

    assert len(generated) == 1
    content = generated[0].read_text(encoding="utf-8")
    assert content.startswith("---\n")
    assert "name: run-tests" in content
    assert "description: Run Airflow tests using correct environment host or Breeze" in content
    assert "## Instructions" in content
    assert "### Local" in content
    assert "### Breeze" in content

    assert generated[0] == tmp_path / "skills" / "run-tests" / "SKILL.md"


def test_validate_skill_markdown_rejects_missing_frontmatter() -> None:
    invalid = "# Run Tests\n\nNo frontmatter"
    with pytest.raises(SkillGenerationError):
        validate_skill_markdown(invalid)


def test_generated_skill_path_structure(tmp_path) -> None:
    workflows = [
        Workflow(
            id="run-tests",
            description="Run tests",
            local="uv run --project airflow-core pytest",
            fallback="breeze run pytest airflow-core/tests -xvs",
        )
    ]
    generated = generate_agent_skills(workflows, output_dir=tmp_path / "skills")
    assert generated[0] == tmp_path / "skills" / "run-tests" / "SKILL.md"
