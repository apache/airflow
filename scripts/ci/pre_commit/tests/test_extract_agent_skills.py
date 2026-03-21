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

"""Tests for agent skill extraction."""

from __future__ import annotations

from scripts.ci.agent_skills.extract_agent_skills import (
    extract_skills_from_text,
    parse_skill_block,
)


def test_parse_skill_block_success():
    block = """
    id: run_targeted_tests
    title: Run targeted tests
    preferred_context: host
    allowed_contexts: host,breeze
    local_command: uv run --project {distribution_folder} pytest {test_path}
    breeze_command: breeze exec pytest {test_path} -xvs
    inside_breeze_command: pytest {test_path} -xvs
    fallback_when: missing_system_dependencies|local_environment_mismatch|ci_local_discrepancy
    """

    parsed = parse_skill_block(block)

    assert parsed["id"] == "run_targeted_tests"
    assert parsed["preferred_context"] == "host"
    assert parsed["allowed_contexts"] == ["host", "breeze"]
    assert parsed["fallback_when"] == [
        "missing_system_dependencies",
        "local_environment_mismatch",
        "ci_local_discrepancy",
    ]


def test_extract_skills_from_text_success():
    text = """
    # PoC Agent Skills

    <!-- agent-skill:start -->
    id: run_targeted_tests
    title: Run targeted tests
    preferred_context: host
    allowed_contexts: host,breeze
    local_command: uv run --project {distribution_folder} pytest {test_path}
    breeze_command: breeze exec pytest {test_path} -xvs
    inside_breeze_command: pytest {test_path} -xvs
    fallback_when: missing_system_dependencies|local_environment_mismatch|ci_local_discrepancy
    <!-- agent-skill:end -->
    """

    skills = extract_skills_from_text(text)

    assert len(skills) == 1
    assert skills[0]["id"] == "run_targeted_tests"


def test_extract_skills_from_text_missing_end_marker_raises():
    text = """
    <!-- agent-skill:start -->
    id: run_targeted_tests
    title: Run targeted tests
    """

    try:
        extract_skills_from_text(text)
        assert False, "Expected ValueError for missing end marker"
    except ValueError as error:
        assert "matching end marker" in str(error)


def test_parse_skill_block_missing_required_keys_raises():
    block = """
    id: run_targeted_tests
    title: Run targeted tests
    """

    try:
        parse_skill_block(block)
        assert False, "Expected ValueError for missing required keys"
    except ValueError as error:
        assert "Missing required keys" in str(error)
