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

import pytest

from airflow_breeze.utils.ai_skill_definitions import (
    BREEZE_AI_SKILLS,
    ExecutionEnvironment,
    OutputFormat,
    export_skills_as_dict,
    get_container_only_skills,
    get_host_only_skills,
    get_skill,
)


class TestAISkillDefinitions:
    """Tests for AI skill definitions module."""

    def test_all_skills_have_required_fields(self):
        """Verify all skills have name, command, description, and environment."""
        for name, skill in BREEZE_AI_SKILLS.items():
            assert skill.name, f"Skill {name} missing name"
            assert skill.command, f"Skill {name} missing command"
            assert skill.description, f"Skill {name} missing description"
            assert skill.environment is not None, f"Skill {name} missing environment"

    def test_skill_names_match_keys(self):
        """Verify skill dictionary keys match the skill name attribute."""
        for key, skill in BREEZE_AI_SKILLS.items():
            assert key == skill.name, f"Key {key} doesn't match skill name {skill.name}"

    def test_get_skill_returns_correct_skill(self):
        """Test get_skill retrieves the correct skill."""
        skill = get_skill("run_static_checks")
        assert skill is not None
        assert skill.name == "run_static_checks"
        assert "pre-commit" in skill.command

    def test_get_skill_returns_none_for_unknown(self):
        """Test get_skill returns None for unknown skills."""
        assert get_skill("nonexistent_skill") is None

    def test_get_host_only_skills(self):
        """Test filtering for host-only skills."""
        host_skills = get_host_only_skills()
        assert len(host_skills) > 0
        for skill in host_skills:
            assert skill.environment in (ExecutionEnvironment.HOST_ONLY, ExecutionEnvironment.BOTH)

    def test_get_container_only_skills(self):
        """Test filtering for container-only skills."""
        container_skills = get_container_only_skills()
        assert len(container_skills) > 0
        for skill in container_skills:
            assert skill.environment in (ExecutionEnvironment.CONTAINER_ONLY, ExecutionEnvironment.BOTH)

    def test_breeze_shell_is_host_only(self):
        """Verify breeze shell is correctly marked as host-only."""
        skill = get_skill("start_breeze_shell")
        assert skill is not None
        assert skill.environment == ExecutionEnvironment.HOST_ONLY

    def test_pytest_is_container_only(self):
        """Verify pytest is correctly marked as container-only."""
        skill = get_skill("run_unit_tests")
        assert skill is not None
        assert skill.environment == ExecutionEnvironment.CONTAINER_ONLY

    def test_export_skills_as_dict(self):
        """Test exporting skills to dictionary format."""
        exported = export_skills_as_dict()
        assert isinstance(exported, dict)
        assert len(exported) == len(BREEZE_AI_SKILLS)

        # Verify structure of exported skill
        run_tests = exported.get("run_unit_tests")
        assert run_tests is not None
        assert run_tests["environment"] == "container"
        assert run_tests["output_format"] == "pytest_output"
        assert isinstance(run_tests["parameters"], list)

    def test_skills_have_valid_output_formats(self):
        """Verify all skills have valid output format values."""
        valid_formats = {fmt for fmt in OutputFormat}
        for skill in BREEZE_AI_SKILLS.values():
            assert skill.output_format in valid_formats

    def test_static_checks_skill_configuration(self):
        """Verify static checks skill is properly configured for pre-commit."""
        skill = get_skill("run_static_checks")
        assert skill is not None
        assert skill.output_format == OutputFormat.PRECOMMIT_OUTPUT
        assert skill.requires_files is True
        assert "--files" in skill.parameters or "--all-files" in skill.parameters

    @pytest.mark.parametrize(
        ("skill_name", "expected_env"),
        [
            ("start_breeze_shell", ExecutionEnvironment.HOST_ONLY),
            ("run_static_checks", ExecutionEnvironment.HOST_ONLY),
            ("run_unit_tests", ExecutionEnvironment.CONTAINER_ONLY),
            ("list_dags", ExecutionEnvironment.CONTAINER_ONLY),
            ("git_add_files", ExecutionEnvironment.HOST_ONLY),
        ],
    )
    def test_skill_environments(self, skill_name: str, expected_env: ExecutionEnvironment):
        """Verify specific skills have correct environments."""
        skill = get_skill(skill_name)
        assert skill is not None, f"Skill {skill_name} not found"
        assert skill.environment == expected_env
