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

"""
AI Skill definitions for Breeze contributor workflows.

This module provides structured skill definitions that AI coding assistants
can use to understand which commands should run on the host vs inside
the Breeze container, and how to interpret their outputs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


class ExecutionEnvironment(Enum):
    """Where a command should be executed."""

    HOST_ONLY = "host"
    CONTAINER_ONLY = "container"
    BOTH = "both"


class OutputFormat(Enum):
    """Expected output format for parsing."""

    PLAIN_TEXT = "plain_text"
    JSON = "json"
    PYTEST_OUTPUT = "pytest_output"
    PRECOMMIT_OUTPUT = "precommit_output"


@dataclass(frozen=True)
class AISkillDefinition:
    """
    Definition of a skill that AI assistants can use for Airflow contribution workflows.

    Attributes:
        name: Unique identifier for the skill
        command: The actual command to execute
        description: Human and AI readable description of what this skill does
        environment: Where this command should be executed
        output_format: Expected format of command output for parsing
        parameters: Optional parameters the command accepts
        success_indicators: Patterns indicating successful execution
        failure_indicators: Patterns indicating failed execution
        requires_files: Whether this command requires specific files as input
        typical_use_case: When an AI should consider using this skill
    """

    name: str
    command: str
    description: str
    environment: ExecutionEnvironment
    output_format: OutputFormat = OutputFormat.PLAIN_TEXT
    parameters: Sequence[str] = field(default_factory=tuple)
    success_indicators: Sequence[str] = field(default_factory=tuple)
    failure_indicators: Sequence[str] = field(default_factory=tuple)
    requires_files: bool = False
    typical_use_case: str = ""


# Core skills for Airflow contribution workflows
BREEZE_AI_SKILLS: dict[str, AISkillDefinition] = {
    "start_breeze_shell": AISkillDefinition(
        name="start_breeze_shell",
        command="breeze shell",
        description="Start an interactive Breeze shell inside the CI container",
        environment=ExecutionEnvironment.HOST_ONLY,
        typical_use_case="When you need to run tests or Airflow commands in the development environment",
        success_indicators=("root@", "airflow@"),
        failure_indicators=("docker: Error", "Cannot connect to the Docker daemon"),
    ),
    "run_static_checks": AISkillDefinition(
        name="run_static_checks",
        command="pre-commit run",
        description="Run pre-commit hooks for code style, linting, and basic validation",
        environment=ExecutionEnvironment.HOST_ONLY,
        output_format=OutputFormat.PRECOMMIT_OUTPUT,
        parameters=("--all-files", "--files", "--from-ref", "--to-ref"),
        requires_files=True,
        typical_use_case="Before committing changes to verify code style and catch common issues",
        success_indicators=("Passed", "passed"),
        failure_indicators=("Failed", "failed"),
    ),
    "run_unit_tests": AISkillDefinition(
        name="run_unit_tests",
        command="pytest",
        description="Run pytest unit tests for specific modules or test files",
        environment=ExecutionEnvironment.CONTAINER_ONLY,
        output_format=OutputFormat.PYTEST_OUTPUT,
        parameters=("-v", "-x", "--tb=short", "-k", "--collect-only"),
        requires_files=True,
        typical_use_case="To verify code changes don't break existing functionality",
        success_indicators=("passed", "PASSED"),
        failure_indicators=("FAILED", "ERROR", "failed"),
    ),
    "run_breeze_tests": AISkillDefinition(
        name="run_breeze_tests",
        command="breeze testing tests",
        description="Run tests through Breeze with proper environment setup",
        environment=ExecutionEnvironment.HOST_ONLY,
        output_format=OutputFormat.PYTEST_OUTPUT,
        parameters=("--test-type", "--parallel-test-types", "--db-reset"),
        requires_files=True,
        typical_use_case="To run tests with database backends and full CI environment",
        success_indicators=("passed", "PASSED"),
        failure_indicators=("FAILED", "ERROR"),
    ),
    "list_dags": AISkillDefinition(
        name="list_dags",
        command="airflow dags list",
        description="List all DAGs recognized by Airflow",
        environment=ExecutionEnvironment.CONTAINER_ONLY,
        output_format=OutputFormat.PLAIN_TEXT,
        typical_use_case="To verify DAG files are valid and loadable",
        success_indicators=("dag_id",),
        failure_indicators=("Error", "Exception", "ImportError"),
    ),
    "test_dag_task": AISkillDefinition(
        name="test_dag_task",
        command="airflow tasks test",
        description="Test a specific task in a DAG without affecting the database",
        environment=ExecutionEnvironment.CONTAINER_ONLY,
        parameters=("dag_id", "task_id", "execution_date"),
        typical_use_case="To debug or verify a specific task works correctly",
        success_indicators=("INFO", "SUCCESS"),
        failure_indicators=("ERROR", "FAILED", "Exception"),
    ),
    "git_add_files": AISkillDefinition(
        name="git_add_files",
        command="git add",
        description="Stage files for commit",
        environment=ExecutionEnvironment.HOST_ONLY,
        requires_files=True,
        typical_use_case="Before running pre-commit checks on staged changes",
    ),
    "git_diff": AISkillDefinition(
        name="git_diff",
        command="git diff",
        description="Show changes between commits, working tree, etc",
        environment=ExecutionEnvironment.HOST_ONLY,
        parameters=("--staged", "--name-only", "HEAD"),
        typical_use_case="To review what changes have been made before committing",
    ),
    "build_docs": AISkillDefinition(
        name="build_docs",
        command="breeze build-docs",
        description="Build Airflow documentation",
        environment=ExecutionEnvironment.HOST_ONLY,
        parameters=("--docs-only", "--spellcheck-only", "--package-filter"),
        typical_use_case="When modifying documentation to verify it builds correctly",
        success_indicators=("build succeeded",),
        failure_indicators=("ERROR", "FAILED", "Warning"),
    ),
}


def get_skill(name: str) -> AISkillDefinition | None:
    """
    Get a skill definition by name.

    Args:
        name: The skill name to look up

    Returns:
        The skill definition if found, None otherwise
    """
    return BREEZE_AI_SKILLS.get(name)


def get_skills_for_environment(env: ExecutionEnvironment) -> list[AISkillDefinition]:
    """
    Get all skills that can run in a specific environment.

    Args:
        env: The execution environment to filter by

    Returns:
        List of skills available in that environment
    """
    return [
        skill
        for skill in BREEZE_AI_SKILLS.values()
        if skill.environment == env or skill.environment == ExecutionEnvironment.BOTH
    ]


def get_host_only_skills() -> list[AISkillDefinition]:
    """Get skills that must run on the host machine."""
    return get_skills_for_environment(ExecutionEnvironment.HOST_ONLY)


def get_container_only_skills() -> list[AISkillDefinition]:
    """Get skills that must run inside the Breeze container."""
    return get_skills_for_environment(ExecutionEnvironment.CONTAINER_ONLY)


def export_skills_as_dict() -> dict:
    """
    Export all skills as a dictionary for JSON serialization.

    This can be used to generate skill definition files for AI agents.

    Returns:
        Dictionary representation of all skills
    """
    return {
        name: {
            "name": skill.name,
            "command": skill.command,
            "description": skill.description,
            "environment": skill.environment.value,
            "output_format": skill.output_format.value,
            "parameters": list(skill.parameters),
            "success_indicators": list(skill.success_indicators),
            "failure_indicators": list(skill.failure_indicators),
            "requires_files": skill.requires_files,
            "typical_use_case": skill.typical_use_case,
        }
        for name, skill in BREEZE_AI_SKILLS.items()
    }
