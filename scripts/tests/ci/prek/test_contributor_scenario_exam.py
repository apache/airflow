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
"""Contributor scenario exam.

These tests verify that an AI agent using the skill system gets the *correct*
command for each realistic contributor situation — using the real contributing-docs
source files, not synthetic fixtures.

Each test is named after the scenario a contributor (or agent) faces. The test
documents what the agent would do WITHOUT skills (the wrong command) and confirms
that WITH skills it gets the correct command.

This is the evaluation harness described in the GSoC project goals:
  "Design a testable user scenario or 'exam' that simulates a typical contribution
   workflow to verify that the added skills work as intended."
"""

from __future__ import annotations

import ci.prek.context_detect as cd
from ci.prek.context_detect import get_command, list_skills_for_context

# ---------------------------------------------------------------------------
# Scenario 1: Running a non-DB test on the host
# ---------------------------------------------------------------------------


def test_scenario_host_runs_single_test_with_uv(monkeypatch):
    """Agent fixed a bug in providers/amazon and wants to run the test.

    WITHOUT skills: agent might run `pytest providers/amazon/tests/... -xvs`
                    directly on host — fails if MySQL libs are missing, or
                    resolves the wrong virtualenv.
    WITH skills:    agent calls run-single-test → gets uv command scoped to
                    the correct project, which handles the monorepo venv correctly.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    cmd = get_command(
        "run-single-test",
        project="providers/amazon",
        test_path="providers/amazon/tests/unit/hooks/test_s3_hook.py",
    )

    # Must use uv scoped to the provider — not bare pytest
    assert cmd.startswith("uv run --project providers/amazon"), (
        f"Expected uv-scoped command on host, got: {cmd}"
    )
    assert "providers/amazon/tests/unit/hooks/test_s3_hook.py" in cmd
    assert "-xvs" in cmd


# ---------------------------------------------------------------------------
# Scenario 2: System dependencies missing — fallback to breeze
# ---------------------------------------------------------------------------


def test_scenario_host_falls_back_to_breeze_when_system_deps_missing(monkeypatch):
    """Agent tries uv but MySQL native libs are missing (e.g. on a fresh macOS).

    WITHOUT skills: agent has no way to know there is a fallback — it gives up
                    or runs breeze incorrectly (breeze shell instead of breeze run).
    WITH skills:    agent passes system_deps_available=false → gets the breeze
                    fallback command automatically.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    cmd = get_command(
        "run-single-test",
        project="providers/amazon",
        test_path="providers/amazon/tests/unit/hooks/test_s3_hook.py",
        system_deps_available="false",
    )

    assert cmd.startswith("breeze run pytest"), (
        f"Expected breeze fallback on host with missing deps, got: {cmd}"
    )
    assert "providers/amazon/tests/unit/hooks/test_s3_hook.py" in cmd


# ---------------------------------------------------------------------------
# Scenario 3: DB test — must never use uv
# ---------------------------------------------------------------------------


def test_scenario_db_test_always_uses_breeze_on_host(monkeypatch):
    """Agent sees @pytest.mark.db_test and chooses the correct runner.

    WITHOUT skills: agent might try uv — cannot provision a live database,
                    test fails with connection errors.
    WITH skills:    agent uses run-db-test → always gets breeze, even on host.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    cmd = get_command(
        "run-db-test",
        test_path="airflow-core/tests/unit/models/test_dag.py",
    )

    assert "breeze run pytest" in cmd, f"DB tests must always use breeze on host, got: {cmd}"
    assert "uv run" not in cmd, "uv must never be used for DB tests"
    assert "airflow-core/tests/unit/models/test_dag.py" in cmd


# ---------------------------------------------------------------------------
# Scenario 4: Agent is already inside Breeze — runs pytest directly
# ---------------------------------------------------------------------------


def test_scenario_inside_breeze_runs_pytest_directly(monkeypatch):
    """Agent is already inside the Breeze container and runs a test.

    WITHOUT skills: agent might prepend 'breeze run' unnecessarily, causing
                    a nested breeze invocation which hangs or errors.
    WITH skills:    agent detects breeze context → gets bare `pytest` command.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "breeze")

    cmd = get_command(
        "run-single-test",
        test_path="airflow-core/tests/cli/test_cli_parser.py",
    )

    assert cmd.startswith("pytest"), f"Inside Breeze, pytest should run directly without prefix, got: {cmd}"
    assert "breeze run" not in cmd, "Must not nest breeze inside breeze"
    assert "uv run" not in cmd, "uv is not used inside Breeze"


# ---------------------------------------------------------------------------
# Scenario 5: Trying to start Breeze from inside Breeze
# ---------------------------------------------------------------------------


def test_scenario_setup_breeze_inside_breeze_returns_guidance(monkeypatch):
    """Agent tries to run setup-breeze-environment while already inside Breeze.

    WITHOUT skills: agent runs `breeze start-airflow` inside container — hangs
                    or errors with nested docker socket issues.
    WITH skills:    agent gets a guidance message instead of an executable command,
                    preventing the incorrect action.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "breeze")

    result = get_command("setup-breeze-environment")

    # Must return guidance, not a dangerous command
    assert "breeze start-airflow" not in result, "Must not return breeze start-airflow inside a container"
    # The result should explain the current context
    assert len(result) > 0


# ---------------------------------------------------------------------------
# Scenario 6: Static checks before committing
# ---------------------------------------------------------------------------


def test_scenario_static_checks_before_commit_on_host(monkeypatch):
    """Agent wants to run static checks before committing on the host.

    WITHOUT skills: agent might run `pre-commit run --all-files` (wrong tool)
                    or `prek --all-files` (wrong flag — should use --from-ref).
    WITH skills:    agent gets exactly the right prek invocation scoped to the
                    target branch.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    cmd = get_command("run-static-checks", target_branch="main")

    assert "prek run" in cmd
    assert "--from-ref main" in cmd
    assert "--stage pre-commit" in cmd


# ---------------------------------------------------------------------------
# Scenario 7: Format a file after editing
# ---------------------------------------------------------------------------


def test_scenario_format_and_lint_after_editing_python_file(monkeypatch):
    """Agent edited a Python file and needs to format+lint it immediately.

    WITHOUT skills: agent runs `ruff format file.py` without --project scoping —
                    resolves wrong venv in the monorepo, may miss provider deps.
    WITH skills:    agent gets both ruff format and ruff check --fix scoped to
                    the correct project.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    cmd = get_command(
        "format-and-lint",
        project="providers/amazon",
        file_path="providers/amazon/hooks/s3.py",
    )

    assert "ruff format" in cmd
    assert "ruff check --fix" in cmd
    assert "--project providers/amazon" in cmd
    assert "providers/amazon/hooks/s3.py" in cmd


# ---------------------------------------------------------------------------
# Scenario 8: Skill discovery — agent lists available skills
# ---------------------------------------------------------------------------


def test_scenario_agent_discovers_available_skills_on_host(monkeypatch):
    """Agent queries available skills to understand what contributor workflows exist.

    This verifies the skill registry is populated from the real contributing docs
    and covers all expected contributor workflow categories.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    skills = list_skills_for_context()
    skill_ids = {s["id"] for s in skills}

    expected = {
        "setup-breeze-environment",
        "run-static-checks",
        "run-manual-checks",
        "run-single-test",
        "run-db-test",
        "format-and-lint",
        "build-docs",
    }
    missing = expected - skill_ids
    assert not missing, f"Skills missing from registry: {missing}"


# ---------------------------------------------------------------------------
# Scenario 9: Full contributor workflow — stage, check, test
# ---------------------------------------------------------------------------


def test_scenario_full_contribution_workflow_command_sequence(monkeypatch):
    """Agent walks through a complete contribution: static checks → test → manual checks.

    Verifies the skill prereq chain produces commands in the right order
    and that each step's command is correct for the host context.
    """
    monkeypatch.setattr(cd, "get_context", lambda: "host")

    # Step 1: run static checks before committing
    check_cmd = get_command("run-static-checks", target_branch="main")
    assert "prek run" in check_cmd and "--stage pre-commit" in check_cmd

    # Step 2: run the targeted test
    test_cmd = get_command(
        "run-single-test",
        project="airflow-core",
        test_path="airflow-core/tests/cli/test_cli_parser.py",
    )
    assert "uv run --project airflow-core" in test_cmd

    # Step 3: run manual checks before opening PR
    manual_cmd = get_command("run-manual-checks", target_branch="main")
    assert "prek run" in manual_cmd and "--stage manual" in manual_cmd

    # The three commands must be distinct — each step does something different
    assert check_cmd != test_cmd != manual_cmd


# ---------------------------------------------------------------------------
# Scenario 10: Skills sourced from real contributing docs (integration)
# ---------------------------------------------------------------------------


def test_scenario_skills_are_sourced_from_real_contributing_docs():
    """Verify the skill system reads from the actual contributing-docs files.

    This is the end-to-end integration check: the contributing docs ARE the
    source of truth. If a skill is missing here, it means the contributing
    doc was not updated — not a separate skill file.
    """
    from ci.prek.context_detect import AGENT_SKILLS_RST_FILES, _parse_skills_from_files

    # All source files must exist
    missing_files = [p for p in AGENT_SKILLS_RST_FILES if not p.exists()]
    assert not missing_files, f"Source contributing-docs files missing: {missing_files}"

    # Skills must be parseable from the real files
    skills = _parse_skills_from_files()
    assert len(skills) >= 7, f"Expected at least 7 skills, found {len(skills)}"

    # Every skill must have at least one executable step
    for skill in skills:
        assert skill["steps"], f"Skill '{skill['id']}' has no executable steps"
