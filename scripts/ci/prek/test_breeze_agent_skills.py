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

# ruff: noqa: S101

"""
Tests for Breeze agent skill extraction and context detection.

Run with:
    python3 -m pytest scripts/ci/prek/test_breeze_agent_skills.py -v
"""

import json
import os
import sys
from pathlib import Path
from unittest import mock

import pytest

# Make scripts importable when running from repo root
sys.path.insert(0, str(Path(__file__).parent))

from breeze_context_detect import get_command, is_inside_breeze
from extract_agent_skills import build_skills_json, check_drift, extract_skills, parse_marker

# ── parse_marker tests ────────────────────────────────────────────────────────


class TestParseMarker:
    def test_parses_quoted_values(self):
        line = '<!-- agent-skill-sync: workflow=run-tests host="uv run pytest" breeze="pytest" -->'
        result = parse_marker(line)
        assert result is not None
        assert result["workflow"] == "run-tests"
        assert result["host"] == "uv run pytest"
        assert result["breeze"] == "pytest"

    def test_parses_unquoted_values(self):
        line = "<!-- agent-skill-sync: workflow=static-checks host=prek breeze=prek -->"
        result = parse_marker(line)
        assert result is not None
        assert result["workflow"] == "static-checks"
        assert result["host"] == "prek"
        assert result["breeze"] == "prek"

    def test_returns_none_for_non_marker_line(self):
        line = "This is just a regular markdown line"
        assert parse_marker(line) is None

    def test_returns_none_for_marker_without_workflow(self):
        line = "<!-- agent-skill-sync: host=prek breeze=prek -->"
        assert parse_marker(line) is None

    def test_parses_fallback_condition(self):
        line = '<!-- agent-skill-sync: workflow=run-tests host="uv run pytest" breeze="pytest" fallback=missing_system_deps -->'
        result = parse_marker(line)
        assert result is not None
        assert result["fallback"] == "missing_system_deps"


# ── extract_skills tests ──────────────────────────────────────────────────────


class TestExtractSkills:
    def test_extracts_all_markers_from_file(self, tmp_path):
        skill_md = tmp_path / "SKILL.md"
        skill_md.write_text(
            """
# Test Skill

Some prose here.

<!-- agent-skill-sync: workflow=static-checks host=prek breeze=prek -->

More prose.

<!-- agent-skill-sync: workflow=run-tests host="uv run pytest {path}" breeze="pytest {path}" fallback=missing_system_deps -->

Even more prose.

<!-- agent-skill-sync: workflow=system-verify host="breeze start-airflow" breeze=N/A -->
""",
            encoding="utf-8",
        )
        skills = extract_skills(skill_md)
        assert len(skills) == 3
        assert skills[0]["workflow"] == "static-checks"
        assert skills[1]["workflow"] == "run-tests"
        assert skills[2]["workflow"] == "system-verify"

    def test_returns_empty_list_when_no_markers(self, tmp_path):
        skill_md = tmp_path / "SKILL.md"
        skill_md.write_text("# No markers here\n\nJust prose.", encoding="utf-8")
        skills = extract_skills(skill_md)
        assert skills == []

    def test_exits_when_file_not_found(self):
        with pytest.raises(SystemExit):
            extract_skills(Path("/nonexistent/SKILL.md"))


# ── build_skills_json tests ───────────────────────────────────────────────────


class TestBuildSkillsJson:
    def test_builds_correct_structure(self):
        skills = [
            {"workflow": "static-checks", "host": "prek", "breeze": "prek"},
            {
                "workflow": "run-tests",
                "host": "uv run pytest",
                "breeze": "pytest",
                "fallback": "missing_system_deps",
            },
        ]
        result = build_skills_json(skills)
        assert "$schema" in result
        assert result["$schema"] == "breeze-agent-skills/v1"
        assert len(result["skills"]) == 2
        assert result["skills"][0]["workflow"] == "static-checks"
        assert result["skills"][1]["fallback_condition"] == "missing_system_deps"

    def test_default_fallback_condition_is_never(self):
        skills = [{"workflow": "static-checks", "host": "prek", "breeze": "prek"}]
        result = build_skills_json(skills)
        assert result["skills"][0]["fallback_condition"] == "never"


# ── check_drift tests ─────────────────────────────────────────────────────────


class TestCheckDrift:
    def test_no_drift_when_files_match(self, tmp_path):
        skills = [{"workflow": "static-checks", "host": "prek", "breeze": "prek"}]
        generated = build_skills_json(skills)
        existing = tmp_path / "skills.json"
        existing.write_text(json.dumps(generated, indent=2) + "\n", encoding="utf-8")
        assert check_drift(generated, existing) is False

    def test_drift_detected_when_skills_differ(self, tmp_path):
        skills_old = [{"workflow": "static-checks", "host": "prek", "breeze": "prek"}]
        skills_new = [
            {"workflow": "static-checks", "host": "prek", "breeze": "prek"},
            {"workflow": "run-tests", "host": "uv run pytest", "breeze": "pytest"},
        ]
        generated = build_skills_json(skills_new)
        existing = tmp_path / "skills.json"
        existing.write_text(json.dumps(build_skills_json(skills_old), indent=2) + "\n", encoding="utf-8")
        assert check_drift(generated, existing) is True

    def test_drift_detected_when_file_missing(self, tmp_path):
        skills = [{"workflow": "static-checks", "host": "prek", "breeze": "prek"}]
        generated = build_skills_json(skills)
        missing_path = tmp_path / "skills.json"
        assert check_drift(generated, missing_path) is True


# ── context detection tests ───────────────────────────────────────────────────


class TestContextDetection:
    def test_detects_breeze_via_env_var(self):
        with mock.patch.dict(os.environ, {"AIRFLOW_BREEZE_CONTAINER": "true"}):
            assert is_inside_breeze() is True

    def test_detects_host_when_env_var_not_set(self):
        env = {k: v for k, v in os.environ.items() if k != "AIRFLOW_BREEZE_CONTAINER"}
        with (
            mock.patch.dict(os.environ, env, clear=True),
            mock.patch("breeze_context_detect.Path") as mock_path_cls,
        ):
            mock_path_cls.return_value.exists.return_value = False
            assert is_inside_breeze() is False

    def test_get_command_returns_host_command_on_host(self):
        with mock.patch("breeze_context_detect.is_inside_breeze", return_value=False):
            result = get_command("static-checks")
            assert result["context"] == "host"
            assert result["command"] == "prek"

    def test_get_command_returns_breeze_command_inside_breeze(self):
        with mock.patch("breeze_context_detect.is_inside_breeze", return_value=True):
            result = get_command("static-checks")
            assert result["context"] == "breeze"
            assert result["command"] == "prek"

    def test_get_command_raises_for_unknown_workflow(self):
        with pytest.raises(ValueError, match="Unknown workflow"):
            get_command("nonexistent-workflow")

    def test_get_command_run_tests_host_uses_uv(self):
        with mock.patch("breeze_context_detect.is_inside_breeze", return_value=False):
            result = get_command(
                "run-tests",
                test_path="tests/unit/test_foo.py",
                distribution_folder="airflow-core",
            )
            assert "uv run" in result["command"]
            assert "airflow-core" in result["command"]
            assert "tests/unit/test_foo.py" in result["command"]

    def test_get_command_run_tests_breeze_uses_pytest_directly(self):
        with mock.patch("breeze_context_detect.is_inside_breeze", return_value=True):
            result = get_command("run-tests", test_path="tests/unit/test_foo.py")
            assert result["command"].startswith("pytest")
            assert "uv" not in result["command"]


class TestRSTExtraction:
    def test_extracts_skill_from_rst(self, tmp_path):
        rst = tmp_path / "test.rst"
        rst.write_text(
            "Some text\n\n"
            ".. agent-skill::\n"
            "   :id: run-tests\n"
            "   :context: host\n"
            "   :local: uv run pytest\n"
            "   :breeze: pytest\n\n"
            "More text\n"
        )
        from ci.prek.extract_agent_skills import extract_skills_from_rst

        skills = extract_skills_from_rst(rst)
        assert len(skills) == 1
        assert skills[0]["workflow"] == "run-tests"

    def test_returns_empty_for_rst_without_skills(self, tmp_path):
        rst = tmp_path / "test.rst"
        rst.write_text("Just regular RST content\n")
        from ci.prek.extract_agent_skills import extract_skills_from_rst

        skills = extract_skills_from_rst(rst)
        assert skills == []

    def test_returns_empty_when_rst_not_found(self):
        from pathlib import Path

        from ci.prek.extract_agent_skills import extract_skills_from_rst

        skills = extract_skills_from_rst(Path("nonexistent.rst"))
        assert skills == []


class TestE2EPipeline:
    def test_full_pipeline_rst_to_command(self, tmp_path):
        """
        E2E: RST skill block -> extraction -> skills.json -> get_command()
        Simulates a contributor adding a skill to contributing docs.
        """
        import json
        import os

        from ci.prek.breeze_context_detect import get_command
        from ci.prek.extract_agent_skills import (
            build_skills_json,
            extract_skills_from_rst,
            write_skills_json,
        )

        # Step 1: Contributor adds skill block to RST doc
        rst_file = tmp_path / "03_contributors_quick_start.rst"
        rst_file.write_text(
            "Running tests\n\n"
            ".. agent-skill::\n"
            "   :id: run-tests\n"
            "   :context: host\n"
            "   :local: uv run --project {distribution_folder} pytest {test_path} -xvs\n"
            "   :breeze: pytest {test_path} -xvs\n"
            "   :prereqs: static-checks\n\n"
            "More content here.\n"
        )

        # Step 2: Extractor parses RST
        skills = extract_skills_from_rst(rst_file)
        assert len(skills) == 1
        assert skills[0]["workflow"] == "run-tests"

        # Step 3: skills.json generated
        output_json = tmp_path / "skills.json"
        data = build_skills_json(skills)
        write_skills_json(data, output_json)
        assert output_json.exists()

        parsed = json.loads(output_json.read_text())
        assert len(parsed["skills"]) == 1
        assert parsed["skills"][0]["workflow"] == "run-tests"

        # Step 4: Agent calls get_command() on HOST
        result = get_command("run-tests")
        assert result["context"] == "host"
        assert "uv run" in result["command"]

        # Step 5: Agent calls get_command() inside Breeze
        os.environ["AIRFLOW_BREEZE_CONTAINER"] = "true"
        try:
            result_breeze = get_command("run-tests")
            assert result_breeze["context"] == "breeze"
            assert "pytest" in result_breeze["command"]
            assert "uv" not in result_breeze["command"]
        finally:
            del os.environ["AIRFLOW_BREEZE_CONTAINER"]
