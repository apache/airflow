#!/usr/bin/env python3
#
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

"""Additional edge-case tests for Breeze context and skill drift detection."""

from __future__ import annotations

import json
import os
from pathlib import Path
from unittest.mock import patch

from scripts.ci.prek.breeze_context import BreezieContext
from scripts.ci.prek.extract_breeze_contribution_skills import (
    extract_skills_from_markdown,
)
from scripts.ci.prek.validate_skills import compute_drift_report, regenerate_skills_json


def _write_payload(path: Path, payload: dict) -> None:
    """Write JSON payload to a file."""
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


class TestEdgeCases:
    """Edge cases around context detection and extraction."""

    def test_context_detects_podman_containerenv(self) -> None:
        """Should detect Breeze when /.containerenv exists (Podman marker)."""
        with patch.dict(os.environ, {}, clear=True):

            def fake_exists(p: str) -> bool:
                return p == "/.containerenv"

            with patch("os.path.exists", side_effect=fake_exists):
                with patch("os.path.isdir", return_value=False):
                    assert BreezieContext.is_in_breeze() is True

    def test_context_detects_ssh_host(self) -> None:
        """Should default to host when Breeze markers are absent even over SSH."""
        with patch.dict(os.environ, {"SSH_CONNECTION": "1"}, clear=True):
            with patch("os.path.exists", return_value=False):
                with patch("os.path.isdir", return_value=False):
                    assert BreezieContext.is_in_breeze() is False

    def test_context_detects_symlink_marker(self) -> None:
        """If dockerenv marker exists (even via symlink), detect Breeze."""
        with patch.dict(os.environ, {}, clear=True):
            with patch("os.path.exists") as mock_exists:
                mock_exists.return_value = True
                with patch("os.path.isdir", return_value=False):
                    assert BreezieContext.is_in_breeze() is True

    def test_parameter_substitution_handles_quotes_spaces(self) -> None:
        """Command template substitution should preserve special characters."""
        test_path = "tests/unit/my file's test.py"
        cmd = BreezieContext.get_command("run-unit-tests", test_path=test_path)
        assert test_path in cmd

    def test_extraction_with_50_plus_skills(self) -> None:
        """Extractor should handle 50+ skills definitions."""
        parts = []
        for i in range(60):
            parts.append(
                "```json\n" + json.dumps({"id": f"skill-{i}", "commands": {"host": "cmd"}}) + "\n```"
            )
        markdown = "\n".join(parts)
        skills = extract_skills_from_markdown(markdown)
        assert len(skills) == 60

    def test_validate_skills_drift_on_modified_skills_json(self, tmp_path: Path) -> None:
        """Drift should be detected if skills.json changes but SKILL.md does not."""
        skill_md = tmp_path / "SKILL.md"
        skill_md.write_text(
            "\n## Skill: a\n```json\n"
            '{"id": "s1", "commands": {"host": "a"}}\n'
            "```\n"
            "## Skill: b\n```json\n"
            '{"id": "s2", "commands": {"host": "b"}}\n'
            "```\n",
            encoding="utf-8",
        )
        skills_json = tmp_path / "skills.json"

        regenerate_skills_json(skill_md, skills_json)
        _ = compute_drift_report(skill_md, skills_json)

        payload = json.loads(skills_json.read_text(encoding="utf-8"))
        assert len(payload["skills"]) == 2
        payload["skills"][0]["commands"]["host"] = "modified"
        _write_payload(skills_json, payload)

        report = compute_drift_report(skill_md, skills_json)
        assert report.drift is True
        assert "Skill" in report.summary or "skill" in report.summary.lower()
