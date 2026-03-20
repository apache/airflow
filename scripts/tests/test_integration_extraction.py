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

"""Integration tests for Breeze skill extraction."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from ci.prek.extract_breeze_contribution_skills import extract_and_generate


def _repo_root() -> Path:
    """Return repository root path."""
    return Path(__file__).resolve().parents[2]


class TestIntegrationExtraction:
    """Integration tests using the real repository SKILL.md."""

    def test_extract_from_real_skill_md(self, tmp_path: Path) -> None:
        """Extractor should generate a valid skills.json from real SKILL.md."""
        skill_md = _repo_root() / ".github" / "skills" / "breeze-contribution" / "SKILL.md"
        out = tmp_path / "skills.json"

        extract_and_generate(str(skill_md), str(out))

        payload = json.loads(out.read_text(encoding="utf-8"))
        assert payload["version"] == "1.0"
        ids = {s["id"] for s in payload["skills"]}
        assert "run-static-checks" in ids
        assert "run-unit-tests" in ids

    def test_malformed_json_in_skill_md_fails(self, tmp_path: Path) -> None:
        """Malformed JSON should raise a helpful ValueError."""
        skill_md = tmp_path / "SKILL.md"
        skill_md.write_text(
            "\n## Skill: bad\n```json\n{ this is not json }\n```\n",
            encoding="utf-8",
        )
        out = tmp_path / "skills.json"

        with pytest.raises(ValueError, match="Malformed JSON"):
            extract_and_generate(str(skill_md), str(out))

    def test_missing_required_fields_fails(self, tmp_path: Path) -> None:
        """Skills missing required keys should fail."""
        skill_md = tmp_path / "SKILL.md"
        skill_md.write_text(
            '\n## Skill: bad\n```json\n{"id": "x"}\n```\n',
            encoding="utf-8",
        )
        out = tmp_path / "skills.json"

        with pytest.raises(ValueError, match="commands"):
            extract_and_generate(str(skill_md), str(out))

    def test_empty_skill_md_fails(self, tmp_path: Path) -> None:
        """An empty SKILL.md should be rejected."""
        skill_md = tmp_path / "SKILL.md"
        skill_md.write_text("", encoding="utf-8")
        out = tmp_path / "skills.json"

        with pytest.raises(ValueError, match="empty"):
            extract_and_generate(str(skill_md), str(out))

    def test_duplicate_skill_ids_fails(self, tmp_path: Path) -> None:
        """Duplicate IDs should fail."""
        skill_md = tmp_path / "SKILL.md"
        skill_md.write_text(
            '\n## Skill: a\n```json\n{"id": "dup", "commands": {}}\n```\n'
            '## Skill: b\n```json\n{"id": "dup", "commands": {}}\n```\n',
            encoding="utf-8",
        )
        out = tmp_path / "skills.json"

        with pytest.raises(ValueError, match="Duplicate"):
            extract_and_generate(str(skill_md), str(out))

    def test_parameter_validation_missing_description_fails(self, tmp_path: Path) -> None:
        """If parameters exist, entries must contain description and required."""
        skill_md = tmp_path / "SKILL.md"
        skill_md.write_text(
            "\n## Skill: p\n"
            "```json\n"
            "{\n"
            '  "id": "skill-p",\n'
            '  "name": "Skill P",\n'
            '  "commands": {"host": "cmd"},\n'
            '  "parameters": {\n'
            '    "path": {"type": "string", "required": true}\n'
            "  }\n"
            "}\n"
            "```\n",
            encoding="utf-8",
        )
        out = tmp_path / "skills.json"

        with pytest.raises(ValueError, match="description"):
            extract_and_generate(str(skill_md), str(out))
