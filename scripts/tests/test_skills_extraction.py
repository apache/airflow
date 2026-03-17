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

"""Tests for skill extraction and validation."""

from __future__ import annotations

import pytest

from scripts.ci.prek.extract_breeze_contribution_skills import (
    extract_skills_from_markdown,
    validate_skills_json,
)


class TestSkillExtraction:
    """Test skill extraction from SKILL.md."""

    def test_extract_single_skill(self):
        """Should extract single skill from markdown."""
        markdown = """
## Skill: run-static-checks
```json
{
  "id": "run-static-checks",
  "commands": {"host": "prek run"}
}
```
        """
        skills = extract_skills_from_markdown(markdown)
        assert len(skills) == 1
        assert skills[0]["id"] == "run-static-checks"

    def test_extract_multiple_skills(self):
        """Should extract multiple skills."""
        markdown = """
## Skill: skill-1
```json
{"id": "skill-1", "commands": {"host": "cmd1"}}
```

## Skill: skill-2
```json
{"id": "skill-2", "commands": {"host": "cmd2"}}
```
        """
        skills = extract_skills_from_markdown(markdown)
        assert len(skills) == 2

    def test_duplicate_ids_raise_error(self):
        """Should raise error on duplicate IDs."""
        markdown = """
```json
{"id": "skill-1", "commands": {}}
```
```json
{"id": "skill-1", "commands": {}}
```
        """
        with pytest.raises(ValueError, match="Duplicate"):
            extract_skills_from_markdown(markdown)


class TestSkillValidation:
    """Test skill validation."""

    def test_valid_skills_json_passes(self):
        """Should validate correct skills.json."""
        data = {"version": "1.0", "skills": [{"id": "test-skill", "commands": {"host": "test"}}]}
        assert validate_skills_json(data) is True

    def test_missing_version_fails(self):
        """Should fail without version."""
        data = {"skills": []}
        with pytest.raises(ValueError, match="version"):
            validate_skills_json(data)

    def test_missing_skills_fails(self):
        """Should fail without skills array."""
        data = {"version": "1.0"}
        with pytest.raises(ValueError, match="skills"):
            validate_skills_json(data)
