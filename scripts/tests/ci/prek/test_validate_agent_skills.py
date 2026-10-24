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

from pathlib import Path

from ci.prek.validate_agent_skills import (
    validate_all_skills,
    validate_skill_file,
    validate_symlinks,
)
from common_prek_utils import AIRFLOW_ROOT_PATH


def test_validate_all_existing_repo_skills_pass():
    errors = validate_all_skills(AIRFLOW_ROOT_PATH)
    assert errors == []


def test_validate_skill_file_missing(tmp_path: Path):
    non_existent = tmp_path / "SKILL.md"
    errors = validate_skill_file(non_existent)
    assert any("file does not exist" in err for err in errors)


def test_validate_skill_file_no_frontmatter(tmp_path: Path):
    skill_dir = tmp_path / "my-skill"
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text("# Just Markdown\nNo frontmatter here.")

    errors = validate_skill_file(skill_file)
    assert any("missing YAML frontmatter" in err for err in errors)


def test_validate_skill_file_unclosed_frontmatter(tmp_path: Path):
    skill_dir = tmp_path / "my-skill"
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text("---\nname: my-skill\nNo closing delimiter")

    errors = validate_skill_file(skill_file)
    assert any("unclosed YAML frontmatter delimiter" in err for err in errors)


def test_validate_skill_file_invalid_yaml(tmp_path: Path):
    skill_dir = tmp_path / "my-skill"
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text("---\nname: [unclosed list\n---\n<!-- SPDX-License-Identifier: Apache-2.0 -->")

    errors = validate_skill_file(skill_file)
    assert any("invalid YAML frontmatter" in err for err in errors)


def test_validate_skill_file_missing_fields(tmp_path: Path):
    skill_dir = tmp_path / "my-skill"
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text("---\nother: field\n---\n<!-- SPDX-License-Identifier: Apache-2.0 -->")

    errors = validate_skill_file(skill_file)
    assert any("missing non-empty 'name' field" in err for err in errors)
    assert any("missing non-empty 'description' field" in err for err in errors)


def test_validate_skill_file_mismatched_name(tmp_path: Path):
    skill_dir = tmp_path / "dir-name"
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text(
        "---\nname: different-name\ndescription: Some description\n---\n"
        "<!-- SPDX-License-Identifier: Apache-2.0 -->"
    )

    errors = validate_skill_file(skill_file)
    assert any("does not match directory" in err for err in errors)


def test_validate_skill_file_invalid_license(tmp_path: Path):
    skill_dir = tmp_path / "my-skill"
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text(
        "---\nname: my-skill\ndescription: Some description\nlicense: MIT\n---\n"
        "<!-- SPDX-License-Identifier: Apache-2.0 -->"
    )

    errors = validate_skill_file(skill_file)
    assert any("frontmatter license must be 'Apache-2.0'" in err for err in errors)


def test_validate_skill_file_missing_license_declaration(tmp_path: Path):
    skill_dir = tmp_path / "my-skill"
    skill_dir.mkdir()
    skill_file = skill_dir / "SKILL.md"
    skill_file.write_text("---\nname: my-skill\ndescription: Some description\n---\n# Title without license")

    errors = validate_skill_file(skill_file)
    assert any("missing Apache-2.0 license declaration" in err for err in errors)


def test_validate_symlinks_broken(tmp_path: Path):
    agents_dir = tmp_path / ".agents" / "skills"
    claude_dir = tmp_path / ".claude" / "skills"
    agents_dir.mkdir(parents=True)
    claude_dir.mkdir(parents=True)

    dangling_link = claude_dir / "broken-skill"
    dangling_link.symlink_to(agents_dir / "nonexistent-skill")

    errors = validate_symlinks(tmp_path)
    assert any("broken symlink" in err or "does not exist" in err for err in errors)


def test_validate_symlinks_outside_skills(tmp_path: Path):
    agents_dir = tmp_path / ".agents" / "skills"
    claude_dir = tmp_path / ".claude" / "skills"
    other_dir = tmp_path / "other_dir"
    agents_dir.mkdir(parents=True)
    claude_dir.mkdir(parents=True)
    other_dir.mkdir()

    outside_link = claude_dir / "escaped-skill"
    outside_link.symlink_to(other_dir)

    errors = validate_symlinks(tmp_path)
    assert any("points outside .agents/skills" in err for err in errors)


def test_validate_symlinks_missing_from_gitignore(tmp_path: Path):
    agents_dir = tmp_path / ".agents" / "skills" / "my-skill"
    claude_dir = tmp_path / ".claude" / "skills"
    agents_dir.mkdir(parents=True)
    claude_dir.mkdir(parents=True)

    link = claude_dir / "my-skill"
    link.symlink_to(agents_dir)

    # Empty .gitignore
    (tmp_path / ".gitignore").write_text(".claude/skills/*\n")

    errors = validate_symlinks(tmp_path)
    assert any("symlink is ignored by .gitignore" in err for err in errors)

    # Add unignore pattern
    (tmp_path / ".gitignore").write_text(".claude/skills/*\n!.claude/skills/my-skill\n")
    errors = validate_symlinks(tmp_path)
    assert errors == []
