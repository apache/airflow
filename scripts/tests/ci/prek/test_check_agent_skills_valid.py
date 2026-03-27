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

from ci.prek.check_agent_skills_valid import validate


def _write_rst(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "agent_skills.rst"
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Valid cases
# ---------------------------------------------------------------------------


def test_valid_skill_passes(tmp_path):
    rst = _write_rst(
        tmp_path,
        """\
.. agent-skill::
   :id: run-tests
   :category: testing
   :description: Run tests
   :local: uv run --project {project} pytest {test_path}
   :params: project:required,test_path:required
""",
    )
    assert validate([rst]) == []


def test_valid_breeze_only_skill_passes(tmp_path):
    rst = _write_rst(
        tmp_path,
        """\
.. agent-skill::
   :id: run-db-test
   :category: testing
   :description: Run db tests
   :breeze: pytest {test_path}
   :params: test_path:required
""",
    )
    assert validate([rst]) == []


def test_optional_param_passes(tmp_path):
    rst = _write_rst(
        tmp_path,
        """\
.. agent-skill::
   :id: build-docs
   :category: documentation
   :description: Build docs
   :local: breeze build-docs
   :params: package:optional
""",
    )
    assert validate([rst]) == []


# ---------------------------------------------------------------------------
# Missing required field: id
# ---------------------------------------------------------------------------


def test_missing_id_reports_error(tmp_path):
    rst = _write_rst(
        tmp_path,
        """\
.. agent-skill::
   :category: testing
   :description: Run tests
   :local: uv run pytest
""",
    )
    errors = validate([rst])
    assert len(errors) == 1
    assert "missing required field 'id'" in errors[0]


# ---------------------------------------------------------------------------
# No executable step
# ---------------------------------------------------------------------------


def test_skill_with_no_step_reports_error(tmp_path):
    rst = _write_rst(
        tmp_path,
        """\
.. agent-skill::
   :id: empty-skill
   :category: testing
   :description: No step
""",
    )
    errors = validate([rst])
    assert len(errors) == 1
    assert "no executable step" in errors[0]
    assert "empty-skill" in errors[0]


# ---------------------------------------------------------------------------
# Duplicate IDs
# ---------------------------------------------------------------------------


def test_duplicate_id_reports_error(tmp_path):
    rst = _write_rst(
        tmp_path,
        """\
.. agent-skill::
   :id: run-tests
   :local: uv run pytest

.. agent-skill::
   :id: run-tests
   :local: breeze run pytest
""",
    )
    errors = validate([rst])
    assert any("duplicate skill id 'run-tests'" in e for e in errors)


# ---------------------------------------------------------------------------
# Malformed params
# ---------------------------------------------------------------------------


def test_param_without_suffix_reports_error(tmp_path):
    rst = _write_rst(
        tmp_path,
        """\
.. agent-skill::
   :id: run-tests
   :local: uv run --project {project} pytest
   :params: project
""",
    )
    errors = validate([rst])
    assert len(errors) == 1
    assert ":required" in errors[0] or ":optional" in errors[0]


# ---------------------------------------------------------------------------
# Missing RST file
# ---------------------------------------------------------------------------


def test_missing_file_reports_error(tmp_path):
    missing = tmp_path / "agent_skills.rst"
    errors = validate([missing])
    assert len(errors) == 1
    assert "not found" in errors[0]


# ---------------------------------------------------------------------------
# Integration check with real contributing-docs files
# ---------------------------------------------------------------------------


def test_real_contributing_docs_skills_are_valid():
    """All embedded agent-skill blocks in the contributing docs must pass validation."""
    errors = validate()
    assert errors == [], "agent skills have validation errors:\n" + "\n".join(errors)
