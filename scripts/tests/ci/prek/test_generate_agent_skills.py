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

import json
from pathlib import Path

from ci.prek.generate_agent_skills import (
    check_drift,
    detect_cycle,
    generate,
    load_workflows,
    parse_rst_skills,
    validate_prereq_references,
    validate_workflow,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_SKILL: dict = {
    "id": "run-single-test",
    "category": "testing",
    "description": "Run a targeted test",
    "prereqs": ["setup-breeze-environment"],
    "steps": [
        {
            "context": "host",
            "command": "uv run --project {project} pytest {test_path} -xvs",
            "condition": "system_deps_available",
            "description": "Preferred: run directly with uv (faster, debuggable in IDE)",
        },
        {
            "context": "host",
            "command": "breeze run pytest {test_path} -xvs",
            "fallback_for": "system_deps_available",
            "description": "Fallback: use Breeze when system deps are missing",
        },
        {"context": "breeze", "command": "pytest {test_path} -xvs"},
    ],
    "parameters": {
        "project": {"required": True},
        "test_path": {"required": True},
    },
    "expected_output": "passed",
}

# Minimal RST with one skill that has :local: + :fallback: + :breeze:
_RST_SINGLE_SKILL = """\
Agent Skills
============

Running a Single Test
---------------------

.. agent-skill::
   :id: run-single-test
   :category: testing
   :description: Run a targeted test for a changed module or provider
   :local: uv run --project {project} pytest {test_path} -xvs
   :fallback: breeze run pytest {test_path} -xvs
   :breeze: pytest {test_path} -xvs
   :prereqs: setup-breeze-environment
   :params: project:required,test_path:required
   :expected-output: passed
"""

# RST with run-db-test pattern: :local: only (no :fallback:) = unconditional Breeze-first
_RST_DB_TEST_SKILL = """\
Agent Skills
============

Running DB Tests
----------------

.. agent-skill::
   :id: run-db-test
   :category: testing
   :description: Run tests marked with @pytest.mark.db_test. Always uses Breeze.
   :local: breeze run pytest {test_path} -xvs
   :breeze: pytest {test_path} -xvs
   :prereqs: setup-breeze-environment
   :params: test_path:required
   :expected-output: passed
"""


def _write_rst(tmp_path: Path, content: str) -> Path:
    f = tmp_path / "agent_skills.rst"
    f.write_text(content, encoding="utf-8")
    return f


def _write_skills_json(output_dir: Path, skills: list[dict]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "skills.json").write_text(
        json.dumps({"version": "1.0", "skills": skills}, indent=2), encoding="utf-8"
    )
    (output_dir / "skill_graph.json").write_text(
        json.dumps({"nodes": [], "edges": []}, indent=2), encoding="utf-8"
    )


# ---------------------------------------------------------------------------
# parse_rst_skills
# ---------------------------------------------------------------------------


def test_parse_single_skill_with_local_fallback_breeze(tmp_path):
    rst = _write_rst(tmp_path, _RST_SINGLE_SKILL)
    skills, errors = parse_rst_skills(rst)
    assert errors == []
    assert len(skills) == 1
    s = skills[0]
    assert s["id"] == "run-single-test"
    assert s["category"] == "testing"
    assert s["description"] == "Run a targeted test for a changed module or provider"
    assert s["prereqs"] == ["setup-breeze-environment"]
    assert s["parameters"] == {"project": {"required": True}, "test_path": {"required": True}}
    assert s["expected_output"] == "passed"
    # Should have 3 steps: conditional host, fallback host, breeze
    assert len(s["steps"]) == 3
    host_steps = [st for st in s["steps"] if st["context"] == "host"]
    breeze_steps = [st for st in s["steps"] if st["context"] == "breeze"]
    assert len(host_steps) == 2
    assert len(breeze_steps) == 1
    assert host_steps[0]["condition"] == "system_deps_available"
    assert host_steps[1]["fallback_for"] == "system_deps_available"


def test_parse_db_test_skill_local_only_is_unconditional(tmp_path):
    """run-db-test pattern: :local: without :fallback: → no condition on host step."""
    rst = _write_rst(tmp_path, _RST_DB_TEST_SKILL)
    skills, errors = parse_rst_skills(rst)
    assert errors == []
    assert len(skills) == 1
    s = skills[0]
    assert s["id"] == "run-db-test"
    # Should have 2 steps: unconditional host + breeze
    assert len(s["steps"]) == 2
    host_step = next(st for st in s["steps"] if st["context"] == "host")
    assert "condition" not in host_step
    assert "fallback_for" not in host_step
    assert host_step["command"] == "breeze run pytest {test_path} -xvs"


def test_parse_missing_id_returns_error(tmp_path):
    rst_content = """\
Skills
======

.. agent-skill::
   :category: testing
   :description: No id here
   :local: echo hi
"""
    rst = _write_rst(tmp_path, rst_content)
    skills, errors = parse_rst_skills(rst)
    assert len(errors) >= 1
    assert any(":id:" in e for e in errors)


def test_parse_missing_required_option_returns_error(tmp_path):
    rst_content = """\
Skills
======

.. agent-skill::
   :id: broken-skill
   :description: Missing category and commands
"""
    rst = _write_rst(tmp_path, rst_content)
    skills, errors = parse_rst_skills(rst)
    assert len(errors) >= 1


def test_parse_multiple_skills(tmp_path):
    rst_content = _RST_SINGLE_SKILL + "\n" + _RST_DB_TEST_SKILL.split("============\n\n", 1)[1]
    rst = _write_rst(tmp_path, rst_content)
    skills, errors = parse_rst_skills(rst)
    assert errors == []
    assert len(skills) == 2
    ids = [s["id"] for s in skills]
    assert "run-single-test" in ids
    assert "run-db-test" in ids


def test_parse_missing_rst_file():
    skills, errors = parse_rst_skills(Path("/nonexistent/agent_skills.rst"))
    assert skills == []
    assert len(errors) == 1


# ---------------------------------------------------------------------------
# validate_workflow
# ---------------------------------------------------------------------------


def test_valid_skill_passes_validation(tmp_path):
    source = tmp_path / "agent_skills.rst"
    errors = validate_workflow(VALID_SKILL, source)
    assert errors == []


def test_missing_required_field_id(tmp_path):
    data = {**VALID_SKILL}
    del data["id"]
    source = tmp_path / "agent_skills.rst"
    errors = validate_workflow(data, source)
    assert any("'id'" in e for e in errors)


def test_missing_required_field_steps(tmp_path):
    data = {**VALID_SKILL, "steps": []}
    source = tmp_path / "agent_skills.rst"
    errors = validate_workflow(data, source)
    assert any("steps" in e for e in errors)


def test_invalid_context_in_step(tmp_path):
    data = {**VALID_SKILL, "steps": [{"context": "container", "command": "echo hi"}]}
    source = tmp_path / "agent_skills.rst"
    errors = validate_workflow(data, source)
    assert any("context" in e for e in errors)


def test_invalid_category(tmp_path):
    data = {**VALID_SKILL, "category": "unknown-category"}
    source = tmp_path / "agent_skills.rst"
    errors = validate_workflow(data, source)
    assert any("category" in e for e in errors)


def test_invalid_id_format(tmp_path):
    data = {**VALID_SKILL, "id": "Invalid ID With Spaces"}
    source = tmp_path / "agent_skills.rst"
    errors = validate_workflow(data, source)
    assert any("id" in e for e in errors)


# ---------------------------------------------------------------------------
# validate_prereq_references
# ---------------------------------------------------------------------------


def test_prereq_references_valid_ids():
    skills = [
        {**VALID_SKILL, "id": "setup-breeze-environment", "prereqs": []},
        {**VALID_SKILL, "id": "run-single-test", "prereqs": ["setup-breeze-environment"]},
    ]
    errors = validate_prereq_references(skills)
    assert errors == []


def test_prereq_references_nonexistent_id():
    skills = [
        {**VALID_SKILL, "id": "run-single-test", "prereqs": ["typo-skill-id"]},
    ]
    errors = validate_prereq_references(skills)
    assert len(errors) == 1
    assert "typo-skill-id" in errors[0]


# ---------------------------------------------------------------------------
# detect_cycle
# ---------------------------------------------------------------------------


def test_no_cycle_returns_none():
    skills = [
        {**VALID_SKILL, "id": "setup-breeze-environment", "prereqs": []},
        {**VALID_SKILL, "id": "run-static-checks", "prereqs": ["setup-breeze-environment"]},
        {**VALID_SKILL, "id": "run-manual-checks", "prereqs": ["run-static-checks"]},
    ]
    assert detect_cycle(skills) is None


def test_cycle_detected():
    skills = [
        {**VALID_SKILL, "id": "skill-a", "prereqs": ["skill-b"]},
        {**VALID_SKILL, "id": "skill-b", "prereqs": ["skill-a"]},
    ]
    cycle = detect_cycle(skills)
    assert cycle is not None
    assert "skill-a" in cycle
    assert "skill-b" in cycle


def test_self_cycle_detected():
    skills = [{**VALID_SKILL, "id": "skill-a", "prereqs": ["skill-a"]}]
    cycle = detect_cycle(skills)
    assert cycle is not None


# ---------------------------------------------------------------------------
# load_workflows
# ---------------------------------------------------------------------------


def test_load_valid_rst(tmp_path):
    rst = _write_rst(tmp_path, _RST_SINGLE_SKILL)
    skills, errors = load_workflows(rst)
    assert errors == []
    assert len(skills) == 1
    assert skills[0]["id"] == "run-single-test"


def test_load_invalid_rst_missing_id(tmp_path):
    rst_content = """\
Skills
======

.. agent-skill::
   :category: testing
   :description: No id
   :local: echo hi
"""
    rst = _write_rst(tmp_path, rst_content)
    skills, errors = load_workflows(rst)
    assert len(errors) >= 1


def test_load_multiple_skills(tmp_path):
    rst_content = _RST_SINGLE_SKILL + "\n" + _RST_DB_TEST_SKILL.split("============\n\n", 1)[1]
    rst = _write_rst(tmp_path, rst_content)
    skills, errors = load_workflows(rst)
    assert errors == []
    assert len(skills) == 2


# ---------------------------------------------------------------------------
# generate
# ---------------------------------------------------------------------------


def test_generate_creates_json_files(tmp_path):
    rst = _write_rst(tmp_path, _RST_SINGLE_SKILL.replace("prereqs: setup-breeze-environment", ""))
    output_dir = tmp_path / "agent_skills"

    rc = generate(rst, output_dir)
    assert rc == 0
    assert (output_dir / "skills.json").exists()
    assert (output_dir / "skill_graph.json").exists()


def test_generate_fails_on_invalid_skill(tmp_path):
    rst_content = """\
Skills
======

.. agent-skill::
   :id: bad-skill
   :category: invalid-cat
   :description: Bad category
   :local: echo hi
"""
    rst = _write_rst(tmp_path, rst_content)
    output_dir = tmp_path / "agent_skills"

    rc = generate(rst, output_dir)
    assert rc == 1


def test_generate_fails_on_cycle(tmp_path):
    rst_content = """\
Skills
======

.. agent-skill::
   :id: skill-a
   :category: testing
   :description: Skill A
   :local: echo a
   :prereqs: skill-b

.. agent-skill::
   :id: skill-b
   :category: testing
   :description: Skill B
   :local: echo b
   :prereqs: skill-a
"""
    rst = _write_rst(tmp_path, rst_content)
    output_dir = tmp_path / "agent_skills"

    rc = generate(rst, output_dir)
    assert rc == 1


# ---------------------------------------------------------------------------
# check_drift
# ---------------------------------------------------------------------------


def test_check_drift_passes_when_in_sync(tmp_path):
    rst_content = _RST_SINGLE_SKILL.replace("prereqs: setup-breeze-environment", "")
    rst = _write_rst(tmp_path, rst_content)
    output_dir = tmp_path / "agent_skills"

    generate(rst, output_dir)
    rc = check_drift(rst, output_dir)
    assert rc == 0


def test_check_drift_fails_when_rst_modified(tmp_path):
    rst_content = _RST_SINGLE_SKILL.replace("prereqs: setup-breeze-environment", "")
    rst = _write_rst(tmp_path, rst_content)
    output_dir = tmp_path / "agent_skills"

    generate(rst, output_dir)

    # Modify the RST after generating JSON
    modified = rst_content.replace(
        "Run a targeted test for a changed module or provider",
        "Modified description",
    )
    rst.write_text(modified, encoding="utf-8")

    rc = check_drift(rst, output_dir)
    assert rc == 1


def test_check_drift_fails_when_json_missing(tmp_path):
    rst_content = _RST_SINGLE_SKILL.replace("prereqs: setup-breeze-environment", "")
    rst = _write_rst(tmp_path, rst_content)
    output_dir = tmp_path / "agent_skills"

    rc = check_drift(rst, output_dir)
    assert rc == 1


def test_check_drift_detects_new_skill_added(tmp_path):
    rst_content = _RST_SINGLE_SKILL.replace("prereqs: setup-breeze-environment", "")
    rst = _write_rst(tmp_path, rst_content)
    output_dir = tmp_path / "agent_skills"

    generate(rst, output_dir)

    # Add a second skill without regenerating
    extra = _RST_DB_TEST_SKILL.split("============\n\n", 1)[1].replace(
        "prereqs: setup-breeze-environment", ""
    )
    rst.write_text(rst_content + "\n" + extra, encoding="utf-8")

    rc = check_drift(rst, output_dir)
    assert rc == 1
