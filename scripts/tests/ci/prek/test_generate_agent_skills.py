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

import yaml
from ci.prek.generate_agent_skills import (
    check_coverage,
    check_drift,
    detect_cycle,
    generate,
    load_workflows,
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
        },
        {
            "context": "host",
            "command": "breeze run pytest {test_path} -xvs",
            "fallback_for": "system_deps_available",
        },
        {"context": "breeze", "command": "pytest {test_path} -xvs"},
    ],
    "parameters": {
        "project": {"description": "distribution folder", "required": True},
        "test_path": {"description": "path to test", "required": True},
    },
    "expected_output": "passed",
}


def _write_yaml(tmp_path: Path, name: str, data: dict) -> Path:
    f = tmp_path / f"{name}.yaml"
    f.write_text(yaml.dump(data), encoding="utf-8")
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
# validate_workflow
# ---------------------------------------------------------------------------


def test_valid_skill_passes_validation(tmp_path):
    source = tmp_path / "run-single-test.yaml"
    errors = validate_workflow(VALID_SKILL, source)
    assert errors == []


def test_missing_required_field_id(tmp_path):
    data = {**VALID_SKILL}
    del data["id"]
    source = tmp_path / "run-single-test.yaml"
    errors = validate_workflow(data, source)
    assert any("'id'" in e for e in errors)


def test_missing_required_field_steps(tmp_path):
    data = {**VALID_SKILL, "steps": []}
    source = tmp_path / "run-single-test.yaml"
    errors = validate_workflow(data, source)
    assert any("steps" in e for e in errors)


def test_invalid_context_in_step(tmp_path):
    data = {**VALID_SKILL, "steps": [{"context": "container", "command": "echo hi"}]}
    source = tmp_path / "run-single-test.yaml"
    errors = validate_workflow(data, source)
    assert any("context" in e for e in errors)


def test_invalid_category(tmp_path):
    data = {**VALID_SKILL, "category": "unknown-category"}
    source = tmp_path / "run-single-test.yaml"
    errors = validate_workflow(data, source)
    assert any("category" in e for e in errors)


def test_filename_must_match_id(tmp_path):
    data = {**VALID_SKILL, "id": "run-single-test"}
    source = tmp_path / "wrong-filename.yaml"
    errors = validate_workflow(data, source)
    assert any("filename" in e for e in errors)


def test_invalid_id_format(tmp_path):
    data = {**VALID_SKILL, "id": "Invalid ID With Spaces"}
    source = tmp_path / "Invalid ID With Spaces.yaml"
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


def test_load_valid_yaml(tmp_path):
    _write_yaml(tmp_path, "run-single-test", VALID_SKILL)
    skills, errors = load_workflows(tmp_path)
    assert errors == []
    assert len(skills) == 1
    assert skills[0]["id"] == "run-single-test"


def test_load_invalid_yaml_syntax(tmp_path):
    f = tmp_path / "bad.yaml"
    f.write_text("id: [\nbad yaml{}", encoding="utf-8")
    skills, errors = load_workflows(tmp_path)
    assert len(errors) >= 1
    assert any("bad.yaml" in e for e in errors)


def test_load_multiple_yaml_files(tmp_path):
    _write_yaml(tmp_path, "run-single-test", VALID_SKILL)
    second = {
        **VALID_SKILL,
        "id": "run-static-checks",
        "category": "linting",
        "prereqs": [],
        "steps": [{"context": "host", "command": "prek run --from-ref main --stage pre-commit"}],
    }
    _write_yaml(tmp_path, "run-static-checks", second)
    skills, errors = load_workflows(tmp_path)
    assert errors == []
    assert len(skills) == 2


# ---------------------------------------------------------------------------
# generate
# ---------------------------------------------------------------------------


def test_generate_creates_json_files(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    output_dir = tmp_path / "agent_skills"

    skill = {**VALID_SKILL, "id": "run-single-test", "prereqs": []}
    _write_yaml(workflows_dir, "run-single-test", skill)

    rc = generate(workflows_dir, output_dir)
    assert rc == 0
    assert (output_dir / "skills.json").exists()
    assert (output_dir / "skill_graph.json").exists()


def test_generate_fails_on_invalid_skill(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    output_dir = tmp_path / "agent_skills"

    bad = {**VALID_SKILL, "context": "invalid", "steps": [{"context": "invalid", "command": "x"}]}
    _write_yaml(workflows_dir, "run-single-test", bad)

    rc = generate(workflows_dir, output_dir)
    assert rc == 1


def test_generate_fails_on_cycle(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    output_dir = tmp_path / "agent_skills"

    skill_a = {
        **VALID_SKILL,
        "id": "skill-a",
        "prereqs": ["skill-b"],
        "steps": [{"context": "host", "command": "echo a"}],
    }
    skill_b = {
        **VALID_SKILL,
        "id": "skill-b",
        "prereqs": ["skill-a"],
        "steps": [{"context": "host", "command": "echo b"}],
    }
    _write_yaml(workflows_dir, "skill-a", skill_a)
    _write_yaml(workflows_dir, "skill-b", skill_b)

    rc = generate(workflows_dir, output_dir)
    assert rc == 1


# ---------------------------------------------------------------------------
# check_drift
# ---------------------------------------------------------------------------


def test_check_drift_passes_when_in_sync(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    output_dir = tmp_path / "agent_skills"

    skill = {**VALID_SKILL, "id": "run-single-test", "prereqs": []}
    _write_yaml(workflows_dir, "run-single-test", skill)

    generate(workflows_dir, output_dir)
    rc = check_drift(workflows_dir, output_dir)
    assert rc == 0


def test_check_drift_fails_when_yaml_modified(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    output_dir = tmp_path / "agent_skills"

    skill = {**VALID_SKILL, "id": "run-single-test", "prereqs": []}
    yaml_file = _write_yaml(workflows_dir, "run-single-test", skill)

    generate(workflows_dir, output_dir)

    # Modify the YAML after generating JSON
    modified = {**skill, "description": "Modified description"}
    yaml_file.write_text(yaml.dump(modified), encoding="utf-8")

    rc = check_drift(workflows_dir, output_dir)
    assert rc == 1


def test_check_drift_fails_when_json_missing(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    output_dir = tmp_path / "agent_skills"

    skill = {**VALID_SKILL, "id": "run-single-test", "prereqs": []}
    _write_yaml(workflows_dir, "run-single-test", skill)

    rc = check_drift(workflows_dir, output_dir)
    assert rc == 1


def test_check_drift_detects_new_skill_added(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    output_dir = tmp_path / "agent_skills"

    skill = {**VALID_SKILL, "id": "run-single-test", "prereqs": []}
    _write_yaml(workflows_dir, "run-single-test", skill)
    generate(workflows_dir, output_dir)

    # Add a second skill without regenerating
    new_skill = {
        **VALID_SKILL,
        "id": "run-static-checks",
        "category": "linting",
        "prereqs": [],
        "steps": [{"context": "host", "command": "prek run --from-ref main --stage pre-commit"}],
    }
    _write_yaml(workflows_dir, "run-static-checks", new_skill)

    rc = check_drift(workflows_dir, output_dir)
    assert rc == 1


# ---------------------------------------------------------------------------
# check_coverage
# ---------------------------------------------------------------------------


def test_coverage_passes_when_all_markers_covered(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    docs_dir = tmp_path / "contributing-docs"
    docs_dir.mkdir()

    _write_yaml(workflows_dir, "run-single-test", VALID_SKILL)
    (docs_dir / "07_local_virtualenv.rst").write_text(
        "Some text\n\n.. workflow-skill:: run-single-test\n\nMore text\n",
        encoding="utf-8",
    )

    rc = check_coverage(workflows_dir, docs_dir)
    assert rc == 0


def test_coverage_fails_when_marker_has_no_yaml(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    docs_dir = tmp_path / "contributing-docs"
    docs_dir.mkdir()

    # No YAML file, but doc has a marker
    (docs_dir / "07_local_virtualenv.rst").write_text(
        ".. workflow-skill:: missing-skill\n",
        encoding="utf-8",
    )

    rc = check_coverage(workflows_dir, docs_dir)
    assert rc == 1


def test_coverage_ignores_rst_without_markers(tmp_path):
    workflows_dir = tmp_path / "workflows"
    workflows_dir.mkdir()
    docs_dir = tmp_path / "contributing-docs"
    docs_dir.mkdir()

    (docs_dir / "01_introduction.rst").write_text("No markers here.\n", encoding="utf-8")

    rc = check_coverage(workflows_dir, docs_dir)
    assert rc == 0
