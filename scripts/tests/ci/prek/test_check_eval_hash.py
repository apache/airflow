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

from unittest import mock

import pytest
from ci.prek.check_eval_hash import (
    compute_guidance_hash,
    compute_skill_hash,
    main,
    read_recorded_hash,
    write_recorded_hash,
)

SKILL = "draft-text"


@pytest.fixture
def guidance(tmp_path):
    """A tmp AGENTS.md and cases dir mirroring the eval input layout."""
    agents_file = tmp_path / "AGENTS.md"
    agents_file.write_text("golden rule\n")
    cases_dir = tmp_path / "cases"
    cases_dir.mkdir()
    (cases_dir / "newsfragment.yaml").write_text("- description: case\n")
    return agents_file, cases_dir


@pytest.fixture
def skill(tmp_path):
    """A tmp skill and its eval suite, laid out like .agents/skills and dev/skill-evals/skills."""
    skill_file = tmp_path / "agent-skills" / SKILL / "SKILL.md"
    skill_file.parent.mkdir(parents=True)
    skill_file.write_text("draft rules\n")
    suite_dir = tmp_path / "skill-suites" / SKILL
    (suite_dir / "cases").mkdir(parents=True)
    (suite_dir / "cases" / "titles.yaml").write_text("- description: title case\n")
    (suite_dir / "output-schema.json").write_text('{"type": "object"}\n')
    return skill_file, suite_dir


@pytest.fixture
def hook_paths(guidance, skill, tmp_path):
    """Point the hook at the tmp inputs, with the AGENTS.md hash recorded as current."""
    agents_file, cases_dir = guidance
    skill_file, suite_dir = skill
    hash_file = tmp_path / "last-eval-hash.txt"
    write_recorded_hash(compute_guidance_hash(agents_file, cases_dir), hash_file)
    with (
        mock.patch("ci.prek.check_eval_hash.AGENTS_FILE", agents_file),
        mock.patch("ci.prek.check_eval_hash.CASES_DIR", cases_dir),
        mock.patch("ci.prek.check_eval_hash.HASH_FILE", hash_file),
        mock.patch("ci.prek.check_eval_hash.SKILLS_DIR", skill_file.parent.parent),
        mock.patch("ci.prek.check_eval_hash.SKILL_EVALS_DIR", suite_dir.parent),
    ):
        yield hash_file


def test_compute_guidance_hash_is_deterministic(guidance):
    agents_file, cases_dir = guidance

    assert compute_guidance_hash(agents_file, cases_dir) == compute_guidance_hash(agents_file, cases_dir)


def test_compute_guidance_hash_is_unchanged_for_existing_inputs(guidance):
    """Hashes already recorded on main must keep matching after skill suites were added."""
    agents_file, cases_dir = guidance
    # Bytes, not text: write_text would turn the newlines into CRLF on Windows.
    agents_file.write_bytes(b"golden rule\n")
    (cases_dir / "newsfragment.yaml").write_bytes(b"- description: case\n")

    assert (
        compute_guidance_hash(agents_file, cases_dir)
        == "498518ff92a26c7a4ff3fcd2f4a2093f1a760613543c4243ea95bfc07b192f1b"
    )


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda agents_file, cases_dir: agents_file.write_text("golden rule, revised\n"),
            id="agents-content",
        ),
        pytest.param(
            lambda agents_file, cases_dir: (cases_dir / "newsfragment.yaml").write_text(
                "- description: revised case\n"
            ),
            id="case-content",
        ),
        pytest.param(
            lambda agents_file, cases_dir: (cases_dir / "routing.yaml").write_text(
                "- description: another case\n"
            ),
            id="case-added",
        ),
        pytest.param(
            lambda agents_file, cases_dir: (cases_dir / "newsfragment.yaml").rename(
                cases_dir / "renamed.yaml"
            ),
            id="case-renamed",
        ),
    ],
)
def test_compute_guidance_hash_changes_when_inputs_change(guidance, mutate):
    agents_file, cases_dir = guidance
    before = compute_guidance_hash(agents_file, cases_dir)

    mutate(agents_file, cases_dir)

    assert compute_guidance_hash(agents_file, cases_dir) != before


@pytest.mark.parametrize(
    "mutate",
    [
        pytest.param(
            lambda skill_file, suite_dir: skill_file.write_text("draft rules, revised\n"),
            id="skill-content",
        ),
        pytest.param(
            lambda skill_file, suite_dir: (suite_dir / "output-schema.json").write_text(
                '{"type": "object", "required": ["title"]}\n'
            ),
            id="schema-content",
        ),
        pytest.param(
            lambda skill_file, suite_dir: (suite_dir / "cases" / "titles.yaml").write_text(
                "- description: revised case\n"
            ),
            id="case-content",
        ),
        pytest.param(
            lambda skill_file, suite_dir: (suite_dir / "cases" / "comments.yaml").write_text(
                "- description: another case\n"
            ),
            id="case-added",
        ),
        pytest.param(
            lambda skill_file, suite_dir: (suite_dir / "cases" / "titles.yaml").rename(
                suite_dir / "cases" / "renamed.yaml"
            ),
            id="case-renamed",
        ),
    ],
)
def test_compute_skill_hash_changes_when_inputs_change(skill, mutate):
    skill_file, suite_dir = skill
    before = compute_skill_hash(skill_file, suite_dir)

    mutate(skill_file, suite_dir)

    assert compute_skill_hash(skill_file, suite_dir) != before


def test_read_recorded_hash_returns_none_when_missing(tmp_path):
    assert read_recorded_hash(tmp_path / "absent.txt") is None


def test_write_then_read_roundtrip(tmp_path):
    hash_file = tmp_path / "last-eval-hash.txt"

    write_recorded_hash("abc123", hash_file)

    assert read_recorded_hash(hash_file) == "abc123"
    assert hash_file.read_text().startswith("# Generated by dev/skill-evals/eval.py")
    assert "SKILL_NAME" not in hash_file.read_text()
    assert hash_file.read_text().endswith("abc123\n")


def test_a_skill_hash_file_names_the_run_that_regenerates_it(tmp_path):
    hash_file = tmp_path / "last-eval-hash.txt"

    write_recorded_hash("abc123", hash_file, skill_name=SKILL)

    assert f"SKILL_NAME={SKILL} prek run run-skill-eval" in hash_file.read_text()
    assert read_recorded_hash(hash_file) == "abc123"


def test_main_passes_when_hash_matches(guidance, tmp_path, capsys):
    agents_file, cases_dir = guidance
    hash_file = tmp_path / "last-eval-hash.txt"
    write_recorded_hash(compute_guidance_hash(agents_file, cases_dir), hash_file)

    with (
        mock.patch("ci.prek.check_eval_hash.AGENTS_FILE", agents_file),
        mock.patch("ci.prek.check_eval_hash.CASES_DIR", cases_dir),
        mock.patch("ci.prek.check_eval_hash.HASH_FILE", hash_file),
        mock.patch("ci.prek.check_eval_hash.SKILL_EVALS_DIR", tmp_path / "no-skill-suites"),
    ):
        assert main() == 0

    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("record_stale_hash", [True, False], ids=["stale-hash", "missing-file"])
def test_main_fails_on_mismatch_or_missing_record(guidance, tmp_path, capsys, record_stale_hash):
    agents_file, cases_dir = guidance
    hash_file = tmp_path / "last-eval-hash.txt"
    if record_stale_hash:
        write_recorded_hash(compute_guidance_hash(agents_file, cases_dir), hash_file)
        agents_file.write_text("changed after the recorded run\n")

    with (
        mock.patch("ci.prek.check_eval_hash.AGENTS_FILE", agents_file),
        mock.patch("ci.prek.check_eval_hash.CASES_DIR", cases_dir),
        mock.patch("ci.prek.check_eval_hash.HASH_FILE", hash_file),
        mock.patch("ci.prek.check_eval_hash.SKILL_EVALS_DIR", tmp_path / "no-skill-suites"),
    ):
        assert main() == 1

    err = capsys.readouterr().err
    assert "prek run run-skill-eval" in err
    assert "SKIP=check-eval-hash" in err


def test_main_passes_when_a_skill_suite_hash_matches(skill, hook_paths, capsys):
    skill_file, suite_dir = skill
    write_recorded_hash(compute_skill_hash(skill_file, suite_dir), suite_dir / "last-eval-hash.txt")

    assert main() == 0
    assert capsys.readouterr().err == ""


@pytest.mark.parametrize("record_stale_hash", [True, False], ids=["stale-hash", "missing-file"])
def test_main_fails_when_a_skill_changed_without_its_eval(skill, hook_paths, capsys, record_stale_hash):
    skill_file, suite_dir = skill
    if record_stale_hash:
        write_recorded_hash(compute_skill_hash(skill_file, suite_dir), suite_dir / "last-eval-hash.txt")
        skill_file.write_text("draft rules, changed after the recorded run\n")

    assert main() == 1

    err = capsys.readouterr().err
    assert f"SKILL_NAME={SKILL} prek run run-skill-eval" in err
    assert "AGENTS.md or dev/skill-evals/cases/" not in err


def test_main_ignores_skills_without_a_suite(skill, hook_paths, capsys):
    skill_file, suite_dir = skill
    write_recorded_hash(compute_skill_hash(skill_file, suite_dir), suite_dir / "last-eval-hash.txt")
    other = skill_file.parent.parent / "no-cases-yet" / "SKILL.md"
    other.parent.mkdir()
    other.write_text("a skill nobody has written cases for\n")

    assert main() == 0


def test_main_fails_on_a_suite_for_a_skill_that_does_not_exist(skill, hook_paths, capsys):
    skill_file, suite_dir = skill
    write_recorded_hash(compute_skill_hash(skill_file, suite_dir), suite_dir / "last-eval-hash.txt")
    orphan = suite_dir.parent / "renamed-skill" / "cases"
    orphan.mkdir(parents=True)
    (orphan / "titles.yaml").write_text("- description: case\n")

    assert main() == 1
    assert ".agents/skills/renamed-skill/SKILL.md" in capsys.readouterr().err
