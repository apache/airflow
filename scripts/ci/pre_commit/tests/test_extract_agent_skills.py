#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use it except in compliance
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
"""Tests for extract_agent_skills.py."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

# Add parent so we can import extract_agent_skills
SCRIPT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

from extract_agent_skills import extract_all_skills, parse_skills, validate_skill

VALID_SKILL_RST = """
.. agent-skill::
   :id: setup-breeze-environment
   :context: host
   :category: environment
   :prereqs: docker, python>=3.9
   :validates: breeze-env-ready
   :description: Start the Airflow Breeze development environment

   .. code-block:: bash

      breeze start-airflow

   .. agent-skill-expected-output::

      Airflow webserver at http://localhost:28080
"""


def test_valid_skill_parsed():
    """Sample RST string with one valid skill block, assert all fields extracted correctly."""
    skills = parse_skills(VALID_SKILL_RST)
    assert len(skills) == 1
    skill = skills[0]
    assert skill["id"] == "setup-breeze-environment"
    assert skill["context"] == "host"
    assert skill["category"] == "environment"
    assert skill["prereqs"] == "docker, python>=3.9"
    assert skill["validates"] == "breeze-env-ready"
    assert "Breeze development environment" in skill["description"]
    assert "breeze start-airflow" in skill["command"]
    assert "localhost:28080" in skill["expected_output"]
    validate_skill(skill, 0)


def test_invalid_context_fails(tmp_path):
    """context: 'container' should raise SystemExit with code 1."""
    rst = """
.. agent-skill::
   :id: bad-context
   :context: container
   :category: environment
   :description: Bad context skill

   .. code-block:: bash

      echo ok

   .. agent-skill-expected-output::

      ok
"""
    docs_file = tmp_path / "AGENTS.md"
    docs_file.write_text(rst)
    out_file = tmp_path / "skills.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--docs-file",
            str(docs_file),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "context" in result.stderr


def test_invalid_id_fails(tmp_path):
    """id with spaces should fail."""
    rst = """
.. agent-skill::
   :id: invalid id with spaces
   :context: host
   :category: testing
   :description: Bad id skill

   .. code-block:: bash

      echo ok

   .. agent-skill-expected-output::

      ok
"""
    docs_file = tmp_path / "AGENTS.md"
    docs_file.write_text(rst)
    out_file = tmp_path / "skills.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--docs-file",
            str(docs_file),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "id" in result.stderr


def test_missing_description_fails(tmp_path):
    """No :description: should fail."""
    rst = """
.. agent-skill::
   :id: no-description
   :context: host
   :category: linting

   .. code-block:: bash

      prek run

   .. agent-skill-expected-output::

      ok
"""
    docs_file = tmp_path / "AGENTS.md"
    docs_file.write_text(rst)
    out_file = tmp_path / "skills.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--docs-file",
            str(docs_file),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "description" in result.stderr


def test_check_mode_passes_when_in_sync(tmp_path):
    """Given skills.json matches extracted skills, assert exit code 0."""
    rst = """
.. agent-skill::
   :id: in-sync-skill
   :context: host
   :category: environment
   :description: In sync skill

   .. code-block:: bash

      echo ok

   .. agent-skill-expected-output::

      ok
"""
    docs_file = tmp_path / "AGENTS.md"
    docs_file.write_text(rst)
    out_file = tmp_path / "skills.json"
    # Generate first
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--docs-file",
            str(docs_file),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        check=True,
    )
    # Then check
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--check",
            "--docs-file",
            str(docs_file),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "OK: skills.json is in sync" in result.stdout


def test_check_mode_fails_when_drifted(tmp_path):
    """Given skills.json has a skill with wrong context, assert exit code 1."""
    rst = """
.. agent-skill::
   :id: drift-skill
   :context: host
   :category: environment
   :description: Drift skill

   .. code-block:: bash

      echo ok

   .. agent-skill-expected-output::

      ok
"""
    docs_file = tmp_path / "AGENTS.md"
    docs_file.write_text(rst)
    out_file = tmp_path / "skills.json"
    # Generate once
    subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--docs-file",
            str(docs_file),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        check=True,
    )
    # Tamper: change context in JSON
    data = json.loads(out_file.read_text())
    data["skills"][0]["context"] = "breeze"
    out_file.write_text(json.dumps(data, indent=2))
    # Check should detect drift
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--check",
            "--docs-file",
            str(docs_file),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "Skills modified" in result.stdout or "DRIFT DETECTED" in result.stdout


def test_check_mode_prints_diff(tmp_path):
    """Given a skill is missing from skills.json, assert output contains DRIFT DETECTED."""
    rst = """
.. agent-skill::
   :id: only-in-source
   :context: host
   :category: environment
   :description: Only in source

   .. code-block:: bash

      echo ok

   .. agent-skill-expected-output::

      ok
"""
    docs_file = tmp_path / "AGENTS.md"
    docs_file.write_text(rst)
    out_file = tmp_path / "skills.json"
    # Write a minimal skills.json that has no skills (or different skill)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(json.dumps({"version": "1.0", "skills": []}, indent=2))
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--check",
            "--docs-file",
            str(docs_file),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "DRIFT DETECTED" in result.stdout


def test_extracts_from_rst_file(tmp_path):
    """Extractor picks up agent-skill from a .rst file under docs_dir."""
    rst_content = """
.. agent-skill::
   :id: only-in-rst
   :context: host
   :category: testing
   :description: Skill defined in RST only

   .. code-block:: bash

      echo from-rst

   .. agent-skill-expected-output::

      from-rst
"""
    (tmp_path / "contributing-docs").mkdir(parents=True)
    (tmp_path / "contributing-docs" / "test.rst").write_text(rst_content)
    (tmp_path / "AGENTS.md").write_text("")
    out_file = tmp_path / "skills.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--docs-file",
            str(tmp_path / "AGENTS.md"),
            "--docs-dir",
            str(tmp_path / "contributing-docs"),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert result.returncode == 0
    data = json.loads(out_file.read_text())
    skills = data["skills"]
    assert len(skills) == 1
    assert skills[0]["id"] == "only-in-rst"
    assert "from-rst" in skills[0]["command"]


def test_extracts_from_both_sources(tmp_path):
    """Extractor merges skills from AGENTS.md and from a .rst file."""
    agents_content = """
.. agent-skill::
   :id: from-agents
   :context: host
   :category: environment
   :description: From AGENTS.md

   .. code-block:: bash

      echo agents

   .. agent-skill-expected-output::

      agents
"""
    rst_content = """
.. agent-skill::
   :id: from-rst
   :context: host
   :category: testing
   :description: From RST

   .. code-block:: bash

      echo rst

   .. agent-skill-expected-output::

      rst
"""
    (tmp_path / "AGENTS.md").write_text(agents_content)
    (tmp_path / "contributing-docs").mkdir(parents=True)
    (tmp_path / "contributing-docs" / "extra.rst").write_text(rst_content)
    out_file = tmp_path / "skills.json"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_DIR / "extract_agent_skills.py"),
            "--docs-file",
            str(tmp_path / "AGENTS.md"),
            "--docs-dir",
            str(tmp_path / "contributing-docs"),
            "--output",
            str(out_file),
        ],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )
    assert result.returncode == 0
    data = json.loads(out_file.read_text())
    skills = {s["id"]: s for s in data["skills"]}
    assert "from-agents" in skills
    assert "from-rst" in skills
    assert len(skills) == 2


def test_rst_skill_appears_in_manifest():
    """After extraction, run-tests-uv-first from contributing-docs RST is in skills.json."""
    root = SCRIPT_DIR.resolve().parents[2]  # repo root (pre_commit -> ci -> scripts -> root)
    skills = extract_all_skills(root, "AGENTS.md", "contributing-docs/")
    ids = [s["id"] for s in skills]
    assert "run-tests-uv-first" in ids


def test_second_rst_skill_extracted():
    """The additional RST skill block is also extracted into skills.json."""
    root = SCRIPT_DIR.resolve().parents[2]  # repo root (pre_commit -> ci -> scripts -> root)
    skills = extract_all_skills(root, "AGENTS.md", "contributing-docs/")
    ids = [s["id"] for s in skills]
    assert "run-static-checks-prek" in ids


def test_multiple_skills_extracted():
    """Two skill blocks in one file, assert len(skills) == 2."""
    rst = """
.. agent-skill::
   :id: first-skill
   :context: host
   :category: environment
   :description: First skill

   .. code-block:: bash

      echo first

   .. agent-skill-expected-output::

      first

.. agent-skill::
   :id: second-skill
   :context: breeze
   :category: testing
   :description: Second skill

   .. code-block:: bash

      echo second

   .. agent-skill-expected-output::

      second
"""
    skills = parse_skills(rst)
    assert len(skills) == 2
    assert skills[0]["id"] == "first-skill"
    assert skills[1]["id"] == "second-skill"
