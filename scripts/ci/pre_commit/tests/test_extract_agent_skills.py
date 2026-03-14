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

import subprocess
import sys
from pathlib import Path

import pytest

# Add parent so we can import extract_agent_skills
SCRIPT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(SCRIPT_DIR))

from extract_agent_skills import parse_skills, validate_skill

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
