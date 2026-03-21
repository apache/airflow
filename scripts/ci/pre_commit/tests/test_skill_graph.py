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
"""Tests for skill_graph.py."""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT_DIR = Path(__file__).resolve().parent.parent


def _run_skill_graph(skills_json: Path, output_json: Path | None = None) -> subprocess.CompletedProcess:
    cmd = [sys.executable, str(SCRIPT_DIR / "skill_graph.py"), "--skills-json", str(skills_json)]
    if output_json is not None:
        cmd.extend(["--output", str(output_json)])
    return subprocess.run(cmd, capture_output=True, text=True)


def test_graph_built_correctly(tmp_path):
    """Given skills.json with known prereqs, assert edges are correct."""
    skills = {
        "version": "1.0",
        "skills": [
            {"id": "root-skill", "context": "host", "category": "environment", "prereqs": "docker", "description": "Root", "command": "echo root", "expected_output": "root"},
            {"id": "child-skill", "context": "host", "category": "testing", "prereqs": "root-skill", "description": "Child", "command": "echo child", "expected_output": "child"},
        ],
    }
    skills_file = tmp_path / "skills.json"
    skills_file.write_text(json.dumps(skills))
    out_file = tmp_path / "skill_graph.json"
    result = _run_skill_graph(skills_file, out_file)
    assert result.returncode == 0
    data = json.loads(out_file.read_text())
    assert "nodes" in data
    assert "edges" in data
    edges = data["edges"]
    assert len(edges) == 1
    assert edges[0]["from"] == "child-skill"
    assert edges[0]["to"] == "root-skill"
    assert edges[0]["type"] == "requires"


def test_circular_dependency_detected(tmp_path):
    """Given skill A requires B and B requires A, assert exit code 1."""
    skills = {
        "version": "1.0",
        "skills": [
            {"id": "skill-a", "context": "host", "category": "environment", "prereqs": "skill-b", "description": "A", "command": "echo a", "expected_output": "a"},
            {"id": "skill-b", "context": "host", "category": "testing", "prereqs": "skill-a", "description": "B", "command": "echo b", "expected_output": "b"},
        ],
    }
    skills_file = tmp_path / "skills.json"
    skills_file.write_text(json.dumps(skills))
    out_file = tmp_path / "skill_graph.json"
    result = _run_skill_graph(skills_file, out_file)
    assert result.returncode == 1
    assert "Circular dependency" in result.stderr or "circular" in result.stderr.lower()


def test_root_nodes_have_no_prereqs(tmp_path):
    """Assert setup-breeze-environment has no outgoing edges (no skill prereqs)."""
    # Use structure matching real skills: setup-breeze-environment has no skill prereqs
    skills = {
        "version": "1.0",
        "skills": [
            {"id": "setup-breeze-environment", "context": "host", "category": "environment", "prereqs": "docker, python>=3.9", "description": "Start Breeze", "command": "breeze start", "expected_output": "ok"},
            {"id": "run-single-test", "context": "host", "category": "testing", "prereqs": "setup-breeze-environment", "description": "Run test", "command": "pytest", "expected_output": "PASSED"},
        ],
    }
    skills_file = tmp_path / "skills.json"
    skills_file.write_text(json.dumps(skills))
    out_file = tmp_path / "skill_graph.json"
    result = _run_skill_graph(skills_file, out_file)
    assert result.returncode == 0
    data = json.loads(out_file.read_text())
    edges = data["edges"]
    # setup-breeze-environment must not be the "from" of any edge (it has no skill prereqs)
    from_ids = {e["from"] for e in edges}
    assert "setup-breeze-environment" not in from_ids
