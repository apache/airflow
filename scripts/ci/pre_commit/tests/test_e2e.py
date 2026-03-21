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
"""End-to-end tests for agent-skills extraction and command routing."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

PRE_COMMIT_DIR = Path(__file__).resolve().parents[1]
CI_DIR = Path(__file__).resolve().parents[2]
REPO_ROOT = Path(__file__).resolve().parents[4]
sys.path.insert(0, str(CI_DIR))

from agent_skills import breeze_context  # noqa: E402


def _extract_skills_to_file(skills_file: Path) -> subprocess.CompletedProcess[str]:
    extract_script = PRE_COMMIT_DIR / "extract_agent_skills.py"
    return subprocess.run(
        [
            sys.executable,
            str(extract_script),
            "--docs-file",
            str(REPO_ROOT / "AGENTS.md"),
            "--docs-dir",
            str(REPO_ROOT / "contributing-docs"),
            "--output",
            str(skills_file),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )


def test_full_pipeline_rst_to_command(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    rst_file = REPO_ROOT / "contributing-docs" / "03_contributors_quick_start.rst"
    rst_text = rst_file.read_text(encoding="utf-8")
    assert ".. agent-skill::" in rst_text
    assert ":id: run-tests-uv-first" in rst_text

    skills_file = tmp_path / "skills.json"
    extract_result = _extract_skills_to_file(skills_file)
    assert extract_result.returncode == 0, extract_result.stderr
    assert skills_file.exists()

    monkeypatch.setattr(breeze_context, "SKILLS_JSON", skills_file)
    monkeypatch.setattr(breeze_context, "get_context", lambda: "host")
    cmd = breeze_context.get_command(
        "run-tests-uv-first",
        project="providers/apache/kafka",
        test_path="tests/foo.py",
    )
    assert cmd.splitlines()[0] == "uv run --project providers/apache/kafka pytest tests/foo.py -xvs"

    check_script = PRE_COMMIT_DIR / "extract_agent_skills.py"
    check_result = subprocess.run(
        [
            sys.executable,
            str(check_script),
            "--check",
            "--docs-file",
            str(REPO_ROOT / "AGENTS.md"),
            "--docs-dir",
            str(REPO_ROOT / "contributing-docs"),
            "--output",
            str(skills_file),
        ],
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )
    assert check_result.returncode == 0, check_result.stdout + check_result.stderr
    assert "OK: skills.json is in sync" in check_result.stdout


def test_wrong_context_returns_guidance(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    skills_file = tmp_path / "skills.json"
    extract_result = _extract_skills_to_file(skills_file)
    assert extract_result.returncode == 0, extract_result.stderr

    monkeypatch.setattr(breeze_context, "SKILLS_JSON", skills_file)
    monkeypatch.setattr(breeze_context, "get_context", lambda: "breeze")
    result = breeze_context.get_command(
        "run-tests-uv-first",
        project="providers/apache/kafka",
        test_path="tests/foo.py",
    )
    assert "requires context" in result
