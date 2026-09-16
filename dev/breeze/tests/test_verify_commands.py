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
import re
from subprocess import CompletedProcess
from unittest.mock import patch

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.verify_commands import get_changed_files_against, verify

ANSI = re.compile(r"\x1b\[[0-9;]*m")


@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    return_value=("airflow-core/docs/index.rst",),
)
def test_json_output_keeps_stdout_parseable(mock_files, monkeypatch):
    # GitHub Actions makes breeze print a "how to reproduce" banner after every command.
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_SHA", "0" * 40)
    result = CliRunner().invoke(verify, ["--json"], catch_exceptions=False)
    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert set(payload) == {
        "base_ref",
        "default_python_version",
        "full_tests_needed",
        "changed_files",
        "items",
    }
    assert payload["changed_files"] == ["airflow-core/docs/index.rst"]
    assert set(payload["items"][0]) == {"kind", "command", "runs_in"}
    assert payload["items"][0]["kind"] == "prek"


@patch(
    "airflow_breeze.commands.verify_commands.run_command",
    return_value=CompletedProcess(
        args=[], returncode=128, stdout="", stderr="fatal: Not a valid object name"
    ),
)
def test_unknown_base_ref_is_a_clean_usage_error(mock_run):
    result = CliRunner().invoke(verify, ["--base-ref", "no-such-branch"])
    assert result.exit_code == 1
    rendered = " ".join(ANSI.sub("", result.output).replace("│", " ").split())
    assert "git merge-base failed for base ref 'no-such-branch': fatal: Not a valid object name" in rendered


@patch(
    "airflow_breeze.commands.verify_commands.get_changed_files_against",
    return_value=("airflow-core/docs/index.rst",),
)
def test_selective_checks_narration_is_hidden_unless_verbose(mock_files):
    result = CliRunner().invoke(verify, [], catch_exceptions=False)
    assert result.exit_code == 0
    assert "FileGroupForCi" not in result.output
    assert "FileGroupForCi" not in result.stderr


@pytest.mark.parametrize(
    ("files", "expanded"),
    [
        (("airflow-core/docs/index.rst",), False),
        (("dev/breeze/src/airflow_breeze/breeze.py",), True),
    ],
)
def test_full_suite_expansion_is_explained(files: tuple[str, ...], expanded: bool):
    with patch("airflow_breeze.commands.verify_commands.get_changed_files_against", return_value=files):
        result = CliRunner().invoke(verify, [], catch_exceptions=False)
        full = CliRunner().invoke(verify, ["--full"], catch_exceptions=False)
    assert result.exit_code == 0
    assert ("CI also runs the full suite" in " ".join(result.output.split())) is expanded
    assert "CI also runs the full suite" not in " ".join(full.output.split())
    assert ("breeze testing core-tests" in " ".join(full.output.split())) is expanded
    assert "breeze testing core-tests" not in " ".join(result.output.split())


@patch("airflow_breeze.commands.verify_commands.run_command")
def test_changed_files_list_renames_like_ci_diff_tree(mock_run):
    mock_run.side_effect = [
        CompletedProcess(args=[], returncode=0, stdout="abc123\n", stderr=""),
        CompletedProcess(args=[], returncode=0, stdout="new.py\nold.py\n", stderr=""),
        CompletedProcess(args=[], returncode=0, stdout="untracked.py\n", stderr=""),
    ]
    assert get_changed_files_against("main") == ("new.py", "old.py", "untracked.py")
    assert mock_run.call_args_list[1].args[0] == ["git", "diff", "--name-only", "--no-renames", "abc123"]
