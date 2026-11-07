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

import pytest
from tools.breeze_context import (
    ContextInfo,
    ContextType,
    detect_context,
    find_repo_root,
    get_test_command,
    infer_uv_project,
    is_git_worktree,
    main,
)


@pytest.fixture
def repo_root() -> Path:
    return find_repo_root()


def test_detect_context_host(repo_root: Path):
    ctx = detect_context(env={}, repo_root=repo_root)

    assert ctx.context == ContextType.HOST.value
    assert ctx.in_breeze is False
    assert ctx.in_container is False
    assert ctx.can_run_breeze is True
    assert ctx.repo_root == str(repo_root)


def test_detect_context_breeze(repo_root: Path):
    ctx = detect_context(env={"BREEZE": "true"}, repo_root=repo_root)

    assert ctx.context == ContextType.BREEZE.value
    assert ctx.in_breeze is True
    assert ctx.in_container is True
    assert ctx.can_run_breeze is False


@pytest.mark.parametrize("var", ["CODESPACES", "REMOTE_CONTAINERS"])
def test_detect_context_devcontainer(repo_root: Path, var: str):
    ctx = detect_context(env={var: "true"}, repo_root=repo_root)

    assert ctx.context == ContextType.DEVCONTAINER.value
    assert ctx.in_breeze is False
    assert ctx.in_container is True
    assert ctx.can_run_breeze is True


def test_is_git_worktree(tmp_path: Path):
    regular_repo = tmp_path / "repo"
    regular_repo.mkdir()
    (regular_repo / ".git").mkdir()
    assert is_git_worktree(regular_repo) is False

    worktree_repo = tmp_path / "worktree"
    worktree_repo.mkdir()
    (worktree_repo / ".git").write_text("gitdir: /path/to/main/.git/worktrees/wt1")
    assert is_git_worktree(worktree_repo) is True


@pytest.mark.parametrize(
    ("path", "expected_project"),
    [
        ("airflow-core/tests/models/test_dag.py", "airflow-core"),
        ("task-sdk/src/airflow/sdk/definitions/dag.py", "task-sdk"),
        ("providers/amazon/tests/unit/amazon/aws/test_s3.py", "providers/amazon"),
        ("providers/apache/beam/tests/test_beam.py", "providers/apache/beam"),
        ("scripts/tests/tools/test_breeze_context.py", "scripts"),
        ("airflow-ctl/tests/test_cli.py", "airflow-ctl"),
        ("shared/logging/src/logger.py", "shared/logging"),
        ("dev/breeze/src/airflow_breeze/main.py", "dev/breeze"),
        ("unknown/path/to/something.txt", None),
    ],
)
def test_infer_uv_project(repo_root: Path, path: str, expected_project: str | None):
    project = infer_uv_project(path, repo_root=repo_root)
    assert project == expected_project


def test_infer_uv_project_absolute_path(repo_root: Path):
    abs_path = repo_root / "airflow-core" / "tests" / "test_sample.py"
    assert infer_uv_project(abs_path, repo_root=repo_root) == "airflow-core"

    outside_path = Path("/tmp/outside_repo/test.py")
    assert infer_uv_project(outside_path, repo_root=repo_root) is None


def test_get_test_command_in_breeze(repo_root: Path):
    ctx = ContextInfo(
        context="breeze",
        in_breeze=True,
        in_container=True,
        can_run_breeze=False,
        repo_root=str(repo_root),
        is_worktree=False,
    )
    rec = get_test_command("airflow-core/tests/models/test_dag.py", context=ctx)

    assert rec.primary_command == "pytest airflow-core/tests/models/test_dag.py -xvs"
    assert rec.fallback_command == "pytest airflow-core/tests/models/test_dag.py --skip-db-tests -xvs"
    assert rec.warning is not None
    assert "nested Docker will fail" in rec.warning


def test_get_test_command_on_host_with_project(repo_root: Path):
    ctx = ContextInfo(
        context="host",
        in_breeze=False,
        in_container=False,
        can_run_breeze=True,
        repo_root=str(repo_root),
        is_worktree=False,
    )
    rec = get_test_command("airflow-core/tests/models/test_dag.py", context=ctx)

    assert rec.project == "airflow-core"
    assert (
        rec.primary_command
        == "uv run --project airflow-core pytest airflow-core/tests/models/test_dag.py -xvs"
    )
    assert rec.fallback_command == "breeze run pytest airflow-core/tests/models/test_dag.py -xvs"
    assert rec.warning is None


def test_get_test_command_on_host_without_project(repo_root: Path):
    ctx = ContextInfo(
        context="host",
        in_breeze=False,
        in_container=False,
        can_run_breeze=True,
        repo_root=str(repo_root),
        is_worktree=False,
    )
    rec = get_test_command("nonexistent/unknown_test.py", context=ctx)

    assert rec.project is None
    assert rec.primary_command == "breeze run pytest nonexistent/unknown_test.py -xvs"
    assert rec.fallback_command == ""


def test_cli_json_output(capsys: pytest.CaptureFixture):
    exit_code = main(["--json", "--path", "airflow-core/tests/models/test_dag.py"])
    assert exit_code == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert "context" in payload
    assert "recommendation" in payload
    assert payload["recommendation"]["project"] == "airflow-core"


def test_cli_check_breeze(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("BREEZE", raising=False)
    assert main(["--check-breeze"]) == 1

    monkeypatch.setenv("BREEZE", "true")
    assert main(["--check-breeze"]) == 0
