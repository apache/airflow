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

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[3] / "ci/prek/check_moved_docs_redirects.py"


def test_hook_is_executable():
    assert os.access(HOOK, os.X_OK)


def run_git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True)


def write_file(repo: Path, path: str, text: str) -> None:
    file = repo / path
    file.parent.mkdir(parents=True, exist_ok=True)
    file.write_text(text, encoding="utf-8")


def run_hook(
    repo: Path, *filenames: str, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    git_env = {
        key: value
        for key, value in os.environ.items()
        if key not in {"PRE_COMMIT_FROM_REF", "PRE_COMMIT_TO_REF"}
    }
    return subprocess.run(
        [sys.executable, str(HOOK), *filenames],
        cwd=repo,
        env={**git_env, **(env or {})},
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    run_git(tmp_path, "init", "-q")
    run_git(tmp_path, "config", "user.email", "test@example.com")
    run_git(tmp_path, "config", "user.name", "Test")
    for path in (
        "airflow-core/docs/old.rst",
        "airflow-ctl/docs/old.rst",
        "chart/docs/old.rst",
        "providers/apache/spark/docs/old.rst",
        "docker-stack-docs/old.rst",
        "providers-summary-docs/old.rst",
        "contributing-docs/old.rst",
        "misc/old.txt",
    ):
        write_file(tmp_path, path, f"Content for {path}\n")
    for path in ("airflow-core/docs/redirects.txt", "chart/docs/redirects.txt"):
        write_file(tmp_path, path, "# Redirects\n")
    run_git(tmp_path, "add", ".")
    run_git(tmp_path, "-c", "core.hooksPath=/dev/null", "commit", "-qm", "initial")
    return tmp_path


def test_rename_without_redirect_fails(repo: Path):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")

    result = run_hook(repo, "airflow-core/docs/new.rst")

    assert result.returncode == 1
    assert "airflow-core/docs/redirects.txt" in result.stdout
    assert "old.rst" in result.stdout


def test_rename_with_staged_redirect_passes(repo: Path):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")
    write_file(repo, "airflow-core/docs/redirects.txt", "# Redirects\n\nold.rst new.rst\n")
    run_git(repo, "add", "airflow-core/docs/redirects.txt")

    result = run_hook(repo, "airflow-core/docs/new.rst")

    assert result.returncode == 0


def test_move_with_existing_redirect_passes(repo: Path):
    write_file(repo, "airflow-core/docs/redirects.txt", "old.rst nested/new.rst\n")
    run_git(repo, "add", "airflow-core/docs/redirects.txt")
    run_git(repo, "-c", "core.hooksPath=/dev/null", "commit", "-qm", "existing redirect")
    (repo / "airflow-core/docs/nested").mkdir()
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/nested/new.rst")

    result = run_hook(repo, "airflow-core/docs/nested/new.rst")

    assert result.returncode == 0


def test_nested_move_without_redirect_fails(repo: Path):
    (repo / "airflow-core/docs/nested").mkdir()
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/nested/new.rst")

    result = run_hook(repo)

    assert result.returncode == 1
    assert "Missing redirect for airflow-core/docs/old.rst" in result.stdout


@pytest.mark.parametrize("change", ["non-doc-rename", "doc-deletion", "non-doc-tree"])
def test_unrelated_changes_are_ignored(repo: Path, change: str):
    if change == "non-doc-rename":
        run_git(repo, "mv", "misc/old.txt", "misc/new.txt")
    elif change == "doc-deletion":
        run_git(repo, "rm", "airflow-core/docs/old.rst")
    else:
        run_git(repo, "mv", "contributing-docs/old.rst", "contributing-docs/new.rst")

    result = run_hook(repo)

    assert result.returncode == 0
    assert result.stdout == ""


@pytest.mark.parametrize(("redirect_tree", "expected"), [("chart/docs", 1), ("airflow-core/docs", 0)])
def test_cross_tree_move_uses_source_tree(repo: Path, redirect_tree: str, expected: int):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "chart/docs/new.rst")
    write_file(repo, f"{redirect_tree}/redirects.txt", "old.rst ../chart/new.rst\n")
    run_git(repo, "add", f"{redirect_tree}/redirects.txt")

    result = run_hook(repo, "chart/docs/new.rst")

    assert result.returncode == expected
    if expected:
        assert "airflow-core/docs/redirects.txt" in result.stdout


@pytest.mark.parametrize(
    "tree",
    [
        "airflow-ctl/docs",
        "chart/docs",
        "providers/apache/spark/docs",
        "docker-stack-docs",
        "providers-summary-docs",
    ],
)
def test_docs_tree_requires_redirect(repo: Path, tree: str):
    run_git(repo, "mv", f"{tree}/old.rst", f"{tree}/new.rst")

    result = run_hook(repo, f"{tree}/new.rst")

    assert result.returncode == 1
    assert f"{tree}/redirects.txt" in result.stdout


def test_unstaged_redirect_does_not_cover_staged_move(repo: Path):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")
    write_file(repo, "airflow-core/docs/redirects.txt", "old.rst new.rst\n")

    result = run_hook(repo)

    assert result.returncode == 1


def test_comment_or_missing_target_is_not_a_redirect(repo: Path):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")
    write_file(repo, "airflow-core/docs/redirects.txt", "# old.rst new.rst\nold.rst\nold.rst\tnew.rst\n")
    run_git(repo, "add", "airflow-core/docs/redirects.txt")

    result = run_hook(repo)

    assert result.returncode == 1


def test_inline_comment_is_not_a_redirect_target(repo: Path):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")
    write_file(repo, "airflow-core/docs/redirects.txt", "old.rst # TODO\n")
    run_git(repo, "add", "airflow-core/docs/redirects.txt")

    result = run_hook(repo)

    assert result.returncode == 1
    assert "Missing redirect for airflow-core/docs/old.rst" in result.stdout


@pytest.mark.parametrize(
    ("redirects", "expected"),
    [("old.rst new.rst\nold.rst\n", 1), ("old.rst\nold.rst new.rst\n", 0)],
)
def test_last_redirect_entry_wins(repo: Path, redirects: str, expected: int):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")
    write_file(repo, "airflow-core/docs/redirects.txt", redirects)
    run_git(repo, "add", "airflow-core/docs/redirects.txt")

    result = run_hook(repo)

    assert result.returncode == expected


def test_git_show_failure_with_partial_output_fails_closed(repo: Path, tmp_path: Path):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")
    real_git = shutil.which("git")
    shim_dir = tmp_path / "bin"
    shim_dir.mkdir()
    shim = shim_dir / "git"
    shim.write_text(
        '#!/bin/sh\nif [ "$1" = "show" ] && [ "$2" = ":airflow-core/docs/redirects.txt" ]; then\n'
        '    printf "old.rst new.rst\\n"\n    exit 128\nfi\n'
        f'exec "{real_git}" "$@"\n',
        encoding="utf-8",
    )
    shim.chmod(0o755)

    result = run_hook(repo, env={"PATH": f"{shim_dir}{os.pathsep}{os.environ['PATH']}"})

    assert result.returncode == 1
    assert "redirects.txt" in result.stderr
    assert result.stdout == ""


def test_git_failure_fails_closed(tmp_path: Path):
    result = run_hook(tmp_path)

    assert result.returncode == 1
    assert "git" in result.stderr.lower()


def test_git_executable_missing_fails_closed(repo: Path):
    result = run_hook(repo, env={"PATH": ""})

    assert result.returncode == 1
    assert "git" in result.stderr.lower()


def test_invalid_ref_fails_closed(repo: Path):
    result = run_hook(repo, env={"PRE_COMMIT_FROM_REF": "missing-ref", "PRE_COMMIT_TO_REF": "HEAD"})

    assert result.returncode == 1
    assert "git" in result.stderr.lower()


def test_rename_with_spaces_is_detected(repo: Path):
    write_file(repo, "airflow-core/docs/old page.rst", "A page with spaces\n")
    run_git(repo, "add", "airflow-core/docs/old page.rst")
    run_git(repo, "-c", "core.hooksPath=/dev/null", "commit", "-qm", "add page")
    run_git(repo, "mv", "airflow-core/docs/old page.rst", "airflow-core/docs/new page.rst")
    shim_dir = repo / "bin"
    shim_dir.mkdir()
    show_marker = repo / "git-show-called"
    shim = shim_dir / "git"
    shim.write_text(
        '#!/bin/sh\nif [ "$1" = show ]; then\n'
        f"    : > {shlex.quote(str(show_marker))}\nfi\n"
        f'exec "{shutil.which("git")}" "$@"\n',
        encoding="utf-8",
    )
    shim.chmod(0o755)

    result = run_hook(repo, env={"PATH": f"{shim_dir}{os.pathsep}{os.environ['PATH']}"})

    assert result.returncode == 1
    assert "old page.rst" in result.stdout
    assert "cannot be represented" in result.stdout
    assert "Missing redirect" not in result.stdout
    assert not show_marker.exists()


def test_repo_root_ending_in_space(tmp_path: Path):
    repo = tmp_path / "repo "
    repo.mkdir()
    run_git(repo, "init", "-q")
    run_git(repo, "config", "user.email", "test@example.com")
    run_git(repo, "config", "user.name", "Test")
    write_file(repo, "airflow-core/docs/old.rst", "A page\n")
    write_file(repo, "airflow-core/docs/redirects.txt", "old.rst new.rst\n")
    run_git(repo, "add", ".")
    run_git(repo, "-c", "core.hooksPath=/dev/null", "commit", "-qm", "initial")
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")

    result = run_hook(repo)

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(("redirect_at_target", "expected"), [(False, 1), (True, 0)])
def test_ref_range_checks_renames_and_redirects_at_target(
    repo: Path, redirect_at_target: bool, expected: int
):
    run_git(repo, "mv", "airflow-core/docs/old.rst", "airflow-core/docs/new.rst")
    if redirect_at_target:
        write_file(repo, "airflow-core/docs/redirects.txt", "old.rst new.rst\n")
        run_git(repo, "add", "airflow-core/docs/redirects.txt")
    run_git(repo, "-c", "core.hooksPath=/dev/null", "commit", "-qm", "move")
    text = "# Redirects\n" if redirect_at_target else "old.rst new.rst\n"
    write_file(repo, "airflow-core/docs/redirects.txt", text)
    run_git(repo, "add", "airflow-core/docs/redirects.txt")
    run_git(repo, "-c", "core.hooksPath=/dev/null", "commit", "-qm", "change redirect")

    result = run_hook(repo, env={"PRE_COMMIT_FROM_REF": "HEAD~2", "PRE_COMMIT_TO_REF": "HEAD~1"})

    assert result.returncode == expected
