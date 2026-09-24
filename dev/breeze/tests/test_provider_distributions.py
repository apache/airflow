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

import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

from airflow_breeze.prepare_providers import provider_distributions
from airflow_breeze.prepare_providers.provider_distributions import (
    check_flit_worktree_compatibility,
)

# Rich output is wrapped to the terminal width and peppered with ANSI escapes;
# strip both when asserting on message contents.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(out: str, err: str) -> str:
    return " ".join(_ANSI_RE.sub("", out + err).split())


@pytest.fixture
def healthy_worktree_root(tmp_path: Path) -> Path:
    """A worktree whose `gitdir:` pointer targets a real directory."""
    gitdir = tmp_path / "main_repo" / ".git" / "worktrees" / "feature"
    gitdir.mkdir(parents=True)
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / ".git").write_text(f"gitdir: {gitdir}\n")
    return worktree


@pytest.fixture
def broken_worktree_root(tmp_path: Path) -> Path:
    """A worktree whose `gitdir:` pointer targets a path that does not exist.

    This is the Breeze-in-Docker shape: the worktree directory is mounted
    into the container but the absolute `gitdir:` path is not.
    """
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    (worktree / ".git").write_text("gitdir: /nonexistent/main/.git/worktrees/feature\n")
    return worktree


@pytest.fixture
def malformed_git_file_root(tmp_path: Path) -> Path:
    """`.git` is a file but the content is not a `gitdir:` pointer."""
    (tmp_path / ".git").write_text("this is not a git pointer\n")
    return tmp_path


@pytest.fixture
def plain_checkout_root(tmp_path: Path) -> Path:
    (tmp_path / ".git").mkdir()
    return tmp_path


@pytest.mark.parametrize("dist_format", ["sdist", "both"])
def test_healthy_worktree_blocks_sdist_build(healthy_worktree_root: Path, dist_format: str) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", healthy_worktree_root):
        with pytest.raises(SystemExit) as exc_info:
            check_flit_worktree_compatibility(dist_format)
        assert exc_info.value.code == 1


def test_healthy_worktree_allows_wheel_build(healthy_worktree_root: Path) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", healthy_worktree_root):
        check_flit_worktree_compatibility("wheel")


@pytest.mark.parametrize("dist_format", ["sdist", "both"])
def test_broken_gitdir_pointer_exits_with_docker_hint(
    broken_worktree_root: Path, capsys: pytest.CaptureFixture[str], dist_format: str
) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", broken_worktree_root):
        with pytest.raises(SystemExit) as exc_info:
            check_flit_worktree_compatibility(dist_format)
        assert exc_info.value.code == 1
    # The Docker-specific message must surface — the broken pointer is the
    # signal we use to tell the user that this is the container-mount case.
    captured = capsys.readouterr()
    output = _plain(captured.out, captured.err)
    assert "Docker" in output
    assert "does not exist" in output


def test_broken_gitdir_still_allows_wheel_build(broken_worktree_root: Path) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", broken_worktree_root):
        check_flit_worktree_compatibility("wheel")


@pytest.mark.parametrize("dist_format", ["sdist", "both"])
def test_malformed_git_file_exits(malformed_git_file_root: Path, dist_format: str) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", malformed_git_file_root):
        with pytest.raises(SystemExit) as exc_info:
            check_flit_worktree_compatibility(dist_format)
        assert exc_info.value.code == 1


def test_malformed_git_file_allows_wheel_build(malformed_git_file_root: Path) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", malformed_git_file_root):
        check_flit_worktree_compatibility("wheel")


def test_plain_checkout_allows_all_formats(plain_checkout_root: Path) -> None:
    with patch.object(provider_distributions, "AIRFLOW_ROOT_PATH", plain_checkout_root):
        check_flit_worktree_compatibility("sdist")
        check_flit_worktree_compatibility("both")
        check_flit_worktree_compatibility("wheel")


@pytest.mark.parametrize("distribution_format", ["wheel", "sdist", "both"])
@pytest.mark.parametrize("build_fails", [False, True])
def test_flit_provider_build_disables_network(tmp_path: Path, distribution_format: str, build_fails: bool):
    with (
        patch.object(
            provider_distributions, "get_provider_distributions_metadata", autospec=True
        ) as metadata,
        patch.object(provider_distributions, "get_provider_details", autospec=True) as details,
        patch.object(provider_distributions, "run_command", autospec=True) as run_command,
    ):
        metadata.return_value = {"duckdb": {"build-system": "flit_core"}}
        details.return_value.source_date_epoch = 1788600000
        if build_fails:
            run_command.side_effect = subprocess.CalledProcessError(1, "flit")
            with pytest.raises(provider_distributions.PrepareReleasePackageErrorBuildingPackageException):
                provider_distributions.build_provider_distribution("duckdb", tmp_path, distribution_format)
        else:
            provider_distributions.build_provider_distribution("duckdb", tmp_path, distribution_format)

    command = [sys.executable, "-m", "flit", "build", "--use-vcs"]
    if distribution_format == "sdist":
        command.extend(["--format", "sdist"])
    run_command.assert_called_once_with(
        command,
        check=True,
        cwd=tmp_path,
        env={"SOURCE_DATE_EPOCH": "1788600000", "FLIT_NO_NETWORK": "1"},
    )
