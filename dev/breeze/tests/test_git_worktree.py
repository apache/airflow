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

from pathlib import Path
from unittest import mock

import pytest

from airflow_breeze.utils.path_utils import get_main_git_dir_for_worktree


class TestGetMainGitDirForWorktree:
    """Tests for get_main_git_dir_for_worktree detection."""

    def test_returns_none_when_dot_git_is_directory(self, tmp_path):
        """Standard clone: .git is a directory, not a worktree."""
        git_dir = tmp_path / ".git"
        git_dir.mkdir()
        with mock.patch("airflow_breeze.utils.path_utils.AIRFLOW_ROOT_PATH", tmp_path):
            assert get_main_git_dir_for_worktree() is None

    def test_returns_none_when_dot_git_missing(self, tmp_path):
        """No .git at all — not a git repo."""
        with mock.patch("airflow_breeze.utils.path_utils.AIRFLOW_ROOT_PATH", tmp_path):
            assert get_main_git_dir_for_worktree() is None

    def test_returns_none_when_dot_git_file_without_gitdir_prefix(self, tmp_path):
        """.git file exists but does not start with 'gitdir: '."""
        git_file = tmp_path / ".git"
        git_file.write_text("something unexpected\n")
        with mock.patch("airflow_breeze.utils.path_utils.AIRFLOW_ROOT_PATH", tmp_path):
            assert get_main_git_dir_for_worktree() is None

    def test_absolute_gitdir_path(self, tmp_path):
        """Worktree with an absolute gitdir path resolves to the main .git directory."""
        # Simulate: /main-repo/.git/worktrees/my-worktree
        main_repo = tmp_path / "main-repo"
        main_git = main_repo / ".git"
        worktree_gitdir = main_git / "worktrees" / "my-worktree"
        worktree_gitdir.mkdir(parents=True)

        worktree_dir = tmp_path / "my-worktree"
        worktree_dir.mkdir()
        (worktree_dir / ".git").write_text(f"gitdir: {worktree_gitdir}\n")

        with mock.patch("airflow_breeze.utils.path_utils.AIRFLOW_ROOT_PATH", worktree_dir):
            result = get_main_git_dir_for_worktree()
            assert result is not None
            assert result == main_git

    def test_relative_gitdir_path(self, tmp_path):
        """Worktree with a relative gitdir path is resolved correctly."""
        # Simulate: main-repo/.git/worktrees/my-worktree
        main_repo = tmp_path / "main-repo"
        main_git = main_repo / ".git"
        worktree_gitdir = main_git / "worktrees" / "my-worktree"
        worktree_gitdir.mkdir(parents=True)

        worktree_dir = tmp_path / "my-worktree"
        worktree_dir.mkdir()
        # Write a relative path from worktree_dir to worktree_gitdir
        relative_gitdir = Path("../main-repo/.git/worktrees/my-worktree")
        (worktree_dir / ".git").write_text(f"gitdir: {relative_gitdir}\n")

        with mock.patch("airflow_breeze.utils.path_utils.AIRFLOW_ROOT_PATH", worktree_dir):
            result = get_main_git_dir_for_worktree()
            assert result is not None
            assert result.resolve() == main_git.resolve()

    @pytest.mark.parametrize(
        "gitdir_content",
        [
            "gitdir: {path}/worktrees/wt\n",
            "gitdir: {path}/worktrees/wt",
            "gitdir:  {path}/worktrees/wt  \n",
        ],
        ids=["trailing-newline", "no-trailing-newline", "extra-whitespace"],
    )
    def test_strips_whitespace_from_gitdir(self, tmp_path, gitdir_content):
        """Whitespace and trailing newlines are stripped from the gitdir content."""
        main_repo = tmp_path / "main-repo"
        main_git = main_repo / ".git"
        (main_git / "worktrees" / "wt").mkdir(parents=True)

        content = gitdir_content.format(path=main_git)
        (tmp_path / ".git").write_text(content)

        with mock.patch("airflow_breeze.utils.path_utils.AIRFLOW_ROOT_PATH", tmp_path):
            result = get_main_git_dir_for_worktree()
            assert result is not None
            assert result == main_git
