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
import os
import re
import types
from unittest import mock
from unittest.mock import patch

import pytest
from git import Repo
from git.exc import GitCommandError, InvalidGitRepositoryError, NoSuchPathError

from airflow.dag_processing.bundles.base import get_bundle_storage_root_path
from airflow.models import Connection
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.git.bundles.git import GitDagBundle
from airflow.providers.git.hooks.git import GitHook

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.version_compat import AIRFLOW_V_3_1_PLUS


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("dag_processor", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


GIT_DEFAULT_BRANCH = "main"

AIRFLOW_HTTPS_URL = "https://github.com/apache/airflow.git"
ACCESS_TOKEN = "my_access_token"
CONN_HTTPS = "my_git_conn"
CONN_ONLY_PATH = "my_git_conn_only_path"
CONN_NO_REPO_URL = "my_git_conn_no_repo_url"


@pytest.fixture
def git_repo(tmp_path_factory):
    directory = tmp_path_factory.mktemp("repo")
    repo = Repo.init(directory)
    repo.git.symbolic_ref("HEAD", f"refs/heads/{GIT_DEFAULT_BRANCH}")
    file_path = directory / "test_dag.py"
    with open(file_path, "w") as f:
        f.write("hello world")
    repo.index.add([file_path])
    repo.index.commit("Initial commit")
    return (directory, repo)


def assert_repo_is_closed(bundle: GitDagBundle):
    # When .git was pruned, repo is cleared and there is nothing to close
    if getattr(bundle, "repo", None) is None:
        return
    # cat-file processes get left around if the repo is not closed, so check it was
    assert bundle.repo.git.cat_file_all is None
    assert bundle.bare_repo.git.cat_file_all is None
    assert bundle.repo.git.cat_file_header is None
    assert bundle.bare_repo.git.cat_file_header is None


class TestGitDagBundle:
    @classmethod
    def teardown_class(cls) -> None:
        return

    # TODO: Potential performance issue, converted setup_class to a setup_connections function level fixture
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db, request):
        # Skip setup for tests that need to create their own connections
        if request.function.__name__ in ["test_view_url", "test_view_url_subdir"]:
            return

        create_connection_without_db(
            Connection(
                conn_id="git_default",
                host="git@github.com:apache/airflow.git",
                conn_type="git",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_HTTPS,
                host=AIRFLOW_HTTPS_URL,
                password=ACCESS_TOKEN,
                conn_type="git",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id=CONN_NO_REPO_URL,
                conn_type="git",
            )
        )

    def test_supports_versioning(self):
        assert GitDagBundle.supports_versioning is True

    def test_uses_dag_bundle_root_storage_path(self):
        bundle = GitDagBundle(name="test", tracking_ref=GIT_DEFAULT_BRANCH)
        base = get_bundle_storage_root_path()
        assert bundle.path.is_relative_to(base)

    def test_repo_url_overrides_connection_host_when_provided(self):
        bundle = GitDagBundle(name="test", tracking_ref=GIT_DEFAULT_BRANCH, repo_url="/some/other/repo")
        assert bundle.repo_url == "/some/other/repo"

    def test_https_access_token_repo_url_overrides_connection_host_when_provided(self):
        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_HTTPS,
            tracking_ref=GIT_DEFAULT_BRANCH,
            repo_url="https://github.com/apache/zzzairflow",
        )
        assert bundle.repo_url == f"https://user:{ACCESS_TOKEN}@github.com/apache/zzzairflow"

    def test_falls_back_to_connection_host_when_no_repo_url_provided(self):
        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref=GIT_DEFAULT_BRANCH)
        assert bundle.repo_url == bundle.hook.repo_url

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_get_current_version(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref=GIT_DEFAULT_BRANCH)

        bundle.initialize()

        assert bundle.get_current_version() == repo.head.commit.hexsha

        assert_repo_is_closed(bundle)

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_get_specific_version(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        # Add new file to the repo
        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        repo.index.commit("Another commit")

        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_HTTPS,
            version=starting_commit.hexsha,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle.initialize()

        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

        assert_repo_is_closed(bundle)

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_get_tag_version(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        # add tag
        repo.create_tag("test")

        # Add new file to the repo
        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        repo.index.commit("Another commit")

        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_HTTPS,
            version="test",
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle.initialize()
        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_get_latest(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        repo.index.commit("Another commit")

        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref=GIT_DEFAULT_BRANCH)
        bundle.initialize()

        assert bundle.get_current_version() != starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py", "new_test.py"} == files_in_repo

        assert_repo_is_closed(bundle)

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_removes_git_dir_for_versioned_bundle_by_default(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_HTTPS,
            version=starting_commit.hexsha,
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        bundle.initialize()

        assert not (bundle.repo_path / ".git").exists()

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

        assert_repo_is_closed(bundle)

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_keeps_git_dir_when_disabled(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_HTTPS,
            version=starting_commit.hexsha,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle.initialize()

        assert (bundle.repo_path / ".git").exists()
        assert bundle.get_current_version() == starting_commit.hexsha

        assert_repo_is_closed(bundle)

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_second_initialize_reuses_pruned_worktree_without_recloning(self, mock_githook, git_repo):
        """When version path exists without .git (pruned), second initialize() uses it and does not re-clone."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit
        version = starting_commit.hexsha
        bundle_name = "test_pruned_reuse"

        # First init: clone and prune (default)
        bundle1 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=version,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=True,
        )
        bundle1.initialize()
        assert not (bundle1.repo_path / ".git").exists()
        assert bundle1.get_current_version() == version
        version_path = bundle1.repo_path

        # Second init: same name and version; should detect pruned worktree and skip clone
        with patch.object(GitDagBundle, "_clone_repo_if_required") as mock_clone:
            bundle2 = GitDagBundle(
                name=bundle_name,
                git_conn_id=CONN_HTTPS,
                version=version,
                tracking_ref=GIT_DEFAULT_BRANCH,
                prune_dotgit_folder=True,
            )
            bundle2.initialize()
            mock_clone.assert_not_called()

        assert bundle2.repo_path == version_path
        assert bundle2.get_current_version() == version
        files_in_repo = {f.name for f in bundle2.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_second_initialize_skips_clone_when_local_repo_has_version(self, mock_githook, git_repo):
        """When the local repo already has the correct version checked out (with .git intact), skip re-cloning."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        version = repo.head.commit.hexsha
        bundle_name = "test_version_reuse"

        # Clone with prune_dotgit_folder=False so .git is preserved
        bundle1 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=version,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle1.initialize()
        assert (bundle1.repo_path / ".git").exists()
        assert bundle1.get_current_version() == version

        # Should detect local repo has correct version and skip clone
        with (
            patch.object(GitDagBundle, "_clone_bare_repo_if_required") as mock_bare_clone,
            patch.object(GitDagBundle, "_clone_repo_if_required") as mock_clone,
        ):
            bundle2 = GitDagBundle(
                name=bundle_name,
                git_conn_id=CONN_HTTPS,
                version=version,
                tracking_ref=GIT_DEFAULT_BRANCH,
                prune_dotgit_folder=False,
            )
            bundle2.initialize()
            mock_bare_clone.assert_not_called()
            mock_clone.assert_not_called()

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_initialize_with_different_versions_creates_separate_repos(self, mock_githook, git_repo):
        """Initializing the same bundle with different versions creates separate repos, each at the requested version."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        first_commit = repo.head.commit.hexsha

        # Add a second commit
        file_path = repo_path / "new_file.py"
        with open(file_path, "w") as f:
            f.write("new content")
        repo.index.add([file_path])
        repo.index.commit("Second commit")
        second_commit = repo.head.commit.hexsha

        bundle_name = "test_version_mismatch"

        # First init: clone at second_commit
        bundle1 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=second_commit,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle1.initialize()
        assert bundle1.get_current_version() == second_commit

        # Second init with first_commit: different version means different repo_path
        bundle2 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=first_commit,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle2.initialize()
        assert bundle2.get_current_version() == first_commit
        assert bundle1.repo_path != bundle2.repo_path

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_local_repo_has_version_returns_false_when_head_mismatches(self, mock_githook, git_repo):
        """When the local repo exists at the version path but HEAD doesn't match, _local_repo_has_version returns False."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        first_commit = repo.head.commit.hexsha

        # Add a second commit
        file_path = repo_path / "new_file.py"
        with open(file_path, "w") as f:
            f.write("new content")
        repo.index.add([file_path])
        repo.index.commit("Second commit")
        second_commit = repo.head.commit.hexsha

        bundle_name = "test_wrong_head"

        # Clone at second_commit with .git preserved
        bundle = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=second_commit,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle.initialize()
        assert bundle.get_current_version() == second_commit
        assert bundle._local_repo_has_version() is True

        # Mutate the cloned repo's HEAD to point at first_commit so HEAD != version
        cloned_repo = Repo(bundle.repo_path)
        cloned_repo.head.reset(first_commit, index=True, working_tree=True)
        cloned_repo.close()

        # Same bundle config but now HEAD doesn't match version — should return False
        assert bundle._local_repo_has_version() is False

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_skip_path_resolves_hexsha_when_version_is_tag(self, mock_githook, git_repo):
        """When the skip path is taken and version is a tag, get_current_version() returns the resolved hexsha."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        hexsha = repo.head.commit.hexsha
        repo.create_tag("v1.0")

        bundle_name = "test_tag_resolution"

        bundle1 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version="v1.0",
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle1.initialize()
        assert bundle1.get_current_version() == hexsha

        # Re-initialize: should take the skip path but still resolve to hexsha
        bundle2 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version="v1.0",
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        with (
            patch.object(GitDagBundle, "_clone_bare_repo_if_required") as mock_bare_clone,
            patch.object(GitDagBundle, "_clone_repo_if_required") as mock_clone,
        ):
            bundle2.initialize()
            mock_bare_clone.assert_not_called()
            mock_clone.assert_not_called()
        assert bundle2.get_current_version() == hexsha

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_skip_path_cleans_dirty_working_tree(self, mock_githook, git_repo):
        """The fast-path discards working-tree mutations left by previous task runs."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        version = repo.head.commit.hexsha
        bundle_name = "test_dirty_tree"

        bundle1 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=version,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle1.initialize()

        # Simulate a dirty tree: modify a tracked file and add an untracked file.
        tracked = bundle1.repo_path / "test_dag.py"
        tracked.write_text("mutated by a previous task")
        untracked = bundle1.repo_path / "leftover.txt"
        untracked.write_text("leftover")

        bundle2 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=version,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle2.initialize()

        # Tracked file is restored, untracked file is removed.
        assert tracked.read_text() != "mutated by a previous task"
        assert not untracked.exists()

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_skip_path_prunes_dotgit_when_config_flipped(self, mock_githook, git_repo):
        """If prune_dotgit_folder is True but a prior run left .git intact, the fast-path prunes it."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        version = repo.head.commit.hexsha
        bundle_name = "test_prune_flip"

        # First init keeps .git
        bundle1 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=version,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle1.initialize()
        assert (bundle1.repo_path / ".git").exists()

        # Second init flips config to prune; fast-path should honor it
        bundle2 = GitDagBundle(
            name=bundle_name,
            git_conn_id=CONN_HTTPS,
            version=version,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=True,
        )
        bundle2.initialize()
        assert not (bundle2.repo_path / ".git").exists()

    @pytest.mark.parametrize(
        "amend",
        [
            True,
            False,
        ],
    )
    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh(self, mock_githook, git_repo, amend):
        """Ensure that the bundle refresh works when tracking a branch, with a new commit and amending the commit"""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        with repo.config_writer() as writer:
            writer.set_value("user", "name", "Test User")
            writer.set_value("user", "email", "test@example.com")

        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref=GIT_DEFAULT_BRANCH)
        bundle.initialize()

        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        commit = repo.git.commit(amend=amend, message="Another commit")

        bundle.refresh()

        assert bundle.get_current_version()[:6] in commit

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py", "new_test.py"} == files_in_repo

        assert_repo_is_closed(bundle)

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_tag(self, mock_githook, git_repo):
        """Ensure that the bundle refresh works when tracking a tag"""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        # add tag
        repo.create_tag("test123")

        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="test123")
        bundle.initialize()
        assert bundle.get_current_version() == starting_commit.hexsha

        # Add new file to the repo
        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        commit = repo.index.commit("Another commit")

        # update tag
        repo.create_tag("test123", force=True)

        bundle.refresh()

        assert bundle.get_current_version() == commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py", "new_test.py"} == files_in_repo

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_tag_force_pushed_to_unrelated_commit(self, mock_githook, git_repo):
        """Ensure that refresh follows a tag moved to an unrelated commit."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        repo.create_tag("release")
        initial_commit = repo.head.commit

        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="release")
        bundle.initialize()
        assert bundle.get_current_version() == initial_commit.hexsha

        repo.git.checkout("--orphan", "new-branch")
        repo.git.rm("-rf", ".")

        new_file = repo_path / "new_file.py"
        new_file.write_text("unrelated content")
        repo.index.add([new_file])
        unrelated_commit = repo.index.commit("Unrelated commit")

        repo.create_tag("release", force=True)

        bundle.refresh()

        assert bundle.get_current_version() == unrelated_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"new_file.py"} == files_in_repo

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_branch_force_pushed_to_unrelated_commit(self, mock_githook, git_repo):
        """Ensure that refresh follows a branch force-pushed to an unrelated commit."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        # Create a second branch to track
        repo.create_head("release")
        initial_commit = repo.head.commit

        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="release")
        bundle.initialize()
        assert bundle.get_current_version() == initial_commit.hexsha

        # Force-push "release" to an orphan commit
        repo.git.checkout("--orphan", "temp-orphan")
        repo.git.rm("-rf", ".")

        new_file = repo_path / "branch_new.py"
        new_file.write_text("unrelated branch content")
        repo.index.add([new_file])
        unrelated_commit = repo.index.commit("Unrelated branch commit")

        # Point the release branch to this orphan commit
        repo.git.branch("-f", "release", unrelated_commit.hexsha)

        bundle.refresh()

        assert bundle.get_current_version() == unrelated_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"branch_new.py"} == files_in_repo

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_tag_moved_forward_and_backward(self, mock_githook, git_repo):
        """Ensure refresh follows a tag moved forward then backward across refreshes."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        commit_a = repo.head.commit
        repo.create_tag("moving")

        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="moving")
        bundle.initialize()
        assert bundle.get_current_version() == commit_a.hexsha

        # Move tag forward to commit B
        file_b = repo_path / "file_b.py"
        file_b.write_text("commit b content")
        repo.index.add([file_b])
        commit_b = repo.index.commit("Commit B")
        repo.create_tag("moving", force=True)

        bundle.refresh()
        assert bundle.get_current_version() == commit_b.hexsha
        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py", "file_b.py"} == files_in_repo

        # Move tag backward to commit A
        repo.create_tag("moving", ref=commit_a, force=True)

        bundle.refresh()
        assert bundle.get_current_version() == commit_a.hexsha
        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_after_force_push_does_not_reclone(self, mock_githook, git_repo):
        """Refresh after force-push must fetch+reset, never clone."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        repo.create_tag("release")
        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="release")
        bundle.initialize()

        # Force-push tag to orphan commit
        repo.git.checkout("--orphan", "orphan-branch")
        repo.git.rm("-rf", ".")
        new_file = repo_path / "orphan_file.py"
        new_file.write_text("orphan content")
        repo.index.add([new_file])
        unrelated_commit = repo.index.commit("Orphan commit")
        repo.create_tag("release", force=True)

        bare_repo_path = bundle.bare_repo_path
        working_repo_path = bundle.repo_path

        with mock.patch("airflow.providers.git.bundles.git.Repo.clone_from") as mock_clone:
            bundle.refresh()
            mock_clone.assert_not_called()

        # Repos were reused, not recreated
        assert bare_repo_path.exists()
        assert working_repo_path.exists()
        assert bundle.get_current_version() == unrelated_commit.hexsha

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_repeated_refreshes_after_force_push_stable(self, mock_githook, git_repo):
        """Repeated refreshes after a force-push remain stable."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        repo.create_tag("release")
        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="release")
        bundle.initialize()

        # Force-push tag to orphan commit
        repo.git.checkout("--orphan", "orphan-branch")
        repo.git.rm("-rf", ".")
        new_file = repo_path / "stable_file.py"
        new_file.write_text("stable content")
        repo.index.add([new_file])
        new_commit = repo.index.commit("Stable orphan commit")
        repo.create_tag("release", force=True)

        # First refresh
        bundle.refresh()
        assert bundle.get_current_version() == new_commit.hexsha
        files_after_first = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"stable_file.py"} == files_after_first

        # Second refresh (no upstream changes)
        bundle.refresh()
        assert bundle.get_current_version() == new_commit.hexsha
        files_after_second = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"stable_file.py"} == files_after_second

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_reinitialize_reuses_repos_after_force_push(self, mock_githook, git_repo):
        """Re-initialization with a new bundle object reuses existing repos after force-push."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        repo.create_tag("release")
        bundle1 = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="release")
        bundle1.initialize()

        # Force-push tag to orphan commit
        repo.git.checkout("--orphan", "orphan-branch")
        repo.git.rm("-rf", ".")
        new_file = repo_path / "reinit_file.py"
        new_file.write_text("reinit content")
        repo.index.add([new_file])
        new_commit = repo.index.commit("Reinit orphan commit")
        repo.create_tag("release", force=True)

        bundle1.refresh()
        assert bundle1.get_current_version() == new_commit.hexsha

        # Simulate DAG processor restart: new bundle object, same name
        bundle2 = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="release")
        with mock.patch("airflow.providers.git.bundles.git.Repo.clone_from") as mock_clone:
            bundle2.initialize()
            mock_clone.assert_not_called()

        assert bundle2.get_current_version() == new_commit.hexsha
        files_in_repo = {f.name for f in bundle2.path.iterdir() if f.is_file()}
        assert {"reinit_file.py"} == files_in_repo

        assert_repo_is_closed(bundle2)

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_head(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        repo.create_head("test")
        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="test")
        bundle.initialize()
        assert bundle.repo.head.ref.name == "test"

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_version_not_found(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_HTTPS,
            version="not_found",
            tracking_ref=GIT_DEFAULT_BRANCH,
        )

        with pytest.raises(AirflowException, match="Version not_found not found in the repository"):
            bundle.initialize()

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_subdir(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        subdir = "somesubdir"
        subdir_path = repo_path / subdir
        subdir_path.mkdir()

        file_path = subdir_path / "some_new_file.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        repo.index.commit("Initial commit")

        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_HTTPS,
            tracking_ref=GIT_DEFAULT_BRANCH,
            subdir=subdir,
        )
        bundle.initialize()

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert str(bundle.path).endswith(subdir)
        assert {"some_new_file.py"} == files_in_repo

    def test_raises_when_no_repo_url(self):
        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_NO_REPO_URL,
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        with pytest.raises(AirflowException, match=f"Connection {CONN_NO_REPO_URL} doesn't have a host url"):
            bundle.initialize()

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_with_path_as_repo_url(self, mock_gitRepo, mock_githook):
        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_ONLY_PATH,
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        bundle.initialize()
        assert mock_gitRepo.clone_from.call_count == 2
        assert mock_gitRepo.return_value.git.checkout.call_count == 1

    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_refresh_with_git_connection(self, mock_gitRepo):
        bundle = GitDagBundle(
            name="test",
            git_conn_id="git_default",
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        bundle.initialize()
        # 1 in _clone_bare_repo_if_required, 1 in refresh() for bare repo, 1 in refresh() for working repo
        assert mock_gitRepo.return_value.remotes.origin.fetch.call_count == 3
        mock_gitRepo.return_value.remotes.origin.fetch.reset_mock()
        bundle.refresh()
        assert mock_gitRepo.return_value.remotes.origin.fetch.call_count == 2

    @pytest.mark.parametrize(
        ("repo_url", "extra_conn_kwargs", "expected_url"),
        [
            ("git@github.com:apache/airflow.git", None, "https://github.com/apache/airflow/tree/0f0f0f"),
            ("git@github.com:apache/airflow", None, "https://github.com/apache/airflow/tree/0f0f0f"),
            ("https://github.com/apache/airflow", None, "https://github.com/apache/airflow/tree/0f0f0f"),
            ("https://github.com/apache/airflow.git", None, "https://github.com/apache/airflow/tree/0f0f0f"),
            ("git@gitlab.com:apache/airflow.git", None, "https://gitlab.com/apache/airflow/-/tree/0f0f0f"),
            ("git@bitbucket.org:apache/airflow.git", None, "https://bitbucket.org/apache/airflow/src/0f0f0f"),
            (
                "git@myorg.github.com:apache/airflow.git",
                None,
                "https://myorg.github.com/apache/airflow/tree/0f0f0f",
            ),
            (
                "https://myorg.github.com/apache/airflow.git",
                None,
                "https://myorg.github.com/apache/airflow/tree/0f0f0f",
            ),
            ("/dev/null", None, None),
            ("file:///dev/null", None, None),
            (
                "https://github.com/apache/airflow",
                {"password": "abc123"},
                "https://github.com/apache/airflow/tree/0f0f0f",
            ),
            (
                "https://github.com/apache/airflow",
                {"login": "abc123"},
                "https://github.com/apache/airflow/tree/0f0f0f",
            ),
            (
                "https://github.com/apache/airflow",
                {"login": "abc123", "password": "def456"},
                "https://github.com/apache/airflow/tree/0f0f0f",
            ),
            (
                "https://github.com:443/apache/airflow",
                None,
                "https://github.com:443/apache/airflow/tree/0f0f0f",
            ),
            (
                "https://github.com:443/apache/airflow",
                {"password": "abc123"},
                "https://github.com:443/apache/airflow/tree/0f0f0f",
            ),
        ],
    )
    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_view_url(
        self, mock_gitrepo, repo_url, extra_conn_kwargs, expected_url, create_connection_without_db
    ):
        create_connection_without_db(
            Connection(
                conn_id="my_git_connection",
                host=repo_url,
                conn_type="git",
                **(extra_conn_kwargs or {}),
            )
        )
        bundle = GitDagBundle(
            name="test",
            git_conn_id="my_git_connection",
            tracking_ref="main",
        )
        bundle.initialize = mock.MagicMock()
        view_url = bundle.view_url("0f0f0f")
        assert view_url == expected_url
        bundle.initialize.assert_not_called()

    @pytest.mark.parametrize(
        ("repo_url", "extra_conn_kwargs", "expected_url"),
        [
            (
                "git@github.com:apache/airflow.git",
                None,
                "https://github.com/apache/airflow/tree/0f0f0f/subdir",
            ),
            ("git@github.com:apache/airflow", None, "https://github.com/apache/airflow/tree/0f0f0f/subdir"),
            (
                "https://github.com/apache/airflow",
                None,
                "https://github.com/apache/airflow/tree/0f0f0f/subdir",
            ),
            (
                "https://github.com/apache/airflow.git",
                None,
                "https://github.com/apache/airflow/tree/0f0f0f/subdir",
            ),
            (
                "git@gitlab.com:apache/airflow.git",
                None,
                "https://gitlab.com/apache/airflow/-/tree/0f0f0f/subdir",
            ),
            (
                "git@bitbucket.org:apache/airflow.git",
                None,
                "https://bitbucket.org/apache/airflow/src/0f0f0f/subdir",
            ),
            (
                "git@myorg.github.com:apache/airflow.git",
                None,
                "https://myorg.github.com/apache/airflow/tree/0f0f0f/subdir",
            ),
            (
                "https://myorg.github.com/apache/airflow.git",
                None,
                "https://myorg.github.com/apache/airflow/tree/0f0f0f/subdir",
            ),
            (
                "https://github.com/apache/airflow",
                {"password": "abc123"},
                "https://github.com/apache/airflow/tree/0f0f0f/subdir",
            ),
            (
                "https://github.com/apache/airflow",
                {"login": "abc123"},
                "https://github.com/apache/airflow/tree/0f0f0f/subdir",
            ),
            (
                "https://github.com/apache/airflow",
                {"login": "abc123", "password": "def456"},
                "https://github.com/apache/airflow/tree/0f0f0f/subdir",
            ),
            (
                "https://github.com:443/apache/airflow",
                None,
                "https://github.com:443/apache/airflow/tree/0f0f0f/subdir",
            ),
            (
                "https://github.com:443/apache/airflow",
                {"password": "abc123"},
                "https://github.com:443/apache/airflow/tree/0f0f0f/subdir",
            ),
        ],
    )
    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_view_url_subdir(
        self, mock_gitrepo, repo_url, extra_conn_kwargs, expected_url, create_connection_without_db
    ):
        create_connection_without_db(
            Connection(
                conn_id="git_default",
                host=repo_url,
                conn_type="git",
                **(extra_conn_kwargs or {}),
            )
        )
        bundle = GitDagBundle(
            name="test",
            tracking_ref="main",
            subdir="subdir",
            git_conn_id="git_default",
        )
        bundle.initialize = mock.MagicMock()
        view_url = bundle.view_url("0f0f0f")
        assert view_url == expected_url
        bundle.initialize.assert_not_called()

    @pytest.mark.skipif(not AIRFLOW_V_3_1_PLUS, reason="Airflow 3.0 does not support view_url_template")
    @pytest.mark.parametrize(
        ("repo_url", "extra_conn_kwargs", "expected_url"),
        [
            (
                "git@github.com:apache/airflow.git",
                None,
                "https://github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "git@github.com:apache/airflow",
                None,
                "https://github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "https://github.com/apache/airflow",
                None,
                "https://github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "https://github.com/apache/airflow.git",
                None,
                "https://github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "git@gitlab.com:apache/airflow.git",
                None,
                "https://gitlab.com/apache/airflow/-/tree/{version}/subdir",
            ),
            (
                "git@bitbucket.org:apache/airflow.git",
                None,
                "https://bitbucket.org/apache/airflow/src/{version}/subdir",
            ),
            (
                "git@myorg.github.com:apache/airflow.git",
                None,
                "https://myorg.github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "https://myorg.github.com/apache/airflow.git",
                None,
                "https://myorg.github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "https://github.com/apache/airflow",
                {"password": "abc123"},
                "https://github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "https://github.com/apache/airflow",
                {"login": "abc123"},
                "https://github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "https://github.com/apache/airflow",
                {"login": "abc123", "password": "def456"},
                "https://github.com/apache/airflow/tree/{version}/subdir",
            ),
            (
                "https://github.com:443/apache/airflow",
                None,
                "https://github.com:443/apache/airflow/tree/{version}/subdir",
            ),
            (
                "https://github.com:443/apache/airflow",
                {"password": "abc123"},
                "https://github.com:443/apache/airflow/tree/{version}/subdir",
            ),
        ],
    )
    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_view_url_template_subdir(
        self, mock_gitrepo, repo_url, extra_conn_kwargs, expected_url, create_connection_without_db
    ):
        create_connection_without_db(
            Connection(
                conn_id="git_default",
                host=repo_url,
                conn_type="git",
                **(extra_conn_kwargs or {}),
            )
        )
        bundle = GitDagBundle(
            name="test",
            tracking_ref="main",
            subdir="subdir",
            git_conn_id="git_default",
        )
        bundle.initialize = mock.MagicMock()
        view_url_template = bundle.view_url_template()
        assert view_url_template == expected_url
        bundle.initialize.assert_not_called()

    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_view_url_returns_none_when_no_version_in_view_url(self, mock_gitrepo):
        bundle = GitDagBundle(
            name="test",
            tracking_ref="main",
        )
        view_url = bundle.view_url(None)
        assert view_url is None

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_clone_bare_repo_git_command_error(self, mock_githook):
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"
        mock_githook.return_value.env = {}

        with mock.patch("airflow.providers.git.bundles.git.Repo.clone_from") as mock_clone:
            mock_clone.side_effect = GitCommandError("clone", "Simulated error")
            bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="main")
            with pytest.raises(
                RuntimeError,
                match=re.escape("Error cloning repository"),
            ):
                bundle.initialize()

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_clone_repo_no_such_path_error(self, mock_githook):
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"

        with mock.patch("airflow.providers.git.bundles.git.os.path.exists", return_value=False):
            with mock.patch("airflow.providers.git.bundles.git.Repo.clone_from") as mock_clone:
                mock_clone.side_effect = NoSuchPathError("Path not found")
                bundle = GitDagBundle(name="test", tracking_ref="main")
                with pytest.raises(AirflowException) as exc_info:
                    bundle._clone_repo_if_required()

                assert "Repository path: %s not found" in str(exc_info.value)

    @patch.dict(os.environ, {"AIRFLOW_CONN_MY_TEST_GIT": '{"host": "something", "conn_type": "git"}'})
    @pytest.mark.parametrize(
        ("conn_id", "repo_url", "expected_hook_type", "exception_expected"),
        [
            ("my_test_git", None, GitHook, False),
            ("something-else", None, None, True),
            ("something-else", "https://github.com/apache/airflow.git", None, False),
        ],
    )
    def test_repo_url_access_missing_connection_raises_exception(
        self, conn_id, repo_url, expected_hook_type, exception_expected
    ):
        if exception_expected:
            with pytest.raises(Exception, match="The conn_id `something-else` isn't defined"):
                GitDagBundle(
                    name="testa",
                    tracking_ref="main",
                    git_conn_id=conn_id,
                    repo_url=repo_url,
                )
        else:
            bundle = GitDagBundle(
                name="testa",
                tracking_ref="main",
                git_conn_id=conn_id,
                repo_url=repo_url,
            )
            if expected_hook_type is None:
                assert bundle.hook is None
            else:
                assert isinstance(bundle.hook, expected_hook_type)

    def test_public_repository_works_without_connection(self):
        """Test that public repositories work without any connection defined."""
        bundle = GitDagBundle(
            name="public-repo",
            tracking_ref="main",
            repo_url="https://github.com/apache/airflow.git",
            git_conn_id="nonexistent_connection",
        )
        assert bundle.hook is None
        assert bundle.repo_url == "https://github.com/apache/airflow.git"

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_lock_used(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref=GIT_DEFAULT_BRANCH)
        with mock.patch("airflow.providers.git.bundles.git.GitDagBundle.lock") as mock_lock:
            bundle.initialize()
            assert mock_lock.call_count == 2  # both initialize and refresh

    @pytest.mark.parametrize(
        ("conn_json", "repo_url", "expected"),
        [
            (
                {"host": "git@github.com:apache/airflow.git"},
                "git@github.com:apache/hello.git",
                "git@github.com:apache/hello.git",
            ),
            ({"host": "git@github.com:apache/airflow.git"}, None, "git@github.com:apache/airflow.git"),
            ({}, "git@github.com:apache/hello.git", "git@github.com:apache/hello.git"),
        ],
    )
    def test_repo_url_precedence(self, conn_json, repo_url, expected):
        conn_str = json.dumps(conn_json)
        with patch.dict(os.environ, {"AIRFLOW_CONN_MY_TEST_GIT": conn_str}):
            bundle = GitDagBundle(
                name="test",
                tracking_ref="main",
                git_conn_id="my_test_git",
                repo_url=repo_url,
            )
            assert bundle.repo_url == expected

    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_clone_passes_env_from_githook(self, mock_gitRepo):
        def _fake_clone_from(*_, **kwargs):
            if "env" not in kwargs:
                raise GitCommandError("git", 128, "Permission denied")
            return types.SimpleNamespace()

        EXPECTED_ENV = {"GIT_SSH_COMMAND": "ssh -i /id_rsa -o StrictHostKeyChecking=no"}

        mock_gitRepo.clone_from.side_effect = _fake_clone_from
        # Mock needs to support the fetch operation called in _clone_bare_repo_if_required
        mock_repo_instance = mock.MagicMock()
        mock_gitRepo.return_value = mock_repo_instance

        with mock.patch("airflow.providers.git.bundles.git.GitHook") as mock_githook:
            mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"
            mock_githook.return_value.env = EXPECTED_ENV

            bundle = GitDagBundle(
                name="my_repo",
                git_conn_id="git_default",
                repo_url="git@github.com:apache/airflow.git",
                tracking_ref="main",
            )
            bundle._clone_bare_repo_if_required()
            _, kwargs = mock_gitRepo.clone_from.call_args
            assert kwargs["env"] == EXPECTED_ENV

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    @mock.patch("airflow.providers.git.bundles.git.shutil.rmtree")
    @mock.patch("airflow.providers.git.bundles.git.os.path.exists")
    def test_clone_bare_repo_invalid_repository_error_retry(self, mock_exists, mock_rmtree, mock_githook):
        """Test that InvalidGitRepositoryError triggers cleanup and retry."""
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"
        mock_githook.return_value.env = {}

        # Set up exists to return True for the bare repo path (simulating corrupted repo exists)
        mock_exists.return_value = True

        with mock.patch("airflow.providers.git.bundles.git.Repo") as mock_repo_class:
            # First call to Repo() raises InvalidGitRepositoryError, second call succeeds
            mock_repo_class.side_effect = [
                InvalidGitRepositoryError("Invalid git repository"),
                mock.MagicMock(),  # Second attempt succeeds
            ]

            # Mock successful clone_from for the retry attempt
            mock_repo_class.clone_from = mock.MagicMock()

            bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="main")

            # This should not raise an exception due to retry logic
            bundle._clone_bare_repo_if_required()

            # Verify cleanup was called
            mock_rmtree.assert_called_once_with(bundle.bare_repo_path)

            # Verify Repo was called twice (failed attempt + retry)
            assert mock_repo_class.call_count == 2

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    @mock.patch("airflow.providers.git.bundles.git.shutil.rmtree")
    @mock.patch("airflow.providers.git.bundles.git.os.path.exists")
    def test_clone_bare_repo_invalid_repository_error_retry_fails(
        self, mock_exists, mock_rmtree, mock_githook
    ):
        """Test that InvalidGitRepositoryError after retry is re-raised (wrapped in AirflowException by caller)."""
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"
        mock_githook.return_value.env = {}

        # Set up exists to return True for the bare repo path
        mock_exists.return_value = True

        with mock.patch("airflow.providers.git.bundles.git.Repo") as mock_repo_class:
            # Both calls to Repo() raise InvalidGitRepositoryError
            mock_repo_class.side_effect = InvalidGitRepositoryError("Invalid git repository")

            bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="main")

            # The raw exception is raised by the method itself, but wrapped by _initialize
            with pytest.raises(InvalidGitRepositoryError, match="Invalid git repository"):
                bundle._clone_bare_repo_if_required()

            # Verify cleanup was called twice (once for each failed attempt)
            assert mock_rmtree.call_count == 2
            mock_rmtree.assert_called_with(bundle.bare_repo_path)

            # Verify Repo was called twice (failed attempt + failed retry)
            assert mock_repo_class.call_count == 2

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_survives_upstream_tag_deletion(self, mock_githook, git_repo):
        """Refresh succeeds when tracked tag is deleted upstream; local copy persists."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        initial_commit = repo.head.commit

        repo.create_tag("ephemeral")
        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="ephemeral")
        bundle.initialize()
        assert bundle.get_current_version() == initial_commit.hexsha

        # Delete the tag from the upstream repo
        repo.delete_tag("ephemeral")

        # Refresh still succeeds because git fetch refspecs don't prune deleted tags
        bundle.refresh()
        assert bundle.get_current_version() == initial_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_failure_preserves_previous_checkout(self, mock_githook, git_repo):
        """A failed refresh must not corrupt the previous working tree."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref=GIT_DEFAULT_BRANCH)
        bundle.initialize()

        files_before = {f.name for f in bundle.path.iterdir() if f.is_file()}
        version_before = bundle.get_current_version()

        # Add a new commit upstream
        new_file = repo_path / "new_file.py"
        new_file.write_text("new content")
        repo.index.add([new_file])
        repo.index.commit("New commit")

        # Make the bare repo fetch fail — this is the first step in refresh()
        with mock.patch.object(bundle, "_fetch_bare_repo", side_effect=GitCommandError("fetch", "simulated")):
            with pytest.raises(GitCommandError):
                bundle.refresh()

        # Working tree should be unchanged from before the failed refresh
        files_after = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert files_before == files_after
        assert bundle.get_current_version() == version_before

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_versioned_bundle_raises(self, mock_githook, git_repo):
        """Calling refresh() on a versioned bundle must raise AirflowException."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        version = repo.head.commit.hexsha

        bundle = GitDagBundle(
            name="test",
            git_conn_id=CONN_HTTPS,
            version=version,
            tracking_ref=GIT_DEFAULT_BRANCH,
            prune_dotgit_folder=False,
        )
        bundle.initialize()

        with pytest.raises(AirflowException, match="Refreshing a specific version is not supported"):
            bundle.refresh()

        assert_repo_is_closed(bundle)

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    @mock.patch("airflow.providers.git.bundles.git.shutil.rmtree")
    @mock.patch("airflow.providers.git.bundles.git.os.path.exists")
    def test_clone_repo_invalid_repository_error_retry(self, mock_exists, mock_rmtree, mock_githook):
        """Test that InvalidGitRepositoryError on working repo triggers cleanup and retry."""
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"
        mock_githook.return_value.env = {}

        mock_exists.return_value = True

        with mock.patch("airflow.providers.git.bundles.git.Repo") as mock_repo_class:
            # First call to Repo() raises InvalidGitRepositoryError, second succeeds
            mock_repo_class.side_effect = [
                InvalidGitRepositoryError("Invalid git repository"),
                mock.MagicMock(),  # Second attempt succeeds
            ]
            mock_repo_class.clone_from = mock.MagicMock()

            bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="main")
            bundle.bare_repo_path = "/fake/bare"

            bundle._clone_repo_if_required()

            # Verify cleanup was called for the working repo path
            mock_rmtree.assert_called_once_with(bundle.repo_path)

            # Verify Repo was called twice (failed attempt + retry)
            assert mock_repo_class.call_count == 2

    @mock.patch("airflow.providers.git.bundles.git.shutil.rmtree")
    @mock.patch("airflow.providers.git.bundles.git.os.path.exists")
    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_initialize_fetches_submodules_when_enabled(
        self, mock_repo_class, mock_githook, mock_exists, mock_rmtree
    ):
        """Test that submodules are synced and updated when submodules=True during initialization."""
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"

        # Mock exists to return True so we skip the clone logic and go straight to initialization
        mock_exists.return_value = True

        mock_repo_instance = mock_repo_class.return_value
        # Ensure _has_version returns True so we don't try to fetch origin
        mock_repo_instance.commit.return_value = mock.MagicMock()

        bundle = GitDagBundle(
            name="test",
            git_conn_id="git_default",
            tracking_ref="main",
            version="123456",
            submodules=True,
        )

        bundle.initialize()

        # Verify submodule commands were called
        mock_repo_instance.git.submodule.assert_has_calls(
            [mock.call("sync", "--recursive"), mock.call("update", "--init", "--recursive", "--jobs", "1")]
        )
        mock_rmtree.assert_not_called()

    @mock.patch("airflow.providers.git.bundles.git.shutil.rmtree")
    @mock.patch("airflow.providers.git.bundles.git.os.path.exists")
    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_refresh_fetches_submodules_when_enabled(
        self, mock_repo_class, mock_githook, mock_exists, mock_rmtree
    ):
        """Test that submodules are synced and updated when submodules=True during refresh."""
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"
        mock_exists.return_value = True

        mock_repo_instance = mock_repo_class.return_value

        bundle = GitDagBundle(
            name="test",
            git_conn_id="git_default",
            tracking_ref="main",
            submodules=True,
        )

        # Calling initialize without a specific version triggers refresh()
        bundle.initialize()

        # Verify submodule commands were called
        mock_repo_instance.git.submodule.assert_has_calls(
            [mock.call("sync", "--recursive"), mock.call("update", "--init", "--recursive", "--jobs", "1")]
        )
        mock_rmtree.assert_not_called()

    @mock.patch("airflow.providers.git.bundles.git.shutil.rmtree")
    @mock.patch("airflow.providers.git.bundles.git.os.path.exists")
    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_submodules_disabled_by_default(self, mock_repo_class, mock_githook, mock_exists, mock_rmtree):
        """Test that submodules are NOT fetched by default."""
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"
        mock_exists.return_value = True

        mock_repo_instance = mock_repo_class.return_value

        bundle = GitDagBundle(
            name="test",
            git_conn_id="git_default",
            tracking_ref="main",
            version="123456",
            # submodules defaults to False
        )

        bundle.initialize()

        # Ensure submodule commands were NOT called
        mock_repo_instance.git.submodule.assert_not_called()

    @mock.patch("airflow.providers.git.bundles.git.shutil.rmtree")
    @mock.patch("airflow.providers.git.bundles.git.os.path.exists")
    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    @mock.patch("airflow.providers.git.bundles.git.Repo")
    def test_submodule_fetch_error_raises_runtime_error(
        self, mock_repo_class, mock_githook, mock_exists, mock_rmtree
    ):
        """Test that a GitCommandError during submodule update is raised as a RuntimeError."""
        mock_githook.return_value.repo_url = "git@github.com:apache/airflow.git"
        mock_exists.return_value = True

        mock_repo_instance = mock_repo_class.return_value
        mock_repo_instance.commit.return_value = mock.MagicMock()

        # Simulate a git error when running submodule update
        mock_repo_instance.git.submodule.side_effect = GitCommandError("submodule update", "failed")

        bundle = GitDagBundle(
            name="test",
            git_conn_id="git_default",
            tracking_ref="main",
            version="123456",
            submodules=True,
        )

        with pytest.raises(RuntimeError, match="Error pulling submodule from repository"):
            bundle.initialize()

        mock_rmtree.assert_not_called()

    @patch.dict(
        os.environ,
        {
            "GIT_CONFIG_COUNT": "1",
            "GIT_CONFIG_KEY_0": "protocol.file.allow",
            "GIT_CONFIG_VALUE_0": "always",
        },
    )
    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_with_real_submodules_after_ref_change(self, mock_githook, tmp_path_factory):
        """Refresh with real submodules works when the main repo changes but submodule ref stays the same."""
        # Create submodule repo
        sub_dir = tmp_path_factory.mktemp("subrepo")
        sub_repo = Repo.init(sub_dir)
        sub_repo.git.symbolic_ref("HEAD", f"refs/heads/{GIT_DEFAULT_BRANCH}")
        sub_file = sub_dir / "sub_file.py"
        sub_file.write_text("sub content")
        sub_repo.index.add([sub_file])
        sub_repo.index.commit("Sub initial commit")

        # Create main repo with submodule
        main_dir = tmp_path_factory.mktemp("mainrepo")
        main_repo = Repo.init(main_dir)
        main_repo.git.symbolic_ref("HEAD", f"refs/heads/{GIT_DEFAULT_BRANCH}")
        main_file = main_dir / "main_file.py"
        main_file.write_text("main content")
        main_repo.index.add([main_file])
        main_repo.index.commit("Main initial commit")

        # Add submodule
        main_repo.git.submodule("add", str(sub_dir), "mysub")
        main_repo.index.commit("Add submodule")
        initial_commit = main_repo.head.commit
        main_repo.create_tag("v1")

        mock_githook.return_value.repo_url = str(main_dir)
        mock_githook.return_value.env = {}
        bundle = GitDagBundle(
            name="test_submod",
            git_conn_id=CONN_HTTPS,
            tracking_ref="v1",
            submodules=True,
        )
        bundle.initialize()
        assert bundle.get_current_version() == initial_commit.hexsha

        # Verify submodule content is checked out
        sub_content = (bundle.repo_path / "mysub" / "sub_file.py").read_text()
        assert sub_content == "sub content"

        # Add a new file to main repo (submodule ref unchanged) and move the tag
        new_main_file = main_dir / "extra.py"
        new_main_file.write_text("extra content")
        main_repo.index.add([new_main_file])
        new_commit = main_repo.index.commit("Extra file")
        main_repo.create_tag("v1", force=True)

        bundle.refresh()

        assert bundle.get_current_version() == new_commit.hexsha
        files_in_repo = {f.name for f in bundle.repo_path.iterdir() if f.is_file()}
        assert "extra.py" in files_in_repo

        # Submodule content remains intact
        sub_content = (bundle.repo_path / "mysub" / "sub_file.py").read_text()
        assert sub_content == "sub content"

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_ambiguous_ref_prefers_branch_over_tag(self, mock_githook, git_repo):
        """When tracking_ref matches both a branch and tag, refresh follows the branch."""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        # Create a branch named "ambiguous" at the initial commit
        repo.create_head("ambiguous")

        # Create a new commit and point a tag named "ambiguous" at it
        tag_file = repo_path / "tag_file.py"
        tag_file.write_text("tag content")
        repo.index.add([tag_file])
        repo.index.commit("Tag commit")
        repo.create_tag("ambiguous")

        # Move the "ambiguous" branch to another new commit
        repo.heads.ambiguous.checkout()
        branch_file = repo_path / "branch_file.py"
        branch_file.write_text("branch content")
        repo.index.add([branch_file])
        branch_commit = repo.index.commit("Branch commit")

        # Switch back to main so the upstream repo has a clean HEAD
        repo.heads[GIT_DEFAULT_BRANCH].checkout()

        bundle = GitDagBundle(name="test", git_conn_id=CONN_HTTPS, tracking_ref="ambiguous")
        bundle.initialize()

        # refresh() prefers origin/ambiguous (branch) over the ambiguous tag
        bundle.refresh()
        assert bundle.get_current_version() == branch_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert "branch_file.py" in files_in_repo
