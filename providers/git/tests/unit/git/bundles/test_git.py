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
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time.comms import ErrorResponse

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
        ("conn_id", "expected_hook_type"),
        [("my_test_git", GitHook), ("something-else", type(None))],
    )
    def test_repo_url_access_missing_connection_doesnt_error(
        self, conn_id, expected_hook_type, mock_supervisor_comms
    ):
        if expected_hook_type is type(None):
            mock_supervisor_comms.send.return_value = ErrorResponse(error=ErrorType.CONNECTION_NOT_FOUND)
        bundle = GitDagBundle(
            name="testa",
            tracking_ref="main",
            git_conn_id=conn_id,
        )
        assert isinstance(bundle.hook, expected_hook_type)

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
