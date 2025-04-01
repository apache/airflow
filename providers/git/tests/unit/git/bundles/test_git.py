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
import re
from unittest import mock
from unittest.mock import patch

import pytest
from git import Repo
from git.exc import GitCommandError, NoSuchPathError

from airflow.dag_processing.bundles.base import get_bundle_storage_root_path
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.git.bundles.git import GitDagBundle
from airflow.providers.git.hooks.git import GitHook
from airflow.utils import db

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections

pytestmark = pytest.mark.db_test


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


class TestGitDagBundle:
    @classmethod
    def teardown_class(cls) -> None:
        clear_db_connections()

    @classmethod
    def setup_class(cls) -> None:
        db.merge_conn(
            Connection(
                conn_id="git_default",
                host="git@github.com:apache/airflow.git",
                conn_type="git",
            )
        )
        db.merge_conn(
            Connection(
                conn_id=CONN_HTTPS,
                host=AIRFLOW_HTTPS_URL,
                password=ACCESS_TOKEN,
                conn_type="git",
            )
        )
        db.merge_conn(
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
        assert bundle.repo_url == f"https://{ACCESS_TOKEN}@github.com/apache/zzzairflow"

    def test_falls_back_to_connection_host_when_no_repo_url_provided(self):
        bundle = GitDagBundle(name="test", tracking_ref=GIT_DEFAULT_BRANCH)
        assert bundle.repo_url == bundle.hook.repo_url

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_get_current_version(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        bundle = GitDagBundle(name="test", tracking_ref=GIT_DEFAULT_BRANCH)

        bundle.initialize()

        assert bundle.get_current_version() == repo.head.commit.hexsha

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
            version=starting_commit.hexsha,
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        bundle.initialize()

        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

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
            version="test",
            tracking_ref=GIT_DEFAULT_BRANCH,
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

        bundle = GitDagBundle(name="test", tracking_ref=GIT_DEFAULT_BRANCH)
        bundle.initialize()

        assert bundle.get_current_version() != starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py", "new_test.py"} == files_in_repo

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

        bundle = GitDagBundle(name="test", tracking_ref=GIT_DEFAULT_BRANCH)
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

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_refresh_tag(self, mock_githook, git_repo):
        """Ensure that the bundle refresh works when tracking a tag"""
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        # add tag
        repo.create_tag("test123")

        bundle = GitDagBundle(name="test", tracking_ref="test123")
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
        bundle = GitDagBundle(name="test", tracking_ref="test")
        bundle.initialize()
        assert bundle.repo.head.ref.name == "test"

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_version_not_found(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        bundle = GitDagBundle(
            name="test",
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
        assert mock_gitRepo.return_value.remotes.origin.fetch.call_count == 2  # 1 in bare, 1 in main repo
        mock_gitRepo.return_value.remotes.origin.fetch.reset_mock()
        bundle.refresh()
        assert mock_gitRepo.return_value.remotes.origin.fetch.call_count == 2

    @pytest.mark.parametrize(
        "repo_url, extra_conn_kwargs, expected_url",
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
    def test_view_url(self, mock_gitrepo, repo_url, extra_conn_kwargs, expected_url, session):
        session.query(Connection).delete()
        conn = Connection(
            conn_id="git_default",
            host=repo_url,
            conn_type="git",
            **(extra_conn_kwargs or {}),
        )
        session.add(conn)
        session.commit()
        bundle = GitDagBundle(
            name="test",
            tracking_ref="main",
        )
        bundle.initialize = mock.MagicMock()
        view_url = bundle.view_url("0f0f0f")
        assert view_url == expected_url
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
            bundle = GitDagBundle(name="test", tracking_ref="main")
            with pytest.raises(
                AirflowException,
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

    @patch.dict(os.environ, {"AIRFLOW_CONN_MY_TEST_GIT": '{"host": "something"}'})
    @pytest.mark.parametrize("conn_id, should_find", [("my_test_git", True), ("something-else", False)])
    def test_repo_url_access_missing_connection_doesnt_error(self, conn_id, should_find):
        bundle = GitDagBundle(
            name="testa",
            tracking_ref="main",
            git_conn_id=conn_id,
            repo_url="some_repo_url",
        )
        assert bundle.repo_url == "some_repo_url"
        if should_find:
            assert isinstance(bundle.hook, GitHook)
        else:
            assert not hasattr(bundle, "hook")

    @mock.patch("airflow.providers.git.bundles.git.GitHook")
    def test_lock_used(self, mock_githook, git_repo):
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        bundle = GitDagBundle(name="test", tracking_ref=GIT_DEFAULT_BRANCH)
        with mock.patch("airflow.providers.git.bundles.git.GitDagBundle.lock") as mock_lock:
            bundle.initialize()
            assert mock_lock.call_count == 2  # both initialize and refresh
