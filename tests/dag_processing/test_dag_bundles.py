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

import tempfile
from pathlib import Path
from unittest import mock

import pytest
from git import Repo

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.dag_processing.bundles.dagfolder import DagsFolderDagBundle
from airflow.dag_processing.bundles.git import GitDagBundle, GitHook, SSHHook
from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.utils import db

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_connections


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("core", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


def test_default_dag_storage_path():
    with conf_vars({("core", "dag_bundle_storage_path"): ""}):
        bundle = LocalDagBundle(name="test", refresh_interval=300, local_folder="/hello")
        assert bundle._dag_bundle_root_storage_path == Path(tempfile.gettempdir(), "airflow", "dag_bundles")


def test_dag_bundle_root_storage_path():
    class BasicBundle(BaseDagBundle):
        def refresh(self):
            pass

        def get_current_version(self):
            pass

        def path(self):
            pass

    with conf_vars({("core", "dag_bundle_storage_path"): None}):
        bundle = BasicBundle(name="test", refresh_interval=300)
        assert bundle._dag_bundle_root_storage_path == Path(tempfile.gettempdir(), "airflow", "dag_bundles")


class TestLocalDagBundle:
    def test_path(self):
        bundle = LocalDagBundle(name="test", refresh_interval=300, local_folder="/hello")
        assert bundle.path == Path("/hello")

    def test_none_for_version(self):
        assert LocalDagBundle.supports_versioning is False

        bundle = LocalDagBundle(name="test", refresh_interval=300, local_folder="/hello")

        assert bundle.get_current_version() is None


class TestDagsFolderDagBundle:
    @conf_vars({("core", "dags_folder"): "/tmp/somewhere/dags"})
    def test_path(self):
        bundle = DagsFolderDagBundle(name="test")
        assert bundle.path == Path("/tmp/somewhere/dags")

    @conf_vars({("scheduler", "dag_dir_list_interval"): "10"})
    def test_refresh_interval_from_config(self):
        bundle = DagsFolderDagBundle(name="test")
        assert bundle.refresh_interval == 10

    @conf_vars({("scheduler", "dag_dir_list_interval"): "10"})
    def test_refresh_interval_from_kwarg(self):
        bundle = DagsFolderDagBundle(name="test", refresh_interval=30)
        assert bundle.refresh_interval == 30


GIT_DEFAULT_BRANCH = "main"


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


AIRFLOW_HTTPS_URL = "https://github.com/apache/airflow.git"
AIRFLOW_GIT = "git@github.com:apache/airflow.git"
ACCESS_TOKEN = "my_access_token"
CONN_DEFAULT = "git_default"
CONN_HTTPS = "my_git_conn"
CONN_HTTPS_PASSWORD = "my_git_conn_https_password"
CONN_ONLY_PATH = "my_git_conn_only_path"
CONN_NO_REPO_URL = "my_git_conn_no_repo_url"


class TestGitHook:
    @classmethod
    def teardown_class(cls) -> None:
        clear_db_connections()

    @classmethod
    def setup_class(cls) -> None:
        db.merge_conn(
            Connection(
                conn_id=CONN_DEFAULT,
                host="github.com",
                conn_type="git",
                extra={"git_repo_url": AIRFLOW_GIT},
            )
        )
        db.merge_conn(
            Connection(
                conn_id=CONN_HTTPS,
                host="github.com",
                conn_type="git",
                extra={"git_repo_url": AIRFLOW_HTTPS_URL, "git_access_token": ACCESS_TOKEN},
            )
        )
        db.merge_conn(
            Connection(
                conn_id=CONN_HTTPS_PASSWORD,
                host="github.com",
                conn_type="git",
                password=ACCESS_TOKEN,
                extra={"git_repo_url": AIRFLOW_HTTPS_URL},
            )
        )
        db.merge_conn(
            Connection(
                conn_id=CONN_ONLY_PATH,
                host="github.com",
                conn_type="git",
                extra={"git_repo_url": "path/to/repo"},
            )
        )

    @pytest.mark.parametrize(
        "conn_id, expected_repo_url",
        [
            (CONN_DEFAULT, AIRFLOW_GIT),
            (CONN_HTTPS, f"https://{ACCESS_TOKEN}@github.com/apache/airflow.git"),
            (CONN_HTTPS_PASSWORD, f"https://{ACCESS_TOKEN}@github.com/apache/airflow.git"),
            (CONN_ONLY_PATH, "path/to/repo"),
        ],
    )
    def test_correct_repo_urls(self, conn_id, expected_repo_url):
        hook = GitHook(git_conn_id=conn_id)
        assert hook.repo_url == expected_repo_url

    @mock.patch.object(SSHHook, "get_conn")
    def test_connection_made_to_ssh_hook(self, mock_ssh_hook_get_conn):
        hook = GitHook(git_conn_id=CONN_DEFAULT)
        hook.get_conn()
        mock_ssh_hook_get_conn.assert_called_once_with()


class TestGitDagBundle:
    @classmethod
    def teardown_class(cls) -> None:
        clear_db_connections()

    @classmethod
    def setup_class(cls) -> None:
        db.merge_conn(
            Connection(
                conn_id="git_default",
                host="github.com",
                conn_type="git",
                extra={"git_repo_url": "git@github.com:apache/airflow.git"},
            )
        )
        db.merge_conn(
            Connection(
                conn_id=CONN_NO_REPO_URL,
                host="github.com",
                conn_type="git",
                extra="{}",
            )
        )

    def test_supports_versioning(self):
        assert GitDagBundle.supports_versioning is True

    def test_uses_dag_bundle_root_storage_path(self, git_repo):
        repo_path, repo = git_repo
        bundle = GitDagBundle(name="test", refresh_interval=300, tracking_ref=GIT_DEFAULT_BRANCH)
        assert str(bundle._dag_bundle_root_storage_path) in str(bundle.path)

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_get_current_version(self, mock_githook, git_repo):
        mock_githook.get_conn.return_value = mock.MagicMock()
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        bundle = GitDagBundle(name="test", refresh_interval=300, tracking_ref=GIT_DEFAULT_BRANCH)

        bundle.initialize()

        assert bundle.get_current_version() == repo.head.commit.hexsha

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_get_specific_version(self, mock_githook, git_repo):
        mock_githook.get_conn.return_value = mock.MagicMock()
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
            refresh_interval=300,
            version=starting_commit.hexsha,
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        bundle.initialize()

        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_get_tag_version(self, mock_githook, git_repo):
        mock_githook.get_conn.return_value = mock.MagicMock()
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        # add tag
        repo.create_tag("test")
        print(repo.tags)

        # Add new file to the repo
        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        repo.index.commit("Another commit")

        bundle = GitDagBundle(
            name="test",
            refresh_interval=300,
            version="test",
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        bundle.initialize()
        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_get_latest(self, mock_githook, git_repo):
        mock_githook.get_conn.return_value = mock.MagicMock()
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        repo.index.commit("Another commit")

        bundle = GitDagBundle(name="test", refresh_interval=300, tracking_ref=GIT_DEFAULT_BRANCH)
        bundle.initialize()

        assert bundle.get_current_version() != starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py", "new_test.py"} == files_in_repo

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_refresh(self, mock_githook, git_repo):
        mock_githook.get_conn.return_value = mock.MagicMock()
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        starting_commit = repo.head.commit

        bundle = GitDagBundle(name="test", refresh_interval=300, tracking_ref=GIT_DEFAULT_BRANCH)
        bundle.initialize()

        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        commit = repo.index.commit("Another commit")

        bundle.refresh()

        assert bundle.get_current_version() == commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py", "new_test.py"} == files_in_repo

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_head(self, mock_githook, git_repo):
        mock_githook.get_conn.return_value = mock.MagicMock()
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path

        repo.create_head("test")
        bundle = GitDagBundle(name="test", refresh_interval=300, tracking_ref="test")
        bundle.initialize()
        assert bundle.repo.head.ref.name == "test"

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_version_not_found(self, mock_githook, git_repo):
        mock_githook.get_conn.return_value = mock.MagicMock()
        repo_path, repo = git_repo
        mock_githook.return_value.repo_url = repo_path
        bundle = GitDagBundle(
            name="test",
            refresh_interval=300,
            version="not_found",
            tracking_ref=GIT_DEFAULT_BRANCH,
        )

        with pytest.raises(AirflowException, match="Version not_found not found in the repository"):
            bundle.initialize()

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_subdir(self, mock_githook, git_repo):
        mock_githook.get_conn.return_value = mock.MagicMock()
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
            refresh_interval=300,
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
            refresh_interval=300,
            git_conn_id=CONN_NO_REPO_URL,
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        with pytest.raises(
            AirflowException, match=f"Connection {CONN_NO_REPO_URL} doesn't have a git_repo_url"
        ):
            bundle.initialize()

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    @mock.patch("airflow.dag_processing.bundles.git.Repo")
    @mock.patch.object(GitDagBundle, "_clone_from")
    def test_with_path_as_repo_url(self, mock_clone_from, mock_gitRepo, mock_githook):
        bundle = GitDagBundle(
            name="test",
            refresh_interval=300,
            git_conn_id=CONN_ONLY_PATH,
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        bundle.initialize()
        assert mock_clone_from.call_count == 2
        assert mock_gitRepo.return_value.git.checkout.call_count == 1

    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    @mock.patch("airflow.dag_processing.bundles.git.Repo")
    def test_refresh_with_git_connection(self, mock_gitRepo, mock_hook):
        bundle = GitDagBundle(
            name="test",
            refresh_interval=300,
            git_conn_id="git_default",
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        bundle.initialize()
        bundle.refresh()
        # check remotes called twice. one at initialize and one at refresh above
        assert mock_gitRepo.return_value.remotes.origin.fetch.call_count == 2

    @pytest.mark.parametrize(
        "repo_url",
        [
            pytest.param("https://github.com/apache/airflow", id="https_url"),
            pytest.param("airflow@example:apache/airflow.git", id="does_not_start_with_git_at"),
            pytest.param("git@example:apache/airflow", id="does_not_end_with_dot_git"),
        ],
    )
    @mock.patch("airflow.dag_processing.bundles.git.GitHook")
    def test_repo_url_starts_with_git_when_using_ssh_conn_id(self, mock_hook, repo_url, session):
        mock_hook.get_conn.return_value = mock.MagicMock()
        mock_hook.return_value.repo_url = repo_url
        bundle = GitDagBundle(
            name="test",
            refresh_interval=300,
            git_conn_id="git_default",
            tracking_ref=GIT_DEFAULT_BRANCH,
        )
        with pytest.raises(
            AirflowException, match=f"Invalid git URL: {repo_url}. URL must start with git@ and end with .git"
        ):
            bundle.initialize()
