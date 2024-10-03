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

import pytest
from git import Repo

from airflow.dag_processing.bundles.git import GitDagBundle
from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.exceptions import AirflowException
from tests.test_utils.config import conf_vars


@pytest.fixture(autouse=True)
def bundle_temp_dir(tmp_path):
    with conf_vars({("core", "dag_bundle_storage_path"): str(tmp_path)}):
        yield tmp_path


def test_default_dag_storage_path():
    with conf_vars({("core", "dag_bundle_storage_path"): ""}):
        bundle = LocalDagBundle(id="test", local_folder="/hello")
        assert bundle._dag_bundle_storage_path == Path(tempfile.gettempdir(), "airflow", "dag_bundles")


class TestLocalDagBundle:
    def test_path(self):
        bundle = LocalDagBundle(id="test", local_folder="/hello")
        assert bundle.path == Path("/hello")

    def test_does_not_support_versioning(self):
        assert LocalDagBundle.supports_versioning is False

        bundle = LocalDagBundle(id="test", local_folder="/hello")

        with pytest.raises(AirflowException):
            bundle.get_current_version()


@pytest.fixture
def git_repo(tmp_path_factory):
    directory = tmp_path_factory.mktemp("repo")
    repo = Repo.init(directory)
    file_path = directory / "test_dag.py"
    with open(file_path, "w") as f:
        f.write("hello world")
    repo.index.add([file_path])
    repo.index.commit("Initial commit")
    return (directory, repo)


class TestGitDagBundle:
    def test_supports_versioning(self):
        assert GitDagBundle.supports_versioning is True

    def test_uses_dag_bundle_storage_path(self, git_repo):
        repo_path, repo = git_repo
        bundle = GitDagBundle(id="test", repo_url=repo_path, head="master")
        assert str(bundle._dag_bundle_storage_path) in str(bundle.path)

    def test_get_current_version(self, git_repo):
        repo_path, repo = git_repo
        bundle = GitDagBundle(id="test", repo_url=repo_path, head="master")

        assert bundle.get_current_version() == repo.head.commit.hexsha

    def test_get_specific_version(self, git_repo):
        repo_path, repo = git_repo
        starting_commit = repo.head.commit

        # Add new file to the repo
        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        repo.index.commit("Another commit")

        bundle = GitDagBundle(id="test", version=starting_commit.hexsha, repo_url=repo_path, head="master")

        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

    def test_get_tag_version(self, git_repo):
        repo_path, repo = git_repo
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

        bundle = GitDagBundle(id="test", version="test", repo_url=repo_path, head="master")

        assert bundle.get_current_version() == starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py"} == files_in_repo

    def test_get_latest(self, git_repo):
        repo_path, repo = git_repo
        starting_commit = repo.head.commit

        file_path = repo_path / "new_test.py"
        with open(file_path, "w") as f:
            f.write("hello world")
        repo.index.add([file_path])
        repo.index.commit("Another commit")

        bundle = GitDagBundle(id="test", repo_url=repo_path, head="master")

        assert bundle.get_current_version() != starting_commit.hexsha

        files_in_repo = {f.name for f in bundle.path.iterdir() if f.is_file()}
        assert {"test_dag.py", "new_test.py"} == files_in_repo

    def test_refresh(self, git_repo):
        repo_path, repo = git_repo
        starting_commit = repo.head.commit

        bundle = GitDagBundle(id="test", repo_url=repo_path, head="master")

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
