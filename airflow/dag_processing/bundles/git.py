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
from typing import TYPE_CHECKING

from git import Repo

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from pathlib import Path


class GitDagBundle(BaseDagBundle):
    """
    git DAG bundle - exposes a git repository as a DAG bundle.

    Instead of cloning the repository every time, we clone the repository once into a bare repo from the source
    and then do a clone for each version from there.

    :param repo_url: URL of the git repository
    :param head: Branch or tag for this DAG bundle
    """

    supports_versioning = True

    def __init__(self, *, repo_url: str, head: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.repo_url = repo_url
        self.head = head

        self.bare_repo_path = self._dag_bundle_root_storage_path / "git" / self.name
        self._ensure_bare_repo_exists()
        self._ensure_version_in_bare_repo()
        self._ensure_repo_exists()

        if self.version:
            if not self._has_version(self.repo, self.version):
                self.repo.remotes.origin.fetch()

            self.repo.head.set_reference(self.repo.commit(self.version))
            self.repo.head.reset(index=True, working_tree=True)
        else:
            self.refresh()

    def _ensure_repo_exists(self) -> None:
        if not os.path.exists(self.path):
            Repo.clone_from(
                url=self.bare_repo_path,
                to_path=self.path,
            )
        self.repo = Repo(self.path)
        self.repo.git.checkout(self.head)

    def _ensure_bare_repo_exists(self) -> None:
        if not os.path.exists(self.bare_repo_path):
            Repo.clone_from(
                url=self.repo_url,
                to_path=self.bare_repo_path,
                bare=True,
            )
        self.bare_repo = Repo(self.bare_repo_path)

    def _ensure_version_in_bare_repo(self) -> None:
        if not self.version:
            return
        if not self._has_version(self.bare_repo, self.version):
            self.bare_repo.remotes.origin.fetch("+refs/heads/*:refs/heads/*")
            if not self._has_version(self.bare_repo, self.version):
                raise AirflowException(f"Version {self.version} not found in the repository")

    def __hash__(self) -> int:
        return hash((self.name, self.get_current_version()))

    def get_current_version(self) -> str:
        return self.repo.head.commit.hexsha

    @property
    def path(self) -> Path:
        location = self.version or self.head
        return self._dag_bundle_root_storage_path / "git" / f"{self.name}+{location}"

    @staticmethod
    def _has_version(repo: Repo, version: str) -> bool:
        try:
            repo.commit(version)
            return True
        except Exception:
            return False

    def refresh(self) -> None:
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        self.bare_repo.remotes.origin.fetch("+refs/heads/*:refs/heads/*")
        self.repo.remotes.origin.pull()
