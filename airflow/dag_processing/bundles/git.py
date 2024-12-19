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
from typing import TYPE_CHECKING, Any

from git import Repo
from git.exc import BadName

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException, AirflowOptionalProviderFeatureException
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from pathlib import Path


class GitDagBundle(BaseDagBundle, LoggingMixin):
    """
    git DAG bundle - exposes a git repository as a DAG bundle.

    Instead of cloning the repository every time, we clone the repository once into a bare repo from the source
    and then do a clone for each version from there.

    :param repo_url: URL of the git repository
    :param tracking_ref: Branch or tag for this DAG bundle
    :param subdir: Subdirectory within the repository where the DAGs are stored (Optional)
    :param ssh_conn_id: Connection ID for SSH connection to the repository (Optional)
    """

    supports_versioning = True

    def __init__(
        self,
        *,
        repo_url: str,
        tracking_ref: str,
        subdir: str | None = None,
        ssh_conn_kwargs: dict[str, str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.repo_url = repo_url
        self.tracking_ref = tracking_ref
        self.subdir = subdir
        self.bare_repo_path = self._dag_bundle_root_storage_path / "git" / self.name
        self.repo_path = (
            self._dag_bundle_root_storage_path / "git" / (self.name + f"+{self.version or self.tracking_ref}")
        )
        self.ssh_conn_kwargs = ssh_conn_kwargs
        self.ssh_hook: Any | None = None

    def _clone_from(self, to_path: Path, bare: bool = False) -> Repo:
        self.log.info("Cloning %s to %s", self.repo_url, to_path)
        return Repo.clone_from(self.repo_url, to_path, bare=bare)

    def _initialize(self):
        self._clone_bare_repo_if_required()
        self._ensure_version_in_bare_repo()
        self._clone_repo_if_required()
        self.repo.git.checkout(self.tracking_ref)

        if self.version:
            if not self._has_version(self.repo, self.version):
                self.repo.remotes.origin.fetch()

            self.repo.head.set_reference(self.repo.commit(self.version))
            self.repo.head.reset(index=True, working_tree=True)
        else:
            self._refresh()

    def _ssh_hook(self):
        try:
            from airflow.providers.ssh.hooks.ssh import SSHHook
        except ImportError as e:
            raise AirflowOptionalProviderFeatureException(e)
        return SSHHook(**self.ssh_conn_kwargs)

    def initialize(self) -> None:
        if self.ssh_conn_kwargs:
            if not self.repo_url.startswith("git@") and not self.repo_url.endswith(".git"):
                raise AirflowException(
                    f"Invalid git URL: {self.repo_url}. URL must start with git@ and end with .git"
                )
            self.ssh_hook = self._ssh_hook()
            with self.ssh_hook.get_conn():
                self._initialize()
        else:
            self._initialize()

    def _clone_repo_if_required(self) -> None:
        if not os.path.exists(self.repo_path):
            self._clone_from(
                to_path=self.repo_path,
            )
        self.repo = Repo(self.repo_path)

    def _clone_bare_repo_if_required(self) -> None:
        if not os.path.exists(self.bare_repo_path):
            self._clone_from(
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

    def __repr__(self):
        return (
            f"<GitDagBundle("
            f"name={self.name!r}, "
            f"tracking_ref={self.tracking_ref!r}, "
            f"subdir={self.subdir!r}, "
            f"version={self.version!r}"
            f")>"
        )

    def get_current_version(self) -> str:
        return self.repo.head.commit.hexsha

    @property
    def path(self) -> Path:
        if self.subdir:
            return self.repo_path / self.subdir
        return self.repo_path

    @staticmethod
    def _has_version(repo: Repo, version: str) -> bool:
        try:
            repo.commit(version)
            return True
        except BadName:
            return False

    def _refresh(self):
        self.bare_repo.remotes.origin.fetch("+refs/heads/*:refs/heads/*")
        self.repo.remotes.origin.pull()

    def refresh(self) -> None:
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        if self.ssh_hook:
            with self.ssh_hook.get_conn():
                self._refresh()
        else:
            self._refresh()
