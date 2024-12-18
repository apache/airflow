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
import tempfile
from typing import TYPE_CHECKING

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
    """

    supports_versioning = True

    def __init__(
        self,
        *,
        tracking_ref: str,
        subdir: str | None = None,
        ssh_conn_id: str | None = None,
        repo_url: str | os.PathLike = "",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.repo_url = repo_url
        self.tracking_ref = tracking_ref
        self.subdir = subdir
        self.ssh_conn_id = ssh_conn_id
        self.bare_repo_path = self._dag_bundle_root_storage_path / "git" / self.name
        self.repo_path = (
            self._dag_bundle_root_storage_path / "git" / (self.name + f"+{self.version or self.tracking_ref}")
        )
        self.env: dict[str, str] = {}
        self.pkey: str | None = None
        self.key_file: str | None = None

    def _clone_from(self, to_path: Path, bare: bool = False) -> Repo:
        return Repo.clone_from(self.repo_url, to_path, bare=bare, env=self.env)

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
            self.refresh()

    def initialize(self) -> None:
        if self.ssh_conn_id:
            try:
                from airflow.providers.ssh.hooks.ssh import SSHHook
            except ImportError as e:
                raise AirflowOptionalProviderFeatureException(e)
            ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
            ssh_hook.get_conn()
            temp_key_file_path = None
            self.key_file = ssh_hook.key_file
            try:
                if not self.key_file:
                    conn = ssh_hook.get_connection(self.ssh_conn_id)
                    private_key: str | None = conn.extra_dejson.get("private_key")
                    if not private_key:
                        raise AirflowException("No private key present in connection")
                    self.pkey = private_key
                    with tempfile.NamedTemporaryFile(delete=False) as key_file:
                        temp_key_file_path = key_file.name
                        key_file.write(self.pkey.encode("utf-8"))
                    self.env["GIT_SSH_COMMAND"] = f"ssh -i {temp_key_file_path} -o IdentitiesOnly=yes"
                else:
                    self.env["GIT_SSH_COMMAND"] = f"ssh -i {self.key_file} -o IdentitiesOnly=yes"
                if ssh_hook.remote_host:
                    self.log.info("Using repo URL defined in the SSH connection")
                    self.repo_url = ssh_hook.remote_host
                self._initialize()
                self.env = {}
            finally:
                if temp_key_file_path:
                    os.remove(temp_key_file_path)
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

    def refresh(self) -> None:
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        def _refresh():
            self.bare_repo.remotes.origin.fetch("+refs/heads/*:refs/heads/*")
            self.repo.remotes.origin.pull()

        if self.env:
            _refresh()
        elif self.ssh_conn_id:
            temp_key_file_path = None
            try:
                if not self.key_file:
                    if not self.pkey:
                        raise AirflowException("Missing private key, please initialize the bundle first")
                    with tempfile.NamedTemporaryFile(delete=False) as key_file:
                        temp_key_file_path = key_file.name
                        key_file.write(self.pkey.encode("utf-8"))
                    GIT_SSH_COMMAND = f"ssh -i {temp_key_file_path} -o IdentitiesOnly=yes"
                else:
                    GIT_SSH_COMMAND = f"ssh -i {self.key_file} -o IdentitiesOnly=yes"
                with self.repo.git.custom_environment(GIT_SSH_COMMAND=GIT_SSH_COMMAND):
                    _refresh()
            finally:
                if temp_key_file_path:
                    os.remove(temp_key_file_path)
        else:
            _refresh()
