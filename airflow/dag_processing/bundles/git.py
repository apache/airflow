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

import contextlib
import json
import os
import tempfile
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from git import Repo
from git.exc import BadName, GitCommandError, NoSuchPathError

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin

if TYPE_CHECKING:
    from pathlib import Path


class GitHook(BaseHook):
    """
    Hook for git repositories.

    :param git_conn_id: Connection ID for SSH connection to the repository

    """

    conn_name_attr = "git_conn_id"
    default_conn_name = "git_default"
    conn_type = "git"
    hook_name = "GIT"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": ["schema"],
            "relabeling": {
                "login": "Username",
                "host": "Repository URL",
                "password": "Access Token (optional)",
            },
            "placeholders": {
                "extra": json.dumps(
                    {
                        "key_file": "optional/path/to/keyfile",
                        "private_key": "optional inline private key",
                    }
                )
            },
        }

    def __init__(
        self, git_conn_id: str = "git_default", repo_url: str | None = None, *args, **kwargs
    ) -> None:
        super().__init__()
        connection = self.get_connection(git_conn_id)
        self.repo_url = repo_url or connection.host
        self.auth_token = connection.password
        self.private_key = connection.extra_dejson.get("private_key")
        self.key_file = connection.extra_dejson.get("key_file")
        self.strict_host_key_checking = connection.extra_dejson.get("strict_host_key_checking", "no")
        self.env: dict[str, str] = {}

        if self.key_file and self.private_key:
            raise AirflowException("Both 'key_file' and 'private_key' cannot be provided at the same time")
        self._process_git_auth_url()

    def _build_ssh_command(self, key_path: str) -> str:
        return (
            f"ssh -i {key_path} "
            f"-o IdentitiesOnly=yes "
            f"-o StrictHostKeyChecking={self.strict_host_key_checking}"
        )

    def _process_git_auth_url(self):
        if not isinstance(self.repo_url, str):
            return
        if self.auth_token and self.repo_url.startswith("https://"):
            self.repo_url = self.repo_url.replace("https://", f"https://{self.auth_token}@")
        elif not self.repo_url.startswith("git@") or not self.repo_url.startswith("https://"):
            self.repo_url = os.path.expanduser(self.repo_url)

    def set_git_env(self, key: str) -> None:
        self.env["GIT_SSH_COMMAND"] = self._build_ssh_command(key)

    @contextlib.contextmanager
    def configure_hook_env(self):
        if self.private_key:
            with tempfile.NamedTemporaryFile(mode="w", delete=True) as tmp_keyfile:
                tmp_keyfile.write(self.private_key)
                tmp_keyfile.flush()
                os.chmod(tmp_keyfile.name, 0o600)
                self.set_git_env(tmp_keyfile.name)
                yield
        else:
            self.set_git_env(self.key_file)
            yield


class GitDagBundle(BaseDagBundle, LoggingMixin):
    """
    git DAG bundle - exposes a git repository as a DAG bundle.

    Instead of cloning the repository every time, we clone the repository once into a bare repo from the source
    and then do a clone for each version from there.

    :param tracking_ref: Branch or tag for this DAG bundle
    :param subdir: Subdirectory within the repository where the DAGs are stored (Optional)
    :param git_conn_id: Connection ID for SSH/token based connection to the repository (Optional)
    :param repo_url: Explicit Git repository URL to override the connection's host. (Optional)
    """

    supports_versioning = True

    def __init__(
        self,
        *,
        tracking_ref: str,
        subdir: str | None = None,
        git_conn_id: str = "git_default",
        repo_url: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tracking_ref = tracking_ref
        self.subdir = subdir
        self.bare_repo_path = self._dag_bundle_root_storage_path / "git" / self.name
        self.repo_path = (
            self._dag_bundle_root_storage_path / "git" / (self.name + f"+{self.version or self.tracking_ref}")
        )
        self.git_conn_id = git_conn_id
        self.repo_url = repo_url
        try:
            self.hook = GitHook(git_conn_id=self.git_conn_id, repo_url=self.repo_url)
            self.repo_url = self.hook.repo_url
        except AirflowException as e:
            self.log.warning("Could not create GitHook for connection %s : %s", self.git_conn_id, e)

    def _initialize(self):
        with self.lock():
            with self.hook.configure_hook_env():
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
        if not self.repo_url:
            raise AirflowException(f"Connection {self.git_conn_id} doesn't have a host url")
        self._initialize()
        super().initialize()

    def _clone_repo_if_required(self) -> None:
        if not os.path.exists(self.repo_path):
            self.log.info("Cloning repository to %s from %s", self.repo_path, self.bare_repo_path)
            try:
                Repo.clone_from(
                    url=self.bare_repo_path,
                    to_path=self.repo_path,
                )
            except NoSuchPathError as e:
                # Protection should the bare repo be removed manually
                raise AirflowException("Repository path: %s not found", self.bare_repo_path) from e

        self.repo = Repo(self.repo_path)

    def _clone_bare_repo_if_required(self) -> None:
        if not self.repo_url:
            raise AirflowException(f"Connection {self.git_conn_id} doesn't have a host url")
        if not os.path.exists(self.bare_repo_path):
            self.log.info("Cloning bare repository to %s", self.bare_repo_path)
            try:
                Repo.clone_from(
                    url=self.repo_url,
                    to_path=self.bare_repo_path,
                    bare=True,
                    env=self.hook.env,
                )
            except GitCommandError as e:
                raise AirflowException("Error cloning repository") from e
        self.bare_repo = Repo(self.bare_repo_path)

    def _ensure_version_in_bare_repo(self) -> None:
        if not self.version:
            return
        if not self._has_version(self.bare_repo, self.version):
            self._fetch_bare_repo()
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

    def _fetch_bare_repo(self):
        refspecs = ["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"]
        if self.hook.env:
            with self.bare_repo.git.custom_environment(GIT_SSH_COMMAND=self.hook.env.get("GIT_SSH_COMMAND")):
                self.bare_repo.remotes.origin.fetch(refspecs)
        else:
            self.bare_repo.remotes.origin.fetch(refspecs)

    def refresh(self) -> None:
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            with self.hook.configure_hook_env():
                self._fetch_bare_repo()
                self.repo.remotes.origin.fetch(
                    ["+refs/heads/*:refs/remotes/origin/*", "+refs/tags/*:refs/tags/*"]
                )
                remote_branch = f"origin/{self.tracking_ref}"
                if remote_branch in [ref.name for ref in self.repo.remotes.origin.refs]:
                    target = remote_branch
                else:
                    target = self.tracking_ref
                self.repo.head.reset(target, index=True, working_tree=True)

    @staticmethod
    def _convert_git_ssh_url_to_https(url: str) -> str:
        if not url.startswith("git@"):
            raise ValueError(f"Invalid git SSH URL: {url}")
        parts = url.split(":")
        domain = parts[0].replace("git@", "https://")
        repo_path = parts[1].replace(".git", "")
        return f"{domain}/{repo_path}"

    def view_url(self, version: str | None = None) -> str | None:
        if not version:
            return None
        url = self.repo_url
        if not url:
            return None
        if url.startswith("git@"):
            url = self._convert_git_ssh_url_to_https(url)
        if url.endswith(".git"):
            url = url[:-4]
        parsed_url = urlparse(url)
        host = parsed_url.hostname
        if not host:
            return None
        host_patterns = {
            "github.com": f"{url}/tree/{version}",
            "gitlab.com": f"{url}/-/tree/{version}",
            "bitbucket.org": f"{url}/src/{version}",
        }
        for allowed_host, template in host_patterns.items():
            if host == allowed_host or host.endswith(f".{allowed_host}"):
                return template
        return None
