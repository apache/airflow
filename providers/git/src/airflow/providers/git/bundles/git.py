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
import shutil
from contextlib import nullcontext
from pathlib import Path
from urllib.parse import urlparse

import structlog
from git import Repo
from git.exc import BadName, GitCommandError, InvalidGitRepositoryError, NoSuchPathError
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from airflow.dag_processing.bundles.base import BaseDagBundle
from airflow.exceptions import AirflowException
from airflow.providers.git.hooks.git import GitHook

log = structlog.get_logger(__name__)


class GitDagBundle(BaseDagBundle):
    """
    git DAG bundle - exposes a git repository as a DAG bundle.

    Instead of cloning the repository every time, we clone the repository once into a bare repo from the source
    and then do a clone for each version from there.

    :param tracking_ref: Branch or tag for this DAG bundle
    :param subdir: Subdirectory within the repository where the DAGs are stored (Optional)
    :param git_conn_id: Connection ID for SSH/token based connection to the repository (Optional)
    :param repo_url: Explicit Git repository URL to override the connection's host. (Optional)
    :param prune_dotgit_folder: Remove .git folder from the versions after cloning.

        The per-version clone is not a full "git" copy (it makes use of git's `--local` ability
        to share the object directory via hard links, but if you have a lot of current versions
        running, or an especially large git repo leaving this as True will save some disk space
        at the expense of `git` operations not working in the bundle that Tasks run from.
    """

    supports_versioning = True

    def __init__(
        self,
        *,
        tracking_ref: str,
        subdir: str | None = None,
        git_conn_id: str | None = None,
        repo_url: str | None = None,
        prune_dotgit_folder: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tracking_ref = tracking_ref
        self.subdir = subdir
        self.bare_repo_path = self.base_dir / "bare"
        if self.version:
            self.repo_path = self.versions_dir / self.version
        else:
            self.repo_path = self.base_dir / "tracking_repo"
        self.git_conn_id = git_conn_id
        self.repo_url = repo_url
        self.prune_dotgit_folder = prune_dotgit_folder

        self._log = log.bind(
            bundle_name=self.name,
            version=self.version,
            bare_repo_path=self.bare_repo_path,
            repo_path=self.repo_path,
            versions_path=self.versions_dir,
            git_conn_id=self.git_conn_id,
        )

        self._log.debug("bundle configured")
        self.hook: GitHook | None = None
        try:
            self.hook = GitHook(git_conn_id=git_conn_id or "git_default", repo_url=self.repo_url)
        except Exception as e:
            # re raise so exception propagates immediately with clear error message
            raise

        if self.hook and self.hook.repo_url:
            self.repo_url = self.hook.repo_url
            self._log.debug("repo_url updated from hook")

    def _initialize(self):
        with self.lock():
            cm = self.hook.configure_hook_env() if self.hook else nullcontext()
            with cm:
                try:
                    self._clone_bare_repo_if_required()
                except GitCommandError as e:
                    raise RuntimeError("Error cloning repository") from e
                except InvalidGitRepositoryError as e:
                    raise RuntimeError(f"Invalid git repository at {self.bare_repo_path}") from e
                self._ensure_version_in_bare_repo()
            self.bare_repo.close()

            try:
                self._clone_repo_if_required()
            except GitCommandError as e:
                raise RuntimeError("Error cloning repository") from e
            except InvalidGitRepositoryError as e:
                raise RuntimeError(f"Invalid git repository at {self.repo_path}") from e
            self.repo.git.checkout(self.tracking_ref)
            self._log.debug("bundle initialize", version=self.version)
            if self.version:
                if not self._has_version(self.repo, self.version):
                    self.repo.remotes.origin.fetch()
                self.repo.head.set_reference(str(self.repo.commit(self.version)))
                self.repo.head.reset(index=True, working_tree=True)
                if self.prune_dotgit_folder:
                    shutil.rmtree(self.repo_path / ".git")
            else:
                self.refresh()
            self.repo.close()

    def initialize(self) -> None:
        if not self.repo_url:
            raise AirflowException(f"Connection {self.git_conn_id} doesn't have a host url")
        self._initialize()
        super().initialize()

    @retry(
        retry=retry_if_exception_type((InvalidGitRepositoryError, GitCommandError)),
        stop=stop_after_attempt(2),
        reraise=True,
    )
    def _clone_repo_if_required(self) -> None:
        try:
            if not os.path.exists(self.repo_path):
                self._log.info(
                    "Cloning repository", repo_path=self.repo_path, bare_repo_path=self.bare_repo_path
                )
                Repo.clone_from(
                    url=self.bare_repo_path,
                    to_path=self.repo_path,
                )
            else:
                self._log.debug("repo exists", repo_path=self.repo_path)
            self.repo = Repo(self.repo_path)
        except NoSuchPathError as e:
            # Protection should the bare repo be removed manually
            raise AirflowException("Repository path: %s not found", self.bare_repo_path) from e
        except (InvalidGitRepositoryError, GitCommandError) as e:
            self._log.warning(
                "Repository clone/open failed, cleaning up and retrying",
                repo_path=self.repo_path,
                exc=e,
            )
            if os.path.exists(self.repo_path):
                shutil.rmtree(self.repo_path)
            raise

    @retry(
        retry=retry_if_exception_type((InvalidGitRepositoryError, GitCommandError)),
        stop=stop_after_attempt(2),
        reraise=True,
    )
    def _clone_bare_repo_if_required(self) -> None:
        if not self.repo_url:
            raise AirflowException(f"Connection {self.git_conn_id} doesn't have a host url")

        try:
            if not os.path.exists(self.bare_repo_path):
                self._log.info("Cloning bare repository", bare_repo_path=self.bare_repo_path)
                Repo.clone_from(
                    url=self.repo_url,
                    to_path=self.bare_repo_path,
                    bare=True,
                    env=self.hook.env if self.hook else None,
                )
            self.bare_repo = Repo(self.bare_repo_path)

            # Fetch to ensure we have latest refs and validate repo integrity
            self._fetch_bare_repo()
        except (InvalidGitRepositoryError, GitCommandError) as e:
            self._log.warning(
                "Bare repository clone/open/fetch failed, cleaning up and retrying",
                bare_repo_path=self.bare_repo_path,
                exc=e,
            )
            if os.path.exists(self.bare_repo_path):
                shutil.rmtree(self.bare_repo_path)
            raise

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
        with self.repo as repo:
            return repo.head.commit.hexsha

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
        except (BadName, ValueError):
            return False

    def _fetch_bare_repo(self):
        refspecs = ["+refs/heads/*:refs/heads/*", "+refs/tags/*:refs/tags/*"]
        cm = nullcontext()
        if self.hook and (cmd := self.hook.env.get("GIT_SSH_COMMAND")):
            cm = self.bare_repo.git.custom_environment(GIT_SSH_COMMAND=cmd)
        with cm:
            self.bare_repo.remotes.origin.fetch(refspecs)
            self.bare_repo.close()

    def refresh(self) -> None:
        if self.version:
            raise AirflowException("Refreshing a specific version is not supported")

        with self.lock():
            cm = self.hook.configure_hook_env() if self.hook else nullcontext()
            with cm:
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
                self.repo.close()

    @staticmethod
    def _convert_git_ssh_url_to_https(url: str) -> str:
        if not url.startswith("git@"):
            raise ValueError(f"Invalid git SSH URL: {url}")
        parts = url.split(":")
        domain = parts[0].replace("git@", "https://")
        repo_path = parts[1].replace(".git", "")
        return f"{domain}/{repo_path}"

    def view_url(self, version: str | None = None) -> str | None:
        """
        Return a URL for viewing the DAGs in the repository.

        This method is deprecated and will be removed when the minimum supported Airflow version is 3.1.
        Use `view_url_template` instead.
        """
        if not version:
            return None
        template = self.view_url_template()
        if not template:
            return None
        if not self.subdir:
            # remove {subdir} from the template if subdir is not set
            template = template.replace("/{subdir}", "")
        return template.format(version=version, subdir=self.subdir)

    def view_url_template(self) -> str | None:
        if hasattr(self, "_view_url_template") and self._view_url_template:
            # Because we use this method in the view_url method, we need to handle
            # backward compatibility for Airflow versions that doesn't have the
            # _view_url_template attribute. Should be removed when we drop support for Airflow 3.0
            return self._view_url_template

        if not self.repo_url:
            return None

        url = self.repo_url
        if url.startswith("git@"):
            url = self._convert_git_ssh_url_to_https(url)
        if url.endswith(".git"):
            url = url[:-4]

        parsed_url = urlparse(url)
        host = parsed_url.hostname
        if not host:
            return None

        if parsed_url.username or parsed_url.password:
            new_netloc = host
            if parsed_url.port:
                new_netloc += f":{parsed_url.port}"
            url = parsed_url._replace(netloc=new_netloc).geturl()

        host_patterns = {
            "github.com": f"{url}/tree/{{version}}",
            "gitlab.com": f"{url}/-/tree/{{version}}",
            "bitbucket.org": f"{url}/src/{{version}}",
        }

        for allowed_host, template in host_patterns.items():
            if host == allowed_host or host.endswith(f".{allowed_host}"):
                if self.subdir:
                    return f"{template}/{self.subdir}"
                return template
        return None
