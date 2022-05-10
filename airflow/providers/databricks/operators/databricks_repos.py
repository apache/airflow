#
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
#
"""This module contains Databricks operators."""
import re
from typing import TYPE_CHECKING, Optional, Sequence
from urllib.parse import urlparse

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class DatabricksReposCreateOperator(BaseOperator):
    """
    Creates a Databricks Repo
    using
    `POST api/2.0/repos <https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/create-repo>`_
    API endpoint and optionally checking it out to a specific branch or tag.

    :param git_url: Required HTTPS URL of a Git repository
    :param git_provider: Optional name of Git provider. Must be provided if we can't guess its name from URL.
    :param repo_path: optional path for a repository. Must be in the format ``/Repos/{folder}/{repo-name}``.
        If not specified, it will be created in the user's directory.
    :param branch: optional name of branch to check out.
    :param tag: optional name of tag to checkout.
    :param ignore_existing_repo: don't throw exception if repository with given path already exists.
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ('repo_path', 'tag', 'branch')

    __git_providers__ = {
        "github.com": "gitHub",
        "dev.azure.com": "azureDevOpsServices",
        "gitlab.com": "gitLab",
        "bitbucket.org": "bitbucketCloud",
    }
    __aws_code_commit_regexp__ = re.compile(r"^git-codecommit\.[^.]+\.amazonaws.com$")
    __repos_path_regexp__ = re.compile(r"/Repos/[^/]+/[^/]+/?$")

    def __init__(
        self,
        *,
        git_url: str,
        git_provider: Optional[str] = None,
        branch: Optional[str] = None,
        tag: Optional[str] = None,
        repo_path: Optional[str] = None,
        ignore_existing_repo: bool = False,
        databricks_conn_id: str = 'databricks_default',
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksReposCreateOperator``."""
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        self.git_url = git_url
        self.ignore_existing_repo = ignore_existing_repo
        if git_provider is None:
            self.git_provider = self.__detect_repo_provider__(git_url)
            if self.git_provider is None:
                raise AirflowException(
                    "git_provider isn't specified and couldn't be guessed for URL {git_url}"
                )
        else:
            self.git_provider = git_provider
        self.repo_path = repo_path
        if branch is not None and tag is not None:
            raise AirflowException("Only one of branch or tag should be provided, but not both")
        self.branch = branch
        self.tag = tag

    @staticmethod
    def __detect_repo_provider__(url):
        provider = None
        try:
            netloc = urlparse(url).netloc
            idx = netloc.rfind("@")
            if idx != -1:
                netloc = netloc[(idx + 1) :]
            netloc = netloc.lower()
            provider = DatabricksReposCreateOperator.__git_providers__.get(netloc)
            if provider is None and DatabricksReposCreateOperator.__aws_code_commit_regexp__.match(netloc):
                provider = "awsCodeCommit"
        except ValueError:
            pass
        return provider

    def _get_hook(self) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context: 'Context'):
        """
        Creates a Databricks Repo

        :param context: context
        :return: Repo ID
        """
        payload = {
            "url": self.git_url,
            "provider": self.git_provider,
        }
        if self.repo_path is not None:
            if not self.__repos_path_regexp__.match(self.repo_path):
                raise AirflowException(
                    f"repo_path should have form of /Repos/{{folder}}/{{repo-name}}, got '{self.repo_path}'"
                )
            payload["path"] = self.repo_path
        hook = self._get_hook()
        existing_repo_id = None
        if self.repo_path is not None:
            existing_repo_id = hook.get_repo_by_path(self.repo_path)
            if existing_repo_id is not None and not self.ignore_existing_repo:
                raise AirflowException(f"Repo with path '{self.repo_path}' already exists")
        if existing_repo_id is None:
            result = hook.create_repo(payload)
            repo_id = result["id"]
        else:
            repo_id = existing_repo_id
        # update repo if necessary
        if self.branch is not None:
            hook.update_repo(str(repo_id), {'branch': str(self.branch)})
        elif self.tag is not None:
            hook.update_repo(str(repo_id), {'tag': str(self.tag)})

        return repo_id


class DatabricksReposUpdateOperator(BaseOperator):
    """
    Updates specified repository to a given branch or tag
    using `PATCH api/2.0/repos
    <https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/update-repo>`_ API endpoint.

    :param branch: optional name of branch to update to. Should be specified if ``tag`` is omitted
    :param tag: optional name of tag to update to. Should be specified if ``branch`` is omitted
    :param repo_id: optional ID of existing repository. Should be specified if ``repo_path`` is omitted
    :param repo_path: optional path of existing repository. Should be specified if ``repo_id`` is omitted
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ('repo_path', 'tag', 'branch')

    def __init__(
        self,
        *,
        branch: Optional[str] = None,
        tag: Optional[str] = None,
        repo_id: Optional[str] = None,
        repo_path: Optional[str] = None,
        databricks_conn_id: str = 'databricks_default',
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksReposUpdateOperator``."""
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        if branch is not None and tag is not None:
            raise AirflowException("Only one of branch or tag should be provided, but not both")
        if branch is None and tag is None:
            raise AirflowException("One of branch or tag should be provided")
        if repo_id is not None and repo_path is not None:
            raise AirflowException("Only one of repo_id or repo_path should be provided, but not both")
        if repo_id is None and repo_path is None:
            raise AirflowException("One of repo_id or repo_path should be provided")
        self.repo_path = repo_path
        self.repo_id = repo_id
        self.branch = branch
        self.tag = tag

    def _get_hook(self) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context: 'Context'):
        hook = self._get_hook()
        if self.repo_path is not None:
            self.repo_id = hook.get_repo_by_path(self.repo_path)
            if self.repo_id is None:
                raise AirflowException(f"Can't find Repo ID for path '{self.repo_path}'")
        if self.branch is not None:
            payload = {'branch': str(self.branch)}
        else:
            payload = {'tag': str(self.tag)}

        result = hook.update_repo(str(self.repo_id), payload)
        return result['head_commit_id']


class DatabricksReposDeleteOperator(BaseOperator):
    """
    Deletes specified repository
    using `DELETE api/2.0/repos
    <https://docs.databricks.com/dev-tools/api/latest/repos.html#operation/delete-repo>`_ API endpoint.

    :param repo_id: optional ID of existing repository. Should be specified if ``repo_path`` is omitted
    :param repo_path: optional path of existing repository. Should be specified if ``repo_id`` is omitted
    :param databricks_conn_id: Reference to the :ref:`Databricks connection <howto/connection:databricks>`.
        By default and in the common case this will be ``databricks_default``. To use
        token based authentication, provide the key ``token`` in the extra field for the
        connection and create the key ``host`` and leave the ``host`` field empty.
    :param databricks_retry_limit: Amount of times retry if the Databricks backend is
        unreachable. Its value must be greater than or equal to 1.
    :param databricks_retry_delay: Number of seconds to wait between retries (it
            might be a floating point number).
    """

    # Used in airflow.models.BaseOperator
    template_fields: Sequence[str] = ('repo_path',)

    def __init__(
        self,
        *,
        repo_id: Optional[str] = None,
        repo_path: Optional[str] = None,
        databricks_conn_id: str = 'databricks_default',
        databricks_retry_limit: int = 3,
        databricks_retry_delay: int = 1,
        **kwargs,
    ) -> None:
        """Creates a new ``DatabricksReposDeleteOperator``."""
        super().__init__(**kwargs)
        self.databricks_conn_id = databricks_conn_id
        self.databricks_retry_limit = databricks_retry_limit
        self.databricks_retry_delay = databricks_retry_delay
        if repo_id is not None and repo_path is not None:
            raise AirflowException("Only one of repo_id or repo_path should be provided, but not both")
        if repo_id is None and repo_path is None:
            raise AirflowException("One of repo_id repo_path tag should be provided")
        self.repo_path = repo_path
        self.repo_id = repo_id

    def _get_hook(self) -> DatabricksHook:
        return DatabricksHook(
            self.databricks_conn_id,
            retry_limit=self.databricks_retry_limit,
            retry_delay=self.databricks_retry_delay,
        )

    def execute(self, context: 'Context'):
        hook = self._get_hook()
        if self.repo_path is not None:
            self.repo_id = hook.get_repo_by_path(self.repo_path)
            if self.repo_id is None:
                raise AirflowException(f"Can't find Repo ID for path '{self.repo_path}'")

        hook.delete_repo(str(self.repo_id))
