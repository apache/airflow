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
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

from github import GithubException

from airflow import AirflowException
from airflow.providers.github.hooks.github import GithubHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GithubSensor(BaseSensorOperator):
    """
    Base GithubSensor which can monitor for any change.

    :param github_conn_id: reference to a pre-defined GitHub Connection
    :param method_name: method name from PyGithub to be executed
    :param method_params: parameters for the method method_name
    :param result_processor: function that return boolean and act as a sensor response
    """

    def __init__(
        self,
        *,
        method_name: str,
        github_conn_id: str = "github_default",
        method_params: dict | None = None,
        result_processor: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.github_conn_id = github_conn_id
        self.result_processor = None
        if result_processor is not None:
            self.result_processor = result_processor
        self.method_name = method_name
        self.method_params = method_params

    def poke(self, context: Context) -> bool:
        hook = GithubHook(github_conn_id=self.github_conn_id)
        github_result = getattr(hook.client, self.method_name)(**self.method_params)

        if self.result_processor:
            return self.result_processor(github_result)

        return github_result


class BaseGithubRepositorySensor(GithubSensor):
    """
    Base GitHub sensor at Repository level.

    :param github_conn_id: reference to a pre-defined GitHub Connection
    :param repository_name: full qualified name of the repository to be monitored, ex. "apache/airflow"
    """

    def __init__(
        self,
        *,
        github_conn_id: str = "github_default",
        repository_name: str | None = None,
        result_processor: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            github_conn_id=github_conn_id,
            result_processor=result_processor,
            method_name="get_repo",
            method_params={"full_name_or_id": repository_name},
            **kwargs,
        )

    def poke(self, context: Context) -> bool:
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise AirflowException("Override me.")


class GithubTagSensor(BaseGithubRepositorySensor):
    """
    Monitors a github tag for its creation.

    :param github_conn_id: reference to a pre-defined GitHub Connection
    :param tag_name: name of the tag to be monitored
    :param repository_name: fully qualified name of the repository to be monitored, ex. "apache/airflow"
    """

    template_fields = ("tag_name",)

    def __init__(
        self,
        *,
        github_conn_id: str = "github_default",
        tag_name: str | None = None,
        repository_name: str | None = None,
        **kwargs,
    ) -> None:
        self.repository_name = repository_name
        self.tag_name = tag_name
        super().__init__(
            github_conn_id=github_conn_id,
            repository_name=repository_name,
            result_processor=self.tag_checker,
            **kwargs,
        )

    def poke(self, context: Context) -> bool:
        self.log.info("Poking for tag: %s in repository: %s", self.tag_name, self.repository_name)
        return GithubSensor.poke(self, context=context)

    def tag_checker(self, repo: Any) -> bool | None:
        """Checking existence of Tag in a Repository"""
        result = None
        try:
            if repo is not None and self.tag_name is not None:
                all_tags = [x.name for x in repo.get_tags()]
                result = self.tag_name in all_tags

        except GithubException as github_error:  # type: ignore[misc]
            raise AirflowException(f"Failed to execute GithubSensor, error: {str(github_error)}")
        except Exception as e:
            raise AirflowException(f"GitHub operator error: {str(e)}")

        if result is True:
            self.log.info("Tag %s exists in %s repository, Success.", self.tag_name, self.repository_name)
        else:
            self.log.info("Tag %s doesn't exists in %s repository yet.", self.tag_name, self.repository_name)
        return result
