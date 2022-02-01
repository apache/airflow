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

from typing import TYPE_CHECKING, Any, Callable, Optional

from github import GithubException

from airflow import AirflowException
from airflow.providers.github.operators.github import GithubOperator
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GithubSensor(BaseSensorOperator):
    """
    Base GithubSensor which can monitor for any change.

    :param github_conn_id: reference to a pre-defined Github Connection
    :type github_conn_id: str
    :param method_name: method name from PyGithub to be executed
    :type method_name: str
    :param method_params: parameters for the method method_name
    :type method_params: dict
    :param result_processor: function that return boolean and act as a sensor response
    :type result_processor: function
    """

    def __init__(
        self,
        *,
        method_name: str,
        github_conn_id: str = 'github_default',
        method_params: Optional[dict] = None,
        result_processor: Optional[Callable] = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.github_conn_id = github_conn_id
        self.result_processor = None
        if result_processor is not None:
            self.result_processor = result_processor
        self.method_name = method_name
        self.method_params = method_params
        self.github_operator = GithubOperator(
            task_id=self.task_id,
            github_conn_id=self.github_conn_id,
            github_method=self.method_name,
            github_method_args=self.method_params,
            result_processor=self.result_processor,
        )

    def poke(self, context: 'Context') -> bool:
        return self.github_operator.execute(context=context)


class BaseGithubRepositorySensor(GithubSensor):
    """
    Base GitHub sensor at Repository level.

    :param github_conn_id: reference to a pre-defined GitHub Connection
    :type github_conn_id: str
    :param repository_name: full qualified name of the repository to be monitored, ex. "apache/airflow"
    :type repository_name: str
    """

    def __init__(
        self,
        *,
        github_conn_id: str = 'github_default',
        repository_name: Optional[str] = None,
        result_processor: Optional[Callable] = None,
        **kwargs,
    ) -> None:
        super().__init__(
            github_conn_id=github_conn_id,
            result_processor=result_processor,
            method_name="get_repo",
            method_params={'full_name_or_id': repository_name},
            **kwargs,
        )

    def poke(self, context: 'Context') -> bool:
        """
        Function that the sensors defined while deriving this class should
        override.
        """
        raise AirflowException('Override me.')


class GithubTagSensor(BaseGithubRepositorySensor):
    """
    Monitors a github tag for its creation.

    :param github_conn_id: reference to a pre-defined Github Connection
    :type github_conn_id: str
    :param tag_name: name of the tag to be monitored
    :type tag_name: str
    :param repository_name: fully qualified name of the repository to be monitored, ex. "apache/airflow"
    :type repository_name: str
    """

    template_fields = ("tag_name",)

    def __init__(
        self,
        *,
        github_conn_id: str = 'github_default',
        tag_name: Optional[str] = None,
        repository_name: Optional[str] = None,
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

    def poke(self, context: 'Context') -> bool:
        self.log.info('Poking for tag: %s in repository: %s', self.tag_name, self.repository_name)
        return GithubSensor.poke(self, context=context)

    def tag_checker(self, repo: Any) -> Optional[bool]:
        """Checking existence of Tag in a Repository"""
        result = None
        try:
            if repo is not None and self.tag_name is not None:
                all_tags = [x.name for x in repo.get_tags()]
                result = self.tag_name in all_tags

        except GithubException as github_error:
            raise AirflowException(f"Failed to execute GithubSensor, error: {str(github_error)}")
        except Exception as e:
            raise AirflowException(f"Github operator error: {str(e)}")

        if result is True:
            self.log.info("Tag %s exists in %s repository, Success.", self.tag_name, self.repository_name)
        else:
            self.log.info("Tag %s doesn't exists in %s repository yet.", self.tag_name, self.repository_name)
        return result
