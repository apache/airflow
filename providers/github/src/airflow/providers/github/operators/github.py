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
from airflow.models import BaseOperator
from airflow.providers.github.hooks.github import GithubHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class GithubOperator(BaseOperator):
    """
    GithubOperator to interact and perform action on GitHub API.
    This operator is designed to use GitHub Python SDK: https://github.com/PyGithub/PyGithub

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:GithubOperator`

    :param github_conn_id: reference to a pre-defined GitHub Connection
    :param github_method: method name from GitHub Python SDK to be called
    :param github_method_args: required method parameters for the github_method. (templated)
    :param result_processor: function to further process the response from GitHub API
    """

    template_fields = ("github_method_args",)

    def __init__(
        self,
        *,
        github_method: str,
        github_conn_id: str = "github_default",
        github_method_args: dict | None = None,
        result_processor: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.github_conn_id = github_conn_id
        self.method_name = github_method
        self.github_method_args = github_method_args
        self.result_processor = result_processor

    def execute(self, context: Context) -> Any:
        try:
            # Default method execution is on the top level GitHub client
            hook = GithubHook(github_conn_id=self.github_conn_id)
            resource = hook.client

            github_result = getattr(resource, self.method_name)(**self.github_method_args)
            if self.result_processor:
                return self.result_processor(github_result)

            return github_result

        except GithubException as github_error:
            raise AirflowException(f"Failed to execute GithubOperator, error: {str(github_error)}")
        except Exception as e:
            raise AirflowException(f"GitHub operator error: {str(e)}")
