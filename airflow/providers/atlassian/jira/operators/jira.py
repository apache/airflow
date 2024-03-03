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

from typing import TYPE_CHECKING, Any, Callable, Sequence

from airflow.models import BaseOperator
from airflow.providers.atlassian.jira.hooks.jira import JiraHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class JiraOperator(BaseOperator):
    """JiraOperator to interact and perform action on Jira issue tracking system.

    This operator is designed to use Atlassian Jira SDK. For more information:
    https://atlassian-python-api.readthedocs.io/jira.html

    :param jira_conn_id: Reference to a pre-defined Jira Connection.
    :param jira_method: Method name from Atlassian Jira Python SDK to be called.
    :param jira_method_args: Method parameters for the jira_method. (templated)
    :param result_processor: Function to further process the response from Jira.
    :param get_jira_resource_method: Function or operator to get Jira resource on which the provided
        jira_method will be executed.
    """

    template_fields: Sequence[str] = ("jira_method_args",)

    def __init__(
        self,
        *,
        jira_method: str,
        jira_conn_id: str = "jira_default",
        jira_method_args: dict | None = None,
        result_processor: Callable | None = None,
        get_jira_resource_method: Callable | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.jira_conn_id = jira_conn_id
        self.method_name = jira_method
        self.jira_method_args = jira_method_args or {}
        self.result_processor = result_processor
        self.get_jira_resource_method = get_jira_resource_method

    def execute(self, context: Context) -> Any:
        if self.get_jira_resource_method is not None:
            # if get_jira_resource_method is provided, jira_method will be executed on
            # resource returned by executing the get_jira_resource_method.
            # This makes all the provided methods of atlassian-python-api JIRA sdk accessible and usable
            # directly at the JiraOperator without additional wrappers.
            # ref: https://atlassian-python-api.readthedocs.io/jira.html
            if isinstance(self.get_jira_resource_method, JiraOperator):
                resource = self.get_jira_resource_method.execute(**context)
            else:
                resource = self.get_jira_resource_method(**context)
        else:
            # Default method execution is on the top level jira client resource
            hook = JiraHook(jira_conn_id=self.jira_conn_id)
            resource = hook.client

        jira_result: Any = getattr(resource, self.method_name)(**self.jira_method_args)

        output = jira_result.get("id", None) if isinstance(jira_result, dict) else None
        self.xcom_push(context, key="id", value=output)

        if self.result_processor:
            return self.result_processor(context, jira_result)

        return jira_result
