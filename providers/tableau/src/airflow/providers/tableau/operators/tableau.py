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

from collections.abc import Sequence
from typing import TYPE_CHECKING

from airflow.providers.common.compat.sdk import AirflowException, BaseOperator
from airflow.providers.tableau.hooks.tableau import (
    TableauHook,
    TableauJobFailedException,
    TableauJobFinishCode,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context

RESOURCES_METHODS = {
    "datasources": ["delete", "refresh"],
    "groups": ["delete"],
    "projects": ["delete"],
    "schedule": ["delete"],
    "sites": ["delete"],
    "subscriptions": ["delete"],
    "tasks": ["delete", "run"],
    "users": ["remove"],
    "workbooks": ["delete", "refresh"],
}


class TableauOperator(BaseOperator):
    """
    Execute a Tableau API Resource.

    https://tableau.github.io/server-client-python/docs/api-ref

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:TableauOperator`

    :param resource: The name of the resource to use.
    :param method: The name of the resource's method to execute.
    :param find: The reference of resource that will receive the action.
    :param match_with: The resource field name to be matched with find parameter.
    :param site_id: The id of the site where the workbook belongs to.
    :param blocking_refresh: By default will be blocking means it will wait until it has finished.
    :param check_interval: time in seconds that the job should wait in
        between each instance state checks until operation is completed
    :param tableau_conn_id: The :ref:`Tableau Connection id <howto/connection:tableau>`
        containing the credentials to authenticate to the Tableau Server.
    """

    template_fields: Sequence[str] = (
        "find",
        "match_with",
    )

    def __init__(
        self,
        *,
        resource: str,
        method: str,
        find: str,
        match_with: str = "id",
        site_id: str | None = None,
        blocking_refresh: bool = True,
        check_interval: float = 20,
        tableau_conn_id: str = "tableau_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource = resource
        self.method = method
        self.find = find
        self.match_with = match_with
        self.check_interval = check_interval
        self.site_id = site_id
        self.blocking_refresh = blocking_refresh
        self.tableau_conn_id = tableau_conn_id

    def execute(self, context: Context) -> str:
        """
        Execute the Tableau API resource and push the job id or downloaded file URI to xcom.

        :param context: The task context during execution.
        :return: the id of the job that executes the extract refresh or downloaded file URI.
        """
        available_resources = RESOURCES_METHODS.keys()
        if self.resource not in available_resources:
            error_message = f"Resource not found! Available Resources: {available_resources}"
            raise AirflowException(error_message)

        available_methods = RESOURCES_METHODS[self.resource]
        if self.method not in available_methods:
            error_message = f"Method not found! Available methods for {self.resource}: {available_methods}"
            raise AirflowException(error_message)

        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:
            resource = getattr(tableau_hook.server, self.resource)
            method = getattr(resource, self.method)

            resource_id = self._get_resource_id(tableau_hook)

            response = method(resource_id)

            job_id = response.id

            if self.method == "refresh":
                if self.blocking_refresh:
                    if not tableau_hook.wait_for_state(
                        job_id=job_id,
                        check_interval=self.check_interval,
                        target_state=TableauJobFinishCode.SUCCESS,
                    ):
                        raise TableauJobFailedException(f"The Tableau Refresh {self.resource} Job failed!")

        return job_id

    def _get_resource_id(self, tableau_hook: TableauHook) -> str:
        if self.match_with == "id":
            return self.find

        for resource in tableau_hook.get_all(resource_name=self.resource):
            if getattr(resource, self.match_with) == self.find:
                resource_id = resource.id
                self.log.info("Found matching with id %s", resource_id)
                self.log.info("Resource object %s", resource)
                self.log.info("Content Name : %s", resource.name)
                self.log.info("Content Folder : %s", resource.project_name)
                self.log.info("Content URL : %s", resource.webpage_url)
                return resource_id

        raise AirflowException(f"{self.resource} with {self.match_with} {self.find} not found!")
