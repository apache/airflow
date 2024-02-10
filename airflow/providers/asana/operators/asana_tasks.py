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

from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.asana.hooks.asana import AsanaHook

if TYPE_CHECKING:
    from airflow.utils.context import Context


class AsanaCreateTaskOperator(BaseOperator):
    """
    This operator can be used to create Asana tasks.

    .. seealso::
        For more information on Asana optional task parameters:
        https://developers.asana.com/docs/create-a-task

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaCreateTaskOperator`

    :param conn_id: The Asana connection to use.
    :param name: Name of the Asana task.
    :param task_parameters: Any of the optional task creation parameters.
        See https://developers.asana.com/docs/create-a-task for a complete list.
        You must specify at least one of 'workspace', 'parent', or 'projects'
        either here or in the connection.
    """

    def __init__(
        self,
        *,
        conn_id: str,
        name: str,
        task_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.name = name
        self.task_parameters = task_parameters

    def execute(self, context: Context) -> str:
        hook = AsanaHook(conn_id=self.conn_id)
        response = hook.create_task(self.name, self.task_parameters)
        self.log.info(response)
        return response["gid"]


class AsanaUpdateTaskOperator(BaseOperator):
    """
    This operator can be used to update Asana tasks.

    .. seealso::
        For more information on Asana optional task parameters:
        https://developers.asana.com/docs/update-a-task

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaUpdateTaskOperator`

    :param conn_id: The Asana connection to use.
    :param asana_task_gid: Asana task ID to update
    :param task_parameters: Any task parameters that should be updated.
        See https://developers.asana.com/docs/update-a-task for a complete list.
    """

    def __init__(
        self,
        *,
        conn_id: str,
        asana_task_gid: str,
        task_parameters: dict,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.asana_task_gid = asana_task_gid
        self.task_parameters = task_parameters

    def execute(self, context: Context) -> None:
        hook = AsanaHook(conn_id=self.conn_id)
        response = hook.update_task(self.asana_task_gid, self.task_parameters)
        self.log.info(response)


class AsanaDeleteTaskOperator(BaseOperator):
    """
    This operator can be used to delete Asana tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaDeleteTaskOperator`

    :param conn_id: The Asana connection to use.
    :param asana_task_gid: Asana Task ID to delete.
    """

    def __init__(
        self,
        *,
        conn_id: str,
        asana_task_gid: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.asana_task_gid = asana_task_gid

    def execute(self, context: Context) -> None:
        hook = AsanaHook(conn_id=self.conn_id)
        response = hook.delete_task(self.asana_task_gid)
        self.log.info(response)


class AsanaFindTaskOperator(BaseOperator):
    """
    This operator can be used to retrieve Asana tasks that match various filters.

    .. seealso::
        For a list of possible filters:
        https://developers.asana.com/docs/update-a-task

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaFindTaskOperator`

    :param conn_id: The Asana connection to use.
    :param search_parameters: The parameters used to find relevant tasks. You must
        specify at least one of `project`, `section`, `tag`, `user_task_list`, or both
        `assignee` and `workspace` either here or in the connection.
    """

    def __init__(
        self,
        *,
        conn_id: str,
        search_parameters: dict | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.conn_id = conn_id
        self.search_parameters = search_parameters

    def execute(self, context: Context) -> list:
        hook = AsanaHook(conn_id=self.conn_id)
        response = hook.find_task(self.search_parameters)
        self.log.info(response)
        return response
