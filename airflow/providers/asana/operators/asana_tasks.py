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
import json

from typing import Dict, Optional

from airflow.models import BaseOperator
from airflow.providers.asana.hooks.asana import AsanaHook
from airflow.utils.decorators import apply_defaults


class AsanaCreateTaskOperator(BaseOperator):
    """
    This operator can be used to create Asana tasks. For more information on Asana optional task parameters, see
    https://developers.asana.com/docs/create-a-task

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaCreateTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param name: Name of the task.
    :type name: str
    :param optional_task_parameters: Any of the optional task creation parameters. See
    https://developers.asana.com/docs/create-a-task for a complete list. You must specify
    at least one of 'workspace', 'parent', or 'projects'
    :type optional_task_parameters: dict
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        name: str,
        optional_task_parameters: Optional[dict] = {},
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.asana_conn_id = asana_conn_id
        self.name = name
        self.optional_task_parameters = optional_task_parameters
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> str:
        asana_client = self.hook.get_conn()

        params = {"name": self.name}
        params.update(self.optional_task_parameters)
        response = asana_client.tasks.create(params=params)

        self.log.info(response)
        return response["gid"]


class AsanaUpdateTaskOperator(BaseOperator):
    """
    This operator can be used to update Asana tasks. For more information on Asana optional task parameters, see
    https://developers.asana.com/docs/update-a-task

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaUpdateTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param asana_task_gid: Asana task ID to update
    :type asana_task_gid: str
    :param optional_task_parameters: Any of the optional task update parameters. See
    https://developers.asana.com/docs/update-a-task for a complete list.
    :type optional_task_parameters: dict
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        asana_task_gid: str,
        optional_task_parameters: Optional[dict] = {},
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.asana_conn_id = asana_conn_id
        self.asana_task_gid = asana_task_gid
        self.optional_task_parameters = optional_task_parameters
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> None:
        asana_client = self.hook.get_conn()

        response = asana_client.tasks.update(task=self.asana_task_gid, params=self.optional_task_parameters)
        self.log.info(response)


class AsanaDeleteTaskOperator(BaseOperator):
    """
    This operator can be used to delete Asana tasks.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:AsanaDeleteTaskOperator`

    :param asana_conn_id: The Asana connection to use.
    :type asana_conn_id: str
    :param asana_task_gid: Asana Task ID to delete.
    :type asana_task_gid: str
    """

    @apply_defaults
    def __init__(
        self,
        *,
        asana_conn_id: str,
        asana_task_gid: str,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.asana_conn_id = asana_conn_id
        self.asana_task_gid = asana_task_gid
        self.hook = AsanaHook(conn_id=self.asana_conn_id)

    def execute(self, context: Dict) -> None:
        asana_client = self.hook.get_conn()
        response = asana_client.tasks.delete_task(self.asana_task_gid)
        self.log.info(response)
