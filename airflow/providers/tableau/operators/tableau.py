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
from typing import Optional


from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.tableau.hooks.tableau import TableauHook
from airflow.providers.tableau.sensors.tableau_job_status import TableauJobStatusSensor

RESOURCES_METHODS = {
    'datasources': ['delete', 'download', 'refresh'],
    'groups': ['delete'],
    'projects': ['delete'],
    'schedule': ['delete'],
    'sites': ['delete'],
    'subscriptions': ['delete'],
    'tasks': ['delete', 'run'],
    'users': ['remove'],
    'workbooks': ['delete', 'download', 'refresh'],
}


class TableauOperator(BaseOperator):
    """
    Exectues a Tableau API Resource

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref

    :param resource: The name of the resource to use.
    :type resource: str
    :param method: The name of the resource's method to execute.
    :type method: str
    :param resource_id: The id of resource wich will recive the action.
    :type resource_id: str
    :param site_id: The id of the site where the workbook belongs to.
    :type site_id: Optional[str]
    :param blocking: By default the extract refresh will be blocking means it will wait until it has finished.
    :type blocking: bool
    :param tableau_conn_id: The :ref:`Tableau Connection id <howto/connection:tableau>`
        containing the credentials to authenticate to the Tableau Server.
    :type tableau_conn_id: str
    """

    def __init__(
        self,
        *,
        resource: str,
        method: str,
        resource_id: str,
        site_id: Optional[str] = None,
        blocking_refresh: bool = True,
        tableau_conn_id: str = 'tableau_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource = resource
        self.method = method
        self.resource_id = resource_id
        self.site_id = site_id
        self.blocking_refresh = blocking_refresh
        self.tableau_conn_id = tableau_conn_id

    def execute(self, context: dict) -> str:
        """
        Executes the Tableau API resource and pushes the job id or downloaded file URI to xcom.
        :param context: The task context during execution.
        :type context: dict
        :return: the id of the job that executes the extract refresh or downloaded file URI.
        :rtype: str
        """

        available_resources = RESOURCES_METHODS.keys()
        if self.resource not in available_resources:
            error_message = f'Resource not found! Available Resources: {available_resources}'
            raise AirflowException(error_message)

        available_methods = RESOURCES_METHODS[self.resource]
        if self.method in available_methods:
            error_message = f'Method not found! Available methods for {self.resource}: {available_methods}'
            raise AirflowException(error_message)

        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:

            resource = getattr(tableau_hook.server, self.resource)
            method = getattr(resource, self.resource)

            response = method(self.resource_id)

        if self.method == 'download':
            return response

        elif self.method == 'refresh':

            job_id = response.id

            if self.blocking_refresh:

                TableauJobStatusSensor(
                    job_id=job_id,
                    site_id=self.site_id,
                    tableau_conn_id=self.tableau_conn_id,
                    task_id='wait_until_succeeded',
                    dag=None,
                ).execute(context={})

                self.log.info(
                    '%s has been successfully refreshed.',  self.resource)

            return job_id
