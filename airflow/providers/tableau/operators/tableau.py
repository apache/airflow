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
from airflow.providers.tableau.hooks.tableau import TableauHook, TableauJobFinishCode
from airflow.providers.tableau.sensors.tableau_job_status import TableauJobFailedException

RESOURCES_METHODS = {
    'datasources': ['delete', 'refresh'],
    'groups': ['delete'],
    'projects': ['delete'],
    'schedule': ['delete'],
    'sites': ['delete'],
    'subscriptions': ['delete'],
    'tasks': ['delete', 'run'],
    'users': ['remove'],
    'workbooks': ['delete', 'refresh'],
}


class TableauOperator(BaseOperator):
    """
    Exectues a Tableau API Resource

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref

    :param resource: The name of the resource to use.
    :type resource: str
    :param method: The name of the resource's method to execute.
    :type method: str
    :param find: The reference of resource wich will recive the action.
    :type find: str
    :param match_with: The resource field name to be matched with find parameter.
    :type match_with: Optional[str]
    :param site_id: The id of the site where the workbook belongs to.
    :type site_id: Optional[str]
    :param blocking_refresh: By default the extract refresh will be blocking means it will wait until it has finished.
    :type blocking_refresh: bool
    :param tableau_conn_id: The :ref:`Tableau Connection id <howto/connection:tableau>`
        containing the credentials to authenticate to the Tableau Server.
    :type tableau_conn_id: str
    """

    def __init__(
        self,
        *,
        resource: str,
        method: str,
        find: str,
        match_with: str = 'id',
        site_id: Optional[str] = None,
        blocking_refresh: bool = True,
        tableau_conn_id: str = 'tableau_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.resource = resource
        self.method = method
        self.find = find
        self.match_with = match_with
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

            resource_id = self._get_resource_id(tableau_hook)

            response = method(resource_id)

        if self.method == 'refresh':

            job_id = response.id

            if self.blocking_refresh:

                finish_code = TableauJobFinishCode.PENDING
                negative_codes = (TableauJobFinishCode.ERROR, TableauJobFinishCode.CANCELED)
                while not finish_code == TableauJobFinishCode.SUCCESS:
                    return_code = int(tableau_hook.server.jobs.get_by_id(job_id).finish_code)
                    finish_code = TableauJobFinishCode(return_code)
                    if finish_code in negative_codes:
                        raise TableauJobFailedException(f'The Tableau Refresh {self.resource} Job failed!')

                self.log.info(f'{self.resource} has been successfully refreshed.')

            return job_id

    def _get_resource_id(self, tableau_hook: TableauHook) -> str:

        if self.match_with == 'id':
            return self.find

        for resource in tableau_hook.get_all(resource_name=self.resource):
            if getattr(resource, self.match_with) == self.find:
                resource_id = resource.id
                self.log.info('Found matching with id %s', resource_id)
                return resource_id

        raise AirflowException(
            f'{self.resource} with {self.match_with} {self.find} not found!')
