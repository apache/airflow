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

from tableauserverclient import DatasourceItem

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.tableau.hooks.tableau import TableauHook, TableauJobFailedException


class TableauRefreshDatasourceOperator(BaseOperator):
    """
    Refreshes a Tableau Datasource

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#data-sources

    :param datasource_name: The name of the datasource to refresh.
    :type datasource_name: str
    :param site_id: The id of the site where the datasource belongs to.
    :type site_id: Optional[str]
    :param blocking: By default the extract refresh will be blocking means it will wait until it has finished.
    :type blocking: bool
    :param tableau_conn_id: The Tableau Connection id containing the credentials
        to authenticate to the Tableau Server.
    :type tableau_conn_id: str
    """

    def __init__(
        self,
        *,
        datasource_name: str,
        site_id: Optional[str] = None,
        blocking: bool = True,
        tableau_conn_id: str = 'tableau_default',
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.datasource_name = datasource_name
        self.site_id = site_id
        self.blocking = blocking
        self.tableau_conn_id = tableau_conn_id

    def execute(self, context: dict) -> str:
        """
        Executes the Tableau Extract Refresh and pushes the job id to xcom.

        :param context: The task context during execution.
        :type context: dict
        :return: the id of the job that executes the extract refresh
        :rtype: str
        """
        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:
            datasource = self._get_datasource_by_name(tableau_hook)

            job_id = self._refresh_datasource(tableau_hook, datasource.id)
            if self.blocking:
                if not tableau_hook.waiting_until_succeeded(job_id=job_id):
                    raise TableauJobFailedException('The Tableau Refresh Datasource Job failed!')

                self.log.info('Datasource %s has been successfully refreshed.', self.datasource_name)
            return job_id

    def _get_datasource_by_name(self, tableau_hook: TableauHook) -> DatasourceItem:
        for datasource in tableau_hook.get_all(resource_name='datasources'):
            if datasource.name == self.datasource_name:
                self.log.info('Found matching datasource with id %s', datasource.id)
                return datasource

        raise AirflowException(f'Datasource {self.datasource_name} not found!')

    def _refresh_datasource(self, tableau_hook: TableauHook, datasource_id: str) -> str:
        job = tableau_hook.server.datasources.refresh(datasource_id)
        self.log.info('Refreshing Datasource %s...', self.datasource_name)
        return job.id
