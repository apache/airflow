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

import warnings
from typing import TYPE_CHECKING

from airflow.models import BaseOperator
from airflow.providers.tableau.operators.tableau import TableauOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


warnings.warn(
    "This module is deprecated. Please use `airflow.providers.tableau.operators.tableau`.",
    DeprecationWarning,
    stacklevel=2,
)


class TableauRefreshWorkbookOperator(BaseOperator):
    """
    This operator is deprecated. Please use `airflow.providers.tableau.operators.tableau`.

    Refreshes a Tableau Workbook/Extract

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#workbooks

    :param workbook_name: The name of the workbook to refresh.
    :param site_id: The id of the site where the workbook belongs to.
    :param blocking: Defines if the job waits until the refresh has finished.
        Default: True.
    :param tableau_conn_id: The :ref:`Tableau Connection id <howto/connection:tableau>`
        containing the credentials to authenticate to the Tableau Server. Default:
        'tableau_default'.
    :param check_interval: time in seconds that the job should wait in
        between each instance state checks until operation is completed
    """

    def __init__(
        self,
        *,
        workbook_name: str,
        site_id: str | None = None,
        blocking: bool = True,
        tableau_conn_id: str = 'tableau_default',
        check_interval: float = 20,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.workbook_name = workbook_name
        self.site_id = site_id
        self.blocking = blocking
        self.tableau_conn_id = tableau_conn_id
        self.check_interval = check_interval

    def execute(self, context: Context) -> str:
        """
        Executes the Tableau Extract Refresh and pushes the job id to xcom.

        :param context: The task context during execution.
        :return: the id of the job that executes the extract refresh
        :rtype: str
        """
        job_id = TableauOperator(
            resource='workbooks',
            method='refresh',
            find=self.workbook_name,
            match_with='name',
            site_id=self.site_id,
            tableau_conn_id=self.tableau_conn_id,
            blocking_refresh=self.blocking,
            check_interval=self.check_interval,
            task_id='refresh_workbook',
            dag=None,
        ).execute(context=context)

        return job_id
