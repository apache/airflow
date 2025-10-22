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

from airflow.providers.common.compat.sdk import BaseSensorOperator
from airflow.providers.tableau.hooks.tableau import (
    TableauHook,
    TableauJobFailedException,
    TableauJobFinishCode,
)

if TYPE_CHECKING:
    from airflow.providers.common.compat.sdk import Context


class TableauJobStatusSensor(BaseSensorOperator):
    """
    Watches the status of a Tableau Server Job.

    .. seealso:: https://tableau.github.io/server-client-python/docs/api-ref#jobs

    :param job_id: Id of the job to watch.
    :param site_id: The id of the site where the workbook belongs to.
    :param tableau_conn_id: The :ref:`Tableau Connection id <howto/connection:tableau>`
        containing the credentials to authenticate to the Tableau Server.
    """

    template_fields: Sequence[str] = ("job_id",)

    def __init__(
        self,
        *,
        job_id: str,
        site_id: str | None = None,
        tableau_conn_id: str = "tableau_default",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.tableau_conn_id = tableau_conn_id
        self.job_id = job_id
        self.site_id = site_id

    def poke(self, context: Context) -> bool:
        """
        Pokes until the job has successfully finished.

        :param context: The task context during execution.
        :return: True if it succeeded and False if not.
        """
        with TableauHook(self.site_id, self.tableau_conn_id) as tableau_hook:
            finish_code = tableau_hook.get_job_status(job_id=self.job_id)
            self.log.info("Current finishCode is %s (%s)", finish_code.name, finish_code.value)

            if finish_code in (TableauJobFinishCode.ERROR, TableauJobFinishCode.CANCELED):
                message = "The Tableau Refresh Workbook Job failed!"
                raise TableauJobFailedException(message)

            return finish_code == TableauJobFinishCode.SUCCESS
